use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::{
    IntervalCache, SortedData, TagSet, ValueAwareLapperCache, extract_tags_from_batch,
};

use std::collections::HashSet;

// Global data loaded once and shared across all benchmarks
static PARQUET_DATA: OnceLock<Vec<(u64, TagSet)>> = OnceLock::new();

/// Calculate cardinality (unique tag combinations) from data
fn calculate_cardinality(data: &[(u64, TagSet)]) -> usize {
    let mut unique_combinations = HashSet::new();

    for (_, tag_set) in data {
        let mut combination = Vec::new();
        for (key, value) in tag_set {
            combination.push(format!("{}={}", key, value));
        }
        combination.sort();
        unique_combinations.insert(combination.join(","));
    }

    unique_combinations.len()
}

/// Load data from all parquet files in a directory
/// Stops when total cardinality reaches 1M or 7 days of data, whichever comes first
fn load_parquet_files(dir_path: &Path) -> std::io::Result<Vec<(u64, TagSet)>> {
    let mut all_data = Vec::new();
    let max_cardinality = 1_000_000;
    let max_duration_ns = 7 * 24 * 60 * 60 * 1_000_000_000u64; // 7 days in nanoseconds

    let mut total_rows = 0usize;
    let mut total_duration_ns = 0u64;
    let mut total_cardinality = 0usize;

    // Collect all parquet files first
    let mut parquet_files: Vec<_> = fs::read_dir(dir_path)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    parquet_files.sort();

    println!(
        "\nLoading parquet files (stopping at {}M cardinality or 7 days of data)...\n",
        max_cardinality / 1_000_000
    );

    // Read all parquet files in the directory
    for (file_index, path) in parquet_files.iter().enumerate() {
        println!("Loading: {:?}", path.file_name().unwrap_or_default());

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let reader = builder
            .build()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut file_data = Vec::new();
        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    let tags = extract_tags_from_batch(&batch);
                    file_data.extend(tags);
                }
                Err(e) => {
                    eprintln!("  Warning: Failed to read batch: {}", e);
                    continue;
                }
            }
        }

        // Calculate file statistics
        let file_rows = file_data.len();

        let file_min_ts = file_data.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
        let file_max_ts = file_data.iter().map(|(ts, _)| *ts).max().unwrap_or(0);
        let file_duration_ns = file_max_ts.saturating_sub(file_min_ts);
        let file_duration_secs = file_duration_ns / 1_000_000_000;

        // Calculate cardinality before and after adding this file
        let cardinality_before = calculate_cardinality(&all_data);
        all_data.extend(file_data);
        let cardinality_after = calculate_cardinality(&all_data);
        let file_cardinality = cardinality_after - cardinality_before;

        // Update totals
        total_rows += file_rows;
        total_duration_ns += file_duration_ns;
        total_cardinality = cardinality_after;

        // Calculate total duration in seconds for display
        let total_duration_secs = total_duration_ns / 1_000_000_000;

        // Print statistics
        println!("  Rows: {} ({} total)", file_rows, total_rows);
        println!(
            "  Duration: {}s ({} total)",
            file_duration_secs, total_duration_secs
        );
        println!(
            "  Cardinality: +{} ({} total)",
            file_cardinality, total_cardinality
        );
        println!();

        // Stop if we've reached either limit
        if total_cardinality >= max_cardinality {
            println!(
                "Reached target cardinality of {}M. Stopping.",
                max_cardinality / 1_000_000
            );
            println!(
                "Loaded {} files with {} total rows\n",
                file_index + 1,
                total_rows
            );
            break;
        }

        if total_duration_ns >= max_duration_ns {
            let days = total_duration_ns / (24 * 60 * 60 * 1_000_000_000);
            let hours =
                (total_duration_ns % (24 * 60 * 60 * 1_000_000_000)) / (60 * 60 * 1_000_000_000);
            println!(
                "Reached target duration of 7 days ({} days {} hours). Stopping.",
                days, hours
            );
            println!(
                "Loaded {} files with {} total rows\n",
                file_index + 1,
                total_rows
            );
            break;
        }
    }

    println!("=== Final Statistics ===");
    println!("Total rows: {}", total_rows);
    println!("Total duration: {}s", total_duration_ns / 1_000_000_000);
    println!("Total cardinality: {}", total_cardinality);
    println!();

    Ok(all_data)
}

/// Load the parquet data once and return a reference to it
fn get_parquet_data() -> Option<&'static Vec<(u64, TagSet)>> {
    PARQUET_DATA.get_or_init(|| {
        let parquet_dir = std::path::PathBuf::from("benches/data/parquet");

        // Check if the parquet data directory exists
        if !parquet_dir.exists() {
            eprintln!(
                "Error: Parquet data directory not found at {:?}",
                parquet_dir
            );
            eprintln!(
                "Please create benches/data/parquet and add parquet files before running benchmarks"
            );
            return Vec::new();
        }

        match load_parquet_files(&parquet_dir) {
            Ok(data) => {
                println!("\n=== Data Statistics ===");
                println!("Lines loaded: {}", data.len());

                // Display time range if data is not empty
                if !data.is_empty() {
                    let timestamps: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                    let min_ts = *timestamps.iter().min().unwrap();
                    let max_ts = *timestamps.iter().max().unwrap();

                    println!("Timestamp range: {} to {}", min_ts, max_ts);

                    let duration_ns = max_ts - min_ts;
                    let duration_secs = duration_ns / 1_000_000_000;
                    let hours = duration_secs / 3600;
                    let minutes = (duration_secs % 3600) / 60;
                    let seconds = duration_secs % 60;

                    println!("Duration: {}h {}m {}s", hours, minutes, seconds);
                    println!("======================\n");
                }

                data
            }
            Err(e) => {
                eprintln!("Error loading parquet files: {}", e);
                Vec::new()
            }
        }
    });

    let data = PARQUET_DATA.get().unwrap();
    if data.is_empty() { None } else { Some(data) }
}

/// Benchmark building ValueAwareLapperCache from parquet data
fn bench_build_cache(c: &mut Criterion) {
    // Get reference to shared data
    let parsed_data = match get_parquet_data() {
        Some(data) => data,
        None => return,
    };

    let mut group = c.benchmark_group("parquet_cache_build");
    group.throughput(Throughput::Elements(parsed_data.len() as u64));

    // Test different resolutions
    let resolutions = [
        ("nanosecond", Duration::from_nanos(1)),
        ("5second", Duration::from_secs(5)),
        ("15second", Duration::from_secs(15)),
        ("1minute", Duration::from_secs(60)),
        ("3minute", Duration::from_secs(180)),
        ("5minute", Duration::from_secs(300)),
    ];

    // Using TagSet for direct tag handling
    let sorted_data = SortedData::from_unsorted(parsed_data.clone());

    for (name, resolution) in &resolutions {
        group.bench_function(format!("{}_resolution", name), |b| {
            // Build cache once to get statistics on first iteration
            let cache = ValueAwareLapperCache::from_sorted_with_resolution(
                sorted_data.clone(),
                *resolution,
            )
            .unwrap();

            println!("\n=== {} Resolution ===", name);
            println!("  Intervals: {}", cache.interval_count());
            println!(
                "  Size: {:.2} MB",
                cache.size_bytes() as f64 / (1024.0 * 1024.0)
            );
            drop(cache);

            b.iter_batched(
                || sorted_data.clone(),
                |data| {
                    let cache = ValueAwareLapperCache::from_sorted_with_resolution(
                        data,
                        *resolution,
                    )
                    .unwrap();
                    black_box(cache);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    drop(sorted_data);

    group.finish();
}

/// Benchmark querying the cache with ranges
fn bench_query_cache(c: &mut Criterion) {
    // Get reference to shared data
    let parsed_data = match get_parquet_data() {
        Some(data) => data,
        None => return,
    };

    // Get the original nanosecond time range for calculating equivalent wall-clock durations
    let original_timestamps: Vec<u64> = parsed_data.iter().map(|(ts, _)| *ts).collect();
    let original_min_ns = *original_timestamps.iter().min().unwrap();
    let original_max_ns = *original_timestamps.iter().max().unwrap();
    let original_range_ns = original_max_ns - original_min_ns;

    // Use 10% of the wall-clock time range for queries
    let wall_clock_range_ns = (original_range_ns as f64 * 0.10) as u64;

    let mut group = c.benchmark_group("parquet_cache_query");

    // Test queries with different timestamp resolutions
    let resolutions = [
        ("nanosecond", Duration::from_nanos(1)),
        ("5second", Duration::from_secs(5)),
        ("15second", Duration::from_secs(15)),
        ("1minute", Duration::from_secs(60)),
        ("3minute", Duration::from_secs(180)),
        ("5minute", Duration::from_secs(300)),
    ];

    // Using TagSet for direct tag handling
    let sorted_data = SortedData::from_unsorted(parsed_data.clone());

    let n_queries = 100;

    // Create test ranges distributed across the time span (in nanoseconds)
    // Each range covers the same wall-clock duration
    let test_ranges: Vec<std::ops::Range<u64>> = (0..n_queries)
        .map(|i| {
            let start = original_min_ns
                + ((original_range_ns - wall_clock_range_ns) * i as u64) / (n_queries as u64);
            let end = start + wall_clock_range_ns;
            start..end
        })
        .collect();

    group.throughput(Throughput::Elements(n_queries as u64));

    for (name, resolution) in &resolutions {
        group.bench_function(BenchmarkId::new("range_10pct", name), |b| {
            // Build cache for this resolution
            let cache = ValueAwareLapperCache::from_sorted_with_resolution(
                sorted_data.clone(),
                *resolution,
            )
            .unwrap();

            b.iter(|| {
                for range in test_ranges.iter() {
                    black_box(cache.query_range(range.clone()));
                }
            });
        });
    }

    drop(sorted_data);
    group.finish();
}

/// Benchmark appending to an existing cache
#[allow(dead_code)]
fn bench_append_cache(c: &mut Criterion) {
    // Get reference to shared data
    let parsed_data = match get_parquet_data() {
        Some(data) => data,
        None => return,
    };

    let mut group = c.benchmark_group("parquet_cache_append");

    // Split data for initial build and append
    let split_point = parsed_data.len() / 2;

    let resolutions = [
        ("nanosecond", Duration::from_nanos(1)),
        ("5second", Duration::from_secs(5)),
        ("15second", Duration::from_secs(15)),
        ("1minute", Duration::from_secs(60)),
        ("3minute", Duration::from_secs(180)),
        ("5minute", Duration::from_secs(300)),
    ];

    // Test append for different timestamp resolutions
    for (name, resolution) in &resolutions {
        let initial_data = parsed_data[..split_point].to_vec();
        let append_data = parsed_data[split_point..].to_vec();

        // Using TagSet for direct tag handling
        let sorted_initial = SortedData::from_unsorted(initial_data);
        let sorted_append = SortedData::from_unsorted(append_data.clone());

        group.throughput(Throughput::Elements(append_data.len() as u64));

        group.bench_function(format!("append_50pct_{}", name), |b| {
            b.iter_batched(
                || {
                    (
                        ValueAwareLapperCache::from_sorted_with_resolution(
                            sorted_initial.clone(),
                            *resolution,
                        )
                        .unwrap(),
                        sorted_append.clone(),
                    )
                },
                |(mut cache, data)| {
                    cache.append_sorted(data).unwrap();
                    black_box(cache);
                },
                criterion::BatchSize::SmallInput,
            );
        });

        drop(sorted_initial);
        drop(sorted_append);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_build_cache,
    bench_query_cache,
    // bench_append_cache, // Temporarily disabled - takes a while and not a current focus
);
criterion_main!(benches);
