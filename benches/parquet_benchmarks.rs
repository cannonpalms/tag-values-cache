use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
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

/// File metadata extracted from parquet without loading full data
#[derive(Debug, Clone)]
struct FileMetadata {
    path: std::path::PathBuf,
    num_rows: usize,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
}

/// Extract min/max timestamps from parquet file metadata statistics
fn get_timestamp_range(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> (Option<i64>, Option<i64>) {
    use parquet::file::statistics::Statistics;

    let mut overall_min: Option<i64> = None;
    let mut overall_max: Option<i64> = None;

    // Iterate through row groups to find timestamp column statistics
    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            // Look for time/timestamp column
            let col_name = column.column_descr().name().to_lowercase();
            if col_name == "time"
                || col_name == "timestamp"
                || col_name == "_time"
                || col_name == "eventtime"
            {
                if let Some(stats) = column.statistics() {
                    // Try to extract as Int64 (most common for timestamps)
                    if let Statistics::Int64(int_stats) = stats {
                        if let (Some(min), Some(max)) = (int_stats.min_opt(), int_stats.max_opt()) {
                            overall_min = Some(overall_min.map_or(*min, |m| m.min(*min)));
                            overall_max = Some(overall_max.map_or(*max, |m| m.max(*max)));
                        }
                    }
                }
            }
        }
    }

    (overall_min, overall_max)
}

/// Load data from all parquet files in a directory
/// Stops when row count reaches 10M OR duration reaches 1 week
fn load_parquet_files(dir_path: &Path) -> std::io::Result<Vec<(u64, TagSet)>> {
    let max_rows = 10_000_000;
    let max_duration_ns = 7 * 24 * 60 * 60 * 1_000_000_000u64; // 1 week in nanoseconds

    // Collect all parquet files first
    let parquet_files: Vec<_> = fs::read_dir(dir_path)?
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

    // Phase 1: Scan metadata from all files in parallel
    let mut file_metadata: Vec<FileMetadata> = parquet_files
        .par_iter()
        .filter_map(|path| {
            let file = File::open(path).ok()?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
            let metadata = builder.metadata();
            let num_rows = metadata.file_metadata().num_rows() as usize;
            let (min_ts, max_ts) = get_timestamp_range(metadata);

            Some(FileMetadata {
                path: path.clone(),
                num_rows,
                min_timestamp: min_ts,
                max_timestamp: max_ts,
            })
        })
        .collect();

    // Sort by min timestamp (oldest first)
    file_metadata.sort_by_key(|metadata| metadata.min_timestamp.unwrap_or(i64::MAX));

    // Phase 2: Select consecutive files based on metadata timestamps, then load in chunks
    // First, filter to only consecutive files
    let mut consecutive_files = Vec::new();
    let mut prev_max_ts: Option<i64> = None;
    const MAX_GAP_NS: i64 = 25 * 60 * 60 * 1_000_000_000; // 25 hours gap tolerance

    for metadata in file_metadata {
        if let (Some(file_min), Some(file_max)) = (metadata.min_timestamp, metadata.max_timestamp) {
            // Check if this file is consecutive with the previous one
            let is_consecutive = if let Some(prev_max) = prev_max_ts {
                let gap = file_min.saturating_sub(prev_max);
                gap <= MAX_GAP_NS
            } else {
                true // First file is always accepted
            };

            if is_consecutive {
                consecutive_files.push(metadata.clone());
                prev_max_ts = Some(file_max);
            } else {
                // Gap detected - stop looking for more files
                break;
            }
        } else {
            // No timestamp metadata - skip this file
            continue;
        }
    }

    // Phase 3: Load consecutive files in chunks of 4
    const CHUNK_SIZE: usize = 4;
    let mut all_data = Vec::new();
    let mut overall_min_ts: Option<u64> = None;
    let mut overall_max_ts: Option<u64> = None;

    for chunk in consecutive_files.chunks(CHUNK_SIZE) {
        // Load this chunk of files in parallel
        let chunk_results: Vec<_> = chunk
            .par_iter()
            .filter_map(|metadata| {
                let file = File::open(&metadata.path).ok()?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
                let reader = builder.build().ok()?;

                let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().ok()?;

                let file_data: Vec<(u64, TagSet)> = batches
                    .par_iter()
                    .flat_map(|batch| extract_tags_from_batch(batch))
                    .collect();

                let file_rows = file_data.len();
                let file_min_ts = file_data.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
                let file_max_ts = file_data.iter().map(|(ts, _)| *ts).max().unwrap_or(0);

                println!(
                    "{:?}: {} rows",
                    metadata.path.file_name().unwrap_or_default(),
                    file_rows,
                );

                Some((file_data, file_min_ts, file_max_ts))
            })
            .collect();

        // Add results from this chunk and check limits
        for (file_data, file_min_ts, file_max_ts) in chunk_results {
            all_data.extend(file_data);

            // Update overall timestamp range
            overall_min_ts = Some(overall_min_ts.map_or(file_min_ts, |m| m.min(file_min_ts)));
            overall_max_ts = Some(overall_max_ts.map_or(file_max_ts, |m| m.max(file_max_ts)));

            // Check limits
            if all_data.len() >= max_rows {
                all_data.truncate(max_rows);
                break;
            }

            if let (Some(min), Some(max)) = (overall_min_ts, overall_max_ts) {
                let duration_ns = max.saturating_sub(min);
                if duration_ns >= max_duration_ns {
                    break;
                }
            }
        }

        // Break out of chunk loop if we hit a limit
        if all_data.len() >= max_rows {
            break;
        }

        if let (Some(min), Some(max)) = (overall_min_ts, overall_max_ts) {
            let duration_ns = max.saturating_sub(min);
            if duration_ns >= max_duration_ns {
                break;
            }
        }
    }

    let cardinality = calculate_cardinality(&all_data);

    println!("\nTotal rows: {}", all_data.len());
    println!("Total cardinality: {}", cardinality);
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
        ("1hour", Duration::from_secs(3600)),
    ];

    // Using TagSet for direct tag handling
    for (name, resolution) in &resolutions {
        // Build cache once to get statistics on first iteration
        let cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(parsed_data.clone(), *resolution)
                .unwrap();

        println!("\n=== {} Resolution ===", name);
        println!("  Intervals: {}", cache.interval_count());
        println!(
            "  Size: {:.2} MB",
            cache.size_bytes() as f64 / (1024.0 * 1024.0)
        );
        group.bench_function(format!("{}_resolution", name), |b| {
            b.iter_batched(
                || parsed_data.clone(),
                |data| {
                    let cache =
                        ValueAwareLapperCache::from_unsorted_with_resolution(data, *resolution)
                            .unwrap();
                    black_box(cache);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

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
    let wall_clock_range_ns_10pct = (original_range_ns as f64 * 0.10) as u64;

    let mut group = c.benchmark_group("parquet_cache_query");

    // Test queries with different timestamp resolutions
    let resolutions = [
        ("nanosecond", Duration::from_nanos(1)),
        ("5second", Duration::from_secs(5)),
        ("15second", Duration::from_secs(15)),
        ("1minute", Duration::from_secs(60)),
        ("3minute", Duration::from_secs(180)),
        ("5minute", Duration::from_secs(300)),
        ("1hour", Duration::from_secs(3600)),
    ];

    let n_queries = 100;

    // Create test ranges for 10% queries - distributed across the time span (in nanoseconds)
    // Each range covers the same wall-clock duration
    let test_ranges_10pct: Vec<std::ops::Range<u64>> = (0..n_queries)
        .map(|i| {
            let start = original_min_ns
                + ((original_range_ns - wall_clock_range_ns_10pct) * i as u64) / (n_queries as u64);
            let end = start + wall_clock_range_ns_10pct;
            start..end
        })
        .collect();

    // Create test range for 100% query - entire time span
    let test_range_100pct = original_min_ns..original_max_ns;

    group.throughput(Throughput::Elements(n_queries as u64));

    for (name, resolution) in &resolutions {
        group.bench_function(BenchmarkId::new("range_10pct", name), |b| {
            // Build cache for this resolution
            let cache = ValueAwareLapperCache::from_unsorted_with_resolution(
                parsed_data.clone(),
                *resolution,
            )
            .unwrap();

            b.iter(|| {
                for range in test_ranges_10pct.iter() {
                    black_box(cache.query_range(range.clone()));
                }
            });
        });

        group.bench_function(BenchmarkId::new("range_100pct", name), |b| {
            // Build cache for this resolution
            let cache = ValueAwareLapperCache::from_unsorted_with_resolution(
                parsed_data.clone(),
                *resolution,
            )
            .unwrap();

            b.iter(|| {
                black_box(cache.query_range(test_range_100pct.clone()));
            });
        });
    }
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
        ("1hour", Duration::from_secs(3600)),
    ];

    // Test append for different timestamp resolutions
    for (name, resolution) in &resolutions {
        let initial_data = parsed_data[..split_point].to_vec();
        let append_data = parsed_data[split_point..].to_vec();

        group.throughput(Throughput::Elements(append_data.len() as u64));

        group.bench_function(format!("append_50pct_{}", name), |b| {
            b.iter_batched(
                || {
                    (
                        ValueAwareLapperCache::from_unsorted_with_resolution(
                            initial_data.clone(),
                            *resolution,
                        )
                        .unwrap(),
                        append_data.clone(),
                    )
                },
                |(mut cache, data)| {
                    cache.append_unsorted(data).unwrap();
                    black_box(cache);
                },
                criterion::BatchSize::SmallInput,
            );
        });
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
