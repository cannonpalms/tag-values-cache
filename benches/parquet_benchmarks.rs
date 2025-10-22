use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tag_values_cache::{
    ArrowValue, RecordBatchRow, SortedData, ValueAwareLapperCache, IntervalCache,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

use std::collections::HashSet;

/// Calculate cardinality (unique tag combinations) from data
fn calculate_cardinality(data: &[(u64, RecordBatchRow)]) -> usize {
    let mut unique_combinations = HashSet::new();

    for (_, row) in data {
        let mut combination = Vec::new();
        for (key, value) in &row.values {
            combination.push(format!("{}={}", key, value));
        }
        combination.sort();
        unique_combinations.insert(combination.join(","));
    }

    unique_combinations.len()
}

/// Load data from all parquet files in a directory
/// Stops when total cardinality reaches 1M or 7 days of data, whichever comes first
fn load_parquet_files(dir_path: &Path) -> std::io::Result<Vec<(u64, RecordBatchRow)>> {
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

    println!("\nLoading parquet files (stopping at {}M cardinality or 7 days of data)...\n", max_cardinality / 1_000_000);

    // Read all parquet files in the directory
    for (file_index, path) in parquet_files.iter().enumerate() {
        println!("Loading: {:?}", path.file_name().unwrap_or_default());

        let file = File::open(&path)?;
        let reader = SerializedFileReader::new(file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut row_iter = reader.get_row_iter(None)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut file_data = Vec::new();
        while let Some(record_result) = row_iter.next() {
            match record_result {
                Ok(record) => {
                    if let Some((timestamp, row)) = parse_parquet_row(&record) {
                        file_data.push((timestamp, row));
                    }
                }
                Err(e) => {
                    eprintln!("  Warning: Failed to read row: {}", e);
                    continue;
                }
            }
        }

        // Calculate file statistics
        let file_rows = file_data.len();

        let file_min_ts = file_data.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
        let file_max_ts = file_data.iter().map(|(ts, _)| *ts).max().unwrap_or(0);
        let file_duration_ns = if file_max_ts > file_min_ts {
            file_max_ts - file_min_ts
        } else {
            0
        };
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
        println!("  Duration: {}s ({} total)", file_duration_secs, total_duration_secs);
        println!("  Cardinality: +{} ({} total)", file_cardinality, total_cardinality);
        println!();

        // Stop if we've reached either limit
        if total_cardinality >= max_cardinality {
            println!("Reached target cardinality of {}M. Stopping.", max_cardinality / 1_000_000);
            println!("Loaded {} files with {} total rows\n", file_index + 1, total_rows);
            break;
        }

        if total_duration_ns >= max_duration_ns {
            let days = total_duration_ns / (24 * 60 * 60 * 1_000_000_000);
            let hours = (total_duration_ns % (24 * 60 * 60 * 1_000_000_000)) / (60 * 60 * 1_000_000_000);
            println!("Reached target duration of 7 days ({} days {} hours). Stopping.", days, hours);
            println!("Loaded {} files with {} total rows\n", file_index + 1, total_rows);
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

/// Parse a single parquet row into our cache format
/// Only string columns are treated as tags; numeric/boolean fields are ignored
fn parse_parquet_row(record: &parquet::record::Row) -> Option<(u64, RecordBatchRow)> {
    let mut values = BTreeMap::new();
    let mut timestamp: Option<u64> = None;

    // Iterate through all fields in the row
    for (name, field) in record.get_column_iter() {
        match name.as_str() {
            // Look for common timestamp field names
            "timestamp" | "time" | "ts" => {
                timestamp = match field {
                    parquet::record::Field::Long(v) => Some(*v as u64),
                    parquet::record::Field::ULong(v) => Some(*v),
                    _ => None,
                };
            }
            // Only process string fields as tags (ignore numeric/boolean fields)
            _ => {
                if let parquet::record::Field::Str(s) = field {
                    values.insert(name.clone(), ArrowValue::String(s.to_string()));
                }
                // All other field types (numeric, boolean, etc.) are ignored
            }
        }
    }

    timestamp.map(|ts| (ts, RecordBatchRow::new(values)))
}

/// Truncate timestamps to 5-second resolution
fn truncate_to_5sec(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 5;
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 15-second resolution
fn truncate_to_15sec(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 15;
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 1-minute resolution
fn truncate_to_1min(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 60;
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 3-minute resolution
fn truncate_to_3min(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 180;
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 5-minute resolution
fn truncate_to_5min(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 300;
            (bucket, row)
        })
        .collect()
}

/// Load the parquet data once and return it
fn load_parquet_data() -> Option<Vec<(u64, RecordBatchRow)>> {
    let parquet_dir = std::path::PathBuf::from("benches/data/parquet");

    // Check if the parquet data directory exists
    if !parquet_dir.exists() {
        eprintln!("Error: Parquet data directory not found at {:?}", parquet_dir);
        eprintln!("Please create benches/data/parquet and add parquet files before running benchmarks");
        return None;
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

            Some(data)
        }
        Err(e) => {
            eprintln!("Error loading parquet files: {}", e);
            None
        }
    }
}

/// Benchmark building ValueAwareLapperCache from parquet data
fn bench_build_cache(c: &mut Criterion) {
    // Load data once
    let parsed_data = match load_parquet_data() {
        Some(data) => data,
        None => return,
    };

    let mut group = c.benchmark_group("parquet_cache_build");
    group.throughput(Throughput::Elements(parsed_data.len() as u64));

    // Test with nanosecond resolution (original)
    let sorted_data_ns = SortedData::from_unsorted(parsed_data.clone());

    // Build cache once to get statistics
    let cache_ns = ValueAwareLapperCache::from_sorted(sorted_data_ns.clone()).unwrap();
    println!("\n=== Nanosecond Resolution ===");
    println!("  Intervals: {}", cache_ns.interval_count());
    println!("  Size: {:.2} MB", cache_ns.size_bytes() as f64 / (1024.0 * 1024.0));

    group.bench_function("nanosecond_resolution", |b| {
        b.iter_batched(
            || sorted_data_ns.clone(),
            |data| {
                let cache = ValueAwareLapperCache::from_sorted(data).unwrap();
                black_box(cache);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(cache_ns);
    drop(sorted_data_ns);

    // Test with 5-second resolution
    let data_5sec = truncate_to_5sec(parsed_data.clone());
    let sorted_data_5sec = SortedData::from_unsorted(data_5sec);

    let cache_5sec = ValueAwareLapperCache::from_sorted(sorted_data_5sec.clone()).unwrap();
    println!("\n=== 5-Second Resolution ===");
    println!("  Intervals: {}", cache_5sec.interval_count());
    println!("  Size: {:.2} MB", cache_5sec.size_bytes() as f64 / (1024.0 * 1024.0));

    group.bench_function("5second_resolution", |b| {
        b.iter_batched(
            || sorted_data_5sec.clone(),
            |data| {
                let cache = ValueAwareLapperCache::from_sorted(data).unwrap();
                black_box(cache);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(cache_5sec);
    drop(sorted_data_5sec);

    // Test with 15-second resolution
    let data_15sec = truncate_to_15sec(parsed_data.clone());
    let sorted_data_15sec = SortedData::from_unsorted(data_15sec);

    let cache_15sec = ValueAwareLapperCache::from_sorted(sorted_data_15sec.clone()).unwrap();
    println!("\n=== 15-Second Resolution ===");
    println!("  Intervals: {}", cache_15sec.interval_count());
    println!("  Size: {:.2} MB", cache_15sec.size_bytes() as f64 / (1024.0 * 1024.0));

    group.bench_function("15second_resolution", |b| {
        b.iter_batched(
            || sorted_data_15sec.clone(),
            |data| {
                let cache = ValueAwareLapperCache::from_sorted(data).unwrap();
                black_box(cache);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(cache_15sec);
    drop(sorted_data_15sec);

    // Test with 1-minute resolution
    let data_1min = truncate_to_1min(parsed_data.clone());
    let sorted_data_1min = SortedData::from_unsorted(data_1min);

    let cache_1min = ValueAwareLapperCache::from_sorted(sorted_data_1min.clone()).unwrap();
    println!("\n=== 1-Minute Resolution ===");
    println!("  Intervals: {}", cache_1min.interval_count());
    println!("  Size: {:.2} MB", cache_1min.size_bytes() as f64 / (1024.0 * 1024.0));

    group.bench_function("1minute_resolution", |b| {
        b.iter_batched(
            || sorted_data_1min.clone(),
            |data| {
                let cache = ValueAwareLapperCache::from_sorted(data).unwrap();
                black_box(cache);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(cache_1min);
    drop(sorted_data_1min);

    // Test with 3-minute resolution
    let data_3min = truncate_to_3min(parsed_data.clone());
    let sorted_data_3min = SortedData::from_unsorted(data_3min);

    let cache_3min = ValueAwareLapperCache::from_sorted(sorted_data_3min.clone()).unwrap();
    println!("\n=== 3-Minute Resolution ===");
    println!("  Intervals: {}", cache_3min.interval_count());
    println!("  Size: {:.2} MB", cache_3min.size_bytes() as f64 / (1024.0 * 1024.0));

    group.bench_function("3minute_resolution", |b| {
        b.iter_batched(
            || sorted_data_3min.clone(),
            |data| {
                let cache = ValueAwareLapperCache::from_sorted(data).unwrap();
                black_box(cache);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(cache_3min);
    drop(sorted_data_3min);

    // Test with 5-minute resolution
    let data_5min = truncate_to_5min(parsed_data.clone());
    let sorted_data_5min = SortedData::from_unsorted(data_5min);

    let cache_5min = ValueAwareLapperCache::from_sorted(sorted_data_5min.clone()).unwrap();
    println!("\n=== 5-Minute Resolution ===");
    println!("  Intervals: {}", cache_5min.interval_count());
    println!("  Size: {:.2} MB", cache_5min.size_bytes() as f64 / (1024.0 * 1024.0));

    group.bench_function("5minute_resolution", |b| {
        b.iter_batched(
            || sorted_data_5min.clone(),
            |data| {
                let cache = ValueAwareLapperCache::from_sorted(data).unwrap();
                black_box(cache);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    drop(cache_5min);
    drop(sorted_data_5min);
    drop(parsed_data); // Drop the original data after all build benchmarks are done

    group.finish();
}

/// Benchmark querying the cache with ranges
fn bench_query_cache(c: &mut Criterion) {
    // Load data once
    let parsed_data = match load_parquet_data() {
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
    for resolution in ["nanosecond", "5second", "15second", "1minute", "3minute", "5minute"] {
        let (data, timestamps, bucket_size_ns) = match resolution {
            "nanosecond" => (parsed_data.clone(), parsed_data.iter().map(|(ts, _)| *ts).collect(), 1u64),
            "5second" => {
                let data = truncate_to_5sec(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 5_000_000_000u64)
            },
            "15second" => {
                let data = truncate_to_15sec(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 15_000_000_000u64)
            },
            "1minute" => {
                let data = truncate_to_1min(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 60_000_000_000u64)
            },
            "3minute" => {
                let data = truncate_to_3min(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 180_000_000_000u64)
            },
            "5minute" => {
                let data = truncate_to_5min(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 300_000_000_000u64)
            },
            _ => unreachable!(),
        };

        // Get min/max from the actual timestamps for this resolution
        let min_ts = *timestamps.iter().min().unwrap();
        let max_ts = *timestamps.iter().max().unwrap();

        // Build cache for this resolution
        let sorted_data = SortedData::from_unsorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        // Convert wall-clock duration to buckets for this resolution
        let range_size_buckets = wall_clock_range_ns / bucket_size_ns;
        let n_queries = 100;

        // Debug: print query statistics
        println!("\n=== Query Stats for {} ===", resolution);
        println!("  min_ts: {}, max_ts: {}", min_ts, max_ts);
        println!("  range: {}", max_ts - min_ts);
        println!("  wall_clock_range_ns: {} ns (~{} seconds)", wall_clock_range_ns, wall_clock_range_ns / 1_000_000_000);
        println!("  range_size (buckets): {}", range_size_buckets);
        println!("  num intervals: {}", cache.interval_count());

        // Create test ranges distributed across the time span (using resolution-specific timestamps)
        // Each range covers the same wall-clock duration
        let test_ranges: Vec<std::ops::Range<u64>> = (0..n_queries)
            .map(|i| {
                let start = min_ts + ((max_ts - min_ts - range_size_buckets) * i as u64) / (n_queries as u64);
                let end = start + range_size_buckets;
                start..end
            })
            .collect();

        // Sample query to check result count
        let sample_results = cache.query_range(test_ranges[0].clone());
        println!("  sample query results: {}", sample_results.len());

        group.throughput(Throughput::Elements(n_queries as u64));

        group.bench_with_input(
            BenchmarkId::new("range_10pct", resolution),
            &(&cache, &test_ranges),
            |b, (cache, ranges)| {
                b.iter(|| {
                    for range in ranges.iter() {
                        black_box(cache.query_range(range.clone()));
                    }
                });
            },
        );

        // Drop data structures after benchmarking this resolution
        drop(cache);
        drop(test_ranges);
        drop(timestamps);
    }

    drop(parsed_data);
    group.finish();
}

/// Benchmark appending to an existing cache
fn bench_append_cache(c: &mut Criterion) {
    // Load data once
    let parsed_data = match load_parquet_data() {
        Some(data) => data,
        None => return,
    };

    let mut group = c.benchmark_group("parquet_cache_append");

    // Split data for initial build and append
    let split_point = parsed_data.len() / 2;

    // Test append for different timestamp resolutions
    for resolution in ["nanosecond", "5second", "15second", "1minute", "3minute", "5minute"] {
        let data = match resolution {
            "nanosecond" => parsed_data.clone(),
            "5second" => truncate_to_5sec(parsed_data.clone()),
            "15second" => truncate_to_15sec(parsed_data.clone()),
            "1minute" => truncate_to_1min(parsed_data.clone()),
            "3minute" => truncate_to_3min(parsed_data.clone()),
            "5minute" => truncate_to_5min(parsed_data.clone()),
            _ => unreachable!(),
        };

        let initial_data = data[..split_point].to_vec();
        let append_data = data[split_point..].to_vec();

        let sorted_initial = SortedData::from_unsorted(initial_data);
        let sorted_append = SortedData::from_unsorted(append_data.clone());

        group.throughput(Throughput::Elements(append_data.len() as u64));

        group.bench_function(format!("append_50pct_{}", resolution), |b| {
            b.iter_batched(
                || (ValueAwareLapperCache::from_sorted(sorted_initial.clone()).unwrap(), sorted_append.clone()),
                |(mut cache, data)| {
                    cache.append_sorted(data).unwrap();
                    black_box(cache);
                },
                criterion::BatchSize::SmallInput,
            );
        });

        // Drop data structures after benchmarking this resolution
        drop(sorted_initial);
        drop(sorted_append);
    }

    drop(parsed_data);
    group.finish();
}

criterion_group!(
    benches,
    bench_build_cache,
    bench_query_cache,
    bench_append_cache,
);
criterion_main!(benches);
