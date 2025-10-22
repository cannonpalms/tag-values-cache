use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use chrono::{Utc, TimeZone};
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::Path;
use tag_values_cache::{
    ArrowValue, RecordBatchRow, SortedData, ValueAwareLapperCache, IntervalCache,
};

/// Parse a single line of line protocol format.
/// Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
///
/// For this benchmark, we'll focus on extracting timestamp and tags (not fields).
fn parse_line_protocol(line: &str) -> Option<(u64, RecordBatchRow)> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        return None;
    }

    // Parse timestamp (last part)
    let timestamp: u64 = parts[2].parse().ok()?;

    // Parse measurement and tags (first part)
    let measurement_tags = parts[0];
    let mut tag_parts = measurement_tags.split(',');
    let measurement = tag_parts.next()?;

    let mut values = BTreeMap::new();

    // Add measurement as a special tag
    values.insert(
        "_measurement".to_string(),
        ArrowValue::String(measurement.to_string()),
    );

    // Parse tags - only keep specific tags we're interested in
    let allowed_tags = [
        "kubernetes_namespace",
        "host",
        "job",
        "applicationId"
    ];

    for tag_part in tag_parts {
        if let Some((key, value)) = tag_part.split_once('=') {
            // Only keep allowed tags
            if allowed_tags.contains(&key) {
                values.insert(
                    key.to_string(),
                    ArrowValue::String(value.to_string()),
                );
            }
        }
    }

    // Parse fields (middle part) - we'll include these as well for completeness
    let fields = parts[1];
    for field_part in fields.split(',') {
        if let Some((key, value_str)) = field_part.split_once('=') {
            // Try to parse as different types
            let arrow_value = if value_str == "true" || value_str == "false" {
                ArrowValue::Boolean(value_str == "true")
            } else if let Ok(i) = value_str.trim_end_matches('i').parse::<i64>() {
                ArrowValue::Int64(i)
            } else if let Ok(f) = value_str.parse::<f64>() {
                ArrowValue::Float64(f)
            } else if value_str.starts_with('"') && value_str.ends_with('"') {
                // String value
                ArrowValue::String(value_str[1..value_str.len()-1].to_string())
            } else {
                ArrowValue::String(value_str.to_string())
            };

            values.insert(format!("_field_{}", key), arrow_value);
        }
    }

    Some((timestamp, RecordBatchRow::new(values)))
}

/// Parse line protocol data from a file using buffered reading
fn parse_line_protocol_file(path: &Path) -> std::io::Result<Vec<(u64, RecordBatchRow)>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut results = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() && !line.starts_with('#') {
            if let Some(parsed) = parse_line_protocol(&line) {
                results.push(parsed);
            }
        }
    }

    Ok(results)
}

/// Convert nanoseconds since epoch to ISO 8601 format
fn nanos_to_iso(nanos: u64) -> String {
    // Convert nanoseconds to seconds and nanosecond remainder
    let secs = (nanos / 1_000_000_000) as i64;
    let nanos_remainder = (nanos % 1_000_000_000) as u32;

    // Create DateTime from timestamp
    match Utc.timestamp_opt(secs, nanos_remainder) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339(),
        _ => format!("{} ns", nanos), // Fallback for invalid timestamps
    }
}

/// Truncate timestamps to 5-second resolution
/// Returns timestamps in 5-second buckets (not converted back to nanoseconds)
fn truncate_to_5sec(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 5;  // Group into 5-second buckets
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 15-second resolution
/// Returns timestamps in 15-second buckets (not converted back to nanoseconds)
fn truncate_to_15sec(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 15;  // Group into 15-second buckets
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 1-minute resolution
/// Returns timestamps in 1-minute buckets (not converted back to nanoseconds)
fn truncate_to_1min(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 60;  // Group into 60-second buckets
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 3-minute resolution
/// Returns timestamps in 3-minute buckets (not converted back to nanoseconds)
fn truncate_to_3min(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 180;  // Group into 180-second buckets
            (bucket, row)
        })
        .collect()
}

/// Truncate timestamps to 5-minute resolution
/// Returns timestamps in 5-minute buckets (not converted back to nanoseconds)
fn truncate_to_5min(data: Vec<(u64, RecordBatchRow)>) -> Vec<(u64, RecordBatchRow)> {
    data.into_iter()
        .map(|(ts, row)| {
            let secs = ts / 1_000_000_000;
            let bucket = secs / 300;  // Group into 300-second buckets
            (bucket, row)
        })
        .collect()
}

/// Analyze tag statistics from parsed data
fn analyze_tag_statistics(data: &[(u64, RecordBatchRow)]) -> (usize, usize, BTreeMap<String, usize>) {
    let mut all_tag_keys = HashSet::new();
    let mut unique_tag_combinations = HashSet::new();
    let mut tag_value_counts: BTreeMap<String, HashSet<String>> = BTreeMap::new();

    for (_, row) in data {
        let mut tag_combination = Vec::new();

        for (key, value) in &row.values {
            // Skip fields (which we prefixed with "_field_") and only count tags
            if !key.starts_with("_field_") {
                all_tag_keys.insert(key.clone());

                // Track unique values per tag key
                tag_value_counts.entry(key.clone())
                    .or_insert_with(HashSet::new)
                    .insert(value.to_string());

                // Build tag combination for cardinality calculation
                tag_combination.push(format!("{}={}", key, value));
            }
        }

        // Sort to ensure consistent ordering for the same combination
        tag_combination.sort();
        unique_tag_combinations.insert(tag_combination.join(","));
    }

    // Convert tag value counts to cardinality per tag
    let tag_cardinalities: BTreeMap<String, usize> = tag_value_counts
        .into_iter()
        .map(|(k, v)| (k, v.len()))
        .collect();

    (all_tag_keys.len(), unique_tag_combinations.len(), tag_cardinalities)
}

/// Load the real-world data once and return it
fn load_real_world_data() -> Option<Vec<(u64, RecordBatchRow)>> {
    let att_file = std::path::PathBuf::from("benches/data/att.lp");

    // Check if the real-world data file exists
    if !att_file.exists() {
        eprintln!("Error: Real-world data file not found at {:?}", att_file);
        eprintln!("Please ensure benches/data/att.lp exists before running benchmarks");
        return None;
    }

    match parse_line_protocol_file(&att_file) {
        Ok(data) => {
            println!("\n=== Data Statistics ===");
            println!("Lines loaded: {}", data.len());

            // Display time range if data is not empty
            if !data.is_empty() {
                let timestamps: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                let min_ts = *timestamps.iter().min().unwrap();
                let max_ts = *timestamps.iter().max().unwrap();

                println!("Time range: {} to {}", nanos_to_iso(min_ts), nanos_to_iso(max_ts));

                let duration_ns = max_ts - min_ts;
                let duration_secs = duration_ns / 1_000_000_000;
                let hours = duration_secs / 3600;
                let minutes = (duration_secs % 3600) / 60;
                let seconds = duration_secs % 60;

                println!("Duration: {}h {}m {}s", hours, minutes, seconds);

                // Analyze and display tag statistics
                let (num_tags, total_cardinality, _tag_cardinalities) = analyze_tag_statistics(&data);

                println!("\n=== Tag Statistics ===");
                println!("Number of unique tag keys: {}", num_tags);
                println!("Total tagset cardinality: {}", total_cardinality);
                println!("======================\n");
            }

            Some(data)
        }
        Err(e) => {
            eprintln!("Error parsing att.lp: {}", e);
            None
        }
    }
}

/// Benchmark building ValueAwareLapperCache from real-world data
fn bench_build_cache(c: &mut Criterion) {
    // Load data once
    let parsed_data = match load_real_world_data() {
        Some(data) => data,
        None => return,
    };

    let mut group = c.benchmark_group("cache_build");
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

    // Test with 5-second resolution
    let data_5sec = truncate_to_5sec(parsed_data.clone());
    let sorted_data_5sec = SortedData::from_unsorted(data_5sec);

    // Build cache once to get statistics
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

    // Test with 15-second resolution
    let data_15sec = truncate_to_15sec(parsed_data.clone());
    let sorted_data_15sec = SortedData::from_unsorted(data_15sec);

    // Build cache once to get statistics
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

    // Test with 1-minute resolution
    let data_1min = truncate_to_1min(parsed_data.clone());
    let sorted_data_1min = SortedData::from_unsorted(data_1min);

    // Build cache once to get statistics
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

    // Test with 3-minute resolution
    let data_3min = truncate_to_3min(parsed_data.clone());
    let sorted_data_3min = SortedData::from_unsorted(data_3min);

    // Build cache once to get statistics
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

    // Test with 5-minute resolution
    let data_5min = truncate_to_5min(parsed_data.clone());
    let sorted_data_5min = SortedData::from_unsorted(data_5min);

    // Build cache once to get statistics
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

    group.finish();
}

/// Benchmark querying the cache with ranges
fn bench_query_cache(c: &mut Criterion) {
    // Load data once
    let parsed_data = match load_real_world_data() {
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

    let mut group = c.benchmark_group("cache_query");

    // Test queries with different timestamp resolutions
    for resolution in ["nanosecond", "5second", "15second", "1minute", "3minute", "5minute"] {
        let (data, timestamps, bucket_size_ns) = match resolution {
            "nanosecond" => (parsed_data.clone(), parsed_data.iter().map(|(ts, _)| *ts).collect(), 1u64),
            "5second" => {
                let data = truncate_to_5sec(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 5_000_000_000u64) // 5 seconds in nanoseconds
            },
            "15second" => {
                let data = truncate_to_15sec(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 15_000_000_000u64) // 15 seconds in nanoseconds
            },
            "1minute" => {
                let data = truncate_to_1min(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 60_000_000_000u64) // 60 seconds in nanoseconds
            },
            "3minute" => {
                let data = truncate_to_3min(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 180_000_000_000u64) // 180 seconds in nanoseconds
            },
            "5minute" => {
                let data = truncate_to_5min(parsed_data.clone());
                let ts: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
                (data, ts, 300_000_000_000u64) // 300 seconds in nanoseconds
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
    }

    group.finish();
}

/// Benchmark appending to an existing cache
fn bench_append_cache(c: &mut Criterion) {
    // Load data once
    let parsed_data = match load_real_world_data() {
        Some(data) => data,
        None => return,
    };

    let mut group = c.benchmark_group("cache_append");

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
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_build_cache,
    bench_query_cache,
    bench_append_cache,
);
criterion_main!(benches);