use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use chrono::{Utc, TimeZone};
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::Path;
use std::time::Duration;
use tag_values_cache::{
    RecordBatchRow, SortedData, ValueAwareLapperCache, IntervalCache,
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
        measurement.to_string(),
    );

    // Parse tags - only keep specific tags we're interested in
    let allowed_tags = [
        "kubernetes_namespace",
        "host",
        "job",
        "applicationId",
        "pod",
        "instance"
    ];

    for tag_part in tag_parts {
        if let Some((key, value)) = tag_part.split_once('=') {
            // Only keep allowed tags
            if allowed_tags.contains(&key) {
                values.insert(
                    key.to_string(),
                    value.to_string(),
                );
            }
        }
    }

    // Parse fields (middle part) - convert all to strings for tags
    let fields = parts[1];
    for field_part in fields.split(',') {
        if let Some((key, value_str)) = field_part.split_once('=') {
            // Convert field values to strings
            let string_value = if value_str.starts_with('"') && value_str.ends_with('"') {
                // String value - remove quotes
                value_str[1..value_str.len()-1].to_string()
            } else if value_str.ends_with('i') {
                // Integer value - remove 'i' suffix
                value_str.trim_end_matches('i').to_string()
            } else {
                // Boolean, float, or other - use as is
                value_str.to_string()
            };

            values.insert(format!("_field_{}", key), string_value);
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
        if !line.trim().is_empty() && !line.starts_with('#')
            && let Some(parsed) = parse_line_protocol(&line) {
                results.push(parsed);
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
                    .or_default()
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

    // Test different resolutions
    let resolutions = [
        ("nanosecond", Duration::from_nanos(1)),
        ("5second", Duration::from_secs(5)),
        ("15second", Duration::from_secs(15)),
        ("1minute", Duration::from_secs(60)),
        ("3minute", Duration::from_secs(180)),
        ("5minute", Duration::from_secs(300)),
    ];

    // ValueAwareLapperCache now works with RecordBatchRow
    let sorted_data = SortedData::from_unsorted(parsed_data.clone());

    for (name, resolution) in &resolutions {
        // Build cache once to get statistics
        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            sorted_data.clone(),
            *resolution,
        )
        .unwrap();

        println!("\n=== {} Resolution ===", name);
        println!("  Intervals: {}", cache.interval_count());
        println!("  Size: {:.2} MB", cache.size_bytes() as f64 / (1024.0 * 1024.0));

        group.bench_function(format!("{}_resolution", name), |b| {
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

        drop(cache);
    }

    drop(sorted_data);
    drop(parsed_data);

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
    let resolutions = [
        ("nanosecond", Duration::from_nanos(1)),
        ("5second", Duration::from_secs(5)),
        ("15second", Duration::from_secs(15)),
        ("1minute", Duration::from_secs(60)),
        ("3minute", Duration::from_secs(180)),
        ("5minute", Duration::from_secs(300)),
    ];

    // ValueAwareLapperCache now works with RecordBatchRow
    let sorted_data = SortedData::from_unsorted(parsed_data.clone());

    for (name, resolution) in &resolutions {
        // Build cache for this resolution
        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            sorted_data.clone(),
            *resolution,
        )
        .unwrap();

        let n_queries = 100;

        // Debug: print query statistics
        println!("\n=== Query Stats for {} ===", name);
        println!("  min_ts: {}, max_ts: {}", original_min_ns, original_max_ns);
        println!("  range: {}", original_range_ns);
        println!("  wall_clock_range_ns: {} ns (~{} seconds)", wall_clock_range_ns, wall_clock_range_ns / 1_000_000_000);
        println!("  num intervals: {}", cache.interval_count());

        // Create test ranges distributed across the time span (in nanoseconds)
        // Each range covers the same wall-clock duration
        let test_ranges: Vec<std::ops::Range<u64>> = (0..n_queries)
            .map(|i| {
                let start = original_min_ns + ((original_range_ns - wall_clock_range_ns) * i as u64) / (n_queries as u64);
                let end = start + wall_clock_range_ns;
                start..end
            })
            .collect();

        // Sample query to check result count
        let sample_results = cache.query_range(test_ranges[0].clone());
        println!("  sample query results: {}", sample_results.len());

        group.throughput(Throughput::Elements(n_queries as u64));

        group.bench_with_input(
            BenchmarkId::new("range_10pct", name),
            &(&cache, &test_ranges),
            |b, (cache, ranges)| {
                b.iter(|| {
                    for range in ranges.iter() {
                        black_box(cache.query_range(range.clone()));
                    }
                });
            },
        );

        drop(cache);
        drop(test_ranges);
    }

    drop(sorted_data);
    drop(parsed_data);
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

        // ValueAwareLapperCache now works with RecordBatchRow
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