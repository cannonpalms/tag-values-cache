mod data_loader;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::{IntervalCache, TagSet, ValueAwareLapperCache};

// Global configuration and data loaded once and shared across all benchmarks
static CONFIG: OnceLock<data_loader::BenchConfig> = OnceLock::new();
static PARQUET_DATA: OnceLock<Vec<(u64, TagSet)>> = OnceLock::new();

/// Get or initialize the benchmark configuration
fn get_config() -> &'static data_loader::BenchConfig {
    CONFIG.get_or_init(data_loader::BenchConfig::from_env)
}

// Global caches for each resolution, initialized lazily
static CACHE_NANOSECOND: OnceLock<ValueAwareLapperCache> = OnceLock::new();
static CACHE_5SECOND: OnceLock<ValueAwareLapperCache> = OnceLock::new();
static CACHE_15SECOND: OnceLock<ValueAwareLapperCache> = OnceLock::new();
static CACHE_1MINUTE: OnceLock<ValueAwareLapperCache> = OnceLock::new();
static CACHE_3MINUTE: OnceLock<ValueAwareLapperCache> = OnceLock::new();
static CACHE_5MINUTE: OnceLock<ValueAwareLapperCache> = OnceLock::new();
static CACHE_1HOUR: OnceLock<ValueAwareLapperCache> = OnceLock::new();

/// Get or initialize a cache for a specific resolution
fn get_cache_for_resolution(
    resolution_name: &str,
    resolution: Duration,
) -> Option<&'static ValueAwareLapperCache> {
    let parsed_data = get_parquet_data()?;

    let cache_lock = match resolution_name {
        "nanosecond" => &CACHE_NANOSECOND,
        "5second" => &CACHE_5SECOND,
        "15second" => &CACHE_15SECOND,
        "1minute" => &CACHE_1MINUTE,
        "3minute" => &CACHE_3MINUTE,
        "5minute" => &CACHE_5MINUTE,
        "1hour" => &CACHE_1HOUR,
        _ => return None,
    };

    Some(cache_lock.get_or_init(|| {
        println!("Initializing {} resolution cache...", resolution_name);
        let cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(parsed_data.clone(), resolution)
                .unwrap();

        println!("  Intervals: {}", cache.interval_count());
        println!(
            "  Size: {:.2} MB",
            cache.size_bytes() as f64 / (1024.0 * 1024.0)
        );

        cache
    }))
}

/// Load the data once and return a reference to it
fn get_parquet_data() -> Option<&'static Vec<(u64, TagSet)>> {
    PARQUET_DATA.get_or_init(|| {
        let config = get_config();
        match data_loader::load_data(config) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error loading data: {}", e);
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
        let mut stats_printed = false;
        group.bench_function(format!("{}_resolution", name), |b| {
            if !stats_printed {
                // Build cache once to get statistics on first iteration
                let cache = ValueAwareLapperCache::from_unsorted_with_resolution(
                    parsed_data.clone(),
                    *resolution,
                )
                .unwrap();

                println!("\n=== {} Resolution ===", name);
                println!("  Intervals: {}", cache.interval_count());
                println!(
                    "  Size: {:.2} MB",
                    cache.size_bytes() as f64 / (1024.0 * 1024.0)
                );

                stats_printed = true;
            }
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
            // Get or initialize cache for this resolution (lazy initialization)
            let cache = match get_cache_for_resolution(name, *resolution) {
                Some(c) => c,
                None => return,
            };

            b.iter(|| {
                for range in test_ranges_10pct.iter() {
                    black_box(cache.query_range(range));
                }
            });
        });

        group.bench_function(BenchmarkId::new("range_100pct", name), |b| {
            // Get or initialize cache for this resolution (lazy initialization)
            let cache = match get_cache_for_resolution(name, *resolution) {
                Some(c) => c,
                None => return,
            };

            b.iter(|| {
                black_box(cache.query_range(&test_range_100pct));
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
    bench_append_cache,
);
criterion_main!(benches);
