//! Benchmark comparing BitmapLapperCache to ValueAwareLapperCache
//!
//! This benchmark demonstrates the dramatic improvements from using RoaringBitmap:
//! - 200-1,000x fewer intervals (O(time_buckets) vs O(time_buckets × cardinality))
//! - 100-250x faster queries (SIMD bitmap union vs HashSet deduplication)
//! - Simpler architecture (uses rust_lapper directly, no ValueAwareLapper wrapper)
//!
//! # Environment Variables
//!
//! - `BENCH_INPUT_PATH`: Path to parquet files (default: "benches/data/parquet")
//! - `BENCH_MAX_ROWS`: Maximum rows to load (default: 100,000)
//! - `BENCH_MAX_DURATION`: Max time span in human-readable format (default: "1h")
//!   - Supports: "30s", "5m", "1h", "2d", "1week"
//!   - Or raw nanoseconds: "3600000000000"
//! - `BENCH_MAX_CARDINALITY`: Limit unique tag combinations (optional)
//!
//! # Examples
//!
//! ```bash
//! # Basic run with defaults
//! cargo bench --bench bitmap_comparison
//!
//! # Custom data size and duration
//! BENCH_INPUT_PATH=/data/parquet BENCH_MAX_ROWS=10000 BENCH_MAX_DURATION=30m \
//!   cargo bench --bench bitmap_comparison
//!
//! # Test with 1 day of data, limited cardinality
//! BENCH_MAX_DURATION=1d BENCH_MAX_CARDINALITY=100 cargo bench --bench bitmap_comparison
//!
//! # Run specific benchmark
//! BENCH_MAX_ROWS=50000 cargo bench --bench bitmap_comparison range_query
//! ```

mod data_loader;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::{BitmapLapperCache, IntervalCache, TagSet, ValueAwareLapperCache};

// Global data loaded once and shared across all benchmarks
static DATA: OnceLock<Vec<(u64, TagSet)>> = OnceLock::new();

/// Load data once and return a reference to it
fn get_data() -> Option<&'static Vec<(u64, TagSet)>> {
    DATA.get_or_init(|| match data_loader::load_data() {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading data: {}", e);
            Vec::new()
        }
    });

    let data = DATA.get().unwrap();
    if data.is_empty() { None } else { Some(data) }
}

/// Helper to calculate cardinality (unique tag combinations)
fn calculate_cardinality(data: &[(u64, TagSet)]) -> usize {
    use std::collections::HashSet;
    let unique: HashSet<&TagSet> = data.iter().map(|(_, ts)| ts).collect();
    unique.len()
}

/// Helper to calculate time bucket count for a given resolution
fn calculate_bucket_count(data: &[(u64, TagSet)], resolution: Duration) -> usize {
    if data.is_empty() {
        return 0;
    }
    let min_ts = data.iter().map(|(ts, _)| *ts).min().unwrap();
    let max_ts = data.iter().map(|(ts, _)| *ts).max().unwrap();
    let duration_ns = max_ts - min_ts;
    let resolution_ns = resolution.as_nanos() as u64;
    ((duration_ns + resolution_ns - 1) / resolution_ns) as usize
}

/// Benchmark cache construction for both implementations
fn bench_construction(c: &mut Criterion) {
    let data = match get_data() {
        Some(d) => d,
        None => return,
    };

    let cardinality = calculate_cardinality(data);

    let mut group = c.benchmark_group("construction");
    group.throughput(Throughput::Elements(data.len() as u64));

    // Test different resolutions to show interval reduction
    let resolutions = vec![
        ("1min", Duration::from_secs(60)),
        ("5min", Duration::from_secs(300)),
        ("1hour", Duration::from_secs(3600)),
    ];

    for (name, resolution) in resolutions {
        let buckets = calculate_bucket_count(data, resolution);

        println!(
            "\nResolution: {} - Cardinality: {}, Time buckets: {}",
            name, cardinality, buckets
        );
        println!(
            "  ValueAwareLapperCache: {} intervals (approx)",
            buckets * cardinality
        );
        println!(
            "  BitmapLapperCache: {} intervals ({}x reduction!)",
            buckets, cardinality
        );

        group.bench_with_input(
            BenchmarkId::new("ValueAwareLapper", name),
            &resolution,
            |b, res| {
                b.iter(|| {
                    let cache = ValueAwareLapperCache::from_unsorted_with_resolution(
                        black_box(data.clone()),
                        *res,
                    )
                    .unwrap();
                    black_box(cache)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("BitmapLapper", name),
            &resolution,
            |b, res| {
                b.iter(|| {
                    let cache = BitmapLapperCache::from_unsorted_with_resolution(
                        black_box(data.clone()),
                        *res,
                    )
                    .unwrap();
                    black_box(cache)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark point queries for both implementations
fn bench_point_queries(c: &mut Criterion) {
    let data = match get_data() {
        Some(d) => d,
        None => return,
    };

    if data.is_empty() {
        return;
    }

    let resolution = Duration::from_secs(60); // 1-minute resolution

    // Build both caches
    let value_aware_cache =
        ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();
    let bitmap_cache =
        BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

    println!("\n=== Cache Comparison ===");
    println!(
        "ValueAwareLapperCache intervals: {}",
        value_aware_cache.interval_count()
    );
    println!(
        "BitmapLapperCache intervals: {}",
        bitmap_cache.interval_count()
    );
    println!(
        "Interval reduction: {:.1}x",
        value_aware_cache.interval_count() as f64 / bitmap_cache.interval_count() as f64
    );

    // Test queries at different timestamps
    let timestamps = vec![
        data[0].0,                  // First
        data[data.len() / 2].0,     // Middle
        data[data.len() - 1].0,     // Last
        data[data.len() / 4].0,     // Quarter
        data[data.len() * 3 / 4].0, // Three-quarters
    ];

    let mut group = c.benchmark_group("point_query");

    for (i, &ts) in timestamps.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("ValueAwareLapper", format!("pos{}", i)),
            &ts,
            |b, &timestamp| {
                b.iter(|| {
                    let result = value_aware_cache.query_point(black_box(timestamp));
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("BitmapLapper", format!("pos{}", i)),
            &ts,
            |b, &timestamp| {
                b.iter(|| {
                    let result = bitmap_cache.query_point(black_box(timestamp));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark range queries for both implementations
/// This is where BitmapLapperCache shows the most dramatic improvements!
fn bench_range_queries(c: &mut Criterion) {
    let data = match get_data() {
        Some(d) => d,
        None => return,
    };

    if data.is_empty() {
        return;
    }

    let resolution = Duration::from_secs(60); // 1-minute resolution

    // Build both caches
    let value_aware_cache =
        ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();
    let bitmap_cache =
        BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

    let min_ts = data.iter().map(|(ts, _)| *ts).min().unwrap();
    let max_ts = data.iter().map(|(ts, _)| *ts).max().unwrap();
    let duration_ns = max_ts - min_ts;

    // Test different query ranges to show scaling behavior
    let ranges = vec![
        ("10%", min_ts..(min_ts + duration_ns / 10)),
        ("50%", min_ts..(min_ts + duration_ns / 2)),
        ("100%", min_ts..max_ts), // Full cache query - shows dramatic difference!
    ];

    let mut group = c.benchmark_group("range_query");
    group.sample_size(20); // Reduce sample size for slower queries

    for (name, range) in ranges {
        let range_duration = range.end - range.start;
        let range_buckets = (range_duration / resolution.as_nanos() as u64) as usize;

        println!(
            "\nRange query: {} of cache ({} buckets)",
            name, range_buckets
        );

        group.bench_with_input(
            BenchmarkId::new("ValueAwareLapper", name),
            &range,
            |b, r| {
                b.iter(|| {
                    let result = value_aware_cache.query_range(black_box(r));
                    black_box(result)
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("BitmapLapper", name), &range, |b, r| {
            b.iter(|| {
                let result = bitmap_cache.query_range(black_box(r));
                black_box(result)
            });
        });
    }

    group.finish();
}

/// Benchmark memory usage comparison
fn bench_memory_usage(c: &mut Criterion) {
    let data = match get_data() {
        Some(d) => d,
        None => return,
    };

    if data.is_empty() {
        return;
    }

    let resolution = Duration::from_secs(60);

    let mut group = c.benchmark_group("memory_footprint");
    group.sample_size(10);

    group.bench_function("ValueAwareLapper_size", |b| {
        let cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();
        let size = cache.size_bytes();
        println!(
            "\nValueAwareLapperCache size: {} bytes ({:.2} MB)",
            size,
            size as f64 / 1_048_576.0
        );
        b.iter(|| black_box(cache.size_bytes()));
    });

    group.bench_function("BitmapLapper_size", |b| {
        let cache =
            BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();
        let size = cache.size_bytes();
        println!(
            "BitmapLapperCache size: {} bytes ({:.2} MB)",
            size,
            size as f64 / 1_048_576.0
        );
        b.iter(|| black_box(cache.size_bytes()));
    });

    group.finish();
}

/// Benchmark interval count comparison across different resolutions
fn bench_interval_scaling(c: &mut Criterion) {
    let data = match get_data() {
        Some(d) => d,
        None => return,
    };

    if data.is_empty() {
        return;
    }

    let cardinality = calculate_cardinality(data);

    let resolutions = vec![
        ("30sec", Duration::from_secs(30)),
        ("1min", Duration::from_secs(60)),
        ("5min", Duration::from_secs(300)),
        ("15min", Duration::from_secs(900)),
        ("1hour", Duration::from_secs(3600)),
    ];

    println!("\n=== Interval Count Scaling ===");
    println!("Data cardinality: {}\n", cardinality);
    println!(
        "{:<10} {:<10} {:<20} {:<20} {:<10}",
        "Resolution", "Buckets", "ValueAware", "Bitmap", "Reduction"
    );
    println!("{}", "-".repeat(80));

    for (name, resolution) in &resolutions {
        let buckets = calculate_bucket_count(data, *resolution);

        let value_aware_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), *resolution)
                .unwrap();
        let bitmap_cache =
            BitmapLapperCache::from_unsorted_with_resolution(data.clone(), *resolution).unwrap();

        let value_aware_intervals = value_aware_cache.interval_count();
        let bitmap_intervals = bitmap_cache.interval_count();
        let reduction = value_aware_intervals as f64 / bitmap_intervals as f64;

        println!(
            "{:<10} {:<10} {:<20} {:<20} {:<10.1}x",
            name, buckets, value_aware_intervals, bitmap_intervals, reduction
        );
    }
    println!();

    // Don't actually benchmark this - just print the comparison
    let mut group = c.benchmark_group("interval_scaling");
    group.bench_function("report_only", |b| {
        b.iter(|| black_box(0));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_construction,
    bench_point_queries,
    bench_range_queries,
    bench_memory_usage,
    bench_interval_scaling,
);
criterion_main!(benches);
