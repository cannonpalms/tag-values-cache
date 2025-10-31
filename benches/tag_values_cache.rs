//! Benchmark for tag values cache using BitmapLapperCache with sorted stream builder
//!
//! This benchmark focuses on the BitmapLapperCache using the sorted streaming API
//! (BitmapSortedStreamBuilder). It includes cache construction benchmarks and 100%
//! range query benchmarks.
//!
//! # Environment Variables
//!
//! - `BENCH_INPUT_PATH`: Path to parquet file or directory (default: "benches/data/by-cardinality/1K.parquet")
//! - `BENCH_MAX_ROWS`: Maximum rows to load (default: no limit)
//! - `BENCH_MAX_DURATION`: Max time span (default: "1h")
//! - `BENCH_MAX_CARDINALITY`: Limit unique tag combinations (optional)
//!
//! # Examples
//!
//! ```bash
//! # Run with 1K cardinality (default)
//! cargo bench --bench tag_values_cache
//!
//! # Run with 10K cardinality
//! BENCH_INPUT_PATH=benches/data/by-cardinality/10K.parquet cargo bench --bench tag_values_cache
//!
//! # Run with 100K cardinality
//! BENCH_INPUT_PATH=benches/data/by-cardinality/100K.parquet cargo bench --bench tag_values_cache
//!
//! # Run with 1M cardinality
//! BENCH_INPUT_PATH=benches/data/by-cardinality/1M.parquet cargo bench --bench tag_values_cache
//! ```

mod data_loader;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::{StreamExt, stream};
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::{
    IntervalCache, format_bytes,
    streaming::{BitmapSortedStreamBuilder, SendableRecordBatchStream},
};

// Global data loaded once and shared across all benchmarks
static BATCHES: OnceLock<Vec<arrow::array::RecordBatch>> = OnceLock::new();

/// Load RecordBatches once and return a reference to them
fn get_batches() -> Option<&'static Vec<arrow::array::RecordBatch>> {
    BATCHES.get_or_init(|| {
        // Set default input path if not specified
        // SAFETY: This is safe because we're only setting env vars once at initialization
        // before any benchmarks run, and benchmarks are single-threaded by default.
        unsafe {
            if std::env::var("BENCH_INPUT_PATH").is_err() {
                std::env::set_var("BENCH_INPUT_PATH", "benches/data/by-cardinality/1K.parquet");
            }
            // Remove row limit - load entire file
            if std::env::var("BENCH_MAX_ROWS").is_err() {
                std::env::set_var("BENCH_MAX_ROWS", usize::MAX.to_string());
            }
        }

        match data_loader::load_record_batches() {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error loading record batches: {}", e);
                Vec::new()
            }
        }
    });

    let batches = BATCHES.get().unwrap();
    if batches.is_empty() {
        None
    } else {
        Some(batches)
    }
}

/// Helper to count total rows in batches
fn count_rows(batches: &[arrow::array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Helper to create a stream from in-memory batches
fn create_stream_from_batches(
    batches: &'static [arrow::array::RecordBatch],
) -> SendableRecordBatchStream {
    stream::iter(batches.iter().map(|b| Ok(b.clone()))).boxed()
}

/// Benchmark cache build using sorted stream builder
fn bench_cache_build(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let total_rows = count_rows(batches);

    let mut group = c.benchmark_group("cache_build");
    group.throughput(Throughput::Elements(total_rows as u64));

    // Test different resolutions
    let config = ("1hour", Duration::from_secs(3600));

    println!("\n=== Cache Construction ===");
    println!("Total rows: {}", total_rows);

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let (name, resolution) = config;
    println!("\nConfig: {} (resolution={:?})", name, resolution);

    group.bench_with_input(
        BenchmarkId::new("BitmapLapper_Sorted", name),
        &resolution,
        |b, res| {
            b.to_async(&runtime).iter(|| async {
                let stream = create_stream_from_batches(batches);
                let cache = BitmapSortedStreamBuilder::from_stream(stream, *res)
                    .await
                    .unwrap();
                black_box(cache)
            });
        },
    );

    group.finish();
}

/// Benchmark 100% range queries on caches
fn bench_range_queries(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let resolution = Duration::from_secs(60);

    // Build cache using sorted streaming builder
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bitmap_cache = runtime.block_on(async {
        let bitmap_stream = create_stream_from_batches(batches);
        BitmapSortedStreamBuilder::from_stream(bitmap_stream, resolution)
            .await
            .unwrap()
    });

    println!("\n=== Sorted-Built Cache Info ===");
    println!(
        "BitmapLapperCache intervals: {}",
        bitmap_cache.interval_count()
    );
    println!(
        "BitmapLapperCache size: {}",
        format_bytes(bitmap_cache.size_bytes())
    );

    // Extract timestamps for queries
    let mut all_timestamps = Vec::new();
    for batch in batches {
        let points = tag_values_cache::extract_tags_from_batch(batch);
        for (ts, _) in points {
            all_timestamps.push(ts);
        }
    }
    all_timestamps.sort();

    if all_timestamps.is_empty() {
        return;
    }

    let min_ts = all_timestamps[0];
    let max_ts = all_timestamps[all_timestamps.len() - 1];

    // Test 100% range query only
    let range = min_ts..max_ts;

    let mut group = c.benchmark_group("range_query");
    group.sample_size(100);

    println!("\n=== Range Query: 100% of time range ===");

    group.bench_with_input(BenchmarkId::new("BitmapLapper", "100%"), &range, |b, r| {
        b.iter(|| bitmap_cache.query_range(r));
    });

    group.finish();
}

criterion_group!(benches, bench_cache_build, bench_range_queries);
criterion_main!(benches);
