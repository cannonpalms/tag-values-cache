//! Benchmark comparing BitmapLapperCache streaming to ValueAwareLapperCache streaming
//!
//! This benchmark demonstrates the performance differences between streaming builders
//! for both cache implementations. Unlike `bitmap_comparison` which uses batch building,
//! this uses the streaming APIs (ChunkedStreamBuilder vs BitmapChunkedStreamBuilder).
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
//! cargo bench --bench bitmap_comparison_streaming
//!
//! # Run with 10K cardinality
//! BENCH_INPUT_PATH=benches/data/by-cardinality/10K.parquet cargo bench --bench bitmap_comparison_streaming
//!
//! # Run with 100K cardinality
//! BENCH_INPUT_PATH=benches/data/by-cardinality/100K.parquet cargo bench --bench bitmap_comparison_streaming
//!
//! # Run with 1M cardinality
//! BENCH_INPUT_PATH=benches/data/by-cardinality/1M.parquet cargo bench --bench bitmap_comparison_streaming
//! ```

mod data_loader;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use data_loader::BenchConfig;
use futures::{StreamExt, stream};
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::{
    BitmapLapperCache, IntervalCache, ValueAwareLapperCache,
    streaming::{
        BitmapChunkedStreamBuilder, BitmapStreamBuilder, ChunkedStreamBuilder,
        SendableRecordBatchStream, SortedStreamBuilder,
    },
};

// Global configuration and data loaded once and shared across all benchmarks
static CONFIG: OnceLock<BenchConfig> = OnceLock::new();
static BATCHES: OnceLock<Vec<arrow::array::RecordBatch>> = OnceLock::new();

/// Get or initialize the benchmark configuration
fn get_config() -> &'static BenchConfig {
    CONFIG.get_or_init(|| {
        let mut config = BenchConfig::from_env();

        // Override default input path if not explicitly set
        if std::env::var("BENCH_INPUT_PATH").is_err() {
            config.input_path = "benches/data/by-cardinality/1K.parquet".into();
            config.input_type = data_loader::InputType::Parquet;
        }

        // Remove row limit - load entire file
        if std::env::var("BENCH_MAX_ROWS").is_err() {
            config.max_rows = usize::MAX;
        }

        config
    })
}

/// Load RecordBatches once and return a reference to them
fn get_batches() -> Option<&'static Vec<arrow::array::RecordBatch>> {
    BATCHES.get_or_init(|| {
        let config = get_config();
        match data_loader::load_record_batches(config) {
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

/// Helper to calculate cardinality from batches
fn calculate_cardinality(batches: &[arrow::array::RecordBatch]) -> usize {
    use std::collections::HashSet;
    use tag_values_cache::TagSet;
    let mut unique: HashSet<TagSet> = HashSet::new();

    for batch in batches {
        let points = tag_values_cache::extract_tags_from_batch(batch);
        for (_, tagset) in points {
            unique.insert(tagset);
        }
    }

    unique.len()
}

/// Helper to create a stream from in-memory batches
fn create_stream_from_batches(
    batches: &'static [arrow::array::RecordBatch],
) -> SendableRecordBatchStream {
    stream::iter(batches.iter().map(|b| Ok(b.clone()))).boxed()
}

/// Benchmark streaming construction for both implementations
fn bench_streaming_construction(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let total_rows = count_rows(batches);
    let cardinality = calculate_cardinality(batches);

    let mut group = c.benchmark_group("streaming_construction");
    group.throughput(Throughput::Elements(total_rows as u64));

    // Test different resolutions and chunk sizes
    let configs = vec![
        ("5min_1M", Duration::from_secs(300), 1_000_000),
        ("1hour_1M", Duration::from_secs(3600), 1_000_000),
    ];

    println!("\n=== Streaming Construction ===");
    println!("Total rows: {}, Cardinality: {}", total_rows, cardinality);

    let runtime = tokio::runtime::Runtime::new().unwrap();

    for (name, resolution, chunk_size) in configs {
        println!(
            "\nConfig: {} (resolution={:?}, chunk_size={})",
            name, resolution, chunk_size
        );

        group.bench_with_input(
            BenchmarkId::new("ValueAwareLapper", name),
            &(resolution, chunk_size),
            |b, (res, cs)| {
                b.to_async(&runtime).iter(|| async {
                    let stream = create_stream_from_batches(batches);
                    let (cache, _) = ChunkedStreamBuilder::from_stream(stream, *res, *cs)
                        .await
                        .unwrap();
                    black_box(cache)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("BitmapLapper", name),
            &(resolution, chunk_size),
            |b, (res, cs)| {
                b.to_async(&runtime).iter(|| async {
                    let stream = create_stream_from_batches(batches);
                    let (cache, _) = BitmapChunkedStreamBuilder::from_stream(stream, *res, *cs)
                        .await
                        .unwrap();
                    black_box(cache)
                });
            },
        );
    }

    // Test sorted builders (no chunk size needed)
    let sorted_configs = vec![
        ("5min_sorted", Duration::from_secs(300)),
        ("1hour_sorted", Duration::from_secs(3600)),
    ];

    for (name, resolution) in sorted_configs {
        println!("\nSorted Config: {} (resolution={:?})", name, resolution);

        group.bench_with_input(
            BenchmarkId::new("ValueAwareLapper_Sorted", name),
            &resolution,
            |b, res| {
                b.to_async(&runtime).iter(|| async {
                    let stream = create_stream_from_batches(batches);
                    let (cache, _) = SortedStreamBuilder::from_stream(stream, *res)
                        .await
                        .unwrap();
                    black_box(cache)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("BitmapLapper_Sorted", name),
            &resolution,
            |b, res| {
                b.to_async(&runtime).iter(|| async {
                    let stream = create_stream_from_batches(batches);
                    let (cache, _) = BitmapStreamBuilder::from_stream(stream, *res)
                        .await
                        .unwrap();
                    black_box(cache)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark query performance on streaming-built caches
fn bench_streaming_queries(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let resolution = Duration::from_secs(60);
    let chunk_size = 1_000_000;

    // Build both caches using streaming with from_stream
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (value_aware_cache, bitmap_cache) = runtime.block_on(async {
        let value_aware_stream = create_stream_from_batches(batches);
        let bitmap_stream = create_stream_from_batches(batches);

        let (value_aware_cache, _) =
            ChunkedStreamBuilder::from_stream(value_aware_stream, resolution, chunk_size)
                .await
                .unwrap();

        let (bitmap_cache, _) =
            BitmapChunkedStreamBuilder::from_stream(bitmap_stream, resolution, chunk_size)
                .await
                .unwrap();

        (value_aware_cache, bitmap_cache)
    });

    println!("\n=== Streaming-Built Cache Comparison ===");
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

    // Test point queries at different positions
    let test_positions = [
        0,
        all_timestamps.len() / 4,
        all_timestamps.len() / 2,
        all_timestamps.len() * 3 / 4,
        all_timestamps.len() - 1,
    ];

    let mut group = c.benchmark_group("streaming_point_query");

    for (i, &pos) in test_positions.iter().enumerate() {
        let ts = all_timestamps[pos];

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

/// Benchmark range queries on streaming-built caches
fn bench_streaming_range_queries(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let resolution = Duration::from_secs(60);
    let chunk_size = 1_000_000;

    // Build both caches using streaming with from_stream
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (value_aware_cache, bitmap_cache) = runtime.block_on(async {
        let value_aware_stream = create_stream_from_batches(batches);
        let bitmap_stream = create_stream_from_batches(batches);

        let (value_aware_cache, _) =
            ChunkedStreamBuilder::from_stream(value_aware_stream, resolution, chunk_size)
                .await
                .unwrap();

        let (bitmap_cache, _) =
            BitmapChunkedStreamBuilder::from_stream(bitmap_stream, resolution, chunk_size)
                .await
                .unwrap();

        (value_aware_cache, bitmap_cache)
    });

    // Get timestamp range
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
    let duration_ns = max_ts - min_ts;

    // Test different range sizes
    let ranges = vec![
        ("10%", min_ts..(min_ts + duration_ns / 10)),
        ("50%", min_ts..(min_ts + duration_ns / 2)),
        ("100%", min_ts..max_ts),
    ];

    let mut group = c.benchmark_group("streaming_range_query");
    group.sample_size(20);

    for (name, range) in ranges {
        println!("\nRange query: {} of data", name);

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

/// Benchmark memory footprint of streaming-built caches
fn bench_streaming_memory(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let resolution = Duration::from_secs(60);
    let chunk_size = 1_000_000;

    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Build chunked streaming caches
    let (value_aware_cache, bitmap_cache) = runtime.block_on(async {
        let value_aware_stream = create_stream_from_batches(batches);
        let bitmap_stream = create_stream_from_batches(batches);

        let (value_aware_cache, _) =
            ChunkedStreamBuilder::from_stream(value_aware_stream, resolution, chunk_size)
                .await
                .unwrap();

        let (bitmap_cache, _) =
            BitmapChunkedStreamBuilder::from_stream(bitmap_stream, resolution, chunk_size)
                .await
                .unwrap();

        (value_aware_cache, bitmap_cache)
    });

    // Build sorted streaming caches
    let (value_aware_sorted_cache, bitmap_sorted_cache) = runtime.block_on(async {
        let value_aware_stream = create_stream_from_batches(batches);
        let bitmap_stream = create_stream_from_batches(batches);

        let (value_aware_cache, _) = SortedStreamBuilder::from_stream(value_aware_stream, resolution)
            .await
            .unwrap();

        let (bitmap_cache, _) = BitmapStreamBuilder::from_stream(bitmap_stream, resolution)
            .await
            .unwrap();

        (value_aware_cache, bitmap_cache)
    });

    let value_aware_size = value_aware_cache.size_bytes();
    let bitmap_size = bitmap_cache.size_bytes();
    let value_aware_sorted_size = value_aware_sorted_cache.size_bytes();
    let bitmap_sorted_size = bitmap_sorted_cache.size_bytes();

    println!("\n=== Cache Sizes ===");
    println!("Chunked streaming (chunk_size=1M):");
    println!(
        "  ValueAwareLapperCache: {} bytes ({:.2} MB)",
        value_aware_size,
        value_aware_size as f64 / 1_048_576.0
    );
    println!(
        "  BitmapLapperCache: {} bytes ({:.2} MB)",
        bitmap_size,
        bitmap_size as f64 / 1_048_576.0
    );
    println!(
        "  Reduction: {:.1}x",
        value_aware_size as f64 / bitmap_size as f64
    );
    println!("\nSorted streaming (single-pass):");
    println!(
        "  ValueAwareLapperCache: {} bytes ({:.2} MB)",
        value_aware_sorted_size,
        value_aware_sorted_size as f64 / 1_048_576.0
    );
    println!(
        "  BitmapLapperCache: {} bytes ({:.2} MB)",
        bitmap_sorted_size,
        bitmap_sorted_size as f64 / 1_048_576.0
    );
    println!(
        "  Reduction: {:.1}x",
        value_aware_sorted_size as f64 / bitmap_sorted_size as f64
    );

    let mut group = c.benchmark_group("streaming_memory");
    group.sample_size(10);

    group.bench_function("ValueAwareLapper_chunked", |b| {
        b.iter(|| black_box(value_aware_cache.size_bytes()));
    });

    group.bench_function("BitmapLapper_chunked", |b| {
        b.iter(|| black_box(bitmap_cache.size_bytes()));
    });

    group.bench_function("ValueAwareLapper_sorted", |b| {
        b.iter(|| black_box(value_aware_sorted_cache.size_bytes()));
    });

    group.bench_function("BitmapLapper_sorted", |b| {
        b.iter(|| black_box(bitmap_sorted_cache.size_bytes()));
    });

    group.finish();
}

/// Benchmark chunking overhead with different chunk sizes
fn bench_chunk_size_impact(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let resolution = Duration::from_secs(60);
    let total_rows = count_rows(batches);

    let chunk_sizes = vec![1_000_000, 5_000_000, 10_000_000];

    println!("\n=== Chunk Size Impact ===");
    println!("Total rows: {}", total_rows);

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("chunk_size_impact");
    group.throughput(Throughput::Elements(total_rows as u64));

    for chunk_size in &chunk_sizes {
        group.bench_with_input(
            BenchmarkId::new("ValueAwareLapper", format!("{}M", chunk_size / 1_000_000)),
            chunk_size,
            |b, &cs| {
                b.to_async(&runtime).iter(|| async {
                    let stream = create_stream_from_batches(batches);
                    let (cache, _) = ChunkedStreamBuilder::from_stream(stream, resolution, cs)
                        .await
                        .unwrap();
                    black_box(cache)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("BitmapLapper", format!("{}M", chunk_size / 1_000_000)),
            chunk_size,
            |b, &cs| {
                b.to_async(&runtime).iter(|| async {
                    let stream = create_stream_from_batches(batches);
                    let (cache, _) = BitmapChunkedStreamBuilder::from_stream(stream, resolution, cs)
                        .await
                        .unwrap();
                    black_box(cache)
                });
            },
        );
    }

    group.finish();
}

/// Compare streaming vs batch building for both implementations
fn bench_streaming_vs_batch(c: &mut Criterion) {
    let batches = match get_batches() {
        Some(b) => b,
        None => return,
    };

    let resolution = Duration::from_secs(60);
    let chunk_size = 1_000_000;
    let total_rows = count_rows(batches);

    // Extract all data as points for batch building
    let mut all_points = Vec::new();
    for batch in batches {
        let points = tag_values_cache::extract_tags_from_batch(batch);
        all_points.extend(points);
    }

    println!("\n=== Streaming vs Batch Comparison ===");
    println!("Total points: {}", all_points.len());

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("streaming_vs_batch");
    group.throughput(Throughput::Elements(total_rows as u64));

    // ValueAwareLapperCache - Streaming
    group.bench_function("ValueAwareLapper_streaming", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_stream_from_batches(batches);
            let (cache, _) = ChunkedStreamBuilder::from_stream(stream, resolution, chunk_size)
                .await
                .unwrap();
            black_box(cache)
        });
    });

    // ValueAwareLapperCache - Batch
    group.bench_function("ValueAwareLapper_batch", |b| {
        b.iter(|| {
            let cache = ValueAwareLapperCache::from_unsorted_with_resolution(
                black_box(all_points.clone()),
                resolution,
            )
            .unwrap();
            black_box(cache)
        });
    });

    // BitmapLapperCache - Streaming
    group.bench_function("BitmapLapper_streaming", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_stream_from_batches(batches);
            let (cache, _) = BitmapChunkedStreamBuilder::from_stream(stream, resolution, chunk_size)
                .await
                .unwrap();
            black_box(cache)
        });
    });

    // BitmapLapperCache - Batch
    group.bench_function("BitmapLapper_batch", |b| {
        b.iter(|| {
            let cache = BitmapLapperCache::from_unsorted_with_resolution(
                black_box(all_points.clone()),
                resolution,
            )
            .unwrap();
            black_box(cache)
        });
    });

    // ValueAwareLapperCache - Sorted Streaming
    group.bench_function("ValueAwareLapper_sorted_streaming", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_stream_from_batches(batches);
            let (cache, _) = SortedStreamBuilder::from_stream(stream, resolution)
                .await
                .unwrap();
            black_box(cache)
        });
    });

    // BitmapLapperCache - Sorted Streaming
    group.bench_function("BitmapLapper_sorted_streaming", |b| {
        b.to_async(&runtime).iter(|| async {
            let stream = create_stream_from_batches(batches);
            let (cache, _) = BitmapStreamBuilder::from_stream(stream, resolution)
                .await
                .unwrap();
            black_box(cache)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_streaming_construction,
    bench_streaming_queries,
    bench_streaming_range_queries,
    bench_streaming_memory,
    bench_chunk_size_impact,
    bench_streaming_vs_batch,
);
criterion_main!(benches);
