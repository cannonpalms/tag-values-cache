//! Benchmark for tag values cache using BitmapLapperCache with stream builder
//!
//! This benchmark focuses on the BitmapLapperCache using the streaming API
//! (BitmapStreamBuilder). It includes cache construction benchmarks and 100%
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

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use data_loader::BenchConfig;
use futures::{StreamExt, stream};
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::{
    IntervalCache, format_bytes,
    streaming::{BitmapStreamBuilder, SendableRecordBatchStream},
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

/// Helper to create a stream from in-memory batches
fn create_stream_from_batches(
    batches: &'static [arrow::array::RecordBatch],
) -> SendableRecordBatchStream {
    stream::iter(batches.iter().map(|b| Ok(b.clone()))).boxed()
}

fn stream_record_batches(config: &BenchConfig) -> SendableRecordBatchStream {
    if config.stream_from_disk {
        data_loader::create_stream_from_disk(config).unwrap()
    } else {
        let batches = get_batches().unwrap();
        create_stream_from_batches(batches)
    }
}

/// Benchmark cache build using stream builder
fn bench_cache_build(c: &mut Criterion) {
    let bench_config = get_config();
    bench_config.print_config();

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let total_rows = if bench_config.stream_from_disk {
        // For streaming mode, we can't easily count rows upfront
        // Use max_rows if finite, otherwise None
        if bench_config.max_rows == usize::MAX {
            None
        } else {
            Some(bench_config.max_rows)
        }
    } else {
        let batches = match get_batches() {
            Some(b) => b,
            None => return,
        };
        Some(count_rows(batches))
    };

    let mut group = c.benchmark_group("cache_build");
    if let Some(rows) = total_rows {
        group.throughput(Throughput::Elements(rows as u64));
    }

    // Test different resolutions
    let config = ("1hour", Duration::from_secs(3600));

    println!("\n=== Cache Construction ===");
    if let Some(rows) = total_rows {
        println!("Total rows: {}", rows);
    } else {
        println!("Total rows: unlimited (streaming from disk)");
    }

    let (name, resolution) = config;
    println!("\nConfig: {} (resolution={:?})", name, resolution);

    // Sequential processing
    group.bench_with_input(
        BenchmarkId::new("BitmapStream_Sequential", name),
        &resolution,
        |b, res| {
            b.to_async(&runtime).iter(|| async {
                let stream = stream_record_batches(&bench_config);
                let (cache, stats) = BitmapStreamBuilder::from_stream(stream, *res)
                    .await
                    .unwrap();

                println!("Built cache - stats: {:#?}", stats);
                black_box(cache)
            });
        },
    );

    // Parallel processing with different parallelism levels
    for parallelism in [2, 4, 8, rayon::current_num_threads()] {
        group.bench_with_input(
            BenchmarkId::new(format!("BitmapStream_Parallel_{}", parallelism), name),
            &(resolution, parallelism),
            |b, (res, par)| {
                b.to_async(&runtime).iter_batched(
                    || stream_record_batches(&bench_config),
                    |stream| async {
                        // let stream = stream_record_batches(&bench_config);
                        let (cache, stats) =
                            BitmapStreamBuilder::from_stream_parallel(stream, *res, *par)
                                .await
                                .unwrap();
                        println!("Built cache - stats: {:#?}", stats);
                        black_box(cache)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark 100% range queries on caches
fn bench_range_queries(c: &mut Criterion) {
    let bench_config = get_config();

    let resolution = Duration::from_secs(3600); // 1 hour

    // Build cache using streaming builder
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bitmap_cache = runtime.block_on(async {
        let stream = stream_record_batches(&bench_config);
        let (cache, _) = BitmapStreamBuilder::from_stream(stream, resolution)
            .await
            .unwrap();
        cache
    });

    println!("\n=== Stream-Built Cache Info ===");
    println!(
        "BitmapLapperCache intervals: {}",
        bitmap_cache.interval_count()
    );
    println!(
        "BitmapLapperCache size: {}",
        format_bytes(bitmap_cache.size_bytes())
    );

    // Get time range from cache
    let min_ts = bitmap_cache.min_timestamp().unwrap();
    let max_ts = bitmap_cache.max_timestamp().unwrap();

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
