// Benchmark comparing synchronous batch processing vs async stream processing.
//
// This benchmark tests four scenarios:
// 1. chunked_sync: Processing batches synchronously with ChunkedStreamBuilder
// 2. chunked_async: Processing a stream asynchronously with ChunkedStreamBuilder
// 3. sorted_sync: Processing batches synchronously with SortedStreamBuilder
// 4. sorted_async: Processing a stream asynchronously with SortedStreamBuilder
//
// The async versions demonstrate the streaming API, though performance is similar
// since data is already loaded in memory. In production, async streams would
// read data incrementally from disk/network, providing memory-bounded processing.

mod data_loader;

use arrow::array::RecordBatch;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::stream;
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::streaming::{
    ChunkedStreamBuilder, SendableRecordBatchStream, SortedStreamBuilder,
};

// Global data loaded once and shared across all benchmarks
static RECORD_BATCHES: OnceLock<Vec<RecordBatch>> = OnceLock::new();

/// Load RecordBatches once and return a reference to them
fn get_record_batches() -> Option<&'static Vec<RecordBatch>> {
    RECORD_BATCHES.get_or_init(|| match data_loader::load_record_batches() {
        Ok(batches) => batches,
        Err(e) => {
            eprintln!("Error loading RecordBatches: {}", e);
            Vec::new()
        }
    });

    let batches = RECORD_BATCHES.get().unwrap();
    if batches.is_empty() {
        None
    } else {
        Some(batches)
    }
}

/// Create a SendableRecordBatchStream from a vector of RecordBatches
fn create_stream(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
    Box::pin(stream::iter(batches.into_iter().map(Ok)))
}

/// Benchmark comparing Chunked vs Sorted streaming builders
fn bench_streaming_builders(c: &mut Criterion) {
    // Load RecordBatches for both streaming builders
    let record_batches = match get_record_batches() {
        Some(batches) => batches,
        None => return,
    };

    let total_rows: usize = record_batches.iter().map(|batch| batch.num_rows()).sum();
    let mut group = c.benchmark_group("streaming_builders");
    group.throughput(Throughput::Elements(total_rows as u64));

    // Fixed parameters: 1h resolution, 1M chunk size
    let resolution = Duration::from_secs(3600);
    let chunk_size = 1_000_000;

    // Create a tokio runtime for running async code
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Benchmark 1: Chunked streaming using synchronous process_batch
    group.bench_function("chunked_sync", |b| {
        b.iter(|| {
            let mut builder = ChunkedStreamBuilder::new(resolution, chunk_size);
            // Process RecordBatches synchronously
            for batch in record_batches {
                builder.process_batch(batch).unwrap();
            }
            let cache = builder.finalize().unwrap();
            black_box(cache);
        });
    });

    // Benchmark 2: Chunked streaming using async process_stream
    group.bench_function("chunked_async", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let stream = create_stream(record_batches.clone());
                let cache = ChunkedStreamBuilder::from_stream(stream, resolution, chunk_size)
                    .await
                    .unwrap();
                black_box(cache);
            });
        });
    });

    // Benchmark 3: Sorted streaming using synchronous process_batch
    group.bench_function("sorted_sync", |b| {
        b.iter(|| {
            let mut builder = SortedStreamBuilder::new(resolution);
            // Process RecordBatches synchronously
            for batch in record_batches {
                builder.process_batch(batch).unwrap();
            }
            let cache = builder.finalize().unwrap();
            black_box(cache);
        });
    });

    // Benchmark 4: Sorted streaming using async process_stream
    group.bench_function("sorted_async", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let stream = create_stream(record_batches.clone());
                let cache = SortedStreamBuilder::from_stream(stream, resolution)
                    .await
                    .unwrap();
                black_box(cache);
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_streaming_builders);
criterion_main!(benches);
