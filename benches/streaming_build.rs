// Benchmark comparing synchronous batch processing vs async stream processing.
//
// This benchmark tests four scenarios:
// 1. chunked_sync: Processing batches synchronously with ChunkedStreamBuilder
// 2. chunked_async: Async streaming directly from disk (true incremental)
// 3. sorted_sync: Processing batches synchronously with SortedStreamBuilder
// 4. sorted_async: Async streaming directly from disk (true incremental)
//
// The async variants demonstrate true streaming benefits - reading data incrementally
// from disk with bounded memory usage, never loading the entire dataset into memory.

mod data_loader;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::stream::{self, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
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

/// Create a SendableRecordBatchStream that reads directly from parquet files on disk
/// This demonstrates true streaming - data is read incrementally without loading everything into memory
async fn create_stream_from_disk() -> Result<SendableRecordBatchStream, std::io::Error> {
    let config = data_loader::BenchConfig::from_env();
    let path = &config.input_path;
    let max_rows = config.max_rows;

    // Get list of parquet files
    let parquet_files: Vec<std::path::PathBuf> = if path.is_file() {
        vec![path.clone()]
    } else if path.is_dir() {
        std::fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let entry_path = entry.path();
                if entry_path.is_file()
                    && entry_path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    Some(entry_path)
                } else {
                    None
                }
            })
            .collect()
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path not found: {:?}", path),
        ));
    };

    if parquet_files.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No parquet files found",
        ));
    }

    // Sort files for consistent ordering
    let mut parquet_files = parquet_files;
    parquet_files.sort();

    // Create a stream that yields batches one by one without collecting
    // This uses unfold to maintain state across iterations
    let rows_read = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rows_read_clone = rows_read.clone();

    let stream = stream::unfold(
        (parquet_files.into_iter(), None, rows_read_clone, max_rows),
        move |(mut files, mut current_reader, rows_counter, limit)| async move {
            loop {
                // Check if we've reached the row limit
                if rows_counter.load(std::sync::atomic::Ordering::Relaxed) >= limit {
                    return None;
                }

                // Get or create the current reader
                let reader = match current_reader.take() {
                    Some(r) => r,
                    None => {
                        // Try to open the next file
                        let file_path = files.next()?;
                        let file = File::open(&file_path)
                            .map_err(|e| {
                                ArrowError::IoError(
                                    format!("Failed to open {:?}: {}", file_path, e),
                                    e,
                                )
                            })
                            .ok()?;
                        let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
                        builder.build().ok()?
                    }
                };

                // Try to read the next batch
                let mut reader_iter = reader.into_iter();
                if let Some(batch_result) = reader_iter.next() {
                    match batch_result {
                        Ok(batch) => {
                            let batch_rows = batch.num_rows();
                            let current_total = rows_counter
                                .fetch_add(batch_rows, std::sync::atomic::Ordering::Relaxed);

                            // Check if this batch would exceed the limit
                            if current_total + batch_rows > limit {
                                // Slice the batch to fit exactly within the limit
                                let remaining = limit - current_total;
                                if remaining > 0 {
                                    let sliced_batch = batch.slice(0, remaining);
                                    return Some((
                                        Ok(sliced_batch),
                                        (files, None, rows_counter, limit),
                                    ));
                                } else {
                                    return None;
                                }
                            }

                            // Return the batch and continue with this reader
                            return Some((
                                Ok(batch),
                                (files, Some(reader_iter), rows_counter, limit),
                            ));
                        }
                        Err(e) => {
                            // Return error and try next file
                            return Some((Err(e), (files, None, rows_counter, limit)));
                        }
                    }
                } else {
                    // Current file exhausted, try next file
                    current_reader = None;
                    continue;
                }
            }
        },
    )
    .boxed();

    Ok(stream)
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

    // Benchmark 2: Chunked streaming using async process_stream (from disk)
    group.bench_function("chunked_async", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&runtime).iter(|| async {
            // Stream directly from disk - true incremental processing
            let stream = create_stream_from_disk()
                .await
                .expect("Failed to create disk stream");
            let cache = ChunkedStreamBuilder::from_stream(stream, resolution, chunk_size)
                .await
                .unwrap();
            black_box(cache);
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

    // Benchmark 4: Sorted streaming using async process_stream (from disk)
    group.bench_function("sorted_async", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&runtime).iter(|| async {
            // Stream directly from disk - true incremental processing
            let stream = create_stream_from_disk()
                .await
                .expect("Failed to create disk stream");
            let cache = SortedStreamBuilder::from_stream(stream, resolution)
                .await
                .unwrap();
            black_box(cache);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_streaming_builders);
criterion_main!(benches);
