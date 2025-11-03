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
//
// # Environment Variables
//
// - `BENCH_INPUT_PATH`: Path to parquet files (default: "benches/data/parquet")
// - `BENCH_MAX_ROWS`: Maximum rows to load (default: 100,000)
// - `BENCH_MAX_DURATION`: Max time span (default: "1h")
//   - Supports: "30s", "5m", "1h", "2d", "1week" or raw nanoseconds
//
// # Examples
//
// ```bash
// # Default run
// cargo bench --bench streaming_build
//
// # Custom duration
// BENCH_MAX_DURATION=30m cargo bench --bench streaming_build
// ```

mod data_loader;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion::config::ConfigOptions;
use datafusion::prelude::*;
use futures::stream::StreamExt;
use std::sync::OnceLock;
use std::time::Duration;
use tag_values_cache::streaming::{
    ChunkedStreamBuilder, SendableRecordBatchStream, SortedStreamBuilder,
};

// Global configuration and data loaded once and shared across all benchmarks
static CONFIG: OnceLock<data_loader::BenchConfig> = OnceLock::new();
static RECORD_BATCHES: OnceLock<Vec<RecordBatch>> = OnceLock::new();

/// Get or initialize the benchmark configuration
fn get_config() -> &'static data_loader::BenchConfig {
    CONFIG.get_or_init(data_loader::BenchConfig::from_env)
}

/// Load RecordBatches once and return a reference to them
fn get_record_batches() -> Option<&'static Vec<RecordBatch>> {
    RECORD_BATCHES.get_or_init(|| {
        let config = get_config();
        match data_loader::load_record_batches(config) {
            Ok(batches) => batches,
            Err(e) => {
                eprintln!("Error loading RecordBatches: {}", e);
                Vec::new()
            }
        }
    });

    let batches = RECORD_BATCHES.get().unwrap();
    if batches.is_empty() {
        None
    } else {
        Some(batches)
    }
}

/// Converts a DataFusion stream to our Arrow stream type
fn adapt_datafusion_stream(
    df_stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    // Map each DataFusion RecordBatch to our Arrow RecordBatch
    df_stream
        .map(|result| {
            match result {
                Ok(df_batch) => {
                    // DataFusion's batch uses a newer arrow version, but the structure is the same.
                    // We need to reconstruct the batch with our arrow version.

                    // Get the raw bytes representation from DataFusion's batch
                    // and reconstruct using our arrow version
                    use arrow::ipc::reader::StreamReader;
                    use std::io::Cursor;

                    // Serialize the DataFusion batch to IPC format
                    let mut buffer = Vec::new();
                    {
                        let df_schema = df_batch.schema();
                        let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(
                            &mut buffer,
                            &df_schema,
                        )
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                        writer
                            .write(&df_batch)
                            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                        writer
                            .finish()
                            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                    }

                    // Deserialize using our arrow version
                    let cursor = Cursor::new(buffer);
                    let reader = StreamReader::try_new(cursor, None)
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;

                    // Get the first (and only) batch
                    let batches: Vec<_> = reader
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;

                    if let Some(batch) = batches.into_iter().next() {
                        Ok(batch)
                    } else {
                        Err(ArrowError::from_external_error(Box::new(
                            std::io::Error::other("Failed to convert batch"),
                        )))
                    }
                }
                Err(e) => Err(ArrowError::from_external_error(Box::new(e))),
            }
        })
        .boxed()
}

/// Create IOx-style DataFusion SessionConfig with proper parquet optimizations
fn create_iox_session_config() -> SessionConfig {
    // Create config with IOx-style optimizations
    let mut options = ConfigOptions::new();

    // Enable parquet predicate pushdown optimization (IOx pattern)
    options.execution.parquet.pushdown_filters = true;
    options.execution.parquet.reorder_filters = true;

    // Enable optimizer settings used by IOx
    options.optimizer.repartition_sorts = true;
    options.optimizer.prefer_existing_union = true;

    // Use row number estimates for optimization (IOx pattern)
    options
        .execution
        .use_row_number_estimates_to_optimize_partitioning = true;

    let mut config = SessionConfig::from(options);

    // Set batch size to 8KB like IOx (8 * 1024 bytes)
    config = config.with_batch_size(8 * 1024);

    // Don't coalesce batches to preserve streaming behavior
    config = config.with_coalesce_batches(false);

    config
}

/// Create a SendableRecordBatchStream that reads directly from parquet files on disk using DataFusion
/// This uses DataFusion's query engine with IOx-style configuration for optimal parquet handling
async fn create_stream_from_disk() -> Result<SendableRecordBatchStream, std::io::Error> {
    let config = get_config();
    let path = &config.input_path;
    let max_rows = config.max_rows;

    // Validate that the input path exists
    if !path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path not found: {:?}", path),
        ));
    }

    // Create DataFusion context with IOx-style configuration
    let session_config = create_iox_session_config();
    let ctx = SessionContext::new_with_config(session_config);

    // Register parquet files
    // Note: We're using a single file to avoid dictionary merge issues
    // IOx handles this by reading files in groups with consistent schemas
    let parquet_file = if path.is_dir() {
        // Find the first parquet file
        std::fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_file() && path.extension()?.to_str()? == "parquet" {
                    Some(path)
                } else {
                    None
                }
            })
            .next()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No parquet files found in directory",
                )
            })?
    } else {
        path.clone()
    };

    // Configure ParquetReadOptions with IOx-style settings
    let parquet_options = ParquetReadOptions::default().parquet_pruning(true); // Enable file pruning based on predicates

    // Register the parquet file
    ctx.register_parquet("data", parquet_file.to_str().unwrap(), parquet_options)
        .await
        .map_err(std::io::Error::other)?;

    // First, get the schema to understand the columns
    let test_df = ctx
        .sql("SELECT * FROM data LIMIT 1")
        .await
        .map_err(std::io::Error::other)?;

    let schema = test_df.schema();

    // Find the time column using IOx conventions
    // IOx marks time columns with metadata "iox::column_type::timestamp"
    let time_column = schema
        .fields()
        .iter()
        .find(|f| {
            // Check metadata first (IOx pattern)
            if let Some(column_type) = f.metadata().get("iox::column::type")
                && column_type == "iox::column_type::timestamp"
            {
                return true;
            }
            // Fallback to name-based detection
            let name_lower = f.name().to_lowercase();
            name_lower == "time"
                || name_lower == "timestamp"
                || name_lower == "_time"
                || name_lower == "eventtime"
        })
        .map(|f| f.name().clone())
        .unwrap_or_else(|| "time".to_string());

    // Build a query that casts dictionary columns to regular strings
    // This avoids schema merge issues with different dictionary IDs
    let column_selections: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            match field.data_type() {
                datafusion::arrow::datatypes::DataType::Dictionary(_, _) => {
                    // Cast dictionary columns to regular strings
                    format!("CAST({} AS VARCHAR) AS {}", field.name(), field.name())
                }
                _ => field.name().clone(),
            }
        })
        .collect();

    let columns_str = column_selections.join(", ");

    // Build the query with ORDER BY and LIMIT
    let query = format!(
        "SELECT {} FROM data ORDER BY {} ASC LIMIT {}",
        columns_str, time_column, max_rows
    );

    // Execute the query
    let df = ctx.sql(&query).await.map_err(std::io::Error::other)?;

    // Get the stream of record batches from DataFusion
    let df_stream = df.execute_stream().await.map_err(std::io::Error::other)?;

    // Convert DataFusion's stream to our Arrow stream type
    Ok(adapt_datafusion_stream(df_stream))
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
