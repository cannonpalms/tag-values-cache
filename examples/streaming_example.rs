use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use futures::stream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::time::Duration;
use tag_values_cache::IntervalCache;
use tag_values_cache::streaming::{
    ChunkedStreamBuilder, SendableRecordBatchStream, SortedStreamBuilder,
};

/// Creates a stream from a Parquet file
fn create_parquet_stream(path: &str) -> Result<SendableRecordBatchStream, ArrowError> {
    let file = File::open(path)
        .map_err(|e| ArrowError::IoError(format!("Failed to open file: {}", e), e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    // Convert the iterator to a stream
    let batches: Result<Vec<RecordBatch>, ArrowError> = reader.collect();
    let batches = batches?;

    // Create a stream from the batches
    let stream = stream::iter(batches.into_iter().map(Ok));
    Ok(Box::pin(stream))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Using ChunkedStreamBuilder with a Parquet file
    println!("Example 1: ChunkedStreamBuilder with Parquet stream");

    let parquet_file = "benches/data/parquet/00d3b7ae-af87-05ba-1461-8b337e39f6ad.parquet";
    if std::path::Path::new(parquet_file).exists() {
        if let Ok(stream) = create_parquet_stream(parquet_file) {
            let (cache, stats) = ChunkedStreamBuilder::from_stream(
                stream,
                Duration::from_secs(3600), // 1 hour resolution
                1_000_000,                 // 1M points per chunk
            )
            .await?;

            println!("Built cache with {} intervals", cache.interval_count());
            println!(
                "Stats: {} points, {} chunks",
                stats.total_points, stats.chunks_flushed
            );
        } else {
            println!("Failed to create stream from parquet file");
        }
    } else {
        println!("Parquet file not found, skipping example 1");
    }

    // Example 2: Using SortedStreamBuilder with sorted stream and timing stats
    println!("\nExample 2: SortedStreamBuilder with from_stream (includes stats)");

    // Try to use a second file if available, otherwise skip
    let second_file = "benches/data/parquet/0f8350c2-cb7d-0c15-2990-5488675b169c.parquet";
    if std::path::Path::new(second_file).exists() {
        if let Ok(stream) = create_parquet_stream(second_file) {
            let (cache, stats) =
                SortedStreamBuilder::from_stream(stream, Duration::from_secs(60)).await?;

            println!(
                "Built sorted cache with {} intervals",
                cache.interval_count()
            );
            println!(
                "Stats: {} points across {} unique tagsets",
                stats.total_points, stats.unique_tagsets
            );
            println!("  I/O time: {:?}", stats.io_time);
            println!("  Processing time: {:?}", stats.processing_time);
            println!("  Out of order detected: {}", stats.out_of_order_detected);
        }
    } else {
        println!("Second parquet file not found, skipping example 2");
    }

    // Example 3: Processing a stream incrementally
    println!("\nExample 3: Incremental processing with process_stream()");

    if std::path::Path::new(parquet_file).exists() {
        if let Ok(stream) = create_parquet_stream(parquet_file) {
            let mut builder = ChunkedStreamBuilder::new(
                Duration::from_secs(3600),
                500_000, // Smaller chunks for demo
            );

            // Process the stream
            builder.process_stream(stream).await?;

            // Finalize and get statistics
            let (cache, stats) = builder.finalize_with_stats()?;

            println!(
                "Processed {} points in {} chunks",
                stats.total_points, stats.chunks_flushed
            );
            println!("Average chunk size: {:.0} points", stats.avg_chunk_size());
            println!("Final cache has {} intervals", cache.interval_count());
        }
    } else {
        println!("Parquet file not found, skipping example 3");
    }

    Ok(())
}
