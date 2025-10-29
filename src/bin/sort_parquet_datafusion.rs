//! Memory-efficient parquet sorting using DataFusion's query engine
//! DataFusion handles spilling to disk automatically when memory is limited
use datafusion::arrow::datatypes::DataType;
use datafusion::config::{ConfigOptions, SpillCompression};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::execution::context::SessionContext;
use datafusion::execution::disk_manager::{DiskManager, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure DataFusion disk spill limit at startup (before any DataFusion initialization)
    // Set to 500GB max. This must be set via environment variable as DataFusion 49.0 has no
    // programmatic API for this configuration.
    let max_disk_bytes = 500 * 1024 * 1024 * 1024u64; // 500GB
    unsafe {
        std::env::set_var(
            "DATAFUSION_DISK_MANAGER_TEMP_MAX_BYTES",
            max_disk_bytes.to_string(),
        );
    }

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input.parquet> [output.parquet]", args[0]);
        eprintln!("If output is not specified, will create <input>-sorted.parquet");
        std::process::exit(1);
    }

    let input_path = Path::new(&args[1]);
    let output_path = if args.len() > 2 {
        PathBuf::from(&args[2])
    } else {
        let stem = input_path.file_stem().unwrap().to_str().unwrap();
        let parent = input_path.parent().unwrap_or(Path::new("."));
        parent.join(format!("{}-sorted.parquet", stem))
    };

    println!("Input:  {}", input_path.display());
    println!("Output: {}", output_path.display());
    println!("Using DataFusion query engine for memory-efficient sorting");
    println!();

    let start_time = Instant::now();

    // Configure DataFusion with memory limits
    // Set to 30GB memory pool with disk spilling enabled
    let memory_pool = Arc::new(FairSpillPool::new(30 * 1024 * 1024 * 1024)) as Arc<dyn MemoryPool>;

    // Create spill directory in current working directory (on actual disk)
    let spill_dir = std::env::current_dir()?.join("datafusion-spill");
    std::fs::create_dir_all(&spill_dir)?;
    println!("Using spill directory: {}", spill_dir.display());
    println!("Memory pool: 30 GB, Disk spill limit: 500 GB");

    let disk_manager_builder = DiskManager::builder()
        .with_mode(DiskManagerMode::Directories(vec![PathBuf::from(
            &spill_dir,
        )]))
        .with_max_temp_directory_size(max_disk_bytes);

    let runtime_builder = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager_builder);

    let runtime_env = Arc::new(runtime_builder.build()?);

    // Configure session with optimizations
    let mut config = ConfigOptions::new();

    // Enable spilling for sorts with smaller reservation
    config.execution.sort_spill_reservation_bytes = 100 * 1024 * 1024; // 2MB reservation per sort
    config.execution.spill_compression = SpillCompression::Zstd;

    config.execution.enable_recursive_ctes = false;

    // Reduce parallelism to use less memory
    config.execution.target_partitions = 16; // Reduce from default 32 to 4 partitions

    // Set batch size for processing
    config.execution.batch_size = 8192;
    config.execution.coalesce_batches = true;

    // Enable parquet optimizations
    config.execution.parquet.pushdown_filters = true;
    config.execution.parquet.reorder_filters = true;
    config.execution.parquet.enable_page_index = true;

    let session_config = SessionConfig::from(config);
    let ctx = SessionContext::new_with_config_rt(session_config, runtime_env);

    // Register the parquet file as a table
    println!("Reading input parquet file...");
    ctx.register_parquet(
        "input_table",
        input_path.to_str().unwrap(),
        Default::default(),
    )
    .await?;

    // Get the schema to check data types
    let df = ctx.table("input_table").await?;
    let schema = df.schema();

    println!("Original schema:");
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }

    // Build the SQL query with CAST if needed
    let time_field = schema.field_with_name(None, "time")?;
    let query = if time_field.data_type() == &DataType::Int64 {
        println!("\nConverting 'time' column from Int64 to Timestamp(Nanosecond)");

        // Build SELECT list with CAST for time column
        // The Int64 values are already in nanoseconds, so we need to use arrow_cast
        // to properly convert them to timestamp without unit conversion
        let mut select_items = Vec::new();
        for field in schema.fields() {
            if field.name() == "time" {
                // Use arrow_cast to convert Int64 nanoseconds directly to Timestamp
                select_items
                    .push("arrow_cast(time, 'Timestamp(Nanosecond, None)') AS time".to_string());
            } else {
                select_items.push(format!("\"{}\"", field.name()));
            }
        }

        format!(
            "SELECT {} FROM input_table ORDER BY time",
            select_items.join(", ")
        )
    } else {
        println!("\nTime column already has correct type");
        "SELECT * FROM input_table ORDER BY time".to_string()
    };

    println!("\nExecuting sort query...");
    println!("Query: {}", query);

    // Execute the query
    let sorted_df = ctx.sql(&query).await?;

    // Show execution plan for debugging
    println!("\nExecution plan:");
    sorted_df.clone().explain(false, false)?.show().await?;

    // Write the sorted data to parquet
    println!("\nWriting sorted data to output file...");

    // Write to parquet
    sorted_df
        .write_parquet(
            output_path.to_str().unwrap(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

    let elapsed = start_time.elapsed();
    let file_size = std::fs::metadata(&output_path)?.len();

    // Get row count
    let count_df = ctx.sql("SELECT COUNT(*) as cnt FROM input_table").await?;
    let batches = count_df.collect().await?;
    let row_count = if let Some(batch) = batches.first() {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        array.value(0) as usize
    } else {
        0
    };

    println!("\n====================================");
    println!("Sorting Complete!");
    println!("====================================");
    println!("  Output file: {}", output_path.display());
    println!("  Total rows: {}", row_count);
    println!(
        "  File size: {:.2} MB",
        file_size as f64 / (1024.0 * 1024.0)
    );
    println!("  Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.0} rows/sec",
        row_count as f64 / elapsed.as_secs_f64()
    );
    println!("  Memory pool: 2 GB, Disk spill limit: 500 GB");

    // Clean up spill directory
    let spill_dir = std::env::current_dir()?.join("datafusion-spill");
    if spill_dir.exists() {
        std::fs::remove_dir_all(&spill_dir)?;
        println!("  Cleaned up spill directory");
    }

    Ok(())
}
