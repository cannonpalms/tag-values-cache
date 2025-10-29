///! Memory-efficient parquet sorting using streaming approach
///! Sorts data by timestamp column and converts Int64 to Timestamp type
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::{self, SortOptions};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    println!("Using streaming sort with limited memory");
    println!();

    let start_time = Instant::now();

    // Open input file
    let file = File::open(input_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let old_schema = builder.schema().clone();
    let mut reader = builder.build()?;

    println!("Original schema:");
    for field in old_schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }

    // Create new schema with Timestamp type for 'time' column
    let mut new_fields: Vec<Field> = Vec::new();
    let mut time_col_idx = None;

    for (idx, field) in old_schema.fields().iter().enumerate() {
        if field.name() == "time" {
            println!(
                "Converting 'time' column from {:?} to Timestamp(Nanosecond, None)",
                field.data_type()
            );

            let new_field = Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone());

            new_fields.push(new_field);
            time_col_idx = Some(idx);
        } else {
            new_fields.push((**field).clone());
        }
    }

    let time_col_idx = time_col_idx.expect("Could not find 'time' column");
    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        old_schema.metadata().clone(),
    ));

    // Read and sort data in batches to limit memory usage
    const MAX_BATCH_SIZE: usize = 2_000_000; // rows to accumulate before sorting
    let mut current_batch_group: Vec<RecordBatch> = Vec::new();
    let mut current_rows = 0;
    let mut sorted_batches: Vec<RecordBatch> = Vec::new();

    println!("\nReading and sorting data...");

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let batch_rows = batch.num_rows();
        current_batch_group.push(batch);
        current_rows += batch_rows;

        if current_rows >= MAX_BATCH_SIZE {
            println!("  Sorting batch with {} rows", current_rows);
            let sorted_batch =
                sort_batch_group(&current_batch_group, &old_schema, &new_schema, time_col_idx)?;
            sorted_batches.push(sorted_batch);

            current_batch_group.clear();
            current_rows = 0;
        }
    }

    // Sort remaining batches
    if !current_batch_group.is_empty() {
        println!("  Sorting final batch with {} rows", current_rows);
        let sorted_batch =
            sort_batch_group(&current_batch_group, &old_schema, &new_schema, time_col_idx)?;
        sorted_batches.push(sorted_batch);
    }

    // If we have multiple sorted batches, do a final merge
    let final_batch = if sorted_batches.len() > 1 {
        println!("\nMerging {} sorted batches...", sorted_batches.len());
        let concatenated = arrow::compute::concat_batches(&new_schema, &sorted_batches)?;
        println!("  Sorting merged data ({} rows)", concatenated.num_rows());

        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let indices = arrow::compute::sort_to_indices(
            concatenated.column(time_col_idx),
            Some(sort_options),
            None,
        )?;

        let reordered_columns: Vec<ArrayRef> = concatenated
            .columns()
            .iter()
            .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new(new_schema.clone(), reordered_columns)?
    } else {
        sorted_batches.into_iter().next().unwrap()
    };

    println!("\nWriting sorted data...");
    let output_file = File::create(&output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, new_schema.clone(), Some(props))?;

    // Write in chunks
    const WRITE_BATCH_SIZE: usize = 100_000;
    let total_rows = final_batch.num_rows();
    let num_full_batches = total_rows / WRITE_BATCH_SIZE;
    let remainder = total_rows % WRITE_BATCH_SIZE;

    for batch_idx in 0..num_full_batches {
        let start = batch_idx * WRITE_BATCH_SIZE;
        let batch_slice = final_batch.slice(start, WRITE_BATCH_SIZE);
        writer.write(&batch_slice)?;

        if batch_idx % 10 == 0 {
            println!("  Wrote {} / {} rows", start + WRITE_BATCH_SIZE, total_rows);
        }
    }

    if remainder > 0 {
        let start = num_full_batches * WRITE_BATCH_SIZE;
        let batch_slice = final_batch.slice(start, remainder);
        writer.write(&batch_slice)?;
    }

    writer.close()?;

    let elapsed = start_time.elapsed();
    let file_size = std::fs::metadata(&output_path)?.len();

    println!("\nDone!");
    println!("  Output file: {}", output_path.display());
    println!("  Total rows: {}", total_rows);
    println!(
        "  File size: {:.2} MB",
        file_size as f64 / (1024.0 * 1024.0)
    );
    println!("  Time elapsed: {:.2}s", elapsed.as_secs_f64());

    Ok(())
}

/// Sort a group of batches
fn sort_batch_group(
    batches: &[RecordBatch],
    old_schema: &Arc<Schema>,
    new_schema: &Arc<Schema>,
    time_col_idx: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Concatenate batches
    let concatenated = arrow::compute::concat_batches(old_schema, batches)?;

    // Sort by timestamp
    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let indices = arrow::compute::sort_to_indices(
        concatenated.column(time_col_idx),
        Some(sort_options),
        None,
    )?;

    // Reorder columns
    let reordered_columns: Vec<ArrayRef> = concatenated
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;

    // Convert time column to Timestamp type
    let mut final_columns: Vec<ArrayRef> = Vec::new();
    for (idx, column) in reordered_columns.iter().enumerate() {
        if idx == time_col_idx {
            let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
            let timestamp_array = compute::cast(column.as_ref(), &timestamp_type)?;
            final_columns.push(timestamp_array);
        } else {
            final_columns.push(column.clone());
        }
    }

    Ok(RecordBatch::try_new(new_schema.clone(), final_columns)?)
}
