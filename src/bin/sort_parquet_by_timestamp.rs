use arrow::array::{ArrayRef, RecordBatch, TimestampNanosecondArray};
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
    println!();

    let start_time = Instant::now();

    // Read all data from input file
    println!("Reading input file...");
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

            // Create new field with Timestamp type, preserving metadata
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

    let schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        old_schema.metadata().clone(),
    ));

    let mut all_batches = Vec::new();
    let mut total_rows = 0;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        total_rows += batch.num_rows();
        all_batches.push(batch);
    }

    println!(
        "  Loaded {} rows in {} batches",
        total_rows,
        all_batches.len()
    );

    // Concatenate all batches into a single batch for sorting
    println!("Concatenating batches...");
    let concatenated = arrow::compute::concat_batches(&old_schema, &all_batches)?;
    println!(
        "  Concatenated into single batch with {} rows",
        concatenated.num_rows()
    );

    // Sort by timestamp (first column, assumed to be "time")
    println!("Sorting by timestamp...");
    let time_col_idx = time_col_idx.expect("Could not find 'time' column");

    let sort_options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let indices = arrow::compute::sort_to_indices(
        concatenated.column(time_col_idx),
        Some(sort_options),
        None,
    )?;

    println!("  Reordering columns...");
    let reordered_columns: Vec<ArrayRef> = concatenated
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;

    // Convert time column from Int64 to Timestamp using Arrow's cast
    println!("  Converting time column to Timestamp type...");
    let mut final_columns: Vec<ArrayRef> = Vec::new();
    for (idx, column) in reordered_columns.iter().enumerate() {
        if idx == time_col_idx {
            // Use Arrow's cast to convert Int64 to Timestamp
            let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
            let timestamp_array = compute::cast(column.as_ref(), &timestamp_type)?;
            final_columns.push(timestamp_array);
        } else {
            final_columns.push(column.clone());
        }
    }

    let sorted_batch = RecordBatch::try_new(schema.clone(), final_columns)?;
    println!("  Sorted {} rows", sorted_batch.num_rows());

    // Verify it's actually sorted
    println!("Verifying sort order...");
    let time_array = sorted_batch
        .column(time_col_idx)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("time column is not TimestampNanosecondArray");

    let mut is_sorted = true;
    for i in 1..time_array.len() {
        if time_array.value(i) < time_array.value(i - 1) {
            is_sorted = false;
            println!("  WARNING: Sort verification failed at row {}", i);
            break;
        }
    }
    println!("  Sort verified: {}", is_sorted);

    println!("\nOutput schema:");
    for field in schema.fields() {
        println!("  {}: {:?}", field.name(), field.data_type());
    }

    // Write sorted data to output file
    println!("\nWriting sorted data to output file...");
    let output_file = File::create(&output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;

    // Write in chunks to avoid memory issues with very large files
    const WRITE_BATCH_SIZE: usize = 100_000;
    let num_full_batches = sorted_batch.num_rows() / WRITE_BATCH_SIZE;
    let remainder = sorted_batch.num_rows() % WRITE_BATCH_SIZE;

    for batch_idx in 0..num_full_batches {
        let start = batch_idx * WRITE_BATCH_SIZE;
        let end = start + WRITE_BATCH_SIZE;

        let batch_slice = sorted_batch.slice(start, WRITE_BATCH_SIZE);
        writer.write(&batch_slice)?;

        if batch_idx % 10 == 0 {
            println!("  Wrote {} / {} rows", end, sorted_batch.num_rows());
        }
    }

    // Write remainder
    if remainder > 0 {
        let start = num_full_batches * WRITE_BATCH_SIZE;
        let batch_slice = sorted_batch.slice(start, remainder);
        writer.write(&batch_slice)?;
    }

    writer.close()?;

    let elapsed = start_time.elapsed();
    let file_size = std::fs::metadata(&output_path)?.len();

    println!();
    println!("Done!");
    println!("  Output file: {}", output_path.display());
    println!("  Total rows: {}", sorted_batch.num_rows());
    println!(
        "  File size: {:.2} MB",
        file_size as f64 / (1024.0 * 1024.0)
    );
    println!("  Time elapsed: {:.2}s", elapsed.as_secs_f64());

    Ok(())
}
