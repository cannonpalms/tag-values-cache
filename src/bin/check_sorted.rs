use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::env;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let path = if args.len() > 1 {
        &args[1]
    } else {
        "benches/data/by-cardinality/1K.parquet"
    };

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema();

    // Check the time column type and clone it for later use
    let time_field = schema.field_with_name("time")?;
    let time_data_type = time_field.data_type().clone();
    println!("Time column type: {:?}", time_data_type);

    let mut reader = builder.build()?;

    let mut prev_timestamp: Option<i64> = None;
    let mut is_sorted = true;
    let mut first_violation_idx = None;
    let mut row_idx = 0;
    let mut first_ts = None;
    let mut last_ts = None;

    while let Some(Ok(batch)) = reader.next() {
        let time_col = batch.column_by_name("time").expect("time column not found");

        // Handle both Int64 and Timestamp types
        let timestamps: Vec<i64> = match &time_data_type {
            DataType::Int64 => {
                let time_array = time_col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("time column is not Int64Array");
                time_array.values().to_vec()
            }
            DataType::Timestamp(_, _) => {
                let time_array = time_col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("time column is not TimestampNanosecondArray");
                time_array.values().to_vec()
            }
            _ => panic!("Unexpected time column type: {:?}", time_data_type),
        };

        for timestamp in timestamps {
            if first_ts.is_none() {
                first_ts = Some(timestamp);
            }
            last_ts = Some(timestamp);

            if let Some(prev) = prev_timestamp
                && timestamp < prev
            {
                is_sorted = false;
                if first_violation_idx.is_none() {
                    first_violation_idx = Some(row_idx);
                    println!(
                        "First violation at row {}: prev={}, current={}",
                        row_idx, prev, timestamp
                    );
                    println!("  Difference: {} ns", prev - timestamp);
                }
                break;
            }

            prev_timestamp = Some(timestamp);
            row_idx += 1;
        }
    }

    println!("\nFile: {}", path);
    println!("Total rows: {}", row_idx);
    println!("First timestamp: {:?}", first_ts);
    println!("Last timestamp: {:?}", last_ts);
    println!("Is sorted (ascending): {}", is_sorted);

    Ok(())
}
