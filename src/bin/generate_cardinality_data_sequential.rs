use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

fn factorize_cardinality(total_cardinality: usize, num_columns: usize) -> Vec<usize> {
    if total_cardinality == 1 {
        return vec![1; num_columns];
    }

    let mut cardinalities = Vec::new();
    let mut remaining = total_cardinality;

    for _ in 0..num_columns - 1 {
        if remaining <= 1 {
            cardinalities.push(1);
            continue;
        }

        let target = (remaining as f64)
            .powf(1.0 / (num_columns - cardinalities.len()) as f64)
            .round() as usize;
        let target = target.max(2);

        let mut best = target;
        if !remaining.is_multiple_of(target) {
            for candidate in (2..=target + 5).rev() {
                if candidate <= remaining && remaining.is_multiple_of(candidate) {
                    best = candidate;
                    break;
                }
            }
        }
        cardinalities.push(best);
        remaining /= best;
    }

    cardinalities.push(remaining);
    cardinalities
}

// Generate tag value for a specific index - no pre-allocation
fn get_tag_value(index: usize) -> String {
    if index == 0 {
        return "a".to_string();
    }

    if index < 26 {
        ((b'a' + index as u8) as char).to_string()
    } else if index < 26 + 26 * 26 {
        let idx = index - 26;
        let first = (b'a' + (idx / 26) as u8) as char;
        let second = (b'a' + (idx % 26) as u8) as char;
        format!("{}{}", first, second)
    } else {
        format!("t{}", index)
    }
}

// Decode a tagset index to its tag values on-the-fly
fn decode_tagset(combo_idx: usize, tag_cardinalities: &[usize]) -> Vec<String> {
    let mut remaining = combo_idx;
    let mut tag_indices = Vec::with_capacity(8);

    for &card in tag_cardinalities.iter().rev() {
        tag_indices.push(remaining % card);
        remaining /= card;
    }
    tag_indices.reverse();

    tag_indices.into_iter().map(get_tag_value).collect()
}

fn generate_parquet_file(cardinality: usize, output_path: PathBuf) -> std::io::Result<()> {
    println!("\nGenerating {} cardinality file...", cardinality);
    let start_time = Instant::now();

    let tag_cardinalities = factorize_cardinality(cardinality, 8);
    let product: usize = tag_cardinalities.iter().product();
    println!(
        "  Tag cardinalities: {:?} (product={})",
        tag_cardinalities, product
    );

    let start_ns: i64 = 1704067200000000000;
    let interval_ns: i64 = 15_000_000_000;
    let points_per_tagset = 5760;

    println!("  Time points per tagset: {}", points_per_tagset);
    println!("  Total rows: {}", cardinality * points_per_tagset);

    // Create schema
    let mut metadata = HashMap::new();
    metadata.insert(
        "iox::column::type".to_string(),
        "iox::column_type::timestamp".to_string(),
    );
    let time_field = Field::new("time", DataType::Int64, false).with_metadata(metadata);

    let mut tag_metadata = HashMap::new();
    tag_metadata.insert(
        "iox::column::type".to_string(),
        "iox::column_type::tag".to_string(),
    );

    let tag_fields: Vec<Field> = (0..8)
        .map(|i| {
            Field::new(format!("t{}", i), DataType::Utf8, true).with_metadata(tag_metadata.clone())
        })
        .collect();

    let mut fields = vec![time_field];
    fields.extend(tag_fields);
    let schema = Arc::new(Schema::new(fields));

    // Create parquet writer
    let file = File::create(&output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(std::io::Error::other)?;

    const ROWS_PER_BATCH: usize = 100_000;
    let mut total_rows_written = 0;

    println!("  True streaming generation (no pre-computation)...");

    // Process tagsets one at a time to minimize memory usage
    // We'll batch rows for writing efficiency, but generate on-demand
    let mut timestamps = Vec::with_capacity(ROWS_PER_BATCH);
    let mut tag_columns: Vec<Vec<Option<String>>> =
        (0..8).map(|_| Vec::with_capacity(ROWS_PER_BATCH)).collect();

    for combo_idx in 0..cardinality {
        // Progress reporting
        if combo_idx > 0 && combo_idx % 10000 == 0 {
            println!(
                "  Progress: {}/{} tagsets ({}%)",
                combo_idx,
                cardinality,
                100 * combo_idx / cardinality
            );
        }

        // Generate tag values on-the-fly for this tagset
        let tag_values = decode_tagset(combo_idx, &tag_cardinalities);

        // Generate all points for this tagset
        for point_idx in 0..points_per_tagset {
            let timestamp = start_ns + point_idx as i64 * interval_ns;
            timestamps.push(timestamp);

            for (col_idx, tag_val) in tag_values.iter().enumerate() {
                tag_columns[col_idx].push(Some(tag_val.clone()));
            }

            // Write batch when full
            if timestamps.len() >= ROWS_PER_BATCH {
                write_batch(&mut writer, &schema, &timestamps, &tag_columns)?;
                total_rows_written += timestamps.len();

                // Clear for next batch
                timestamps.clear();
                for col in tag_columns.iter_mut() {
                    col.clear();
                }
            }
        }
    }

    // Write any remaining data
    if !timestamps.is_empty() {
        write_batch(&mut writer, &schema, &timestamps, &tag_columns)?;
        total_rows_written += timestamps.len();
    }

    writer.close().map_err(std::io::Error::other)?;

    let file_size = std::fs::metadata(&output_path)?.len();
    let elapsed = start_time.elapsed();

    println!("  Generated {} rows", total_rows_written);
    println!(
        "  File size: {:.2} MB ({} bytes)",
        file_size as f64 / (1024.0 * 1024.0),
        file_size
    );
    println!("  Time: {:.2}s", elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.0} rows/sec",
        total_rows_written as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

fn write_batch(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    timestamps: &[i64],
    tag_columns: &[Vec<Option<String>>],
) -> std::io::Result<()> {
    let time_array = Arc::new(Int64Array::from(timestamps.to_vec())) as ArrayRef;

    let tag_arrays: Vec<ArrayRef> = tag_columns
        .iter()
        .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
        .collect();

    let mut arrays = vec![time_array];
    arrays.extend(tag_arrays);

    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(std::io::Error::other)?;

    writer.write(&batch).map_err(std::io::Error::other)?;

    Ok(())
}

fn parse_cardinality(s: &str) -> usize {
    let s = s.to_uppercase();
    if s.ends_with('K') {
        (s[..s.len() - 1].parse::<f64>().unwrap() * 1000.0) as usize
    } else if s.ends_with('M') {
        (s[..s.len() - 1].parse::<f64>().unwrap() * 1_000_000.0) as usize
    } else {
        s.parse().unwrap()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let cardinalities = if args.len() > 1 {
        args[1..]
            .iter()
            .map(|s| parse_cardinality(s))
            .collect::<Vec<_>>()
    } else {
        vec![1_000, 10_000, 100_000, 1_000_000]
    };

    let output_dir = PathBuf::from("benches/data/by-cardinality");
    std::fs::create_dir_all(&output_dir).expect("Failed to create output directory");

    println!("====================================================================");
    println!("SEQUENTIAL STREAMING CARDINALITY DATA GENERATOR");
    println!("====================================================================");
    println!("\nParameters:");
    println!("  - Time span: 24 hours");
    println!("  - Interval: 15 seconds");
    println!("  - Points per tagset: 5,760");
    println!("  - Tag columns: 8");
    println!("  - Memory usage: MINIMAL (true streaming)");
    println!("  - Processing: Sequential (low memory, slower)");
    println!();

    for card in cardinalities {
        let card_str = if card < 1_000_000 {
            format!("{}K", card / 1000)
        } else {
            format!("{}M", card / 1_000_000)
        };
        let output_path = output_dir.join(format!("{}.parquet", card_str));

        match generate_parquet_file(card, output_path) {
            Ok(_) => {}
            Err(e) => eprintln!("  ERROR: {}", e),
        }
    }

    println!("\n====================================================================");
    println!("GENERATION COMPLETE");
    println!("====================================================================");
}
