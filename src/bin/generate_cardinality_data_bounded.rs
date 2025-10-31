use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
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

// Generate tag value for a specific index
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

// Decode a tagset index to its tag values
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

    // Adaptive chunk sizing based on cardinality
    let tagsets_per_chunk = if cardinality >= 500_000 {
        50 // Small chunks for 1M+
    } else if cardinality >= 50_000 {
        200 // Medium chunks for 100K
    } else {
        1000 // Large chunks for smaller cardinalities
    };

    const ROWS_PER_BATCH: usize = 100_000;
    const MAX_PARALLEL_CHUNKS: usize = 24; // Limit parallelism

    let mut total_rows_written = 0;

    println!("  Bounded parallel generation...");
    println!("  Chunk size: {} tagsets", tagsets_per_chunk);
    println!("  Max parallel chunks: {}", MAX_PARALLEL_CHUNKS);

    // Process tagsets in bounded parallel chunks
    for chunk_batch_start in (0..cardinality).step_by(tagsets_per_chunk * MAX_PARALLEL_CHUNKS) {
        let chunk_batch_end =
            (chunk_batch_start + tagsets_per_chunk * MAX_PARALLEL_CHUNKS).min(cardinality);

        // Progress reporting
        if chunk_batch_start > 0 && chunk_batch_start % 10000 == 0 {
            println!(
                "  Progress: {}/{} tagsets ({}%)",
                chunk_batch_start,
                cardinality,
                100 * chunk_batch_start / cardinality
            );
        }

        // Generate batches for this chunk batch in parallel
        let chunk_indices: Vec<usize> = (chunk_batch_start..chunk_batch_end)
            .step_by(tagsets_per_chunk)
            .collect();

        let batches: Vec<RecordBatch> = chunk_indices
            .into_par_iter()
            .flat_map(|chunk_start| {
                let chunk_end = (chunk_start + tagsets_per_chunk).min(cardinality);
                let mut chunk_batches = Vec::new();

                let mut timestamps = Vec::with_capacity(ROWS_PER_BATCH);
                let mut tag_columns: Vec<Vec<Option<String>>> =
                    (0..8).map(|_| Vec::with_capacity(ROWS_PER_BATCH)).collect();

                for combo_idx in chunk_start..chunk_end {
                    // Generate tag values on-the-fly
                    let tag_values = decode_tagset(combo_idx, &tag_cardinalities);

                    // Generate all points for this tagset
                    for point_idx in 0..points_per_tagset {
                        let timestamp = start_ns + point_idx as i64 * interval_ns;
                        timestamps.push(timestamp);

                        for (col_idx, tag_val) in tag_values.iter().enumerate() {
                            tag_columns[col_idx].push(Some(tag_val.clone()));
                        }

                        // Create batch when full
                        if timestamps.len() >= ROWS_PER_BATCH {
                            let time_array =
                                Arc::new(Int64Array::from(timestamps.clone())) as ArrayRef;
                            let tag_arrays: Vec<ArrayRef> = tag_columns
                                .iter()
                                .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
                                .collect();

                            let mut arrays = vec![time_array];
                            arrays.extend(tag_arrays);

                            let batch = RecordBatch::try_new(schema.clone(), arrays)
                                .expect("Failed to create batch");

                            chunk_batches.push(batch);

                            // Clear for next batch
                            timestamps.clear();
                            for col in tag_columns.iter_mut() {
                                col.clear();
                            }
                        }
                    }
                }

                // Save final partial batch if any
                if !timestamps.is_empty() {
                    let time_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
                    let tag_arrays: Vec<ArrayRef> = tag_columns
                        .into_iter()
                        .map(|col| Arc::new(StringArray::from(col)) as ArrayRef)
                        .collect();

                    let mut arrays = vec![time_array];
                    arrays.extend(tag_arrays);

                    let batch = RecordBatch::try_new(schema.clone(), arrays)
                        .expect("Failed to create batch");

                    chunk_batches.push(batch);
                }

                chunk_batches
            })
            .collect();

        // Write all batches from this chunk batch
        for batch in batches {
            total_rows_written += batch.num_rows();
            writer.write(&batch).map_err(std::io::Error::other)?;
        }
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
    // Set rayon thread pool to 24 threads
    rayon::ThreadPoolBuilder::new()
        .num_threads(24)
        .build_global()
        .expect("Failed to configure rayon thread pool");

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
    println!("BOUNDED PARALLEL CARDINALITY DATA GENERATOR");
    println!("====================================================================");
    println!("\nParameters:");
    println!("  - Time span: 24 hours");
    println!("  - Interval: 15 seconds");
    println!("  - Points per tagset: 5,760");
    println!("  - Tag columns: 8");
    println!("  - Parallel threads: 24");
    println!("  - Memory control: Bounded parallelism (max 24 chunks)");
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
