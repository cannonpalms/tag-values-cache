use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn factorize_cardinality(total_cardinality: usize, num_columns: usize) -> Vec<usize> {
    if total_cardinality == 1 {
        return vec![1; num_columns];
    }

    let _base = (total_cardinality as f64)
        .powf(1.0 / num_columns as f64)
        .round() as usize;
    let _base = _base.max(2);

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
        if remaining % target != 0 {
            for candidate in (2..=target + 5).rev() {
                if candidate <= remaining && remaining % candidate == 0 {
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

// Use Arc<str> for interned strings
fn generate_tag_values(cardinality: usize) -> Vec<Arc<str>> {
    if cardinality == 1 {
        return vec![Arc::from("a")];
    }

    (0..cardinality)
        .map(|i| {
            let s = if i < 26 {
                ((b'a' + i as u8) as char).to_string()
            } else if i < 26 + 26 * 26 {
                let idx = i - 26;
                let first = (b'a' + (idx / 26) as u8) as char;
                let second = (b'a' + (idx % 26) as u8) as char;
                format!("{}{}", first, second)
            } else {
                format!("t{}", i)
            };
            Arc::from(s.as_str())
        })
        .collect()
}

fn generate_parquet_file(cardinality: usize, output_path: PathBuf) -> std::io::Result<()> {
    println!("\nGenerating {} cardinality file...", cardinality);
    let start_time = Instant::now();

    // Calculate tag column cardinalities
    let tag_cardinalities = factorize_cardinality(cardinality, 8);
    let product: usize = tag_cardinalities.iter().product();
    println!(
        "  Tag cardinalities: {:?} (product={})",
        tag_cardinalities, product
    );

    // Generate interned tag values for each column
    let tag_values_per_column: Vec<Vec<Arc<str>>> = tag_cardinalities
        .iter()
        .map(|&card| generate_tag_values(card))
        .collect();

    // Time parameters
    let start_ns: i64 = 1704067200_000_000_000; // 2024-01-01 00:00:00 UTC
    let interval_ns: i64 = 15_000_000_000; // 15 seconds
    let points_per_tagset = 5760; // 24 hours * 3600 / 15

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

    // Create parquet writer wrapped in Arc<Mutex> for thread-safe sequential writing
    let file = File::create(&output_path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let writer = Arc::new(Mutex::new(writer));
    let rows_written = Arc::new(Mutex::new(0usize));

    // Process tagsets in parallel chunks, generate and write batches
    // Adjust chunk size based on cardinality to avoid OOM
    let tagsets_per_chunk = if cardinality >= 500_000 {
        50 // Very small chunks for 1M+
    } else if cardinality >= 50_000 {
        200 // Small chunks for 100K
    } else {
        1000 // Normal chunks for smaller cardinalities
    };
    const ROWS_PER_BATCH: usize = 100_000;

    println!("  Streaming data generation with parallel processing...");
    println!("  Using chunk size: {} tagsets", tagsets_per_chunk);

    let num_chunks = (cardinality + tagsets_per_chunk - 1) / tagsets_per_chunk;

    // Process chunks in batches to control memory usage
    // Process up to 24 chunks in parallel at a time
    for chunk_batch in (0..num_chunks).collect::<Vec<_>>().chunks(24) {
        // Process this batch of chunks in parallel
        let results: Vec<(usize, Vec<RecordBatch>)> = chunk_batch
            .into_par_iter()
            .map(|&chunk_idx| {
                let chunk_start = chunk_idx * tagsets_per_chunk;
                let chunk_end = (chunk_start + tagsets_per_chunk).min(cardinality);

                // Generate batch for this chunk
                let mut timestamps = Vec::with_capacity(ROWS_PER_BATCH);
                let mut tag_columns: Vec<Vec<Option<String>>> =
                    (0..8).map(|_| Vec::with_capacity(ROWS_PER_BATCH)).collect();

                let mut batches = Vec::new();

                for combo_idx in chunk_start..chunk_end {
                    // Decode combo_idx to tag values on-the-fly
                    let mut remaining = combo_idx;
                    let mut tag_indices = Vec::with_capacity(8);
                    for &card in tag_cardinalities.iter().rev() {
                        tag_indices.push(remaining % card);
                        remaining /= card;
                    }
                    tag_indices.reverse();

                    // Get tag values using Arc<str> (cheap clones)
                    let tag_vals: Vec<Arc<str>> = tag_indices
                        .iter()
                        .enumerate()
                        .map(|(i, &idx)| tag_values_per_column[i][idx].clone())
                        .collect();

                    // Generate points for this tagset
                    for point_idx in 0..points_per_tagset {
                        let timestamp = start_ns + point_idx as i64 * interval_ns;
                        timestamps.push(timestamp);

                        for (col_idx, tag_val) in tag_vals.iter().enumerate() {
                            tag_columns[col_idx].push(Some(tag_val.to_string()));
                        }

                        // When batch is full, save it
                        if timestamps.len() >= ROWS_PER_BATCH {
                            // Create arrays
                            let time_array =
                                Arc::new(Int64Array::from(timestamps.clone())) as ArrayRef;
                            let tag_arrays: Vec<ArrayRef> = tag_columns
                                .iter()
                                .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
                                .collect();

                            let mut arrays = vec![time_array];
                            arrays.extend(tag_arrays);

                            let batch = RecordBatch::try_new(schema.clone(), arrays)
                                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                                .unwrap();

                            batches.push(batch);

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
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                        .unwrap();

                    batches.push(batch);
                }

                (chunk_idx, batches)
            })
            .collect();

        // Write batches sequentially (parquet writer isn't thread-safe)
        for (chunk_idx, batches) in results {
            for batch in batches {
                let num_rows = batch.num_rows();

                let mut w = writer.lock().unwrap();
                w.write(&batch).unwrap();
                drop(w);

                let mut count = rows_written.lock().unwrap();
                *count += num_rows;

                // Progress reporting
                if chunk_idx % 10 == 0 && chunk_idx > 0 {
                    println!("  Progress: {} rows written", *count);
                }
            }
        }
    }

    // Close writer - need to extract it from the Arc<Mutex> since close() consumes it
    let writer = Arc::try_unwrap(writer)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to unwrap writer"))?
        .into_inner()
        .unwrap();
    writer
        .close()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let file_size = std::fs::metadata(&output_path)?.len();
    let elapsed = start_time.elapsed();
    let total_rows = *rows_written.lock().unwrap();

    println!("  Generated {} rows", total_rows);
    println!(
        "  File size: {:.2} MB ({} bytes)",
        file_size as f64 / (1024.0 * 1024.0),
        file_size
    );
    println!("  Time: {:.2}s", elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.0} rows/sec",
        total_rows as f64 / elapsed.as_secs_f64()
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
    println!("GENERATING CARDINALITY TEST DATA (STREAMING VERSION)");
    println!("====================================================================");
    println!("\nParameters:");
    println!("  - Time span: 24 hours");
    println!("  - Interval: 15 seconds");
    println!("  - Points per tagset: 5,760");
    println!("  - Tag columns: 8");
    println!("  - Parallel processing: 24 threads");
    println!("  - Streaming: YES (on-the-fly generation)");
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
