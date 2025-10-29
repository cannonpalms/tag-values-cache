use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use tokio::fs::File;

fn factorize_cardinality(total_cardinality: usize, num_columns: usize) -> Vec<usize> {
    if total_cardinality == 1 {
        return vec![1; num_columns];
    }

    let base = (total_cardinality as f64)
        .powf(1.0 / num_columns as f64)
        .round() as usize;
    let base = base.max(2);

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

    let start_ns: i64 = 1704067200_000_000_000;
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

    // Adaptive chunk sizing
    let tagsets_per_chunk = if cardinality >= 500_000 {
        50
    } else if cardinality >= 50_000 {
        200
    } else {
        1000
    };

    // Optimal parameters for 24-core system
    const ROWS_PER_BATCH: usize = 500_000; // Larger batches = better Arrow performance
    const CHANNEL_CAPACITY: usize = 48; // Just enough to keep pipeline full (2x producer count)
    const NUM_PRODUCERS: usize = 22; // Use 22 of 24 cores (leave 2 for writer + system)
    const NUM_WRITERS: usize = 2; // Only 1-2 since writing is serialized by mutex

    println!("  MPMC Architecture:");
    println!("    - {} producer threads generating data", NUM_PRODUCERS);
    println!("    - {} writer threads writing to parquet", NUM_WRITERS);
    println!("    - Channel capacity: {} batches", CHANNEL_CAPACITY);
    println!("    - Chunk size: {} tagsets", tagsets_per_chunk);

    // Create bounded MPMC channel using crossbeam
    let (tx, rx) = crossbeam_channel::bounded::<Option<RecordBatch>>(CHANNEL_CAPACITY);

    // Shared progress counter
    let progress = Arc::new(Mutex::new(0usize));
    let total_written = Arc::new(Mutex::new(0usize));

    // Clone for producers
    let schema_prod = schema.clone();
    let progress_prod = progress.clone();

    // Spawn producer threads
    let producer_handles: Vec<_> = (0..NUM_PRODUCERS)
        .map(|producer_id| {
            let tx = tx.clone();
            let schema = schema_prod.clone();
            let progress = progress_prod.clone();
            let tag_cardinalities = tag_cardinalities.clone();

            thread::spawn(move || {
                // Each producer handles a portion of the tagsets
                let chunk_size = tagsets_per_chunk;
                let producer_start = producer_id * chunk_size;

                // Process tagsets assigned to this producer
                for combo_base in (producer_start..cardinality).step_by(NUM_PRODUCERS * chunk_size)
                {
                    let combo_end = (combo_base + chunk_size).min(cardinality);

                    // Progress reporting
                    if combo_base % 10000 == 0 {
                        let mut prog = progress.lock().unwrap();
                        *prog = combo_base;
                        if combo_base > 0 {
                            println!(
                                "  Progress: {}/{} tagsets ({}%)",
                                combo_base,
                                cardinality,
                                100 * combo_base / cardinality
                            );
                        }
                    }

                    let mut timestamps = Vec::with_capacity(ROWS_PER_BATCH);
                    let mut tag_columns: Vec<Vec<Option<String>>> =
                        (0..8).map(|_| Vec::with_capacity(ROWS_PER_BATCH)).collect();

                    for combo_idx in combo_base..combo_end {
                        // Generate tag values on-the-fly
                        let tag_values = decode_tagset(combo_idx, &tag_cardinalities);

                        // Generate all points for this tagset
                        for point_idx in 0..points_per_tagset {
                            let timestamp = start_ns + point_idx as i64 * interval_ns;
                            timestamps.push(timestamp);

                            for (col_idx, tag_val) in tag_values.iter().enumerate() {
                                tag_columns[col_idx].push(Some(tag_val.clone()));
                            }

                            // Create and send batch when full
                            if timestamps.len() >= ROWS_PER_BATCH {
                                // Move data instead of cloning by swapping with empty vecs
                                let batch_timestamps = std::mem::replace(
                                    &mut timestamps,
                                    Vec::with_capacity(ROWS_PER_BATCH),
                                );
                                let batch_tag_columns = std::mem::replace(
                                    &mut tag_columns,
                                    (0..8).map(|_| Vec::with_capacity(ROWS_PER_BATCH)).collect(),
                                );

                                let time_array =
                                    Arc::new(Int64Array::from(batch_timestamps)) as ArrayRef;
                                let tag_arrays: Vec<ArrayRef> = batch_tag_columns
                                    .into_iter()
                                    .map(|col| Arc::new(StringArray::from(col)) as ArrayRef)
                                    .collect();

                                let mut arrays = vec![time_array];
                                arrays.extend(tag_arrays);

                                let batch = RecordBatch::try_new(schema.clone(), arrays)
                                    .expect("Failed to create batch");

                                // Send batch through channel (blocks if channel is full)
                                if tx.send(Some(batch)).is_err() {
                                    return; // Channel closed, stop producing
                                }
                            }
                        }
                    }

                    // Send final partial batch if any
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

                        let _ = tx.send(Some(batch));
                    }
                }
            })
        })
        .collect();

    // Drop the original sender so channel closes when producers finish
    drop(tx);

    // Create tokio runtime for async I/O
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(NUM_WRITERS)
        .enable_all()
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // Use runtime to create async writer
    let writer = runtime.block_on(async {
        let file = File::create(&output_path).await?;
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .set_dictionary_enabled(true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .build();

        AsyncArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    })?;

    let writer = Arc::new(Mutex::new(Some(writer)));

    // Spawn async writer tasks
    let writer_handles: Vec<_> = (0..NUM_WRITERS)
        .map(|_writer_id| {
            let rx = rx.clone();
            let writer = writer.clone();
            let total_written = total_written.clone();
            let runtime_handle = runtime.handle().clone();

            thread::spawn(move || {
                // Run async writer in tokio context
                runtime_handle.block_on(async {
                    // Each writer consumes batches from the channel
                    while let Ok(Some(batch)) = rx.recv() {
                        let num_rows = batch.num_rows();

                        // Write batch asynchronously
                        {
                            let mut w_guard = writer.lock().unwrap();
                            if let Some(ref mut w) = *w_guard {
                                w.write(&batch).await.expect("Failed to write batch");
                            }
                        }

                        // Update total written
                        {
                            let mut total = total_written.lock().unwrap();
                            *total += num_rows;
                        }
                    }
                });
            })
        })
        .collect();

    // Wait for all producers to finish
    for handle in producer_handles {
        handle.join().expect("Producer thread panicked");
    }

    // Signal writers to stop by closing channel (already closed by dropping tx)

    // Wait for all writers to finish
    for handle in writer_handles {
        handle.join().expect("Writer thread panicked");
    }

    // Close the writer asynchronously
    let mut w_guard = writer.lock().unwrap();
    if let Some(w) = w_guard.take() {
        drop(w_guard); // Release lock before async operation
        runtime.block_on(async {
            w.close()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        })?;
    }

    let file_size = std::fs::metadata(&output_path)?.len();
    let elapsed = start_time.elapsed();
    let total_rows = *total_written.lock().unwrap();

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
    println!("MPMC PRODUCER-CONSUMER CARDINALITY DATA GENERATOR");
    println!("====================================================================");
    println!("\nParameters:");
    println!("  - Time span: 24 hours");
    println!("  - Interval: 15 seconds");
    println!("  - Points per tagset: 5,760");
    println!("  - Tag columns: 8");
    println!("  - Architecture: MPMC channel (producer-consumer)");
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
