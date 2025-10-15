use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tag_values_cache::{
    InteravlCache, IntervalCache, IntervalTreeCache, SortedData, VecCache, extract_rows_from_batch, RecordBatchRow,
};

fn print_usage() {
    println!("Usage: {} <parquet_path> [initial_rows] [append_rows]", env::args().next().unwrap_or_else(|| "program".to_string()));
    println!();
    println!("Arguments:");
    println!("  parquet_path  - Path to a parquet file or directory containing parquet files");
    println!("  initial_rows  - Number of rows to load initially (default: all available)");
    println!("  append_rows   - Number of rows to append (default: 0)");
    println!();
    println!("Examples:");
    println!("  # Load all available rows from small files");
    println!("  cargo run --release test_fixtures/influxql_logs/");
    println!();
    println!("  # Load 1M rows initially, no append");
    println!("  cargo run --release test_fixtures/clickbench/hits.parquet 1000000");
    println!();
    println!("  # Load 500k rows initially, then append 500k more");
    println!("  cargo run --release test_fixtures/clickbench/hits.parquet 500000 500000");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let parquet_path = &args[1];

    // Parse optional row count arguments
    const DEFAULT_MAX_ROWS: usize = 1_000_000; // 1M row safety limit when no args provided

    let (initial_rows, append_rows) = if args.len() >= 3 {
        // initial_rows provided
        let initial: usize = args[2].parse()
            .map_err(|_| format!("Invalid initial_rows: {}", args[2]))?;

        let append: usize = if args.len() > 3 {
            args[3].parse()
                .map_err(|_| format!("Invalid append_rows: {}", args[3]))?
        } else {
            0
        };

        (Some(initial), append)
    } else {
        // No row counts provided - will use all available rows up to a reasonable limit
        (None, 0)
    };

    // If initial_rows is specified, calculate total; otherwise use default limit
    let total_rows = initial_rows.map(|i| i + append_rows).or(Some(DEFAULT_MAX_ROWS));

    println!("=== Tag Values Cache Benchmark ===\n");
    println!("Configuration:");
    println!("  Path: {}", parquet_path);
    if let Some(initial) = initial_rows {
        println!("  Initial rows: {}", initial);
        println!("  Append rows: {}", append_rows);
        println!("  Total rows: {}", initial + append_rows);
    } else {
        println!("  Initial rows: all available (up to {} max)", DEFAULT_MAX_ROWS);
        println!("  Append rows: 0");
    }
    println!();

    // Get list of parquet files to process
    let parquet_files = get_parquet_files(parquet_path)?;

    if parquet_files.is_empty() {
        return Err(format!("No parquet files found at: {}", parquet_path).into());
    }

    println!("Found {} parquet file(s) to process", parquet_files.len());

    // Load data from parquet files
    let mut all_data = Vec::new();
    let start_load = Instant::now();
    let mut total_batch_count = 0;

    for file_path in &parquet_files {
        // Check if we've reached our limit (if one was specified)
        if let Some(limit) = total_rows {
            if all_data.len() >= limit {
                break;
            }
        }

        println!("\nLoading parquet file: {}", file_path);

        // Open the parquet file
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Get metadata about the file
        let metadata = builder.metadata();
        let file_rows = metadata.file_metadata().num_rows();
        println!("  Rows in file: {}", file_rows);

        // Build the reader
        let mut reader = builder.build()?;

        // Process batches until we have enough rows
        while let Some(batch_result) = reader.next() {
            // Check if we've reached our limit (if one was specified)
            if let Some(limit) = total_rows {
                if all_data.len() >= limit {
                    break;
                }
            }

            let batch = batch_result?;
            total_batch_count += 1;

            if total_batch_count % 50 == 0 {
                println!(
                    "    Processed {} batches ({} rows)...",
                    total_batch_count,
                    all_data.len()
                );
            }

            let rows = extract_rows_from_batch(batch);
            all_data.extend(rows);

            // Trim to exactly total_rows if we went over
            if let Some(limit) = total_rows {
                if all_data.len() > limit {
                    all_data.truncate(limit);
                    break;
                }
            }
        }
    }

    println!(
        "\nLoaded {} rows in {:?}",
        all_data.len(),
        start_load.elapsed()
    );

    // Filter to only keep the 3 optimal tag columns
    println!("\nFiltering to only 3 tag columns: WithHash, CounterID, CookieEnable");
    let filtered_data: Vec<(u64, RecordBatchRow)> = all_data
        .into_iter()
        .map(|(ts, row)| {
            // Create a new RecordBatchRow with only the 3 columns
            let mut filtered_values = std::collections::BTreeMap::new();

            // Only keep WithHash, CounterID, and CookieEnable
            if let Some(v) = row.values.get("WithHash") {
                filtered_values.insert("WithHash".to_string(), v.clone());
            }
            if let Some(v) = row.values.get("CounterID") {
                filtered_values.insert("CounterID".to_string(), v.clone());
            }
            if let Some(v) = row.values.get("CookieEnable") {
                filtered_values.insert("CookieEnable".to_string(), v.clone());
            }

            (ts, RecordBatchRow::new(filtered_values))
        })
        .collect();

    // Split the data based on configuration
    let (first_half, second_half) = if append_rows > 0 {
        let split_point = initial_rows.unwrap_or(filtered_data.len() / 2).min(filtered_data.len());
        let (first, second) = filtered_data.split_at(split_point);
        println!("\nData split:");
        println!("  Initial: {} rows", first.len());
        println!("  Append:  {} rows", second.len());
        (first, second)
    } else {
        println!("\nUsing all {} rows for initial build (no append)", filtered_data.len());
        (filtered_data.as_slice(), &[][..])
    };

    // Create sorted data for initial build
    println!("\nSorting data...");
    let start_sort = Instant::now();
    let sorted_data1 = SortedData::from_unsorted(first_half.to_vec());
    let sorted_data2 = if append_rows > 0 {
        Some(SortedData::from_unsorted(second_half.to_vec()))
    } else {
        None
    };
    println!("  Sorting completed in {:?}", start_sort.elapsed());

    // Benchmark building from first half
    println!("\n=== Build Performance ===");
    println!("Building cache from {} rows...\n", first_half.len());

    // Benchmark IntervalTreeCache
    println!("Building IntervalTreeCache...");
    let start = Instant::now();
    let mut tree_cache = IntervalTreeCache::from_sorted(sorted_data1.clone())?;
    let tree_build_time = start.elapsed();
    println!("  IntervalTreeCache: {:.2} ms", tree_build_time.as_secs_f64() * 1000.0);

    // Benchmark VecCache
    println!("Building VecCache...");
    let start = Instant::now();
    let mut vec_cache = VecCache::from_sorted(sorted_data1.clone())?;
    let vec_build_time = start.elapsed();
    println!("  VecCache: {:.2} ms", vec_build_time.as_secs_f64() * 1000.0);

    // Benchmark InteravlCache
    println!("Building InteravlCache...");
    let start = Instant::now();
    let mut avl_cache = InteravlCache::from_sorted(sorted_data1.clone())?;
    let avl_build_time = start.elapsed();
    println!("  InteravlCache: {:.2} ms", avl_build_time.as_secs_f64() * 1000.0);

    // Find fastest build
    let min_build = tree_build_time.min(vec_build_time).min(avl_build_time);
    let fastest_build = if min_build == vec_build_time {
        "VecCache"
    } else if min_build == tree_build_time {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };
    println!("\nFastest build: {}\n", fastest_build);

    // Report number of intervals after initial build
    println!("Intervals after initial build ({} data points):", first_half.len());
    println!("  IntervalTreeCache: {} intervals", tree_cache.interval_count());
    println!("  VecCache:          {} intervals", vec_cache.interval_count());
    println!("  InteravlCache:     {} intervals\n", avl_cache.interval_count());

    // Benchmark append performance (if configured)
    let (tree_append_time, vec_append_time, avl_append_time) = if let Some(sorted_data2) = sorted_data2 {
        println!("=== Append Performance ===");
        println!("Appending {} rows...\n", second_half.len());

        // Append to IntervalTreeCache
        println!("Appending to IntervalTreeCache...");
        let start = Instant::now();
        tree_cache.append_sorted(sorted_data2.clone())?;
        let tree_append = start.elapsed();
        println!("  IntervalTreeCache: {:.2} ms", tree_append.as_secs_f64() * 1000.0);

        // Append to VecCache
        println!("Appending to VecCache...");
        let start = Instant::now();
        vec_cache.append_sorted(sorted_data2.clone())?;
        let vec_append = start.elapsed();
        println!("  VecCache: {:.2} ms", vec_append.as_secs_f64() * 1000.0);

        // Append to InteravlCache
        println!("Appending to InteravlCache...");
        let start = Instant::now();
        avl_cache.append_sorted(sorted_data2)?;
        let avl_append = start.elapsed();
        println!("  InteravlCache: {:.2} ms", avl_append.as_secs_f64() * 1000.0);

        (tree_append, vec_append, avl_append)
    } else {
        // No append phase
        (Duration::from_secs(0), Duration::from_secs(0), Duration::from_secs(0))
    };

    // Find fastest append (if append was performed)
    let (min_append, fastest_append) = if append_rows > 0 {
        let min_time = tree_append_time.min(vec_append_time).min(avl_append_time);
        let fastest = if min_time == vec_append_time {
            "VecCache"
        } else if min_time == tree_append_time {
            "IntervalTreeCache"
        } else {
            "InteravlCache"
        };
        println!("\nFastest append: {}\n", fastest);
        (min_time, fastest)
    } else {
        (Duration::from_secs(0), "N/A")
    };

    // Report final number of intervals
    let final_label = if append_rows > 0 { "after append" } else { "final" };
    println!("Intervals {} ({} total data points):", final_label, first_half.len() + second_half.len());
    println!("  IntervalTreeCache: {} intervals", tree_cache.interval_count());
    println!("  VecCache:          {} intervals", vec_cache.interval_count());
    println!("  InteravlCache:     {} intervals", avl_cache.interval_count());

    // Calculate compression ratio
    let total_points = first_half.len() + second_half.len();
    let tree_compression = (total_points as f64) / (tree_cache.interval_count() as f64);
    let vec_compression = (total_points as f64) / (vec_cache.interval_count() as f64);
    let avl_compression = (total_points as f64) / (avl_cache.interval_count() as f64);

    println!("\nCompression ratio (data points / intervals):");
    println!("  IntervalTreeCache: {:.2}x", tree_compression);
    println!("  VecCache:          {:.2}x", vec_compression);
    println!("  InteravlCache:     {:.2}x\n", avl_compression);

    // Generate query test points
    println!("=== Query Performance ===");

    // Get some sample timestamps from the sorted data for realistic queries
    let all_data = sorted_data1.into_inner();
    let sample_size = 1000.min(all_data.len());
    let step = all_data.len() / sample_size;
    let test_points: Vec<u64> = (0..sample_size).map(|i| all_data[i * step].0).collect();

    let min_ts = all_data.first().map(|p| p.0).unwrap_or(0);
    let max_ts = all_data.last().map(|p| p.0).unwrap_or(1000000);
    let range_size = (max_ts - min_ts) / 100; // 100 test ranges
    let test_ranges: Vec<std::ops::Range<u64>> = (0..100)
        .map(|i| {
            let start = min_ts + i * range_size;
            let end = start + range_size;
            start..end
        })
        .collect();

    println!(
        "Running {} point queries and {} range queries...\n",
        test_points.len(),
        test_ranges.len()
    );

    // Benchmark point queries
    println!("Point queries ({} queries):", test_points.len());

    let start = Instant::now();
    for &t in &test_points {
        let _ = tree_cache.query_point(t);
    }
    let tree_point_time = start.elapsed();
    println!("  IntervalTreeCache: {:.2} ms", tree_point_time.as_secs_f64() * 1000.0);

    let start = Instant::now();
    for &t in &test_points {
        let _ = vec_cache.query_point(t);
    }
    let vec_point_time = start.elapsed();
    println!("  VecCache: {:.2} ms", vec_point_time.as_secs_f64() * 1000.0);

    let start = Instant::now();
    for &t in &test_points {
        let _ = avl_cache.query_point(t);
    }
    let avl_point_time = start.elapsed();
    println!("  InteravlCache: {:.2} ms", avl_point_time.as_secs_f64() * 1000.0);

    // Find fastest point query
    let min_point = tree_point_time.min(vec_point_time).min(avl_point_time);
    let fastest_point = if min_point == vec_point_time {
        "VecCache"
    } else if min_point == tree_point_time {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };
    println!("\n  Fastest: {}", fastest_point);

    // Benchmark range queries
    println!("\nRange queries ({} queries):", test_ranges.len());

    let start = Instant::now();
    for range in &test_ranges {
        let _ = tree_cache.query_range(range.clone());
    }
    let tree_range_time = start.elapsed();
    println!("  IntervalTreeCache: {:.2} ms", tree_range_time.as_secs_f64() * 1000.0);

    let start = Instant::now();
    for range in &test_ranges {
        let _ = vec_cache.query_range(range.clone());
    }
    let vec_range_time = start.elapsed();
    println!("  VecCache: {:.2} ms", vec_range_time.as_secs_f64() * 1000.0);

    let start = Instant::now();
    for range in &test_ranges {
        let _ = avl_cache.query_range(range.clone());
    }
    let avl_range_time = start.elapsed();
    println!("  InteravlCache: {:.2} ms", avl_range_time.as_secs_f64() * 1000.0);

    // Find fastest range query
    let min_range = tree_range_time.min(vec_range_time).min(avl_range_time);
    let fastest_range = if min_range == vec_range_time {
        "VecCache"
    } else if min_range == tree_range_time {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };
    println!("\n  Fastest: {}", fastest_range);

    // Measure memory usage
    println!("\n=== Memory Usage ===");
    println!("Measuring actual memory usage for each cache implementation...\n");

    // Get actual memory usage from the size_bytes() method
    let tree_memory = tree_cache.size_bytes();
    let vec_memory = vec_cache.size_bytes();
    let avl_memory = avl_cache.size_bytes();

    println!(
        "  IntervalTreeCache: {} MB ({} bytes)",
        tree_memory / 1_000_000,
        tree_memory
    );
    println!(
        "  VecCache:          {} MB ({} bytes)",
        vec_memory / 1_000_000,
        vec_memory
    );
    println!(
        "  InteravlCache:     {} MB ({} bytes)",
        avl_memory / 1_000_000,
        avl_memory
    );

    let min_memory = tree_memory.min(vec_memory).min(avl_memory);
    let lowest_memory = if min_memory == vec_memory {
        "VecCache"
    } else if min_memory == tree_memory {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };
    println!("\nLowest memory usage: {}\n", lowest_memory);

    // Summary
    println!("\n=== Summary ===");
    println!(
        "Dataset: {} total rows processed (with 3 tag columns only)",
        first_half.len() + second_half.len()
    );
    println!("\nPerformance winners:");
    println!("  Build:        {} ({:.2} ms)", fastest_build, min_build.as_secs_f64() * 1000.0);
    if append_rows > 0 {
        println!("  Append:       {} ({:.2} ms)", fastest_append, min_append.as_secs_f64() * 1000.0);
    }
    println!("  Point Query:  {} ({:.2} ms)", fastest_point, min_point.as_secs_f64() * 1000.0);
    println!("  Range Query:  {} ({:.2} ms)", fastest_range, min_range.as_secs_f64() * 1000.0);
    println!(
        "  Memory:       {} ({} MB)",
        lowest_memory,
        min_memory / 1_000_000
    );

    Ok(())
}

/// Get list of parquet files from a path (either a single file or directory)
fn get_parquet_files(path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let path = Path::new(path);

    if path.is_file() {
        // Single file - check if it's a parquet file
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            Ok(vec![path.to_string_lossy().to_string()])
        } else {
            Err(format!("File is not a parquet file: {}", path.display()).into())
        }
    } else if path.is_dir() {
        // Directory - find all parquet files
        let mut parquet_files = Vec::new();

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                parquet_files.push(path.to_string_lossy().to_string());
            }
        }

        parquet_files.sort(); // Sort for consistent ordering
        Ok(parquet_files)
    } else {
        Err(format!("Path does not exist: {}", path.display()).into())
    }
}
