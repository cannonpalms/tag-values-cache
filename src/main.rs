use std::fs::File;
use std::time::Instant;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tag_values_cache::{
    InteravlCache, IntervalCache, IntervalTreeCache, SortedData, VecCache,
    extract_rows_from_batch,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Tag Values Cache Benchmark ===\n");

    // Use the large clickbench dataset
    let file_path = "test_fixtures/clickbench/hits.parquet";

    println!("Loading parquet file: {}", file_path);

    // Open the parquet file
    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Get metadata about the file
    let metadata = builder.metadata();
    let total_rows = metadata.file_metadata().num_rows();
    println!("Total rows in file: {}", total_rows);

    // Build the reader
    let mut reader = builder.build()?;

    // Process batches until we have 200k rows
    println!("\nReading and processing RecordBatches (limiting to 200k rows)...");
    let start_load = Instant::now();
    let mut all_data = Vec::new();
    let mut batch_count = 0;
    let target_rows = 200_000; // 200k rows total (100k initial + 100k append)

    while let Some(batch_result) = reader.next() {
        if all_data.len() >= target_rows {
            break;
        }

        let batch = batch_result?;
        batch_count += 1;

        if batch_count % 20 == 0 {
            println!(
                "  Processed {} batches ({} rows)...",
                batch_count,
                all_data.len()
            );
        }

        let rows = extract_rows_from_batch(batch);
        all_data.extend(rows);

        // Trim to exactly target_rows if we went over
        if all_data.len() > target_rows {
            all_data.truncate(target_rows);
            break;
        }
    }

    println!(
        "  Processed {} batches ({} rows) in {:?}",
        batch_count,
        all_data.len(),
        start_load.elapsed()
    );

    // Split the data in half
    let mid_point = all_data.len() / 2;
    let (first_half, second_half) = all_data.split_at(mid_point);

    println!("\nData split:");
    println!(
        "  First half: {} rows (for initial build)",
        first_half.len()
    );
    println!("  Second half: {} rows (for append)", second_half.len());

    // Create sorted data for each half
    println!("\nSorting data...");
    let start_sort = Instant::now();
    let sorted_data1 = SortedData::from_unsorted(first_half.to_vec());
    let sorted_data2 = SortedData::from_unsorted(second_half.to_vec());
    println!("  Sorting completed in {:?}", start_sort.elapsed());

    // Benchmark building from first half
    println!("\n=== Build Performance ===");
    println!("Building cache from {} rows...\n", first_half.len());

    // Benchmark IntervalTreeCache
    println!("Building IntervalTreeCache...");
    let start = Instant::now();
    let mut tree_cache = IntervalTreeCache::from_sorted(sorted_data1.clone())?;
    let tree_build_time = start.elapsed();
    println!("  IntervalTreeCache: {:?}", tree_build_time);

    // Benchmark VecCache
    println!("Building VecCache...");
    let start = Instant::now();
    let mut vec_cache = VecCache::from_sorted(sorted_data1.clone())?;
    let vec_build_time = start.elapsed();
    println!("  VecCache: {:?}", vec_build_time);

    // Benchmark InteravlCache
    println!("Building InteravlCache...");
    let start = Instant::now();
    let mut avl_cache = InteravlCache::from_sorted(sorted_data1.clone())?;
    let avl_build_time = start.elapsed();
    println!("  InteravlCache: {:?}", avl_build_time);

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

    // Benchmark append performance
    println!("=== Append Performance ===");
    println!("Appending {} rows...\n", second_half.len());

    // Append to IntervalTreeCache
    println!("Appending to IntervalTreeCache...");
    let start = Instant::now();
    tree_cache.append_sorted(sorted_data2.clone())?;
    let tree_append_time = start.elapsed();
    println!("  IntervalTreeCache: {:?}", tree_append_time);

    // Append to VecCache
    println!("Appending to VecCache...");
    let start = Instant::now();
    vec_cache.append_sorted(sorted_data2.clone())?;
    let vec_append_time = start.elapsed();
    println!("  VecCache: {:?}", vec_append_time);

    // Append to InteravlCache
    println!("Appending to InteravlCache...");
    let start = Instant::now();
    avl_cache.append_sorted(sorted_data2)?;
    let avl_append_time = start.elapsed();
    println!("  InteravlCache: {:?}", avl_append_time);

    // Find fastest append
    let min_append = tree_append_time.min(vec_append_time).min(avl_append_time);
    let fastest_append = if min_append == vec_append_time {
        "VecCache"
    } else if min_append == tree_append_time {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };
    println!("\nFastest append: {}\n", fastest_append);

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
    println!("  IntervalTreeCache: {:?}", tree_point_time);

    let start = Instant::now();
    for &t in &test_points {
        let _ = vec_cache.query_point(t);
    }
    let vec_point_time = start.elapsed();
    println!("  VecCache: {:?}", vec_point_time);

    let start = Instant::now();
    for &t in &test_points {
        let _ = avl_cache.query_point(t);
    }
    let avl_point_time = start.elapsed();
    println!("  InteravlCache: {:?}", avl_point_time);

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
    println!("  IntervalTreeCache: {:?}", tree_range_time);

    let start = Instant::now();
    for range in &test_ranges {
        let _ = vec_cache.query_range(range.clone());
    }
    let vec_range_time = start.elapsed();
    println!("  VecCache: {:?}", vec_range_time);

    let start = Instant::now();
    for range in &test_ranges {
        let _ = avl_cache.query_range(range.clone());
    }
    let avl_range_time = start.elapsed();
    println!("  InteravlCache: {:?}", avl_range_time);

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
    println!("=== Memory Usage ===");
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
        "Dataset: {} total rows processed",
        all_data.len() + second_half.len()
    );
    println!("\nPerformance winners:");
    println!("  Build:        {} ({:?})", fastest_build, min_build);
    println!("  Append:       {} ({:?})", fastest_append, min_append);
    println!("  Point Query:  {} ({:?})", fastest_point, min_point);
    println!("  Range Query:  {} ({:?})", fastest_range, min_range);
    println!(
        "  Memory:       {} ({} MB)",
        lowest_memory,
        min_memory / 1_000_000
    );

    // Calculate total times for overall winner
    let tree_total = tree_build_time + tree_append_time + tree_point_time + tree_range_time;
    let vec_total = vec_build_time + vec_append_time + vec_point_time + vec_range_time;
    let avl_total = avl_build_time + avl_append_time + avl_point_time + avl_range_time;

    println!("\nTotal times:");
    println!("  IntervalTreeCache: {:?}", tree_total);
    println!("  VecCache:          {:?}", vec_total);
    println!("  InteravlCache:     {:?}", avl_total);

    let min_total = tree_total.min(vec_total).min(avl_total);
    let overall_winner = if min_total == vec_total {
        "VecCache"
    } else if min_total == tree_total {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };

    println!(
        "\nOverall winner: {} ({:?} total)",
        overall_winner, min_total
    );

    Ok(())
}

