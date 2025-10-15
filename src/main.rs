use std::fs::File;
use std::time::Instant;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tag_values_cache::{
    record_batches_to_row_data, InteravlCache, IntervalCache, IntervalTreeCache, VecCache,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Tag Values Cache Benchmark ===\n");

    // Load the three parquet files
    let file1_path = "test_fixtures/influxql_logs/influxql_log_1.parquet";
    let file2_path = "test_fixtures/influxql_logs/influxql_log_2.parquet";
    let file3_path = "test_fixtures/influxql_logs/influxql_log_3.parquet";

    println!("Loading parquet files...");

    // Load first file for building
    let file1 = File::open(file1_path)?;
    let builder1 = ParquetRecordBatchReaderBuilder::try_new(file1)?;
    let reader1 = builder1.build()?;
    let sorted_data1 = record_batches_to_row_data(reader1)?;
    let num_points1 = sorted_data1.clone().into_inner().len();
    println!("  File 1: {} data points", num_points1);

    // Load second file for appending
    let file2 = File::open(file2_path)?;
    let builder2 = ParquetRecordBatchReaderBuilder::try_new(file2)?;
    let reader2 = builder2.build()?;
    let sorted_data2 = record_batches_to_row_data(reader2)?;
    let num_points2 = sorted_data2.clone().into_inner().len();
    println!("  File 2: {} data points", num_points2);

    // Load third file for appending
    let file3 = File::open(file3_path)?;
    let builder3 = ParquetRecordBatchReaderBuilder::try_new(file3)?;
    let reader3 = builder3.build()?;
    let sorted_data3 = record_batches_to_row_data(reader3)?;
    let num_points3 = sorted_data3.clone().into_inner().len();
    println!("  File 3: {} data points", num_points3);

    println!("  Total: {} data points\n", num_points1 + num_points2 + num_points3);

    // Show a sample of the data structure
    println!("Sample data point:");
    if let Some((ts, row)) = sorted_data1.clone().into_inner().first() {
        println!("  Timestamp: {}", ts);
        println!("  Columns: {:?}", row.values.keys().collect::<Vec<_>>());
        println!("  Values: {}\n", row);
    }

    // Benchmark building from first file
    println!("=== Build Performance ===");
    println!("Building cache from {} points...\n", num_points1);

    // Benchmark IntervalTreeCache
    let start = Instant::now();
    let mut tree_cache = IntervalTreeCache::from_sorted(sorted_data1.clone())?;
    let tree_build_time = start.elapsed();
    println!("IntervalTreeCache: {:?}", tree_build_time);

    // Benchmark VecCache
    let start = Instant::now();
    let mut vec_cache = VecCache::from_sorted(sorted_data1.clone())?;
    let vec_build_time = start.elapsed();
    println!("VecCache:          {:?}", vec_build_time);

    // Benchmark InteravlCache
    let start = Instant::now();
    let mut avl_cache = InteravlCache::from_sorted(sorted_data1.clone())?;
    let avl_build_time = start.elapsed();
    println!("InteravlCache:     {:?}", avl_build_time);

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
    println!("Appending {} + {} points...\n", num_points2, num_points3);

    // Append to IntervalTreeCache
    let start = Instant::now();
    tree_cache.append_sorted(sorted_data2.clone())?;
    tree_cache.append_sorted(sorted_data3.clone())?;
    let tree_append_time = start.elapsed();
    println!("IntervalTreeCache: {:?}", tree_append_time);

    // Append to VecCache
    let start = Instant::now();
    vec_cache.append_sorted(sorted_data2.clone())?;
    vec_cache.append_sorted(sorted_data3.clone())?;
    let vec_append_time = start.elapsed();
    println!("VecCache:          {:?}", vec_append_time);

    // Append to InteravlCache
    let start = Instant::now();
    avl_cache.append_sorted(sorted_data2.clone())?;
    avl_cache.append_sorted(sorted_data3)?;
    let avl_append_time = start.elapsed();
    println!("InteravlCache:     {:?}", avl_append_time);

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

    // Get some sample timestamps from the data for realistic queries
    let all_data = sorted_data1.into_inner();
    let sample_size = 1000.min(all_data.len());
    let step = all_data.len() / sample_size;
    let test_points: Vec<u64> = (0..sample_size)
        .map(|i| all_data[i * step].0)
        .collect();

    let min_ts = all_data.first().map(|p| p.0).unwrap_or(0);
    let max_ts = all_data.last().map(|p| p.0).unwrap_or(1000000);
    let range_size = (max_ts - min_ts) / 10;
    let test_ranges: Vec<std::ops::Range<u64>> = (0..10)
        .map(|i| {
            let start = min_ts + i * range_size;
            let end = start + range_size;
            start..end
        })
        .collect();

    println!("Running {} point queries and {} range queries...\n", test_points.len(), test_ranges.len());

    // Benchmark point queries
    println!("Point queries:");

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
    println!("  VecCache:          {:?}", vec_point_time);

    let start = Instant::now();
    for &t in &test_points {
        let _ = avl_cache.query_point(t);
    }
    let avl_point_time = start.elapsed();
    println!("  InteravlCache:     {:?}", avl_point_time);

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
    println!("\nRange queries:");

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
    println!("  VecCache:          {:?}", vec_range_time);

    let start = Instant::now();
    for range in &test_ranges {
        let _ = avl_cache.query_range(range.clone());
    }
    let avl_range_time = start.elapsed();
    println!("  InteravlCache:     {:?}", avl_range_time);

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

    // Summary
    println!("\n=== Summary ===");
    println!("Build:        {} wins", fastest_build);
    println!("Append:       {} wins", fastest_append);
    println!("Point Query:  {} wins", fastest_point);
    println!("Range Query:  {} wins", fastest_range);

    // Calculate total times for overall winner
    let tree_total = tree_build_time + tree_append_time + tree_point_time + tree_range_time;
    let vec_total = vec_build_time + vec_append_time + vec_point_time + vec_range_time;
    let avl_total = avl_build_time + avl_append_time + avl_point_time + avl_range_time;

    let min_total = tree_total.min(vec_total).min(avl_total);
    let overall_winner = if min_total == vec_total {
        "VecCache"
    } else if min_total == tree_total {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };

    println!("\nOverall winner: {} (total: {:?})", overall_winner, min_total);

    Ok(())
}