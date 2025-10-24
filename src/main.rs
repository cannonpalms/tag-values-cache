use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::time::{Duration, Instant};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tag_values_cache::{
    BTreeCache, IntervalCache, IntervalTreeCache, LapperCache, NCListCache, TagSet,
    SortedData, UnmergedBTreeCache, ValueAwareLapperCache, VecCache, extract_tags_from_batch,
};

fn print_usage() {
    println!(
        "Usage: {} <parquet_path> [initial_rows] [append_rows]",
        env::args().next().unwrap_or_else(|| "program".to_string())
    );
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

/// Format a duration in the most appropriate unit (µs, ms, or s) with limited decimal places
fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();

    if nanos < 1_000 {
        // Less than 1 microsecond - show in nanoseconds
        format!("{nanos} ns")
    } else if nanos < 1_000_000 {
        // Less than 1 millisecond - show in microseconds
        format!("{:.2} µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        // Less than 1 second - show in milliseconds
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        // 1 second or more - show in seconds
        format!("{:.2} s", nanos as f64 / 1_000_000_000.0)
    }
}

/// Format bytes in the most appropriate unit (B, KiB, MiB, or GiB) with limited decimal places
fn format_bytes(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    let bytes_f64 = bytes as f64;

    if bytes < 1024 {
        // Less than 1 KiB - show in bytes
        format!("{bytes} B")
    } else if bytes_f64 < MIB {
        // Less than 1 MiB - show in KiB
        format!("{:.2} KiB", bytes_f64 / KIB)
    } else if bytes_f64 < GIB {
        // Less than 1 GiB - show in MiB
        format!("{:.2} MiB", bytes_f64 / MIB)
    } else {
        // 1 GiB or more - show in GiB
        format!("{:.2} GiB", bytes_f64 / GIB)
    }
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
        let initial: usize = args[2]
            .parse()
            .map_err(|_| format!("Invalid initial_rows: {}", args[2]))?;

        let append: usize = if args.len() > 3 {
            args[3]
                .parse()
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
    let total_rows = initial_rows
        .map(|i| i + append_rows)
        .or(Some(DEFAULT_MAX_ROWS));

    println!("=== Tag Values Cache Benchmark ===\n");
    println!("Configuration:");
    println!("  Path: {parquet_path}");
    if let Some(initial) = initial_rows {
        println!("  Initial rows: {initial}");
        println!("  Append rows: {append_rows}");
        println!("  Total rows: {}", initial + append_rows);
    } else {
        println!("  Initial rows: all available (up to {DEFAULT_MAX_ROWS} max)");
        println!("  Append rows: 0");
    }
    println!();

    // Get list of parquet files to process
    let parquet_files = get_parquet_files(parquet_path)?;

    if parquet_files.is_empty() {
        return Err(format!("No parquet files found at: {parquet_path}").into());
    }

    println!("Found {} parquet file(s) to process", parquet_files.len());

    // Load data from parquet files
    let mut all_data = Vec::new();
    let start_load = Instant::now();
    let mut total_batch_count = 0;

    for file_path in &parquet_files {
        // Check if we've reached our limit (if one was specified)
        if let Some(limit) = total_rows
            && all_data.len() >= limit
        {
            break;
        }

        println!("\nLoading parquet file: {file_path}");

        // Open the parquet file
        let file = File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Get metadata about the file
        let metadata = builder.metadata();
        let file_rows = metadata.file_metadata().num_rows();
        println!("  Rows in file: {file_rows}");

        // Build the reader
        let reader = builder.build()?;

        // Process batches until we have enough rows
        for batch_result in reader {
            // Check if we've reached our limit (if one was specified)
            if let Some(limit) = total_rows
                && all_data.len() >= limit
            {
                break;
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

            let tags = extract_tags_from_batch(&batch);
            all_data.extend(tags);

            // Trim to exactly total_rows if we went over
            if let Some(limit) = total_rows
                && all_data.len() > limit
            {
                all_data.truncate(limit);
                break;
            }
        }
    }

    println!(
        "\nLoaded {} rows in {:?}",
        all_data.len(),
        start_load.elapsed()
    );

    // Filter to only keep the 4 optimal tag columns
    println!("\nFiltering to only 4 tag columns: WithHash, CounterID, CookieEnable, URLHash");
    let filtered_data: Vec<(u64, TagSet)> = all_data
        .into_iter()
        .map(|(ts, tag_set)| {
            // Create a new TagSet with only the 4 columns
            let mut filtered_set = TagSet::new();

            // Only keep WithHash, CounterID, CookieEnable, and URLHash
            for (key, value) in &tag_set {
                if key == "WithHash" || key == "CounterID" || key == "CookieEnable" || key == "URLHash" {
                    filtered_set.insert((key.clone(), value.clone()));
                }
            }

            (ts, filtered_set)
        })
        .collect();

    // Split the data based on configuration
    let (first_half, second_half) = if append_rows > 0 {
        let split_point = initial_rows
            .unwrap_or(filtered_data.len() / 2)
            .min(filtered_data.len());
        let (first, second) = filtered_data.split_at(split_point);
        println!("\nData split:");
        println!("  Initial: {} rows", first.len());
        println!("  Append:  {} rows", second.len());
        (first, second)
    } else {
        println!(
            "\nUsing all {} rows for initial build (no append)",
            filtered_data.len()
        );
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
    println!("  IntervalTreeCache: {}", format_duration(tree_build_time));

    // Benchmark VecCache
    println!("Building VecCache...");
    let start = Instant::now();
    let mut vec_cache = VecCache::from_sorted(sorted_data1.clone())?;
    let vec_build_time = start.elapsed();
    println!("  VecCache: {}", format_duration(vec_build_time));

    // Benchmark LapperCache
    println!("Building LapperCache...");
    let start = Instant::now();
    let mut lapper_cache = LapperCache::from_sorted(sorted_data1.clone())?;
    let lapper_build_time = start.elapsed();
    println!("  LapperCache: {}", format_duration(lapper_build_time));

    // Benchmark ValueAwareLapperCache
    println!("Building ValueAwareLapperCache...");
    let start = Instant::now();
    let mut value_aware_lapper_cache = ValueAwareLapperCache::from_sorted(sorted_data1.clone())?;
    let value_lapper_build_time = start.elapsed();
    println!(
        "  ValueAwareLapperCache: {}",
        format_duration(value_lapper_build_time)
    );

    // Benchmark BTreeCache
    println!("Building BTreeCache...");
    let start = Instant::now();
    let mut btree_cache = BTreeCache::from_sorted(sorted_data1.clone())?;
    let btree_build_time = start.elapsed();
    println!("  BTreeCache: {}", format_duration(btree_build_time));

    // Benchmark NCListCache
    println!("Building NCListCache...");
    let start = Instant::now();
    let mut nclist_cache = NCListCache::from_sorted(sorted_data1.clone())?;
    let nclist_build_time = start.elapsed();
    println!("  NCListCache: {}", format_duration(nclist_build_time));

    // Benchmark SegmentTreeCache
    // println!("Building SegmentTreeCache...");
    // let start = Instant::now();
    // let mut segment_tree_cache = SegmentTreeCache::from_sorted(sorted_data1.clone())?;
    // let segment_tree_build_time = start.elapsed();
    // println!(
    //     "  SegmentTreeCache: {}",
    //     format_duration(segment_tree_build_time)
    // );

    // Benchmark UnmergedBTreeCache
    println!("Building UnmergedBTreeCache...");
    let start = Instant::now();
    let mut unmerged_btree_cache = UnmergedBTreeCache::from_sorted(sorted_data1.clone())?;
    let unmerged_btree_build_time = start.elapsed();
    println!(
        "  UnmergedBTreeCache: {}",
        format_duration(unmerged_btree_build_time)
    );

    // Find fastest build
    let min_build = tree_build_time
        .min(vec_build_time)
        .min(lapper_build_time)
        .min(value_lapper_build_time)
        .min(btree_build_time)
        .min(nclist_build_time)
        // .min(segment_tree_build_time)
        .min(unmerged_btree_build_time);
    let fastest_build = if min_build == vec_build_time {
        "VecCache"
    } else if min_build == tree_build_time {
        "IntervalTreeCache"
    } else if min_build == lapper_build_time {
        "LapperCache"
    } else if min_build == value_lapper_build_time {
        "ValueAwareLapperCache"
    } else if min_build == btree_build_time {
        "BTreeCache"
    } else if min_build == nclist_build_time {
        "NCListCache"
    // } else if min_build == segment_tree_build_time {
    //     "SegmentTreeCache"
    } else {
        "UnmergedBTreeCache"
    };
    println!("\nFastest build: {fastest_build}\n");

    // Report number of intervals after initial build
    println!(
        "Intervals after initial build ({} data points):",
        first_half.len()
    );
    println!(
        "  IntervalTreeCache: {} intervals",
        tree_cache.interval_count()
    );
    println!(
        "  VecCache:          {} intervals",
        vec_cache.interval_count()
    );
    println!(
        "  LapperCache:       {} intervals",
        lapper_cache.interval_count()
    );
    println!(
        "  ValueAwareLapperCache:  {} intervals",
        value_aware_lapper_cache.interval_count()
    );
    println!(
        "  BTreeCache:        {} intervals",
        btree_cache.interval_count()
    );
    println!(
        "  NCListCache:       {} intervals",
        nclist_cache.interval_count()
    );
    // println!(
    //     "  SegmentTreeCache:  {} intervals",
    //     segment_tree_cache.interval_count()
    // );
    println!(
        "  UnmergedBTreeCache: {} intervals\n",
        unmerged_btree_cache.interval_count()
    );

    // Benchmark append performance (if configured)
    let (
        tree_append_time,
        vec_append_time,
        lapper_append_time,
        value_lapper_append_time,
        btree_append_time,
        nclist_append_time,
        // segment_tree_append_time,
        unmerged_btree_append_time,
    ) = if let Some(sorted_data2) = sorted_data2 {
        println!("=== Append Performance ===");
        println!("Appending {} rows...\n", second_half.len());

        // Append to IntervalTreeCache
        println!("Appending to IntervalTreeCache...");
        let start = Instant::now();
        tree_cache.append_sorted(sorted_data2.clone())?;
        let tree_append = start.elapsed();
        println!("  IntervalTreeCache: {}", format_duration(tree_append));

        // Append to VecCache
        println!("Appending to VecCache...");
        let start = Instant::now();
        vec_cache.append_sorted(sorted_data2.clone())?;
        let vec_append = start.elapsed();
        println!("  VecCache: {}", format_duration(vec_append));

        // Append to LapperCache
        println!("Appending to LapperCache...");
        let start = Instant::now();
        lapper_cache.append_sorted(sorted_data2.clone())?;
        let lapper_append = start.elapsed();
        println!("  LapperCache: {}", format_duration(lapper_append));

        // Append to ValueAwareLapperCache
        println!("Appending to ValueAwareLapperCache...");
        let start = Instant::now();
        value_aware_lapper_cache.append_sorted(sorted_data2.clone())?;
        let value_lapper_append = start.elapsed();
        println!(
            "  ValueAwareLapperCache: {}",
            format_duration(value_lapper_append)
        );

        // Append to BTreeCache
        println!("Appending to BTreeCache...");
        let start = Instant::now();
        btree_cache.append_sorted(sorted_data2.clone())?;
        let btree_append = start.elapsed();
        println!("  BTreeCache: {}", format_duration(btree_append));

        // Append to NCListCache
        println!("Appending to NCListCache...");
        let start = Instant::now();
        nclist_cache.append_sorted(sorted_data2.clone())?;
        let nclist_append = start.elapsed();
        println!("  NCListCache: {}", format_duration(nclist_append));

        // Append to SegmentTreeCache
        // println!("Appending to SegmentTreeCache...");
        // let start = Instant::now();
        // segment_tree_cache.append_sorted(sorted_data2.clone())?;
        // let segment_tree_append = start.elapsed();
        // println!(
        //     "  SegmentTreeCache: {}",
        //     format_duration(segment_tree_append)
        // );

        // Append to UnmergedBTreeCache
        println!("Appending to UnmergedBTreeCache...");
        let start = Instant::now();
        unmerged_btree_cache.append_sorted(sorted_data2)?;
        let unmerged_btree_append = start.elapsed();
        println!(
            "  UnmergedBTreeCache: {}",
            format_duration(unmerged_btree_append)
        );

        (
            tree_append,
            vec_append,
            lapper_append,
            value_lapper_append,
            btree_append,
            nclist_append,
            // segment_tree_append,
            unmerged_btree_append,
        )
    } else {
        // No append phase
        (
            Duration::from_secs(0),
            Duration::from_secs(0),
            Duration::from_secs(0),
            Duration::from_secs(0),
            Duration::from_secs(0),
            Duration::from_secs(0),
            // Duration::from_secs(0),
            Duration::from_secs(0),
        )
    };

    // Find fastest append (if append was performed)
    let (min_append, fastest_append) = if append_rows > 0 {
        let min_time = tree_append_time
            .min(vec_append_time)
            .min(lapper_append_time)
            .min(value_lapper_append_time)
            .min(btree_append_time)
            .min(nclist_append_time)
            // .min(segment_tree_append_time)
            .min(unmerged_btree_append_time);
        let fastest = if min_time == vec_append_time {
            "VecCache"
        } else if min_time == tree_append_time {
            "IntervalTreeCache"
        } else if min_time == lapper_append_time {
            "LapperCache"
        } else if min_time == value_lapper_append_time {
            "ValueAwareLapperCache"
        } else if min_time == btree_append_time {
            "BTreeCache"
        } else if min_time == nclist_append_time {
            "NCListCache"
        // } else if min_time == segment_tree_append_time {
        //     "SegmentTreeCache"
        } else {
            "UnmergedBTreeCache"
        };
        println!("\nFastest append: {fastest}\n");
        (min_time, fastest)
    } else {
        (Duration::from_secs(0), "N/A")
    };

    // Report final number of intervals
    let final_label = if append_rows > 0 {
        "after append"
    } else {
        "final"
    };
    println!(
        "Intervals {} ({} total data points):",
        final_label,
        first_half.len() + second_half.len()
    );
    println!(
        "  IntervalTreeCache: {} intervals",
        tree_cache.interval_count()
    );
    println!(
        "  VecCache:          {} intervals",
        vec_cache.interval_count()
    );
    println!(
        "  LapperCache:       {} intervals",
        lapper_cache.interval_count()
    );
    println!(
        "  ValueAwareLapperCache:  {} intervals",
        value_aware_lapper_cache.interval_count()
    );
    println!(
        "  BTreeCache:        {} intervals",
        btree_cache.interval_count()
    );
    println!(
        "  NCListCache:       {} intervals",
        nclist_cache.interval_count()
    );
    // println!(
    //     "  SegmentTreeCache:  {} intervals",
    //     segment_tree_cache.interval_count()
    // );
    println!(
        "  UnmergedBTreeCache: {} intervals",
        unmerged_btree_cache.interval_count()
    );

    // Calculate compression ratio
    let total_points = first_half.len() + second_half.len();
    let tree_compression = (total_points as f64) / (tree_cache.interval_count() as f64);
    let vec_compression = (total_points as f64) / (vec_cache.interval_count() as f64);
    let lapper_compression = (total_points as f64) / (lapper_cache.interval_count() as f64);
    let value_lapper_compression =
        (total_points as f64) / (value_aware_lapper_cache.interval_count() as f64);
    let btree_compression = (total_points as f64) / (btree_cache.interval_count() as f64);
    let nclist_compression = (total_points as f64) / (nclist_cache.interval_count() as f64);
    // let segment_tree_compression =
    //     (total_points as f64) / (segment_tree_cache.interval_count() as f64);
    let unmerged_btree_compression =
        (total_points as f64) / (unmerged_btree_cache.interval_count() as f64);

    println!("\nCompression ratio (data points / intervals):");
    println!("  IntervalTreeCache: {tree_compression:.2}x");
    println!("  VecCache:          {vec_compression:.2}x");
    println!("  LapperCache:       {lapper_compression:.2}x");
    println!("  ValueAwareLapperCache:  {value_lapper_compression:.2}x");
    println!("  BTreeCache:        {btree_compression:.2}x");
    println!("  NCListCache:       {nclist_compression:.2}x");
    // println!("  SegmentTreeCache:  {segment_tree_compression:.2}x");
    println!("  UnmergedBTreeCache: {unmerged_btree_compression:.2}x\n");

    // Generate query test points
    println!("=== Query Performance ===");

    // Get some sample timestamps from the sorted data for realistic queries
    let all_data = sorted_data1.into_inner();
    let sample_size = 1000.min(all_data.len());
    let step = all_data.len() / sample_size;
    let test_points: Vec<u64> = (0..sample_size).map(|i| all_data[i * step].0).collect();

    let min_ts = all_data.first().map_or(0, |p| p.0);
    let max_ts = all_data.last().map_or(1000000, |p| p.0);
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

    use std::hint::black_box;
    let start = Instant::now();
    for &t in &test_points {
        black_box(tree_cache.query_point(t));
    }
    let tree_point_time = start.elapsed();
    println!("  IntervalTreeCache: {}", format_duration(tree_point_time));

    let start = Instant::now();
    for &t in &test_points {
        black_box(vec_cache.query_point(t));
    }
    let vec_point_time = start.elapsed();
    println!("  VecCache: {}", format_duration(vec_point_time));

    let start = Instant::now();
    for &t in &test_points {
        black_box(lapper_cache.query_point(t));
    }
    let lapper_point_time = start.elapsed();
    println!("  LapperCache: {}", format_duration(lapper_point_time));

    let start = Instant::now();
    for &t in &test_points {
        black_box(value_aware_lapper_cache.query_point(t));
    }
    let value_lapper_point_time = start.elapsed();
    println!(
        "  ValueAwareLapperCache: {}",
        format_duration(value_lapper_point_time)
    );

    let start = Instant::now();
    for &t in &test_points {
        black_box(btree_cache.query_point(t));
    }
    let btree_point_time = start.elapsed();
    println!("  BTreeCache: {}", format_duration(btree_point_time));

    let start = Instant::now();
    for &t in &test_points {
        black_box(nclist_cache.query_point(t));
    }
    let nclist_point_time = start.elapsed();
    println!("  NCListCache: {}", format_duration(nclist_point_time));

    // let start = Instant::now();
    // for &t in &test_points {
    //     black_box(segment_tree_cache.query_point(t));
    // }
    // let segment_tree_point_time = start.elapsed();
    // println!(
    //     "  SegmentTreeCache: {}",
    //     format_duration(segment_tree_point_time)
    // );

    let start = Instant::now();
    for &t in &test_points {
        black_box(unmerged_btree_cache.query_point(t));
    }
    let unmerged_btree_point_time = start.elapsed();
    println!(
        "  UnmergedBTreeCache: {}",
        format_duration(unmerged_btree_point_time)
    );

    // Find fastest point query
    let min_point = tree_point_time
        .min(vec_point_time)
        .min(lapper_point_time)
        .min(value_lapper_point_time)
        .min(btree_point_time)
        .min(nclist_point_time)
        // .min(segment_tree_point_time)
        .min(unmerged_btree_point_time);
    let fastest_point = if min_point == vec_point_time {
        "VecCache"
    } else if min_point == tree_point_time {
        "IntervalTreeCache"
    } else if min_point == lapper_point_time {
        "LapperCache"
    } else if min_point == value_lapper_point_time {
        "ValueAwareLapperCache"
    } else if min_point == btree_point_time {
        "BTreeCache"
    } else if min_point == nclist_point_time {
        "NCListCache"
    // } else if min_point == segment_tree_point_time {
    //     "SegmentTreeCache"
    } else {
        "UnmergedBTreeCache"
    };
    println!("\n  Fastest: {fastest_point}");

    // Benchmark range queries
    println!("\nRange queries ({} queries):", test_ranges.len());

    let start = Instant::now();
    for range in &test_ranges {
        black_box(tree_cache.query_range(range.clone()));
    }
    let tree_range_time = start.elapsed();
    println!("  IntervalTreeCache: {}", format_duration(tree_range_time));

    let start = Instant::now();
    for range in &test_ranges {
        black_box(vec_cache.query_range(range.clone()));
    }
    let vec_range_time = start.elapsed();
    println!("  VecCache: {}", format_duration(vec_range_time));

    let start = Instant::now();
    for range in &test_ranges {
        black_box(lapper_cache.query_range(range.clone()));
    }
    let lapper_range_time = start.elapsed();
    println!("  LapperCache: {}", format_duration(lapper_range_time));

    let start = Instant::now();
    for range in &test_ranges {
        black_box(value_aware_lapper_cache.query_range(range.clone()));
    }
    let value_lapper_range_time = start.elapsed();
    println!(
        "  ValueAwareLapperCache: {}",
        format_duration(value_lapper_range_time)
    );

    let start = Instant::now();
    for range in &test_ranges {
        black_box(btree_cache.query_range(range.clone()));
    }
    let btree_range_time = start.elapsed();
    println!("  BTreeCache: {}", format_duration(btree_range_time));

    let start = Instant::now();
    for range in &test_ranges {
        black_box(nclist_cache.query_range(range.clone()));
    }
    let nclist_range_time = start.elapsed();
    println!("  NCListCache: {}", format_duration(nclist_range_time));

    // let start = Instant::now();
    // for range in &test_ranges {
    //     black_box(segment_tree_cache.query_range(range.clone()));
    // }
    // let segment_tree_range_time = start.elapsed();
    // println!(
    //     "  SegmentTreeCache: {}",
    //     format_duration(segment_tree_range_time)
    // );

    let start = Instant::now();
    for range in &test_ranges {
        black_box(unmerged_btree_cache.query_range(range.clone()));
    }
    let unmerged_btree_range_time = start.elapsed();
    println!(
        "  UnmergedBTreeCache: {}",
        format_duration(unmerged_btree_range_time)
    );

    // Find fastest range query
    let min_range = tree_range_time
        .min(vec_range_time)
        .min(lapper_range_time)
        .min(value_lapper_range_time)
        .min(btree_range_time)
        .min(nclist_range_time)
        // .min(segment_tree_range_time)
        .min(unmerged_btree_range_time);
    let fastest_range = if min_range == vec_range_time {
        "VecCache"
    } else if min_range == tree_range_time {
        "IntervalTreeCache"
    } else if min_range == lapper_range_time {
        "LapperCache"
    } else if min_range == value_lapper_range_time {
        "ValueAwareLapperCache"
    } else if min_range == btree_range_time {
        "BTreeCache"
    } else if min_range == nclist_range_time {
        "NCListCache"
    // } else if min_range == segment_tree_range_time {
    //     "SegmentTreeCache"
    } else {
        "UnmergedBTreeCache"
    };
    println!("\n  Fastest: {fastest_range}");

    // Verify all caches return the same results
    println!("\n=== Result Verification ===");
    println!("Verifying that all caches return identical results...\n");

    let mut all_match = true;

    // Verify point queries
    for &t in &test_points {
        let tree_result = tree_cache.query_point(t);
        let vec_result = vec_cache.query_point(t);
        let lapper_result = lapper_cache.query_point(t);
        let value_lapper_result = value_aware_lapper_cache.query_point(t);
        let btree_result = btree_cache.query_point(t);
        let nclist_result = nclist_cache.query_point(t);
        // let segment_tree_result = segment_tree_cache.query_point(t);
        let unmerged_btree_result = unmerged_btree_cache.query_point(t);

        // All caches now return HashSet<&RecordBatchRow>, so comparison works directly

        if tree_result != vec_result
            || tree_result != lapper_result
            || tree_result != value_lapper_result
            || tree_result != btree_result
            || tree_result != nclist_result
            // || tree_result != segment_tree_result
            || tree_result != unmerged_btree_result
        {
            println!("  MISMATCH at timestamp {t}:");

            // Find the majority result by comparing all pairs
            let results = vec![
                ("IntervalTreeCache", &tree_result),
                ("VecCache", &vec_result),
                ("LapperCache", &lapper_result),
                ("ValueAwareLapperCache", &value_lapper_result),
                ("BTreeCache", &btree_result),
                ("NCListCache", &nclist_result),
                // ("SegmentTreeCache", &segment_tree_result),
                ("UnmergedBTreeCache", &unmerged_btree_result),
            ];

            // Count how many results match each implementation
            let mut match_counts = vec![0; results.len()];
            for i in 0..results.len() {
                for j in 0..results.len() {
                    if results[i].1 == results[j].1 {
                        match_counts[i] += 1;
                    }
                }
            }

            // Find the result with the most matches
            // Not foolproof, but I do not have the correct answers to compare against
            let majority_idx = match_counts
                .iter()
                .enumerate()
                .max_by_key(|(_, count)| *count)
                .map(|(idx, _)| idx)
                .unwrap();
            let majority_result = results[majority_idx].1;

            for (name, result) in &results {
                if result != &majority_result {
                    let majority_set: HashSet<_> = majority_result.iter().collect();
                    let result_set: HashSet<_> = result.iter().collect();
                    let missing: Vec<_> = majority_set.difference(&result_set).collect();
                    let extra: Vec<_> = result_set.difference(&majority_set).collect();
                    print!("    {name}: ");
                    if !missing.is_empty() {
                        print!("missing {:?}", missing);
                    }
                    if !extra.is_empty() {
                        if !missing.is_empty() {
                            print!(", ");
                        }
                        print!("extra {:?}", extra);
                    }
                    println!();
                }
            }

            all_match = false;
        }
    }

    // Verify range queries
    for range in &test_ranges {
        let tree_result = tree_cache.query_range(range.clone());
        let vec_result = vec_cache.query_range(range.clone());
        let lapper_result = lapper_cache.query_range(range.clone());
        let value_lapper_result = value_aware_lapper_cache.query_range(range.clone());
        let btree_result = btree_cache.query_range(range.clone());
        let nclist_result = nclist_cache.query_range(range.clone());
        // let segment_tree_result = segment_tree_cache.query_range(range.clone());
        let unmerged_btree_result = unmerged_btree_cache.query_range(range.clone());

        // All caches now return HashSet<&RecordBatchRow>, so comparison works directly

        if tree_result != vec_result
            || tree_result != lapper_result
            || tree_result != value_lapper_result
            || tree_result != btree_result
            || tree_result != nclist_result
            // || tree_result != segment_tree_result
            || tree_result != unmerged_btree_result
        {
            println!("  MISMATCH at range {:?}:", range);

            // Find the majority result by comparing all pairs
            let results = vec![
                ("IntervalTreeCache", &tree_result),
                ("VecCache", &vec_result),
                ("LapperCache", &lapper_result),
                ("ValueAwareLapperCache", &value_lapper_result),
                ("BTreeCache", &btree_result),
                ("NCListCache", &nclist_result),
                // ("SegmentTreeCache", &segment_tree_result),
                ("UnmergedBTreeCache", &unmerged_btree_result),
            ];

            // Count how many results match each implementation
            let mut match_counts = vec![0; results.len()];
            for i in 0..results.len() {
                for j in 0..results.len() {
                    if results[i].1 == results[j].1 {
                        match_counts[i] += 1;
                    }
                }
            }

            // Find the result with the most matches
            let majority_idx = match_counts
                .iter()
                .enumerate()
                .max_by_key(|(_, count)| *count)
                .map(|(idx, _)| idx)
                .unwrap();
            let majority_result = results[majority_idx].1;

            for (name, result) in &results {
                if result != &majority_result {
                    let majority_set: HashSet<_> = majority_result.iter().collect();
                    let result_set: HashSet<_> = result.iter().collect();
                    let missing: Vec<_> = majority_set.difference(&result_set).collect();
                    let extra: Vec<_> = result_set.difference(&majority_set).collect();
                    print!("    {name}: ");
                    if !missing.is_empty() {
                        print!("missing {:?}", missing);
                    }
                    if !extra.is_empty() {
                        if !missing.is_empty() {
                            print!(", ");
                        }
                        print!("extra {:?}", extra);
                    }
                    println!();
                }
            }

            all_match = false;
        }
    }

    if all_match {
        println!("  ✓ All caches return identical results");
    } else {
        println!("  ✗ Result mismatches detected!");
    }

    // Measure memory usage
    println!("\n=== Memory Usage ===");
    println!("Measuring actual memory usage for each cache implementation...\n");

    // Get actual memory usage from the size_bytes() method
    let tree_memory = tree_cache.size_bytes();
    let vec_memory = vec_cache.size_bytes();
    let lapper_memory = lapper_cache.size_bytes();
    let value_lapper_memory = value_aware_lapper_cache.size_bytes();
    let btree_memory = btree_cache.size_bytes();
    let nclist_memory = nclist_cache.size_bytes();
    // let segment_tree_memory = segment_tree_cache.size_bytes();
    let unmerged_btree_memory = unmerged_btree_cache.size_bytes();

    println!(
        "  IntervalTreeCache: {} ({} bytes)",
        format_bytes(tree_memory),
        tree_memory
    );
    println!(
        "  VecCache:          {} ({} bytes)",
        format_bytes(vec_memory),
        vec_memory
    );
    println!(
        "  LapperCache:       {} ({} bytes)",
        format_bytes(lapper_memory),
        lapper_memory
    );
    println!(
        "  ValueAwareLapperCache:  {} ({} bytes)",
        format_bytes(value_lapper_memory),
        value_lapper_memory
    );
    println!(
        "  BTreeCache:        {} ({} bytes)",
        format_bytes(btree_memory),
        btree_memory
    );
    println!(
        "  NCListCache:       {} ({} bytes)",
        format_bytes(nclist_memory),
        nclist_memory
    );
    // println!(
    //     "  SegmentTreeCache:  {} ({} bytes)",
    //     format_bytes(segment_tree_memory),
    //     segment_tree_memory
    // );
    println!(
        "  UnmergedBTreeCache: {} ({} bytes)",
        format_bytes(unmerged_btree_memory),
        unmerged_btree_memory
    );

    let min_memory = tree_memory
        .min(vec_memory)
        .min(lapper_memory)
        .min(value_lapper_memory)
        .min(btree_memory)
        .min(nclist_memory)
        // .min(segment_tree_memory)
        .min(unmerged_btree_memory);
    let lowest_memory = if min_memory == vec_memory {
        "VecCache"
    } else if min_memory == tree_memory {
        "IntervalTreeCache"
    } else if min_memory == lapper_memory {
        "LapperCache"
    } else if min_memory == value_lapper_memory {
        "ValueAwareLapperCache"
    } else if min_memory == btree_memory {
        "BTreeCache"
    } else if min_memory == nclist_memory {
        "NCListCache"
    // } else if min_memory == segment_tree_memory {
    //     "SegmentTreeCache"
    } else {
        "UnmergedBTreeCache"
    };
    println!("\nLowest memory usage: {lowest_memory}\n");

    // Summary
    println!("\n=== Summary ===");
    println!(
        "Dataset: {} total rows processed (with 4 tag columns only)",
        first_half.len() + second_half.len()
    );
    println!("\nPerformance winners:");
    println!(
        "  Build:        {} ({})",
        fastest_build,
        format_duration(min_build)
    );
    if append_rows > 0 {
        println!(
            "  Append:       {} ({})",
            fastest_append,
            format_duration(min_append)
        );
    }
    println!(
        "  Point Query:  {} ({})",
        fastest_point,
        format_duration(min_point)
    );
    println!(
        "  Range Query:  {} ({})",
        fastest_range,
        format_duration(min_range)
    );
    println!(
        "  Memory:       {} ({})",
        lowest_memory,
        format_bytes(min_memory)
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
