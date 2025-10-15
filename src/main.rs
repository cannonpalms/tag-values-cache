use tag_values_cache::{
    compare::{benchmark_append, compare_build_times, compare_implementations, verify_equivalence},
    CacheBuilder, InteravlCache, IntervalCache, IntervalTreeCache, SortedData, Timestamp,
    VecCache,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Basic usage with string values
    println!("=== Example 1: Basic String Values ===");
    let data: Vec<(Timestamp, String)> = vec![
        (0, "A".to_string()),
        (1, "A".to_string()), // Will be merged with timestamp 0 into [0, 2)
        (2, "A".to_string()), // Will be merged into [0, 3)
        (5, "B".to_string()),
        (6, "B".to_string()), // Will be merged with timestamp 5 into [5, 7)
        (10, "A".to_string()), // New interval for "A" at [10, 11)
        (15, "C".to_string()),
    ];

    // Direct construction
    let cache = IntervalTreeCache::new(data.clone())?;

    println!("Query at t=1: {:?}", cache.query_point(1));
    println!("Query at t=3: {:?}", cache.query_point(3));  // Gap between intervals
    println!("Query at t=5: {:?}", cache.query_point(5));
    println!("Query at t=10: {:?}", cache.query_point(10));
    println!("Query range [0, 6): {:?}", cache.query_range(0..6));

    // Using the builder pattern
    println!("\n=== Using Builder Pattern ===");
    let cache = CacheBuilder::new(data).build_interval_tree()?;
    println!("Query range [5, 16): {:?}", cache.query_range(5..16));

    // Example 2: Multiple values at same timestamp
    println!("\n=== Example 2: Overlapping Intervals ===");
    let data: Vec<(Timestamp, &str)> = vec![
        (0, "sensor1"),
        (1, "sensor1"),
        (2, "sensor1"),  // sensor1 creates interval [0, 3)
        (1, "sensor2"),  // sensor2 starts at t=1
        (2, "sensor2"),
        (3, "sensor2"),  // sensor2 creates interval [1, 4)
    ];

    let cache = IntervalTreeCache::new(data)?;

    println!("Query at t=0 (sensor1 only): {:?}", cache.query_point(0));
    println!("Query at t=1 (overlapping): {:?}", cache.query_point(1));
    println!("Query at t=2 (overlapping): {:?}", cache.query_point(2));
    println!("Query at t=3 (sensor2 only): {:?}", cache.query_point(3));

    // Example 3: Demonstrating interval merging
    println!("\n=== Example 3: Interval Merging Behavior ===");
    let data: Vec<(Timestamp, String)> = vec![
        (0, "event_A".to_string()),
        (1, "event_A".to_string()),
        (2, "event_A".to_string()),  // Merges into [0, 3)
        (10, "event_B".to_string()),
        (11, "event_B".to_string()), // Merges into [10, 12)
        (20, "event_A".to_string()), // New interval for A at [20, 21)
    ];

    let cache = IntervalTreeCache::new(data)?;

    println!("Intervals created:");
    println!("  event_A: [0, 3) and [20, 21)");
    println!("  event_B: [10, 12)");
    println!();
    println!("Query at t=1: {:?}", cache.query_point(1));
    println!("Query at t=5 (gap): {:?}", cache.query_point(5));
    println!("Query at t=10: {:?}", cache.query_point(10));
    println!("Query range [0, 25): {:?}", cache.query_range(0..25));

    // Example 4: Generic usage and comparing all three implementations
    println!("\n=== Example 4: Comparing All Three Implementations ===");

    // Create test data with repeating pattern
    let comparison_data: Vec<(Timestamp, String)> = (0..30)
        .map(|i| {
            let value = match i % 10 {
                0..=2 => "A",  // Timestamps 0-2, 10-12, 20-22
                3..=5 => "B",  // Timestamps 3-5, 13-15, 23-25
                6..=7 => "C",  // Timestamps 6-7, 16-17, 26-27
                _ => "D",      // Timestamps 8-9, 18-19, 28-29
            };
            (i, value.to_string())
        })
        .collect();

    // Build all three cache types with the same data
    let tree_cache = IntervalTreeCache::new(comparison_data.clone())?;
    let vec_cache = VecCache::new(comparison_data.clone())?;
    let avl_cache = InteravlCache::new(comparison_data)?;

    // Generic function that works with ANY cache implementation
    fn generic_analysis<C>(cache: &C, name: &str)
    where
        C: IntervalCache<String>,
    {
        println!("\n{} Analysis:", name);
        let test_points = vec![0, 5, 10, 15, 20, 25];
        for t in test_points {
            println!("  t={:2}: {} values", t, cache.query_point(t).len());
        }
    }

    // Use the same function with different implementations
    generic_analysis(&tree_cache, "IntervalTreeCache");
    generic_analysis(&vec_cache, "VecCache");

    // Compare the implementations
    let test_points: Vec<Timestamp> = (0..30).step_by(5).collect();
    let test_ranges = vec![0..10, 10..20, 20..30];

    println!("\n=== Verifying Equivalence ===");
    let equivalent = verify_equivalence(&tree_cache, &vec_cache, &test_points, &test_ranges);
    println!("Implementations produce identical results: {}", equivalent);

    // Performance comparison
    compare_implementations(
        &tree_cache,
        &vec_cache,
        "IntervalTreeCache",
        "VecCache",
        &test_points,
        &test_ranges,
    );

    // Example 5: Demonstrating append functionality
    println!("\n=== Example 5: Append Functionality ===");

    // Start with initial data
    let initial_data = vec![
        (0, "A".to_string()),
        (1, "A".to_string()),
        (2, "B".to_string()),
    ];

    let mut cache = IntervalTreeCache::new(initial_data)?;
    println!("Initial cache built");
    println!("  Query at t=1: {:?}", cache.query_point(1));

    // Append batch 1: extending existing interval
    let batch1 = vec![
        (3, "B".to_string()), // Extends B interval
        (4, "B".to_string()),
    ];
    cache.append_batch(batch1)?;
    println!("\nAfter appending batch 1:");
    println!("  Query at t=3: {:?}", cache.query_point(3));
    println!("  Query range [0, 5): {:?}", cache.query_range(0..5));

    // Append batch 2: adding new values
    let batch2 = vec![
        (10, "C".to_string()),
        (11, "C".to_string()),
        (15, "D".to_string()),
    ];
    cache.append_batch(batch2)?;
    println!("\nAfter appending batch 2:");
    println!("  Query at t=10: {:?}", cache.query_point(10));
    println!("  Query range [0, 20): {:?}", cache.query_range(0..20));

    // Example 6: Build time benchmarks
    println!("\n=== Example 6: Build Time Benchmarks ===");

    // Generate larger dataset for benchmarking
    let benchmark_data: Vec<(Timestamp, String)> = (0..1000)
        .map(|i| {
            let value = match i % 4 {
                0 => "Type_A",
                1 => "Type_B",
                2 => "Type_C",
                _ => "Type_D",
            };
            (i, value.to_string())
        })
        .collect();

    // Benchmark build times with 10 iterations
    compare_build_times(benchmark_data.clone(), 10);

    // Example 7: Append benchmarks
    println!("\n=== Example 7: Append Performance ===");

    // Create caches for append benchmarking
    let initial: Vec<(Timestamp, String)> = (0..100)
        .map(|i| (i, format!("initial_{}", i % 10)))
        .collect();

    let mut tree_cache = IntervalTreeCache::new(initial.clone())?;
    let mut vec_cache = VecCache::new(initial.clone())?;
    let mut avl_cache = InteravlCache::new(initial)?;

    // Create batches for appending
    let batches: Vec<SortedData<String>> = (0..5)
        .map(|batch_idx| {
            let start = 100 + batch_idx * 50;
            let data: Vec<(Timestamp, String)> = (start..start + 50)
                .map(|i| (i as u64, format!("batch_{}_{}", batch_idx, i % 5)))
                .collect();
            SortedData::from_unsorted(data)
        })
        .collect();

    println!("Appending 5 batches of 50 items each:");
    benchmark_append(&mut tree_cache, "IntervalTreeCache", batches.clone());
    benchmark_append(&mut vec_cache, "VecCache", batches.clone());
    benchmark_append(&mut avl_cache, "InteravlCache", batches);

    // Example 8: Demonstrating sorted vs unsorted data
    println!("\n=== Example 8: Sorted vs Unsorted Data ===");

    let unsorted_data = vec![
        (5, "B".to_string()),
        (1, "A".to_string()),
        (3, "A".to_string()),
        (2, "A".to_string()),
        (4, "B".to_string()),
    ];

    println!("Building from unsorted data (will sort automatically):");
    let start = std::time::Instant::now();
    let cache1 = IntervalTreeCache::new(unsorted_data.clone())?;
    println!("  Time with automatic sorting: {:?}", start.elapsed());

    println!("\nPre-sorting and using from_sorted:");
    let start = std::time::Instant::now();
    let sorted_data = SortedData::from_unsorted(unsorted_data);
    let cache2 = IntervalTreeCache::from_sorted(sorted_data)?;
    println!("  Time with pre-sorted data: {:?}", start.elapsed());

    // Verify both produce same results
    println!("\nVerifying both approaches produce same results:");
    println!("  Cache1 query at t=2: {:?}", cache1.query_point(2));
    println!("  Cache2 query at t=2: {:?}", cache2.query_point(2));

    Ok(())
}