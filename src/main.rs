use tag_values_cache::{InteravlCache, IntervalCache, IntervalTreeCache, VecCache, Timestamp};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<(Timestamp, String)> = vec![
        (0, "A".to_string()),
        (1, "A".to_string()),
        (2, "A".to_string()),
        (5, "B".to_string()),
        (6, "B".to_string()),
        (10, "A".to_string()),
    ];

    println!("=== IntervalTreeCache Example ===");
    let tree_cache = IntervalTreeCache::new(data.clone())?;
    println!("Query at t=1: {:?}", tree_cache.query_point(1));
    println!("Query at t=3: {:?}", tree_cache.query_point(3));
    println!("Query range [0, 10): {:?}", tree_cache.query_range(0..10));

    println!("\n=== VecCache Example ===");
    let vec_cache = VecCache::new(data.clone())?;
    println!("Query at t=1: {:?}", vec_cache.query_point(1));
    println!("Query at t=5: {:?}", vec_cache.query_point(5));
    println!("Query range [0, 10): {:?}", vec_cache.query_range(0..10));

    println!("\n=== InteravlCache (AVL-based) Example ===");
    let avl_cache = InteravlCache::new(data)?;
    println!("Query at t=1: {:?}", avl_cache.query_point(1));
    println!("Query at t=6: {:?}", avl_cache.query_point(6));
    println!("Query range [0, 10): {:?}", avl_cache.query_range(0..10));

    // Demonstrate batch appending
    println!("\n=== Batch Append Example ===");
    let initial_data = vec![
        (0, "A".to_string()),
        (1, "A".to_string()),
    ];

    let mut cache = IntervalTreeCache::new(initial_data)?;
    println!("Initial cache - Query at t=0: {:?}", cache.query_point(0));

    // Append new data
    let batch1 = vec![
        (2, "A".to_string()), // Extends existing interval
        (5, "B".to_string()),
    ];
    cache.append_batch(batch1)?;
    println!("After batch 1 - Query range [0, 6): {:?}", cache.query_range(0..6));

    // Append more data
    let batch2 = vec![
        (10, "C".to_string()),
        (11, "C".to_string()),
    ];
    cache.append_batch(batch2)?;
    println!("After batch 2 - Query range [0, 12): {:?}", cache.query_range(0..12));

    Ok(())
}