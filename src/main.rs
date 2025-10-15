use tag_values_cache::{IntervalCache, IntervalTreeCache, VecCache, Timestamp};

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
    let vec_cache = VecCache::new(data)?;
    println!("Query at t=1: {:?}", vec_cache.query_point(1));
    println!("Query at t=5: {:?}", vec_cache.query_point(5));
    println!("Query range [0, 10): {:?}", vec_cache.query_range(0..10));

    Ok(())
}