use tag_values_cache::{IntervalCache, IntervalTreeCache, Timestamp};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== IntervalTreeCache Example ===");

    let data: Vec<(Timestamp, String)> = vec![
        (0, "A".to_string()),
        (1, "A".to_string()), // Will be merged with timestamp 0
        (2, "A".to_string()), // Will be merged into [0, 3)
        (5, "B".to_string()),
        (6, "B".to_string()), // Will be merged with timestamp 5
        (10, "A".to_string()), // New interval for "A"
    ];

    let cache = IntervalTreeCache::new(data)?;

    println!("Query at t=1: {:?}", cache.query_point(1));
    println!("Query at t=3: {:?}", cache.query_point(3));  // Gap
    println!("Query at t=5: {:?}", cache.query_point(5));
    println!("Query range [0, 10): {:?}", cache.query_range(0..10));

    Ok(())
}