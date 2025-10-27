//! Minimal demo of dictionary encoding in ValueAwareLapperCache
//!
//! This shows how the new API works with direct string references from the dictionary.

use std::collections::BTreeSet;
use tag_values_cache::{IntervalCache, SortedData, ValueAwareLapperCache};

fn main() {
    println!("Minimal Dictionary Encoding Demo");
    println!("================================\n");

    // Create some TagSets
    let mut tag_set_server1 = BTreeSet::new();
    tag_set_server1.insert(("host".to_string(), "server1".to_string()));
    tag_set_server1.insert(("region".to_string(), "us-east".to_string()));

    let mut tag_set_server2 = BTreeSet::new();
    tag_set_server2.insert(("host".to_string(), "server2".to_string()));
    tag_set_server2.insert(("region".to_string(), "us-east".to_string()));

    // Create data with duplicate TagSets
    let mut data = Vec::new();
    for i in 0..10 {
        data.push((i * 10, tag_set_server1.clone()));
    }
    for i in 10..20 {
        data.push((i * 10, tag_set_server2.clone()));
    }
    // Add more server1 data to show reuse
    for i in 20..30 {
        data.push((i * 10, tag_set_server1.clone()));
    }

    println!("Created {} data points", data.len());

    // Create the cache
    let sorted_data = SortedData::from_sorted(data);
    let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

    // Get statistics
    let stats = cache.dictionary_stats();
    println!("\nDictionary Statistics:");
    println!("- Unique strings in dictionary: {}", stats.unique_entries);
    println!("- Unique TagSets: {}", cache.unique_tagsets());
    println!("- Dictionary size: {} bytes", stats.dictionary_size_bytes);
    println!("- Total intervals: {}\n", cache.interval_count());

    // Query using the IntervalCache trait API
    println!("Query Examples:");
    println!("------------------------");

    // Query at timestamp 50 (should be server1)
    let results = cache.query_point(50);
    println!("Query at t=50 returns {} TagSet(s)", results.len());
    for tagset in results {
        println!("  Tags: {:?}", tagset);
    }

    // Query at timestamp 150 (should be server2)
    let results = cache.query_point(150);
    println!("\nQuery at t=150:");
    for tagset in results {
        println!("  Tags: {:?}", tagset);
    }

    // Show that strings are shared in the dictionary
    println!("\n✅ Key insight: All tag keys and values are stored once in the dictionary!");
    println!("   - 'host', 'region', 'us-east' are each stored only once");
    println!("   - TagSets reference these strings by ID");
    println!("   - Query results return references directly into the dictionary");
}
