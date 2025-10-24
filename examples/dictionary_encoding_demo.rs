//! Demo of dictionary encoding in ValueAwareLapperCache
//!
//! This example shows how TagSets are encoded into dictionary IDs for memory efficiency.
//! Multiple identical TagSets will share the same dictionary ID.

use std::collections::BTreeSet;
use tag_values_cache::{SortedData, ValueAwareLapperCache, IntervalCache};

fn main() {
    println!("Dictionary Encoding Demo for ValueAwareLapperCache");
    println!("==================================================\n");

    // Create some TagSets with varying degrees of duplication
    let mut tag_set_server1 = BTreeSet::new();
    tag_set_server1.insert(("host".to_string(), "server1".to_string()));
    tag_set_server1.insert(("region".to_string(), "us-east".to_string()));
    tag_set_server1.insert(("service".to_string(), "web".to_string()));

    let mut tag_set_server2 = BTreeSet::new();
    tag_set_server2.insert(("host".to_string(), "server2".to_string()));
    tag_set_server2.insert(("region".to_string(), "us-east".to_string()));
    tag_set_server2.insert(("service".to_string(), "web".to_string()));

    let mut tag_set_server3 = BTreeSet::new();
    tag_set_server3.insert(("host".to_string(), "server3".to_string()));
    tag_set_server3.insert(("region".to_string(), "us-west".to_string()));
    tag_set_server3.insert(("service".to_string(), "api".to_string()));

    // Create data with many duplicate TagSets
    let mut data = Vec::new();

    // Add data for server1 (many points)
    for i in 0..100 {
        data.push((i * 10, tag_set_server1.clone()));
    }

    // Add data for server2 (many points)
    for i in 100..200 {
        data.push((i * 10, tag_set_server2.clone()));
    }

    // Add data for server3 (fewer points)
    for i in 200..250 {
        data.push((i * 10, tag_set_server3.clone()));
    }

    // Add more server1 data (to show same TagSet reused)
    for i in 250..350 {
        data.push((i * 10, tag_set_server1.clone()));
    }

    println!("Created {} data points with TagSets", data.len());
    println!("- 200 points for server1 (same TagSet)");
    println!("- 100 points for server2 (different TagSet)");
    println!("- 50 points for server3 (different TagSet)\n");

    // Create the cache with dictionary encoding
    let sorted_data = SortedData::from_sorted(data);
    let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

    // Get initial dictionary statistics
    let stats = cache.dictionary_stats();

    println!("Dictionary Encoding Statistics (Before Queries):");
    println!("------------------------------------------------");
    println!("Unique TagSet entries in dictionary: {}", stats.unique_entries);
    println!("Dictionary size: {} bytes", stats.dictionary_size_bytes);
    println!("Decoded cache size: {} bytes (lazily populated)", stats.cache_size_bytes);
    println!("Total intervals in cache: {}", cache.interval_count());
    println!("Total cache size: {} bytes\n", cache.size_bytes());

    // Demonstrate that queries still work correctly
    println!("Query Examples:");
    println!("--------------");

    // Query for server1 data
    let result = cache.query_point(100);
    println!("Query at timestamp 100 (server1): {} unique TagSets found", result.len());
    for tagset in result.iter().take(1) {
        println!("  TagSet: {:?}", tagset);
    }

    // Query for server2 data
    let result = cache.query_point(1500);
    println!("Query at timestamp 1500 (server2): {} unique TagSets found", result.len());
    for tagset in result.iter().take(1) {
        println!("  TagSet: {:?}", tagset);
    }

    // Query for server3 data
    let result = cache.query_point(2100);
    println!("Query at timestamp 2100 (server3): {} unique TagSets found", result.len());
    for tagset in result.iter().take(1) {
        println!("  TagSet: {:?}", tagset);
    }

    // Get statistics after queries (decoded cache is now populated)
    let final_stats = cache.dictionary_stats();

    println!("\n✅ Dictionary encoding is working! Only {} unique entries stored for {} data points",
             final_stats.unique_entries, 350);

    println!("\nDictionary Statistics After Queries:");
    println!("------------------------------------");
    println!("Decoded cache size: {} bytes (now populated with {} entries)",
             final_stats.cache_size_bytes, final_stats.unique_entries);

    // Calculate memory savings
    let raw_tagset_size = 350 * std::mem::size_of::<BTreeSet<(String, String)>>();
    let encoded_size = final_stats.dictionary_size_bytes + final_stats.cache_size_bytes;
    let savings = if raw_tagset_size > encoded_size {
        ((raw_tagset_size - encoded_size) as f64 / raw_tagset_size as f64) * 100.0
    } else {
        0.0
    };

    println!("Estimated memory savings: {:.1}% (excluding string content)", savings);
}