//! Generic comparison utilities for different cache implementations.
//!
//! This module demonstrates how the IntervalCache trait enables
//! generic programming and comparison of different implementations.

use std::ops::Range;
use std::time::{Duration, Instant};

use crate::{IntervalCache, SortedData, Timestamp};

/// Generic function that works with any IntervalCache implementation.
///
/// This demonstrates how we can write code that works with any cache
/// implementation, not tied to a specific data structure.
pub fn analyze_cache<C, V>(cache: &C, test_points: &[Timestamp])
where
    C: IntervalCache<V>,
    V: Clone + Eq + std::hash::Hash + std::fmt::Debug,
{
    println!("Cache Analysis:");
    println!("--------------");

    for &t in test_points {
        let values = cache.query_point(t);
        println!("  Time {}: {} values found", t, values.len());
    }
}

/// Compare performance of two cache implementations.
///
/// This function accepts two different cache implementations and
/// compares their query performance on the same set of test queries.
pub fn compare_implementations<C1, C2, V>(
    cache1: &C1,
    cache2: &C2,
    name1: &str,
    name2: &str,
    test_points: &[Timestamp],
    test_ranges: &[Range<Timestamp>],
) where
    C1: IntervalCache<V>,
    C2: IntervalCache<V>,
    V: Clone + Eq + std::hash::Hash,
{
    println!("\nPerformance Comparison");
    println!("======================");

    // Compare point queries
    println!("\nPoint Queries ({} points):", test_points.len());

    let start = Instant::now();
    for &t in test_points {
        let _ = cache1.query_point(t);
    }
    let time1 = start.elapsed();

    let start = Instant::now();
    for &t in test_points {
        let _ = cache2.query_point(t);
    }
    let time2 = start.elapsed();

    println!("  {}: {:?}", name1, time1);
    println!("  {}: {:?}", name2, time2);

    // Compare range queries
    println!("\nRange Queries ({} ranges):", test_ranges.len());

    let start = Instant::now();
    for range in test_ranges {
        let _ = cache1.query_range(range.clone());
    }
    let time1 = start.elapsed();

    let start = Instant::now();
    for range in test_ranges {
        let _ = cache2.query_range(range.clone());
    }
    let time2 = start.elapsed();

    println!("  {}: {:?}", name1, time1);
    println!("  {}: {:?}", name2, time2);
}

/// Verify that two cache implementations produce identical results.
///
/// This is useful for testing that different implementations are
/// semantically equivalent.
pub fn verify_equivalence<C1, C2, V>(
    cache1: &C1,
    cache2: &C2,
    test_points: &[Timestamp],
    test_ranges: &[Range<Timestamp>],
) -> bool
where
    C1: IntervalCache<V>,
    C2: IntervalCache<V>,
    V: Clone + Eq + std::hash::Hash + Ord + std::fmt::Debug,
{
    // Check point queries
    for &t in test_points {
        let mut results1 = cache1.query_point(t);
        let mut results2 = cache2.query_point(t);

        // Sort for comparison (order might differ)
        results1.sort();
        results2.sort();

        if results1 != results2 {
            println!("Mismatch at point {}: {:?} vs {:?}", t, results1, results2);
            return false;
        }
    }

    // Check range queries
    for range in test_ranges {
        let mut results1 = cache1.query_range(range.clone());
        let mut results2 = cache2.query_range(range.clone());

        // Sort for comparison
        results1.sort();
        results2.sort();

        if results1 != results2 {
            println!(
                "Mismatch in range {:?}: {:?} vs {:?}",
                range, results1, results2
            );
            return false;
        }
    }

    true
}

/// Benchmark the build time of a cache implementation.
///
/// Measures how long it takes to build a cache from sorted data.
pub fn benchmark_build<C, V, F>(name: &str, sorted_data: SortedData<V>, build_fn: F) -> Duration
where
    V: Clone + Eq + std::hash::Hash,
    F: FnOnce(SortedData<V>) -> Result<C, crate::CacheBuildError>,
{
    let start = Instant::now();
    let _ = build_fn(sorted_data).expect("Failed to build cache");
    let duration = start.elapsed();

    println!("  {}: {:?}", name, duration);
    duration
}

/// Benchmark append operations on different cache implementations.
pub fn benchmark_append<C, V>(cache: &mut C, name: &str, batches: Vec<SortedData<V>>) -> Duration
where
    C: IntervalCache<V>,
    V: Clone + Eq + std::hash::Hash,
{
    let mut total_duration = Duration::default();

    for (i, batch) in batches.into_iter().enumerate() {
        let start = Instant::now();
        cache.append_sorted(batch).expect("Failed to append batch");
        let duration = start.elapsed();
        total_duration += duration;

        if i == 0 || i % 10 == 9 {
            println!("  {} - Batch {}: {:?}", name, i + 1, duration);
        }
    }

    println!("  {} - Total append time: {:?}", name, total_duration);
    total_duration
}

/// Compare build times for multiple cache implementations.
pub fn compare_build_times<V>(data: Vec<(Timestamp, V)>, iterations: usize)
where
    V: Clone + Eq + std::hash::Hash + Ord,
{
    println!("\nBuild Time Comparison ({} iterations)", iterations);
    println!("==================================");

    // Warm up
    let _ = crate::IntervalTreeCache::from_sorted(SortedData::from_unsorted(data.clone()));
    let _ = crate::VecCache::from_sorted(SortedData::from_unsorted(data.clone()));
    let _ = crate::InteravlCache::from_sorted(SortedData::from_unsorted(data.clone()));

    let mut tree_times = Vec::new();
    let mut vec_times = Vec::new();
    let mut avl_times = Vec::new();

    for _ in 0..iterations {
        let sorted = SortedData::from_unsorted(data.clone());

        let start = Instant::now();
        let _ = crate::IntervalTreeCache::from_sorted(sorted.clone());
        tree_times.push(start.elapsed());

        let start = Instant::now();
        let _ = crate::VecCache::from_sorted(sorted.clone());
        vec_times.push(start.elapsed());

        let start = Instant::now();
        let _ = crate::InteravlCache::from_sorted(sorted);
        avl_times.push(start.elapsed());
    }

    // Calculate averages
    let tree_avg = tree_times.iter().sum::<Duration>() / iterations as u32;
    let vec_avg = vec_times.iter().sum::<Duration>() / iterations as u32;
    let avl_avg = avl_times.iter().sum::<Duration>() / iterations as u32;

    println!("\nAverage build times:");
    println!("  IntervalTreeCache: {:?}", tree_avg);
    println!("  VecCache: {:?}", vec_avg);
    println!("  InteravlCache: {:?}", avl_avg);

    // Find the fastest
    let min_time = tree_avg.min(vec_avg).min(avl_avg);
    let fastest = if min_time == vec_avg {
        "VecCache"
    } else if min_time == tree_avg {
        "IntervalTreeCache"
    } else {
        "InteravlCache"
    };

    println!("\nFastest: {}", fastest);
}
