//! A BTree-based implementation using `BTreeMap` for efficient interval storage and queries.
//!
//! This implementation stores intervals in a `BTreeMap<Timestamp, (Timestamp, V)>` where:
//! - Key: interval start time
//! - Value: tuple of (end_time, value)
//!
//! The sorted nature of `BTreeMap` enables efficient range queries using the `range()` method
//! to quickly find intervals that might overlap with the query range.
//!
//! Performance characteristics:
//! - Point queries: O(log n + k) where k is the number of matching intervals
//! - Range queries: O(log n + k) where k is the number of matching intervals
//! - Insertion/append: O(n log n) for merging all intervals
//!
//! The BTree structure provides good cache locality for range queries and automatic
//! ordering of intervals by start time. However, it has higher memory overhead compared
//! to vector-based implementations due to tree node allocations.

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::ops::Range;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// A cache implementation using `BTreeMap` for interval storage.
///
/// This implementation leverages the ordered nature of `BTreeMap` to efficiently
/// query intervals. Each entry maps a start timestamp to its corresponding end
/// timestamp and value.
///
/// The BTree structure provides O(log n) lookup times and efficient range
/// iteration, making it well-suited for both point and range queries.
///
/// Performance characteristics:
/// - Point queries: O(log n + k) where k is matching intervals
/// - Range queries: O(log n + k) where k is matching intervals
/// - Memory overhead: ~40 bytes per BTree node plus key/value storage
pub struct BTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Maps interval start times to (end_time, value) tuples.
    /// The BTreeMap maintains intervals sorted by start time.
    intervals: BTreeMap<Timestamp, (Timestamp, V)>,
}

impl<V> BTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Merge adjacent or overlapping intervals with the same value.
    ///
    /// Takes a list of intervals and merges any that are adjacent (touching) or overlapping
    /// and have the same value. Also removes any duplicate intervals.
    ///
    /// # Arguments
    /// * `intervals` - Vector of (Range, Value) pairs to merge
    ///
    /// # Returns
    /// A new vector with merged intervals
    fn merge_intervals(
        mut intervals: Vec<(Range<Timestamp>, V)>,
    ) -> Vec<(Range<Timestamp>, V)> {
        if intervals.is_empty() {
            return intervals;
        }

        // Sort by start time, then by end time
        intervals.sort_by_key(|interval| (interval.0.start, interval.0.end));

        // Remove exact duplicates
        intervals.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        // Merge adjacent/overlapping intervals with same value
        let mut merged = Vec::new();
        let mut iter = intervals.into_iter();
        let (first_range, first_value) = iter.next().unwrap();

        let mut current_range = first_range;
        let mut current_value = first_value;

        for (range, value) in iter {
            if value == current_value && range.start <= current_range.end {
                // Adjacent or overlapping with same value - merge
                current_range.end = current_range.end.max(range.end);
            } else {
                // Different value or gap - save current and start new
                merged.push((current_range.clone(), current_value.clone()));
                current_range = range;
                current_value = value;
            }
        }

        // Don't forget the last interval
        merged.push((current_range, current_value));

        // Final deduplication check (in case merging created duplicates)
        merged.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        merged
    }
}

impl<V> IntervalCache<V> for BTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        if points.is_empty() {
            return Ok(Self {
                intervals: BTreeMap::new(),
            });
        }

        // Build intervals by merging consecutive identical values
        let mut temp_intervals = Vec::new();
        let mut current_start = points[0].0;
        let mut current_end = points[0]
            .0
            .checked_add(1)
            .ok_or(CacheBuildError::TimestampOverflow(points[0].0))?;
        let mut current_value = points[0].1.clone();

        for (t, v) in points.into_iter().skip(1) {
            let next_end = t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(t))?;

            if v == current_value && current_end == t {
                // Extend current interval
                current_end = next_end;
            } else {
                // Save current interval and start new one
                temp_intervals.push((current_start..current_end, current_value));

                current_start = t;
                current_end = next_end;
                current_value = v;
            }
        }

        // Don't forget the last interval
        temp_intervals.push((current_start..current_end, current_value));

        // Merge any intervals that touch or overlap with same value
        let merged = Self::merge_intervals(temp_intervals);

        // Build BTreeMap from merged intervals
        let mut intervals = BTreeMap::new();
        for (range, value) in merged {
            intervals.insert(range.start, (range.end, value));
        }

        Ok(Self { intervals })
    }

    fn query_point(&self, t: Timestamp) -> HashSet<&V> {
        let mut results = HashSet::new();

        if self.intervals.is_empty() {
            return results;
        }

        // Use BTreeMap's range() to efficiently find intervals that could contain t.
        // We need to check all intervals whose start <= t
        // The range query gives us an iterator over intervals in sorted order
        for (&start, &(end, ref value)) in self.intervals.range(..=t) {
            // Only include if the interval actually contains t
            // Interval is [start, end) so we need start <= t < end
            if start <= t && t < end {
                results.insert(value);
            }
        }

        results
    }

    fn query_range(&self, range: Range<Timestamp>) -> HashSet<&V> {
        let mut results = HashSet::new();

        if self.intervals.is_empty() {
            return results;
        }

        // Use BTreeMap's range() to efficiently find intervals that could overlap.
        // We need intervals whose start < range.end (they might overlap)
        // An interval [start, end) overlaps with [range.start, range.end) if:
        // start < range.end AND end > range.start
        for (_, &(end, ref value)) in self.intervals.range(..range.end) {
            // Check if this interval actually overlaps the query range
            if end > range.start {
                results.insert(value);
            }
        }

        results
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        // Build intervals from sorted points
        let new_cache = Self::from_sorted(sorted_data)?;

        // If existing cache is empty, just replace with new
        if self.intervals.is_empty() {
            self.intervals = new_cache.intervals;
            return Ok(());
        }

        // If new cache is empty, nothing to do
        if new_cache.intervals.is_empty() {
            return Ok(());
        }

        // Collect all intervals as Range objects for merging
        let mut all_intervals: Vec<(Range<Timestamp>, V)> = Vec::new();

        // Collect existing intervals
        for (&start, &(end, ref value)) in &self.intervals {
            all_intervals.push((start..end, value.clone()));
        }

        // Add new intervals
        for (&start, &(end, ref value)) in &new_cache.intervals {
            all_intervals.push((start..end, value.clone()));
        }

        // Merge intervals using the same logic
        let merged = Self::merge_intervals(all_intervals);

        // Rebuild the BTreeMap from merged intervals
        self.intervals.clear();
        for (range, value) in merged {
            self.intervals.insert(range.start, (range.end, value));
        }

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the BTreeCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // BTreeMap has overhead for tree nodes (approximately 40 bytes per node)
        // plus the size of keys and values
        const NODE_OVERHEAD: usize = 40;
        size += self.intervals.len() * NODE_OVERHEAD;

        // Size of keys (Timestamp = u64)
        size += self.intervals.len() * std::mem::size_of::<Timestamp>();

        // Size of values (tuple of (Timestamp, V))
        size += self.intervals.len() * std::mem::size_of::<(Timestamp, V)>();

        // Add heap size for values if they contain heap-allocated data
        for (_, (_, value)) in &self.intervals {
            size += value.heap_size();
        }

        size
    }

    fn interval_count(&self) -> usize {
        self.intervals.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_cache_basic() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (4, "B".to_string()),
        ];

        let cache = BTreeCache::new(data).unwrap();

        assert_eq!(cache.query_point(1), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(2), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(3), HashSet::<&String>::new());
        assert_eq!(cache.query_point(4), HashSet::from([&"B".to_string()]));
    }

    #[test]
    fn test_btree_cache_range() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (5, "B".to_string()),
            (6, "C".to_string()),
        ];

        let cache = BTreeCache::new(data).unwrap();

        let range_values = cache.query_range(1..6);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&&"A".to_string()));
        assert!(range_values.contains(&&"B".to_string()));
    }

    #[test]
    fn test_btree_cache_empty() {
        let cache: BTreeCache<String> = BTreeCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1), HashSet::new());
        assert_eq!(cache.query_range(0..100), HashSet::new());
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_btree_cache_single_point() {
        let data = vec![(5, "X".to_string())];
        let cache = BTreeCache::new(data).unwrap();

        assert_eq!(cache.query_point(5), HashSet::from([&"X".to_string()]));
        assert_eq!(cache.query_point(4), HashSet::new());
        assert_eq!(cache.query_point(6), HashSet::new());
        assert_eq!(cache.interval_count(), 1);
    }

    #[test]
    fn test_btree_cache_merge_intervals() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (3, "A".to_string()),
            (5, "B".to_string()),
            (6, "B".to_string()),
        ];

        let cache = BTreeCache::new(data).unwrap();

        // Should have merged into 2 intervals: [1,4) with "A" and [5,7) with "B"
        assert_eq!(cache.interval_count(), 2);
        assert_eq!(cache.query_point(1), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(3), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(4), HashSet::new());
        assert_eq!(cache.query_point(5), HashSet::from([&"B".to_string()]));
    }

    #[test]
    fn test_btree_cache_append() {
        let data = vec![(1, "A".to_string()), (2, "A".to_string())];
        let mut cache = BTreeCache::new(data).unwrap();

        let new_data = vec![(3, "A".to_string()), (5, "B".to_string())];
        cache.append_batch(new_data).unwrap();

        // Should merge [1,3) with [3,4) into [1,4)
        assert_eq!(cache.query_point(1), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(3), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(5), HashSet::from([&"B".to_string()]));
    }

    #[test]
    fn test_btree_cache_range_boundary() {
        let data = vec![
            (1, "A".to_string()),
            (5, "B".to_string()),
            (10, "C".to_string()),
        ];

        let cache = BTreeCache::new(data).unwrap();

        // Range [1..2) should only contain "A" (interval [1,2))
        let result = cache.query_range(1..2);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&&"A".to_string()));

        // Range [0..1) should be empty (before any intervals)
        let result = cache.query_range(0..1);
        assert_eq!(result.len(), 0);

        // Range [2..5) should be empty (gap between intervals)
        let result = cache.query_range(2..5);
        assert_eq!(result.len(), 0);

        // Range [1..6) should contain "A" and "B"
        let result = cache.query_range(1..6);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&&"A".to_string()));
        assert!(result.contains(&&"B".to_string()));
    }
}
