//! An unmerged BTree-based implementation that stores all intervals without merging.
//!
//! This implementation stores intervals in a `BTreeMap<Timestamp, Vec<(Timestamp, V)>>` where:
//! - Key: interval start time
//! - Value: vector of (end_time, value) tuples
//!
//! Unlike `BTreeCache`, this implementation does NOT merge adjacent or overlapping intervals
//! with the same value. Each discrete timestamp-value pair becomes its own interval.
//!
//! This can be useful for:
//! - Preserving exact input granularity
//! - Avoiding merge overhead during construction
//! - Comparing performance characteristics with merged implementations
//!
//! Performance characteristics:
//! - Point queries: O(log n + k) where k is the number of matching intervals
//! - Range queries: O(log n + k) where k is the number of matching intervals
//! - Build time: O(n) - faster than merged implementations (no merge pass)
//! - Memory usage: Higher than merged implementations (stores more intervals)

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::ops::Range;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// A cache implementation using `BTreeMap` that does not merge intervals.
///
/// This implementation stores every input timestamp as a separate interval
/// without attempting to merge adjacent or overlapping intervals with the same value.
///
/// Each interval represents exactly one timestamp from the input data and spans
/// [timestamp, timestamp+1).
///
/// Performance characteristics:
/// - Point queries: O(log n + k) where k is matching intervals
/// - Range queries: O(log n + k) where k is matching intervals
/// - Build time: O(n) - no merging overhead
/// - Memory: Higher than merged implementations
pub struct UnmergedBTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Maps interval start times to vectors of (end_time, value) tuples.
    /// The BTreeMap maintains intervals sorted by start time.
    /// Multiple intervals can share the same start time (stored in the Vec).
    intervals: BTreeMap<Timestamp, Vec<(Timestamp, V)>>,
}

impl<V> IntervalCache<V> for UnmergedBTreeCache<V>
where
    V: Clone + Eq + Hash,
    for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        if points.is_empty() {
            return Ok(Self {
                intervals: BTreeMap::new(),
            });
        }

        // Build intervals WITHOUT merging - each timestamp becomes its own interval
        let mut intervals = BTreeMap::new();

        for (t, v) in points {
            let end = t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(t))?;

            // Add this interval to the map
            // Multiple intervals can have the same start time
            intervals.entry(t).or_insert_with(Vec::new).push((end, v));
        }

        Ok(Self { intervals })
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let mut results = Vec::new();

        if self.intervals.is_empty() {
            return results;
        }

        // Use BTreeMap's range() to efficiently find intervals that could contain t.
        // We need to check all intervals whose start <= t
        // The range query gives us an iterator over intervals in sorted order
        for (&start, intervals_at_start) in self.intervals.range(..=t) {
            // Check each interval that starts at this timestamp
            for &(end, ref value) in intervals_at_start {
                // Only include if the interval actually contains t
                // Interval is [start, end) so we need start <= t < end
                if start <= t && t < end {
                    let tag_vec: Vec<(&str, &str)> = value
                        .into_iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    results.push(tag_vec);
                }
            }
        }

        results
    }

    fn query_range(&self, range: &Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        if self.intervals.is_empty() {
            return results;
        }

        // Use BTreeMap's range() to efficiently find intervals that could overlap.
        // We need intervals whose start < range.end (they might overlap)
        // An interval [start, end) overlaps with [range.start, range.end) if:
        // start < range.end AND end > range.start
        for (_, intervals_at_start) in self.intervals.range(..range.end) {
            for &(end, ref value) in intervals_at_start {
                // Check if this interval actually overlaps the query range
                if end > range.start {
                    // Deduplicate based on the value
                    if seen.insert(value) {
                        let tag_vec: Vec<(&str, &str)> = value
                            .into_iter()
                            .map(|(k, v)| (k.as_str(), v.as_str()))
                            .collect();
                        results.push(tag_vec);
                    }
                }
            }
        }

        results
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        let points = sorted_data.into_inner();

        if points.is_empty() {
            return Ok(());
        }

        // Simply add new intervals without merging
        for (t, v) in points {
            let end = t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(t))?;

            self.intervals.entry(t).or_default().push((end, v));
        }

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the UnmergedBTreeCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // BTreeMap has overhead for tree nodes (approximately 40 bytes per node)
        const NODE_OVERHEAD: usize = 40;
        size += self.intervals.len() * NODE_OVERHEAD;

        // Size of keys (Timestamp = u64)
        size += self.intervals.len() * std::mem::size_of::<Timestamp>();

        // For each entry in the BTreeMap, we have a Vec
        for intervals_at_start in self.intervals.values() {
            // Vec overhead
            size += std::mem::size_of::<Vec<(Timestamp, V)>>();

            // Capacity of the Vec
            size += intervals_at_start.capacity() * std::mem::size_of::<(Timestamp, V)>();

            // Add heap size for values if they contain heap-allocated data
            for (_, value) in intervals_at_start {
                size += value.heap_size();
            }
        }

        size
    }

    fn interval_count(&self) -> usize {
        // Count total number of intervals across all start times
        self.intervals.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TagSet;

    fn make_tagset(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_unmerged_btree_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (4, tag_b.clone())];

        let cache = UnmergedBTreeCache::new(data).unwrap();

        let result1 = cache.query_point(1);
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0], vec![("host", "server1")]);

        let result2 = cache.query_point(2);
        assert_eq!(result2.len(), 1);
        assert_eq!(result2[0], vec![("host", "server1")]);

        assert_eq!(cache.query_point(3).len(), 0);

        let result4 = cache.query_point(4);
        assert_eq!(result4.len(), 1);
        assert_eq!(result4[0], vec![("host", "server2")]);
    }

    #[test]
    fn test_unmerged_btree_cache_empty() {
        let cache: UnmergedBTreeCache<TagSet> = UnmergedBTreeCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(&(0..100)).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_unmerged_btree_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (3, tag_a.clone())];

        let cache = UnmergedBTreeCache::new(data).unwrap();

        // UnmergedBTreeCache does NOT merge, so should have 3 separate intervals
        assert_eq!(cache.interval_count(), 3);
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert_eq!(cache.query_point(4).len(), 0);
    }
}
