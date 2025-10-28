//! `IntervalCache` implementation using the interavl crate (AVL-based interval tree).
//!
//! This implementation uses an AVL tree-based interval tree, which provides
//! guaranteed O(log n) operations and automatic balancing.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Range;

use interavl::IntervalTree;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// An interval cache implementation using the interavl AVL-based tree.
///
/// This provides guaranteed O(log n) operations with automatic balancing,
/// which can be beneficial for datasets with many updates or unbalanced intervals.
///
/// # ⚠️ NOT UNDER CONSIDERATION FOR PRODUCTION USE
///
/// This implementation has been removed from consideration due to a critical bug
/// that causes data loss when multiple distinct values exist at the same timestamp.
///
/// ## The Problem
///
/// When multiple different values exist at the same timestamp, `InteravlCache`
/// creates multiple intervals with identical ranges (e.g., both `[5, 6)`). When
/// these intervals are inserted into the `interavl` crate's `IntervalTree`,
/// **only the last one is retained** because the tree does not support duplicate ranges.
///
/// ### Example of Data Loss:
/// ```text
/// Input:  (5, RecordBatchRow { "a": 1 })
///         (5, RecordBatchRow { "a": 2 })
///
/// Expected: Both values at timestamp 5
/// Actual:   Only RecordBatchRow { "a": 2 } is retained
/// ```
///
/// ## Root Cause
///
/// The `interavl::IntervalTree::insert()` method overwrites any existing interval
/// with the same range. During tree construction (see `from_sorted`, lines 131-134),
/// when we insert:
/// - First:  `tree.insert([5, 6), index_0)` → value A
/// - Second: `tree.insert([5, 6), index_1)` → value B (overwrites index_0!)
///
/// ## Impact
///
/// Any dataset with multiple distinct values at the same timestamp will lose all
/// but the last value during tree construction and query operations.
///
/// ## Why Not Fixed?
///
/// Potential fixes would require fundamental changes:
/// 1. **Store `Vec<usize>` instead of `usize`** - Would require extensive refactoring
///    of the tree operations and query logic
/// 2. **Switch interval tree libraries** - The `interavl` crate is not designed for
///    duplicate ranges
/// 3. **Pre-group identical ranges** - Would add significant complexity and overhead
///
/// ## Alternative Implementations
///
/// Use one of these production-ready implementations instead:
/// - **`ValueAwareLapperCache`** - Fastest for most workloads, handles duplicates correctly
/// - **`LapperCache`** - Good balance of speed and memory, handles duplicates correctly
/// - **`VecCache`** - Simple and correct, handles duplicates correctly
/// - **`IntervalTreeCache`** - Uses a different tree library, handles duplicates correctly
///
/// ## References
///
/// - Bug documentation: `docs/INTERAVL_BUG_ROOT_CAUSE.md`
/// - Reproducer test: `test_known_bug_duplicate_timestamp_values()` (line 311)
pub struct InteravlCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Maps each interval to its associated value
    intervals: Vec<(Range<u64>, V)>,
    /// The AVL-based interval tree for efficient queries
    tree: IntervalTree<u64, usize>, // usize is index into intervals vec
}

impl<V> InteravlCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Merge adjacent or overlapping intervals with the same value.
    ///
    /// Takes a list of intervals and merges any that are adjacent (touching) or overlapping
    /// and have the same value.
    fn merge_intervals(mut intervals: Vec<(Range<u64>, V)>) -> Vec<(Range<u64>, V)> {
        if intervals.is_empty() {
            return intervals;
        }

        // Sort by start time, then by end time
        intervals.sort_by_key(|(range, _)| (range.start, range.end));

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
                merged.push((current_range, current_value));
                current_range = range;
                current_value = value;
            }
        }

        // Don't forget the last interval
        merged.push((current_range, current_value));
        merged
    }
}

impl<V> IntervalCache<V> for InteravlCache<V>
where
    V: Clone + Eq + Hash,
    for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        let mut intervals = Vec::new();
        let mut tree = IntervalTree::default();

        if points.is_empty() {
            return Ok(Self { intervals, tree });
        }

        // Build intervals by merging consecutive identical values
        let current_start = points[0].0;
        let current_end = points[0]
            .0
            .checked_add(1)
            .ok_or(CacheBuildError::TimestampOverflow(points[0].0))?;
        let current_value = points[0].1.clone();

        // Track open intervals for each value to handle overlapping
        let mut open_intervals: HashMap<V, (u64, u64)> = HashMap::new();
        open_intervals.insert(current_value.clone(), (current_start, current_end));

        for (t, v) in points.into_iter().skip(1) {
            let next_end = t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(t))?;

            match open_intervals.get_mut(&v) {
                Some((_, end)) if *end == t => {
                    // Extend existing interval
                    *end = next_end;
                }
                Some((start, end)) => {
                    // Gap detected - save old interval and start new one
                    let interval = *start..*end;
                    let idx = intervals.len();
                    intervals.push((interval.clone(), v.clone()));
                    tree.insert(interval, idx);

                    *start = t;
                    *end = next_end;
                }
                None => {
                    // New value - start tracking it
                    open_intervals.insert(v.clone(), (t, next_end));
                }
            }
        }

        // Flush all remaining open intervals
        for (v, (start, end)) in open_intervals {
            intervals.push((start..end, v));
        }

        // Merge any intervals that touch or overlap with same value
        let merged = Self::merge_intervals(intervals);

        // Rebuild the tree with merged intervals
        let mut tree = IntervalTree::default();
        for (idx, (interval, _)) in merged.iter().enumerate() {
            tree.insert(interval.clone(), idx);
        }

        Ok(Self {
            intervals: merged,
            tree,
        })
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let point_interval = t..(t + 1);

        self.tree
            .iter_overlaps(&point_interval)
            .filter_map(|(_, idx)| {
                self.intervals.get(*idx).map(|(_, v)| {
                    v.into_iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect()
                })
            })
            .collect()
    }

    fn query_range(&self, range: &Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        let mut seen = HashSet::new();
        self.tree
            .iter_overlaps(range)
            .filter_map(|(_, idx)| {
                self.intervals.get(*idx).and_then(|(_, v)| {
                    if seen.insert(v) {
                        Some(
                            v.into_iter()
                                .map(|(k, v)| (k.as_str(), v.as_str()))
                                .collect(),
                        )
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points
        let new_cache = Self::from_sorted(sorted_data)?;

        // Collect all intervals (existing + new)
        let mut all_intervals = self.intervals.clone();
        all_intervals.extend(new_cache.intervals);

        // Merge adjacent intervals with same value for better efficiency
        let merged = Self::merge_intervals(all_intervals);

        // Rebuild the tree with merged intervals
        self.intervals = merged;
        self.tree = IntervalTree::default();
        for (idx, (interval, _)) in self.intervals.iter().enumerate() {
            self.tree.insert(interval.clone(), idx);
        }

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the InteravlCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of the intervals vector
        // Each element is (Range<u64>, V)
        size += self.intervals.capacity()
            * (std::mem::size_of::<Range<u64>>() + std::mem::size_of::<V>());

        // Add heap size for values if they contain heap-allocated data
        for (_, value) in &self.intervals {
            size += value.heap_size();
        }

        // Size of the IntervalTree
        // The interavl tree stores intervals and indices
        // Estimate based on number of intervals
        let tree_node_overhead = 40; // Estimated overhead per AVL tree node
        size += self.intervals.len()
            * (
                std::mem::size_of::<Range<u64>>() +  // Interval range
            std::mem::size_of::<usize>() +        // Index value
            tree_node_overhead
                // Tree node overhead (pointers, balance factor, etc.)
            );

        size
    }

    fn interval_count(&self) -> usize {
        self.intervals.len()
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
    fn test_interavl_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (4, tag_b.clone())];

        let cache = InteravlCache::new(data).unwrap();

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
    fn test_interavl_cache_empty() {
        let cache: InteravlCache<TagSet> = InteravlCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(&(0..100)).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_interavl_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (3, tag_a.clone())];

        let cache = InteravlCache::new(data).unwrap();

        // Should have merged into 1 interval: [1,4)
        assert_eq!(cache.interval_count(), 1);
        assert!(!cache.query_point(1).is_empty());
        assert!(!cache.query_point(3).is_empty());
        assert_eq!(cache.query_point(4).len(), 0);
    }
}
