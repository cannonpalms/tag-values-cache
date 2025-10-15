//! IntervalCache implementation using the interavl crate (AVL-based interval tree).
//!
//! This implementation uses an AVL tree-based interval tree, which provides
//! guaranteed O(log n) operations and automatic balancing.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

use interavl::IntervalTree;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// An interval cache implementation using the interavl AVL-based tree.
///
/// This provides guaranteed O(log n) operations with automatic balancing,
/// which can be beneficial for datasets with many updates or unbalanced intervals.
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

    fn query_point(&self, t: Timestamp) -> Vec<&V> {
        let point_interval = t..(t + 1);

        self.tree
            .iter_overlaps(&point_interval)
            .filter_map(|(_, idx)| self.intervals.get(*idx).map(|(_, v)| v))
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        // Collect unique values to avoid duplicates
        let mut seen = std::collections::HashSet::new();
        self.tree
            .iter_overlaps(&range)
            .filter_map(|(_, idx)| {
                self.intervals.get(*idx).and_then(
                    |(_, v)| {
                        if seen.insert(v) { Some(v) } else { None }
                    },
                )
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

    #[test]
    fn test_interavl_cache_basic() {
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (2, "A".to_string()),
            (5, "B".to_string()),
            (6, "B".to_string()),
        ];

        let cache = InteravlCache::new(data).unwrap();

        // Check merged intervals
        assert_eq!(cache.query_point(0), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(1), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(2), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(3), Vec::<&String>::new());
        assert_eq!(cache.query_point(5), vec![&"B".to_string()]);
    }

    #[test]
    fn test_interavl_cache_overlapping() {
        let data = vec![
            (0, "X".to_string()),
            (1, "X".to_string()),
            (1, "Y".to_string()),
            (2, "Y".to_string()),
        ];

        let cache = InteravlCache::new(data).unwrap();

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.iter().any(|v| **v == "X".to_string()));
        assert!(values_at_1.iter().any(|v| **v == "Y".to_string()));
    }

    #[test]
    fn test_interavl_cache_range_query() {
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (10, "B".to_string()),
            (11, "B".to_string()),
            (20, "C".to_string()),
        ];

        let cache = InteravlCache::new(data).unwrap();

        let range_values = cache.query_range(0..15);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.iter().any(|v| **v == "A".to_string()));
        assert!(range_values.iter().any(|v| **v == "B".to_string()));
    }
}
