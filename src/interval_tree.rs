//! IntervalTree-based implementation of the `IntervalCache` trait.
//!
//! This implementation uses an interval tree data structure to efficiently
//! store and query time intervals. It merges consecutive timestamps with
//! identical values into continuous intervals.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

use intervaltree::IntervalTree;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// An interval cache implementation using an interval tree.
///
/// This structure efficiently handles overlapping intervals and provides
/// O(log n + k) query performance where n is the number of intervals and
/// k is the number of matching intervals.
pub struct IntervalTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    tree: IntervalTree<u64, V>,
}

impl<V> IntervalCache<V> for IntervalTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        // No sorting needed - data is guaranteed to be sorted
        let points = sorted_data.into_inner();
        let temp_tree = Self::build_multivalued_tree(points)?;

        // Extract all intervals and merge any that touch or overlap with same value
        let intervals: Vec<(Range<u64>, V)> = temp_tree
            .iter()
            .map(|entry| (entry.range.clone(), entry.value.clone()))
            .collect();

        let merged = Self::merge_intervals(intervals);
        let tree = merged.into_iter().collect();

        Ok(Self { tree })
    }

    fn query_point(&self, t: Timestamp) -> Vec<&V> {
        self.query_point_impl(t)
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        self.query_range_impl(range)
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        let new_points = sorted_data.into_inner();
        if new_points.is_empty() {
            return Ok(());
        }

        // Build new intervals from the sorted points
        let new_tree = Self::build_multivalued_tree(new_points)?;

        // Merge the new tree with the existing one
        let mut all_intervals = Vec::new();

        // Collect existing intervals
        for entry in &self.tree {
            all_intervals.push((entry.range.clone(), entry.value.clone()));
        }

        // Collect new intervals
        for entry in &new_tree {
            all_intervals.push((entry.range.clone(), entry.value.clone()));
        }

        // Merge adjacent/overlapping intervals with same value
        let merged = Self::merge_intervals(all_intervals);
        self.tree = merged.into_iter().collect();

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the IntervalTreeCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // Count intervals and estimate their sizes
        let mut interval_count = 0;
        let mut total_value_size = 0;

        for entry in &self.tree {
            interval_count += 1;
            // Each interval entry contains:
            // - Range<u64>: 16 bytes (2 u64s)
            // - The value: we need to estimate its size
            total_value_size += std::mem::size_of::<V>() + entry.value.heap_size();
        }

        // IntervalTree internal structure overhead
        // The intervaltree crate uses a sorted Vec internally
        // Each entry has: Range (16 bytes) + value + some overhead for the tree node
        // Estimate ~32 bytes overhead per entry for tree structure
        size += interval_count * (16 + 32) + total_value_size;

        size
    }

    fn interval_count(&self) -> usize {
        self.tree.iter().count()
    }
}

impl<V> IntervalTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Merge adjacent or overlapping intervals with the same value.
    ///
    /// Takes a list of intervals and merges any that are adjacent (touching) or overlapping
    /// and have the same value. Also removes any duplicate intervals.
    fn merge_intervals(mut intervals: Vec<(Range<u64>, V)>) -> Vec<(Range<u64>, V)> {
        if intervals.is_empty() {
            return intervals;
        }

        // First, remove exact duplicates
        // Sort to group identical intervals together
        // We only sort by interval bounds, not values
        intervals.sort_by_key(|interval| (interval.0.start, interval.0.end));

        // Deduplicate identical intervals
        intervals.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        // Now merge adjacent/overlapping intervals with same value
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

    /// Build an interval tree from discrete timestamp-value pairs.
    ///
    /// Consecutive timestamps with the same value are merged into single intervals.
    /// For example: [(1, "A"), (2, "A"), (4, "B")] becomes:
    /// - Interval [1, 3) with value "A"
    /// - Interval [4, 5) with value "B"
    fn build_multivalued_tree(
        points: Vec<(Timestamp, V)>,
    ) -> Result<IntervalTree<u64, V>, CacheBuildError> {
        // Map: value -> (current_run_start, current_run_end)
        let mut open: HashMap<V, (u64, u64)> = HashMap::new();
        let mut runs: Vec<(Range<u64>, V)> = Vec::new();

        for (t, v) in points {
            // Check for overflow when creating interval [t, t+1)
            let end = t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(t))?;

            // If we saw this value last at exactly t-1, extend; otherwise flush/start new.
            match open.get_mut(&v) {
                Some((start, last_end)) => {
                    if *last_end == t {
                        // Consecutive timestamp - extend the current run
                        *last_end = end;
                    } else {
                        // Gap detected - flush old run and start new one
                        runs.push((*start..*last_end, v.clone()));
                        *start = t;
                        *last_end = end;
                    }
                }
                None => {
                    // First occurrence of this value
                    open.insert(v.clone(), (t, end));
                }
            }
        }

        // Flush all remaining open runs
        for (v, (start, end)) in open {
            runs.push((start..end, v));
        }

        // Build the interval tree (overlapping runs are supported)
        Ok(runs.into_iter().collect())
    }

    /// Query for all values at a specific timestamp.
    fn query_point_impl(&self, t: u64) -> Vec<&V> {
        self.tree.query_point(t).map(|entry| &entry.value).collect()
    }

    /// Query for all values within a range.
    fn query_range_impl(&self, range: Range<u64>) -> Vec<&V> {
        // Collect unique values to avoid duplicates
        let mut seen = std::collections::HashSet::new();
        self.tree
            .query(range)
            .filter_map(|entry| {
                if seen.insert(&entry.value) {
                    Some(&entry.value)
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consecutive_values_merged() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (3, "A".to_string()),
            (5, "B".to_string()),
            (6, "B".to_string()),
        ];

        let cache = IntervalTreeCache::new(data).unwrap();

        // Check that consecutive "A" values are merged
        assert_eq!(cache.query_point(1), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(2), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(3), vec![&"A".to_string()]);

        // Gap at timestamp 4
        assert_eq!(cache.query_point(4), Vec::<&String>::new());

        // Check "B" values
        assert_eq!(cache.query_point(5), vec![&"B".to_string()]);
        assert_eq!(cache.query_point(6), vec![&"B".to_string()]);
    }

    #[test]
    fn test_multiple_values_same_timestamp() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (2, "B".to_string()), // Different value at same timestamp
            (3, "B".to_string()),
        ];

        let cache = IntervalTreeCache::new(data).unwrap();

        // At timestamp 2, both runs should be active
        let values_at_2 = cache.query_point(2);
        assert_eq!(values_at_2.len(), 2);
        assert!(values_at_2.contains(&&"A".to_string()));
        assert!(values_at_2.contains(&&"B".to_string()));
    }

    #[test]
    fn test_range_query() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (5, "B".to_string()),
            (6, "C".to_string()),
        ];

        let cache = IntervalTreeCache::new(data).unwrap();

        // Query range [1, 6) should return A and B but not C (since 6 is exclusive)
        let range_values = cache.query_range(1..6);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.iter().any(|v| **v == "A".to_string()));
        assert!(range_values.iter().any(|v| **v == "B".to_string()));
    }

    #[test]
    fn test_unsorted_input() {
        let data = vec![
            (5, "B".to_string()),
            (1, "A".to_string()),
            (2, "A".to_string()),
            (6, "B".to_string()),
        ];

        let cache = IntervalTreeCache::new(data).unwrap();

        // Should still correctly merge consecutive values after sorting
        assert_eq!(cache.query_point(1), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(2), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(5), vec![&"B".to_string()]);
        assert_eq!(cache.query_point(6), vec![&"B".to_string()]);
    }

    #[test]
    fn test_timestamp_overflow() {
        let data = vec![(u64::MAX, "A".to_string())];

        let result = IntervalTreeCache::new(data);
        assert!(matches!(result, Err(CacheBuildError::TimestampOverflow(_))));
    }
}
