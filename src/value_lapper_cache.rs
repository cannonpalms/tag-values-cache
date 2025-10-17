//! `IntervalCache` implementation using ValueLapper for value-aware merging.
//!
//! This implementation provides the same interface as LapperCache but uses
//! ValueLapper internally, which only merges intervals when both boundaries
//! AND values match.

use std::ops::Range;

use rust_lapper::Interval;

use crate::{value_lapper::ValueLapper, CacheBuildError, HeapSize, IntervalCache, SortedData, Timestamp};

/// An interval cache implementation using ValueLapper.
///
/// Unlike LapperCache which uses a HashMap to separate intervals by value,
/// ValueLapperCache uses a single ValueLapper instance that handles value-aware
/// merging internally through sorting.
pub struct ValueLapperCache<V>
where
    V: Clone + Eq + Ord + std::hash::Hash + Send + Sync,
{
    /// The ValueLapper instance containing all intervals
    value_lapper: ValueLapper<u64, V>,
}

impl<V> ValueLapperCache<V>
where
    V: Clone + Eq + Ord + std::hash::Hash + Send + Sync,
{
    /// Build intervals from sorted timestamp-value pairs.
    ///
    /// Consecutive timestamps with the same value are merged into continuous intervals.
    fn build_intervals(points: Vec<(Timestamp, V)>) -> Result<Vec<Interval<u64, V>>, CacheBuildError> {
        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(intervals);
        }

        // Build intervals by merging consecutive identical values
        // Track open intervals for each value to handle overlapping
        let mut open_intervals: std::collections::HashMap<V, (u64, u64)> = std::collections::HashMap::new();

        for (t, v) in points {
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
                    intervals.push(Interval {
                        start: *start,
                        stop: *end,
                        val: v.clone(),
                    });

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
            intervals.push(Interval {
                start,
                stop: end,
                val: v,
            });
        }

        Ok(intervals)
    }
}

impl<V> IntervalCache<V> for ValueLapperCache<V>
where
    V: Clone + Eq + Ord + std::hash::Hash + Send + Sync,
{
    fn from_sorted(sorted_data: SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();
        let intervals = Self::build_intervals(points)?;

        let mut value_lapper = ValueLapper::new(intervals);
        value_lapper.merge_with_values();

        Ok(Self { value_lapper })
    }

    fn query_point(&self, t: Timestamp) -> Vec<&V> {
        let start = t;
        let stop = t + 1;

        self.value_lapper
            .find(start, stop)
            .map(|interval| &interval.val)
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        let start = range.start;
        let stop = range.end;

        // Collect unique values
        let mut seen = std::collections::HashSet::new();
        let mut results = Vec::new();

        for interval in self.value_lapper.find(start, stop) {
            if seen.insert(&interval.val) {
                results.push(&interval.val);
            }
        }

        results
    }

    fn append_sorted(&mut self, sorted_data: SortedData<V>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points
        let new_intervals = Self::build_intervals(sorted_data.into_inner())?;

        // Collect all existing intervals
        let mut all_intervals: Vec<_> = self.value_lapper.iter().cloned().collect();
        all_intervals.extend(new_intervals);

        // Rebuild with all intervals and merge
        self.value_lapper = ValueLapper::new(all_intervals);
        self.value_lapper.merge_with_values();

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of all intervals in the ValueLapper
        size += self.value_lapper.len() * std::mem::size_of::<Interval<u64, V>>();

        // Add heap size for values
        for interval in self.value_lapper.iter() {
            size += interval.val.heap_size();
        }

        size
    }

    fn interval_count(&self) -> usize {
        self.value_lapper.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_lapper_cache_basic() {
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (2, "A".to_string()),
            (5, "B".to_string()),
            (6, "B".to_string()),
        ];

        let cache = ValueLapperCache::new(data).unwrap();

        // Check merged intervals
        assert_eq!(cache.query_point(0), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(1), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(2), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(3), Vec::<&String>::new());
        assert_eq!(cache.query_point(5), vec![&"B".to_string()]);
    }

    #[test]
    fn test_value_lapper_cache_overlapping() {
        let data = vec![
            (0, "X".to_string()),
            (1, "X".to_string()),
            (1, "Y".to_string()),
            (2, "Y".to_string()),
        ];

        let cache = ValueLapperCache::new(data).unwrap();

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.iter().any(|v| **v == "X".to_string()));
        assert!(values_at_1.iter().any(|v| **v == "Y".to_string()));
    }

    #[test]
    fn test_value_lapper_cache_range_query() {
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (10, "B".to_string()),
            (11, "B".to_string()),
            (20, "C".to_string()),
        ];

        let cache = ValueLapperCache::new(data).unwrap();

        let range_values = cache.query_range(0..15);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.iter().any(|v| **v == "A".to_string()));
        assert!(range_values.iter().any(|v| **v == "B".to_string()));
    }

    #[test]
    fn test_value_lapper_cache_append() {
        let initial_data = vec![(0, "A".to_string()), (1, "A".to_string())];

        let mut cache = ValueLapperCache::new(initial_data).unwrap();

        let append_data = vec![(5, "B".to_string()), (6, "B".to_string())];

        cache.append_batch(append_data).unwrap();

        assert_eq!(cache.query_point(0), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(5), vec![&"B".to_string()]);
    }

    #[test]
    fn test_value_aware_merging() {
        // Test that intervals with same boundaries but different values don't merge
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (0, "B".to_string()),
            (1, "B".to_string()),
        ];

        let cache = ValueLapperCache::new(data).unwrap();

        // Both values should be present at timestamp 0 and 1
        let values_at_0 = cache.query_point(0);
        assert_eq!(values_at_0.len(), 2);
        assert!(values_at_0.iter().any(|v| **v == "A".to_string()));
        assert!(values_at_0.iter().any(|v| **v == "B".to_string()));

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.iter().any(|v| **v == "A".to_string()));
        assert!(values_at_1.iter().any(|v| **v == "B".to_string()));

        // Should have 2 intervals total (one for A, one for B)
        assert_eq!(cache.interval_count(), 2);
    }
}
