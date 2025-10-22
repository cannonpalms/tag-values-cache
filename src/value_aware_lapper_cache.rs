//! `IntervalCache` implementation using ValueAwareLapper for value-aware merging.
//!
//! This implementation provides the same interface as LapperCache but uses
//! ValueAwareLapper internally, which only merges intervals when both boundaries
//! AND values match.

use std::collections::HashSet;
use std::ops::Range;
use std::time::Duration;

use rust_lapper::Interval;

use crate::{
    CacheBuildError, HeapSize, IntervalCache, SortedData, Timestamp,
    value_aware_lapper::ValueAwareLapper,
};

/// An interval cache implementation using ValueAwareLapper.
///
/// Unlike LapperCache which uses a HashMap to separate intervals by value,
/// ValueAwareLapperCache uses a single ValueAwareLapper instance that handles value-aware
/// merging internally through sorting.
pub struct ValueAwareLapperCache<V>
where
    V: Clone + Eq + Ord + std::hash::Hash + Send + Sync,
{
    /// The ValueAwareLapper instance containing all intervals
    value_lapper: ValueAwareLapper<u64, V>,

    /// Time resolution for bucketing timestamps
    /// Duration::from_nanos(1) = nanosecond resolution (no bucketing)
    resolution: Duration,
}

impl<V> ValueAwareLapperCache<V>
where
    V: Clone + Eq + Ord + std::hash::Hash + Send + Sync,
{
    /// Bucket a timestamp according to the specified resolution.
    ///
    /// For nanosecond resolution (Duration::from_nanos(1) or less), returns the timestamp unchanged.
    /// Otherwise, buckets the timestamp to the nearest multiple of the resolution.
    #[inline]
    fn bucket_timestamp(ts: Timestamp, resolution: Duration) -> Timestamp {
        let resolution_ns = resolution.as_nanos() as u64;
        if resolution_ns <= 1 {
            ts
        } else {
            (ts / resolution_ns) * resolution_ns
        }
    }

    /// Build intervals from sorted timestamp-value pairs with optional bucketing.
    ///
    /// Consecutive timestamps with the same value are merged into continuous intervals.
    /// If resolution is provided, timestamps are bucketed before building intervals.
    fn build_intervals(
        points: Vec<(Timestamp, V)>,
        resolution: Duration,
    ) -> Result<Vec<Interval<u64, V>>, CacheBuildError> {
        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(intervals);
        }

        // Build intervals by merging consecutive identical values
        // Track open intervals for each value to handle overlapping
        let mut open_intervals: std::collections::HashMap<V, (u64, u64)> =
            std::collections::HashMap::new();

        for (t, v) in points {
            // Bucket the timestamp according to the resolution
            let bucketed_t = Self::bucket_timestamp(t, resolution);

            let next_end = bucketed_t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(bucketed_t))?;

            match open_intervals.get_mut(&v) {
                Some((_, end)) if *end == bucketed_t => {
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

                    *start = bucketed_t;
                    *end = next_end;
                }
                None => {
                    // New value - start tracking it
                    open_intervals.insert(v.clone(), (bucketed_t, next_end));
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

    /// Create a cache from sorted data with a specific time resolution.
    ///
    /// Timestamps will be bucketed according to the resolution before building intervals.
    ///
    /// # Arguments
    /// * `sorted_data` - Pre-sorted data wrapped in `SortedData` type
    /// * `resolution` - Time bucket size (e.g., `Duration::from_secs(5)` for 5-second buckets)
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    /// use tag_values_cache::{ValueAwareLapperCache, SortedData};
    ///
    /// let data = vec![(0, "A"), (1, "A"), (5, "B")];
    /// let cache = ValueAwareLapperCache::from_sorted_with_resolution(
    ///     SortedData::from_unsorted(data),
    ///     Duration::from_secs(5),
    /// ).unwrap();
    /// ```
    pub fn from_sorted_with_resolution(
        sorted_data: SortedData<V>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();
        let intervals = Self::build_intervals(points, resolution)?;

        let mut value_lapper = ValueAwareLapper::new(intervals);
        value_lapper.merge_with_values();

        Ok(Self {
            value_lapper,
            resolution,
        })
    }
}

impl<V> IntervalCache<V> for ValueAwareLapperCache<V>
where
    V: Clone + Eq + Ord + std::hash::Hash + Send + Sync,
{
    fn from_sorted(sorted_data: SortedData<V>) -> Result<Self, CacheBuildError> {
        // Default to nanosecond resolution for backward compatibility
        Self::from_sorted_with_resolution(sorted_data, Duration::from_nanos(1))
    }

    fn query_point(&self, t: Timestamp) -> HashSet<&V> {
        // Bucket the query timestamp to match the cache resolution
        let bucketed_t = Self::bucket_timestamp(t, self.resolution);
        let start = bucketed_t;
        let stop = bucketed_t + 1;

        self.value_lapper
            .find(start, stop)
            .map(|interval| &interval.val)
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> HashSet<&V> {
        // Bucket the query range to match the cache resolution
        let bucketed_start = Self::bucket_timestamp(range.start, self.resolution);
        // For the end, we need to ensure we don't miss data by rounding down
        // So we bucket it and then add the resolution to cover the full bucket
        let bucketed_end = Self::bucket_timestamp(range.end, self.resolution);

        // If the end was bucketed down, we need to extend it to cover that bucket
        let query_end = if bucketed_end < range.end {
            let resolution_ns = self.resolution.as_nanos() as u64;
            if resolution_ns > 1 {
                bucketed_end + resolution_ns
            } else {
                bucketed_end
            }
        } else {
            bucketed_end
        };

        self.value_lapper
            .find(bucketed_start, query_end)
            .map(|interval| &interval.val)
            .collect()
    }

    fn append_sorted(&mut self, sorted_data: SortedData<V>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points using the cache's resolution
        let new_intervals = Self::build_intervals(sorted_data.into_inner(), self.resolution)?;

        // Collect all existing intervals
        let mut all_intervals: Vec<_> = self.value_lapper.iter().cloned().collect();
        all_intervals.extend(new_intervals);

        // Rebuild with all intervals and merge
        self.value_lapper = ValueAwareLapper::new(all_intervals);
        self.value_lapper.merge_with_values();

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of all intervals in the ValueAwareLapper
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
    fn test_value_aware_lapper_cache_basic() {
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (2, "A".to_string()),
            (5, "B".to_string()),
            (6, "B".to_string()),
        ];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        // Check merged intervals
        assert_eq!(cache.query_point(0), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(1), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(2), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(3), HashSet::<&String>::new());
        assert_eq!(cache.query_point(5), HashSet::from([&"B".to_string()]));
    }

    #[test]
    fn test_value_aware_lapper_cache_overlapping() {
        let data = vec![
            (0, "X".to_string()),
            (1, "X".to_string()),
            (1, "Y".to_string()),
            (2, "Y".to_string()),
        ];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.contains(&&"X".to_string()));
        assert!(values_at_1.contains(&&"Y".to_string()));
    }

    #[test]
    fn test_value_aware_lapper_cache_range_query() {
        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (10, "B".to_string()),
            (11, "B".to_string()),
            (20, "C".to_string()),
        ];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        let range_values = cache.query_range(0..15);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&&"A".to_string()));
        assert!(range_values.contains(&&"B".to_string()));
    }

    #[test]
    fn test_value_aware_lapper_cache_append() {
        let initial_data = vec![(0, "A".to_string()), (1, "A".to_string())];

        let mut cache = ValueAwareLapperCache::new(initial_data).unwrap();

        let append_data = vec![(5, "B".to_string()), (6, "B".to_string())];

        cache.append_batch(append_data).unwrap();

        assert_eq!(cache.query_point(0), HashSet::from([&"A".to_string()]));
        assert_eq!(cache.query_point(5), HashSet::from([&"B".to_string()]));
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

        let cache = ValueAwareLapperCache::new(data).unwrap();

        // Both values should be present at timestamp 0 and 1
        let values_at_0 = cache.query_point(0);
        assert_eq!(values_at_0.len(), 2);
        assert!(values_at_0.contains(&&"A".to_string()));
        assert!(values_at_0.contains(&&"B".to_string()));

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.contains(&&"A".to_string()));
        assert!(values_at_1.contains(&&"B".to_string()));

        // Should have 2 intervals total (one for A, one for B)
        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_5_seconds() {
        use std::time::Duration;

        // Timestamps at nanosecond resolution within a 5-second window
        let data = vec![
            (0, "A".to_string()),             // Bucket 0
            (1_000_000_000, "A".to_string()), // Bucket 0 (1 second)
            (4_999_999_999, "A".to_string()), // Bucket 0 (just under 5 seconds)
            (5_000_000_000, "B".to_string()), // Bucket 5000000000 (exactly 5 seconds)
            (9_999_999_999, "B".to_string()), // Bucket 5000000000 (just under 10 seconds)
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // All timestamps in [0, 5) seconds should be bucketed to 0
        assert_eq!(cache.query_point(0), HashSet::from([&"A".to_string()]));
        assert_eq!(
            cache.query_point(1_000_000_000),
            HashSet::from([&"A".to_string()])
        );
        assert_eq!(
            cache.query_point(4_999_999_999),
            HashSet::from([&"A".to_string()])
        );

        // Timestamps in [5, 10) seconds should be bucketed to 5000000000
        assert_eq!(
            cache.query_point(5_000_000_000),
            HashSet::from([&"B".to_string()])
        );
        assert_eq!(
            cache.query_point(9_999_999_999),
            HashSet::from([&"B".to_string()])
        );

        // Should have 2 intervals (one per bucket)
        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_1_minute() {
        use std::time::Duration;

        let data = vec![
            (0, "A".to_string()),              // Minute 0
            (30_000_000_000, "A".to_string()), // Minute 0 (30 seconds)
            (59_999_999_999, "A".to_string()), // Minute 0 (just under 1 minute)
            (60_000_000_000, "B".to_string()), // Minute 1 (exactly 1 minute)
            (90_000_000_000, "B".to_string()), // Minute 1 (1.5 minutes)
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(60),
        )
        .unwrap();

        // All timestamps in minute 0 should map to the same bucket
        assert_eq!(cache.query_point(0), HashSet::from([&"A".to_string()]));
        assert_eq!(
            cache.query_point(30_000_000_000),
            HashSet::from([&"A".to_string()])
        );

        // Timestamps in minute 1 should map to a different bucket
        assert_eq!(
            cache.query_point(60_000_000_000),
            HashSet::from([&"B".to_string()])
        );

        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_merging() {
        use std::time::Duration;

        // With 5-second resolution, these should all merge into one interval
        let data = vec![
            (100, "X".to_string()),           // Bucket 0
            (1_000_000_000, "X".to_string()), // Bucket 0
            (2_500_000_000, "X".to_string()), // Bucket 0
            (4_000_000_000, "X".to_string()), // Bucket 0
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // Should have only 1 interval since all timestamps bucket to 0
        assert_eq!(cache.interval_count(), 1);

        // All queries within the 5-second bucket should return "X"
        assert_eq!(cache.query_point(0), HashSet::from([&"X".to_string()]));
        assert_eq!(
            cache.query_point(4_999_999_999),
            HashSet::from([&"X".to_string()])
        );
    }

    #[test]
    fn test_resolution_range_query() {
        use std::time::Duration;

        let data = vec![
            (0, "A".to_string()),              // Bucket 0
            (5_000_000_000, "B".to_string()),  // Bucket 5000000000
            (10_000_000_000, "C".to_string()), // Bucket 10000000000
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // Range query from 0 to 7.5 seconds should return A and B
        let range_values = cache.query_range(0..7_500_000_000);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&&"A".to_string()));
        assert!(range_values.contains(&&"B".to_string()));

        // Range query from 5 to 15 seconds should return B and C
        let range_values = cache.query_range(5_000_000_000..15_000_000_000);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&&"B".to_string()));
        assert!(range_values.contains(&&"C".to_string()));
    }

    #[test]
    fn test_nanosecond_resolution_backward_compat() {
        use std::time::Duration;

        let data = vec![
            (0, "A".to_string()),
            (1, "A".to_string()),
            (2, "A".to_string()),
        ];

        // Explicitly using nanosecond resolution should work the same as default
        let cache_explicit = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data.clone()),
            Duration::from_nanos(1),
        )
        .unwrap();

        let cache_default = ValueAwareLapperCache::new(data).unwrap();

        assert_eq!(
            cache_explicit.interval_count(),
            cache_default.interval_count()
        );
        assert_eq!(cache_explicit.query_point(0), cache_default.query_point(0));
        assert_eq!(cache_explicit.query_point(1), cache_default.query_point(1));
    }
}
