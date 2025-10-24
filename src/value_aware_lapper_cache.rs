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
    CacheBuildError, IntervalCache, SortedData, TagSet, Timestamp,
    value_aware_lapper::ValueAwareLapper,
};

/// An interval cache implementation using ValueAwareLapper for value-aware merging.
///
/// This cache stores TagSet values, which represent sets of (tag_name, tag_value) pairs.
/// Values are stored directly in the intervals without any Arc wrapping or deduplication.
pub struct ValueAwareLapperCache {
    /// The ValueAwareLapper instance containing all intervals
    value_lapper: ValueAwareLapper<u64, TagSet>,

    /// Time resolution for bucketing timestamps
    /// Duration::from_nanos(1) = nanosecond resolution (no bucketing)
    resolution: Duration,
}

impl ValueAwareLapperCache {
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
        points: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
    ) -> Result<Vec<Interval<u64, TagSet>>, CacheBuildError> {
        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(intervals);
        }

        // Build intervals by merging consecutive identical values
        // Track open intervals for each value to handle overlapping
        let mut open_intervals: std::collections::HashMap<TagSet, (u64, u64)> =
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
    pub fn from_sorted_with_resolution(
        sorted_data: SortedData<TagSet>,
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

impl IntervalCache<TagSet> for ValueAwareLapperCache {
    fn from_sorted(sorted_data: SortedData<TagSet>) -> Result<Self, CacheBuildError> {
        // Default to nanosecond resolution for backward compatibility
        Self::from_sorted_with_resolution(sorted_data, Duration::from_nanos(1))
    }

    fn query_point(&self, t: Timestamp) -> HashSet<&TagSet> {
        // Bucket the query timestamp to match the cache resolution
        let bucketed_t = Self::bucket_timestamp(t, self.resolution);
        let start = bucketed_t;
        let stop = bucketed_t + 1;

        // Find matching intervals and get value references
        self.value_lapper
            .find(start, stop)
            .map(|interval| &interval.val)
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> HashSet<&TagSet> {
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

    fn append_sorted(&mut self, sorted_data: SortedData<TagSet>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points using the cache's resolution
        let new_intervals = Self::build_intervals(
            sorted_data.into_inner(),
            self.resolution,
        )?;

        // Collect all existing intervals
        let mut all_intervals: Vec<_> = self.value_lapper.iter().cloned().collect();
        all_intervals.extend(new_intervals);

        // Rebuild with all intervals and merge
        self.value_lapper = ValueAwareLapper::new(all_intervals);
        self.value_lapper.merge_with_values();

        Ok(())
    }

    fn size_bytes(&self) -> usize {
        // Size of the struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of all intervals in the ValueAwareLapper
        size += self.value_lapper.len() * std::mem::size_of::<Interval<u64, TagSet>>();

        // Note: This doesn't account for heap allocations within TagSet
        // For accurate heap size, we'd need to calculate the size of each tag name-value pair

        size
    }

    fn interval_count(&self) -> usize {
        self.value_lapper.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_value_aware_lapper_cache_basic() {
        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        let data = vec![
            (0, tag_set_a.clone()),
            (1, tag_set_a.clone()),
            (2, tag_set_a.clone()),
            (5, tag_set_b.clone()),
            (6, tag_set_b.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        // Check merged intervals
        assert_eq!(cache.query_point(0), HashSet::from([&tag_set_a]));
        assert_eq!(cache.query_point(1), HashSet::from([&tag_set_a]));
        assert_eq!(cache.query_point(2), HashSet::from([&tag_set_a]));
        assert_eq!(cache.query_point(3), HashSet::<&TagSet>::new());
        assert_eq!(cache.query_point(5), HashSet::from([&tag_set_b]));
    }

    #[test]
    fn test_value_aware_lapper_cache_overlapping() {
        let mut tag_set_x = BTreeSet::new();
        tag_set_x.insert(("tag1".to_string(), "X".to_string()));

        let mut tag_set_y = BTreeSet::new();
        tag_set_y.insert(("tag1".to_string(), "Y".to_string()));

        let data = vec![
            (0, tag_set_x.clone()),
            (1, tag_set_x.clone()),
            (1, tag_set_y.clone()),
            (2, tag_set_y.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.contains(&tag_set_x));
        assert!(values_at_1.contains(&tag_set_y));
    }

    #[test]
    fn test_value_aware_lapper_cache_range_query() {
        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        let mut tag_set_c = BTreeSet::new();
        tag_set_c.insert(("tag1".to_string(), "C".to_string()));

        let data = vec![
            (0, tag_set_a.clone()),
            (1, tag_set_a.clone()),
            (10, tag_set_b.clone()),
            (11, tag_set_b.clone()),
            (20, tag_set_c.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        let range_values = cache.query_range(0..15);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&tag_set_a));
        assert!(range_values.contains(&tag_set_b));
    }

    #[test]
    fn test_value_aware_lapper_cache_append() {
        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        let initial_data = vec![(0, tag_set_a.clone()), (1, tag_set_a.clone())];
        let sorted_initial = SortedData::from_sorted(initial_data);
        let mut cache = ValueAwareLapperCache::from_sorted(sorted_initial).unwrap();

        let append_data = vec![(5, tag_set_b.clone()), (6, tag_set_b.clone())];
        let sorted_append = SortedData::from_sorted(append_data);
        cache.append_sorted(sorted_append).unwrap();

        assert_eq!(cache.query_point(0), HashSet::from([&tag_set_a]));
        assert_eq!(cache.query_point(5), HashSet::from([&tag_set_b]));
    }

    #[test]
    fn test_value_aware_merging() {
        // Test that intervals with same boundaries but different values don't merge
        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        let data = vec![
            (0, tag_set_a.clone()),
            (0, tag_set_b.clone()),
            (1, tag_set_a.clone()),
            (1, tag_set_b.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        // Both values should be present at timestamp 0 and 1
        let values_at_0 = cache.query_point(0);
        assert_eq!(values_at_0.len(), 2);
        assert!(values_at_0.contains(&tag_set_a));
        assert!(values_at_0.contains(&tag_set_b));

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.contains(&tag_set_a));
        assert!(values_at_1.contains(&tag_set_b));

        // Should have 2 intervals total (one for A, one for B)
        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_5_seconds() {
        use std::time::Duration;

        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        // Timestamps at nanosecond resolution within a 5-second window
        let data = vec![
            (0, tag_set_a.clone()),             // Bucket 0
            (1_000_000_000, tag_set_a.clone()), // Bucket 0 (1 second)
            (4_999_999_999, tag_set_a.clone()), // Bucket 0 (just under 5 seconds)
            (5_000_000_000, tag_set_b.clone()), // Bucket 5000000000 (exactly 5 seconds)
            (9_999_999_999, tag_set_b.clone()), // Bucket 5000000000 (just under 10 seconds)
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // All timestamps in [0, 5) seconds should be bucketed to 0
        assert_eq!(cache.query_point(0), HashSet::from([&tag_set_a]));
        assert_eq!(
            cache.query_point(1_000_000_000),
            HashSet::from([&tag_set_a])
        );
        assert_eq!(
            cache.query_point(4_999_999_999),
            HashSet::from([&tag_set_a])
        );

        // Timestamps in [5, 10) seconds should be bucketed to 5000000000
        assert_eq!(
            cache.query_point(5_000_000_000),
            HashSet::from([&tag_set_b])
        );
        assert_eq!(
            cache.query_point(9_999_999_999),
            HashSet::from([&tag_set_b])
        );

        // Should have 2 intervals (one per bucket)
        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_1_minute() {
        use std::time::Duration;

        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        let data = vec![
            (0, tag_set_a.clone()),              // Minute 0
            (30_000_000_000, tag_set_a.clone()), // Minute 0 (30 seconds)
            (59_999_999_999, tag_set_a.clone()), // Minute 0 (just under 1 minute)
            (60_000_000_000, tag_set_b.clone()), // Minute 1 (exactly 1 minute)
            (90_000_000_000, tag_set_b.clone()), // Minute 1 (1.5 minutes)
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(60),
        )
        .unwrap();

        // All timestamps in minute 0 should map to the same bucket
        assert_eq!(cache.query_point(0), HashSet::from([&tag_set_a]));
        assert_eq!(
            cache.query_point(30_000_000_000),
            HashSet::from([&tag_set_a])
        );

        // Timestamps in minute 1 should map to a different bucket
        assert_eq!(
            cache.query_point(60_000_000_000),
            HashSet::from([&tag_set_b])
        );

        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_merging() {
        use std::time::Duration;

        let mut tag_set_x = BTreeSet::new();
        tag_set_x.insert(("tag1".to_string(), "X".to_string()));

        // With 5-second resolution, these should all merge into one interval
        let data = vec![
            (100, tag_set_x.clone()),           // Bucket 0
            (1_000_000_000, tag_set_x.clone()), // Bucket 0
            (2_500_000_000, tag_set_x.clone()), // Bucket 0
            (4_000_000_000, tag_set_x.clone()), // Bucket 0
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // Should have only 1 interval since all timestamps bucket to 0
        assert_eq!(cache.interval_count(), 1);

        // All queries within the 5-second bucket should return "X"
        assert_eq!(cache.query_point(0), HashSet::from([&tag_set_x]));
        assert_eq!(
            cache.query_point(4_999_999_999),
            HashSet::from([&tag_set_x])
        );
    }

    #[test]
    fn test_resolution_range_query() {
        use std::time::Duration;

        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let mut tag_set_b = BTreeSet::new();
        tag_set_b.insert(("tag1".to_string(), "B".to_string()));

        let mut tag_set_c = BTreeSet::new();
        tag_set_c.insert(("tag1".to_string(), "C".to_string()));

        let data = vec![
            (0, tag_set_a.clone()),              // Bucket 0
            (5_000_000_000, tag_set_b.clone()),  // Bucket 5000000000
            (10_000_000_000, tag_set_c.clone()), // Bucket 10000000000
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // Range query from 0 to 7.5 seconds should return A and B
        let range_values = cache.query_range(0..7_500_000_000);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&tag_set_a));
        assert!(range_values.contains(&tag_set_b));

        // Range query from 5 to 15 seconds should return B and C
        let range_values = cache.query_range(5_000_000_000..15_000_000_000);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&tag_set_b));
        assert!(range_values.contains(&tag_set_c));
    }

    #[test]
    fn test_nanosecond_resolution_backward_compat() {
        use std::time::Duration;

        let mut tag_set_a = BTreeSet::new();
        tag_set_a.insert(("tag1".to_string(), "A".to_string()));

        let data = vec![
            (0, tag_set_a.clone()),
            (1, tag_set_a.clone()),
            (2, tag_set_a.clone()),
        ];

        // Explicitly using nanosecond resolution should work the same as default
        let cache_explicit = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data.clone()),
            Duration::from_nanos(1),
        )
        .unwrap();

        let sorted_data = SortedData::from_sorted(data);
        let cache_default = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        assert_eq!(
            cache_explicit.interval_count(),
            cache_default.interval_count()
        );
        assert_eq!(cache_explicit.query_point(0), cache_default.query_point(0));
        assert_eq!(cache_explicit.query_point(1), cache_default.query_point(1));
    }
}
