//! `IntervalCache` implementation using ValueAwareLapper for value-aware merging.
//!
//! This implementation provides the same interface as LapperCache but uses
//! ValueAwareLapper internally, which only merges intervals when both boundaries
//! AND values match.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexSet;
use rust_lapper::Interval;

use crate::{
    CacheBuildError, HeapSize, IntervalCache, RecordBatchRow, SortedData, Timestamp,
    value_aware_lapper::ValueAwareLapper,
};

/// An interval cache implementation using ValueAwareLapper with dictionary encoding.
///
/// This cache uses Arc for RecordBatchRow deduplication internally, ensuring each unique
/// RecordBatchRow is only stored once in memory. Uses IndexSet to maintain insertion order
/// and provide O(1) lookups without redundant storage.
pub struct ValueAwareLapperCache {
    /// The ValueAwareLapper instance containing all intervals with interned RecordBatchRows
    value_lapper: ValueAwareLapper<u64, Arc<RecordBatchRow>>,

    /// Deduplicated RecordBatchRows with stable indexing via insertion order
    unique_rows: IndexSet<Arc<RecordBatchRow>>,

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

    /// Intern a RecordBatchRow, ensuring only one copy exists in memory.
    ///
    /// Returns the Arc from the IndexSet, either existing or newly inserted.
    fn intern_row(
        unique_rows: &mut IndexSet<Arc<RecordBatchRow>>,
        row: RecordBatchRow,
    ) -> Arc<RecordBatchRow> {
        let arc_row = Arc::new(row);

        // Check if it already exists and return the existing Arc
        if let Some(existing) = unique_rows.get(&arc_row) {
            existing.clone()
        } else {
            // Insert and return the new Arc
            unique_rows.insert(arc_row.clone());
            arc_row
        }
    }

    /// Build intervals from sorted timestamp-value pairs with optional bucketing.
    ///
    /// Consecutive timestamps with the same value are merged into continuous intervals.
    /// If resolution is provided, timestamps are bucketed before building intervals.
    fn build_intervals(
        points: Vec<(Timestamp, RecordBatchRow)>,
        resolution: Duration,
        unique_rows: &mut IndexSet<Arc<RecordBatchRow>>,
    ) -> Result<Vec<Interval<u64, Arc<RecordBatchRow>>>, CacheBuildError> {
        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(intervals);
        }

        // Build intervals by merging consecutive identical values
        // Track open intervals for each value to handle overlapping
        let mut open_intervals: std::collections::HashMap<Arc<RecordBatchRow>, (u64, u64)> =
            std::collections::HashMap::new();

        for (t, v) in points {
            // Bucket the timestamp according to the resolution
            let bucketed_t = Self::bucket_timestamp(t, resolution);

            let next_end = bucketed_t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(bucketed_t))?;

            // Intern the row to ensure deduplication
            let interned = Self::intern_row(unique_rows, v);

            match open_intervals.get_mut(&interned) {
                Some((_, end)) if *end == bucketed_t => {
                    // Extend existing interval
                    *end = next_end;
                }
                Some((start, end)) => {
                    // Gap detected - save old interval and start new one
                    intervals.push(Interval {
                        start: *start,
                        stop: *end,
                        val: interned.clone(),
                    });

                    *start = bucketed_t;
                    *end = next_end;
                }
                None => {
                    // New value - start tracking it
                    open_intervals.insert(interned.clone(), (bucketed_t, next_end));
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
        sorted_data: SortedData<RecordBatchRow>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        let mut unique_rows = IndexSet::new();
        let points = sorted_data.into_inner();
        let intervals = Self::build_intervals(points, resolution, &mut unique_rows)?;

        let mut value_lapper = ValueAwareLapper::new(intervals);
        value_lapper.merge_with_values();

        Ok(Self {
            value_lapper,
            unique_rows,
            resolution,
        })
    }
}

impl IntervalCache<RecordBatchRow> for ValueAwareLapperCache {
    fn from_sorted(sorted_data: SortedData<RecordBatchRow>) -> Result<Self, CacheBuildError> {
        // Default to nanosecond resolution for backward compatibility
        Self::from_sorted_with_resolution(sorted_data, Duration::from_nanos(1))
    }

    fn query_point(&self, t: Timestamp) -> HashSet<&RecordBatchRow> {
        // Bucket the query timestamp to match the cache resolution
        let bucketed_t = Self::bucket_timestamp(t, self.resolution);
        let start = bucketed_t;
        let stop = bucketed_t + 1;

        // Find matching intervals and get RecordBatchRow references from IndexSet
        self.value_lapper
            .find(start, stop)
            .filter_map(|interval| {
                // IndexSet allows us to get a reference to the Arc's inner value
                self.unique_rows.get(&interval.val).map(|arc| arc.as_ref())
            })
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> HashSet<&RecordBatchRow> {
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
            .filter_map(|interval| {
                self.unique_rows.get(&interval.val).map(|arc| arc.as_ref())
            })
            .collect()
    }

    fn append_sorted(&mut self, sorted_data: SortedData<RecordBatchRow>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points using the cache's resolution
        let new_intervals = Self::build_intervals(
            sorted_data.into_inner(),
            self.resolution,
            &mut self.unique_rows,
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
        size += self.value_lapper.len() * std::mem::size_of::<Interval<u64, Arc<RecordBatchRow>>>();

        // Add heap size for unique rows in IndexSet
        // IndexSet stores both keys and an index map internally
        size += self.unique_rows.capacity() * std::mem::size_of::<Arc<RecordBatchRow>>();
        for arc_row in &self.unique_rows {
            size += arc_row.heap_size();
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
    use std::collections::BTreeMap;

    #[test]
    fn test_value_aware_lapper_cache_basic() {
        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        let data = vec![
            (0, row_a.clone()),
            (1, row_a.clone()),
            (2, row_a.clone()),
            (5, row_b.clone()),
            (6, row_b.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        // Check merged intervals
        assert_eq!(cache.query_point(0), HashSet::from([&row_a]));
        assert_eq!(cache.query_point(1), HashSet::from([&row_a]));
        assert_eq!(cache.query_point(2), HashSet::from([&row_a]));
        assert_eq!(cache.query_point(3), HashSet::<&RecordBatchRow>::new());
        assert_eq!(cache.query_point(5), HashSet::from([&row_b]));
    }

    #[test]
    fn test_value_aware_lapper_cache_overlapping() {
        let mut values_x = BTreeMap::new();
        values_x.insert("tag1".to_string(), "X".to_string());
        let row_x = RecordBatchRow { values: values_x };

        let mut values_y = BTreeMap::new();
        values_y.insert("tag1".to_string(), "Y".to_string());
        let row_y = RecordBatchRow { values: values_y };

        let data = vec![
            (0, row_x.clone()),
            (1, row_x.clone()),
            (1, row_y.clone()),
            (2, row_y.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.contains(&row_x));
        assert!(values_at_1.contains(&row_y));
    }

    #[test]
    fn test_value_aware_lapper_cache_range_query() {
        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        let mut values_c = BTreeMap::new();
        values_c.insert("tag1".to_string(), "C".to_string());
        let row_c = RecordBatchRow { values: values_c };

        let data = vec![
            (0, row_a.clone()),
            (1, row_a.clone()),
            (10, row_b.clone()),
            (11, row_b.clone()),
            (20, row_c.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        let range_values = cache.query_range(0..15);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&row_a));
        assert!(range_values.contains(&row_b));
    }

    #[test]
    fn test_value_aware_lapper_cache_append() {
        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        let initial_data = vec![(0, row_a.clone()), (1, row_a.clone())];
        let sorted_initial = SortedData::from_sorted(initial_data);
        let mut cache = ValueAwareLapperCache::from_sorted(sorted_initial).unwrap();

        let append_data = vec![(5, row_b.clone()), (6, row_b.clone())];
        let sorted_append = SortedData::from_sorted(append_data);
        cache.append_sorted(sorted_append).unwrap();

        assert_eq!(cache.query_point(0), HashSet::from([&row_a]));
        assert_eq!(cache.query_point(5), HashSet::from([&row_b]));
    }

    #[test]
    fn test_value_aware_merging() {
        // Test that intervals with same boundaries but different values don't merge
        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        let data = vec![
            (0, row_a.clone()),
            (0, row_b.clone()),
            (1, row_a.clone()),
            (1, row_b.clone()),
        ];

        let sorted_data = SortedData::from_sorted(data);
        let cache = ValueAwareLapperCache::from_sorted(sorted_data).unwrap();

        // Both values should be present at timestamp 0 and 1
        let values_at_0 = cache.query_point(0);
        assert_eq!(values_at_0.len(), 2);
        assert!(values_at_0.contains(&row_a));
        assert!(values_at_0.contains(&row_b));

        let values_at_1 = cache.query_point(1);
        assert_eq!(values_at_1.len(), 2);
        assert!(values_at_1.contains(&row_a));
        assert!(values_at_1.contains(&row_b));

        // Should have 2 intervals total (one for A, one for B)
        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_5_seconds() {
        use std::time::Duration;

        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        // Timestamps at nanosecond resolution within a 5-second window
        let data = vec![
            (0, row_a.clone()),             // Bucket 0
            (1_000_000_000, row_a.clone()), // Bucket 0 (1 second)
            (4_999_999_999, row_a.clone()), // Bucket 0 (just under 5 seconds)
            (5_000_000_000, row_b.clone()), // Bucket 5000000000 (exactly 5 seconds)
            (9_999_999_999, row_b.clone()), // Bucket 5000000000 (just under 10 seconds)
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // All timestamps in [0, 5) seconds should be bucketed to 0
        assert_eq!(cache.query_point(0), HashSet::from([&row_a]));
        assert_eq!(
            cache.query_point(1_000_000_000),
            HashSet::from([&row_a])
        );
        assert_eq!(
            cache.query_point(4_999_999_999),
            HashSet::from([&row_a])
        );

        // Timestamps in [5, 10) seconds should be bucketed to 5000000000
        assert_eq!(
            cache.query_point(5_000_000_000),
            HashSet::from([&row_b])
        );
        assert_eq!(
            cache.query_point(9_999_999_999),
            HashSet::from([&row_b])
        );

        // Should have 2 intervals (one per bucket)
        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_1_minute() {
        use std::time::Duration;

        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        let data = vec![
            (0, row_a.clone()),              // Minute 0
            (30_000_000_000, row_a.clone()), // Minute 0 (30 seconds)
            (59_999_999_999, row_a.clone()), // Minute 0 (just under 1 minute)
            (60_000_000_000, row_b.clone()), // Minute 1 (exactly 1 minute)
            (90_000_000_000, row_b.clone()), // Minute 1 (1.5 minutes)
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(60),
        )
        .unwrap();

        // All timestamps in minute 0 should map to the same bucket
        assert_eq!(cache.query_point(0), HashSet::from([&row_a]));
        assert_eq!(
            cache.query_point(30_000_000_000),
            HashSet::from([&row_a])
        );

        // Timestamps in minute 1 should map to a different bucket
        assert_eq!(
            cache.query_point(60_000_000_000),
            HashSet::from([&row_b])
        );

        assert_eq!(cache.interval_count(), 2);
    }

    #[test]
    fn test_resolution_merging() {
        use std::time::Duration;

        let mut values_x = BTreeMap::new();
        values_x.insert("tag1".to_string(), "X".to_string());
        let row_x = RecordBatchRow { values: values_x };

        // With 5-second resolution, these should all merge into one interval
        let data = vec![
            (100, row_x.clone()),           // Bucket 0
            (1_000_000_000, row_x.clone()), // Bucket 0
            (2_500_000_000, row_x.clone()), // Bucket 0
            (4_000_000_000, row_x.clone()), // Bucket 0
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // Should have only 1 interval since all timestamps bucket to 0
        assert_eq!(cache.interval_count(), 1);

        // All queries within the 5-second bucket should return "X"
        assert_eq!(cache.query_point(0), HashSet::from([&row_x]));
        assert_eq!(
            cache.query_point(4_999_999_999),
            HashSet::from([&row_x])
        );
    }

    #[test]
    fn test_resolution_range_query() {
        use std::time::Duration;

        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let mut values_b = BTreeMap::new();
        values_b.insert("tag1".to_string(), "B".to_string());
        let row_b = RecordBatchRow { values: values_b };

        let mut values_c = BTreeMap::new();
        values_c.insert("tag1".to_string(), "C".to_string());
        let row_c = RecordBatchRow { values: values_c };

        let data = vec![
            (0, row_a.clone()),              // Bucket 0
            (5_000_000_000, row_b.clone()),  // Bucket 5000000000
            (10_000_000_000, row_c.clone()), // Bucket 10000000000
        ];

        let cache = ValueAwareLapperCache::from_sorted_with_resolution(
            SortedData::from_unsorted(data),
            Duration::from_secs(5),
        )
        .unwrap();

        // Range query from 0 to 7.5 seconds should return A and B
        let range_values = cache.query_range(0..7_500_000_000);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&row_a));
        assert!(range_values.contains(&row_b));

        // Range query from 5 to 15 seconds should return B and C
        let range_values = cache.query_range(5_000_000_000..15_000_000_000);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.contains(&row_b));
        assert!(range_values.contains(&row_c));
    }

    #[test]
    fn test_nanosecond_resolution_backward_compat() {
        use std::time::Duration;

        let mut values_a = BTreeMap::new();
        values_a.insert("tag1".to_string(), "A".to_string());
        let row_a = RecordBatchRow { values: values_a };

        let data = vec![
            (0, row_a.clone()),
            (1, row_a.clone()),
            (2, row_a.clone()),
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
