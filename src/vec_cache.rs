//! A vector-based implementation using parallel vectors with binary search optimization.
//!
//! This implementation stores intervals as three parallel vectors:
//! - starts: interval start times (sorted)
//! - ends: interval end times
//! - values: the values for each interval
//!
//! Query operations use binary search on the sorted starts array to reduce
//! the number of intervals that need to be checked.

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Range;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// A cache implementation using three parallel vectors with binary search.
///
/// This implementation uses binary search on the sorted start times to
/// efficiently narrow down the set of intervals to check during queries.
///
/// Performance characteristics:
/// - Point queries: O(log n + k) where k is the number of matching intervals
/// - Range queries: O(n) where n is the number of intervals that start before range.end
///   (Limited by the need to check intervals that might start before the range but extend into it)
pub struct VecCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Start times of intervals (sorted)
    starts: Vec<u64>,
    /// End times of intervals (exclusive)
    ends: Vec<u64>,
    /// Values corresponding to each interval
    values: Vec<V>,
}

impl<V> VecCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Merge adjacent or overlapping intervals with the same value.
    ///
    /// Takes a list of intervals and merges any that are adjacent (touching) or overlapping
    /// and have the same value. Also removes any duplicate intervals.
    fn merge_intervals(
        mut intervals: Vec<(std::ops::Range<u64>, V)>,
    ) -> Vec<(std::ops::Range<u64>, V)> {
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
}

impl<V> IntervalCache<V> for VecCache<V>
where
    V: Clone + Eq + Hash,
    for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut values = Vec::new();

        if points.is_empty() {
            return Ok(Self {
                starts,
                ends,
                values,
            });
        }

        // Build intervals by merging consecutive identical values
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
                starts.push(current_start);
                ends.push(current_end);
                values.push(current_value);

                current_start = t;
                current_end = next_end;
                current_value = v;
            }
        }

        // Don't forget the last interval
        starts.push(current_start);
        ends.push(current_end);
        values.push(current_value);

        // Merge any intervals that touch or overlap with same value
        let intervals: Vec<(Range<u64>, V)> = starts
            .into_iter()
            .zip(ends)
            .zip(values)
            .map(|((start, end), value)| (start..end, value))
            .collect();

        let merged = Self::merge_intervals(intervals);

        // Unpack back into separate vectors
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut values = Vec::new();
        for (range, value) in merged {
            starts.push(range.start);
            ends.push(range.end);
            values.push(value);
        }

        Ok(Self {
            starts,
            ends,
            values,
        })
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let mut results = Vec::new();

        if self.starts.is_empty() {
            return results;
        }

        // Binary search to find the first interval whose start > t
        let first_after = self.starts.partition_point(|&start| start <= t);

        // Check all intervals that could contain t
        // Start from the beginning since we need to check all intervals
        // whose start <= t and end > t
        // But we can stop at first_after since those intervals start after t
        for i in 0..first_after {
            if self.ends[i] > t {
                let tag_vec: Vec<(&str, &str)> = self.values[i].into_iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                results.push(tag_vec);
            }
        }

        results
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        if self.starts.is_empty() {
            return results;
        }

        // With only starts sorted, we can't efficiently use binary search for range queries
        // because we need to check intervals that might start before the range but extend into it.
        // The ends array is not sorted, so we can't binary search on it.

        // Best we can do: use binary search to find where to stop, then linear scan.
        // This at least avoids checking intervals that start after the range.

        // Find the first interval whose start >= range.end (these can't overlap)
        let first_after = self.starts.partition_point(|&start| start < range.end);

        // Linear scan from the beginning up to first_after
        // This is actually optimal given our data structure constraints
        for i in 0..first_after {
            if self.ends[i] > range.start {
                // Deduplicate based on the value
                if seen.insert(&self.values[i]) {
                    let tag_vec: Vec<(&str, &str)> = self.values[i].into_iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    results.push(tag_vec);
                }
            }
        }

        results
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        // Build intervals from sorted points
        let new_cache = Self::from_sorted(sorted_data)?;

        // If existing cache is empty, just replace with new
        if self.starts.is_empty() {
            self.starts = new_cache.starts;
            self.ends = new_cache.ends;
            self.values = new_cache.values;
            return Ok(());
        }

        // If new cache is empty, nothing to do
        if new_cache.starts.is_empty() {
            return Ok(());
        }

        // Collect all intervals as Range objects for merging
        let mut all_intervals: Vec<(std::ops::Range<u64>, V)> = Vec::new();

        // Collect existing intervals
        for i in 0..self.starts.len() {
            all_intervals.push((self.starts[i]..self.ends[i], self.values[i].clone()));
        }

        // Add new intervals
        for i in 0..new_cache.starts.len() {
            all_intervals.push((
                new_cache.starts[i]..new_cache.ends[i],
                new_cache.values[i].clone(),
            ));
        }

        // Merge intervals using the same logic as other implementations
        let merged = Self::merge_intervals(all_intervals);

        // Rebuild the vectors from merged intervals
        self.starts.clear();
        self.ends.clear();
        self.values.clear();

        for (range, value) in merged {
            self.starts.push(range.start);
            self.ends.push(range.end);
            self.values.push(value);
        }

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the VecCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of the three vectors
        // Each Vec has capacity * element_size bytes allocated

        // starts vector (Vec<u64>)
        size += self.starts.capacity() * std::mem::size_of::<u64>();

        // ends vector (Vec<u64>)
        size += self.ends.capacity() * std::mem::size_of::<u64>();

        // values vector
        // For the values, we need to account for both the vector's allocation
        // and any heap memory the values themselves might use
        size += self.values.capacity() * std::mem::size_of::<V>();

        // Add heap size for values if they contain heap-allocated data
        // For types like String or Vec, this would include their heap allocations
        for value in &self.values {
            size += value.heap_size();
        }

        size
    }

    fn interval_count(&self) -> usize {
        self.starts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TagSet;

    fn make_tagset(pairs: &[(&str, &str)]) -> TagSet {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_vec_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (4, tag_b.clone()),
        ];

        let cache = VecCache::new(data).unwrap();

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
    fn test_vec_cache_empty() {
        let cache: VecCache<TagSet> = VecCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(0..100).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_vec_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (3, tag_a.clone()),
        ];

        let cache = VecCache::new(data).unwrap();

        // Should have merged into 1 interval: [1,4)
        assert_eq!(cache.interval_count(), 1);
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert_eq!(cache.query_point(4).len(), 0);
    }
}

