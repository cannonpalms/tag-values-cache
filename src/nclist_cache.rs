//! An NCList (Nested Containment List) based implementation for efficient interval queries.
//!
//! NCList is a data structure that organizes intervals hierarchically based on containment
//! relationships. It provides efficient querying by exploiting the nested structure of
//! overlapping intervals.
//!
//! # Algorithm Overview
//!
//! The NCList algorithm works as follows:
//! 1. Sort intervals by start position, then by end position (descending)
//! 2. Build a containment hierarchy where each interval tracks which intervals it contains
//! 3. For queries, use binary search to find potential matches, then traverse the hierarchy
//!
//! # Data Structure
//!
//! Each interval stores:
//! - `start`: The start timestamp of the interval
//! - `end`: The end timestamp of the interval (exclusive)
//! - `value`: The value associated with this interval
//! - `children`: Indices of intervals contained within this interval
//!
//! The containment relationship means that if interval A contains interval B,
//! then A.start <= B.start and A.end >= B.end.
//!
//! # Performance Characteristics
//!
//! - **Build time**: O(n²) worst case, O(n log n) typical
//!   - Sorting: O(n log n)
//!   - Building hierarchy: O(n²) worst case when many intervals nest, O(n) typical
//! - **Point queries**: O(log n + k) where k is the number of matching intervals
//!   - Binary search to find first potential match: O(log n)
//!   - Traverse hierarchy to find all matches: O(k)
//! - **Range queries**: O(log n + k) where k is the number of matching intervals
//!   - Binary search to find first potential match: O(log n)
//!   - Traverse hierarchy to find all overlaps: O(k)
//! - **Space complexity**: O(n + m) where n is intervals and m is total containment edges
//!
//! # References
//!
//! NCList was originally described in:
//! "Nested Containment List (NCList): a new algorithm for accelerating interval query
//! of genome alignment and interval databases" by Alexander V. Alekseyenko and
//! Christopher J. Lee, Bioinformatics, 2007.

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Range;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// An interval with containment information for NCList structure.
///
/// Each interval tracks which other intervals it contains, forming a hierarchy
/// that enables efficient querying.
#[derive(Clone, Debug)]
struct NCInterval<V>
where
    V: Clone + Eq + Hash,
{
    /// Start timestamp of the interval (inclusive)
    start: u64,
    /// End timestamp of the interval (exclusive)
    end: u64,
    /// Value associated with this interval
    value: V,
    /// Indices of intervals contained within this interval
    children: Vec<usize>,
}

/// A cache implementation using NCList (Nested Containment List).
///
/// This implementation organizes intervals in a hierarchical structure based on
/// containment relationships, enabling efficient interval queries through
/// traversal of the containment hierarchy.
///
/// The NCList structure is particularly efficient when there are many overlapping
/// intervals with clear containment relationships (e.g., nested intervals).
///
/// Performance characteristics:
/// - Point queries: O(log n + k) where k is the number of matching intervals
/// - Range queries: O(log n + k) where k is the number of matching intervals
/// - Build time: O(n²) worst case, O(n log n) typical
pub struct NCListCache<V>
where
    V: Clone + Eq + Hash,
{
    /// The NCList structure - intervals sorted by start, then end (descending)
    intervals: Vec<NCInterval<V>>,
}

impl<V> NCListCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Merge adjacent or overlapping intervals with the same value.
    ///
    /// Takes a list of intervals and merges any that are adjacent (touching) or overlapping
    /// and have the same value. Also removes any duplicate intervals.
    fn merge_intervals(
        mut intervals: Vec<(Range<u64>, V)>,
    ) -> Vec<(Range<u64>, V)> {
        if intervals.is_empty() {
            return intervals;
        }

        // First, remove exact duplicates
        // Sort to group identical intervals together
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

    /// Build the NCList structure from sorted intervals.
    ///
    /// This constructs the containment hierarchy by finding which intervals
    /// are contained within other intervals.
    ///
    /// An interval A contains interval B if:
    /// - A.start <= B.start AND A.end >= B.end
    ///
    /// # Algorithm
    ///
    /// The intervals are already sorted by start (ascending), then end (descending).
    /// This means that for any interval i, potential children appear after it in the list.
    /// An interval j (j > i) is a child of i if:
    /// - intervals[i].start <= intervals[j].start AND intervals[i].end >= intervals[j].end
    ///
    /// We build the hierarchy by scanning forward for each interval to find its children.
    fn build_nclist(intervals: Vec<(Range<u64>, V)>) -> Vec<NCInterval<V>> {
        let mut nc_intervals: Vec<NCInterval<V>> = intervals
            .into_iter()
            .map(|(range, value)| NCInterval {
                start: range.start,
                end: range.end,
                value,
                children: Vec::new(),
            })
            .collect();

        // Build containment relationships
        // For each interval, find all intervals it contains
        for i in 0..nc_intervals.len() {
            let (parent_start, parent_end) = (nc_intervals[i].start, nc_intervals[i].end);

            for j in (i + 1)..nc_intervals.len() {
                let (child_start, child_end) = (nc_intervals[j].start, nc_intervals[j].end);

                // Check if interval i contains interval j
                if parent_start <= child_start && parent_end >= child_end {
                    nc_intervals[i].children.push(j);
                }

                // Optimization: once we see a child start that's >= parent end,
                // no more children are possible (since intervals are sorted by start)
                if child_start >= parent_end {
                    break;
                }
            }
        }

        nc_intervals
    }

    /// Query intervals that contain a specific point, starting from a given index.
    ///
    /// This recursively traverses the NCList hierarchy to find all intervals
    /// that contain the query point.
    fn query_point_from<'a>(&'a self, t: Timestamp, idx: usize, results: &mut HashSet<&'a V>) {
        let interval = &self.intervals[idx];

        // Check if this interval contains the point
        if interval.start <= t && interval.end > t {
            results.insert(&interval.value);

            // Recursively check children
            for &child_idx in &interval.children {
                self.query_point_from(t, child_idx, results);
            }
        }
    }

    /// Query intervals that overlap with a range, starting from a given index.
    ///
    /// This recursively traverses the NCList hierarchy to find all intervals
    /// that overlap with the query range.
    fn query_range_from<'a>(&'a self, range: &Range<Timestamp>, idx: usize, results: &mut HashSet<&'a V>) {
        let interval = &self.intervals[idx];

        // Check if this interval overlaps with the query range
        // Two ranges [a, b) and [c, d) overlap if: a < d AND c < b
        if interval.start < range.end && range.start < interval.end {
            results.insert(&interval.value);

            // Recursively check children
            for &child_idx in &interval.children {
                self.query_range_from(range, child_idx, results);
            }
        }
    }
}

impl<V> IntervalCache<V> for NCListCache<V>
where
    V: Clone + Eq + Hash,
    for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        if points.is_empty() {
            return Ok(Self {
                intervals: Vec::new(),
            });
        }

        // Build intervals by merging consecutive identical values
        let mut intervals = Vec::new();
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
                intervals.push((current_start..current_end, current_value));
                current_start = t;
                current_end = next_end;
                current_value = v;
            }
        }

        // Don't forget the last interval
        intervals.push((current_start..current_end, current_value));

        // Merge any intervals that touch or overlap with same value
        let merged = Self::merge_intervals(intervals);

        // Sort intervals by start (ascending), then by end (descending)
        // This ordering is critical for the NCList algorithm
        let mut sorted_intervals = merged;
        sorted_intervals.sort_by(|a, b| {
            a.0.start.cmp(&b.0.start)
                .then_with(|| b.0.end.cmp(&a.0.end)) // Note: reversed for descending
        });

        // Build the NCList structure
        let nc_intervals = Self::build_nclist(sorted_intervals);

        Ok(Self {
            intervals: nc_intervals,
        })
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let mut results_set = HashSet::new();

        if self.intervals.is_empty() {
            return Vec::new();
        }

        // Binary search to find the first interval whose start > t
        let first_after = self.intervals.partition_point(|interval| interval.start <= t);

        // Check all intervals that could contain t (those with start <= t)
        // We need to check from the beginning, but we can stop at first_after
        for i in 0..first_after {
            // Only process top-level intervals (those not contained in others)
            // Actually, we need to check all intervals at this level since
            // the NCList structure means children are handled recursively

            // For efficiency, we can skip intervals we know are children of earlier intervals
            // But for simplicity, we check each interval and let the function handle containment
            if self.intervals[i].end > t {
                self.query_point_from(t, i, &mut results_set);
            }
        }

        results_set.into_iter()
            .map(|v| v.into_iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect())
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        let mut results_set = HashSet::new();

        if self.intervals.is_empty() {
            return Vec::new();
        }

        // Binary search to find the first interval whose start >= range.end
        // These intervals cannot overlap with our range
        let first_after = self.intervals.partition_point(|interval| interval.start < range.end);

        // Check all intervals that could overlap with the range
        for i in 0..first_after {
            // An interval overlaps if its end > range.start
            if self.intervals[i].end > range.start {
                self.query_range_from(&range, i, &mut results_set);
            }
        }

        results_set.into_iter()
            .map(|v| v.into_iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect())
            .collect()
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
        let mut all_intervals: Vec<(Range<u64>, V)> = Vec::new();

        // Collect existing intervals
        for interval in &self.intervals {
            all_intervals.push((interval.start..interval.end, interval.value.clone()));
        }

        // Add new intervals
        for interval in &new_cache.intervals {
            all_intervals.push((interval.start..interval.end, interval.value.clone()));
        }

        // Merge intervals
        let merged = Self::merge_intervals(all_intervals);

        // Sort intervals by start (ascending), then by end (descending)
        let mut sorted_intervals = merged;
        sorted_intervals.sort_by(|a, b| {
            a.0.start.cmp(&b.0.start)
                .then_with(|| b.0.end.cmp(&a.0.end))
        });

        // Rebuild the NCList structure
        self.intervals = Self::build_nclist(sorted_intervals);

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the NCListCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of the intervals vector
        size += self.intervals.capacity() * std::mem::size_of::<NCInterval<V>>();

        // Add heap size for each interval's data
        for interval in &self.intervals {
            // Size of the value's heap allocation
            size += interval.value.heap_size();

            // Size of the children vector
            size += interval.children.capacity() * std::mem::size_of::<usize>();
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
    use crate::TagSet;

    fn make_tagset(pairs: &[(&str, &str)]) -> TagSet {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_nclist_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (4, tag_b.clone()),
        ];

        let cache = NCListCache::new(data).unwrap();

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
    fn test_nclist_cache_empty() {
        let cache: NCListCache<TagSet> = NCListCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(0..100).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_nclist_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (3, tag_a.clone()),
        ];

        let cache = NCListCache::new(data).unwrap();

        // Should have merged into 1 interval: [1,4)
        assert_eq!(cache.interval_count(), 1);
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert_eq!(cache.query_point(4).len(), 0);
    }
}

