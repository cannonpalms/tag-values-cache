//! `IntervalCache` implementation using the rust-lapper crate.
//!
//! This implementation uses rust-lapper's interval tree, which provides
//! fast interval overlap queries and is optimized for genomic data workloads.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

use rust_lapper::{Interval, Lapper};

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// An interval cache implementation using rust-lapper.
///
/// Rust-lapper is designed for fast interval overlap queries and provides
/// excellent performance for range queries. It uses a sorted vector with
/// augmented interval tree metadata.
pub struct LapperCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Maps each unique value to its own Lapper instance
    lappers: HashMap<V, Lapper<usize, usize>>,
    /// All intervals stored in a single vector
    intervals: Vec<(Range<u64>, V)>,
}

impl<V> LapperCache<V>
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

    /// Build the Lapper structures from the intervals vector
    fn build_lappers(intervals: &[(Range<u64>, V)]) -> HashMap<V, Lapper<usize, usize>> {
        // Group intervals by value
        let mut value_intervals: HashMap<V, Vec<Interval<usize, usize>>> = HashMap::new();

        for (idx, (range, value)) in intervals.iter().enumerate() {
            let interval = Interval {
                start: range.start as usize,
                stop: range.end as usize,
                val: idx,
            };
            value_intervals
                .entry(value.clone())
                .or_default()
                .push(interval);
        }

        // Create a Lapper for each value
        value_intervals
            .into_iter()
            .map(|(value, mut ivs)| {
                // rust-lapper requires intervals to be sorted by start
                ivs.sort_by_key(|iv| iv.start);
                (value, Lapper::new(ivs))
            })
            .collect()
    }
}

impl<V> IntervalCache<V> for LapperCache<V>
where
    V: Clone + Eq + Hash,
    for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(Self {
                intervals,
                lappers: HashMap::new(),
            });
        }

        // Build intervals by merging consecutive identical values
        // Track open intervals for each value to handle overlapping
        let mut open_intervals: HashMap<V, (u64, u64)> = HashMap::new();

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
                    let interval = *start..*end;
                    intervals.push((interval, v.clone()));

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

        // Build the Lapper structures
        let lappers = Self::build_lappers(&merged);

        Ok(Self {
            intervals: merged,
            lappers,
        })
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let start = t as usize;
        let stop = (t + 1) as usize;

        let mut results = Vec::new();

        // Query all lappers and collect results
        for (value, lapper) in &self.lappers {
            if lapper.find(start, stop).next().is_some() {
                let tag_vec: Vec<(&str, &str)> = value
                    .into_iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                results.push(tag_vec);
            }
        }

        results
    }

    fn query_range(&self, range: &Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        let start = range.start as usize;
        let stop = range.end as usize;

        let mut results = Vec::new();

        // Query all lappers and collect unique values
        for (value, lapper) in &self.lappers {
            if lapper.find(start, stop).next().is_some() {
                let tag_vec: Vec<(&str, &str)> = value
                    .into_iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                results.push(tag_vec);
            }
        }

        results
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points
        let new_cache = Self::from_sorted(sorted_data)?;

        // Collect all intervals (existing + new)
        let mut all_intervals = self.intervals.clone();
        all_intervals.extend(new_cache.intervals);

        // Merge adjacent intervals with same value for better efficiency
        let merged = Self::merge_intervals(all_intervals);

        // Rebuild the Lapper structures
        self.intervals = merged;
        self.lappers = Self::build_lappers(&self.intervals);

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the LapperCache struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of the intervals vector
        // Each element is (Range<u64>, V)
        size += self.intervals.capacity()
            * (std::mem::size_of::<Range<u64>>() + std::mem::size_of::<V>());

        // Add heap size for values if they contain heap-allocated data
        for (_, value) in &self.intervals {
            size += value.heap_size();
        }

        // Size of the HashMap and Lapper structures
        // HashMap overhead
        size += self.lappers.capacity() * std::mem::size_of::<(V, Lapper<usize, usize>)>();

        // Each Lapper contains a sorted Vec of intervals
        // Estimate based on number of intervals per lapper
        for (value, lapper) in &self.lappers {
            size += value.heap_size();
            // Lapper internally stores Vec<Interval<usize, usize>>
            // Each Interval is 24 bytes (3 * usize)
            size += lapper.len() * std::mem::size_of::<Interval<usize, usize>>();
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
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_lapper_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (4, tag_b.clone())];

        let cache = LapperCache::new(data).unwrap();

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
    fn test_lapper_cache_empty() {
        let cache: LapperCache<TagSet> = LapperCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(&(0..100)).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_lapper_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (3, tag_a.clone())];

        let cache = LapperCache::new(data).unwrap();

        // Should have merged into 1 interval: [1,4)
        assert_eq!(cache.interval_count(), 1);
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert_eq!(cache.query_point(4).len(), 0);
    }
}
