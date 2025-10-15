//! A simple vector-based implementation using parallel vectors with shared indexing.
//!
//! This implementation stores intervals as three parallel vectors:
//! - starts: interval start times
//! - ends: interval end times
//! - values: the values for each interval
//!
//! This demonstrates how different data structures can implement the same
//! IntervalCache trait interface.

use std::hash::Hash;
use std::ops::Range;

use crate::{CacheBuildError, IntervalCache, Timestamp};

/// A cache implementation using three parallel vectors.
///
/// This is a simpler (though potentially less efficient for large datasets)
/// alternative to the interval tree implementation.
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

impl<V> IntervalCache<V> for VecCache<V>
where
    V: Clone + Eq + Hash,
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

        Ok(Self {
            starts,
            ends,
            values,
        })
    }

    fn query_point(&self, t: Timestamp) -> Vec<&V> {
        let mut results = Vec::new();

        // Linear scan through all intervals
        for i in 0..self.starts.len() {
            if t >= self.starts[i] && t < self.ends[i] {
                results.push(&self.values[i]);
            }
        }

        results
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Find all intervals that overlap with the query range
        for i in 0..self.starts.len() {
            // Check if interval [starts[i], ends[i]) overlaps with range
            if self.starts[i] < range.end && self.ends[i] > range.start {
                if seen.insert(&self.values[i]) {
                    results.push(&self.values[i]);
                }
            }
        }

        results
    }

    fn append_sorted(&mut self, sorted_data: crate::SortedData<V>) -> Result<(), CacheBuildError> {
        // Build intervals from sorted points
        let new_cache = Self::from_sorted(sorted_data)?;

        // Simply append new intervals to existing ones
        // TODO: Merge adjacent intervals with same value for better efficiency
        self.starts.extend(new_cache.starts);
        self.ends.extend(new_cache.ends);
        self.values.extend(new_cache.values);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_cache_basic() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (4, "B".to_string()),
        ];

        let cache = VecCache::new(data).unwrap();

        assert_eq!(cache.query_point(1), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(2), vec![&"A".to_string()]);
        assert_eq!(cache.query_point(3), Vec::<&String>::new());
        assert_eq!(cache.query_point(4), vec![&"B".to_string()]);
    }

    #[test]
    fn test_vec_cache_range() {
        let data = vec![
            (1, "A".to_string()),
            (2, "A".to_string()),
            (5, "B".to_string()),
            (6, "C".to_string()),
        ];

        let cache = VecCache::new(data).unwrap();

        let range_values = cache.query_range(1..6);
        assert_eq!(range_values.len(), 2);
        assert!(range_values.iter().any(|v| **v == "A".to_string()));
        assert!(range_values.iter().any(|v| **v == "B".to_string()));
    }
}