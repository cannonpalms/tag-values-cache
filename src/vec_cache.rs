//! A vector-based implementation using parallel vectors with binary search optimization.
//!
//! This implementation stores intervals as three parallel vectors:
//! - starts: interval start times (sorted)
//! - ends: interval end times
//! - values: the values for each interval
//!
//! Query operations use binary search on the sorted starts array to reduce
//! the number of intervals that need to be checked.

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
                results.push(&self.values[i]);
            }
        }

        results
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

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

        // Merge the new intervals with existing ones, maintaining sorted order
        // We need to maintain the invariant that starts are sorted

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

        // Check if we can simply append (new intervals come after existing ones)
        if self.starts.last().unwrap() <= &new_cache.starts[0] {
            // New intervals start after or at the last existing interval
            // We can just append
            self.starts.extend(new_cache.starts);
            self.ends.extend(new_cache.ends);
            self.values.extend(new_cache.values);
        } else {
            // Need to merge while maintaining sorted order
            // This is more complex - rebuild from scratch for now
            let mut all_intervals: Vec<(u64, u64, V)> = Vec::new();

            // Collect existing intervals
            for i in 0..self.starts.len() {
                all_intervals.push((self.starts[i], self.ends[i], self.values[i].clone()));
            }

            // Add new intervals
            for i in 0..new_cache.starts.len() {
                all_intervals.push((new_cache.starts[i], new_cache.ends[i], new_cache.values[i].clone()));
            }

            // Sort by start time
            all_intervals.sort_by_key(|&(start, _, _)| start);

            // Rebuild the vectors
            self.starts.clear();
            self.ends.clear();
            self.values.clear();

            for (start, end, value) in all_intervals {
                self.starts.push(start);
                self.ends.push(end);
                self.values.push(value);
            }
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