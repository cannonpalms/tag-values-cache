//! A value-aware wrapper around rust-lapper that considers values when merging intervals.
//!
//! Unlike rust-lapper's native `merge_overlaps()` which ignores the `val` field,
//! this wrapper only merges intervals when both the boundaries AND values match.

use num_traits::{PrimInt, Unsigned};
use rust_lapper::{Interval, Lapper};
use std::fmt;
use std::ops::Range;

/// A wrapper around rust-lapper's Lapper that provides value-aware merging.
///
/// This type works the same way as Lapper, except when determining node equality
/// or overlap for merge purposes, the values must match too. Intervals with the
/// same boundaries but different values will NOT be merged.
///
/// # Type Parameters
/// * `T` - The numeric type for interval boundaries (must satisfy rust-lapper's requirements)
/// * `V` - The value type (must be Clone + Eq for value-aware merging)
pub struct ValueAwareLapper<T, V>
where
    T: PrimInt + Unsigned + Ord + Clone + Send + Sync,
    V: Clone + Eq + Ord + Send + Sync,
{
    /// The underlying Lapper instance
    lapper: Lapper<T, V>,
}

impl<T, V> ValueAwareLapper<T, V>
where
    T: PrimInt + Unsigned + Ord + Clone + Send + Sync,
    V: Clone + Eq + Ord + Send + Sync,
{
    /// Create a new ValueAwareLapper from a vector of intervals.
    ///
    /// The intervals will be sorted by start position as required by rust-lapper.
    ///
    /// # Arguments
    /// * `intervals` - A vector of Interval<T, V> where each interval has
    ///   start/stop boundaries and an associated value
    ///
    /// # Example
    /// ```ignore
    /// use rust_lapper::Interval;
    /// use tag_values_cache::ValueAwareLapper;
    ///
    /// let intervals = vec![
    ///     Interval { start: 5, stop: 10, val: "A" },
    ///     Interval { start: 5, stop: 10, val: "B" }, // Same boundaries, different value
    /// ];
    /// let vlapper = ValueAwareLapper::new(intervals);
    /// ```
    pub fn new(mut intervals: Vec<Interval<T, V>>) -> Self {
        intervals.sort_by(|a, b| a.start.cmp(&b.start));
        Self {
            lapper: Lapper::new(intervals),
        }
    }

    /// Merge overlapping or adjacent intervals that have the same value.
    ///
    /// This is similar to rust-lapper's `merge_overlaps()` but with a critical difference:
    /// intervals are only merged if they have the same value (as determined by `Eq`).
    ///
    /// Uses a stack-based algorithm to properly handle complex overlapping scenarios.
    /// Two intervals are merged if:
    /// 1. They have the same value (`val` field)
    /// 2. They overlap (one starts before the other ends)
    /// 3. OR they are adjacent (one starts where the other ends)
    ///
    /// # Example
    /// ```ignore
    /// // These WILL merge (same value, overlapping):
    /// Interval { start: 5, stop: 10, val: "A" }
    /// Interval { start: 8, stop: 15, val: "A" }
    /// // Result: Interval { start: 5, stop: 15, val: "A" }
    ///
    /// // These will NOT merge (different values):
    /// Interval { start: 5, stop: 10, val: "A" }
    /// Interval { start: 5, stop: 10, val: "B" }
    /// // Result: Both intervals retained
    /// ```
    pub fn merge_with_values(&mut self) {
        use std::collections::VecDeque;

        // Extract all intervals from the lapper
        let mut intervals: Vec<_> = self.lapper.iter().cloned().collect();

        if intervals.is_empty() {
            return;
        }

        // Sort by value first, then by start, then by stop
        // This groups intervals with the same value together
        intervals.sort_by(|a, b| {
            a.val
                .cmp(&b.val)
                .then(a.start.cmp(&b.start))
                .then(a.stop.cmp(&b.stop))
        });

        // Use stack-based merging (like Lapper's merge_overlaps)
        // but only merge intervals with the same value
        let mut stack: VecDeque<Interval<T, V>> = VecDeque::new();
        let mut ivs = intervals.into_iter();

        if let Some(first) = ivs.next() {
            stack.push_back(first);

            for interval in ivs {
                let mut top = stack.pop_back().unwrap();

                // Check if we can merge: same value AND overlapping/adjacent
                if top.val == interval.val && top.stop >= interval.start {
                    // Merge by extending the top interval
                    if top.stop < interval.stop {
                        top.stop = interval.stop;
                    }
                    stack.push_back(top);
                } else {
                    // Different value or gap - cannot merge
                    stack.push_back(top);
                    stack.push_back(interval);
                }
            }

            // Collect all merged intervals
            let mut merged: Vec<_> = stack.into_iter().collect();

            // Sort by start position for Lapper (required for efficient queries)
            merged.sort_by(|a, b| a.start.cmp(&b.start).then(a.stop.cmp(&b.stop)));

            // Rebuild the lapper
            self.lapper = Lapper::new(merged);
        }
    }

    /// Find all intervals that overlap with the given range.
    ///
    /// Returns an iterator over intervals that overlap with [start, stop).
    ///
    /// # Arguments
    /// * `start` - The start of the query range (inclusive)
    /// * `stop` - The end of the query range (exclusive)
    pub fn find(&self, start: T, stop: T) -> impl Iterator<Item = &Interval<T, V>> {
        self.lapper.find(start, stop)
    }

    /// Get the number of intervals in this ValueAwareLapper.
    pub fn len(&self) -> usize {
        self.lapper.len()
    }

    /// Check if this ValueAwareLapper is empty.
    pub fn is_empty(&self) -> bool {
        self.lapper.is_empty()
    }

    /// Iterate over all intervals in this ValueAwareLapper.
    pub fn iter(&self) -> impl Iterator<Item = &Interval<T, V>> {
        self.lapper.iter()
    }

    /// Seek to a specific position and return an iterator from that point.
    ///
    /// This is useful for sequential queries.
    ///
    /// # Arguments
    /// * `start` - The start of the query range
    /// * `stop` - The end of the query range
    /// * `cursor` - A mutable cursor position for tracking iteration state
    pub fn seek(&self, start: T, stop: T, cursor: &mut usize) -> impl Iterator<Item = &Interval<T, V>> {
        self.lapper.seek(start, stop, cursor)
    }

    /// Get a reference to the underlying Lapper.
    ///
    /// This allows access to other Lapper methods not wrapped by ValueAwareLapper.
    pub fn inner(&self) -> &Lapper<T, V> {
        &self.lapper
    }

    /// Convert a Range into start/stop values for querying.
    ///
    /// Helper method for working with Rust's Range types.
    pub fn find_range(&self, range: Range<T>) -> impl Iterator<Item = &Interval<T, V>> {
        self.find(range.start, range.end)
    }
}

impl<T, V> fmt::Debug for ValueAwareLapper<T, V>
where
    T: PrimInt + Unsigned + Ord + Clone + Send + Sync + fmt::Debug,
    V: Clone + Eq + Ord + Send + Sync + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueAwareLapper")
            .field("len", &self.len())
            .field("intervals", &self.lapper.iter().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_value_aware_lapper() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 15u32,
                stop: 20u32,
                val: "B",
            },
        ];

        let vlapper = ValueAwareLapper::new(intervals);
        assert_eq!(vlapper.len(), 2);
    }

    #[test]
    fn test_merge_same_value_overlapping() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 8u32,
                stop: 15u32,
                val: "A",
            }, // Same value, overlapping
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        assert_eq!(vlapper.len(), 1);
        let merged = vlapper.iter().next().unwrap();
        assert_eq!(merged.start, 5u32);
        assert_eq!(merged.stop, 15u32);
        assert_eq!(merged.val, "A");
    }

    #[test]
    fn test_merge_same_value_adjacent() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 10u32,
                stop: 15u32,
                val: "A",
            }, // Same value, adjacent
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        assert_eq!(vlapper.len(), 1);
        let merged = vlapper.iter().next().unwrap();
        assert_eq!(merged.start, 5u32);
        assert_eq!(merged.stop, 15u32);
    }

    #[test]
    fn test_no_merge_different_values() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "B",
            }, // Same boundaries, different value
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        // Should NOT merge - different values
        assert_eq!(vlapper.len(), 2);
    }

    #[test]
    fn test_no_merge_different_values_overlapping() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 12u32,
                val: "A",
            },
            Interval {
                start: 8u32,
                stop: 15u32,
                val: "B",
            }, // Overlapping, different value
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        // Should NOT merge - different values
        assert_eq!(vlapper.len(), 2);
    }

    #[test]
    fn test_merge_duplicates_same_value() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            }, // Exact duplicate
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            }, // Another duplicate
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        assert_eq!(vlapper.len(), 1);
    }

    #[test]
    fn test_no_merge_with_gap() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 15u32,
                stop: 20u32,
                val: "A",
            }, // Same value but gap
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        // Should NOT merge - gap between intervals
        assert_eq!(vlapper.len(), 2);
    }

    #[test]
    fn test_complex_merge_scenario() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 8u32,
                stop: 15u32,
                val: "A",
            }, // Overlapping, same value
            Interval {
                start: 12u32,
                stop: 20u32,
                val: "A",
            }, // Overlapping with previous, same value
            Interval {
                start: 25u32,
                stop: 30u32,
                val: "A",
            }, // Gap, same value
            Interval {
                start: 10u32,
                stop: 18u32,
                val: "B",
            }, // Overlapping with A's, different value
        ];

        let mut vlapper = ValueAwareLapper::new(intervals);
        vlapper.merge_with_values();

        // Should have:
        // - One merged "A" interval [5, 20) (merges [5,15), [8,15), and [12,20))
        // - One "B" interval [10, 18)
        // - One separate "A" interval [25, 30) (gap)
        assert_eq!(vlapper.len(), 3);

        let merged: Vec<_> = vlapper.iter().collect();

        // Check that we have the right intervals
        assert!(merged.iter().any(|iv| iv.start == 5u32
            && iv.stop == 20u32
            && iv.val == "A"));
        assert!(merged.iter().any(|iv| iv.start == 10u32
            && iv.stop == 18u32
            && iv.val == "B"));
        assert!(merged.iter().any(|iv| iv.start == 25u32
            && iv.stop == 30u32
            && iv.val == "A"));
    }

    #[test]
    fn test_find_queries() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "B",
            }, // Same boundaries, different value
            Interval {
                start: 15u32,
                stop: 20u32,
                val: "C",
            },
        ];

        let vlapper = ValueAwareLapper::new(intervals);

        // Query at position 7 should return both A and B
        let results: Vec<_> = vlapper.find(7u32, 8u32).collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|iv| iv.val == "A"));
        assert!(results.iter().any(|iv| iv.val == "B"));

        // Query at position 17 should return only C
        let results: Vec<_> = vlapper.find(17u32, 18u32).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].val, "C");
    }

    #[test]
    fn test_find_range() {
        let intervals = vec![
            Interval {
                start: 5u32,
                stop: 10u32,
                val: "A",
            },
            Interval {
                start: 15u32,
                stop: 20u32,
                val: "B",
            },
        ];

        let vlapper = ValueAwareLapper::new(intervals);

        // Query range that overlaps first interval
        let results: Vec<_> = vlapper.find_range(0u32..12u32).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].val, "A");

        // Query range that overlaps both
        let results: Vec<_> = vlapper.find_range(0u32..25u32).collect();
        assert_eq!(results.len(), 2);
    }
}
