//! IntervalCache implementation using the interavl crate (AVL-based interval tree).

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

use interavl::IntervalTree;

use crate::{CacheBuildError, IntervalCache, Timestamp};

/// An interval cache implementation using the interavl AVL-based tree.
pub struct InteravlCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Maps each interval to its associated value
    intervals: Vec<(Range<u64>, V)>,
    /// The AVL-based interval tree for efficient queries
    tree: IntervalTree<u64, usize>,  // usize is index into intervals vec
}

impl<V> IntervalCache<V> for InteravlCache<V>
where
    V: Clone + Eq + Hash,
{
    fn new(mut points: Vec<(Timestamp, V)>) -> Result<Self, CacheBuildError> {
        // Sort points by timestamp
        points.sort_by_key(|(t, _)| *t);

        let mut intervals = Vec::new();
        let mut tree = IntervalTree::default();

        if points.is_empty() {
            return Ok(Self { intervals, tree });
        }

        // Track open intervals for each value to handle overlapping
        let mut open_intervals: HashMap<V, (u64, u64)> = HashMap::new();

        for (t, v) in points.into_iter() {
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
                    let idx = intervals.len();
                    intervals.push((interval.clone(), v.clone()));
                    tree.insert(interval, idx);

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
            let interval = start..end;
            let idx = intervals.len();
            intervals.push((interval.clone(), v));
            tree.insert(interval, idx);
        }

        Ok(Self { intervals, tree })
    }

    fn query_point(&self, t: Timestamp) -> Vec<&V> {
        let point_interval = t..(t + 1);

        self.tree
            .iter_overlaps(&point_interval)
            .filter_map(|(_, idx)| {
                self.intervals.get(*idx).map(|(_, v)| v)
            })
            .collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        // Collect unique values to avoid duplicates
        let mut seen = std::collections::HashSet::new();
        self.tree
            .iter_overlaps(&range)
            .filter_map(|(_, idx)| {
                self.intervals.get(*idx).and_then(|(_, v)| {
                    if seen.insert(v) {
                        Some(v)
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    fn append_batch(&mut self, new_points: Vec<(Timestamp, V)>) -> Result<(), CacheBuildError> {
        // Build new intervals from new points
        let new_cache = Self::new(new_points)?;

        // Add new intervals to our collection
        for (interval, value) in new_cache.intervals {
            let idx = self.intervals.len();
            self.intervals.push((interval.clone(), value));
            self.tree.insert(interval, idx);
        }

        Ok(())
    }
}