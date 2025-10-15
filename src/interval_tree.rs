//! IntervalTree-based implementation of the IntervalCache trait.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

use intervaltree::IntervalTree;

use crate::{CacheBuildError, IntervalCache, Timestamp};

/// An interval cache implementation using an interval tree.
pub struct IntervalTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    tree: IntervalTree<u64, V>,
}

impl<V> IntervalCache<V> for IntervalTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    fn new(mut points: Vec<(Timestamp, V)>) -> Result<Self, CacheBuildError> {
        // Sort points by timestamp
        points.sort_by_key(|(t, _)| *t);

        let tree = Self::build_multivalued_tree(points)?;
        Ok(Self { tree })
    }

    fn query_point(&self, t: Timestamp) -> Vec<&V> {
        self.tree.query_point(t).map(|e| &e.value).collect()
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V> {
        self.tree.query(range).map(|e| &e.value).collect()
    }
}

impl<V> IntervalTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    fn build_multivalued_tree(
        points: Vec<(u64, V)>,
    ) -> Result<IntervalTree<u64, V>, CacheBuildError> {
        // Map: value -> (current_run_start, current_run_end)
        let mut open: HashMap<V, (u64, u64)> = HashMap::new();
        let mut runs: Vec<(Range<u64>, V)> = Vec::new();

        for (t, v) in points.into_iter() {
            let end = t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(t))?;

            // If we saw this value last at exactly t-1, extend; otherwise flush/start new.
            match open.get_mut(&v) {
                Some((start, last_end)) => {
                    if *last_end == t {
                        *last_end = end; // extend consecutive run
                    } else {
                        // gap: flush old run, start new
                        runs.push((*start..*last_end, v.clone()));
                        *start = t;
                        *last_end = end;
                    }
                }
                None => {
                    open.insert(v.clone(), (t, end));
                }
            }
        }

        // flush all open runs
        for (v, (s, e)) in open.into_iter() {
            runs.push((s..e, v));
        }

        // Build the interval tree
        Ok(runs.into_iter().collect())
    }
}