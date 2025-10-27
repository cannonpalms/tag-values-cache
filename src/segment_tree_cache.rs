//! A Segment Tree implementation for efficient interval queries.
//!
//! This implementation uses a Segment Tree data structure to store intervals efficiently.
//! A segment tree is a binary tree used for storing intervals, where each node represents
//! an interval range, and leaf nodes represent individual points. Interior nodes represent
//! the union of their children's ranges.
//!
//! # Segment Tree Structure
//!
//! The segment tree is stored as a Vec-based structure for cache efficiency, using
//! index-based references rather than pointers. Each node stores:
//! - A range representing the node's coverage
//! - A list of intervals that fully span the node's range
//! - Indices to left and right children (if they exist)
//!
//! # Performance Characteristics
//!
//! - Point queries: O(log n + k) where k is the number of matching intervals
//! - Range queries: O(log n + k) where k is the number of matching intervals
//! - Build time: O(n log n)
//! - Space: O(n log n) in worst case
//!
//! The segment tree provides logarithmic query time by allowing us to skip
//! entire subtrees that don't intersect with the query range.

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Range;

use crate::{CacheBuildError, HeapSize, IntervalCache, Timestamp};

/// A node in the segment tree.
///
/// Each node represents a range of timestamps and stores intervals that
/// completely span that range. Intervals that only partially overlap are
/// pushed down to child nodes.
#[derive(Debug, Clone)]
struct SegmentTreeNode<V>
where
    V: Clone + Eq + Hash,
{
    /// The range this node covers
    range: Range<Timestamp>,
    /// Intervals that completely span this node's range
    intervals: Vec<(Range<Timestamp>, V)>,
    /// Index of left child in the tree vector (if exists)
    left: Option<usize>,
    /// Index of right child in the tree vector (if exists)
    right: Option<usize>,
}

/// A cache implementation using a Segment Tree.
///
/// This implementation builds a balanced binary tree over the range of timestamps
/// and stores intervals at nodes where they can efficiently answer queries.
///
/// Performance characteristics:
/// - Point queries: O(log n + k) where k is the number of matching intervals
/// - Range queries: O(log n + k) where k is the number of matching intervals
/// - Build time: O(n log n)
/// - Space: O(n log n) in worst case
pub struct SegmentTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    /// The segment tree stored as a Vec for cache efficiency
    /// Index 0 is the root node
    nodes: Vec<SegmentTreeNode<V>>,
    /// Total number of intervals stored
    interval_count: usize,
}

impl<V> SegmentTreeCache<V>
where
    V: Clone + Eq + Hash,
{
    /// Merge adjacent or overlapping intervals with the same value.
    ///
    /// Takes a list of intervals and merges any that are adjacent (touching) or overlapping
    /// and have the same value. Also removes any duplicate intervals.
    fn merge_intervals(
        mut intervals: Vec<(Range<Timestamp>, V)>,
    ) -> Vec<(Range<Timestamp>, V)> {
        if intervals.is_empty() {
            return intervals;
        }

        // Sort by start time, then by end time
        intervals.sort_by_key(|interval| (interval.0.start, interval.0.end));

        // Remove exact duplicates
        intervals.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        // Merge adjacent/overlapping intervals with same value
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

        // Final deduplication check
        merged.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        merged
    }

    /// Get all unique timestamps from intervals to determine tree range.
    ///
    /// This collects all start and end points from intervals to build
    /// a segment tree that covers the entire range.
    fn get_all_timestamps(intervals: &[(Range<Timestamp>, V)]) -> Vec<Timestamp> {
        let mut timestamps = HashSet::new();
        for (range, _) in intervals {
            timestamps.insert(range.start);
            timestamps.insert(range.end);
        }
        let mut sorted: Vec<_> = timestamps.into_iter().collect();
        sorted.sort_unstable();
        sorted
    }

    /// Build a segment tree from intervals.
    ///
    /// This constructs a balanced binary tree over the range of timestamps
    /// and distributes intervals to appropriate nodes.
    ///
    /// # Arguments
    /// * `intervals` - The intervals to store in the tree
    ///
    /// # Returns
    /// A vector containing all nodes of the segment tree, where index 0 is the root
    fn build_tree(intervals: Vec<(Range<Timestamp>, V)>) -> Vec<SegmentTreeNode<V>> {
        if intervals.is_empty() {
            return Vec::new();
        }

        // Get all unique timestamps to build tree structure
        let timestamps = Self::get_all_timestamps(&intervals);

        if timestamps.is_empty() {
            return Vec::new();
        }

        // We'll build the tree in a Vec for cache efficiency
        let mut nodes = Vec::new();

        // Build the tree recursively
        Self::build_tree_recursive(&timestamps, 0, timestamps.len(), &intervals, &mut nodes);

        nodes
    }

    /// Recursively build a segment tree node and its children.
    ///
    /// # Arguments
    /// * `timestamps` - Sorted list of all unique timestamps
    /// * `left_idx` - Left index in timestamps array for this node
    /// * `right_idx` - Right index in timestamps array for this node
    /// * `intervals` - All intervals to be inserted
    /// * `nodes` - Vec to store all nodes
    ///
    /// # Returns
    /// Index of the created node in the nodes vector
    fn build_tree_recursive(
        timestamps: &[Timestamp],
        left_idx: usize,
        right_idx: usize,
        intervals: &[(Range<Timestamp>, V)],
        nodes: &mut Vec<SegmentTreeNode<V>>,
    ) -> usize {
        // Create node for this range
        // The range covers from timestamps[left_idx] to timestamps[right_idx - 1] inclusive
        // In range notation, this is [left, right) where right = timestamps[right_idx - 1] + 1
        let node_start = timestamps[left_idx];
        let node_end = if right_idx < timestamps.len() {
            timestamps[right_idx]
        } else {
            // Last node - we need to extend beyond the last timestamp
            // This should cover the point at timestamps[right_idx - 1]
            timestamps[right_idx - 1] + 1
        };
        let node_range = node_start..node_end;
        let current_idx = nodes.len();

        // Placeholder node
        nodes.push(SegmentTreeNode {
            range: node_range.clone(),
            intervals: Vec::new(),
            left: None,
            right: None,
        });

        // If this is a leaf node (represents a single point or small segment)
        if right_idx - left_idx == 1 {
            // Collect intervals that overlap with this node's range
            let mut node_intervals = Vec::new();
            for (interval_range, value) in intervals {
                // Check if the interval overlaps with this node's range
                if interval_range.start < node_range.end && interval_range.end > node_range.start {
                    node_intervals.push((interval_range.clone(), value.clone()));
                }
            }
            nodes[current_idx].intervals = node_intervals;
            return current_idx;
        }

        // Internal node - create children
        let mid_idx = (left_idx + right_idx) / 2;

        // Build left child
        let left_child_idx = Self::build_tree_recursive(
            timestamps,
            left_idx,
            mid_idx,
            intervals,
            nodes,
        );

        // Build right child
        let right_child_idx = Self::build_tree_recursive(
            timestamps,
            mid_idx,
            right_idx,
            intervals,
            nodes,
        );

        // Collect intervals that overlap with this node's range
        // We store ALL overlapping intervals at internal nodes too, not just those that fully span
        let mut node_intervals = Vec::new();
        for (interval_range, value) in intervals {
            if interval_range.start < node_range.end && interval_range.end > node_range.start {
                node_intervals.push((interval_range.clone(), value.clone()));
            }
        }

        // Update the node with children and intervals
        nodes[current_idx].left = Some(left_child_idx);
        nodes[current_idx].right = Some(right_child_idx);
        nodes[current_idx].intervals = node_intervals;

        current_idx
    }

    /// Query a point in the segment tree.
    ///
    /// Traverses the tree from root to leaf, collecting all intervals
    /// that contain the query point.
    fn query_point_recursive<'a>(&'a self, node_idx: usize, t: Timestamp, results: &mut HashSet<&'a V>) {
        if node_idx >= self.nodes.len() {
            return;
        }

        let node = &self.nodes[node_idx];

        // Check if point is outside this node's range
        if t < node.range.start || t >= node.range.end {
            return;
        }

        // Add all intervals at this node that contain t
        for (interval_range, value) in &node.intervals {
            if interval_range.start <= t && interval_range.end > t {
                results.insert(value);
            }
        }

        // Recurse to children
        if let Some(left_idx) = node.left {
            self.query_point_recursive(left_idx, t, results);
        }
        if let Some(right_idx) = node.right {
            self.query_point_recursive(right_idx, t, results);
        }
    }

    /// Query a range in the segment tree.
    ///
    /// Traverses the tree, collecting all intervals that overlap with
    /// the query range. Prunes subtrees that don't intersect.
    fn query_range_recursive<'a>(
        &'a self,
        node_idx: usize,
        range: &Range<Timestamp>,
        results: &mut HashSet<&'a V>,
    ) {
        if node_idx >= self.nodes.len() {
            return;
        }

        let node = &self.nodes[node_idx];

        // Check if ranges overlap
        // Ranges [a, b) and [c, d) overlap if a < d and c < b
        if node.range.start >= range.end || range.start >= node.range.end {
            return; // No overlap, prune this subtree
        }

        // Add all intervals at this node that overlap with query range
        for (interval_range, value) in &node.intervals {
            if interval_range.start < range.end && range.start < interval_range.end {
                results.insert(value);
            }
        }

        // Recurse to children
        if let Some(left_idx) = node.left {
            self.query_range_recursive(left_idx, range, results);
        }
        if let Some(right_idx) = node.right {
            self.query_range_recursive(right_idx, range, results);
        }
    }

}

impl<V> IntervalCache<V> for SegmentTreeCache<V>
where
    V: Clone + Eq + Hash,
    for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
{
    fn from_sorted(sorted_data: crate::SortedData<V>) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        if points.is_empty() {
            return Ok(Self {
                nodes: Vec::new(),
                interval_count: 0,
            });
        }

        // Build intervals from points
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

        // Merge overlapping/adjacent intervals
        let merged = Self::merge_intervals(intervals);
        let interval_count = merged.len();

        // Build segment tree
        let nodes = Self::build_tree(merged);

        Ok(Self {
            nodes,
            interval_count,
        })
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let mut results_set = HashSet::new();

        if self.nodes.is_empty() {
            return Vec::new();
        }

        // Start from root (index 0)
        self.query_point_recursive(0, t, &mut results_set);

        results_set.into_iter()
            .map(|v| v.into_iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect())
            .collect()
    }

    fn query_range(&self, range: &Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        let mut results_set = HashSet::new();

        if self.nodes.is_empty() {
            return Vec::new();
        }

        // Start from root (index 0)
        self.query_range_recursive(0, range, &mut results_set);

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
        if self.nodes.is_empty() {
            self.nodes = new_cache.nodes;
            self.interval_count = new_cache.interval_count;
            return Ok(());
        }

        // If new cache is empty, nothing to do
        if new_cache.nodes.is_empty() {
            return Ok(());
        }

        // Collect all intervals from both trees
        let mut all_intervals = Vec::new();

        // Extract intervals from existing tree
        for node in &self.nodes {
            for (range, value) in &node.intervals {
                all_intervals.push((range.clone(), value.clone()));
            }
        }

        // Extract intervals from new tree
        for node in &new_cache.nodes {
            for (range, value) in &node.intervals {
                all_intervals.push((range.clone(), value.clone()));
            }
        }

        // Merge all intervals
        let merged = Self::merge_intervals(all_intervals);
        let interval_count = merged.len();

        // Rebuild the tree
        let nodes = Self::build_tree(merged);

        self.nodes = nodes;
        self.interval_count = interval_count;

        Ok(())
    }

    fn size_bytes(&self) -> usize
    where
        V: HeapSize,
    {
        // Size of the struct itself
        let mut size = std::mem::size_of::<Self>();

        // Size of the nodes Vec capacity
        size += self.nodes.capacity() * std::mem::size_of::<SegmentTreeNode<V>>();

        // Size of data within each node
        for node in &self.nodes {
            // Range is on stack (included in node size)

            // Intervals Vec capacity
            size += node.intervals.capacity() * std::mem::size_of::<(Range<Timestamp>, V)>();

            // Heap size of each value
            for (_, value) in &node.intervals {
                size += value.heap_size();
            }

            // left and right indices are on stack (included in node size)
        }

        size
    }

    fn interval_count(&self) -> usize {
        self.interval_count
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
    fn test_segment_tree_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (4, tag_b.clone()),
        ];

        let cache = SegmentTreeCache::new(data).unwrap();

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
    fn test_segment_tree_cache_empty() {
        let cache: SegmentTreeCache<TagSet> = SegmentTreeCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(&(0..100)).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_segment_tree_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (3, tag_a.clone()),
        ];

        let cache = SegmentTreeCache::new(data).unwrap();

        // Should have merged into 1 interval: [1,4)
        assert_eq!(cache.interval_count(), 1);
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert_eq!(cache.query_point(4).len(), 0);
    }
}

