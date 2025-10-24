//! `IntervalCache` implementation using ValueAwareLapper for value-aware merging.
//!
//! This implementation provides the same interface as LapperCache but uses
//! ValueAwareLapper internally, which only merges intervals when both boundaries
//! AND values match.

use std::collections::HashSet;
use std::ops::Range;
use std::time::Duration;

use arrow_util::dictionary::StringDictionary;
use rust_lapper::Interval;

use crate::{
    CacheBuildError, IntervalCache, SortedData, TagSet, Timestamp,
    value_aware_lapper::ValueAwareLapper,
};

/// Statistics about the dictionary encoding in the cache
#[derive(Debug, Clone)]
pub struct DictionaryStats {
    /// Number of unique TagSet entries in the dictionary
    pub unique_entries: usize,
    /// Memory used by the string dictionary (in bytes)
    pub dictionary_size_bytes: usize,
    /// Memory used by the decoded cache (in bytes)
    pub cache_size_bytes: usize,
}

/// Represents a dictionary-encoded TagSet as a list of (key_id, value_id) pairs
type EncodedTagSet = Vec<(usize, usize)>;

/// An interval cache implementation using ValueAwareLapper with dictionary encoding.
///
/// This cache stores TagSets with all strings dictionary-encoded for memory efficiency.
/// Each unique string (tag key or value) gets a dictionary ID, and TagSets are
/// stored as vectors of (key_id, value_id) pairs.
pub struct ValueAwareLapperCache {
    /// The ValueAwareLapper instance containing all intervals with encoded TagSet IDs
    value_lapper: ValueAwareLapper<u64, usize>,

    /// Dictionary for all unique strings (both keys and values)
    string_dict: StringDictionary<usize>,

    /// Maps from TagSet ID to its encoded representation
    tagsets: Vec<EncodedTagSet>,

    /// Time resolution for bucketing timestamps
    /// Duration::from_nanos(1) = nanosecond resolution (no bucketing)
    resolution: Duration,
}

impl ValueAwareLapperCache {
    /// Get statistics about the dictionary encoding
    pub fn dictionary_stats(&self) -> DictionaryStats {
        DictionaryStats {
            unique_entries: self.string_dict.values().len(),
            dictionary_size_bytes: self.string_dict.size(),
            cache_size_bytes: self.tagsets.len() * std::mem::size_of::<EncodedTagSet>(),
        }
    }

    /// Get the number of unique TagSets stored
    pub fn unique_tagsets(&self) -> usize {
        self.tagsets.len()
    }

    /// Query and return dictionary IDs for a point
    fn query_point_ids(&self, t: Timestamp) -> HashSet<usize> {
        let bucketed_t = Self::bucket_timestamp(t, self.resolution);
        let start = bucketed_t;
        let stop = bucketed_t + 1;

        self.value_lapper
            .find(start, stop)
            .map(|interval| interval.val)
            .collect()
    }

    /// Query and return dictionary IDs for a range
    fn query_range_ids(&self, range: Range<Timestamp>) -> HashSet<usize> {
        let bucketed_start = Self::bucket_timestamp(range.start, self.resolution);
        let bucketed_end = Self::bucket_timestamp(range.end, self.resolution);

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
            .map(|interval| interval.val)
            .collect()
    }

    /// Decode a tagset ID to a vector of (&str, &str) pairs
    fn decode_tagset(&self, id: usize) -> Vec<(&str, &str)> {
        self.tagsets
            .get(id)
            .map(|encoded| {
                encoded
                    .iter()
                    .filter_map(|(key_id, value_id)| {
                        let key = self.string_dict.lookup_id(*key_id)?;
                        let value = self.string_dict.lookup_id(*value_id)?;
                        Some((key, value))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Query for values at a point and return decoded tag pairs
    /// Returns references directly into the string dictionary
    fn query_point_decoded(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        self.query_point_ids(t)
            .into_iter()
            .map(|id| self.decode_tagset(id))
            .collect()
    }

    /// Query for values in a range and return decoded tag pairs
    fn query_range_decoded(&self, range: Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        self.query_range_ids(range)
            .into_iter()
            .map(|id| self.decode_tagset(id))
            .collect()
    }

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

    /// Build intervals from sorted timestamp-value pairs with optional bucketing.
    ///
    /// Consecutive timestamps with the same value are merged into continuous intervals.
    /// If resolution is provided, timestamps are bucketed before building intervals.
    fn build_intervals(
        points: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
        string_dict: &mut StringDictionary<usize>,
        tagsets: &mut Vec<EncodedTagSet>,
    ) -> Result<Vec<Interval<u64, usize>>, CacheBuildError> {
        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(intervals);
        }

        // Build intervals by merging consecutive identical values
        // Track open intervals for each encoded value to handle overlapping
        let mut open_intervals: std::collections::HashMap<usize, (u64, u64)> =
            std::collections::HashMap::new();

        for (t, v) in points {
            // Bucket the timestamp according to the resolution
            let bucketed_t = Self::bucket_timestamp(t, resolution);

            // Encode the TagSet
            let encoded: EncodedTagSet = v
                .iter()
                .map(|(k, val)| {
                    let key_id = string_dict.lookup_value_or_insert(k);
                    let value_id = string_dict.lookup_value_or_insert(val);
                    (key_id, value_id)
                })
                .collect();

            // Find or create tagset ID
            let encoded_id = tagsets
                .iter()
                .position(|ts| *ts == encoded)
                .unwrap_or_else(|| {
                    let id = tagsets.len();
                    tagsets.push(encoded);
                    id
                });

            let next_end = bucketed_t
                .checked_add(1)
                .ok_or(CacheBuildError::TimestampOverflow(bucketed_t))?;

            match open_intervals.get_mut(&encoded_id) {
                Some((_, end)) if *end == bucketed_t => {
                    // Extend existing interval
                    *end = next_end;
                }
                Some((start, end)) => {
                    // Gap detected - save old interval and start new one
                    intervals.push(Interval {
                        start: *start,
                        stop: *end,
                        val: encoded_id,
                    });

                    *start = bucketed_t;
                    *end = next_end;
                }
                None => {
                    // New value - start tracking it
                    open_intervals.insert(encoded_id, (bucketed_t, next_end));
                }
            }
        }

        // Flush all remaining open intervals
        for (encoded_id, (start, end)) in open_intervals {
            intervals.push(Interval {
                start,
                stop: end,
                val: encoded_id,
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
        sorted_data: SortedData<TagSet>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        // Initialize the string dictionary and tagset storage
        let mut string_dict = StringDictionary::new();
        let mut tagsets = Vec::new();

        // Build intervals with encoding
        let intervals = Self::build_intervals(points, resolution, &mut string_dict, &mut tagsets)?;

        let mut value_lapper = ValueAwareLapper::new(intervals);
        value_lapper.merge_with_values();

        Ok(Self {
            value_lapper,
            string_dict,
            tagsets,
            resolution,
        })
    }
}

impl IntervalCache<TagSet> for ValueAwareLapperCache {
    fn from_sorted(sorted_data: SortedData<TagSet>) -> Result<Self, CacheBuildError> {
        // Default to nanosecond resolution for backward compatibility
        Self::from_sorted_with_resolution(sorted_data, Duration::from_nanos(1))
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        self.query_point_decoded(t)
    }

    fn query_range(&self, range: Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
        self.query_range_decoded(range)
    }

    fn append_sorted(&mut self, sorted_data: SortedData<TagSet>) -> Result<(), CacheBuildError> {
        // Build new intervals from sorted points using the cache's resolution with encoding
        let new_intervals = Self::build_intervals(
            sorted_data.into_inner(),
            self.resolution,
            &mut self.string_dict,
            &mut self.tagsets,
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

        // Size of all intervals in the ValueAwareLapper (now storing usize IDs)
        size += self.value_lapper.len() * std::mem::size_of::<Interval<u64, usize>>();

        // Size of the string dictionary
        size += self.string_dict.size();

        // Size of the tagsets array
        size += self.tagsets.capacity() * std::mem::size_of::<EncodedTagSet>();
        for tagset in &self.tagsets {
            size += tagset.capacity() * std::mem::size_of::<(usize, usize)>();
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
    use std::time::Duration;

    fn make_tagset(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_value_aware_lapper_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (4, tag_b.clone())];

        let cache = ValueAwareLapperCache::new(data).unwrap();

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
    fn test_value_aware_lapper_cache_empty() {
        let cache: ValueAwareLapperCache = ValueAwareLapperCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(0..100).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_value_aware_lapper_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (3, tag_a.clone())];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        // Should have merged into 1 interval: [1,4)
        assert_eq!(cache.interval_count(), 1);
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert_eq!(cache.query_point(4).len(), 0);
    }

    #[test]
    fn test_dictionary_encoding() {
        let tag_a = make_tagset(&[("host", "server1"), ("env", "prod")]);
        let tag_b = make_tagset(&[("host", "server1"), ("env", "dev")]);
        let tag_c = make_tagset(&[("host", "server2"), ("env", "prod")]);

        let data = vec![(1, tag_a.clone()), (2, tag_b.clone()), (3, tag_c.clone())];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        // Check dictionary stats
        let stats = cache.dictionary_stats();

        // Should have 6 unique dictionary entries: "host", "env", "server1", "server2", "prod", "dev"
        assert_eq!(stats.unique_entries, 6);

        // Should have 3 unique tagsets
        assert_eq!(cache.unique_tagsets(), 3);
    }

    #[test]
    fn test_custom_resolution() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (3, tag_a.clone())];

        // Use 1-second resolution
        let sorted_data = SortedData::from_unsorted(data.clone());
        let cache =
            ValueAwareLapperCache::from_sorted_with_resolution(sorted_data, Duration::from_secs(1))
                .unwrap();

        // Should still work correctly
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(2).len() > 0);
        assert!(cache.query_point(3).len() > 0);
    }

    #[test]
    fn test_query_range() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);
        let tag_c = make_tagset(&[("host", "server3")]);

        let data = vec![
            (1, tag_a.clone()),
            (2, tag_a.clone()),
            (5, tag_b.clone()),
            (6, tag_b.clone()),
            (10, tag_c.clone()),
        ];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        // Query range [1, 6) should get tag_a and tag_b
        let results = cache.query_range(1..6);
        assert_eq!(results.len(), 2);

        // Check both tagsets are present
        let has_server1 = results
            .iter()
            .any(|tags| tags.contains(&("host", "server1")));
        let has_server2 = results
            .iter()
            .any(|tags| tags.contains(&("host", "server2")));
        assert!(has_server1);
        assert!(has_server2);

        // Query range [0, 1) should be empty (before any data)
        assert_eq!(cache.query_range(0..1).len(), 0);

        // Query range [3, 5) should be empty (gap in data)
        assert_eq!(cache.query_range(3..5).len(), 0);
    }

    #[test]
    fn test_append_sorted() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let initial_data = vec![(1, tag_a.clone()), (2, tag_a.clone())];

        let mut cache = ValueAwareLapperCache::new(initial_data).unwrap();

        let append_data = vec![
            (3, tag_a.clone()), // Should merge with existing
            (5, tag_b.clone()),
        ];

        cache.append_batch(append_data).unwrap();

        // Should have 2 intervals after append and merge
        // [1, 4) with tag_a and [5, 6) with tag_b
        assert_eq!(cache.interval_count(), 2);

        // Verify data is accessible
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert!(cache.query_point(5).len() > 0);
    }

    #[test]
    fn test_multiple_tags() {
        let tag_a = make_tagset(&[("host", "server1"), ("region", "us-west"), ("env", "prod")]);

        let data = vec![(100, tag_a.clone())];
        let cache = ValueAwareLapperCache::new(data).unwrap();

        let result = cache.query_point(100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 3);

        // Verify all tags are present
        assert!(result[0].contains(&("host", "server1")));
        assert!(result[0].contains(&("region", "us-west")));
        assert!(result[0].contains(&("env", "prod")));
    }

    #[test]
    fn test_dictionary_string_reuse() {
        // Test that the same strings get reused in the dictionary
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);
        let tag_c = make_tagset(&[("region", "us-west")]);

        let data = vec![(1, tag_a.clone()), (2, tag_b.clone()), (3, tag_c.clone())];

        let cache = ValueAwareLapperCache::new(data).unwrap();
        let stats = cache.dictionary_stats();

        // Should have 5 unique dictionary entries: "host", "region", "server1", "server2", "us-west"
        assert_eq!(stats.unique_entries, 5);

        // 3 unique tagsets
        assert_eq!(cache.unique_tagsets(), 3);
    }

    #[test]
    fn test_overlapping_intervals() {
        // Test case with same timestamp, different values (overlapping)
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![
            (1, tag_a.clone()),
            (1, tag_b.clone()), // Same timestamp, different value
            (2, tag_a.clone()),
            (2, tag_b.clone()),
        ];

        let cache = ValueAwareLapperCache::new(data).unwrap();

        // Query at timestamp 1 should return both values
        let result = cache.query_point(1);
        assert_eq!(result.len(), 2);

        // Check both values are present
        let has_server1 = result
            .iter()
            .any(|tags| tags.contains(&("host", "server1")));
        let has_server2 = result
            .iter()
            .any(|tags| tags.contains(&("host", "server2")));
        assert!(has_server1);
        assert!(has_server2);
    }
}
