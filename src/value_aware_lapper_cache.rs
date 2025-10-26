//! `IntervalCache` implementation using ValueAwareLapper for value-aware merging.
//!
//! This implementation provides the same interface as LapperCache but uses
//! ValueAwareLapper internally, which only merges intervals when both boundaries
//! AND values match.

use std::collections::HashSet;
use std::ops::Range;
use std::time::Duration;

use arrow_util::dictionary::StringDictionary;
use rayon::prelude::*;
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

        // HashMap for O(1) tagset ID lookups
        // Pre-populate with existing tagsets to avoid creating duplicates
        let mut tagset_map: std::collections::HashMap<EncodedTagSet, usize> = tagsets
            .iter()
            .enumerate()
            .map(|(id, tagset)| (tagset.clone(), id))
            .collect();

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

            // Find or create tagset ID using HashMap for O(1) lookup
            let encoded_id = *tagset_map.entry(encoded.clone()).or_insert_with(|| {
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

    /// Helper to encode a chunk of data with a local dictionary.
    /// Returns (encoded_points, local_string_dict, local_tagsets, local_tagset_map)
    fn encode_chunk(
        chunk: Vec<(Timestamp, TagSet)>,
    ) -> (
        Vec<(Timestamp, usize)>,
        StringDictionary<usize>,
        Vec<EncodedTagSet>,
        std::collections::HashMap<EncodedTagSet, usize>,
    ) {
        let mut string_dict = StringDictionary::new();
        let mut tagsets = Vec::new();
        let mut tagset_map: std::collections::HashMap<EncodedTagSet, usize> =
            std::collections::HashMap::new();

        let encoded_points: Vec<(Timestamp, usize)> = chunk
            .into_iter()
            .map(|(ts, tagset)| {
                // Encode the TagSet
                let encoded: EncodedTagSet = tagset
                    .iter()
                    .map(|(k, val)| {
                        let key_id = string_dict.lookup_value_or_insert(k);
                        let value_id = string_dict.lookup_value_or_insert(val);
                        (key_id, value_id)
                    })
                    .collect();

                // Find or create tagset ID using HashMap for O(1) lookup
                let encoded_id = *tagset_map.entry(encoded.clone()).or_insert_with(|| {
                    let id = tagsets.len();
                    tagsets.push(encoded);
                    id
                });

                (ts, encoded_id)
            })
            .collect();

        (encoded_points, string_dict, tagsets, tagset_map)
    }

    /// Merge multiple local dictionaries into a global dictionary.
    /// Returns (global_dict, global_tagsets, id_remapping_per_chunk)
    fn merge_dictionaries(
        local_dicts: Vec<(
            StringDictionary<usize>,
            Vec<EncodedTagSet>,
            std::collections::HashMap<EncodedTagSet, usize>,
        )>,
    ) -> (
        StringDictionary<usize>,
        Vec<EncodedTagSet>,
        Vec<Vec<usize>>, // Remapping table: chunk_idx -> [local_id -> global_id]
    ) {
        let mut global_dict = StringDictionary::new();
        let mut global_tagsets = Vec::new();
        let mut global_tagset_map: std::collections::HashMap<EncodedTagSet, usize> =
            std::collections::HashMap::new();
        let mut id_remappings = Vec::new();

        for (local_dict, local_tagsets, _local_tagset_map) in local_dicts {
            // Build string ID remapping for this chunk
            let string_id_remap: Vec<usize> = (0..local_dict.values().len())
                .map(|local_id| {
                    let string = local_dict.lookup_id(local_id).unwrap();
                    global_dict.lookup_value_or_insert(string)
                })
                .collect();

            // Build tagset ID remapping for this chunk
            let mut tagset_id_remap = Vec::new();
            for local_tagset in local_tagsets {
                // Remap the encoded tagset using the string ID remapping
                let remapped_tagset: EncodedTagSet = local_tagset
                    .iter()
                    .map(|(key_id, val_id)| (string_id_remap[*key_id], string_id_remap[*val_id]))
                    .collect();

                // Find or create global tagset ID
                let global_id = *global_tagset_map
                    .entry(remapped_tagset.clone())
                    .or_insert_with(|| {
                        let id = global_tagsets.len();
                        global_tagsets.push(remapped_tagset);
                        id
                    });

                tagset_id_remap.push(global_id);
            }

            id_remappings.push(tagset_id_remap);
        }

        (global_dict, global_tagsets, id_remappings)
    }

    /// Create a cache from unsorted data with a specific time resolution.
    ///
    /// This method is optimized for performance: it dictionary-encodes TagSets BEFORE sorting,
    /// allowing the sort to compare cheap usize IDs instead of expensive TagSet structures.
    /// This can be 10-100x faster than sorting TagSets directly, especially for complex TagSets.
    ///
    /// # Arguments
    /// * `points` - Unsorted data points (timestamp, TagSet pairs)
    /// * `resolution` - Time bucket size (e.g., `Duration::from_secs(5)` for 5-second buckets)
    ///
    /// # Performance
    /// For data with many unique TagSets or complex TagSets (many key-value pairs, long strings),
    /// this method is significantly faster than sorting first and then encoding.
    pub fn from_unsorted_with_resolution(
        points: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        if points.is_empty() {
            return Ok(Self {
                value_lapper: ValueAwareLapper::new(vec![]),
                string_dict: StringDictionary::new(),
                tagsets: Vec::new(),
                resolution,
            });
        }

        // 1. Dictionary-encode all TagSets FIRST using parallel chunks
        // Determine chunk size based on data size and available parallelism
        let num_threads = rayon::current_num_threads();
        let chunk_size = (points.len() / num_threads).max(1000); // At least 1000 items per chunk

        // Process chunks in parallel, each building local dictionaries
        let local_results: Vec<_> = points
            .par_chunks(chunk_size)
            .map(|chunk| {
                let chunk_vec = chunk.to_vec();
                Self::encode_chunk(chunk_vec)
            })
            .collect();

        // Separate the results
        let local_dicts: Vec<_> = local_results
            .into_iter()
            .map(|(encoded, dict, tagsets, map)| (dict, tagsets, map, encoded))
            .collect();

        let encoded_chunks: Vec<_> = local_dicts
            .iter()
            .map(|(_, _, _, enc)| enc.clone())
            .collect();
        let dict_data: Vec<_> = local_dicts
            .into_iter()
            .map(|(dict, tagsets, map, _)| (dict, tagsets, map))
            .collect();

        // Merge dictionaries into global dictionary
        let (string_dict, tagsets, id_remappings) = Self::merge_dictionaries(dict_data);

        // Remap local IDs to global IDs in parallel
        let mut encoded_points: Vec<(Timestamp, usize)> = encoded_chunks
            .par_iter()
            .zip(id_remappings.par_iter())
            .flat_map(|(chunk_encoded, id_remap)| {
                chunk_encoded
                    .iter()
                    .map(|(ts, local_id)| (*ts, id_remap[*local_id]))
                    .collect::<Vec<_>>()
            })
            .collect();

        // 2. Sort by (timestamp, tagset_id) - FAST integer comparisons!
        // Use parallel sort for better performance on large datasets
        encoded_points.par_sort_unstable_by_key(|(time, id)| (*time, *id));

        // 3. Build intervals from pre-sorted encoded data
        let intervals = Self::build_intervals_from_encoded(encoded_points, resolution)?;

        // 4. Create ValueAwareLapper and merge
        let mut value_lapper = ValueAwareLapper::new(intervals);
        value_lapper.merge_with_values();

        Ok(Self {
            value_lapper,
            string_dict,
            tagsets,
            resolution,
        })
    }

    /// Build intervals from pre-encoded and pre-sorted data.
    ///
    /// This is an optimized path for when data has already been dictionary-encoded
    /// and sorted by (timestamp, tagset_id).
    fn build_intervals_from_encoded(
        points: Vec<(Timestamp, usize)>,
        resolution: Duration,
    ) -> Result<Vec<Interval<u64, usize>>, CacheBuildError> {
        let mut intervals = Vec::new();

        if points.is_empty() {
            return Ok(intervals);
        }

        // Track open intervals for each encoded value to handle overlapping
        let mut open_intervals: std::collections::HashMap<usize, (u64, u64)> =
            std::collections::HashMap::new();

        for (t, encoded_id) in points {
            // Bucket the timestamp according to the resolution
            let bucketed_t = Self::bucket_timestamp(t, resolution);

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

    /// Append unsorted data to the cache.
    ///
    /// This method is optimized for performance: it dictionary-encodes TagSets BEFORE sorting,
    /// avoiding expensive TagSet comparisons during the sort operation.
    ///
    /// # Arguments
    /// * `points` - Unsorted data points to append (timestamp, TagSet pairs)
    ///
    /// # Performance
    /// Significantly faster than sorting first then calling `append_sorted`, especially for
    /// complex TagSets with many key-value pairs or long strings.
    pub fn append_unsorted(
        &mut self,
        points: Vec<(Timestamp, TagSet)>,
    ) -> Result<(), CacheBuildError> {
        if points.is_empty() {
            return Ok(());
        }

        // Determine chunk size based on data size and available parallelism
        let num_threads = rayon::current_num_threads();
        let chunk_size = (points.len() / num_threads).max(1000); // At least 1000 items per chunk

        // Process chunks in parallel, each building local dictionaries
        let local_results: Vec<_> = points
            .par_chunks(chunk_size)
            .map(|chunk| {
                let chunk_vec = chunk.to_vec();
                Self::encode_chunk(chunk_vec)
            })
            .collect();

        // Separate the results
        let local_dicts: Vec<_> = local_results
            .into_iter()
            .map(|(encoded, dict, tagsets, map)| (dict, tagsets, map, encoded))
            .collect();

        let encoded_chunks: Vec<_> = local_dicts
            .iter()
            .map(|(_, _, _, enc)| enc.clone())
            .collect();
        let dict_data: Vec<_> = local_dicts
            .into_iter()
            .map(|(dict, tagsets, map, _)| (dict, tagsets, map))
            .collect();

        // Merge new dictionaries with existing dictionary
        // First merge the local dictionaries
        let (new_string_dict, new_tagsets, id_remappings) = Self::merge_dictionaries(dict_data);

        // Now merge with existing global dictionary
        let string_id_remap: Vec<usize> = (0..new_string_dict.values().len())
            .map(|local_id| {
                let string = new_string_dict.lookup_id(local_id).unwrap();
                self.string_dict.lookup_value_or_insert(string)
            })
            .collect();

        // Build final tagset ID remapping
        let mut tagset_map: std::collections::HashMap<EncodedTagSet, usize> = self
            .tagsets
            .iter()
            .enumerate()
            .map(|(id, tagset)| (tagset.clone(), id))
            .collect();

        let final_id_remappings: Vec<Vec<usize>> = id_remappings
            .into_iter()
            .map(|chunk_remap| {
                chunk_remap
                    .into_iter()
                    .map(|new_tagset_id| {
                        let new_tagset = &new_tagsets[new_tagset_id];
                        let remapped_tagset: EncodedTagSet = new_tagset
                            .iter()
                            .map(|(key_id, val_id)| {
                                (string_id_remap[*key_id], string_id_remap[*val_id])
                            })
                            .collect();

                        *tagset_map
                            .entry(remapped_tagset.clone())
                            .or_insert_with(|| {
                                let id = self.tagsets.len();
                                self.tagsets.push(remapped_tagset);
                                id
                            })
                    })
                    .collect()
            })
            .collect();

        // Remap local IDs to global IDs in parallel
        let mut encoded_points: Vec<(Timestamp, usize)> = encoded_chunks
            .par_iter()
            .zip(final_id_remappings.par_iter())
            .flat_map(|(chunk_encoded, id_remap)| {
                chunk_encoded
                    .iter()
                    .map(|(ts, local_id)| (*ts, id_remap[*local_id]))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Sort by (timestamp, tagset_id) - FAST integer comparisons!
        // Use parallel sort for better performance on large datasets
        encoded_points.par_sort_unstable_by_key(|(time, id)| (*time, *id));

        // Build intervals from pre-sorted encoded data
        let new_intervals = Self::build_intervals_from_encoded(encoded_points, self.resolution)?;

        // Collect all existing intervals
        let mut all_intervals: Vec<_> = self.value_lapper.iter().cloned().collect();
        all_intervals.extend(new_intervals);

        // Rebuild with all intervals and merge
        self.value_lapper = ValueAwareLapper::new(all_intervals);
        self.value_lapper.merge_with_values();

        Ok(())
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

    #[test]
    fn test_append_unsorted() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let initial_data = vec![(1, tag_a.clone()), (2, tag_a.clone())];

        let mut cache = ValueAwareLapperCache::from_unsorted_with_resolution(
            initial_data,
            Duration::from_nanos(1),
        )
        .unwrap();

        let append_data = vec![
            (3, tag_a.clone()), // Should merge with existing
            (5, tag_b.clone()),
        ];

        cache.append_unsorted(append_data).unwrap();

        // Should have 2 intervals after append and merge
        // [1, 4) with tag_a and [5, 6) with tag_b
        assert_eq!(cache.interval_count(), 2);

        // Verify data is accessible
        assert!(cache.query_point(1).len() > 0);
        assert!(cache.query_point(3).len() > 0);
        assert!(cache.query_point(5).len() > 0);
    }

    #[test]
    fn test_append_unsorted_equals_append_sorted() {
        // Verify that append_unsorted produces identical results to append_sorted
        let tag_a = make_tagset(&[("host", "server1"), ("env", "prod")]);
        let tag_b = make_tagset(&[("host", "server2"), ("env", "dev")]);
        let tag_c = make_tagset(&[("host", "server3"), ("env", "prod")]);

        let initial_data = vec![(1, tag_a.clone()), (2, tag_a.clone())];

        let append_data = vec![
            (5, tag_b.clone()),
            (3, tag_a.clone()), // Unsorted - tests sorting
            (10, tag_c.clone()),
            (6, tag_b.clone()),
        ];

        let resolution = Duration::from_secs(1);

        // Build cache using append_sorted path
        let mut cache_sorted =
            ValueAwareLapperCache::from_unsorted_with_resolution(initial_data.clone(), resolution)
                .unwrap();
        let sorted_append = SortedData::from_unsorted(append_data.clone());
        cache_sorted.append_sorted(sorted_append).unwrap();

        // Build cache using append_unsorted path
        let mut cache_unsorted =
            ValueAwareLapperCache::from_unsorted_with_resolution(initial_data, resolution).unwrap();
        cache_unsorted.append_unsorted(append_data).unwrap();

        // Both should have same number of intervals
        assert_eq!(
            cache_sorted.interval_count(),
            cache_unsorted.interval_count()
        );

        // Both should return same results for point queries
        for t in 0..15 {
            let result_sorted = cache_sorted.query_point(t);
            let result_unsorted = cache_unsorted.query_point(t);
            assert_eq!(
                result_sorted.len(),
                result_unsorted.len(),
                "Mismatch at timestamp {}",
                t
            );

            // Sort results for comparison (order doesn't matter)
            let mut sorted_vec = result_sorted.clone();
            sorted_vec.sort();
            let mut unsorted_vec = result_unsorted.clone();
            unsorted_vec.sort();
            assert_eq!(
                sorted_vec, unsorted_vec,
                "Different results at timestamp {}",
                t
            );
        }

        // Both should return same results for range queries
        let range = 0..15;
        let mut result_sorted = cache_sorted.query_range(range.clone());
        result_sorted.sort();
        let mut result_unsorted = cache_unsorted.query_range(range);
        result_unsorted.sort();
        assert_eq!(result_sorted, result_unsorted);
    }

    #[test]
    fn test_from_unsorted_equals_from_sorted() {
        // Verify that from_unsorted_with_resolution produces identical results
        // to from_sorted_with_resolution
        let tag_a = make_tagset(&[("host", "server1"), ("env", "prod")]);
        let tag_b = make_tagset(&[("host", "server2"), ("env", "dev")]);
        let tag_c = make_tagset(&[("host", "server3"), ("env", "prod")]);

        let data = vec![
            (5, tag_b.clone()),
            (1, tag_a.clone()),
            (10, tag_c.clone()),
            (2, tag_a.clone()),
            (6, tag_b.clone()),
            (3, tag_a.clone()),
        ];

        let resolution = Duration::from_secs(1);

        // Build using old path
        let sorted_data = SortedData::from_unsorted(data.clone());
        let cache_sorted =
            ValueAwareLapperCache::from_sorted_with_resolution(sorted_data, resolution).unwrap();

        // Build using new optimized path
        let cache_unsorted =
            ValueAwareLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Both should have same number of intervals
        assert_eq!(
            cache_sorted.interval_count(),
            cache_unsorted.interval_count()
        );

        // Both should return same results for point queries
        for t in 0..15 {
            let result_sorted = cache_sorted.query_point(t);
            let result_unsorted = cache_unsorted.query_point(t);
            assert_eq!(
                result_sorted.len(),
                result_unsorted.len(),
                "Mismatch at timestamp {}",
                t
            );

            // Sort results for comparison (order doesn't matter)
            let mut sorted_vec = result_sorted.clone();
            sorted_vec.sort();
            let mut unsorted_vec = result_unsorted.clone();
            unsorted_vec.sort();
            assert_eq!(
                sorted_vec, unsorted_vec,
                "Different results at timestamp {}",
                t
            );
        }

        // Both should return same results for range queries
        let range = 0..15;
        let mut result_sorted = cache_sorted.query_range(range.clone());
        result_sorted.sort();
        let mut result_unsorted = cache_unsorted.query_range(range);
        result_unsorted.sort();
        assert_eq!(result_sorted, result_unsorted);
    }
}
