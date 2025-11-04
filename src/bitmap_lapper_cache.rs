//! `IntervalCache` implementation using RoaringBitmap with rust-lapper.
//!
//! This implementation eliminates the interval explosion problem by storing
//! one interval per time bucket (not one per tagset per bucket). Each interval
//! contains a RoaringBitmap of all tagset IDs present in that bucket.
//!
//! ## Key Benefits
//! - **200-1,000x fewer intervals**: O(time_buckets) instead of O(time_buckets × cardinality)
//! - **100-250x faster queries**: No HashSet deduplication of millions of intervals
//! - **Simpler architecture**: Uses rust_lapper::Lapper directly, no custom wrapper needed
//! - **Linear scaling**: Scales with time range, not cardinality

use std::ops::Range;
use std::time::Duration;

use ahash::AHashMap;
use arrow_util::dictionary::StringDictionary;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use rust_lapper::{Interval, Lapper};

use crate::{
    CacheBuildError, EncodedTagSet, FastIndexSet, IntervalCache, SortedData, TagSet, Timestamp,
    encode_tagset,
};

/// Wrapper around RoaringBitmap that implements Eq.
///
/// This is safe because RoaringBitmap's PartialEq is deterministic and reflexive.
#[derive(Clone, Debug)]
pub(crate) struct EqRoaringBitmap(RoaringBitmap);

impl PartialEq for EqRoaringBitmap {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for EqRoaringBitmap {}

impl std::ops::Deref for EqRoaringBitmap {
    type Target = RoaringBitmap;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for EqRoaringBitmap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<RoaringBitmap> for EqRoaringBitmap {
    fn from(bitmap: RoaringBitmap) -> Self {
        EqRoaringBitmap(bitmap)
    }
}

impl From<EqRoaringBitmap> for RoaringBitmap {
    fn from(wrapper: EqRoaringBitmap) -> Self {
        wrapper.0
    }
}

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

/// An interval cache implementation using RoaringBitmap with dictionary encoding.
///
/// Unlike ValueAwareLapperCache which creates one interval per tagset per time bucket,
/// BitmapLapperCache creates ONE interval per time bucket containing a RoaringBitmap
/// of all tagset IDs in that bucket.
///
/// ## Example
/// With 200 tagsets at 1-minute resolution for 1 hour:
/// - ValueAwareLapperCache: 60 × 200 = 12,000 intervals
/// - BitmapLapperCache: 60 intervals (200x reduction!)
pub struct BitmapLapperCache {
    /// Direct use of rust_lapper (no ValueAwareLapper wrapper needed!)
    lapper: Lapper<u64, EqRoaringBitmap>,

    /// Dictionary for all unique strings (both keys and values)
    string_dict: StringDictionary<usize>,

    /// Vector of encoded TagSets (index = tagset ID)
    tagsets: FastIndexSet<EncodedTagSet>,

    /// Time resolution for bucketing timestamps
    resolution: Duration,
}

impl BitmapLapperCache {
    pub fn merge_overlaps(&mut self) {
        self.lapper.merge_overlaps();
    }

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

    /// Returns the minimum timestamp in the cache (start of first interval)
    pub fn min_timestamp(&self) -> Option<Timestamp> {
        self.lapper.intervals.first().map(|interval| interval.start)
    }

    /// Returns the maximum timestamp in the cache (end of last interval bucket)
    pub fn max_timestamp(&self) -> Option<Timestamp> {
        self.lapper.intervals.last().map(|interval| {
            let resolution_ns = self.resolution.as_nanos() as u64;
            interval.start + resolution_ns
        })
    }

    /// Construct a cache from pre-built components.
    ///
    /// This is an internal constructor used by streaming builders.
    pub(crate) fn from_parts(
        lapper: Lapper<u64, EqRoaringBitmap>,
        string_dict: StringDictionary<usize>,
        tagsets: FastIndexSet<EncodedTagSet>,
        resolution: Duration,
    ) -> Self {
        Self {
            lapper,
            string_dict,
            tagsets,
            resolution,
        }
    }

    /// Decode a tagset ID to a vector of (&str, &str) pairs
    fn decode_tagset(&self, id: u32) -> Vec<(&str, &str)> {
        self.tagsets
            .get_index(id as usize)
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

    /// Bucket a timestamp according to the specified resolution.
    #[inline]
    pub(crate) fn bucket_timestamp(ts: Timestamp, resolution: Duration) -> Timestamp {
        let resolution_ns = resolution.as_nanos() as u64;
        if resolution_ns <= 1 {
            ts
        } else {
            (ts / resolution_ns) * resolution_ns
        }
    }

    /// Build intervals from sorted timestamp-value pairs with bucketing.
    ///
    /// This is the key optimization: group all tagsets by time bucket,
    /// creating ONE interval per bucket containing a RoaringBitmap of all tagset IDs.
    fn build_intervals(
        points: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
        string_dict: &mut StringDictionary<usize>,
        tagsets: &mut FastIndexSet<EncodedTagSet>,
    ) -> Result<Vec<Interval<u64, EqRoaringBitmap>>, CacheBuildError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        // Use AHashMap to group tagsets by bucket (faster than BTreeMap)
        let mut bucket_map: AHashMap<u64, RoaringBitmap> = AHashMap::new();

        for (ts, tagset) in points {
            let bucket = Self::bucket_timestamp(ts, resolution);

            // Encode the TagSet
            let encoded = encode_tagset(&tagset, string_dict);

            let (tagset_id, _) = tagsets.insert_full(encoded);

            // Add tagset ID to the bucket's bitmap
            bucket_map
                .entry(bucket)
                .or_default()
                .insert(tagset_id as u32);
        }

        // Convert to intervals - ONE per bucket!
        let intervals: Vec<Interval<u64, EqRoaringBitmap>> = bucket_map
            .into_iter()
            .map(|(start, bitmap)| {
                let stop = start.checked_add(1).expect("timestamp overflow");
                Interval {
                    start,
                    stop,
                    val: bitmap.into(),
                }
            })
            .collect();

        Ok(intervals)
    }

    /// Create a cache from sorted data with a specific time resolution.
    pub fn from_sorted_with_resolution(
        sorted_data: SortedData<TagSet>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        let points = sorted_data.into_inner();

        let mut string_dict = StringDictionary::new();
        let mut tagsets = FastIndexSet::default();

        let intervals = Self::build_intervals(points, resolution, &mut string_dict, &mut tagsets)?;

        // Use Lapper directly - no custom wrapper needed!
        let lapper = Lapper::new(intervals);

        Ok(Self {
            lapper,
            string_dict,
            tagsets,
            resolution,
        })
    }

    /// Create a cache from pre-bucketed and deduped data.
    ///
    /// This function assumes the input data is already:
    /// 1. Pre-bucketed: timestamps are aligned to bucket boundaries
    /// 2. Deduped: no duplicate tagsets within the same bucket
    /// 3. Sorted: by timestamp
    ///
    /// It skips the bucketing process but still builds the dictionary and other
    /// internal data structures.
    pub fn from_prebucketed(
        prebucketed_data: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        if prebucketed_data.is_empty() {
            return Ok(Self {
                lapper: Lapper::new(vec![]),
                string_dict: StringDictionary::new(),
                tagsets: FastIndexSet::default(),
                resolution,
            });
        }

        let mut string_dict = StringDictionary::new();
        let mut tagsets = FastIndexSet::default();

        // Build intervals directly from pre-bucketed data
        let intervals = Self::build_intervals_from_prebucketed(
            prebucketed_data,
            &mut string_dict,
            &mut tagsets,
        )?;

        let lapper = Lapper::new(intervals);

        Ok(Self {
            lapper,
            string_dict,
            tagsets,
            resolution,
        })
    }

    /// Build intervals from pre-bucketed and deduped data.
    ///
    /// Unlike build_intervals, this assumes timestamps are already bucket-aligned
    /// and doesn't perform bucketing or deduplication.
    fn build_intervals_from_prebucketed(
        points: Vec<(Timestamp, TagSet)>,
        string_dict: &mut StringDictionary<usize>,
        tagsets: &mut FastIndexSet<EncodedTagSet>,
    ) -> Result<Vec<Interval<u64, EqRoaringBitmap>>, CacheBuildError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        // Use AHashMap to group tagsets by their pre-bucketed timestamp
        let mut bucket_map: AHashMap<u64, RoaringBitmap> = AHashMap::new();

        for (bucket_ts, tagset) in points {
            // Encode the TagSet
            let encoded = encode_tagset(&tagset, string_dict);
            let (tagset_id, _) = tagsets.insert_full(encoded);

            // Add tagset ID to the bucket's bitmap
            // Note: bucket_ts is already bucket-aligned, no need to call bucket_timestamp
            bucket_map
                .entry(bucket_ts)
                .or_default()
                .insert(tagset_id as u32);
        }

        // Convert to intervals - ONE per bucket!
        let intervals: Vec<Interval<u64, EqRoaringBitmap>> = bucket_map
            .into_iter()
            .map(|(start, bitmap)| {
                let stop = start.checked_add(1).expect("timestamp overflow");
                Interval {
                    start,
                    stop,
                    val: bitmap.into(),
                }
            })
            .collect();

        Ok(intervals)
    }

    /// Create a cache from unsorted data with a specific time resolution.
    ///
    /// Optimized version that dictionary-encodes before sorting.
    pub fn from_unsorted_with_resolution(
        points: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        if points.is_empty() {
            return Ok(Self {
                lapper: Lapper::new(vec![]),
                string_dict: StringDictionary::new(),
                tagsets: FastIndexSet::default(),
                resolution,
            });
        }

        // Dictionary-encode in parallel chunks
        let num_threads = rayon::current_num_threads();
        let chunk_size = (points.len() / num_threads).max(1000);

        let local_results: Vec<_> = points
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut string_dict = StringDictionary::new();
                let mut tagsets = FastIndexSet::default();

                let encoded_points: Vec<(Timestamp, usize)> = chunk
                    .iter()
                    .map(|(ts, tagset)| {
                        let encoded = encode_tagset(tagset, &mut string_dict);
                        let (tagset_id, _) = tagsets.insert_full(encoded);
                        (*ts, tagset_id)
                    })
                    .collect();

                (encoded_points, string_dict, tagsets)
            })
            .collect();

        // Merge dictionaries
        let mut global_dict = StringDictionary::new();
        let mut global_tagsets = FastIndexSet::default();
        let mut id_remappings = Vec::new();

        for (_, local_dict, local_tagsets) in &local_results {
            // Build string ID remapping
            let string_id_remap: Vec<usize> = (0..local_dict.values().len())
                .map(|local_id| {
                    let string = local_dict.lookup_id(local_id).unwrap();
                    global_dict.lookup_value_or_insert(string)
                })
                .collect();

            // Build tagset ID remapping
            let mut tagset_id_remap = Vec::new();
            for local_tagset in local_tagsets {
                let remapped_tagset: EncodedTagSet = local_tagset
                    .iter()
                    .map(|(key_id, val_id)| (string_id_remap[*key_id], string_id_remap[*val_id]))
                    .collect();

                // Find or create global tagset ID
                let (global_id, _) = global_tagsets.insert_full(remapped_tagset);
                tagset_id_remap.push(global_id);
            }

            id_remappings.push(tagset_id_remap);
        }

        // Remap local IDs to global IDs
        let mut encoded_points: Vec<(Timestamp, usize)> = local_results
            .par_iter()
            .zip(id_remappings.par_iter())
            .flat_map(|((chunk_encoded, _, _), id_remap)| {
                chunk_encoded
                    .iter()
                    .map(|(ts, local_id)| (*ts, id_remap[*local_id]))
                    .collect::<Vec<_>>()
            })
            .collect();

        // Sort by timestamp
        encoded_points.par_sort_unstable_by_key(|(ts, _)| *ts);

        // Build intervals from encoded data
        let intervals = Self::build_intervals_from_encoded(encoded_points, resolution)?;

        let lapper = Lapper::new(intervals);

        Ok(Self {
            lapper,
            string_dict: global_dict,
            tagsets: global_tagsets,
            resolution,
        })
    }

    /// Build intervals from pre-encoded and sorted data.
    fn build_intervals_from_encoded(
        points: Vec<(Timestamp, usize)>,
        resolution: Duration,
    ) -> Result<Vec<Interval<u64, EqRoaringBitmap>>, CacheBuildError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }

        // Group by bucket
        let mut bucket_map: AHashMap<u64, RoaringBitmap> = AHashMap::new();

        for (ts, tagset_id) in points {
            let bucket = Self::bucket_timestamp(ts, resolution);
            bucket_map
                .entry(bucket)
                .or_default()
                .insert(tagset_id as u32);
        }

        // Convert to intervals
        let intervals: Vec<Interval<u64, EqRoaringBitmap>> = bucket_map
            .into_iter()
            .map(|(start, bitmap)| {
                let stop = start.checked_add(1).expect("timestamp overflow");
                Interval {
                    start,
                    stop,
                    val: bitmap.into(),
                }
            })
            .collect();

        Ok(intervals)
    }

    /// Append unsorted data to the cache.
    pub fn append_unsorted(
        &mut self,
        points: Vec<(Timestamp, TagSet)>,
    ) -> Result<(), CacheBuildError> {
        if points.is_empty() {
            return Ok(());
        }

        // Encode new points
        let num_threads = rayon::current_num_threads();
        let chunk_size = (points.len() / num_threads).max(1000);

        let local_results: Vec<_> = points
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut string_dict = StringDictionary::new();
                let mut tagsets = FastIndexSet::default();

                let encoded_points: Vec<(Timestamp, usize)> = chunk
                    .iter()
                    .map(|(ts, tagset)| {
                        let encoded = encode_tagset(tagset, &mut string_dict);

                        let (tagset_id, _) = tagsets.insert_full(encoded);
                        (*ts, tagset_id)
                    })
                    .collect();

                (encoded_points, string_dict, tagsets)
            })
            .collect();

        // Merge with existing dictionaries
        let mut id_remappings = Vec::new();

        for (_, local_dict, local_tagsets) in &local_results {
            let string_id_remap: Vec<usize> = (0..local_dict.values().len())
                .map(|local_id| {
                    let string = local_dict.lookup_id(local_id).unwrap();
                    self.string_dict.lookup_value_or_insert(string)
                })
                .collect();

            let mut tagset_id_remap = Vec::new();
            for local_tagset in local_tagsets {
                let remapped_tagset: EncodedTagSet = local_tagset
                    .iter()
                    .map(|(key_id, val_id)| (string_id_remap[*key_id], string_id_remap[*val_id]))
                    .collect();

                // Find or create global tagset ID
                let (global_id, _) = self.tagsets.insert_full(remapped_tagset);
                tagset_id_remap.push(global_id);
            }

            id_remappings.push(tagset_id_remap);
        }

        // Remap and build new intervals
        let mut encoded_points: Vec<(Timestamp, usize)> = local_results
            .par_iter()
            .zip(id_remappings.par_iter())
            .flat_map(|((chunk_encoded, _, _), id_remap)| {
                chunk_encoded
                    .iter()
                    .map(|(ts, local_id)| (*ts, id_remap[*local_id]))
                    .collect::<Vec<_>>()
            })
            .collect();

        encoded_points.par_sort_unstable_by_key(|(ts, _)| *ts);

        let new_intervals = Self::build_intervals_from_encoded(encoded_points, self.resolution)?;

        // Merge with existing intervals
        let mut all_intervals: Vec<_> = self.lapper.iter().cloned().collect();
        all_intervals.extend(new_intervals);

        // Rebuild lapper
        self.lapper = Lapper::new(all_intervals);

        Ok(())
    }
}

impl IntervalCache<TagSet> for BitmapLapperCache {
    fn from_sorted(sorted_data: SortedData<TagSet>) -> Result<Self, CacheBuildError> {
        Self::from_sorted_with_resolution(sorted_data, Duration::from_nanos(1))
    }

    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
        let bucketed_t = Self::bucket_timestamp(t, self.resolution);
        let start = bucketed_t;
        let stop = bucketed_t + 1;

        // Collect all bitmaps that overlap this point
        let mut result_bitmap = RoaringBitmap::new();
        for interval in self.lapper.find(start, stop) {
            result_bitmap |= &*interval.val; // Deref EqRoaringBitmap to RoaringBitmap
        }

        // Decode tagsets
        result_bitmap
            .iter()
            .map(|id| self.decode_tagset(id))
            .collect()
    }

    fn query_range(&self, range: &Range<Timestamp>) -> Vec<Vec<(&str, &str)>> {
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

        // Collect all bitmaps that overlap this range using SIMD-optimized union
        let mut result_bitmap = RoaringBitmap::new();
        for interval in self.lapper.find(bucketed_start, query_end) {
            result_bitmap |= &*interval.val; // Deref EqRoaringBitmap to RoaringBitmap
        }

        // Decode unique tagsets
        result_bitmap
            .iter()
            .map(|id| self.decode_tagset(id))
            .collect()
    }

    fn append_sorted(&mut self, sorted_data: SortedData<TagSet>) -> Result<(), CacheBuildError> {
        let new_intervals = Self::build_intervals(
            sorted_data.into_inner(),
            self.resolution,
            &mut self.string_dict,
            &mut self.tagsets,
        )?;

        // Merge with existing intervals
        let mut all_intervals: Vec<_> = self.lapper.iter().cloned().collect();
        all_intervals.extend(new_intervals);

        // Rebuild lapper
        self.lapper = Lapper::new(all_intervals);

        Ok(())
    }

    fn size_bytes(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();

        // Size of all intervals in the Lapper
        size += self.lapper.len() * std::mem::size_of::<Interval<u64, EqRoaringBitmap>>();

        // Size of bitmaps
        for interval in self.lapper.iter() {
            size += interval.val.serialized_size();
        }

        // Size of the string dictionary
        size += self.string_dict.size();

        // Size of the tagsets array
        size += self.tagsets.len() * std::mem::size_of::<EncodedTagSet>();
        for tagset in &self.tagsets {
            size += tagset.len() * std::mem::size_of::<(usize, usize)>();
        }

        size
    }

    fn interval_count(&self) -> usize {
        self.lapper.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tagset(pairs: &[(&str, &str)]) -> TagSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_bitmap_lapper_cache_basic() {
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (4, tag_b.clone())];

        let cache = BitmapLapperCache::new(data).unwrap();

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
    fn test_bitmap_lapper_cache_empty() {
        let cache: BitmapLapperCache = BitmapLapperCache::new(vec![]).unwrap();

        assert_eq!(cache.query_point(1).len(), 0);
        assert_eq!(cache.query_range(&(0..100)).len(), 0);
        assert_eq!(cache.interval_count(), 0);
    }

    #[test]
    fn test_bitmap_lapper_cache_merge() {
        let tag_a = make_tagset(&[("host", "server1")]);

        let data = vec![(1, tag_a.clone()), (2, tag_a.clone()), (3, tag_a.clone())];

        let cache = BitmapLapperCache::new(data).unwrap();

        // With nanosecond resolution, creates 3 intervals (one per timestamp)
        // This is correct - BitmapLapperCache groups by time bucket, not by value
        assert_eq!(cache.interval_count(), 3);
        assert!(!cache.query_point(1).is_empty());
        assert!(!cache.query_point(3).is_empty());
        assert_eq!(cache.query_point(4).len(), 0);
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

        let cache = BitmapLapperCache::new(data).unwrap();

        // Should have just 2 intervals (one per bucket), not 4!
        assert_eq!(cache.interval_count(), 2);

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

        let cache = BitmapLapperCache::new(data).unwrap();

        // Query range [1, 6) should get tag_a and tag_b
        let results = cache.query_range(&(1..6));
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
    }

    #[test]
    fn test_interval_reduction() {
        // Verify dramatic interval reduction vs ValueAwareLapperCache
        let tag_a = make_tagset(&[("host", "server1")]);
        let tag_b = make_tagset(&[("host", "server2")]);

        // 10 timestamps, 2 tagsets each = 20 data points
        let mut data = Vec::new();
        for t in 0..10 {
            data.push((t, tag_a.clone()));
            data.push((t, tag_b.clone()));
        }

        let cache = BitmapLapperCache::new(data).unwrap();

        // Should have only 10 intervals (one per timestamp), not 20!
        assert_eq!(cache.interval_count(), 10);
    }
}
