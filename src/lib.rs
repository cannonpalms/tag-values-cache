//! A library for building time-interval caches from discrete timestamp-value pairs.
//!
//! This library provides a trait-based interface for creating caches that convert
//! vectors of (timestamp, value) pairs into efficient queryable interval structures.
//! Consecutive timestamps with the same value are merged into continuous intervals.
//!
//! # Example
//! ```ignore
//! use tag_values_cache::{IntervalCache, IntervalTreeCache};
//!
//! let data = vec![
//!     (1, "A"),
//!     (2, "A"),  // Will be merged with timestamp 1 into interval [1, 3)
//!     (4, "B"),
//!     (5, "B"),  // Will be merged with timestamp 4 into interval [4, 6)
//! ];
//!
//! let cache = IntervalTreeCache::new(data)?;
//! let values = cache.query_point(2);  // Returns ["A"]
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Range;
use std::rc::Rc;

use arrow::array::{Array, StringArray};
use arrow::array::{RecordBatch, as_dictionary_array, as_primitive_array, as_string_array};
use arrow::datatypes::{DataType, Int32Type, TimestampNanosecondType};

pub mod bitmap_lapper_cache;
pub mod btree_cache;
pub mod interavl_cache;
pub mod interval_tree;
pub mod lapper_cache;
pub mod nclist_cache;
pub mod segment_tree_cache;
pub mod streaming;
pub mod tag_generator;
pub mod unmerged_btree_cache;
pub mod value_aware_lapper;
pub mod value_aware_lapper_cache;
pub mod vec_cache;

// Re-export the implementations for convenience
pub use bitmap_lapper_cache::BitmapLapperCache;
pub use btree_cache::BTreeCache;
pub use interval_tree::IntervalTreeCache;
pub use lapper_cache::LapperCache;
pub use nclist_cache::NCListCache;
pub use segment_tree_cache::SegmentTreeCache;
pub use unmerged_btree_cache::UnmergedBTreeCache;
pub use value_aware_lapper::ValueAwareLapper;
pub use value_aware_lapper_cache::ValueAwareLapperCache;
pub use vec_cache::VecCache;

/// The type used for timestamps (nanoseconds since epoch)
pub type Timestamp = u64;

/// A set of tag name-value pairs for direct string handling.
///
/// This type uses BTreeSet to provide:
/// - Deterministic ordering for consistent iteration
/// - Value-based equality (two TagSets with same elements are equal)
/// - Efficient comparison and merging operations
pub type TagSet = BTreeSet<(String, String)>;

/// Represents a dictionary-encoded TagSet as a list of (key_id, value_id) pairs.
///
/// This is used internally by caches to store TagSets more efficiently.
/// Using Box<[T]> instead of Vec<T> avoids excess capacity overhead.
pub(crate) type EncodedTagSet = Box<[(usize, usize)]>;

/// A fast IndexSet using ahash for hashing.
///
/// Uses ahash::RandomState instead of the default hasher for better performance
/// on non-cryptographic hashing workloads.
pub(crate) type FastIndexSet<T> = indexmap::IndexSet<T, ahash::RandomState>;

/// Encodes a TagSet into a dictionary-encoded representation.
///
/// This function takes all strings (keys and values) from the TagSet and looks them up
/// (or inserts them) in the provided string dictionary, returning a vector of ID pairs.
///
/// This is an internal function used by streaming builders.
pub(crate) fn encode_tagset(
    tagset: &TagSet,
    string_dict: &mut arrow_util::dictionary::StringDictionary<usize>,
) -> EncodedTagSet {
    tagset
        .iter()
        .map(|(k, val)| {
            let key_id = string_dict.lookup_value_or_insert(k);
            let value_id = string_dict.lookup_value_or_insert(val);
            (key_id, value_id)
        })
        .collect()
}

/// Trait for types that can estimate their heap-allocated memory size.
///
/// This trait should be implemented by types that allocate memory on the heap,
/// allowing cache implementations to accurately track memory usage.
pub trait HeapSize {
    /// Returns the estimated number of bytes allocated on the heap by this value.
    ///
    /// This should NOT include the size of the value itself (`size_of::`<Self>()),
    /// only the additional heap-allocated memory it owns.
    fn heap_size(&self) -> usize;
}

// Implement HeapSize for common types
impl HeapSize for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>()
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for BTreeMap<K, V> {
    fn heap_size(&self) -> usize {
        // BTreeMap node overhead: approximately 40 bytes per node
        const NODE_OVERHEAD: usize = 40;
        let mut size = self.len() * NODE_OVERHEAD;

        // Add the heap size of all keys and values
        for (k, v) in self {
            size += k.heap_size();
            size += v.heap_size();
        }

        size
    }
}

impl<T: HeapSize> HeapSize for BTreeSet<T> {
    fn heap_size(&self) -> usize {
        // BTreeSet node overhead: approximately 40 bytes per node
        const NODE_OVERHEAD: usize = 40;
        let mut size = self.len() * NODE_OVERHEAD;

        // Add the heap size of all values
        for v in self {
            size += v.heap_size();
        }

        size
    }
}

impl HeapSize for (String, String) {
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size()
    }
}

// Default implementation for types that don't allocate heap memory
impl HeapSize for u8 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u16 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u32 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for u64 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i8 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i16 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i32 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for i64 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for f32 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for f64 {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for bool {
    fn heap_size(&self) -> usize {
        0
    }
}

/// Errors that can occur when building an interval cache
#[derive(thiserror::Error, Debug)]
pub enum CacheBuildError {
    /// Occurs when trying to create an interval [t, t+1) where t == `u64::MAX`
    #[error("cannot create interval [t, t+1) because t == u64::MAX")]
    TimestampOverflow(u64),

    /// Occurs when trying to append data with a different time resolution than the cache
    #[error(
        "resolution mismatch: cannot append data with resolution {append_resolution:?} to cache with resolution {cache_resolution:?}"
    )]
    ResolutionMismatch {
        cache_resolution: std::time::Duration,
        append_resolution: std::time::Duration,
    },

    /// Occurs when trying to finalize a builder with no data
    #[error("no data was processed - cache is empty")]
    NoData,

    /// Occurs when an Arrow operation fails during streaming
    #[error("arrow error: {0}")]
    ArrowError(String),
}

/// Wrapper type that guarantees data is sorted by timestamp then value.
///
/// This type encodes the assumption that data has been pre-sorted,
/// allowing cache implementations to skip sorting steps.
#[derive(Clone, Debug)]
pub struct SortedData<V> {
    points: Vec<(Timestamp, V)>,
}

impl<V> SortedData<V> {
    /// Consume the wrapper and return the inner sorted data
    #[must_use]
    pub fn into_inner(self) -> Vec<(Timestamp, V)> {
        self.points
    }
}

impl<V> SortedData<V>
where
    V: Clone + Ord,
{
    /// Create a new `SortedData` from unsorted input.
    /// This will sort the data by timestamp, then by value.
    #[must_use]
    pub fn from_unsorted(mut points: Vec<(Timestamp, V)>) -> Self {
        points.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        Self { points }
    }

    /// Create `SortedData` from pre-sorted input.
    ///
    /// # Safety
    /// The caller must guarantee that `points` is sorted by timestamp
    /// first, then by value in ascending order.
    ///
    /// In debug builds, this will verify the sorting assumption.
    #[must_use]
    pub fn from_sorted(points: Vec<(Timestamp, V)>) -> Self {
        #[cfg(debug_assertions)]
        {
            for window in points.windows(2) {
                assert!(
                    window[0].0 < window[1].0
                        || (window[0].0 == window[1].0 && window[0].1 <= window[1].1),
                    "Data is not sorted by timestamp then value"
                );
            }
        }
        Self { points }
    }
}

/// The main trait for interval-based caches.
///
/// Implementations of this trait build interval structures from discrete
/// timestamp-value pairs, where consecutive timestamps with identical values
/// are merged into continuous intervals.
///
/// # Performance Note
/// All methods in this trait assume input data is pre-sorted by timestamp
/// first, then by value. Use the `SortedData` wrapper type to ensure this
/// invariant is maintained.
///
/// # Type Parameters
/// * `V` - The value type stored in the cache. Must be cloneable and comparable.
pub trait IntervalCache<V>: Sized
where
    V: Clone + Eq + std::hash::Hash,
{
    /// Create a new cache from sorted (timestamp, value) pairs.
    ///
    /// # Arguments
    /// * `sorted_data` - Pre-sorted data wrapped in `SortedData` type
    ///
    /// # Returns
    /// * `Ok(Self)` - The constructed cache
    /// * `Err(CacheBuildError)` - If a timestamp overflow occurs
    ///
    /// # Performance
    /// This method assumes the data is already sorted and will not re-sort it.
    fn from_sorted(sorted_data: SortedData<V>) -> Result<Self, CacheBuildError>;

    /// Query for all values that exist at a specific timestamp.
    ///
    /// Returns all values whose intervals contain the given timestamp.
    /// Multiple values can exist at the same timestamp if intervals overlap.
    ///
    /// # Arguments
    /// * `t` - The timestamp to query
    ///
    /// # Returns
    /// A set of references to all values at the given timestamp.
    /// Returns an empty set if no values exist at that timestamp.
    /// The order of values in the set is unspecified.
    fn query_point(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>>;

    /// Query for all values that exist within a time range.
    ///
    /// Returns all unique values whose intervals overlap with the given range.
    ///
    /// # Arguments
    /// * `range` - The time range to query [start, end)
    ///
    /// # Returns
    /// A set of references to all unique values within the range.
    /// Returns an empty set if no values exist in the range.
    /// The order of values in the set is unspecified.
    fn query_range(&self, range: &Range<Timestamp>) -> Vec<Vec<(&str, &str)>>;

    /// Append a batch of pre-sorted timestamp-value pairs to the cache.
    ///
    /// This method adds new data points to the existing cache, potentially
    /// merging with existing intervals if the new data is consecutive with
    /// existing intervals for the same value.
    ///
    /// # Arguments
    /// * `sorted_data` - Pre-sorted data wrapped in `SortedData` type
    ///
    /// # Returns
    /// * `Ok(())` - If the append was successful
    /// * `Err(CacheBuildError)` - If a timestamp overflow occurs
    ///
    /// # Performance
    /// This method assumes the new data is already sorted and will not re-sort it.
    fn append_sorted(&mut self, sorted_data: SortedData<V>) -> Result<(), CacheBuildError>;

    /// Convenience method to create a cache from unsorted data.
    ///
    /// This will sort the data before building the cache.
    fn new(points: Vec<(Timestamp, V)>) -> Result<Self, CacheBuildError>
    where
        V: Ord,
    {
        Self::from_sorted(SortedData::from_unsorted(points))
    }

    /// Convenience method to append unsorted data.
    ///
    /// This will sort the data before appending.
    fn append_batch(&mut self, points: Vec<(Timestamp, V)>) -> Result<(), CacheBuildError>
    where
        V: Ord,
    {
        self.append_sorted(SortedData::from_unsorted(points))
    }

    /// Calculate the approximate memory usage of this cache in bytes.
    ///
    /// This includes the memory used by the data structures themselves,
    /// as well as any heap-allocated memory they contain.
    ///
    /// # Returns
    /// The estimated total memory usage in bytes.
    fn size_bytes(&self) -> usize
    where
        V: HeapSize;

    /// Return the number of intervals stored in the cache.
    ///
    /// This represents the number of distinct time ranges after merging
    /// adjacent/overlapping intervals with identical values.
    ///
    /// # Returns
    /// The total number of intervals in the cache.
    fn interval_count(&self) -> usize;
}

/// Builder pattern for creating interval caches with different configurations
pub struct CacheBuilder<V> {
    data: Vec<(Timestamp, V)>,
}

impl<V> CacheBuilder<V>
where
    V: Clone + Eq + std::hash::Hash,
{
    /// Create a new builder with the given data
    #[must_use]
    pub fn new(data: Vec<(Timestamp, V)>) -> Self {
        Self { data }
    }

    /// Build an `IntervalTreeCache` from the data
    pub fn build_interval_tree(self) -> Result<IntervalTreeCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        IntervalTreeCache::new(self.data)
    }

    /// Build a `VecCache` from the data
    pub fn build_vec_cache(self) -> Result<VecCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        VecCache::new(self.data)
    }

    /// Build a `BTreeCache` from the data
    pub fn build_btree_cache(self) -> Result<BTreeCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        BTreeCache::new(self.data)
    }

    /// Build a `LapperCache` from the data
    pub fn build_lapper_cache(self) -> Result<LapperCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        LapperCache::new(self.data)
    }

    /// Build an `NCListCache` from the data
    pub fn build_nclist_cache(self) -> Result<NCListCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        NCListCache::new(self.data)
    }

    /// Build a `SegmentTreeCache` from the data
    pub fn build_segment_tree_cache(self) -> Result<SegmentTreeCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        SegmentTreeCache::new(self.data)
    }

    /// Build an `UnmergedBTreeCache` from the data
    pub fn build_unmerged_btree_cache(self) -> Result<UnmergedBTreeCache<V>, CacheBuildError>
    where
        V: Ord,
        for<'a> &'a V: IntoIterator<Item = &'a (String, String)>,
    {
        UnmergedBTreeCache::new(self.data)
    }
}

/// Represents a single value from an Arrow column
#[derive(Clone, Debug, PartialEq)]
pub enum ArrowValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    /// For types we don't handle specifically
    Unsupported(String),
}

impl fmt::Display for ArrowValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArrowValue::Null => write!(f, "null"),
            ArrowValue::Boolean(v) => write!(f, "{v}"),
            ArrowValue::Int8(v) => write!(f, "{v}"),
            ArrowValue::Int16(v) => write!(f, "{v}"),
            ArrowValue::Int32(v) => write!(f, "{v}"),
            ArrowValue::Int64(v) => write!(f, "{v}"),
            ArrowValue::UInt8(v) => write!(f, "{v}"),
            ArrowValue::UInt16(v) => write!(f, "{v}"),
            ArrowValue::UInt32(v) => write!(f, "{v}"),
            ArrowValue::UInt64(v) => write!(f, "{v}"),
            ArrowValue::Float32(v) => write!(f, "{v}"),
            ArrowValue::Float64(v) => write!(f, "{v}"),
            ArrowValue::String(v) => write!(f, "{v}"),
            ArrowValue::Binary(v) => write!(f, "{v:?}"),
            ArrowValue::Unsupported(v) => write!(f, "{v}"),
        }
    }
}

impl ArrowValue {
    /// Returns a consistent ordering value for different variants
    fn variant_order(&self) -> u8 {
        match self {
            ArrowValue::Null => 0,
            ArrowValue::Boolean(_) => 1,
            ArrowValue::Int8(_) => 2,
            ArrowValue::Int16(_) => 3,
            ArrowValue::Int32(_) => 4,
            ArrowValue::Int64(_) => 5,
            ArrowValue::UInt8(_) => 6,
            ArrowValue::UInt16(_) => 7,
            ArrowValue::UInt32(_) => 8,
            ArrowValue::UInt64(_) => 9,
            ArrowValue::Float32(_) => 10,
            ArrowValue::Float64(_) => 11,
            ArrowValue::String(_) => 12,
            ArrowValue::Binary(_) => 13,
            ArrowValue::Unsupported(_) => 14,
        }
    }
}

impl Eq for ArrowValue {}

impl std::hash::Hash for ArrowValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ArrowValue::Null => {}
            ArrowValue::Boolean(v) => v.hash(state),
            ArrowValue::Int8(v) => v.hash(state),
            ArrowValue::Int16(v) => v.hash(state),
            ArrowValue::Int32(v) => v.hash(state),
            ArrowValue::Int64(v) => v.hash(state),
            ArrowValue::UInt8(v) => v.hash(state),
            ArrowValue::UInt16(v) => v.hash(state),
            ArrowValue::UInt32(v) => v.hash(state),
            ArrowValue::UInt64(v) => v.hash(state),
            ArrowValue::Float32(v) => v.to_bits().hash(state),
            ArrowValue::Float64(v) => v.to_bits().hash(state),
            ArrowValue::String(v) => v.hash(state),
            ArrowValue::Binary(v) => v.hash(state),
            ArrowValue::Unsupported(v) => v.hash(state),
        }
    }
}

impl Ord for ArrowValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (ArrowValue::Null, ArrowValue::Null) => std::cmp::Ordering::Equal,
            (ArrowValue::Null, _) => std::cmp::Ordering::Less,
            (_, ArrowValue::Null) => std::cmp::Ordering::Greater,
            (ArrowValue::Boolean(a), ArrowValue::Boolean(b)) => a.cmp(b),
            (ArrowValue::Int8(a), ArrowValue::Int8(b)) => a.cmp(b),
            (ArrowValue::Int16(a), ArrowValue::Int16(b)) => a.cmp(b),
            (ArrowValue::Int32(a), ArrowValue::Int32(b)) => a.cmp(b),
            (ArrowValue::Int64(a), ArrowValue::Int64(b)) => a.cmp(b),
            (ArrowValue::UInt8(a), ArrowValue::UInt8(b)) => a.cmp(b),
            (ArrowValue::UInt16(a), ArrowValue::UInt16(b)) => a.cmp(b),
            (ArrowValue::UInt32(a), ArrowValue::UInt32(b)) => a.cmp(b),
            (ArrowValue::UInt64(a), ArrowValue::UInt64(b)) => a.cmp(b),
            (ArrowValue::Float32(a), ArrowValue::Float32(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (ArrowValue::Float64(a), ArrowValue::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (ArrowValue::String(a), ArrowValue::String(b)) => a.cmp(b),
            (ArrowValue::Binary(a), ArrowValue::Binary(b)) => a.cmp(b),
            (ArrowValue::Unsupported(a), ArrowValue::Unsupported(b)) => a.cmp(b),
            // For mixed types, use a consistent ordering based on variant
            _ => {
                let self_ord = self.variant_order();
                let other_ord = other.variant_order();
                self_ord.cmp(&other_ord)
            }
        }
    }
}

impl PartialOrd for ArrowValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl HeapSize for ArrowValue {
    fn heap_size(&self) -> usize {
        match self {
            ArrowValue::String(s) | ArrowValue::Unsupported(s) => s.heap_size(),
            ArrowValue::Binary(v) => v.heap_size(),
            _ => 0,
        }
    }
}

/// A row of data from a `RecordBatch`, excluding the time column.
/// This represents all tag values for a specific timestamp.
/// Since tags are always strings, we store them as String instead of ArrowValue.
#[derive(Clone, Debug)]
pub struct RecordBatchRow {
    /// Tag names and their string values
    pub values: BTreeMap<String, String>,
}

impl RecordBatchRow {
    /// Create a new `RecordBatchRow` from tag values
    #[must_use]
    pub fn new(values: BTreeMap<String, String>) -> Self {
        Self { values }
    }
}

impl fmt::Display for RecordBatchRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .values
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

impl PartialEq for RecordBatchRow {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for RecordBatchRow {}

impl std::hash::Hash for RecordBatchRow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for (k, v) in &self.values {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl Ord for RecordBatchRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.values.cmp(&other.values)
    }
}

impl PartialOrd for RecordBatchRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl HeapSize for RecordBatchRow {
    fn heap_size(&self) -> usize {
        // The BTreeMap itself has heap allocation, plus the strings
        self.values.heap_size()
    }
}

/// Extract timestamp and row data from a `RecordBatch`.
///
/// This function processes a `RecordBatch` and extracts timestamp-value pairs
/// where the value contains all non-time columns for that row.
///
/// # Returns
/// A vector of (timestamp, `RecordBatchRow`) pairs where `RecordBatchRow`
/// contains all column data except the timestamp.
#[must_use]
pub fn extract_rows_from_batch(batch: &RecordBatch) -> Vec<(Timestamp, RecordBatchRow)> {
    let schema = batch.schema_ref();

    // Find the timestamp column
    let mut ts_idx = None;
    let mut non_time_columns = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column_type) = field.metadata().get("iox::column::type") {
            if column_type == "iox::column_type::timestamp" {
                ts_idx = Some(idx);
            } else {
                // Only include Utf8 columns (tags)
                match field.data_type() {
                    DataType::Utf8 | DataType::Dictionary(_, _) => {
                        non_time_columns.push((idx, field.name().clone()));
                    }
                    _ => {} // Skip non-string columns
                }
            }
        } else {
            // If no metadata, check field name for time column
            let name_lower = field.name().to_lowercase();
            if name_lower == "time"
                || name_lower == "timestamp"
                || name_lower == "_time"
                || name_lower == "eventtime"
            {
                ts_idx = Some(idx);
            } else {
                // Only include Utf8 columns (tags)
                match field.data_type() {
                    DataType::Utf8 | DataType::Dictionary(_, _) => {
                        non_time_columns.push((idx, field.name().clone()));
                    }
                    _ => {} // Skip non-string columns
                }
            }
        }
    }

    // panic if no timestamp found
    let ts_idx = ts_idx.expect("No timestamp column found in RecordBatch");

    // Get the timestamp column - it might be Int64 or TimestampNanosecond
    let ts_column = batch.column(ts_idx);
    let timestamps_vec: Vec<Timestamp> = match ts_column.data_type() {
        DataType::Int64 => as_primitive_array::<arrow::datatypes::Int64Type>(ts_column)
            .values()
            .iter()
            .map(|v| *v as u64)
            .collect(),
        DataType::Timestamp(_, _) => as_primitive_array::<TimestampNanosecondType>(ts_column)
            .values()
            .iter()
            .map(|v| *v as u64)
            .collect(),
        dt => panic!("Unexpected data type for timestamp column: {dt:?}"),
    };

    // Build results
    let mut results = Vec::with_capacity(batch.num_rows());

    for (row_idx, ts) in timestamps_vec.iter().enumerate().take(batch.num_rows()) {
        // Collect all non-time column values for this row as strings
        let mut row_values = BTreeMap::new();

        for &(col_idx, ref col_name) in &non_time_columns {
            let array = batch.column(col_idx);

            // Extract value from Utf8 or Dictionary columns only
            let value = if array.is_valid(row_idx) {
                match array.data_type() {
                    DataType::Utf8 => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map_or("string_error".to_string(), |arr| {
                            arr.value(row_idx).to_string()
                        }),
                    DataType::Dictionary(_, _) => {
                        // Handle dictionary encoded columns (like tags)
                        let dict_array = as_dictionary_array::<Int32Type>(array);
                        if let Some(key) = dict_array.key(row_idx) {
                            let values = as_string_array(dict_array.values());
                            values.value(key).to_string()
                        } else {
                            "null".to_string()
                        }
                    }
                    dt => {
                        // This shouldn't happen since we filter for Utf8/Dictionary columns
                        panic!("Unexpected data type in non-time columns: {dt:?}")
                    }
                }
            } else {
                "null".to_string()
            };

            row_values.insert(col_name.to_string(), value);
        }

        let row = RecordBatchRow::new(row_values);
        results.push((*ts, row));
    }

    results
}

/// Process multiple `RecordBatches` into sorted data with `RecordBatchRow` values.
///
/// This function processes all batches and returns them as `SortedData`
/// ready for cache construction with full column data preserved.
pub fn record_batches_to_row_data(
    batches: impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>,
) -> Result<SortedData<RecordBatchRow>, Box<dyn std::error::Error>> {
    let mut all_points = Vec::new();

    for batch in batches {
        let batch = batch?;
        let points = extract_rows_from_batch(&batch);
        all_points.extend(points);
    }

    Ok(SortedData::from_unsorted(all_points))
}

use ahash::AHashMap;
use smallvec::SmallVec;

enum ColAcc<'a> {
    Utf8(&'a arrow::array::StringArray),
    Utf8View(&'a arrow::array::StringViewArray),
    Dict32 {
        keys: &'a arrow::array::Int32Array,
        values: &'a arrow::array::StringArray,
    },
}

pub fn extract_tags_from_batch(batch: &RecordBatch) -> Vec<(Timestamp, TagSet)> {
    // … find ts_idx and tag_column_indices …
    let schema = batch.schema_ref();

    // First pass: check if any field has IOx metadata
    let has_iox_metadata = schema
        .fields()
        .iter()
        .any(|f| f.metadata().contains_key("iox::column::type"));

    // Find the timestamp column and tag columns
    let mut ts_idx = None;
    let mut tag_column_indices = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column_type) = field.metadata().get("iox::column::type") {
            if column_type == "iox::column_type::timestamp" {
                ts_idx = Some(idx);
            } else if column_type == "iox::column_type::tag" {
                // Only include columns explicitly marked as tags
                match field.data_type() {
                    DataType::Utf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
                        tag_column_indices.push(idx);
                    }
                    _ => {} // Skip non-string columns
                }
            }
            // Skip other column types (fields, etc.)
        } else {
            // If no metadata, check field name for time column
            let name_lower = field.name().to_lowercase();
            if name_lower == "time"
                || name_lower == "timestamp"
                || name_lower == "_time"
                || name_lower == "eventtime"
            {
                ts_idx = Some(idx);
            } else if !has_iox_metadata {
                // Fallback: If there's no IOx metadata anywhere in the schema,
                // treat all string columns (except timestamp) as tags
                match field.data_type() {
                    DataType::Utf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
                        tag_column_indices.push(idx);
                    }
                    _ => {} // Skip non-string columns
                }
            }
            // Otherwise skip columns without metadata when IOx metadata exists
        }
    }

    // panic if no timestamp found
    let ts_idx = ts_idx.expect("No timestamp column found in RecordBatch");

    // Timestamp accessor (no full copy)
    enum TsAcc<'a> {
        I64(&'a arrow::array::Int64Array),
        TsNs(&'a arrow::array::TimestampNanosecondArray),
    }
    let ts_acc = match batch.column(ts_idx).data_type() {
        DataType::Int64 => TsAcc::I64(arrow::array::as_primitive_array::<
            arrow::datatypes::Int64Type,
        >(batch.column(ts_idx))),
        DataType::Timestamp(_, _) => TsAcc::TsNs(arrow::array::as_primitive_array::<
            arrow::datatypes::TimestampNanosecondType,
        >(batch.column(ts_idx))),
        dt => panic!("bad ts: {dt:?}"),
    };

    // Build column accessors + intern column names once (Rc<str>)
    let mut tag_cols: Vec<(Rc<str>, ColAcc)> = tag_column_indices
        .into_iter()
        .map(|i| {
            let field = schema.field(i);
            let name_rc: Rc<str> = Rc::from(field.name().as_str()); // 1 alloc per column
            let arr = batch.column(i);
            let acc = match arr.data_type() {
                DataType::Utf8 => ColAcc::Utf8(arrow::array::as_string_array(arr)),
                DataType::Utf8View => ColAcc::Utf8View(
                    arr.as_any()
                        .downcast_ref::<arrow::array::StringViewArray>()
                        .unwrap(),
                ),
                DataType::Dictionary(_, _) => {
                    let dict =
                        arrow::array::as_dictionary_array::<arrow::datatypes::Int32Type>(arr);
                    ColAcc::Dict32 {
                        keys: dict.keys(),
                        values: arrow::array::as_string_array(dict.values()),
                    }
                }
                dt => panic!("unexpected tag type: {dt:?}"),
            };
            (name_rc, acc)
        })
        .collect();

    // Ensure deterministic order without BTreeSet work per row
    tag_cols.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    // Per-column interners: &str -> Rc<str>, so each unique value allocates once per column
    struct ValIntern<'a> {
        map: AHashMap<&'a str, Rc<str>>,
    }
    let mut interner: Vec<ValIntern> = tag_cols
        .iter()
        .map(|_| ValIntern {
            map: AHashMap::default(),
        })
        .collect();

    let num_rows = batch.num_rows();
    let mut out = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        // timestamp on demand
        let ts = match ts_acc {
            TsAcc::I64(a) => a.value(row) as u64,
            TsAcc::TsNs(a) => a.value(row) as u64,
        };

        // build small, already-ordered vector of pairs
        let mut pairs: SmallVec<[(Rc<str>, Rc<str>); 16]> = SmallVec::with_capacity(tag_cols.len());

        for (col_idx, (col_name, acc)) in tag_cols.iter().enumerate() {
            let s_opt: Option<&str> = match acc {
                ColAcc::Utf8(a) => a.is_valid(row).then(|| a.value(row)),
                ColAcc::Utf8View(a) => a.is_valid(row).then(|| a.value(row)),
                ColAcc::Dict32 { keys, values } => {
                    if keys.is_valid(row) {
                        Some(values.value(keys.value(row) as usize))
                    } else {
                        None
                    }
                }
            };

            let v_rc = match s_opt {
                Some(s) => {
                    // intern per column
                    if let Some(existing) = interner[col_idx].map.get(s) {
                        existing.clone()
                    } else {
                        let owned = Rc::<str>::from(s);
                        interner[col_idx].map.insert(s, owned.clone());
                        owned
                    }
                }
                None => Rc::<str>::from("null"),
            };

            pairs.push((col_name.clone(), v_rc));
        }

        // If you must return TagSet = BTreeSet<(String, String)>, convert here:
        let mut ts_set: TagSet = BTreeSet::new();
        // Pushing in name-sorted order; conversion still allocates, but only once per unique string thanks to interning.
        for (k, v) in pairs {
            ts_set.insert((k.as_ref().to_owned(), v.as_ref().to_owned()));
        }

        out.push((ts, ts_set));
    }

    out
}

/// Extract and encode tag data from a RecordBatch directly into dictionary encoding.
///
/// This function combines extraction and encoding into a single pass, avoiding the
/// intermediate TagSet creation and multiple iterations over the data.
///
/// # Arguments
/// * `batch` - The RecordBatch to extract data from
/// * `string_dict` - The string dictionary to encode strings into
/// * `tagsets` - The IndexSet to store/lookup encoded tagsets
///
/// # Returns
/// A vector of (timestamp, tagset_id) pairs where tagset_id is the index in the tagsets IndexSet
pub fn extract_and_encode_tags_from_batch(
    batch: &RecordBatch,
    string_dict: &mut arrow_util::dictionary::StringDictionary<usize>,
    tagsets: &mut FastIndexSet<EncodedTagSet>,
) -> Vec<(Timestamp, usize)> {
    let schema = batch.schema_ref();

    // First pass: check if any field has IOx metadata
    let has_iox_metadata = schema
        .fields()
        .iter()
        .any(|f| f.metadata().contains_key("iox::column::type"));

    // Find the timestamp column and tag columns
    let mut ts_idx = None;
    let mut tag_column_indices = Vec::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column_type) = field.metadata().get("iox::column::type") {
            if column_type == "iox::column_type::timestamp" {
                ts_idx = Some(idx);
            } else if column_type == "iox::column_type::tag" {
                // Only include columns explicitly marked as tags
                match field.data_type() {
                    DataType::Utf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
                        tag_column_indices.push(idx);
                    }
                    _ => {} // Skip non-string columns
                }
            }
            // Skip other column types (fields, etc.)
        } else {
            // If no metadata, check field name for time column
            let name_lower = field.name().to_lowercase();
            if name_lower == "time"
                || name_lower == "timestamp"
                || name_lower == "_time"
                || name_lower == "eventtime"
            {
                ts_idx = Some(idx);
            } else if !has_iox_metadata {
                // Fallback: If there's no IOx metadata anywhere in the schema,
                // treat all string columns (except timestamp) as tags
                match field.data_type() {
                    DataType::Utf8 | DataType::Utf8View | DataType::Dictionary(_, _) => {
                        tag_column_indices.push(idx);
                    }
                    _ => {} // Skip non-string columns
                }
            }
            // Otherwise skip columns without metadata when IOx metadata exists
        }
    }

    // panic if no timestamp found
    let ts_idx = ts_idx.expect("No timestamp column found in RecordBatch");

    // Timestamp accessor (no full copy)
    enum TsAcc<'a> {
        I64(&'a arrow::array::Int64Array),
        TsNs(&'a arrow::array::TimestampNanosecondArray),
    }
    let ts_acc = match batch.column(ts_idx).data_type() {
        DataType::Int64 => TsAcc::I64(arrow::array::as_primitive_array::<
            arrow::datatypes::Int64Type,
        >(batch.column(ts_idx))),
        DataType::Timestamp(_, _) => TsAcc::TsNs(arrow::array::as_primitive_array::<
            arrow::datatypes::TimestampNanosecondType,
        >(batch.column(ts_idx))),
        dt => panic!("bad ts: {dt:?}"),
    };

    // Build column accessors + encode column names once
    // Store column_name_id with each accessor
    let mut tag_cols: Vec<(usize, ColAcc)> = tag_column_indices
        .into_iter()
        .map(|i| {
            // Encode column name into dictionary once
            let name_id = string_dict.lookup_value_or_insert(schema.field(i).name().as_str());
            let arr = batch.column(i);
            let acc = match arr.data_type() {
                DataType::Utf8 => ColAcc::Utf8(arrow::array::as_string_array(arr)),
                DataType::Utf8View => ColAcc::Utf8View(
                    arr.as_any()
                        .downcast_ref::<arrow::array::StringViewArray>()
                        .unwrap(),
                ),
                DataType::Dictionary(_, _) => {
                    let dict =
                        arrow::array::as_dictionary_array::<arrow::datatypes::Int32Type>(arr);
                    ColAcc::Dict32 {
                        keys: dict.keys(),
                        values: arrow::array::as_string_array(dict.values()),
                    }
                }
                dt => panic!("unexpected tag type: {dt:?}"),
            };
            (name_id, acc)
        })
        .collect();

    // Sort by column name ID for deterministic ordering
    tag_cols.sort_unstable_by_key(|(name_id, _)| *name_id);

    let num_rows = batch.num_rows();
    let mut out = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        // timestamp on demand
        let ts = match &ts_acc {
            TsAcc::I64(a) => a.value(row) as u64,
            TsAcc::TsNs(a) => a.value(row) as u64,
        };

        // Build encoded tagset directly - no intermediate collections
        let mut encoded_pairs: Vec<(usize, usize)> = Vec::with_capacity(tag_cols.len());

        for (col_name_id, acc) in &tag_cols {
            let s_opt: Option<&str> = match acc {
                ColAcc::Utf8(a) => a.is_valid(row).then(|| a.value(row)),
                ColAcc::Utf8View(a) => a.is_valid(row).then(|| a.value(row)),
                ColAcc::Dict32 { keys, values } => {
                    if keys.is_valid(row) {
                        Some(values.value(keys.value(row) as usize))
                    } else {
                        None
                    }
                }
            };

            let value_str = s_opt.unwrap_or("");
            let value_id = string_dict.lookup_value_or_insert(value_str);

            encoded_pairs.push((*col_name_id, value_id));
        }

        // Convert to EncodedTagSet and get/create tagset ID
        let encoded_tagset: EncodedTagSet = encoded_pairs.into_boxed_slice();
        let (tagset_id, _) = tagsets.insert_full(encoded_tagset);

        out.push((ts, tagset_id));
    }

    out
}

/// Process multiple RecordBatches into sorted TagSet data.
///
/// This function processes all batches and returns them as `SortedData`
/// ready for cache construction with TagSet values.
pub fn record_batches_to_tag_data(
    batches: impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>,
) -> Result<SortedData<TagSet>, Box<dyn std::error::Error>> {
    let mut all_points = Vec::new();

    for batch in batches {
        let batch = batch?;
        let points = extract_tags_from_batch(&batch);
        all_points.extend(points);
    }

    Ok(SortedData::from_unsorted(all_points))
}

/// Extract timestamp and tag data from a `RecordBatch`.
///
/// This function processes a `RecordBatch` from a parquet file and extracts
/// timestamp-value pairs where the value is a serialized representation
/// of all tag columns for that row.
///
/// # Returns
/// A vector of (timestamp, `tag_string`) pairs where `tag_string` contains
/// all tags concatenated with their values.
#[must_use]
pub fn extract_time_series_from_batch(batch: &RecordBatch) -> Vec<(Timestamp, String)> {
    let schema = batch.schema_ref();

    // Find the timestamp column
    let mut ts_idx = None;
    let mut tag_indices = BTreeMap::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column_type) = field.metadata().get("iox::column::type") {
            match column_type.as_str() {
                "iox::column_type::timestamp" => ts_idx = Some(idx),
                "iox::column_type::tag" => {
                    tag_indices.insert(field.name().clone(), idx);
                }
                _ => {}
            }
        }
    }

    let ts_idx = match ts_idx {
        Some(idx) => idx,
        None => {
            // Fallback: look for a column named "time" or similar
            schema
                .fields()
                .iter()
                .position(|f| {
                    f.name().to_lowercase() == "time" || f.name().to_lowercase() == "timestamp"
                })
                .unwrap_or(0)
        }
    };

    let timestamps = as_primitive_array::<TimestampNanosecondType>(batch.column(ts_idx));

    // Collect tag arrays
    let tag_arrays: BTreeMap<String, _> = tag_indices
        .iter()
        .map(|(name, idx)| {
            (
                name.clone(),
                as_dictionary_array::<Int32Type>(batch.column(*idx)),
            )
        })
        .collect();

    // Pre-extract dictionary values for efficiency
    let tag_values: BTreeMap<String, Vec<Rc<str>>> = tag_arrays
        .iter()
        .map(|(name, arr)| {
            let values = as_string_array(arr.values())
                .iter()
                .map(|v| Rc::from(v.unwrap_or("")))
                .collect();
            (name.clone(), values)
        })
        .collect();

    // Build time-series pairs
    let mut results = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let ts = timestamps.value(row) as u64;

        // Collect tag values for this row
        let mut tag_string_parts = Vec::new();
        for (name, arr) in &tag_arrays {
            if let Some(idx) = arr.key(row)
                && let Some(vals) = tag_values.get(name)
                && let Some(val) = vals.get(idx)
                && !val.is_empty()
            {
                tag_string_parts.push(format!("{name}={val}"));
            }
        }

        // Create composite tag string
        let tag_string = tag_string_parts.join(",");
        results.push((ts, tag_string));
    }

    results
}

/// Process multiple `RecordBatches` from a parquet reader into sorted data.
///
/// This is a convenience function that processes all batches from a reader
/// and returns them as `SortedData` ready for cache construction.
pub fn record_batches_to_sorted_data(
    batches: impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>,
) -> Result<SortedData<String>, Box<dyn std::error::Error>> {
    let mut all_points = Vec::new();

    for batch in batches {
        let batch = batch?;
        let points = extract_time_series_from_batch(&batch);
        all_points.extend(points);
    }

    Ok(SortedData::from_unsorted(all_points))
}

/// Format bytes in the most appropriate unit (B, KiB, MiB, or GiB) with limited decimal places
pub fn format_bytes(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    let bytes_f64 = bytes as f64;

    if bytes < 1024 {
        // Less than 1 KiB - show in bytes
        format!("{bytes} B")
    } else if bytes_f64 < MIB {
        // Less than 1 MiB - show in KiB
        format!("{:.2} KiB", bytes_f64 / KIB)
    } else if bytes_f64 < GIB {
        // Less than 1 GiB - show in MiB
        format!("{:.2} MiB", bytes_f64 / MIB)
    } else {
        // 1 GiB or more - show in GiB
        format!("{:.2} GiB", bytes_f64 / GIB)
    }
}
