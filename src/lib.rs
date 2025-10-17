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

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::rc::Rc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::array::{RecordBatch, as_dictionary_array, as_primitive_array, as_string_array};
use arrow::datatypes::{DataType, Int32Type, TimestampNanosecondType};

pub mod btree_cache;
pub mod interavl_cache;
pub mod interval_tree;
pub mod lapper_cache;
pub mod nclist_cache;
pub mod segment_tree_cache;
pub mod value_lapper;
pub mod value_lapper_cache;
pub mod vec_cache;

// Re-export the implementations for convenience
pub use btree_cache::BTreeCache;
pub use interval_tree::IntervalTreeCache;
pub use lapper_cache::LapperCache;
pub use nclist_cache::NCListCache;
pub use segment_tree_cache::SegmentTreeCache;
pub use value_lapper::ValueLapper;
pub use value_lapper_cache::ValueLapperCache;
pub use vec_cache::VecCache;

/// The type used for timestamps (nanoseconds since epoch)
pub type Timestamp = u64;

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
    fn query_point(&self, t: Timestamp) -> HashSet<&V>;

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
    fn query_range(&self, range: Range<Timestamp>) -> HashSet<&V>;

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
    {
        IntervalTreeCache::new(self.data)
    }

    /// Build a `VecCache` from the data
    pub fn build_vec_cache(self) -> Result<VecCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        VecCache::new(self.data)
    }

    /// Build a `BTreeCache` from the data
    pub fn build_btree_cache(self) -> Result<BTreeCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        BTreeCache::new(self.data)
    }

    /// Build a `LapperCache` from the data
    pub fn build_lapper_cache(self) -> Result<LapperCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        LapperCache::new(self.data)
    }

    /// Build an `NCListCache` from the data
    pub fn build_nclist_cache(self) -> Result<NCListCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        NCListCache::new(self.data)
    }

    /// Build a `SegmentTreeCache` from the data
    pub fn build_segment_tree_cache(self) -> Result<SegmentTreeCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        SegmentTreeCache::new(self.data)
    }

    /// Build a `ValueLapperCache` from the data
    pub fn build_value_lapper_cache(self) -> Result<ValueLapperCache<V>, CacheBuildError>
    where
        V: Ord + Send + Sync,
    {
        ValueLapperCache::new(self.data)
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
/// This represents all column values for a specific timestamp.
#[derive(Clone, Debug)]
pub struct RecordBatchRow {
    /// Column names and their typed values
    pub values: BTreeMap<String, ArrowValue>,
}

impl RecordBatchRow {
    /// Create a new `RecordBatchRow` from column values
    #[must_use]
    pub fn new(values: BTreeMap<String, ArrowValue>) -> Self {
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
        // The BTreeMap itself has heap allocation, plus the strings and ArrowValues
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
                non_time_columns.push((idx, field.name().clone()));
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
                non_time_columns.push((idx, field.name().clone()));
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
        // Collect all non-time column values for this row
        let mut row_values = BTreeMap::new();

        for &(col_idx, ref col_name) in &non_time_columns {
            let array = batch.column(col_idx);

            // Extract typed value for this row
            let value = if array.is_valid(row_idx) {
                match array.data_type() {
                    DataType::Null => ArrowValue::Null,
                    DataType::Boolean => array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .map_or(ArrowValue::Unsupported("bool_error".to_string()), |arr| {
                            ArrowValue::Boolean(arr.value(row_idx))
                        }),
                    DataType::Int8 => array
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .map_or(ArrowValue::Unsupported("int8_error".to_string()), |arr| {
                            ArrowValue::Int8(arr.value(row_idx))
                        }),
                    DataType::Int16 => array
                        .as_any()
                        .downcast_ref::<arrow::array::Int16Array>()
                        .map_or(ArrowValue::Unsupported("int16_error".to_string()), |arr| {
                            ArrowValue::Int16(arr.value(row_idx))
                        }),
                    DataType::Int32 => array
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .map_or(ArrowValue::Unsupported("int32_error".to_string()), |arr| {
                            ArrowValue::Int32(arr.value(row_idx))
                        }),
                    DataType::Int64 => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map_or(ArrowValue::Unsupported("int64_error".to_string()), |arr| {
                            ArrowValue::Int64(arr.value(row_idx))
                        }),
                    DataType::UInt8 => array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt8Array>()
                        .map_or(ArrowValue::Unsupported("uint8_error".to_string()), |arr| {
                            ArrowValue::UInt8(arr.value(row_idx))
                        }),
                    DataType::UInt16 => array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt16Array>()
                        .map_or(ArrowValue::Unsupported("uint16_error".to_string()), |arr| {
                            ArrowValue::UInt16(arr.value(row_idx))
                        }),
                    DataType::UInt32 => array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt32Array>()
                        .map_or(ArrowValue::Unsupported("uint32_error".to_string()), |arr| {
                            ArrowValue::UInt32(arr.value(row_idx))
                        }),
                    DataType::UInt64 => array
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .map_or(ArrowValue::Unsupported("uint64_error".to_string()), |arr| {
                            ArrowValue::UInt64(arr.value(row_idx))
                        }),
                    DataType::Float32 => array
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .map_or(
                            ArrowValue::Unsupported("float32_error".to_string()),
                            |arr| ArrowValue::Float32(arr.value(row_idx)),
                        ),
                    DataType::Float64 => array.as_any().downcast_ref::<Float64Array>().map_or(
                        ArrowValue::Unsupported("float64_error".to_string()),
                        |arr| ArrowValue::Float64(arr.value(row_idx)),
                    ),
                    DataType::Utf8 => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map_or(ArrowValue::Unsupported("string_error".to_string()), |arr| {
                            ArrowValue::String(arr.value(row_idx).to_string())
                        }),
                    DataType::Binary => array
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .map_or(ArrowValue::Unsupported("binary_error".to_string()), |arr| {
                            ArrowValue::Binary(arr.value(row_idx).to_vec())
                        }),
                    DataType::Dictionary(_, _) => {
                        // Handle dictionary encoded columns (like tags)
                        let dict_array = as_dictionary_array::<Int32Type>(array);
                        if let Some(key) = dict_array.key(row_idx) {
                            let values = as_string_array(dict_array.values());
                            ArrowValue::String(values.value(key).to_string())
                        } else {
                            ArrowValue::Null
                        }
                    }
                    dt => {
                        // For unsupported types, store the type name
                        ArrowValue::Unsupported(format!("{dt:?}"))
                    }
                }
            } else {
                ArrowValue::Null
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
