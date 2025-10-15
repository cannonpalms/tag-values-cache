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

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::rc::Rc;

use arrow::array::{
    as_dictionary_array, as_primitive_array, as_string_array, RecordBatch,
};
use arrow::datatypes::{Int32Type, TimestampNanosecondType};

pub mod compare;
pub mod interavl_cache;
pub mod interval_tree;
pub mod vec_cache;

// Re-export the implementations for convenience
pub use interavl_cache::InteravlCache;
pub use interval_tree::IntervalTreeCache;
pub use vec_cache::VecCache;

/// The type used for timestamps (nanoseconds since epoch)
pub type Timestamp = u64;

/// Errors that can occur when building an interval cache
#[derive(thiserror::Error, Debug)]
pub enum CacheBuildError {
    /// Occurs when trying to create an interval [t, t+1) where t == u64::MAX
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
    pub fn into_inner(self) -> Vec<(Timestamp, V)> {
        self.points
    }
}

impl<V> SortedData<V>
where
    V: Clone + Ord,
{
    /// Create a new SortedData from unsorted input.
    /// This will sort the data by timestamp, then by value.
    pub fn from_unsorted(mut points: Vec<(Timestamp, V)>) -> Self {
        points.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        Self { points }
    }

    /// Create SortedData from pre-sorted input.
    ///
    /// # Safety
    /// The caller must guarantee that `points` is sorted by timestamp
    /// first, then by value in ascending order.
    ///
    /// In debug builds, this will verify the sorting assumption.
    pub fn from_sorted(points: Vec<(Timestamp, V)>) -> Self {
        #[cfg(debug_assertions)]
        {
            for window in points.windows(2) {
                assert!(
                    window[0].0 < window[1].0 ||
                    (window[0].0 == window[1].0 && window[0].1 <= window[1].1),
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
    /// A vector of references to all values at the given timestamp.
    /// Returns an empty vector if no values exist at that timestamp.
    fn query_point(&self, t: Timestamp) -> Vec<&V>;

    /// Query for all values that exist within a time range.
    ///
    /// Returns all unique values whose intervals overlap with the given range.
    ///
    /// # Arguments
    /// * `range` - The time range to query [start, end)
    ///
    /// # Returns
    /// A vector of references to all unique values within the range.
    /// Returns an empty vector if no values exist in the range.
    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V>;

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
    pub fn new(data: Vec<(Timestamp, V)>) -> Self {
        Self { data }
    }

    /// Build an IntervalTreeCache from the data
    pub fn build_interval_tree(self) -> Result<IntervalTreeCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        IntervalTreeCache::new(self.data)
    }

    /// Build a VecCache from the data
    pub fn build_vec_cache(self) -> Result<VecCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        VecCache::new(self.data)
    }

    /// Build an InteravlCache (AVL-based) from the data
    pub fn build_interavl_cache(self) -> Result<InteravlCache<V>, CacheBuildError>
    where
        V: Ord,
    {
        InteravlCache::new(self.data)
    }
}

/// A row of data from a RecordBatch, excluding the time column.
/// This represents all column values for a specific timestamp.
#[derive(Clone, Debug)]
pub struct RecordBatchRow {
    /// Column names and their values as strings (for comparison)
    pub values: BTreeMap<String, String>,
}

impl RecordBatchRow {
    /// Create a new RecordBatchRow from column values
    pub fn new(values: BTreeMap<String, String>) -> Self {
        Self { values }
    }
}

impl fmt::Display for RecordBatchRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.values
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
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

/// Extract timestamp and row data from a RecordBatch.
///
/// This function processes a RecordBatch and extracts timestamp-value pairs
/// where the value contains all non-time columns for that row.
///
/// # Returns
/// A vector of (timestamp, RecordBatchRow) pairs where RecordBatchRow
/// contains all column data except the timestamp.
pub fn extract_rows_from_batch(batch: RecordBatch) -> Vec<(Timestamp, RecordBatchRow)> {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType;

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
            if field.name().to_lowercase() == "time" ||
               field.name().to_lowercase() == "timestamp" {
                ts_idx = Some(idx);
            } else {
                non_time_columns.push((idx, field.name().clone()));
            }
        }
    }

    let ts_idx = ts_idx.unwrap_or(0);
    let timestamps = as_primitive_array::<TimestampNanosecondType>(batch.column(ts_idx));

    // Build results
    let mut results = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let ts = timestamps.value(row_idx) as u64;

        // Collect all non-time column values for this row
        let mut row_values = BTreeMap::new();

        for &(col_idx, ref col_name) in &non_time_columns {
            let array = batch.column(col_idx);

            // Extract value as string for this row
            let value_str = if !array.is_valid(row_idx) {
                "null".to_string()
            } else {
                match array.data_type() {
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
                    DataType::Utf8 => {
                        array.as_any()
                            .downcast_ref::<StringArray>()
                            .map(|arr| arr.value(row_idx).to_string())
                            .unwrap_or_else(|| "string_error".to_string())
                    }
                    DataType::Int64 => {
                        array.as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .map(|arr| arr.value(row_idx).to_string())
                            .unwrap_or_else(|| "int64_error".to_string())
                    }
                    DataType::Float64 => {
                        array.as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .map(|arr| arr.value(row_idx).to_string())
                            .unwrap_or_else(|| "float64_error".to_string())
                    }
                    DataType::Boolean => {
                        array.as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .map(|arr| arr.value(row_idx).to_string())
                            .unwrap_or_else(|| "bool_error".to_string())
                    }
                    _ => {
                        // For other types, try to get string representation
                        format!("{:?}", array.data_type())
                    }
                }
            };

            row_values.insert(col_name.to_string(), value_str);
        }

        let row = RecordBatchRow::new(row_values);
        results.push((ts, row));
    }

    results
}

/// Process multiple RecordBatches into sorted data with RecordBatchRow values.
///
/// This function processes all batches and returns them as SortedData
/// ready for cache construction with full column data preserved.
pub fn record_batches_to_row_data(
    batches: impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>
) -> Result<SortedData<RecordBatchRow>, Box<dyn std::error::Error>> {
    let mut all_points = Vec::new();

    for batch in batches {
        let batch = batch?;
        let points = extract_rows_from_batch(batch);
        all_points.extend(points);
    }

    Ok(SortedData::from_unsorted(all_points))
}

/// Extract timestamp and tag data from a RecordBatch.
///
/// This function processes a RecordBatch from a parquet file and extracts
/// timestamp-value pairs where the value is a serialized representation
/// of all tag columns for that row.
///
/// # Returns
/// A vector of (timestamp, tag_string) pairs where tag_string contains
/// all tags concatenated with their values.
pub fn extract_time_series_from_batch(batch: RecordBatch) -> Vec<(Timestamp, String)> {
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
            schema.fields().iter().position(|f| {
                f.name().to_lowercase() == "time" ||
                f.name().to_lowercase() == "timestamp"
            }).unwrap_or(0)
        }
    };

    let timestamps = as_primitive_array::<TimestampNanosecondType>(batch.column(ts_idx));

    // Collect tag arrays
    let tag_arrays: BTreeMap<String, _> = tag_indices
        .iter()
        .map(|(name, idx)| {
            (name.clone(), as_dictionary_array::<Int32Type>(batch.column(*idx)))
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
            if let Some(idx) = arr.key(row) {
                if let Some(vals) = tag_values.get(name) {
                    if let Some(val) = vals.get(idx) {
                        if !val.is_empty() {
                            tag_string_parts.push(format!("{}={}", name, val));
                        }
                    }
                }
            }
        }

        // Create composite tag string
        let tag_string = tag_string_parts.join(",");
        results.push((ts, tag_string));
    }

    results
}

/// Process multiple RecordBatches from a parquet reader into sorted data.
///
/// This is a convenience function that processes all batches from a reader
/// and returns them as SortedData ready for cache construction.
pub fn record_batches_to_sorted_data(
    batches: impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>
) -> Result<SortedData<String>, Box<dyn std::error::Error>> {
    let mut all_points = Vec::new();

    for batch in batches {
        let batch = batch?;
        let points = extract_time_series_from_batch(batch);
        all_points.extend(points);
    }

    Ok(SortedData::from_unsorted(all_points))
}