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

use std::ops::Range;

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