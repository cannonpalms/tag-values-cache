//! A library for building time-interval caches from discrete timestamp-value pairs.

use std::ops::Range;

pub mod interavl_cache;
pub mod interval_tree;
pub mod vec_cache;

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

/// The main trait for interval-based caches.
///
/// Implementations of this trait build interval structures from discrete
/// timestamp-value pairs, where consecutive timestamps with identical values
/// are merged into continuous intervals.
pub trait IntervalCache<V>: Sized
where
    V: Clone + Eq + std::hash::Hash,
{
    /// Create a new cache from a vector of (timestamp, value) pairs.
    fn new(points: Vec<(Timestamp, V)>) -> Result<Self, CacheBuildError>;

    /// Query for all values that exist at a specific timestamp.
    fn query_point(&self, t: Timestamp) -> Vec<&V>;

    /// Query for all values that exist within a time range.
    fn query_range(&self, range: Range<Timestamp>) -> Vec<&V>;

    /// Append a batch of new timestamp-value pairs to the cache.
    ///
    /// This method adds new data points to the existing cache, potentially
    /// merging with existing intervals if the new data is consecutive with
    /// existing intervals for the same value.
    fn append_batch(&mut self, new_points: Vec<(Timestamp, V)>) -> Result<(), CacheBuildError>;
}