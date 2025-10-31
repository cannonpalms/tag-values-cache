//! Streaming builders for constructing caches incrementally from record batch streams.
//!
//! This module provides builders that can construct `ValueAwareLapperCache` and
//! `BitmapLapperCache` instances from streams of record batches, avoiding the need
//! to load all data into memory at once.
//!
//! # Example
//! ```ignore
//! use tag_values_cache::streaming::{ChunkedStreamBuilder, BitmapChunkedStreamBuilder};
//! use std::time::Duration;
//!
//! // ValueAwareLapperCache builder
//! let mut builder = ChunkedStreamBuilder::new(
//!     Duration::from_secs(3600), // 1 hour resolution
//!     1_000_000,                  // 1M points per chunk
//! );
//!
//! // BitmapLapperCache builder (often more efficient)
//! let mut bitmap_builder = BitmapChunkedStreamBuilder::new(
//!     Duration::from_secs(3600),
//!     1_000_000,
//! );
//!
//! // Process stream of batches
//! for batch in record_batch_stream {
//!     builder.process_batch(&batch)?;
//!     bitmap_builder.process_batch(&batch)?;
//! }
//!
//! // Finalize and get the caches
//! let cache = builder.finalize()?;
//! let bitmap_cache = bitmap_builder.finalize()?;
//! ```

use arrow::array::RecordBatch;
use arrow_util::dictionary::StringDictionary;
use indexmap::IndexSet;
use std::time::Duration;

use crate::bitmap_lapper_cache::BitmapLapperCache;
use crate::value_aware_lapper_cache::ValueAwareLapperCache;
use crate::{CacheBuildError, EncodedTagSet, TagSet, extract_tags_from_batch};

use arrow::error::ArrowError;
use futures::StreamExt;
use futures::stream::BoxStream;

/// Type alias for a sendable stream of record batches.
/// This matches DataFusion's `SendableRecordBatchStream` but uses our Arrow version.
pub type SendableRecordBatchStream = BoxStream<'static, Result<RecordBatch, ArrowError>>;

/// A builder that constructs a cache by processing data in chunks.
///
/// This builder accumulates points into a buffer and flushes them to the cache
/// when the buffer reaches a configured size. This allows processing datasets
/// larger than available memory by trading some performance for memory efficiency.
///
/// # Memory Usage
///
/// Memory usage is O(chunk_size + cache_size) instead of O(full_dataset + cache_size).
/// The chunk_size determines the trade-off between memory usage and performance:
///
/// - Smaller chunks: Lower memory, more overhead from multiple sorts/appends
/// - Larger chunks: Higher memory, less overhead
///
/// Recommended default: 1M points (~200MB) provides a good balance.
pub struct ChunkedStreamBuilder {
    /// The cache being built (None until first chunk is processed)
    cache: Option<ValueAwareLapperCache>,

    /// Buffer for accumulating points before flushing
    chunk_buffer: Vec<(u64, TagSet)>,

    /// Maximum number of points to buffer before flushing
    chunk_size: usize,

    /// Time resolution for bucketing timestamps
    resolution: Duration,

    /// Statistics for monitoring
    total_points_processed: usize,
    chunks_flushed: usize,
}

impl ChunkedStreamBuilder {
    /// Creates a new chunked stream builder.
    ///
    /// # Arguments
    ///
    /// * `resolution` - Time resolution for bucketing timestamps
    /// * `chunk_size` - Number of points to buffer before flushing to cache
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    /// use tag_values_cache::streaming::ChunkedStreamBuilder;
    ///
    /// // 1 hour resolution, 1M point chunks
    /// let builder = ChunkedStreamBuilder::new(
    ///     Duration::from_secs(3600),
    ///     1_000_000,
    /// );
    /// ```
    pub fn new(resolution: Duration, chunk_size: usize) -> Self {
        Self {
            cache: None,
            chunk_buffer: Vec::with_capacity(chunk_size),
            chunk_size,
            resolution,
            total_points_processed: 0,
            chunks_flushed: 0,
        }
    }

    /// Returns the number of points currently buffered.
    pub fn buffered_count(&self) -> usize {
        self.chunk_buffer.len()
    }

    /// Returns the total number of points processed so far.
    pub fn total_points(&self) -> usize {
        self.total_points_processed
    }

    /// Returns the number of chunks flushed so far.
    pub fn chunks_flushed(&self) -> usize {
        self.chunks_flushed
    }

    /// Processes a record batch, extracting tag data and adding to buffer.
    ///
    /// If the buffer reaches the configured chunk size, it will be automatically
    /// flushed to the cache.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The batch cannot be processed (missing columns, wrong types)
    /// - Cache building fails during flush
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<(), CacheBuildError> {
        let points = extract_tags_from_batch(batch);
        self.process_points(points)
    }

    /// Processes a vector of (timestamp, TagSet) points directly.
    ///
    /// If the buffer reaches the configured chunk size, it will be automatically
    /// flushed to the cache.
    ///
    /// # Note
    ///
    /// This method is public primarily for benchmarking and testing purposes.
    /// **Production code should use `process_batch`** to work with Arrow RecordBatch
    /// streams, which is the intended API for this builder.
    ///
    /// # Errors
    ///
    /// Returns an error if cache building fails during flush.
    #[doc(hidden)] // Hide from normal documentation
    pub fn process_points_unchecked(
        &mut self,
        points: Vec<(u64, TagSet)>,
    ) -> Result<(), CacheBuildError> {
        self.process_points(points)
    }

    /// Internal implementation of process_points
    fn process_points(&mut self, points: Vec<(u64, TagSet)>) -> Result<(), CacheBuildError> {
        let num_points = points.len();
        self.chunk_buffer.extend(points);
        self.total_points_processed += num_points;

        // Check if we need to flush
        // Note: This flushes the entire buffer, not just chunk_size elements
        if self.chunk_buffer.len() >= self.chunk_size {
            self.flush_chunk()?;
        }

        Ok(())
    }

    /// Flushes the current buffer to the cache.
    ///
    /// This is called automatically when the buffer reaches chunk_size, but can
    /// also be called manually to flush partial chunks.
    ///
    /// Does nothing if the buffer is empty.
    pub fn flush_chunk(&mut self) -> Result<(), CacheBuildError> {
        if self.chunk_buffer.is_empty() {
            return Ok(());
        }

        // Take ownership of the buffer and replace with a new one
        let chunk_data =
            std::mem::replace(&mut self.chunk_buffer, Vec::with_capacity(self.chunk_size));

        match &mut self.cache {
            None => {
                // First chunk - create the initial cache
                self.cache = Some(ValueAwareLapperCache::from_unsorted_with_resolution(
                    chunk_data,
                    self.resolution,
                )?);
            }
            Some(cache) => {
                // Subsequent chunks - append to existing cache
                cache.append_unsorted(chunk_data)?;
            }
        }

        self.chunks_flushed += 1;

        Ok(())
    }

    /// Finalizes the builder and returns the completed cache.
    ///
    /// This flushes any remaining buffered data and consumes the builder.
    ///
    /// # Errors
    ///
    /// Returns an error if cache building fails during final flush.
    ///
    /// # Panics
    ///
    /// Panics if no data was processed. Use `process_batch` or `process_points`
    /// to add data before calling `finalize`.
    pub fn finalize(mut self) -> Result<ValueAwareLapperCache, CacheBuildError> {
        // Flush any remaining buffered data
        self.flush_chunk()?;

        // Return the cache or panic if no data was processed
        Ok(self.cache.expect("No data was processed - cache is empty"))
    }

    /// Finalizes the builder and returns both the cache and statistics.
    ///
    /// This is useful for monitoring and debugging streaming builds.
    ///
    /// # Errors
    ///
    /// Returns an error if cache building fails.
    ///
    /// # Panics
    ///
    /// Panics if no data was processed.
    pub fn finalize_with_stats(
        mut self,
    ) -> Result<(ValueAwareLapperCache, StreamingStats), CacheBuildError> {
        // Flush remaining data first
        self.flush_chunk()?;

        let stats = StreamingStats {
            total_points: self.total_points_processed,
            chunks_flushed: self.chunks_flushed,
            chunk_size: self.chunk_size,
        };

        let cache = self.cache.expect("No data was processed - cache is empty");

        Ok((cache, stats))
    }

    /// Processes all batches from a `SendableRecordBatchStream`.
    ///
    /// This method consumes the entire stream, processing each batch as it arrives.
    /// Memory usage remains bounded at O(current_batch + chunk_size) regardless of
    /// the total stream size.
    ///
    /// # Example
    /// ```ignore
    /// use arrow::record_batch::SendableRecordBatchStream;
    ///
    /// let stream: SendableRecordBatchStream = get_parquet_stream().await?;
    /// let mut builder = ChunkedStreamBuilder::new(resolution, chunk_size);
    /// builder.process_stream(stream).await?;
    /// let cache = builder.finalize()?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The stream produces an error
    /// - Processing a batch fails
    /// - Cache building fails during chunk flush
    pub async fn process_stream(
        &mut self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<(), CacheBuildError> {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| CacheBuildError::ArrowError(e.to_string()))?;
            self.process_batch(&batch)?;
        }
        Ok(())
    }

    /// Builds a cache directly from a `SendableRecordBatchStream`.
    ///
    /// This is a convenience method that creates a builder, processes the stream,
    /// and finalizes the cache in one call.
    ///
    /// # Example
    /// ```ignore
    /// let cache = ChunkedStreamBuilder::from_stream(
    ///     stream,
    ///     Duration::from_secs(3600),
    ///     1_000_000,
    /// ).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if stream processing or cache building fails.
    pub async fn from_stream(
        stream: SendableRecordBatchStream,
        resolution: Duration,
        chunk_size: usize,
    ) -> Result<ValueAwareLapperCache, CacheBuildError> {
        let mut builder = Self::new(resolution, chunk_size);
        builder.process_stream(stream).await?;
        builder.finalize()
    }
}

/// Statistics about a streaming build operation.
#[derive(Debug, Clone, Copy)]
pub struct StreamingStats {
    /// Total number of points processed
    pub total_points: usize,

    /// Number of chunks flushed
    pub chunks_flushed: usize,

    /// Configured chunk size
    pub chunk_size: usize,
}

impl StreamingStats {
    /// Returns the average chunk size actually processed.
    pub fn avg_chunk_size(&self) -> f64 {
        if self.chunks_flushed == 0 {
            0.0
        } else {
            self.total_points as f64 / self.chunks_flushed as f64
        }
    }
}

impl std::fmt::Display for StreamingStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamingStats {{ total_points: {}, chunks_flushed: {}, chunk_size: {}, avg_chunk_size: {:.1} }}",
            self.total_points,
            self.chunks_flushed,
            self.chunk_size,
            self.avg_chunk_size()
        )
    }
}

/// A builder that constructs a cache from **sorted** data in a single pass.
///
/// This builder is optimized for data that arrives in sorted order. It builds intervals
/// incrementally without buffering or sorting, making it the most memory-efficient and
/// performant option.
///
/// # Requirements
///
/// - **Input must be sorted by (timestamp, tagset)** in ascending order
/// - This matches the sort order used internally by `ValueAwareLapperCache::from_unsorted_with_resolution`
/// - Violating this requirement will produce incorrect results
///
/// # Memory Usage
///
/// Memory usage is O(cardinality) where cardinality is the number of unique tag combinations.
/// Typically 1-10MB for most datasets, regardless of total data size.
///
/// # Performance
///
/// - **Best latency**: Single pass, no sorting
/// - **Lowest memory**: Only tracks open intervals (one per unique tag combination)
/// - **Highest throughput**: Minimal overhead per point
///
/// # Example
///
/// ```ignore
/// use tag_values_cache::streaming::SortedStreamBuilder;
/// use std::time::Duration;
///
/// // Create builder with 1-hour resolution
/// let mut builder = SortedStreamBuilder::new(Duration::from_secs(3600));
///
/// // Process sorted record batches
/// for batch in sorted_record_batch_stream {
///     builder.process_batch(&batch)?;
/// }
///
/// // Finalize and get the cache
/// let cache = builder.finalize()?;
/// ```
pub struct SortedStreamBuilder {
    /// Time resolution for bucketing timestamps
    resolution: Duration,

    /// String dictionary for encoding tag keys and values
    string_dict: StringDictionary<usize>,

    /// Set of encoded TagSets with preserved insertion order (index = ID)
    tagsets: indexmap::IndexSet<crate::EncodedTagSet>,

    /// Currently open intervals: tagset_id -> (start, stop)
    /// Where start is the bucket timestamp and stop is exclusive (next bucket)
    open_intervals: std::collections::HashMap<usize, (u64, u64)>,

    /// Completed intervals that have been closed
    completed_intervals: Vec<rust_lapper::Interval<u64, usize>>,

    /// Last timestamp processed (for detecting out-of-order data)
    last_timestamp: Option<u64>,

    /// Statistics for monitoring
    total_points_processed: usize,
    out_of_order_detected: bool,
}

impl SortedStreamBuilder {
    /// Creates a new sorted stream builder.
    ///
    /// # Arguments
    ///
    /// * `resolution` - Time resolution for bucketing timestamps
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    /// use tag_values_cache::streaming::SortedStreamBuilder;
    ///
    /// // 1 hour resolution
    /// let builder = SortedStreamBuilder::new(Duration::from_secs(3600));
    /// ```
    pub fn new(resolution: Duration) -> Self {
        Self {
            resolution,
            string_dict: StringDictionary::new(),
            tagsets: indexmap::IndexSet::new(),
            open_intervals: std::collections::HashMap::new(),
            completed_intervals: Vec::new(),
            last_timestamp: None,
            total_points_processed: 0,
            out_of_order_detected: false,
        }
    }

    /// Returns the total number of points processed so far.
    pub fn total_points(&self) -> usize {
        self.total_points_processed
    }

    /// Returns true if out-of-order data was detected.
    ///
    /// When this is true, the cache being built may be incorrect.
    /// Consider using `ChunkedStreamBuilder` instead for unsorted data.
    pub fn is_out_of_order(&self) -> bool {
        self.out_of_order_detected
    }

    /// Processes a record batch, extracting tag data and updating intervals.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be processed (missing columns, wrong types).
    ///
    /// # Note
    ///
    /// This method does NOT validate that data is sorted. It is the caller's responsibility
    /// to ensure timestamps are in ascending order. Use `is_out_of_order()` to detect
    /// violations after processing.
    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<(), CacheBuildError> {
        let encoded_points = crate::extract_and_encode_tags_from_batch(
            batch,
            &mut self.string_dict,
            &mut self.tagsets,
        );
        self.process_encoded_points(encoded_points)
    }

    /// Processes a vector of (timestamp, TagSet) points directly.
    ///
    /// # Note
    ///
    /// This method is public primarily for benchmarking and testing purposes.
    /// **Production code should use `process_batch`** to work with Arrow RecordBatch
    /// streams, which is the intended API for this builder.
    ///
    /// # Errors
    ///
    /// Returns an error if interval building fails.
    #[doc(hidden)] // Hide from normal documentation
    pub fn process_points_unchecked(
        &mut self,
        points: Vec<(u64, TagSet)>,
    ) -> Result<(), CacheBuildError> {
        self.process_points(points)
    }

    /// Internal implementation of process_points
    fn process_points(&mut self, points: Vec<(u64, TagSet)>) -> Result<(), CacheBuildError> {
        for (timestamp, tagset) in points {
            self.process_point(timestamp, tagset)?;
        }
        Ok(())
    }

    /// Process already-encoded points
    fn process_encoded_points(&mut self, points: Vec<(u64, usize)>) -> Result<(), CacheBuildError> {
        for (timestamp, tagset_id) in points {
            self.process_encoded_point(timestamp, tagset_id)?;
        }
        Ok(())
    }

    /// Process a single point, updating intervals incrementally.
    fn process_point(&mut self, timestamp: u64, tagset: TagSet) -> Result<(), CacheBuildError> {
        // Encode the tagset
        let encoded_tagset = crate::encode_tagset(&tagset, &mut self.string_dict);
        let (tagset_id, _) = self.tagsets.insert_full(encoded_tagset);

        self.process_encoded_point(timestamp, tagset_id)
    }

    /// Process a single already-encoded point, updating intervals incrementally.
    fn process_encoded_point(
        &mut self,
        timestamp: u64,
        tagset_id: usize,
    ) -> Result<(), CacheBuildError> {
        // Check for out-of-order data
        if let Some(last_ts) = self.last_timestamp
            && timestamp < last_ts
        {
            self.out_of_order_detected = true;
        }
        self.last_timestamp = Some(timestamp);
        self.total_points_processed += 1;

        // Bucket the timestamp according to resolution
        let bucketed_ts = ValueAwareLapperCache::bucket_timestamp(timestamp, self.resolution);

        let next_end = bucketed_ts
            .checked_add(1)
            .ok_or(CacheBuildError::TimestampOverflow(bucketed_ts))?;

        // This matches the reference implementation in ValueAwareLapperCache::build_intervals
        match self.open_intervals.get_mut(&tagset_id) {
            Some((_, end)) if *end == bucketed_ts => {
                // Interval already extends to this bucket - extend it further
                *end = next_end;
            }
            Some((start, end)) => {
                // Gap detected - close old interval and start new one
                let interval = rust_lapper::Interval {
                    start: *start,
                    stop: *end,
                    val: tagset_id,
                };
                self.completed_intervals.push(interval);

                *start = bucketed_ts;
                *end = next_end;
            }
            None => {
                // New tagset - start tracking it
                self.open_intervals
                    .insert(tagset_id, (bucketed_ts, next_end));
            }
        }

        Ok(())
    }

    /// Finalizes the builder and returns the completed cache.
    ///
    /// This closes all open intervals and constructs the final cache.
    ///
    /// # Errors
    ///
    /// Returns an error if cache building fails.
    ///
    /// # Panics
    ///
    /// Panics if no data was processed. Use `process_batch` or `process_points_unchecked`
    /// to add data before calling `finalize`.
    pub fn finalize(mut self) -> Result<ValueAwareLapperCache, CacheBuildError> {
        // Close all open intervals
        for (tagset_id, (start, stop)) in self.open_intervals.drain() {
            let interval = rust_lapper::Interval {
                start,
                stop,
                val: tagset_id,
            };
            self.completed_intervals.push(interval);
        }

        if self.completed_intervals.is_empty() {
            return Err(CacheBuildError::NoData);
        }

        // Build the lapper from the completed intervals
        let mut lapper = crate::value_aware_lapper::ValueAwareLapper::new(self.completed_intervals);

        // Merge adjacent/overlapping intervals with the same value
        // This matches what from_unsorted_with_resolution does
        lapper.merge_with_values();

        // Construct the cache
        Ok(ValueAwareLapperCache::from_parts(
            lapper,
            self.string_dict,
            self.tagsets,
            self.resolution,
        ))
    }

    /// Finalizes the builder and returns both the cache and statistics.
    ///
    /// This is useful for monitoring and debugging streaming builds.
    ///
    /// # Errors
    ///
    /// Returns an error if cache building fails.
    ///
    /// # Panics
    ///
    /// Panics if no data was processed.
    pub fn finalize_with_stats(
        self,
    ) -> Result<(ValueAwareLapperCache, SortedStreamStats), CacheBuildError> {
        let stats = SortedStreamStats {
            total_points: self.total_points_processed,
            unique_tagsets: self.tagsets.len(),
            out_of_order_detected: self.out_of_order_detected,
        };

        let cache = self.finalize()?;

        Ok((cache, stats))
    }

    /// Processes all batches from a `SendableRecordBatchStream`.
    ///
    /// This method consumes the entire stream, processing each batch as it arrives.
    /// For best performance, the stream should provide batches in timestamp order.
    ///
    /// # Example
    /// ```ignore
    /// use arrow::record_batch::SendableRecordBatchStream;
    ///
    /// let stream: SendableRecordBatchStream = get_sorted_parquet_stream().await?;
    /// let mut builder = SortedStreamBuilder::new(resolution);
    /// builder.process_stream(stream).await?;
    /// let cache = builder.finalize()?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The stream produces an error
    /// - Processing a batch fails
    /// - Timestamp ordering violations are detected
    pub async fn process_stream(
        &mut self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<(), CacheBuildError> {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| CacheBuildError::ArrowError(e.to_string()))?;
            self.process_batch(&batch)?;
        }
        Ok(())
    }

    /// Builds a cache directly from a sorted `SendableRecordBatchStream`.
    ///
    /// This is a convenience method that creates a builder, processes the stream,
    /// and finalizes the cache in one call. The stream should provide data in
    /// timestamp order for optimal performance.
    ///
    /// # Example
    /// ```ignore
    /// let cache = SortedStreamBuilder::from_stream(
    ///     sorted_stream,
    ///     Duration::from_secs(3600),
    /// ).await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if stream processing or cache building fails.
    pub async fn from_stream(
        stream: SendableRecordBatchStream,
        resolution: Duration,
    ) -> Result<ValueAwareLapperCache, CacheBuildError> {
        let mut builder = Self::new(resolution);
        builder.process_stream(stream).await?;
        builder.finalize()
    }
}

/// Statistics about a sorted streaming build operation.
#[derive(Debug, Clone, Copy)]
pub struct SortedStreamStats {
    /// Total number of points processed
    pub total_points: usize,

    /// Number of unique tag combinations encountered
    pub unique_tagsets: usize,

    /// Whether out-of-order data was detected
    pub out_of_order_detected: bool,
}

impl std::fmt::Display for SortedStreamStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SortedStreamStats {{ total_points: {}, unique_tagsets: {}, out_of_order: {} }}",
            self.total_points, self.unique_tagsets, self.out_of_order_detected
        )
    }
}

// ============================================================================
// BitmapLapperCache Streaming Builders
// ============================================================================

/// A builder that constructs a `BitmapLapperCache` by processing data in chunks.
///
/// This is identical in behavior to `ChunkedStreamBuilder` but produces a `BitmapLapperCache`
/// instead of `ValueAwareLapperCache`. The bitmap implementation is often more efficient
/// for high-cardinality data.
///
/// See `ChunkedStreamBuilder` documentation for usage details.
pub struct BitmapChunkedStreamBuilder {
    /// The cache being built (None until first chunk is processed)
    cache: Option<BitmapLapperCache>,

    /// Buffer for accumulating points before flushing
    chunk_buffer: Vec<(u64, TagSet)>,

    /// Maximum number of points to buffer before flushing
    chunk_size: usize,

    /// Time resolution for bucketing timestamps
    resolution: Duration,

    /// Statistics for monitoring
    total_points_processed: usize,
    chunks_flushed: usize,
}

impl BitmapChunkedStreamBuilder {
    /// Creates a new bitmap chunked stream builder.
    pub fn new(resolution: Duration, chunk_size: usize) -> Self {
        Self {
            cache: None,
            chunk_buffer: Vec::with_capacity(chunk_size),
            chunk_size,
            resolution,
            total_points_processed: 0,
            chunks_flushed: 0,
        }
    }

    pub fn buffered_count(&self) -> usize {
        self.chunk_buffer.len()
    }

    pub fn total_points(&self) -> usize {
        self.total_points_processed
    }

    pub fn chunks_flushed(&self) -> usize {
        self.chunks_flushed
    }

    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<(), CacheBuildError> {
        let points = extract_tags_from_batch(batch);
        self.process_points(points)
    }

    fn process_points(&mut self, points: Vec<(u64, TagSet)>) -> Result<(), CacheBuildError> {
        let num_points = points.len();
        self.chunk_buffer.extend(points);
        self.total_points_processed += num_points;

        if self.chunk_buffer.len() >= self.chunk_size {
            self.flush_chunk()?;
        }

        Ok(())
    }

    pub fn flush_chunk(&mut self) -> Result<(), CacheBuildError> {
        if self.chunk_buffer.is_empty() {
            return Ok(());
        }

        let chunk_data =
            std::mem::replace(&mut self.chunk_buffer, Vec::with_capacity(self.chunk_size));

        match &mut self.cache {
            None => {
                self.cache = Some(BitmapLapperCache::from_unsorted_with_resolution(
                    chunk_data,
                    self.resolution,
                )?);
            }
            Some(cache) => {
                cache.append_unsorted(chunk_data)?;
            }
        }

        self.chunks_flushed += 1;

        Ok(())
    }

    pub fn finalize(mut self) -> Result<BitmapLapperCache, CacheBuildError> {
        self.flush_chunk()?;
        let cache = self.cache.expect("No data was processed - cache is empty");
        Ok(cache)
    }

    pub fn finalize_with_stats(
        mut self,
    ) -> Result<(BitmapLapperCache, StreamingStats), CacheBuildError> {
        self.flush_chunk()?;

        let stats = StreamingStats {
            total_points: self.total_points_processed,
            chunks_flushed: self.chunks_flushed,
            chunk_size: self.chunk_size,
        };

        let cache = self.cache.expect("No data was processed - cache is empty");

        Ok((cache, stats))
    }

    pub async fn process_stream(
        &mut self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<(), CacheBuildError> {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| CacheBuildError::ArrowError(e.to_string()))?;
            self.process_batch(&batch)?;
        }
        Ok(())
    }

    pub async fn from_stream(
        stream: SendableRecordBatchStream,
        resolution: Duration,
        chunk_size: usize,
    ) -> Result<BitmapLapperCache, CacheBuildError> {
        let mut builder = Self::new(resolution, chunk_size);
        builder.process_stream(stream).await?;
        builder.finalize()
    }
}

/// A builder that constructs a `BitmapLapperCache` from **sorted** data in a single pass.
///
/// This builder uses the bitmap approach which groups tagsets by time bucket rather than
/// tracking individual tagset intervals. This is significantly more memory efficient for
/// high-cardinality data.
///
/// See `SortedStreamBuilder` documentation for usage details.
pub struct BitmapSortedStreamBuilder {
    /// Time resolution for bucketing timestamps
    resolution: Duration,

    /// String dictionary for encoding tag keys and values
    string_dict: StringDictionary<usize>,

    /// Vector of encoded TagSets (index = ID)
    tagsets: IndexSet<EncodedTagSet>,

    /// Buckets: bucketed_timestamp -> RoaringBitmap of tagset IDs
    /// We don't need to track open/closed - just accumulate all buckets and
    /// convert to intervals at the end. Lapper will handle any merging.
    buckets: std::collections::BTreeMap<u64, roaring::RoaringBitmap>,

    /// Last timestamp processed (for detecting out-of-order data)
    last_timestamp: Option<u64>,

    /// Statistics
    total_points_processed: usize,
    out_of_order_detected: bool,
}

impl BitmapSortedStreamBuilder {
    /// Creates a new bitmap sorted stream builder.
    pub fn new(resolution: Duration) -> Self {
        Self {
            resolution,
            string_dict: StringDictionary::new(),
            tagsets: IndexSet::new(),
            buckets: std::collections::BTreeMap::new(),
            last_timestamp: None,
            total_points_processed: 0,
            out_of_order_detected: false,
        }
    }

    pub fn total_points(&self) -> usize {
        self.total_points_processed
    }

    pub fn is_out_of_order(&self) -> bool {
        self.out_of_order_detected
    }

    pub fn process_batch(&mut self, batch: &RecordBatch) -> Result<(), CacheBuildError> {
        let encoded_points = crate::extract_and_encode_tags_from_batch(
            batch,
            &mut self.string_dict,
            &mut self.tagsets,
        );
        self.process_encoded_points(encoded_points)
    }

    fn process_encoded_points(&mut self, points: Vec<(u64, usize)>) -> Result<(), CacheBuildError> {
        for (timestamp, tagset_id) in points {
            self.process_encoded_point(timestamp, tagset_id)?;
        }
        Ok(())
    }

    #[inline]
    fn process_encoded_point(
        &mut self,
        timestamp: u64,
        tagset_id: usize,
    ) -> Result<(), CacheBuildError> {
        // Check for out-of-order data
        if let Some(last_ts) = self.last_timestamp
            && timestamp < last_ts
        {
            self.out_of_order_detected = true;
        }
        self.last_timestamp = Some(timestamp);
        self.total_points_processed += 1;

        // Bucket the timestamp
        let bucketed_ts = BitmapLapperCache::bucket_timestamp(timestamp, self.resolution);

        // Add tagset to bucket - no need to close old buckets, we'll convert
        // all buckets to intervals at finalize time
        self.buckets
            .entry(bucketed_ts)
            .or_default()
            .insert(tagset_id as u32);

        Ok(())
    }

    pub fn finalize(self) -> Result<BitmapLapperCache, CacheBuildError> {
        if self.buckets.is_empty() {
            return Err(CacheBuildError::NoData);
        }

        // Convert all buckets to intervals
        let intervals: Vec<
            rust_lapper::Interval<u64, crate::bitmap_lapper_cache::EqRoaringBitmap>,
        > = self
            .buckets
            .into_iter()
            .map(|(bucket_ts, bitmap)| {
                let stop = bucket_ts
                    .checked_add(1)
                    .expect("timestamp overflow when creating interval");
                rust_lapper::Interval {
                    start: bucket_ts,
                    stop,
                    val: bitmap.into(),
                }
            })
            .collect();

        // Build the lapper
        let lapper = rust_lapper::Lapper::new(intervals);

        // Construct the cache
        Ok(BitmapLapperCache::from_parts(
            lapper,
            self.string_dict,
            self.tagsets,
            self.resolution,
        ))
    }

    pub fn finalize_with_stats(
        self,
    ) -> Result<(BitmapLapperCache, SortedStreamStats), CacheBuildError> {
        let stats = SortedStreamStats {
            total_points: self.total_points_processed,
            unique_tagsets: self.tagsets.len(),
            out_of_order_detected: self.out_of_order_detected,
        };

        let cache = self.finalize()?;

        Ok((cache, stats))
    }

    pub async fn process_stream(
        &mut self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<(), CacheBuildError> {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| CacheBuildError::ArrowError(e.to_string()))?;
            self.process_batch(&batch)?;
        }
        Ok(())
    }

    pub async fn from_stream(
        stream: SendableRecordBatchStream,
        resolution: Duration,
    ) -> Result<BitmapLapperCache, CacheBuildError> {
        let mut builder = Self::new(resolution);
        builder.process_stream(stream).await?;
        builder.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IntervalCache;
    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn make_tagset(tags: &[(&str, &str)]) -> TagSet {
        tags.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeSet<_>>()
    }

    /// Helper to create a RecordBatch from test data for streaming tests.
    /// This simulates what would come from a real Arrow/Parquet stream with
    /// proper IOx column type metadata.
    fn create_record_batch(data: &[(u64, TagSet)]) -> RecordBatch {
        use std::collections::HashMap;

        if data.is_empty() {
            // Return empty batch with schema
            let mut metadata = HashMap::new();
            metadata.insert(
                "iox::column::type".to_string(),
                "iox::column_type::timestamp".to_string(),
            );
            let schema = Schema::new(vec![
                Field::new("time", DataType::Int64, false).with_metadata(metadata),
            ]);
            return RecordBatch::new_empty(Arc::new(schema));
        }

        // Extract timestamps
        let timestamps: Vec<i64> = data.iter().map(|(ts, _)| *ts as i64).collect();
        let time_array = Int64Array::from(timestamps);

        // Collect all unique tag keys
        let mut tag_keys = BTreeSet::new();
        for (_, tagset) in data {
            for (key, _) in tagset {
                tag_keys.insert(key.as_str());
            }
        }

        // Build schema with IOx metadata
        let mut fields = Vec::new();

        // Add timestamp field with metadata
        let mut ts_metadata = HashMap::new();
        ts_metadata.insert(
            "iox::column::type".to_string(),
            "iox::column_type::timestamp".to_string(),
        );
        fields.push(Field::new("time", DataType::Int64, false).with_metadata(ts_metadata));

        // Add tag fields with metadata
        for key in &tag_keys {
            let mut tag_metadata = HashMap::new();
            tag_metadata.insert(
                "iox::column::type".to_string(),
                "iox::column_type::tag".to_string(),
            );
            fields.push(Field::new(*key, DataType::Utf8, true).with_metadata(tag_metadata));
        }

        let schema = Arc::new(Schema::new(fields));

        // Build columns
        let mut columns: Vec<ArrayRef> = vec![Arc::new(time_array)];

        for key in &tag_keys {
            let values: Vec<Option<String>> = data
                .iter()
                .map(|(_, tagset)| {
                    tagset
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                })
                .collect();
            let array = StringArray::from(values);
            columns.push(Arc::new(array));
        }

        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn test_record_batch_with_iox_metadata() {
        // Test that RecordBatch helper creates proper IOx metadata
        let data = vec![
            (1, make_tagset(&[("host", "a"), ("region", "us-east")])),
            (2, make_tagset(&[("host", "b"), ("region", "us-west")])),
        ];

        let batch = create_record_batch(&data);
        let schema = batch.schema_ref();

        // Verify timestamp field has correct metadata
        let time_field = schema.field(0);
        assert_eq!(time_field.name(), "time");
        assert_eq!(
            time_field.metadata().get("iox::column::type"),
            Some(&"iox::column_type::timestamp".to_string())
        );

        // Verify tag fields have correct metadata
        for field in schema.fields().iter().skip(1) {
            assert_eq!(
                field.metadata().get("iox::column::type"),
                Some(&"iox::column_type::tag".to_string()),
                "Field {} should have tag metadata",
                field.name()
            );
        }

        // Verify extract_tags_from_batch correctly reads the metadata
        let extracted = crate::extract_tags_from_batch(&batch);
        assert_eq!(extracted.len(), 2);
        assert_eq!(extracted[0].0, 1);
        assert_eq!(extracted[1].0, 2);

        // Verify both tags are extracted
        assert_eq!(extracted[0].1.len(), 2);
        assert!(
            extracted[0]
                .1
                .contains(&("host".to_string(), "a".to_string()))
        );
        assert!(
            extracted[0]
                .1
                .contains(&("region".to_string(), "us-east".to_string()))
        );
    }

    #[test]
    fn test_record_batch_metadata_filters_non_tags() {
        use std::collections::HashMap;

        // Manually create a batch with a field column (not a tag) to ensure it's filtered
        let timestamps: Vec<i64> = vec![1, 2];
        let time_array = Int64Array::from(timestamps);

        let tags: Vec<Option<String>> = vec![Some("a".to_string()), Some("b".to_string())];
        let tag_array = StringArray::from(tags);

        let fields_data: Vec<f64> = vec![1.5, 2.5];
        let field_array = arrow::array::Float64Array::from(fields_data);

        // Create schema with mixed column types
        let mut ts_metadata = HashMap::new();
        ts_metadata.insert(
            "iox::column::type".to_string(),
            "iox::column_type::timestamp".to_string(),
        );

        let mut tag_metadata = HashMap::new();
        tag_metadata.insert(
            "iox::column::type".to_string(),
            "iox::column_type::tag".to_string(),
        );

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            "iox::column::type".to_string(),
            "iox::column_type::field::float".to_string(),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false).with_metadata(ts_metadata),
            Field::new("host", DataType::Utf8, true).with_metadata(tag_metadata),
            Field::new("temperature", DataType::Float64, true).with_metadata(field_metadata),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(time_array),
                Arc::new(tag_array),
                Arc::new(field_array),
            ],
        )
        .unwrap();

        // Extract and verify only tags are included
        let extracted = crate::extract_tags_from_batch(&batch);
        assert_eq!(extracted.len(), 2);

        // Should only have the "host" tag, not the "temperature" field
        assert_eq!(extracted[0].1.len(), 1);
        assert!(
            extracted[0]
                .1
                .contains(&("host".to_string(), "a".to_string()))
        );
        assert!(!extracted[0].1.iter().any(|(k, _)| k == "temperature"));
    }

    #[test]
    fn test_chunked_builder_single_chunk() {
        let resolution = Duration::from_secs(1);
        let mut builder = ChunkedStreamBuilder::new(resolution, 10);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "b")])),
        ];

        builder.process_points(points).unwrap();
        assert_eq!(builder.buffered_count(), 3);
        assert_eq!(builder.chunks_flushed(), 0);

        let cache = builder.finalize().unwrap();

        // Verify cache works correctly - should have results at timestamps 1-3
        let results1 = cache.query_point(1);
        assert!(!results1.is_empty(), "Should have results at timestamp 1");

        let results2 = cache.query_point(2);
        assert!(!results2.is_empty(), "Should have results at timestamp 2");

        let results3 = cache.query_point(3);
        assert!(!results3.is_empty(), "Should have results at timestamp 3");
    }

    #[test]
    fn test_chunked_builder_multiple_chunks() {
        let resolution = Duration::from_secs(1);
        let mut builder = ChunkedStreamBuilder::new(resolution, 5);

        // First chunk (will auto-flush at 5 points)
        let points1 = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (4, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "a")])),
        ];

        builder.process_points(points1).unwrap();
        assert_eq!(builder.buffered_count(), 0); // Flushed
        assert_eq!(builder.chunks_flushed(), 1);

        // Second chunk (partial)
        let points2 = vec![
            (6, make_tagset(&[("host", "b")])),
            (7, make_tagset(&[("host", "b")])),
        ];

        builder.process_points(points2).unwrap();
        assert_eq!(builder.buffered_count(), 2); // Not flushed yet
        assert_eq!(builder.chunks_flushed(), 1);

        let cache = builder.finalize().unwrap();

        // Verify cache has data from both chunks
        let results1 = cache.query_point(1);
        assert!(!results1.is_empty(), "Should have results from first chunk");

        let results2 = cache.query_point(6);
        assert!(
            !results2.is_empty(),
            "Should have results from second chunk"
        );
    }

    #[test]
    fn test_chunked_builder_with_stats() {
        let resolution = Duration::from_secs(1);
        let mut builder = ChunkedStreamBuilder::new(resolution, 3);

        // Add points in smaller batches to test chunking behavior
        let points1 = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
        ];
        builder.process_points(points1).unwrap();
        assert_eq!(
            builder.chunks_flushed(),
            1,
            "Should have auto-flushed first chunk"
        );
        assert_eq!(
            builder.buffered_count(),
            0,
            "Buffer should be empty after flush"
        );

        let points2 = vec![
            (4, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "a")])),
        ];
        builder.process_points(points2).unwrap();
        assert_eq!(
            builder.chunks_flushed(),
            1,
            "Should still have only 1 chunk flushed"
        );
        assert_eq!(builder.buffered_count(), 2, "Should have 2 points buffered");

        let (_cache, stats) = builder.finalize_with_stats().unwrap();

        assert_eq!(stats.total_points, 5);
        // After finalize_with_stats flushes remaining data:
        // - First 3 points triggered auto-flush (chunk 1)
        // - Remaining 2 points flushed by finalize_with_stats (chunk 2)
        assert_eq!(stats.chunks_flushed, 2);
        assert_eq!(stats.chunk_size, 3);
        assert_eq!(stats.avg_chunk_size(), 2.5); // (3 + 2) / 2
    }

    #[test]
    #[should_panic(expected = "No data was processed - cache is empty")]
    fn test_empty_builder() {
        let resolution = Duration::from_secs(1);
        let builder = ChunkedStreamBuilder::new(resolution, 10);

        let _cache = builder.finalize().unwrap();
    }

    #[test]
    fn test_manual_flush() {
        let resolution = Duration::from_secs(1);
        let mut builder = ChunkedStreamBuilder::new(resolution, 100);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
        ];

        builder.process_points(points).unwrap();
        assert_eq!(builder.buffered_count(), 2);
        assert_eq!(builder.chunks_flushed(), 0);

        // Manual flush
        builder.flush_chunk().unwrap();
        assert_eq!(builder.buffered_count(), 0);
        assert_eq!(builder.chunks_flushed(), 1);
    }

    // Helper function to normalize query results for comparison
    // Results are sets, so order doesn't matter
    fn normalize_results<'a>(
        mut results: Vec<Vec<(&'a str, &'a str)>>,
    ) -> Vec<Vec<(&'a str, &'a str)>> {
        for tagset in results.iter_mut() {
            tagset.sort();
        }
        results.sort();
        results
    }

    #[test]
    fn test_streaming_equals_batch_simple() {
        let resolution = Duration::from_secs(1);
        let chunk_size = 3;

        let data = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "b")])),
            (6, make_tagset(&[("host", "b")])),
            (10, make_tagset(&[("host", "a")])),
        ];

        // Build with streaming
        let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

        // Assert they produce identical results
        assert_eq!(
            streaming_cache.interval_count(),
            batch_cache.interval_count(),
            "Interval counts must match"
        );

        assert_eq!(
            streaming_cache.size_bytes(),
            batch_cache.size_bytes(),
            "Cache sizes must match"
        );

        // Query several points and verify results match
        for timestamp in [1, 2, 3, 4, 5, 6, 7, 10, 15] {
            let streaming_results = normalize_results(streaming_cache.query_point(timestamp));
            let batch_results = normalize_results(batch_cache.query_point(timestamp));

            assert_eq!(
                streaming_results, batch_results,
                "Query results must match for timestamp {}",
                timestamp
            );
        }
    }

    #[test]
    fn test_streaming_equals_batch_large() {
        let resolution = Duration::from_secs(60); // 1 minute
        let chunk_size = 100;

        // Generate larger dataset with multiple tags and varied patterns
        let mut data = Vec::new();

        // Pattern 1: Continuous run of host=a
        for t in 1..=200 {
            data.push((t, make_tagset(&[("host", "a"), ("region", "us-east")])));
        }

        // Pattern 2: Interleaved hosts
        for t in 201..=400 {
            let host = if t % 2 == 0 { "b" } else { "c" };
            data.push((t, make_tagset(&[("host", host), ("region", "us-west")])));
        }

        // Pattern 3: Gaps in data
        for t in (500..=700).step_by(10) {
            data.push((t, make_tagset(&[("host", "d"), ("region", "eu-west")])));
        }

        // Pattern 4: High cardinality burst
        for t in 800..=1000 {
            let host = format!("host-{}", t % 20);
            data.push((t, make_tagset(&[("host", &host), ("region", "ap-south")])));
        }

        // Build with streaming (multiple chunks)
        let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

        // Assert they produce identical results
        assert_eq!(
            streaming_cache.interval_count(),
            batch_cache.interval_count(),
            "Interval counts must match"
        );

        assert_eq!(
            streaming_cache.size_bytes(),
            batch_cache.size_bytes(),
            "Cache sizes must match"
        );

        // Query full range and verify
        use std::ops::Range;
        let query_range = Range {
            start: 1,
            end: 1001,
        };
        let streaming_results = normalize_results(streaming_cache.query_range(&query_range));
        let batch_results = normalize_results(batch_cache.query_range(&query_range));

        assert_eq!(
            streaming_results, batch_results,
            "Range query results must match"
        );

        // Spot check various points
        for timestamp in [1, 50, 150, 250, 350, 500, 650, 850, 950, 1000] {
            let streaming_results = normalize_results(streaming_cache.query_point(timestamp));
            let batch_results = normalize_results(batch_cache.query_point(timestamp));

            assert_eq!(
                streaming_results, batch_results,
                "Query results must match for timestamp {}",
                timestamp
            );
        }
    }

    #[test]
    fn test_streaming_equals_batch_different_chunk_sizes() {
        let resolution = Duration::from_secs(5);

        // Generate test data
        let mut data = Vec::new();
        for t in 1..=300 {
            let host = format!("host-{}", t % 10);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Build batch cache (reference)
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

        // Test different chunk sizes
        let chunk_sizes = [10, 50, 100, 299, 300, 500];

        for chunk_size in chunk_sizes {
            let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
            streaming_builder.process_points(data.clone()).unwrap();
            let streaming_cache = streaming_builder.finalize().unwrap();

            assert_eq!(
                streaming_cache.interval_count(),
                batch_cache.interval_count(),
                "Interval count mismatch with chunk_size={}",
                chunk_size
            );

            assert_eq!(
                streaming_cache.size_bytes(),
                batch_cache.size_bytes(),
                "Size mismatch with chunk_size={}",
                chunk_size
            );

            // Spot check
            for timestamp in [1, 150, 300] {
                let streaming_results = normalize_results(streaming_cache.query_point(timestamp));
                let batch_results = normalize_results(batch_cache.query_point(timestamp));

                assert_eq!(
                    streaming_results, batch_results,
                    "Query mismatch at timestamp {} with chunk_size={}",
                    timestamp, chunk_size
                );
            }
        }
    }

    #[test]
    fn test_streaming_equals_batch_different_resolutions() {
        // Generate test data
        let mut data = Vec::new();
        for t in 1..=1000 {
            let host = if t % 3 == 0 {
                "a"
            } else if t % 3 == 1 {
                "b"
            } else {
                "c"
            };
            data.push((t, make_tagset(&[("host", host)])));
        }

        let chunk_size = 100;

        // Test different resolutions
        let resolutions = [
            Duration::from_nanos(1),
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_secs(60),
            Duration::from_secs(300),
        ];

        for resolution in resolutions {
            let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
            streaming_builder.process_points(data.clone()).unwrap();
            let streaming_cache = streaming_builder.finalize().unwrap();

            let batch_cache =
                ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                    .unwrap();

            assert_eq!(
                streaming_cache.interval_count(),
                batch_cache.interval_count(),
                "Interval count mismatch with resolution={:?}",
                resolution
            );

            assert_eq!(
                streaming_cache.size_bytes(),
                batch_cache.size_bytes(),
                "Size mismatch with resolution={:?}",
                resolution
            );

            // Query range
            use std::ops::Range;
            let query_range = Range {
                start: 1,
                end: 1001,
            };
            let streaming_results = normalize_results(streaming_cache.query_range(&query_range));
            let batch_results = normalize_results(batch_cache.query_range(&query_range));

            assert_eq!(
                streaming_results, batch_results,
                "Range query mismatch with resolution={:?}",
                resolution
            );
        }
    }

    /// Helper to deeply compare two caches for structural equality
    fn assert_caches_deeply_equal(
        streaming_cache: &ValueAwareLapperCache,
        batch_cache: &ValueAwareLapperCache,
        context: &str,
    ) {
        // No need to normalize - Box<[T]> has no excess capacity

        // 1. Check resolutions match
        assert_eq!(
            streaming_cache.get_resolution(),
            batch_cache.get_resolution(),
            "{}: Resolutions must match",
            context
        );

        // 2. Check interval counts match
        assert_eq!(
            streaming_cache.interval_count(),
            batch_cache.interval_count(),
            "{}: Interval counts must match",
            context
        );

        // 3. Check cache sizes match exactly after normalization
        assert_eq!(
            streaming_cache.size_bytes(),
            batch_cache.size_bytes(),
            "{}: Cache sizes must match after normalizing Vec capacities",
            context
        );

        // 4. Check intervals are identical (sorted)
        let streaming_intervals = streaming_cache.get_intervals_sorted();
        let batch_intervals = batch_cache.get_intervals_sorted();
        assert_eq!(
            streaming_intervals, batch_intervals,
            "{}: Intervals must be identical",
            context
        );

        // 5. Check dictionaries contain the same strings
        let streaming_dict = streaming_cache.get_dictionary_strings_sorted();
        let batch_dict = batch_cache.get_dictionary_strings_sorted();
        assert_eq!(
            streaming_dict, batch_dict,
            "{}: Dictionaries must contain identical strings",
            context
        );

        // 6. Check tagset counts match
        assert_eq!(
            streaming_cache.unique_tagsets(),
            batch_cache.unique_tagsets(),
            "{}: Tagset counts must match",
            context
        );

        // 7. Check encoded tagsets are identical
        let streaming_tagsets = streaming_cache.get_tagsets_sorted();
        let batch_tagsets = batch_cache.get_tagsets_sorted();
        assert_eq!(
            streaming_tagsets, batch_tagsets,
            "{}: Encoded tagsets must be identical",
            context
        );
    }

    #[test]
    fn test_deep_equality_simple() {
        let resolution = Duration::from_secs(1);
        let chunk_size = 3;

        let data = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "b")])),
            (6, make_tagset(&[("host", "b")])),
            (10, make_tagset(&[("host", "a")])),
        ];

        // Build with streaming
        let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Deep equality check
        assert_caches_deeply_equal(&streaming_cache, &batch_cache, "simple data");
    }

    #[test]
    fn test_deep_equality_multiple_chunk_sizes() {
        let resolution = Duration::from_secs(5);

        // Generate test data with various patterns
        let mut data = Vec::new();
        for t in 1..=300 {
            let host = format!("host-{}", t % 10);
            let region = if t < 100 {
                "us-east"
            } else if t < 200 {
                "us-west"
            } else {
                "eu-west"
            };
            data.push((t, make_tagset(&[("host", &host), ("region", region)])));
        }

        // Build batch cache (reference)
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

        // Test different chunk sizes
        let chunk_sizes = [10, 50, 100, 299, 300, 500];

        for chunk_size in chunk_sizes {
            let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
            streaming_builder.process_points(data.clone()).unwrap();
            let streaming_cache = streaming_builder.finalize().unwrap();

            assert_caches_deeply_equal(
                &streaming_cache,
                &batch_cache,
                &format!("chunk_size={}", chunk_size),
            );
        }
    }

    #[test]
    fn test_deep_equality_high_cardinality() {
        let resolution = Duration::from_secs(60);
        let chunk_size = 100;

        // Generate high cardinality data
        let mut data = Vec::new();
        for t in 1..=500 {
            let host = format!("host-{}", t % 50); // 50 different hosts
            let region = format!("region-{}", t % 5); // 5 different regions
            let env = if t % 3 == 0 {
                "prod"
            } else if t % 3 == 1 {
                "staging"
            } else {
                "dev"
            };
            data.push((
                t,
                make_tagset(&[("host", &host), ("region", &region), ("env", env)]),
            ));
        }

        // Build with streaming
        let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Deep equality check
        assert_caches_deeply_equal(&streaming_cache, &batch_cache, "high cardinality");

        // Additional verification: print stats
        println!("High cardinality test:");
        println!("  Intervals: {}", streaming_cache.interval_count());
        println!("  Unique tagsets: {}", streaming_cache.unique_tagsets());
        println!(
            "  Dictionary size: {} bytes",
            streaming_cache.dictionary_stats().dictionary_size_bytes
        );
    }

    #[test]
    fn test_deep_equality_gaps_and_patterns() {
        let resolution = Duration::from_secs(10);
        let chunk_size = 50;

        let mut data = Vec::new();

        // Pattern 1: Dense continuous data
        for t in 1..=100 {
            data.push((t, make_tagset(&[("pattern", "dense"), ("host", "a")])));
        }

        // Pattern 2: Sparse with gaps
        for t in (200..=300).step_by(5) {
            data.push((t, make_tagset(&[("pattern", "sparse"), ("host", "b")])));
        }

        // Pattern 3: Single isolated points
        for t in [400, 450, 500, 600, 750] {
            data.push((t, make_tagset(&[("pattern", "isolated"), ("host", "c")])));
        }

        // Pattern 4: Rapid switching values
        for t in 800..=900 {
            let host = if t % 2 == 0 { "d" } else { "e" };
            data.push((t, make_tagset(&[("pattern", "switching"), ("host", host)])));
        }

        // Build with streaming
        let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Deep equality check
        assert_caches_deeply_equal(&streaming_cache, &batch_cache, "gaps and patterns");
    }

    #[test]
    fn test_deep_equality_different_resolutions() {
        // Generate test data
        let mut data = Vec::new();
        for t in 1..=500 {
            let host = format!("host-{}", t % 20);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        let chunk_size = 75;

        // Test different resolutions
        let resolutions = [
            ("1ns", Duration::from_nanos(1)),
            ("1s", Duration::from_secs(1)),
            ("10s", Duration::from_secs(10)),
            ("1m", Duration::from_secs(60)),
            ("5m", Duration::from_secs(300)),
        ];

        for (name, resolution) in resolutions {
            let mut streaming_builder = ChunkedStreamBuilder::new(resolution, chunk_size);
            streaming_builder.process_points(data.clone()).unwrap();
            let streaming_cache = streaming_builder.finalize().unwrap();

            let batch_cache =
                ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                    .unwrap();

            assert_caches_deeply_equal(
                &streaming_cache,
                &batch_cache,
                &format!("resolution={}", name),
            );
        }
    }

    // ============================================================================
    // Tests for SortedStreamBuilder (Tier 2)
    // ============================================================================

    #[test]
    fn test_sorted_builder_simple() {
        let resolution = Duration::from_nanos(1); // Nanosecond resolution
        let mut builder = SortedStreamBuilder::new(resolution);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "b")])),
            (4, make_tagset(&[("host", "b")])),
        ];

        builder.process_points_unchecked(points).unwrap();
        assert_eq!(builder.total_points(), 4);
        assert!(!builder.is_out_of_order());

        let cache = builder.finalize().unwrap();

        // Verify cache works correctly
        let results1 = cache.query_point(1);
        assert!(!results1.is_empty(), "Should have results at timestamp 1");

        let results3 = cache.query_point(3);
        assert!(!results3.is_empty(), "Should have results at timestamp 3");
    }

    #[test]
    fn test_sorted_builder_single_tagset() {
        let resolution = Duration::from_nanos(1);
        let mut builder = SortedStreamBuilder::new(resolution);

        // All points have the same tagset, should create one continuous interval per bucket
        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (4, make_tagset(&[("host", "a")])),
        ];

        builder.process_points_unchecked(points).unwrap();
        let cache = builder.finalize().unwrap();

        // Should be able to query all timestamps
        for t in 1..=4 {
            let results = cache.query_point(t);
            assert_eq!(
                results.len(),
                1,
                "Should have exactly one result at timestamp {}",
                t
            );
        }
    }

    #[test]
    fn test_sorted_builder_interleaved_tagsets() {
        let resolution = Duration::from_nanos(1); // Use nanosecond resolution for nanosecond timestamps
        let mut builder = SortedStreamBuilder::new(resolution);

        // Interleaved tagsets - should create separate intervals
        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "b")])),
            (3, make_tagset(&[("host", "a")])),
            (4, make_tagset(&[("host", "b")])),
        ];

        builder.process_points_unchecked(points).unwrap();
        let cache = builder.finalize().unwrap();

        // Each timestamp should have exactly one tagset
        for t in 1..=4 {
            let results = cache.query_point(t);
            assert_eq!(
                results.len(),
                1,
                "Should have exactly one result at timestamp {}",
                t
            );
        }
    }

    #[test]
    fn test_sorted_builder_with_resolution() {
        let resolution = Duration::from_secs(10); // 10 second buckets
        let mut builder = SortedStreamBuilder::new(resolution);

        // Points within same 10s bucket
        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "a")])),
            (9, make_tagset(&[("host", "a")])),
            (11, make_tagset(&[("host", "a")])), // Different bucket
            (15, make_tagset(&[("host", "a")])),
        ];

        builder.process_points_unchecked(points).unwrap();
        let cache = builder.finalize().unwrap();

        // Query within first bucket [0, 10)
        let results1 = cache.query_point(5);
        assert!(!results1.is_empty());

        // Query within second bucket [10, 20)
        let results2 = cache.query_point(15);
        assert!(!results2.is_empty());
    }

    #[test]
    fn test_sorted_builder_detects_out_of_order() {
        let resolution = Duration::from_nanos(1);
        let mut builder = SortedStreamBuilder::new(resolution);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])), // Out of order!
            (4, make_tagset(&[("host", "a")])),
        ];

        builder.process_points_unchecked(points).unwrap();
        assert!(builder.is_out_of_order(), "Should detect out-of-order data");

        // Can still finalize, but results may be incorrect
        let _cache = builder.finalize().unwrap();
    }

    #[test]
    fn test_sorted_builder_equals_batch() {
        let resolution = Duration::from_nanos(1);

        // Create data
        let mut data = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "b")])),
            (5, make_tagset(&[("host", "b")])),
            (6, make_tagset(&[("host", "a")])),
            (10, make_tagset(&[("host", "c")])),
        ];

        // Sort by (timestamp, tagset) - this is what batch builder expects
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Build with sorted stream builder
        let mut sorted_builder = SortedStreamBuilder::new(resolution);
        sorted_builder
            .process_points_unchecked(data.clone())
            .unwrap();
        let sorted_cache = sorted_builder.finalize().unwrap();

        // Build with batch method (for comparison)
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Use the deep equality check helper
        assert_caches_deeply_equal(&sorted_cache, &batch_cache, "sorted vs batch");
    }

    #[test]
    fn test_sorted_builder_equals_batch_large() {
        let resolution = Duration::from_secs(60); // 1 minute

        // Generate larger dataset
        let mut data = Vec::new();
        for t in 1..=1000 {
            let host = format!("host-{}", t % 10);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Sort by (timestamp, tagset)
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Build with sorted stream builder
        let mut sorted_builder = SortedStreamBuilder::new(resolution);
        sorted_builder
            .process_points_unchecked(data.clone())
            .unwrap();
        let sorted_cache = sorted_builder.finalize().unwrap();

        // Build with batch method
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution).unwrap();

        // Deep equality check
        assert_caches_deeply_equal(&sorted_cache, &batch_cache, "large sorted data");
    }

    #[test]
    fn test_sorted_builder_with_stats() {
        let resolution = Duration::from_nanos(1);
        let mut builder = SortedStreamBuilder::new(resolution);

        let data = vec![
            (1, make_tagset(&[("host", "a"), ("region", "us-east")])),
            (2, make_tagset(&[("host", "b"), ("region", "us-west")])),
            (3, make_tagset(&[("host", "a"), ("region", "us-east")])), // Reused tagset
            (4, make_tagset(&[("host", "c"), ("region", "eu-west")])),
        ];

        builder.process_points_unchecked(data).unwrap();
        let (cache, stats) = builder.finalize_with_stats().unwrap();

        assert_eq!(stats.total_points, 4);
        assert_eq!(stats.unique_tagsets, 3); // Three unique tag combinations
        assert!(!stats.out_of_order_detected);

        // Verify cache is valid
        assert!(cache.interval_count() > 0);
    }

    #[test]
    fn test_sorted_builder_high_cardinality() {
        let resolution = Duration::from_secs(60);
        let mut builder = SortedStreamBuilder::new(resolution);

        // Generate high cardinality data
        let mut data = Vec::new();
        for t in 1..=500 {
            let host = format!("host-{}", t % 50);
            let region = format!("region-{}", t % 5);
            data.push((t, make_tagset(&[("host", &host), ("region", &region)])));
        }

        // Sort by (timestamp, tagset)
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        builder.process_points_unchecked(data.clone()).unwrap();
        let sorted_cache = builder.finalize().unwrap();

        // Compare with batch build
        let batch_cache =
            ValueAwareLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Deep equality check
        assert_caches_deeply_equal(&sorted_cache, &batch_cache, "high cardinality sorted");
    }

    #[test]
    fn test_sorted_builder_different_resolutions() {
        // Generate sorted test data with nanosecond timestamps
        let mut data = Vec::new();
        for t in 1..=500 {
            let host = format!("host-{}", t % 10);
            // Use nanosecond timestamps for proper bucketing
            data.push((t * 1_000_000_000, make_tagset(&[("host", &host)])));
        }

        // Test different resolutions
        let resolutions = [
            ("1ns", Duration::from_nanos(1)),
            ("1s", Duration::from_secs(1)),
            ("10s", Duration::from_secs(10)),
            ("1m", Duration::from_secs(60)),
            ("5m", Duration::from_secs(300)),
        ];

        for (name, resolution) in resolutions {
            let mut sorted_builder = SortedStreamBuilder::new(resolution);
            sorted_builder
                .process_points_unchecked(data.clone())
                .unwrap();
            let sorted_cache = sorted_builder.finalize().unwrap();

            let batch_cache =
                ValueAwareLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                    .unwrap();

            assert_caches_deeply_equal(
                &sorted_cache,
                &batch_cache,
                &format!("resolution={}", name),
            );
        }
    }

    #[test]
    fn test_sorted_builder_empty() {
        let resolution = Duration::from_nanos(1);
        let builder = SortedStreamBuilder::new(resolution);

        // Should return NoData error
        let result = builder.finalize();
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CacheBuildError::NoData));
        }
    }

    #[test]
    fn test_sorted_vs_chunked_builders() {
        let resolution = Duration::from_secs(60);

        // Generate data
        let mut data = Vec::new();
        for t in 1..=1000 {
            let host = format!("host-{}", t % 20);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Sort by (timestamp, tagset) for sorted builder
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Build with sorted builder
        let mut sorted_builder = SortedStreamBuilder::new(resolution);
        sorted_builder
            .process_points_unchecked(data.clone())
            .unwrap();
        let sorted_cache = sorted_builder.finalize().unwrap();

        // Build with chunked builder (Tier 1)
        let mut chunked_builder = ChunkedStreamBuilder::new(resolution, 100);
        chunked_builder
            .process_points_unchecked(data.clone())
            .unwrap();
        let chunked_cache = chunked_builder.finalize().unwrap();

        // Both should produce identical results
        assert_caches_deeply_equal(&sorted_cache, &chunked_cache, "sorted vs chunked");
    }

    // ============================================================================
    // Tests for BitmapLapperCache Streaming Builders
    // ============================================================================

    /// Helper to normalize bitmap query results for comparison
    fn normalize_bitmap_results<'a>(
        mut results: Vec<Vec<(&'a str, &'a str)>>,
    ) -> Vec<Vec<(&'a str, &'a str)>> {
        for tagset in results.iter_mut() {
            tagset.sort();
        }
        results.sort();
        results
    }

    #[test]
    fn test_bitmap_chunked_builder_single_chunk() {
        let resolution = Duration::from_secs(1);
        let mut builder = super::BitmapChunkedStreamBuilder::new(resolution, 10);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "b")])),
        ];

        builder.process_points(points).unwrap();
        assert_eq!(builder.buffered_count(), 3);
        assert_eq!(builder.chunks_flushed(), 0);

        let cache = builder.finalize().unwrap();

        // Verify cache works correctly
        let results1 = cache.query_point(1);
        assert!(!results1.is_empty(), "Should have results at timestamp 1");

        let results2 = cache.query_point(2);
        assert!(!results2.is_empty(), "Should have results at timestamp 2");

        let results3 = cache.query_point(3);
        assert!(!results3.is_empty(), "Should have results at timestamp 3");
    }

    #[test]
    fn test_bitmap_chunked_builder_multiple_chunks() {
        let resolution = Duration::from_secs(1);
        let mut builder = super::BitmapChunkedStreamBuilder::new(resolution, 5);

        // First chunk
        let points1 = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (4, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "a")])),
        ];

        builder.process_points(points1).unwrap();
        assert_eq!(builder.buffered_count(), 0); // Flushed
        assert_eq!(builder.chunks_flushed(), 1);

        // Second chunk
        let points2 = vec![
            (6, make_tagset(&[("host", "b")])),
            (7, make_tagset(&[("host", "b")])),
        ];

        builder.process_points(points2).unwrap();
        assert_eq!(builder.buffered_count(), 2);
        assert_eq!(builder.chunks_flushed(), 1);

        let cache = builder.finalize().unwrap();

        // Verify data from both chunks
        let results1 = cache.query_point(1);
        assert!(!results1.is_empty(), "Should have results from first chunk");

        let results2 = cache.query_point(6);
        assert!(
            !results2.is_empty(),
            "Should have results from second chunk"
        );
    }

    #[test]
    fn test_bitmap_chunked_equals_batch() {
        let resolution = Duration::from_secs(1);
        let chunk_size = 3;

        let data = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "b")])),
            (6, make_tagset(&[("host", "b")])),
            (10, make_tagset(&[("host", "a")])),
        ];

        // Build with streaming
        let mut streaming_builder = super::BitmapChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            crate::BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                .unwrap();

        // Assert they produce identical results
        assert_eq!(
            streaming_cache.interval_count(),
            batch_cache.interval_count(),
            "Interval counts must match"
        );

        // Query several points and verify results match
        for timestamp in [1, 2, 3, 4, 5, 6, 7, 10, 15] {
            let streaming_results =
                normalize_bitmap_results(streaming_cache.query_point(timestamp));
            let batch_results = normalize_bitmap_results(batch_cache.query_point(timestamp));

            assert_eq!(
                streaming_results, batch_results,
                "Query results must match for timestamp {}",
                timestamp
            );
        }
    }

    #[test]
    fn test_bitmap_chunked_equals_batch_large() {
        let resolution = Duration::from_secs(60);
        let chunk_size = 100;

        // Generate larger dataset
        let mut data = Vec::new();
        for t in 1..=1000 {
            let host = format!("host-{}", t % 20);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Build with streaming
        let mut streaming_builder = super::BitmapChunkedStreamBuilder::new(resolution, chunk_size);
        streaming_builder.process_points(data.clone()).unwrap();
        let streaming_cache = streaming_builder.finalize().unwrap();

        // Build with batch
        let batch_cache =
            crate::BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                .unwrap();

        // Assert interval counts match
        assert_eq!(
            streaming_cache.interval_count(),
            batch_cache.interval_count(),
            "Interval counts must match"
        );

        // Query full range
        use std::ops::Range;
        let query_range = Range {
            start: 1,
            end: 1001,
        };
        let streaming_results = normalize_bitmap_results(streaming_cache.query_range(&query_range));
        let batch_results = normalize_bitmap_results(batch_cache.query_range(&query_range));

        assert_eq!(
            streaming_results, batch_results,
            "Range query results must match"
        );
    }

    #[test]
    fn test_bitmap_chunked_different_chunk_sizes() {
        let resolution = Duration::from_secs(5);

        // Generate test data
        let mut data = Vec::new();
        for t in 1..=300 {
            let host = format!("host-{}", t % 10);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Build batch cache (reference)
        let batch_cache =
            crate::BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                .unwrap();

        // Test different chunk sizes
        let chunk_sizes = [10, 50, 100, 299, 300, 500];

        for chunk_size in chunk_sizes {
            let mut streaming_builder =
                super::BitmapChunkedStreamBuilder::new(resolution, chunk_size);
            streaming_builder.process_points(data.clone()).unwrap();
            let streaming_cache = streaming_builder.finalize().unwrap();

            assert_eq!(
                streaming_cache.interval_count(),
                batch_cache.interval_count(),
                "Interval count mismatch with chunk_size={}",
                chunk_size
            );

            // Spot check
            for timestamp in [1, 150, 300] {
                let streaming_results =
                    normalize_bitmap_results(streaming_cache.query_point(timestamp));
                let batch_results = normalize_bitmap_results(batch_cache.query_point(timestamp));

                assert_eq!(
                    streaming_results, batch_results,
                    "Query mismatch at timestamp {} with chunk_size={}",
                    timestamp, chunk_size
                );
            }
        }
    }

    #[test]
    fn test_bitmap_sorted_builder_simple() {
        let resolution = Duration::from_nanos(1);
        let mut builder = super::BitmapSortedStreamBuilder::new(resolution);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "b")])),
            (4, make_tagset(&[("host", "b")])),
        ];

        builder.process_points(points).unwrap();
        assert_eq!(builder.total_points(), 4);
        assert!(!builder.is_out_of_order());

        let cache = builder.finalize().unwrap();

        // Verify cache works
        let results1 = cache.query_point(1);
        assert!(!results1.is_empty(), "Should have results at timestamp 1");

        let results3 = cache.query_point(3);
        assert!(!results3.is_empty(), "Should have results at timestamp 3");
    }

    #[test]
    fn test_bitmap_sorted_builder_detects_out_of_order() {
        let resolution = Duration::from_nanos(1);
        let mut builder = super::BitmapSortedStreamBuilder::new(resolution);

        let points = vec![
            (1, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])), // Out of order!
            (4, make_tagset(&[("host", "a")])),
        ];

        builder.process_points(points).unwrap();
        assert!(builder.is_out_of_order(), "Should detect out-of-order data");

        // Can still finalize
        let _cache = builder.finalize().unwrap();
    }

    #[test]
    fn test_bitmap_sorted_equals_batch() {
        let resolution = Duration::from_nanos(1);

        let mut data = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "b")])),
            (5, make_tagset(&[("host", "b")])),
            (6, make_tagset(&[("host", "a")])),
            (10, make_tagset(&[("host", "c")])),
        ];

        // Sort by (timestamp, tagset)
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Build with sorted stream builder
        let mut sorted_builder = super::BitmapSortedStreamBuilder::new(resolution);
        sorted_builder.process_points(data.clone()).unwrap();
        let sorted_cache = sorted_builder.finalize().unwrap();

        // Build with batch method
        let batch_cache =
            crate::BitmapLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Assert interval counts match
        assert_eq!(
            sorted_cache.interval_count(),
            batch_cache.interval_count(),
            "Interval counts must match"
        );

        // Spot check queries
        for timestamp in [1, 2, 3, 5, 6, 10] {
            let sorted_results = normalize_bitmap_results(sorted_cache.query_point(timestamp));
            let batch_results = normalize_bitmap_results(batch_cache.query_point(timestamp));

            assert_eq!(
                sorted_results, batch_results,
                "Query results must match for timestamp {}",
                timestamp
            );
        }
    }

    #[test]
    fn test_bitmap_sorted_equals_batch_large() {
        let resolution = Duration::from_secs(60);

        // Generate larger dataset
        let mut data = Vec::new();
        for t in 1..=1000 {
            let host = format!("host-{}", t % 10);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Sort by (timestamp, tagset)
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Build with sorted stream builder
        let mut sorted_builder = super::BitmapSortedStreamBuilder::new(resolution);
        sorted_builder.process_points(data.clone()).unwrap();
        let sorted_cache = sorted_builder.finalize().unwrap();

        // Build with batch method
        let batch_cache =
            crate::BitmapLapperCache::from_unsorted_with_resolution(data.clone(), resolution)
                .unwrap();

        // Assert interval counts match
        assert_eq!(
            sorted_cache.interval_count(),
            batch_cache.interval_count(),
            "Interval counts must match"
        );

        // Range query
        use std::ops::Range;
        let query_range = Range {
            start: 1,
            end: 1001,
        };
        let sorted_results = normalize_bitmap_results(sorted_cache.query_range(&query_range));
        let batch_results = normalize_bitmap_results(batch_cache.query_range(&query_range));

        assert_eq!(
            sorted_results, batch_results,
            "Range query results must match"
        );
    }

    #[test]
    fn test_bitmap_sorted_builder_high_cardinality() {
        let resolution = Duration::from_secs(60);
        let mut builder = super::BitmapSortedStreamBuilder::new(resolution);

        // Generate high cardinality data
        let mut data = Vec::new();
        for t in 1..=500 {
            let host = format!("host-{}", t % 50);
            let region = format!("region-{}", t % 5);
            data.push((t, make_tagset(&[("host", &host), ("region", &region)])));
        }

        // Sort by (timestamp, tagset)
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        builder.process_points(data.clone()).unwrap();
        let sorted_cache = builder.finalize().unwrap();

        // Compare with batch build
        let batch_cache =
            crate::BitmapLapperCache::from_unsorted_with_resolution(data, resolution).unwrap();

        // Assert interval counts match
        assert_eq!(
            sorted_cache.interval_count(),
            batch_cache.interval_count(),
            "High cardinality: interval counts must match"
        );
    }

    #[test]
    fn test_bitmap_sorted_vs_chunked_builders() {
        let resolution = Duration::from_secs(60);

        // Generate data
        let mut data = Vec::new();
        for t in 1..=1000 {
            let host = format!("host-{}", t % 20);
            data.push((t, make_tagset(&[("host", &host)])));
        }

        // Sort by (timestamp, tagset) for sorted builder
        data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Build with bitmap sorted builder
        let mut sorted_builder = super::BitmapSortedStreamBuilder::new(resolution);
        sorted_builder.process_points(data.clone()).unwrap();
        let sorted_cache = sorted_builder.finalize().unwrap();

        // Build with bitmap chunked builder
        let mut chunked_builder = super::BitmapChunkedStreamBuilder::new(resolution, 100);
        chunked_builder.process_points(data.clone()).unwrap();
        let chunked_cache = chunked_builder.finalize().unwrap();

        // Both should produce identical results
        assert_eq!(
            sorted_cache.interval_count(),
            chunked_cache.interval_count(),
            "Interval counts must match"
        );

        // Spot check queries
        for timestamp in [1, 250, 500, 750, 1000] {
            let sorted_results = normalize_bitmap_results(sorted_cache.query_point(timestamp));
            let chunked_results = normalize_bitmap_results(chunked_cache.query_point(timestamp));

            assert_eq!(
                sorted_results, chunked_results,
                "Query results must match for timestamp {}",
                timestamp
            );
        }
    }

    #[test]
    fn test_bitmap_chunked_with_stats() {
        let resolution = Duration::from_secs(1);
        let mut builder = super::BitmapChunkedStreamBuilder::new(resolution, 3);

        let points1 = vec![
            (1, make_tagset(&[("host", "a")])),
            (2, make_tagset(&[("host", "a")])),
            (3, make_tagset(&[("host", "a")])),
        ];
        builder.process_points(points1).unwrap();

        let points2 = vec![
            (4, make_tagset(&[("host", "a")])),
            (5, make_tagset(&[("host", "a")])),
        ];
        builder.process_points(points2).unwrap();

        let (_cache, stats) = builder.finalize_with_stats().unwrap();

        assert_eq!(stats.total_points, 5);
        assert_eq!(stats.chunks_flushed, 2);
        assert_eq!(stats.chunk_size, 3);
        assert_eq!(stats.avg_chunk_size(), 2.5);
    }

    #[test]
    fn test_bitmap_sorted_with_stats() {
        let resolution = Duration::from_nanos(1);
        let mut builder = super::BitmapSortedStreamBuilder::new(resolution);

        let data = vec![
            (1, make_tagset(&[("host", "a"), ("region", "us-east")])),
            (2, make_tagset(&[("host", "b"), ("region", "us-west")])),
            (3, make_tagset(&[("host", "a"), ("region", "us-east")])), // Reused tagset
            (4, make_tagset(&[("host", "c"), ("region", "eu-west")])),
        ];

        builder.process_points(data).unwrap();
        let (cache, stats) = builder.finalize_with_stats().unwrap();

        assert_eq!(stats.total_points, 4);
        assert_eq!(stats.unique_tagsets, 3);
        assert!(!stats.out_of_order_detected);

        assert!(cache.interval_count() > 0);
    }

    #[test]
    #[should_panic(expected = "No data was processed - cache is empty")]
    fn test_bitmap_chunked_empty() {
        let resolution = Duration::from_secs(1);
        let builder = super::BitmapChunkedStreamBuilder::new(resolution, 10);
        let _cache = builder.finalize().unwrap();
    }

    #[test]
    fn test_bitmap_sorted_empty() {
        let resolution = Duration::from_nanos(1);
        let builder = super::BitmapSortedStreamBuilder::new(resolution);

        let result = builder.finalize();
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(matches!(e, CacheBuildError::NoData));
        }
    }
}
