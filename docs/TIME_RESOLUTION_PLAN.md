# Time Resolution Plan for ValueAwareLapperCache

## Overview
Add built-in time resolution support to ValueAwareLapperCache, allowing the cache to internally handle timestamp bucketing during build time rather than requiring pre-processed data.

## Goals
1. **Simplify API**: Users provide nanosecond-resolution data; cache handles bucketing internally
2. **Improve Performance**: Single-pass building without data cloning/reprocessing
3. **Reduce Memory**: Eliminate intermediate data structures during benchmark/usage
4. **Maintain Compatibility**: Existing production code continues to work (default = nanosecond resolution)
   - Note: Benchmarks currently using truncated timestamps (5s buckets, 1min buckets, etc.) will be updated to provide nanosecond data and let the cache handle bucketing internally

## Design Specification

### Core Changes

#### 1. Add Resolution Field to Cache
```rust
use std::time::Duration;

pub struct ValueAwareLapperCache<V: Clone + PartialEq> {
    pub(crate) resolution: Duration,  // Time bucket size (Duration::from_nanos(1) = nanosecond resolution)
    pub(crate) lapper: rust_lapper::Lapper<u64, V>,
    // ... existing fields remain unchanged
}
```

#### 2. Updated Constructor API
```rust
impl<V: Clone + PartialEq> ValueAwareLapperCache<V> {
    /// Create cache with default nanosecond resolution (backward compatible)
    pub fn from_sorted(data: SortedData<V>) -> Result<Self, CacheBuildError> {
        Self::from_sorted_with_resolution(data, Duration::from_nanos(1))
    }

    /// Create cache with specified time resolution
    pub fn from_sorted_with_resolution(
        data: SortedData<V>,
        resolution: Duration
    ) -> Result<Self, CacheBuildError> {
        // Implementation details below
    }
}
```

### Implementation Strategy

#### Phase 1: Timestamp Bucketing During Build

1. **Bucket timestamps on-the-fly** during interval creation:
   ```rust
   fn bucket_timestamp(ts: u64, resolution: Duration) -> u64 {
       let resolution_ns = resolution.as_nanos() as u64;
       if resolution_ns <= 1 {
           ts  // No bucketing for nanosecond resolution
       } else {
           (ts / resolution_ns) * resolution_ns
       }
   }
   ```

2. **Merge consecutive buckets** with identical values:
   - Current: Merges when gap = 1 nanosecond
   - New: Merges when timestamps fall in consecutive buckets
   - Example: With 5-second resolution, timestamps 1000ns and 4999ns both map to bucket 0

#### Phase 2: Query Handling

1. **Range queries**:
   - Bucket query start/end timestamps using same resolution
   - Query proceeds normally with bucketed values
   - May miss some edge points due to bucketing (acceptable per requirements)

2. **Point queries**:
   - Bucket the query timestamp
   - Search for intervals containing the bucketed value

#### Phase 3: Append Operations

1. **Append must use same resolution**:
   ```rust
   pub fn append_sorted_with_resolution(
       &mut self,
       data: SortedData<V>,
       resolution: Duration
   ) -> Result<(), CacheBuildError> {
       if resolution != self.resolution {
           return Err(CacheBuildError::ResolutionMismatch);
       }
       // ... append logic with bucketing
   }
   ```

## Implementation Steps

### Step 1: Core Structure Changes
- [ ] Add `resolution: Duration` field to `ValueAwareLapperCache`
- [ ] Add `ResolutionMismatch` variant to `CacheBuildError`
- [ ] Update all existing constructors to set `resolution = Duration::from_nanos(1)`

### Step 2: Build Logic
- [ ] Create `bucket_timestamp` helper function
- [ ] Modify `from_sorted` to call `from_sorted_with_resolution(data, Duration::from_nanos(1))`
- [ ] Implement `from_sorted_with_resolution`:
  - Bucket timestamps as intervals are created
  - Keep existing merge logic (but with bucketed timestamps)
  - Store bucketed values in intervals

### Step 3: Query Logic
- [ ] Update `query_point`:
  - Bucket query timestamp before searching
- [ ] Update `query_range`:
  - Bucket start and end timestamps before searching
- [ ] No changes needed to underlying Lapper queries

### Step 4: Append Logic
- [ ] Add resolution check to `append_sorted`
- [ ] Apply bucketing during append operations
- [ ] Ensure proper merging with existing bucketed intervals

### Step 5: Testing
- [ ] Unit tests for bucketing function
- [ ] Tests for each resolution (1ns, 1s, 5s, 1m, 5m)
- [ ] Tests for merge behavior with bucketing
- [ ] Tests for query edge cases
- [ ] Tests for append with matching resolution

### Step 6: Benchmark Updates
- [ ] Update `line_protocol_benchmarks.rs`:
  - Remove all `truncate_to_*` functions
  - Pass original nanosecond timestamps to cache
  - Use `from_sorted_with_resolution(data, Duration::from_secs(5))` for 5-second resolution
  - Use `from_sorted_with_resolution(data, Duration::from_secs(60))` for 1-minute resolution, etc.
- [ ] Update `parquet_benchmarks.rs`:
  - Remove all `truncate_to_*` functions
  - Pass original nanosecond timestamps to cache
  - Use `from_sorted_with_resolution(data, Duration::from_secs(5))` for 5-second resolution
  - Use `from_sorted_with_resolution(data, Duration::from_secs(60))` for 1-minute resolution, etc.
- [ ] Compare performance: old (pre-process) vs new (built-in)

## Benefits

### Performance
- **Build time**: Single pass through data (no cloning/reprocessing)
- **Memory**: No intermediate truncated datasets
- **Append**: More efficient incremental updates

### Code Simplification
- Remove all `truncate_to_*` functions from benchmarks
- Simpler benchmark code (no data preprocessing)
- Cleaner API for users

### Future Enhancements (Not in Initial Implementation)
1. **Dictionary encoding**: Store bucket indices instead of full timestamps
2. **Adaptive resolution**: Automatically adjust based on data density
3. **Multi-resolution support**: Different resolutions for different time ranges

## Success Criteria
1. All existing tests pass with `resolution_ns = 1`
2. Benchmark performance equal or better than current approach
3. Memory usage reduced (no intermediate data structures)
4. API remains backward compatible

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking existing code | Default to nanosecond resolution for backward compatibility |
| Query semantics confusion | Document that queries use bucketed timestamps |
| Performance regression | Benchmark before/after, optimize if needed |
| Complex merge logic | Keep existing merge algorithm, just with bucketed values |

## Timeline
- Step 1-2: Core implementation (1-2 hours)
- Step 3-4: Query and append logic (1 hour)
- Step 5: Testing (1-2 hours)
- Step 6: Benchmark updates and validation (1 hour)

Total estimated time: 4-6 hours