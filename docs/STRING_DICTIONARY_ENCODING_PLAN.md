# Direct String Handling Refactoring Plan

## Overview

This document outlines the plan to refactor the ValueAwareLapperCache to remove `RecordBatchRow` and handle strings directly. The refactoring is divided into two phases:

1. **Phase 1**: Remove dictionary encoding - simplify the current implementation
2. **Phase 2**: Replace RecordBatchRow with sets of `(String, String)` tuples for direct string handling

## Current State Analysis

### Existing Architecture

The current implementation uses `RecordBatchRow` which contains a `BTreeMap<String, String>` for tag values. The cache interns entire rows using `Arc<RecordBatchRow>` for deduplication.

**Current structure:**
```rust
pub struct RecordBatchRow {
    pub values: BTreeMap<String, String>,
}

pub struct ValueAwareLapperCache {
    value_lapper: ValueAwareLapper<u64, Arc<RecordBatchRow>>,
    unique_rows: IndexSet<Arc<RecordBatchRow>>,
    resolution: Duration,
}
```

### Issues with Current Approach

1. **Unnecessary abstraction**: `RecordBatchRow` wrapper adds no value for single-column tag handling
2. **Dictionary encoding complexity**: The current Arc-based interning is complex for marginal gains
3. **Rigid structure**: BTreeMap wrapper makes direct string manipulation cumbersome

## Proposed Solution: Two-Phase Refactoring

### Phase 1: Remove Dictionary Encoding and Make Cache Generic

**Goal**: Simplify by removing Arc-based row interning and make ValueAwareLapperCache generic over the value type.

**Changes**:
- Remove `unique_rows: IndexSet<Arc<RecordBatchRow>>` from ValueAwareLapperCache
- Remove `intern_row()` function
- Make the cache generic: `ValueAwareLapperCache<V>` where `V: Clone + Eq + Ord`
- Store values directly: `ValueAwareLapper<u64, V>` (no Arc wrapping)
- Simplify the value-aware merging to work directly with values

**New structure**:
```rust
pub struct ValueAwareLapperCache<V>
where
    V: Clone + Eq + Ord,
{
    value_lapper: ValueAwareLapper<u64, V>,
    resolution: Duration,
}
```

**Benefits**:
- Simpler codebase
- No Arc/IndexSet overhead
- Flexible: can use with any value type (RecordBatchRow, TagSet, etc.)
- Foundation for Phase 2

### Phase 2: Add TagSet Type and Use in Benchmarks

**Goal**: Define a `TagSet` type for direct string handling and use it in benchmarks while keeping RecordBatchRow in main.

**New types**:
```rust
// Type alias for clarity
pub type TagSet = BTreeSet<(String, String)>;
```

**Usage**:
- `main.rs` continues using `ValueAwareLapperCache<RecordBatchRow>`
- Benchmarks use `ValueAwareLapperCache<TagSet>` for direct string handling
- Both share the same generic implementation

**Benefits**:
- **Simplicity**: Direct string handling without wrapper types
- **Flexibility**: Easy to work with standard Rust collections
- **Transparency**: Clear what data is stored (tag name-value pairs)
- **Performance**: No indirection through wrapper types
- **Backward compatibility**: Existing code continues to work
- **Standard types**: Leverages BTreeSet's built-in Ord/Eq implementations

### Why BTreeSet instead of HashMap?

- **Deterministic ordering**: BTreeSet provides consistent iteration order
- **Equality by value**: Two BTreeSets with same elements are equal regardless of insertion order
- **Efficient merging**: Value-aware lapper can compare tag sets directly using BTreeSet's Eq implementation
- **Memory efficiency**: No hash overhead for small tag sets

## Implementation Plan

### Phase 1: Remove Dictionary Encoding and Make Cache Generic

**Files to modify:**
- `src/value_aware_lapper_cache.rs`

**Changes:**

1. **Make ValueAwareLapperCache generic**:
```rust
pub struct ValueAwareLapperCache<V>
where
    V: Clone + Eq + Ord,
{
    value_lapper: ValueAwareLapper<u64, V>,
    resolution: Duration,
}

impl<V> ValueAwareLapperCache<V>
where
    V: Clone + Eq + Ord,
{
    // All methods become generic over V
}
```

2. **Remove `unique_rows` field and `intern_row()` function** - no longer needed

3. **Simplify `build_intervals()`** to work with generic values:
```rust
fn build_intervals(
    data: Vec<(Timestamp, V)>,
    resolution: Duration,
) -> Vec<Interval<u64, V>> {
    // Build intervals directly with V values
    // No Arc wrapping or interning needed
}
```

4. **Update query methods** to work without the IndexSet:
```rust
pub fn query_point(&self, t: Timestamp) -> HashSet<&V> {
    let bucketed_t = Self::bucket_timestamp(t, self.resolution);
    self.value_lapper
        .find(bucketed_t, bucketed_t + 1)
        .map(|interval| &interval.val)
        .collect()
}
```

5. **Update `size_bytes()`** to be generic (requires `V` to provide size info):
```rust
pub fn size_bytes(&self) -> usize
where
    V: HeapSize,  // Trait for calculating heap size
{
    let mut size = std::mem::size_of::<Self>();
    size += self.value_lapper.len() * std::mem::size_of::<Interval<u64, V>>();
    // Add heap size of values
    size
}
```

6. **Update all tests** to specify concrete type (e.g., `ValueAwareLapperCache<RecordBatchRow>`)

### Phase 2: Add TagSet Type and Use in Benchmarks

**Files to modify:**
- `src/lib.rs` (add TagSet type alias)
- Benchmark files to use TagSet

**Changes:**

1. **Define TagSet type in `src/lib.rs`**:
```rust
use std::collections::BTreeSet;

/// A set of tag name-value pairs for direct string handling
pub type TagSet = BTreeSet<(String, String)>;
```

2. **Add helper function to extract TagSet from RecordBatch**:
```rust
pub fn extract_tags_from_batch(
    batch: &RecordBatch,
    time_column: &str,
) -> Result<Vec<(Timestamp, TagSet)>> {
    // Extract timestamps
    // For each row, collect tag (name, value) pairs into BTreeSet
    // Return Vec<(Timestamp, TagSet)>
}
```

3. **Update benchmarks** to use `ValueAwareLapperCache<TagSet>`:
```rust
// In benchmark files
let cache = ValueAwareLapperCache::<TagSet>::from_sorted(
    SortedData::from_unsorted(tag_data)?,
    Duration::from_nanos(1),
)?;
```

4. **Keep main.rs using RecordBatchRow**:
```rust
// main.rs continues to work as-is
let cache = ValueAwareLapperCache::<RecordBatchRow>::from_sorted(
    SortedData::from_unsorted(row_data)?,
    Duration::from_nanos(1),
)?;
```

### Testing and Validation

#### Correctness Tests

1. **Dictionary invariants**:
   - Same string always gets same ID
   - IDs are stable and never reused
   - Round-trip encoding/decoding preserves data
   - Thread safety for concurrent access

2. **Cache behavior**:
   - Value-aware merging still works correctly
   - Query results match expected values
   - Append operations maintain consistency
   - Time bucketing behaves identically

3. **Edge cases**:
   - Empty tags
   - Very large dictionaries (>1M unique strings)
   - Unicode strings and special characters
   - Null/missing values
   - Dictionary overflow (>4B unique strings)

#### Performance Tests

1. **Memory usage**:
   - Measure dictionary overhead vs string savings
   - Compare total memory with current implementation
   - Test with various data patterns (high/low cardinality)
   - Memory growth over time with appends

2. **Query performance**:
   - Benchmark point queries
   - Benchmark range queries
   - Compare with current implementation
   - Cache miss patterns

3. **Ingestion performance**:
   - Measure dictionary encoding overhead
   - Test bulk loading scenarios
   - Benchmark append operations
   - Dictionary lookup costs

### Phase 6: Performance Optimization

1. **Dictionary optimizations**:
   - Pre-size hashmaps based on expected cardinality
   - Implement dictionary compression for cold data
   - Consider bloom filters for non-existent strings

2. **Cache optimizations**:
   - Ensure hot paths are inlined
   - Optimize comparison functions for encoded tags
   - Consider SIMD for bulk operations

3. **Memory optimizations**:
   - Implement dictionary pruning for unused strings
   - Consider memory mapping for large dictionaries
   - Optimize Arc usage patterns

## Migration Strategy

### Step 1: Parallel Implementation

- Keep existing `RecordBatchRow` implementation working
- Build new dictionary-encoded system in separate module
- Use feature flags to toggle between implementations:
```rust
#[cfg(feature = "dictionary-encoding")]
mod dictionary_cache;

#[cfg(not(feature = "dictionary-encoding"))]
mod legacy_cache;
```

### Step 2: Compatibility Layer

Create adapters to convert between representations:
```rust
impl From<RecordBatchRow> for DictionaryEncodedTags {
    fn from(row: RecordBatchRow) -> Self {
        // Convert with dictionary encoding
    }
}

impl From<DictionaryEncodedTags> for RecordBatchRow {
    fn from(encoded: DictionaryEncodedTags) -> Self {
        // Decode to original format
    }
}
```

### Step 3: Incremental Validation

1. Run both implementations in parallel
2. Compare results for correctness
3. Measure performance differences
4. Validate memory savings

### Step 4: Gradual Cutover

1. Enable new implementation in development
2. Run A/B tests in staging
3. Monitor metrics in production
4. Remove legacy code after validation period

## Alternative Approach (Fallback Option)

If dictionary encoding proves too complex or doesn't provide expected benefits, implement a simpler string interning approach:

```rust
pub struct TagSet {
    // Direct string storage with Arc for sharing
    tags: BTreeMap<Arc<str>, Arc<str>>,
}

pub struct ValueAwareLapperCache {
    value_lapper: ValueAwareLapper<u64, Arc<TagSet>>,
    unique_tag_sets: IndexSet<Arc<TagSet>>,

    // Global string interning pool
    string_pool: HashSet<Arc<str>>,

    resolution: Duration,
}

impl ValueAwareLapperCache {
    fn intern_string(&mut self, s: String) -> Arc<str> {
        if let Some(existing) = self.string_pool.get(s.as_str()) {
            existing.clone()
        } else {
            let arc_str: Arc<str> = s.into();
            self.string_pool.insert(arc_str.clone());
            arc_str
        }
    }
}
```

**Benefits of fallback approach:**
- Simpler implementation
- No numeric ID management
- Still provides memory savings
- Easier to debug and maintain

**Tradeoffs:**
- Less memory efficient than full dictionary encoding
- String comparisons instead of numeric
- Less optimization potential

## Success Metrics

### Required Outcomes

1. **Functional parity**: All existing tests pass
2. **Memory improvement**: At least 40% reduction for typical workloads
3. **Query performance**: No regression, ideally 20% improvement
4. **Maintainability**: Clear separation of concerns, well-documented

### Stretch Goals

1. **Memory reduction**: 70%+ for high-cardinality data
2. **Query performance**: 2x improvement
3. **Dictionary compression**: Additional 20% memory savings
4. **Concurrent updates**: Lock-free dictionary updates

## Risks and Mitigation

### Risk 1: Dictionary Encoding Overhead

**Risk**: Dictionary lookups add latency to queries
**Mitigation**:
- Cache hot entries
- Optimize dictionary data structures
- Fall back to simpler interning if needed

### Risk 2: Memory Overhead for Low Cardinality

**Risk**: Dictionary overhead exceeds savings for small datasets
**Mitigation**:
- Adaptive encoding based on cardinality
- Lazy dictionary initialization
- Hybrid approach for small datasets

### Risk 3: Backward Compatibility

**Risk**: Breaking changes for existing users
**Mitigation**:
- Maintain API compatibility
- Provide migration tools
- Support both formats temporarily

### Risk 4: Complexity Increase

**Risk**: System becomes harder to understand and maintain
**Mitigation**:
- Comprehensive documentation
- Clear abstraction boundaries
- Extensive test coverage
- Consider simpler alternative if complexity spirals

## Timeline Estimate

- **Phase 1**: 2 days - Dictionary infrastructure
- **Phase 2**: 3 days - Cache refactoring
- **Phase 3**: 2 days - Ingestion pipeline
- **Phase 4**: 1 day - Query interface
- **Phase 5**: 3 days - Testing and validation
- **Phase 6**: 2 days - Optimization

**Total**: ~13 days of development

## Conclusion

This refactoring will significantly improve the memory efficiency and performance of the ValueAwareLapperCache by:

1. Eliminating string duplication through dictionary encoding
2. Reducing comparison overhead with numeric IDs
3. Improving cache locality with smaller data structures
4. Enabling future optimizations like dictionary compression

The phased approach ensures we can validate improvements incrementally while maintaining a working system throughout the migration.