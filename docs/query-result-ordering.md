# Query Result Ordering Analysis

## Question

What causes the different ordering of query results between streaming and batch cache builds?

## Answer

The ordering difference is caused by **`HashSet` iteration being non-deterministic**, not by any difference in the cache structures themselves.

## Root Cause Trace

### 1. Query Method Implementation

In `src/value_aware_lapper_cache.rs:71-80`:

```rust
fn query_point_ids(&self, t: Timestamp) -> HashSet<usize> {
    let bucketed_t = Self::bucket_timestamp(t, self.resolution);
    let start = bucketed_t;
    let stop = bucketed_t + 1;

    self.value_lapper
        .find(start, stop)
        .map(|interval| interval.val)
        .collect()  // <-- Collects into HashSet
}
```

The method collects tagset IDs into a `HashSet<usize>`.

### 2. Converting to User-Visible Results

In `src/value_aware_lapper_cache.rs:123-128`:

```rust
fn query_point_decoded(&self, t: Timestamp) -> Vec<Vec<(&str, &str)>> {
    self.query_point_ids(t)
        .into_iter()  // <-- HashSet iteration order is NON-DETERMINISTIC
        .map(|id| self.decode_tagset(id))
        .collect()
}
```

The `HashSet` is iterated to decode tagsets. **HashSet iteration order is not guaranteed** and depends on:
- Internal hash bucket layout
- Hash function randomization (for security)
- **The order elements were inserted**

### 3. Why Different Insertion Orders?

Even though the deep equality tests prove that:
- ✅ Streaming and batch produce **identical intervals**
- ✅ Same tagset IDs in the same positions
- ✅ Same dictionary encoding

The `HashSet` can still have different iteration orders because:

1. **Lapper iterator order**: While the intervals are identical, the `rust-lapper` library's iterator might traverse them in slightly different orders depending on internal tree structure

2. **Hash randomization**: Rust's `HashSet` uses a randomized hasher for security, so even the same IDs inserted in the same order might hash to different buckets

## Evidence

### Deep Equality Tests Pass

All 5 deep equality tests verify:

```rust
// From streaming.rs:693-700
let streaming_intervals = streaming_cache.get_intervals_sorted();
let batch_intervals = batch_cache.get_intervals_sorted();
assert_eq!(
    streaming_intervals, batch_intervals,
    "{}: Intervals must be identical",
    context
);
```

**Result**: ✅ PASS - Intervals are **byte-for-byte identical**

### Query Results Differ Only in Order

From failing test before normalization:

```
left: [[("host", "a")], [("host", "b")]]
right: [[("host", "b")], [("host", "a")]]
```

**Same content, different order**.

## Solution

The tests now use a `normalize_results()` helper that sorts results before comparison:

```rust
fn normalize_results(mut results: Vec<Vec<(&str, &str)>>) -> Vec<Vec<(&str, &str)>> {
    for tagset in results.iter_mut() {
        tagset.sort();  // Sort tags within each tagset
    }
    results.sort();  // Sort tagsets
    results
}
```

This is correct because query results are **sets** - the order doesn't matter semantically.

## Implications

### For Users

**Query results are sets** - the order is not guaranteed and should not be relied upon. If you need deterministic ordering, sort the results in your application.

### For Cache Correctness

The ordering difference is **purely cosmetic** and does not indicate any functional difference:

- ✅ Same tagsets are returned (verified)
- ✅ Same intervals stored (verified)
- ✅ Same dictionary encoding (verified)
- ✅ Same compression ratio (verified)
- ✅ Same memory footprint (verified)

## Why Use HashSet?

The `HashSet` is used in query methods to **deduplicate tagset IDs**. When multiple overlapping intervals have the same tagset ID, the HashSet ensures each unique tagset is only returned once.

Example:
```
Intervals at timestamp 5:
  [1-10, tagset_id=0]
  [5-15, tagset_id=0]  // Same ID, overlapping
  [5-8,  tagset_id=1]

HashSet deduplication:
  {0, 1} → returns 2 unique tagsets instead of 3 intervals
```

## Potential Future Improvement

If deterministic ordering is desired, the query methods could be changed to use `BTreeSet<usize>` instead of `HashSet<usize>`:

```rust
fn query_point_ids(&self, t: Timestamp) -> BTreeSet<usize> {
    // BTreeSet maintains sorted order by key
    self.value_lapper
        .find(start, stop)
        .map(|interval| interval.val)
        .collect()
}
```

**Trade-off**:
- ✅ Deterministic ordering
- ❌ Slightly slower (O(log n) insert vs O(1) for HashSet)
- ❌ More memory (tree structure vs hash table)

However, since query results are semantically sets (order doesn't matter), the current HashSet implementation is appropriate.

## Summary

**The different ordering between streaming and batch query results is caused by HashSet iteration being non-deterministic, not by any difference in cache structure.**

- The caches are **structurally identical** (proven by deep equality tests)
- The intervals are **byte-for-byte identical**
- The dictionaries are **identical**
- The ordering difference is **purely cosmetic**
- Query results are **semantically correct** (same sets of tagsets)

This is expected behavior and not a bug.
