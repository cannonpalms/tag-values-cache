# rust-lapper Native Behavior Analysis

## Summary

**rust-lapper does NOT deduplicate or merge intervals by default.** It provides a `merge_overlaps()` method, but this method **ignores the `val` field** and merges purely based on interval boundaries.

## Key Findings

### 1. Default Behavior (No Deduplication)

```rust
let intervals = vec![
    Interval { start: 5, stop: 10, val: "A" },
    Interval { start: 5, stop: 10, val: "A" }, // Exact duplicate
    Interval { start: 5, stop: 10, val: "A" }, // Another duplicate
];
let lapper = Lapper::new(intervals);
```

**Result**:
- Stores **3 intervals**
- Query at t=7 returns **3 results** (all three duplicates)
- No automatic deduplication

### 2. The `merge_overlaps()` Method

rust-lapper provides a `merge_overlaps()` method that:

✅ **Does:**
- Merges exact duplicate intervals (same boundaries)
- Merges overlapping intervals (`[5,8)` + `[7,10)` → `[5,10)`)
- Merges adjacent intervals (`[5,8)` + `[8,10)` → `[5,10)`)
- Preserves gaps (won't merge `[5,8)` and `[10,15)`)

❌ **Does NOT:**
- Consider the `val` field when merging
- Distinguish between intervals with different values
- Provide value-aware merging

### 3. Test Results

| Scenario | Before merge_overlaps() | After merge_overlaps() |
|----------|------------------------|------------------------|
| Duplicate [5,10) | 3 intervals | 1 interval |
| Overlap [5,8) + [7,10) | 2 intervals | 1 interval [5,10) |
| Gap [5,8) + [10,15) | 2 intervals | 2 intervals (unchanged) |
| Adjacent [5,8) + [8,10) | 2 intervals | 1 interval [5,10) |
| Complex [5,10) + [7,15) + [12,20) | 3 intervals | 1 interval [5,20) |

### 4. Value-Agnostic Merging Problem

```rust
// This scenario CANNOT be handled correctly by merge_overlaps()
let intervals = vec![
    Interval { start: 5, stop: 10, val: "A" },
    Interval { start: 5, stop: 10, val: "B" }, // Same interval, different value
];

// merge_overlaps() would merge these into ONE interval
// losing one of the values!
```

## Why LapperCache Needs Custom Logic

### The Problem

For our use case, we need:
- Merge intervals **only when they have the SAME value**
- Keep separate intervals when values differ **even if boundaries are identical**

### Our Solution: `HashMap<Value, Lapper>`

Our `LapperCache` implementation uses:

```rust
pub struct LapperCache<V> {
    lappers: HashMap<V, Lapper<usize, usize>>,  // Separate Lapper per value
    intervals: Vec<(Range<u64>, V)>,             // Original intervals with values
}
```

**This design:**
1. **Groups intervals by value** before creating Lappers
2. Each unique value gets its own Lapper instance
3. Merging happens **within each value's Lapper** (safe to use merge_overlaps per value)
4. Queries iterate over all Lappers to collect results

### Custom Merge Logic

We implement `merge_intervals()` that:
```rust
fn merge_intervals(mut intervals: Vec<(Range<u64>, V)>) -> Vec<(Range<u64>, V)> {
    // Sort by interval boundaries
    intervals.sort_by_key(|(range, _)| (range.start, range.end));

    // Merge ONLY when: value == current_value && range.start <= current_range.end
    for (range, value) in iter {
        if value == current_value && range.start <= current_range.end {
            // Merge - same value
        } else {
            // Keep separate - different value OR gap
        }
    }
}
```

## Comparison

### Using rust-lapper directly (wrong for our use case):
```rust
// BAD: Loses value distinctions
let intervals = vec![
    Interval { start: 5, stop: 10, val: "A" },
    Interval { start: 5, stop: 10, val: "B" },
];
let mut lapper = Lapper::new(intervals);
lapper.merge_overlaps();  // ❌ Merges to 1 interval, loses "B"
```

### Using LapperCache (correct):
```rust
// GOOD: Preserves value distinctions
let data = vec![
    (5, "A"), (6, "A"), ..., (9, "A"),  // → [5,10) "A"
    (5, "B"), (6, "B"), ..., (9, "B"),  // → [5,10) "B"
];
let cache = LapperCache::new(data).unwrap();
// Stores 2 intervals: [5,10) "A" and [5,10) "B" in separate Lappers
```

## Methods Available in rust-lapper

From testing and source code review:

| Method | Description | Use in LapperCache |
|--------|-------------|-------------------|
| `new(intervals)` | Create Lapper from Vec<Interval> | ✅ Used per value |
| `find(start, stop)` | Iterator of overlapping intervals | ✅ Used for queries |
| `iter()` | Iterate all intervals | Considered |
| `len()` | Number of intervals | ✅ Used for size |
| `merge_overlaps()` | Merge overlapping intervals | ❌ Too aggressive |
| `overlaps_merged` | Flag indicating if merged | Not used |
| `cov()` | Coverage calculation | Not used |

## Conclusion

**rust-lapper does NOT handle our duplicate/merge requirements by default.**

The native `merge_overlaps()` method is:
- ✅ Good for genomic data where intervals don't have meaningful distinct values
- ❌ Wrong for our use case where we need value-aware merging

Our `LapperCache` implementation correctly:
- ✅ Merges intervals with the **same** value (deduplicate)
- ✅ Keeps intervals with **different** values separate (preserve multi-value overlaps)
- ✅ Uses rust-lapper's fast query operations per value

This is accomplished by the `HashMap<Value, Lapper>` architecture, which partitions intervals by value before using rust-lapper's efficient structures.
