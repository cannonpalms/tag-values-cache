# Root Cause: IntervalTree Overwrites Duplicate Ranges

## The Bug

When InteravlCache has multiple values at the same timestamp, it creates multiple intervals with identical ranges but different values. When these are inserted into the `interavl` crate's `IntervalTree`, **only the last one is retained**.

## Proof

```rust
let mut tree: IntervalTree<u64, usize> = IntervalTree::default();
tree.insert(5..6, 0);  // Insert [5,6) with value 0
tree.insert(5..6, 1);  // Insert [5,6) with value 1 - OVERWRITES!

let overlaps: Vec<_> = tree.iter_overlaps(&(5..6)).collect();
// Result: [(5..6, 1)] - only the second insert remains!
```

## The Flow

1. **Input**: Two records at timestamp 5 with different values
   - (5, RecordBatchRow { "a": 1 })
   - (5, RecordBatchRow { "a": 2 })

2. **Interval Building**: Correctly creates two intervals
   - [5, 6) -> RecordBatchRow { "a": 1 }
   - [5, 6) -> RecordBatchRow { "a": 2 }

3. **Merge**: Correctly keeps both (different values, not merged)
   - [5, 6) -> RecordBatchRow { "a": 1 }
   - [5, 6) -> RecordBatchRow { "a": 2 }

4. **Tree Rebuild**: **BUG OCCURS HERE**
   ```rust
   for (idx, (interval, _)) in merged.iter().enumerate() {
       tree.insert(interval.clone(), idx);
   }
   ```
   - Insert [5, 6) with index 0
   - Insert [5, 6) with index 1 - **overwrites index 0!**

5. **Query**: Only finds index 1 in the tree
   - Returns only RecordBatchRow { "a": 2 }
   - RecordBatchRow { "a": 1 } is lost!

## Why Other Implementations Don't Have This Bug

- **VecCache**: Uses a Vec, allows duplicate ranges
- **IntervalTreeCache**: Uses a different interval tree implementation (interval-tree crate)
- **LapperCache/ValueLapperCache**: Uses rust-lapper which handles duplicates
- **InteravlCache**: Uses the interavl crate which has this limitation

## Possible Fixes

### Option 1: Store Multiple Indices per Range
Instead of storing a single index in the tree, store a Vec of indices:
```rust
tree: IntervalTree<u64, Vec<usize>>
```

### Option 2: Use Different Data Structure
Replace IntervalTree with a structure that allows duplicate ranges.

### Option 3: Combine Same-Range Intervals
Before inserting into the tree, group intervals by range and store them together.

### Option 4: Make Ranges Unique
Add a small disambiguation factor to make ranges unique (hacky, not recommended).

## Reproducer Test

```rust
#[test]
fn test_interavl_duplicate_timestamp_bug() {
    let mut values1 = BTreeMap::new();
    values1.insert("a".to_string(), ArrowValue::Int64(1));
    let row1 = RecordBatchRow::new(values1);

    let mut values2 = BTreeMap::new();
    values2.insert("a".to_string(), ArrowValue::Int64(2));
    let row2 = RecordBatchRow::new(values2);

    let data = vec![(5, row1), (5, row2)];

    let vec_cache = VecCache::new(data.clone()).unwrap();
    let interavl_cache = InteravlCache::new(data).unwrap();

    let vec_result = vec_cache.query_point(5);
    let interavl_result = interavl_cache.query_point(5);

    assert_eq!(vec_result.len(), 2);  // ✓ Passes
    assert_eq!(interavl_result.len(), 2);  // ✗ Fails: only 1!
}
```

## Impact

Any dataset with:
- Multiple different values at the same timestamp
- That get built into intervals with identical ranges [t, t+1)

Will lose all but the last value when using InteravlCache.

This explains why the ClickBench data shows missing records - it likely has multiple tag combinations at the same timestamps.