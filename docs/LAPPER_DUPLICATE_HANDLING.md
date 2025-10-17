# Duplicate Handling in LapperCache

## Summary

The `LapperCache` implementation handles duplicates as follows:

### 1. Duplicate Intervals with Same Value ✓ Merged
**Example**: Two `[5, 10)` intervals both with value `"A"`
- **Result**: Merged into a single `[5, 10)` interval with value `"A"`
- **Behavior**: 1 interval stored, queries return 1 value

### 2. Same Interval with Different Values ✓ Kept Separate
**Example**: `[5, 10)` with value `"A"` AND `[5, 10)` with value `"B"`
- **Result**: Both intervals preserved separately
- **Behavior**: 2 intervals stored, queries return 2 values `["A", "B"]`

## How It Works

### Data Flow in `from_sorted()`:

1. **Interval Building Phase** (lines 116-117):
   ```rust
   let mut open_intervals: HashMap<V, (u64, u64)> = HashMap::new();
   ```
   - Uses a `HashMap<Value, (start, end)>` to track ONE open interval per unique value
   - When consecutive timestamps have the same value, extends the existing interval
   - When there's a gap, closes the interval and starts a new one for that value

2. **Merge Phase** (line 120):
   ```rust
   let merged = Self::merge_intervals(intervals);
   ```
   - Sorts intervals by `(start, end)`
   - Merges adjacent/overlapping intervals **only if they have the same value**
   - Check: `value == current_value && range.start <= current_range.end`
   - For exact duplicates: `max(end1, end2)` produces the same interval
   - For different values: intervals are kept separate

3. **Lapper Building Phase** (lines 70-95):
   ```rust
   fn build_lappers(intervals: &[(Range<u64>, V)]) -> HashMap<V, Lapper<usize, usize>>
   ```
   - Groups intervals by value into separate `Lapper` instances
   - Each unique value gets its own interval tree
   - Same interval with different values → stored in different Lappers

### Query Behavior:

```rust
fn query_point(&self, t: Timestamp) -> Vec<&V> {
    for (value, lapper) in &self.lappers {
        if lapper.find(start, stop).next().is_some() {
            results.push(value);
        }
    }
}
```

- Iterates over ALL value-specific Lappers
- Returns all values whose intervals overlap with the query point
- Multiple overlapping intervals with different values → multiple results

## Test Results

```
Scenario 1: Duplicate intervals with same value
  Input: [5,10) "A" + [5,10) "A" (created from duplicate data points)
  Result: 1 interval
  Query(7): 1 value

Scenario 2: Same interval, different values
  Input: [5,10) "A" + [5,10) "B"
  Result: 2 intervals
  Query(7): 2 values ["A", "B"]

Scenario 3: Overlapping intervals, same value
  Input: [5,8) "A" + [7,10) "A"
  Result: 1 merged interval [5,10) "A"
  Query(7): 1 value

Scenario 4: Overlapping intervals, different values
  Input: [5,8) "A" + [7,10) "B"
  Result: 2 intervals
  Query(7): 2 values ["A", "B"]
```

## Comparison with Other Implementations

| Implementation    | Duplicate Same Value | Same Interval Diff Values | Overlap Same Value | Overlap Diff Values |
|-------------------|---------------------|---------------------------|-------------------|---------------------|
| LapperCache       | ✓ Merged (1)       | ✓ Separate (2)            | ✓ Merged (1)      | ✓ Separate (2)      |
| IntervalTreeCache | ✓ Merged (1)       | ✓ Separate (2)            | ✓ Merged (1)      | ✓ Separate (2)      |
| VecCache          | ✓ Merged (1)       | ⚠️ Less efficient (10)    | ✓ Merged (1)      | ✓ Separate (2)      |

**Note**: VecCache doesn't merge as aggressively for same-interval-different-values scenarios,
resulting in more stored intervals but producing correct query results.

## Key Design Decision

The `HashMap<Value, Lapper>` structure ensures:
- ✅ Efficient merging of intervals with the same value
- ✅ Correct handling of overlapping intervals with different values
- ✅ Fast queries by partitioning intervals by value
- ✅ Each value's intervals are stored in a separate optimized structure

This design prevents accidental merging of intervals with different values while
efficiently consolidating intervals with the same value.
