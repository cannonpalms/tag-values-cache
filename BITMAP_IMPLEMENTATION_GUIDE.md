# RoaringBitmap Implementation Guide

## Overview
This guide migrates ValueAwareLapperCache from creating **one interval per tagset per time bucket** (causing millions of overlapping intervals) to using RoaringBitmap with **one interval per time bucket** containing all tagsets. This also allows us to **remove ValueAwareLapper** and use rust_lapper::Lapper directly.

## The Problem We're Solving
Current implementation with 1,000 tagsets at 1-minute resolution:
- Creates **1.44 million overlapping intervals per day** (1,440 buckets × 1,000 tagsets)
- Requires ValueAwareLapper to prevent incorrect merging of overlapping intervals
- rust_lapper struggles with millions of overlapping intervals
- Queries return millions of duplicate tagset IDs

RoaringBitmap solution:
- Creates **1,440 intervals per day** (one per bucket)
- Each interval has unique boundaries - can use rust_lapper::Lapper directly
- No ValueAwareLapper needed - simpler architecture
- 1,000x reduction in interval count

## Phase 1: Add Dependencies

```toml
# Cargo.toml
[dependencies]
roaring = "0.10"
rust-lapper = "1.2"  # Already a dependency, just use directly now

[dev-dependencies]
criterion = "0.5"  # For benchmarking
```

## Phase 2: Core Data Structure Changes

### Current Structure (ValueAwareLapper Required)
```rust
use crate::value_aware_lapper::ValueAwareLapper;  // Custom wrapper

pub struct ValueAwareLapperCache {
    value_lapper: ValueAwareLapper<u64, usize>,  // Needed for overlapping intervals
    string_dict: StringDictionary<usize>,
    tagsets: IndexSet<EncodedTagSet>,
    resolution: Duration,
}

// Creates overlapping intervals:
// Interval { start: 0, stop: 60, val: tagset_1 }
// Interval { start: 0, stop: 60, val: tagset_2 }
// ... 998 more with same start/stop - requires ValueAwareLapper!
```

### New Structure (Direct Lapper Usage)
```rust
use rust_lapper::Lapper;  // Use directly, no wrapper needed!
use roaring::RoaringBitmap;

pub struct ValueAwareLapperCache {
    lapper: Lapper<u64, RoaringBitmap>,  // Direct usage - simpler!
    string_dict: StringDictionary<usize>,
    tagsets: Vec<EncodedTagSet>,
    tagset_lookup: HashMap<EncodedTagSet, u32>,
    resolution: Duration,
}

// Creates non-overlapping intervals:
// Interval { start: 0, stop: 60, val: RoaringBitmap{1,2,3,...,1000} }
// Only ONE interval per time bucket - no overlap, no custom wrapper needed!
```

## Phase 3: Update Build Logic

### Critical Changes
1. Group by time bucket (not by tagset)
2. Create one interval per bucket
3. Use Lapper directly (no ValueAwareLapper)

```rust
impl ValueAwareLapperCache {
    pub fn from_unsorted_with_resolution(
        points: Vec<(Timestamp, TagSet)>,
        resolution: Duration,
    ) -> Result<Self, CacheBuildError> {
        // Dictionary encode tagsets
        let mut string_dict = StringDictionary::new();
        let mut tagsets = Vec::new();
        let mut tagset_lookup = HashMap::new();

        // KEY CHANGE: Group by time bucket, not by (bucket, tagset)
        let mut interval_map: BTreeMap<u64, RoaringBitmap> = BTreeMap::new();

        for (timestamp, tagset) in points {
            let encoded = encode_tagset(&tagset, &mut string_dict);

            let tagset_id = match tagset_lookup.get(&encoded) {
                Some(&id) => id,
                None => {
                    let id = tagsets.len() as u32;
                    tagsets.push(encoded.clone());
                    tagset_lookup.insert(encoded, id);
                    id
                }
            };

            let bucket = (timestamp / resolution.as_nanos() as u64)
                * resolution.as_nanos() as u64;

            // Add tagset to this bucket's bitmap
            interval_map.entry(bucket)
                .or_insert_with(RoaringBitmap::new)
                .insert(tagset_id);
        }

        // Convert to intervals - ONE per bucket with unique boundaries
        let intervals: Vec<Interval<u64, RoaringBitmap>> = interval_map
            .into_iter()
            .map(|(start, bitmap)| Interval {
                start,
                stop: start + resolution.as_nanos() as u64,
                val: bitmap,  // Contains ALL tagsets for this bucket
            })
            .collect();

        println!("Created {} intervals (vs {} in old implementation)",
                 intervals.len(),
                 points.len());  // Dramatic reduction!

        // KEY CHANGE: Use rust_lapper::Lapper directly!
        let lapper = Lapper::new(intervals);  // No custom wrapper

        Ok(Self {
            lapper,  // Direct Lapper, not ValueAwareLapper
            string_dict,
            tagsets,
            tagset_lookup,
            resolution,
        })
    }
}
```

## Phase 4: Remove ValueAwareLapper Module

Since we no longer need ValueAwareLapper, we can delete the entire module:

```bash
# Remove the custom wrapper module
rm src/value_aware_lapper.rs

# Remove from lib.rs
# Delete: mod value_aware_lapper;
# Delete: pub use value_aware_lapper::ValueAwareLapper;
```

## Phase 5: Update Query Logic

### Range Query - Simplified Without Wrapper
```rust
impl IntervalCache for ValueAwareLapperCache {
    fn query_range(&self, range: &Range<Timestamp>) -> Vec<TagSet> {
        let bitmap = self.query_range_bitmap(range);

        // Bitmap inherently contains unique tagsets only
        bitmap.iter()
            .map(|id| self.decode_tagset(id as usize))
            .collect()
    }
}

impl ValueAwareLapperCache {
    fn query_range_bitmap(&self, range: &Range<Timestamp>) -> RoaringBitmap {
        let mut result = RoaringBitmap::new();

        // Use Lapper's find directly - no wrapper overhead
        for interval in self.lapper.find(range.start, range.end) {
            result |= &interval.val;  // SIMD-optimized union
        }

        result  // Already unique, no HashSet needed!
    }

    fn interval_count(&self) -> usize {
        self.lapper.len()  // Direct access to Lapper methods
    }
}
```

## Phase 6: Migration Strategy

### Side-by-Side Comparison
```rust
pub struct DualCache {
    old_cache: OldValueAwareLapperCache,  // With ValueAwareLapper
    new_cache: BitmapCache,                // Direct Lapper usage
}

impl DualCache {
    pub fn build_and_compare(data: Vec<(Timestamp, TagSet)>) {
        let old_cache = OldValueAwareLapperCache::build(data.clone());
        let new_cache = BitmapCache::build(data);

        println!("Architecture comparison:");
        println!("  Old: ValueAwareLapper wrapper + {} intervals",
                 old_cache.interval_count());
        println!("  New: Direct Lapper usage + {} intervals",
                 new_cache.interval_count());

        // Verify architectural simplification
        assert!(new_cache.interval_count() < old_cache.interval_count() / 100);
    }

    pub fn query_and_compare(&self, range: &Range<Timestamp>) {
        let start = Instant::now();
        let old_result = self.old_cache.query_range(range);
        let old_time = start.elapsed();

        let start = Instant::now();
        let new_result = self.new_cache.query_range(range);
        let new_time = start.elapsed();

        println!("Performance comparison:");
        println!("  Old (ValueAwareLapper): {} tagsets in {:?}",
                 old_result.len(), old_time);
        println!("  New (Direct Lapper): {} tagsets in {:?}",
                 new_result.len(), new_time);
        println!("  Speedup: {:.1}x", old_time.as_secs_f64() / new_time.as_secs_f64());

        assert_eq!(HashSet::from(old_result), HashSet::from(new_result));
    }
}
```

## Phase 7: Benchmarking

### Benchmark Interval Creation
```rust
fn bench_interval_creation(c: &mut Criterion) {
    let data = generate_test_data(1_000_000, 1000);  // 1M points, 1K tagsets

    let mut group = c.benchmark_group("interval_creation");

    group.bench_function("old_value_aware_lapper", |b| {
        b.iter(|| {
            let cache = OldCache::build(black_box(data.clone()));
            println!("Old: {} overlapping intervals with ValueAwareLapper",
                    cache.interval_count());
        })
    });

    group.bench_function("new_direct_lapper", |b| {
        b.iter(|| {
            let cache = BitmapCache::build(black_box(data.clone()));
            println!("New: {} non-overlapping intervals with direct Lapper",
                    cache.interval_count());
        })
    });
}
```

### Benchmark Architectural Simplification
```rust
fn bench_code_complexity(c: &mut Criterion) {
    // Measure the impact of removing ValueAwareLapper

    let mut group = c.benchmark_group("architecture");

    // Old: Goes through ValueAwareLapper wrapper
    group.bench_function("old_wrapper_overhead", |b| {
        let cache = build_old_cache();
        b.iter(|| {
            // ValueAwareLapper -> Lapper -> Binary search
            black_box(cache.value_lapper.find(0, 100))
        })
    });

    // New: Direct Lapper access
    group.bench_function("new_direct_access", |b| {
        let cache = build_new_cache();
        b.iter(|| {
            // Direct Lapper -> Binary search
            black_box(cache.lapper.find(0, 100))
        })
    });
}
```

## Phase 8: Optimization Opportunities

### 1. Pre-compute Full Cache Bitmap
```rust
use std::sync::OnceLock;

pub struct ValueAwareLapperCache {
    lapper: Lapper<u64, RoaringBitmap>,
    // ... other fields ...
    full_cache_bitmap: OnceLock<RoaringBitmap>,
}

impl ValueAwareLapperCache {
    pub fn query_all(&self) -> RoaringBitmap {
        self.full_cache_bitmap.get_or_init(|| {
            let mut result = RoaringBitmap::new();
            // Direct iteration over Lapper
            for interval in self.lapper.iter() {
                result |= &interval.val;
            }
            result
        }).clone()
    }
}
```

### 2. Leverage Lapper's Built-in Methods
```rust
impl ValueAwareLapperCache {
    // Use Lapper's coverage directly
    pub fn coverage(&self) -> f64 {
        self.lapper.coverage()
    }

    // Use Lapper's count
    pub fn interval_count(&self) -> usize {
        self.lapper.len()
    }

    // Direct access to intervals
    pub fn iter_intervals(&self) -> impl Iterator<Item = &Interval<u64, RoaringBitmap>> {
        self.lapper.iter()
    }
}
```

## Testing Strategy

### Correctness Test
```rust
#[test]
fn test_identical_results() {
    for cardinality in [10, 100, 1000, 10000] {
        let data = generate_test_data(100_000, cardinality);

        // Old implementation with ValueAwareLapper
        let old_cache = OldCache::build(data.clone());

        // New implementation with direct Lapper
        let new_cache = BitmapCache::build(data);

        // Verify interval reduction
        assert!(new_cache.lapper.len() < old_cache.value_lapper.len() / cardinality,
                "Should have roughly 1/cardinality fewer intervals");

        // Test various query patterns
        let ranges = vec![
            0..u64::MAX,           // Full cache
            0..3600_000_000_000,   // First hour
            0..86400_000_000_000,  // First day
        ];

        for range in ranges {
            let old_result: HashSet<_> = old_cache.query_range(&range)
                .into_iter().collect();
            let new_result: HashSet<_> = new_cache.query_range(&range)
                .into_iter().collect();

            assert_eq!(old_result, new_result,
                      "Results differ for cardinality {}", cardinality);
        }
    }
}
```

### Architecture Simplification Test
```rust
#[test]
fn test_no_value_aware_lapper_needed() {
    let data = generate_daily_data(1000);  // 1K tagsets, 1 day
    let cache = BitmapCache::build(data);

    // Verify no overlapping intervals
    let intervals: Vec<_> = cache.lapper.iter().collect();
    for i in 0..intervals.len() - 1 {
        assert!(intervals[i].stop <= intervals[i + 1].start,
                "Intervals should not overlap: {:?} and {:?}",
                intervals[i], intervals[i + 1]);
    }

    // Verify we can use Lapper methods directly
    let coverage = cache.lapper.coverage();
    assert!(coverage > 0.0, "Should have coverage");
}
```

## Rollout Plan

1. **Week 1**: Implement RoaringBitmap with direct Lapper usage
2. **Week 2**: Remove ValueAwareLapper module and tests
3. **Week 3**: Deploy behind feature flag to staging
4. **Week 4**: A/B test showing architectural simplification
5. **Week 5**: Full rollout and celebrate simpler codebase

## Monitoring Metrics

After deployment, monitor:
- **Interval count reduction**: 100-1,000x fewer intervals
- **Query latency**: 100-250x improvement expected
- **Code complexity**: Lines of code reduced (ValueAwareLapper removed)
- **Binary size**: Should decrease slightly
- **Maintenance burden**: Fewer custom components

## Success Criteria

✅ **Interval reduction**: 100-1,000x fewer intervals (buckets only, not buckets×tagsets)
✅ **Query performance**: 100-250x faster for full cache queries
✅ **Architectural simplification**: ValueAwareLapper module removed
✅ **Direct Lapper usage**: No custom wrapper needed
✅ **Correctness**: Identical query results to current implementation
✅ **Maintainability**: Simpler codebase with fewer custom components