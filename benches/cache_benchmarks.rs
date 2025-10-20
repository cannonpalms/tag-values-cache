use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::collections::BTreeMap;
use tag_values_cache::{
    ArrowValue, BTreeCache, IntervalCache, IntervalTreeCache, LapperCache, NCListCache,
    RecordBatchRow, SortedData, UnmergedBTreeCache, ValueLapperCache, VecCache,
};

/// Linear-feedback shift register based PRNG.
///
/// Generates 4,294,967,295 unique values before cycling.
#[derive(Debug, Clone)]
pub struct Lfsr(u32);

impl Default for Lfsr {
    fn default() -> Self {
        Self(42)
    }
}

impl Lfsr {
    pub fn new(seed: u32) -> Self {
        Self(seed)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> u32 {
        let lsb = self.0 & 1;
        self.0 >>= 1;
        if lsb == 1 {
            self.0 ^= 0x80000057
        }
        self.0
    }

    /// Return a valid timestamp (u64) by combining two u32 values
    pub fn next_timestamp(&mut self) -> u64 {
        let high = self.next() as u64;
        let low = self.next() as u64;
        (high << 32) | low
    }
}

/// Generate a string value of the specified size
fn generate_value(base_value: usize, value_size: usize) -> String {
    let base = base_value.to_string();
    if value_size <= base.len() {
        base
    } else {
        // Pad with repeated pattern to reach desired size
        let padding_needed = value_size - base.len();
        let pattern = "0123456789abcdefghijklmnopqrstuvwxyz";
        let mut result = base;
        result.reserve(padding_needed);
        for i in 0..padding_needed {
            result.push(pattern.as_bytes()[i % pattern.len()] as char);
        }
        result
    }
}

/// Generate synthetic data with the specified cardinality and value size using LFSR
/// Cardinality is the number of unique tag value combinations (excluding time)
/// Value size is the approximate size in bytes of each tag value string
/// Returns the data (sorted by timestamp only) and the LFSR state after generation
fn generate_data_with_lfsr(
    num_rows: usize,
    cardinality: usize,
    value_size: usize,
    mut lfsr: Lfsr,
) -> (Vec<(u64, RecordBatchRow)>, Lfsr) {
    let mut data = Vec::with_capacity(num_rows);

    // Generate 4 tag columns similar to the real benchmark
    // Each will have roughly cardinality^(1/4) unique values so that
    // the total number of unique combinations across 4 columns equals cardinality
    let values_per_column = (cardinality as f64).powf(0.25).ceil() as usize;

    for i in 0..num_rows {
        // Use LFSR to generate deterministic timestamps
        let timestamp = lfsr.next_timestamp();

        let mut values = BTreeMap::new();

        // Generate tag values cycling through the cardinality space
        let combo_idx = i % cardinality;

        values.insert(
            "WithHash".to_string(),
            ArrowValue::String(generate_value(combo_idx % values_per_column, value_size)),
        );
        values.insert(
            "CounterID".to_string(),
            ArrowValue::String(generate_value(
                (combo_idx / values_per_column) % values_per_column,
                value_size,
            )),
        );
        values.insert(
            "CookieEnable".to_string(),
            ArrowValue::String(generate_value(
                (combo_idx / (values_per_column * values_per_column)) % values_per_column,
                value_size,
            )),
        );
        values.insert(
            "URLHash".to_string(),
            ArrowValue::String(generate_value(
                (combo_idx / (values_per_column * values_per_column * values_per_column))
                    % values_per_column,
                value_size,
            )),
        );

        data.push((timestamp, RecordBatchRow::new(values)));
    }

    // Sort by timestamp only (not by RecordBatchRow which is expensive)
    // This is much faster than SortedData::from_unsorted which sorts by both timestamp and value
    data.sort_by_key(|(ts, _)| *ts);

    (data, lfsr)
}

/// Benchmark building caches from scratch
fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("build");

    // Test with different cardinalities up to 1M and different value sizes
    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            // Use 1M rows for 1M cardinality, 100K rows for everything else
            let num_rows = if cardinality == 1_000_000 { 1_000_000 } else { 100_000 };

            group.throughput(Throughput::Elements(num_rows as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            // Macro to generate data, build cache, benchmark, then drop
            macro_rules! bench_cache_build {
                ($cache_type:ty, $name:expr) => {
                    let lfsr = Lfsr::default();
                    let (data, _) = generate_data_with_lfsr(num_rows, cardinality, value_size, lfsr);
                    let sorted_data = SortedData::from_sorted(data);

                    group.bench_with_input(
                        BenchmarkId::new($name, &param_name),
                        &sorted_data,
                        |b, data| {
                            b.iter(|| {
                                let cache = <$cache_type>::from_sorted(data.clone()).unwrap();
                                black_box(cache);
                            });
                        },
                    );
                    // sorted_data is dropped here
                };
            }

            bench_cache_build!(IntervalTreeCache<RecordBatchRow>, "IntervalTreeCache");
            bench_cache_build!(VecCache<RecordBatchRow>, "VecCache");
            bench_cache_build!(LapperCache<RecordBatchRow>, "LapperCache");
            bench_cache_build!(ValueLapperCache<RecordBatchRow>, "ValueLapperCache");
            bench_cache_build!(BTreeCache<RecordBatchRow>, "BTreeCache");
            bench_cache_build!(NCListCache<RecordBatchRow>, "NCListCache");
            bench_cache_build!(UnmergedBTreeCache<RecordBatchRow>, "UnmergedBTreeCache");
        }
    }

    group.finish();
}

/// Benchmark appending data to existing caches
fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");

    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            // Use 1M rows for 1M cardinality, 100K rows for everything else, split 50/50
            let total_rows = if cardinality == 1_000_000 { 1_000_000 } else { 100_000 };
            let initial_rows = total_rows / 2;
            let append_rows = total_rows - initial_rows;

            group.throughput(Throughput::Elements(append_rows as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            // Macro to generate data, build cache, benchmark append, then drop
            macro_rules! bench_cache_append {
                ($cache_type:ty, $name:expr) => {
                    let lfsr = Lfsr::default();
                    let (initial_data, lfsr) =
                        generate_data_with_lfsr(initial_rows, cardinality, value_size, lfsr);
                    let (append_data, _) =
                        generate_data_with_lfsr(append_rows, cardinality, value_size, lfsr);

                    let sorted_initial = SortedData::from_sorted(initial_data);
                    let sorted_append = SortedData::from_sorted(append_data);

                    group.bench_with_input(
                        BenchmarkId::new($name, &param_name),
                        &(&sorted_initial, &sorted_append),
                        |b, (initial, append)| {
                            b.iter_batched(
                                || <$cache_type>::from_sorted((*initial).clone()).unwrap(),
                                |mut cache| {
                                    cache.append_sorted((*append).clone()).unwrap();
                                    black_box(cache);
                                },
                                criterion::BatchSize::SmallInput,
                            );
                        },
                    );
                    // sorted_initial and sorted_append are dropped here
                };
            }

            bench_cache_append!(IntervalTreeCache<RecordBatchRow>, "IntervalTreeCache");
            bench_cache_append!(VecCache<RecordBatchRow>, "VecCache");
            bench_cache_append!(LapperCache<RecordBatchRow>, "LapperCache");
            bench_cache_append!(ValueLapperCache<RecordBatchRow>, "ValueLapperCache");
            bench_cache_append!(BTreeCache<RecordBatchRow>, "BTreeCache");
            bench_cache_append!(NCListCache<RecordBatchRow>, "NCListCache");
            bench_cache_append!(UnmergedBTreeCache<RecordBatchRow>, "UnmergedBTreeCache");
        }
    }

    group.finish();
}

/// Benchmark point queries - both hits and misses
fn bench_point_queries(c: &mut Criterion) {
    let n_lookups = 1000;
    let mut group = c.benchmark_group(format!("point_query/n_queries={n_lookups}"));

    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            // Use 1M rows for 1M cardinality, 100K rows for everything else
            let num_rows = if cardinality == 1_000_000 { 1_000_000 } else { 100_000 };

            // Generate data with LFSR
            let lfsr = Lfsr::default();
            let (data, lfsr_after_data) =
                generate_data_with_lfsr(num_rows, cardinality, value_size, lfsr);
            let sorted_data = SortedData::from_sorted(data.clone());

            group.throughput(Throughput::Elements(n_lookups as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            // Macro to build cache, benchmark, then drop to save memory
            macro_rules! bench_cache_hits_misses {
                ($cache_type:ty, $name:expr) => {
                    // Build cache once for this benchmark
                    let cache = <$cache_type>::from_sorted(sorted_data.clone()).unwrap();

                    // Hits: Use fresh LFSR with same seed to regenerate the same timestamps
                    group.bench_function(
                        BenchmarkId::new(concat!($name, "/hits"), &param_name),
                        |b| {
                            b.iter_batched(
                                || Lfsr::default(), // Fresh LFSR for each iteration
                                |mut lfsr| {
                                    for _ in 0..n_lookups {
                                        let t = lfsr.next_timestamp();
                                        black_box(cache.query_point(t));
                                    }
                                },
                                criterion::BatchSize::SmallInput,
                            );
                        },
                    );

                    // Misses: Use LFSR state after data generation to get new timestamps
                    group.bench_function(
                        BenchmarkId::new(concat!($name, "/misses"), &param_name),
                        |b| {
                            b.iter_batched(
                                || lfsr_after_data.clone(), // LFSR continuing from after data gen
                                |mut lfsr| {
                                    for _ in 0..n_lookups {
                                        let t = lfsr.next_timestamp();
                                        black_box(cache.query_point(t));
                                    }
                                },
                                criterion::BatchSize::SmallInput,
                            );
                        },
                    );

                    // Cache is dropped here
                };
            }

            bench_cache_hits_misses!(IntervalTreeCache<RecordBatchRow>, "IntervalTreeCache");
            bench_cache_hits_misses!(VecCache<RecordBatchRow>, "VecCache");
            bench_cache_hits_misses!(LapperCache<RecordBatchRow>, "LapperCache");
            bench_cache_hits_misses!(ValueLapperCache<RecordBatchRow>, "ValueLapperCache");
            bench_cache_hits_misses!(BTreeCache<RecordBatchRow>, "BTreeCache");
            bench_cache_hits_misses!(NCListCache<RecordBatchRow>, "NCListCache");
            bench_cache_hits_misses!(UnmergedBTreeCache<RecordBatchRow>, "UnmergedBTreeCache");
        }
    }

    group.finish();
}

/// Benchmark range queries with deterministic data
fn bench_range_queries(c: &mut Criterion) {
    let n_ranges = 100;
    let mut group = c.benchmark_group(format!("range_query/n_queries={n_ranges}"));

    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            // Use 1M rows for 1M cardinality, 100K rows for everything else
            let num_rows = if cardinality == 1_000_000 { 1_000_000 } else { 100_000 };

            // Generate data with LFSR
            let lfsr = Lfsr::default();
            let (data, _) = generate_data_with_lfsr(num_rows, cardinality, value_size, lfsr);
            let sorted_data = SortedData::from_sorted(data.clone());

            // Get min and max timestamps from actual data
            let timestamps: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
            let min_ts = *timestamps.iter().min().unwrap();
            let max_ts = *timestamps.iter().max().unwrap();

            // Create test ranges that cover the actual time span
            let range_size = (max_ts - min_ts) / (n_ranges as u64);
            let test_ranges: Vec<std::ops::Range<u64>> = (0..n_ranges)
                .map(|i| {
                    let start = min_ts + (i as u64) * range_size;
                    let end = start + range_size;
                    start..end
                })
                .collect();

            group.throughput(Throughput::Elements(n_ranges as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            // Macro to build cache, benchmark, then drop to save memory
            macro_rules! bench_cache_range {
                ($cache_type:ty, $name:expr) => {
                    // Build cache once for this benchmark
                    let cache = <$cache_type>::from_sorted(sorted_data.clone()).unwrap();

                    group.bench_with_input(
                        BenchmarkId::new($name, &param_name),
                        &(&cache, &test_ranges),
                        |b, (cache, ranges)| {
                            b.iter(|| {
                                for range in *ranges {
                                    black_box(cache.query_range(range.clone()));
                                }
                            });
                        },
                    );

                    // Cache is dropped here
                };
            }

            bench_cache_range!(IntervalTreeCache<RecordBatchRow>, "IntervalTreeCache");
            bench_cache_range!(VecCache<RecordBatchRow>, "VecCache");
            bench_cache_range!(LapperCache<RecordBatchRow>, "LapperCache");
            bench_cache_range!(ValueLapperCache<RecordBatchRow>, "ValueLapperCache");
            bench_cache_range!(BTreeCache<RecordBatchRow>, "BTreeCache");
            bench_cache_range!(NCListCache<RecordBatchRow>, "NCListCache");
            bench_cache_range!(UnmergedBTreeCache<RecordBatchRow>, "UnmergedBTreeCache");
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_build,
    bench_append,
    bench_point_queries,
    bench_range_queries
);
criterion_main!(benches);
