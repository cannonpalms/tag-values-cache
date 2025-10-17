use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::collections::BTreeMap;
use tag_values_cache::{
    ArrowValue, BTreeCache, IntervalCache, IntervalTreeCache, LapperCache, NCListCache,
    RecordBatchRow, SortedData, UnmergedBTreeCache, ValueLapperCache, VecCache,
};

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

/// Generate synthetic data with the specified cardinality and value size
/// Cardinality is the number of unique tag value combinations (excluding time)
/// Value size is the approximate size in bytes of each tag value string
fn generate_data(num_rows: usize, cardinality: usize, value_size: usize) -> Vec<(u64, RecordBatchRow)> {
    let mut data = Vec::with_capacity(num_rows);

    // Generate 4 tag columns similar to the real benchmark
    // Each will have roughly cardinality^(1/4) unique values so that
    // the total number of unique combinations across 4 columns equals cardinality
    let values_per_column = (cardinality as f64).powf(0.25).ceil() as usize;

    for i in 0..num_rows {
        let timestamp = i as u64 * 1000; // Timestamps in milliseconds, incrementing

        let mut values = BTreeMap::new();

        // Generate tag values cycling through the cardinality space
        let combo_idx = i % cardinality;

        values.insert(
            "WithHash".to_string(),
            ArrowValue::String(generate_value(combo_idx % values_per_column, value_size)),
        );
        values.insert(
            "CounterID".to_string(),
            ArrowValue::String(generate_value((combo_idx / values_per_column) % values_per_column, value_size)),
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

    data
}

/// Benchmark building caches from scratch
fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("build");

    // Test with different cardinalities up to 1M and different value sizes
    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            let num_rows = 100_000; // Fixed number of rows
            let data = generate_data(num_rows, cardinality, value_size);
            let sorted_data = SortedData::from_unsorted(data);

            group.throughput(Throughput::Elements(num_rows as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            group.bench_with_input(
                BenchmarkId::new("IntervalTreeCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = IntervalTreeCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("VecCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = VecCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("LapperCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = LapperCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("ValueLapperCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = ValueLapperCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("BTreeCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = BTreeCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("NCListCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = NCListCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("UnmergedBTreeCache", &param_name),
                &sorted_data,
                |b, data| {
                    b.iter(|| {
                        let cache = UnmergedBTreeCache::from_sorted(data.clone()).unwrap();
                        black_box(cache);
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark appending data to existing caches
fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");

    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            let initial_rows = 50_000;
            let append_rows = 50_000;

            let initial_data = generate_data(initial_rows, cardinality, value_size);
            let append_data = generate_data(append_rows, cardinality, value_size);

            let sorted_initial = SortedData::from_unsorted(initial_data);
            let sorted_append = SortedData::from_unsorted(append_data);

            group.throughput(Throughput::Elements(append_rows as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            group.bench_with_input(
                BenchmarkId::new("IntervalTreeCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || IntervalTreeCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("VecCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || VecCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("LapperCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || LapperCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("ValueLapperCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || ValueLapperCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("BTreeCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || BTreeCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("NCListCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || NCListCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );

            group.bench_with_input(
                BenchmarkId::new("UnmergedBTreeCache", &param_name),
                &(&sorted_initial, &sorted_append),
                |b, (initial, append)| {
                    b.iter_batched(
                        || UnmergedBTreeCache::from_sorted((*initial).clone()).unwrap(),
                        |mut cache| {
                            cache.append_sorted((*append).clone()).unwrap();
                            black_box(cache);
                        },
                        criterion::BatchSize::SmallInput,
                    );
                },
            );
        }
    }

    group.finish();
}

/// Benchmark point queries
fn bench_point_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_query");

    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            let num_rows = 100_000;
            let data = generate_data(num_rows, cardinality, value_size);
            let sorted_data = SortedData::from_unsorted(data.clone());

            // Create test points - sample every 100th timestamp
            let test_points: Vec<u64> = (0..1000).map(|i| (i * 100) as u64 * 1000).collect();

            group.throughput(Throughput::Elements(test_points.len() as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            // Build caches once
            let tree_cache = IntervalTreeCache::from_sorted(sorted_data.clone()).unwrap();
            let vec_cache = VecCache::from_sorted(sorted_data.clone()).unwrap();
            let lapper_cache = LapperCache::from_sorted(sorted_data.clone()).unwrap();
            let value_lapper_cache = ValueLapperCache::from_sorted(sorted_data.clone()).unwrap();
            let btree_cache = BTreeCache::from_sorted(sorted_data.clone()).unwrap();
            let nclist_cache = NCListCache::from_sorted(sorted_data.clone()).unwrap();
            let unmerged_btree_cache = UnmergedBTreeCache::from_sorted(sorted_data).unwrap();

            group.bench_with_input(
                BenchmarkId::new("IntervalTreeCache", &param_name),
                &(&tree_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("VecCache", &param_name),
                &(&vec_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("LapperCache", &param_name),
                &(&lapper_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("ValueLapperCache", &param_name),
                &(&value_lapper_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("BTreeCache", &param_name),
                &(&btree_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("NCListCache", &param_name),
                &(&nclist_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("UnmergedBTreeCache", &param_name),
                &(&unmerged_btree_cache, &test_points),
                |b, (cache, points)| {
                    b.iter(|| {
                        for &t in *points {
                            black_box(cache.query_point(t));
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark range queries
fn bench_range_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");

    for cardinality in [100, 1_000, 10_000, 100_000, 1_000_000] {
        for value_size in [8, 128, 512, 1024] {
            let num_rows = 100_000;
            let data = generate_data(num_rows, cardinality, value_size);
            let sorted_data = SortedData::from_unsorted(data.clone());

            // Create test ranges - 100 ranges each covering 1% of the total time span
            let min_ts = 0u64;
            let max_ts = (num_rows as u64) * 1000;
            let range_size = (max_ts - min_ts) / 100;
            let test_ranges: Vec<std::ops::Range<u64>> = (0..100)
                .map(|i| {
                    let start = min_ts + i * range_size;
                    let end = start + range_size;
                    start..end
                })
                .collect();

            group.throughput(Throughput::Elements(test_ranges.len() as u64));

            let param_name = format!("card={}/size={}", cardinality, value_size);

            // Build caches once
            let tree_cache = IntervalTreeCache::from_sorted(sorted_data.clone()).unwrap();
            let vec_cache = VecCache::from_sorted(sorted_data.clone()).unwrap();
            let lapper_cache = LapperCache::from_sorted(sorted_data.clone()).unwrap();
            let value_lapper_cache = ValueLapperCache::from_sorted(sorted_data.clone()).unwrap();
            let btree_cache = BTreeCache::from_sorted(sorted_data.clone()).unwrap();
            let nclist_cache = NCListCache::from_sorted(sorted_data.clone()).unwrap();
            let unmerged_btree_cache = UnmergedBTreeCache::from_sorted(sorted_data).unwrap();

            group.bench_with_input(
                BenchmarkId::new("IntervalTreeCache", &param_name),
                &(&tree_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("VecCache", &param_name),
                &(&vec_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("LapperCache", &param_name),
                &(&lapper_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("ValueLapperCache", &param_name),
                &(&value_lapper_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("BTreeCache", &param_name),
                &(&btree_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("NCListCache", &param_name),
                &(&nclist_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new("UnmergedBTreeCache", &param_name),
                &(&unmerged_btree_cache, &test_ranges),
                |b, (cache, ranges)| {
                    b.iter(|| {
                        for range in *ranges {
                            black_box(cache.query_range(range.clone()));
                        }
                    });
                },
            );
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
