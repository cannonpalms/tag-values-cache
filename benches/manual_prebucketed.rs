//! Benchmark for building BitmapLapperCache from manually created pre-bucketed data
//!
//! This benchmark creates in-memory pre-bucketed (timestamp, tagset) data with
//! arbitrary cardinality N, where for N=1, we create 1 row per time bucket.
//!
//! The benchmark covers 24 hours of data at 1-hour resolution (24 buckets total).
//! It measures the time taken to BUILD a BitmapLapperCache from this pre-bucketed data.
//!
//! # Environment Variables
//! - `BENCH_CARDINALITY`: Number of unique tagsets (default: 1, 10, 100, 1000, 10000)
//!
//! # Examples
//! ```bash
//! # Run with default cardinalities
//! cargo bench --bench manual_prebucketed
//!
//! # Run with specific cardinality
//! BENCH_CARDINALITY=5000 cargo bench --bench manual_prebucketed
//! ```

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use std::time::Duration;
use tag_values_cache::{
    BitmapLapperCache, IntervalCache, SortedData, TagSet, Timestamp,
    tag_generator::{factorize_cardinality, generate_tagset_for_index},
};

/// Configuration for the benchmark
struct BenchConfig {
    /// Number of unique tagsets
    cardinality: usize,
    /// Total duration to cover
    total_duration: Duration,
    /// Resolution for bucketing
    resolution: Duration,
}

impl BenchConfig {
    /// Create configuration from environment or use defaults
    fn from_env() -> Vec<Self> {
        if let Ok(cardinality_str) = std::env::var("BENCH_CARDINALITY") {
            if let Ok(cardinality) = cardinality_str.parse() {
                vec![Self {
                    cardinality,
                    total_duration: Duration::from_secs(24 * 3600), // 24 hours
                    resolution: Duration::from_secs(3600),          // 1 hour
                }]
            } else {
                eprintln!("Invalid BENCH_CARDINALITY value, using defaults");
                Self::default_configs()
            }
        } else {
            Self::default_configs()
        }
    }

    fn default_configs() -> Vec<Self> {
        vec![1_000, 10_000, 100_000, 1_000_000, 2_000_000]
            .into_iter()
            .map(|cardinality| Self {
                cardinality,
                total_duration: Duration::from_secs(24 * 3600), // 24 hours
                resolution: Duration::from_secs(3600),          // 1 hour
            })
            .collect()
    }
}

/// Generate pre-bucketed data with the specified cardinality
///
/// For cardinality N, creates N unique tagsets using the standard 8-tag schema.
/// Each tagset appears once per time bucket (24 buckets for 24 hours at 1-hour resolution).
/// This results in exactly N rows per bucket, and N * 24 total rows.
fn generate_prebucketed_data(config: &BenchConfig) -> Vec<(Timestamp, TagSet)> {
    let mut data = Vec::new();

    // Calculate number of buckets
    let num_buckets = config.total_duration.as_secs() / config.resolution.as_secs();

    // Factorize cardinality across 8 tags (matching the schema)
    let tag_cardinalities = factorize_cardinality(config.cardinality, 8);

    // Generate unique tagsets using the standard schema
    let tagsets: Vec<TagSet> = (0..config.cardinality)
        .map(|i| generate_tagset_for_index(i, &tag_cardinalities))
        .collect();

    // For each bucket, add all tagsets
    for bucket_idx in 0..num_buckets {
        let timestamp = bucket_idx * config.resolution.as_nanos() as u64;

        for tagset in &tagsets {
            data.push((timestamp, tagset.clone()));
        }
    }

    // Data is already sorted by timestamp (bucket order), which is what we want
    data
}

/// Benchmark cache build from pre-bucketed data
fn bench_cache_build_prebucketed(c: &mut Criterion) {
    let configs = BenchConfig::from_env();

    let mut group = c.benchmark_group("manual_prebucketed_cache_build");

    for config in configs {
        let cardinality = config.cardinality;

        // Generate the data once
        let data = generate_prebucketed_data(&config);
        let total_rows = data.len();

        println!("\n=== Pre-bucketed Cache Construction ===");
        println!("Cardinality: {} unique tagsets", cardinality);
        println!("Time range: 24 hours");
        println!("Resolution: 1 hour (24 buckets)");
        println!("Total rows: {} ({} rows/bucket)", total_rows, cardinality);
        println!("Expected intervals in cache: 24");

        group.throughput(Throughput::Elements(total_rows as u64));

        // New benchmark using from_prebucketed (most optimized path)
        let cardinality_label = if cardinality >= 1_000_000 {
            format!("{}M", cardinality as f64 / 1_000_000.0)
        } else {
            format!("{}K", cardinality / 1_000)
        };

        group.bench_with_input(
            BenchmarkId::new("BitmapLapperCache_from_prebucketed", &cardinality_label),
            &config,
            |b, cfg| {
                b.iter_batched(
                    || {
                        // Clone the pre-generated data for each batch
                        data.clone()
                    },
                    |data| {
                        // Build the cache using the new prebucketed function
                        let cache = BitmapLapperCache::from_prebucketed(data, cfg.resolution)
                            .expect("Failed to build cache");

                        black_box(cache);
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("BitmapLapperCache_from_sorted", &cardinality_label),
            &config,
            |b, cfg| {
                b.iter_batched(
                    || {
                        // Clone the pre-generated data for each batch
                        data.clone()
                    },
                    |data| {
                        // Create SortedData (data is already sorted)
                        let sorted_data = SortedData::from_sorted(data);

                        // Build the cache
                        let cache = BitmapLapperCache::from_sorted_with_resolution(
                            sorted_data,
                            cfg.resolution,
                        )
                        .expect("Failed to build cache");

                        black_box(cache);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

/// Verify the cache was built correctly by checking its properties
fn bench_cache_verification(c: &mut Criterion) {
    let configs = BenchConfig::from_env();

    let mut group = c.benchmark_group("manual_prebucketed_verification");
    group.sample_size(10); // Smaller sample size for verification

    for config in configs {
        let cardinality = config.cardinality;

        // Generate the data and build cache once
        let data = generate_prebucketed_data(&config);
        let sorted_data = SortedData::from_sorted(data);
        let cache = BitmapLapperCache::from_sorted_with_resolution(sorted_data, config.resolution)
            .expect("Failed to build cache");

        println!(
            "\n=== Cache Verification for Cardinality {} ===",
            cardinality
        );
        println!("Interval count: {}", cache.interval_count());
        println!("Unique tagsets: {}", cache.unique_tagsets());
        println!("Cache size: {} bytes", cache.size_bytes());

        if let Some(min_ts) = cache.min_timestamp() {
            println!("Min timestamp: {}", min_ts);
        }
        if let Some(max_ts) = cache.max_timestamp() {
            println!("Max timestamp: {}", max_ts);
        }

        // Quick sanity check
        assert_eq!(
            cache.interval_count(),
            24,
            "Should have exactly 24 intervals (1 per hour)"
        );
        assert_eq!(
            cache.unique_tagsets(),
            cardinality,
            "Should have exactly {} unique tagsets",
            cardinality
        );

        // Benchmark a full range query to verify the cache works
        let min_ts = cache.min_timestamp().unwrap();
        let max_ts = cache.max_timestamp().unwrap();
        let range = min_ts..max_ts;

        let cardinality_label = if cardinality >= 1_000_000 {
            format!("{}M", cardinality as f64 / 1_000_000.0)
        } else {
            format!("{}K", cardinality / 1_000)
        };

        group.bench_with_input(
            BenchmarkId::new("query_full_range", &cardinality_label),
            &range,
            |b, r| {
                b.iter(|| {
                    let result = cache.query_range(r);
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_cache_build_prebucketed,
    bench_cache_verification
);
criterion_main!(benches);
