//! Verify that streaming and batch methods are actually loading identical data
//!
//! This test exposes the differences in data loading between the two methods.
//!
//! Run with: BENCH_INPUT_PATH=path/to/parquet cargo run --example verify_identical_loading

use std::env;
use tag_values_cache::extract_tags_from_batch;

// Import the data_loader module from benchmarks
#[path = "../benches/data_loader.rs"]
mod data_loader;

use data_loader::{BenchConfig, load_data, load_record_batches};

fn main() {
    println!("=== Data Loading Verification ===\n");

    // First, print the configuration being used
    let config = BenchConfig::from_env();
    config.print_config();

    // Load data using batch method (load_parquet_data)
    println!("\n--- Loading data for BATCH method (load_parquet_data) ---");
    let batch_data = match load_data(&config) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error loading batch data: {}", e);
            std::process::exit(1);
        }
    };
    println!("Batch method loaded: {} points", batch_data.len());

    // Calculate cardinality for batch data
    let batch_cardinality = {
        let mut unique = std::collections::HashSet::new();
        for (_, tagset) in &batch_data {
            let mut combo = Vec::new();
            for (k, v) in tagset {
                combo.push(format!("{}={}", k, v));
            }
            combo.sort();
            unique.insert(combo.join(","));
        }
        unique.len()
    };
    println!(
        "Batch data cardinality: {} unique tag combinations",
        batch_cardinality
    );

    // Load data using streaming method (load_record_batches)
    println!("\n--- Loading data for STREAMING method (load_record_batches) ---");
    let record_batches = match load_record_batches(&config) {
        Ok(batches) => batches,
        Err(e) => {
            eprintln!("Error loading record batches: {}", e);
            std::process::exit(1);
        }
    };

    // Extract data from record batches to compare
    let mut streaming_data = Vec::new();
    for batch in &record_batches {
        let points = extract_tags_from_batch(batch);
        streaming_data.extend(points);
    }
    println!("Streaming method loaded: {} points", streaming_data.len());

    // Calculate cardinality for streaming data
    let streaming_cardinality = {
        let mut unique = std::collections::HashSet::new();
        for (_, tagset) in &streaming_data {
            let mut combo = Vec::new();
            for (k, v) in tagset {
                combo.push(format!("{}={}", k, v));
            }
            combo.sort();
            unique.insert(combo.join(","));
        }
        unique.len()
    };
    println!(
        "Streaming data cardinality: {} unique tag combinations",
        streaming_cardinality
    );

    // Compare the data
    println!("\n=== Comparison Results ===");

    let count_match = batch_data.len() == streaming_data.len();
    if count_match {
        println!("✅ Data point counts match: {}", batch_data.len());
    } else {
        println!("❌ Data point count MISMATCH!");
        println!("   Batch:     {} points", batch_data.len());
        println!("   Streaming: {} points", streaming_data.len());
        println!(
            "   Difference: {} points",
            (batch_data.len() as i64 - streaming_data.len() as i64).abs()
        );
    }

    let cardinality_match = batch_cardinality == streaming_cardinality;
    if cardinality_match {
        println!("✅ Cardinality matches: {}", batch_cardinality);
    } else {
        println!("❌ Cardinality MISMATCH!");
        println!("   Batch:     {} unique combinations", batch_cardinality);
        println!(
            "   Streaming: {} unique combinations",
            streaming_cardinality
        );
    }

    // Check if batch data is a subset of streaming data (due to cardinality filtering)
    if !count_match && batch_data.len() < streaming_data.len() {
        println!(
            "\n🔍 Investigating: Batch has fewer points. Checking if it's due to filtering..."
        );

        // Check if BENCH_MAX_CARDINALITY is set
        if let Ok(max_card) = env::var("BENCH_MAX_CARDINALITY") {
            println!("   BENCH_MAX_CARDINALITY is set to: {}", max_card);
            println!("   This explains why batch method has fewer points!");
            println!("   Batch method applies cardinality filtering, streaming does not.");
        }

        // Check if max duration might be the cause
        if batch_data.len() < streaming_data.len() {
            let batch_duration = {
                let min = batch_data.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
                let max = batch_data.iter().map(|(ts, _)| *ts).max().unwrap_or(0);
                max - min
            };
            let streaming_duration = {
                let min = streaming_data.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
                let max = streaming_data.iter().map(|(ts, _)| *ts).max().unwrap_or(0);
                max - min
            };

            if batch_duration < streaming_duration {
                println!("   Batch duration: {} ns", batch_duration);
                println!("   Streaming duration: {} ns", streaming_duration);
                println!("   Batch method may have stopped early due to duration limit.");
            }
        }
    }

    // For exact comparison, sort both datasets and check if one is a subset
    let mut batch_sorted = batch_data.clone();
    let mut streaming_sorted = streaming_data.clone();

    batch_sorted.sort_by_key(|(ts, tags)| (*ts, format!("{:?}", tags)));
    streaming_sorted.sort_by_key(|(ts, tags)| (*ts, format!("{:?}", tags)));

    if batch_sorted == streaming_sorted {
        println!("\n✅ Data is IDENTICAL (after sorting)");
    } else {
        println!("\n❌ Data is DIFFERENT");

        // Check if batch is a subset of streaming
        let batch_set: std::collections::HashSet<_> = batch_sorted
            .iter()
            .map(|(ts, tags)| (*ts, format!("{:?}", tags)))
            .collect();
        let streaming_set: std::collections::HashSet<_> = streaming_sorted
            .iter()
            .map(|(ts, tags)| (*ts, format!("{:?}", tags)))
            .collect();

        if batch_set.is_subset(&streaming_set) {
            println!("   Batch data is a SUBSET of streaming data");
            println!("   This is likely due to cardinality or duration filtering in batch method");
        } else if streaming_set.is_subset(&batch_set) {
            println!("   Streaming data is a SUBSET of batch data");
            println!("   This is unexpected and indicates a bug");
        } else {
            println!("   Neither is a subset of the other - they have different data points!");

            // Find some examples of differences
            let batch_only: Vec<_> = batch_set.difference(&streaming_set).take(3).collect();
            let streaming_only: Vec<_> = streaming_set.difference(&batch_set).take(3).collect();

            if !batch_only.is_empty() {
                println!("\n   Examples of data only in batch method:");
                for item in batch_only {
                    println!("     {:?}", item);
                }
            }

            if !streaming_only.is_empty() {
                println!("\n   Examples of data only in streaming method:");
                for item in streaming_only {
                    println!("     {:?}", item);
                }
            }
        }
    }

    println!("\n=== Summary ===");
    if count_match && cardinality_match {
        println!("✅ Both methods load IDENTICAL data");
        println!("The cache differences must be due to processing, not data loading.");
    } else {
        println!("⚠️  WARNING: Methods are loading DIFFERENT data!");
        println!("This explains why streaming and batch builds produce different caches.");
        println!("\nTo ensure fair comparison:");
        println!("1. Unset BENCH_MAX_CARDINALITY to disable cardinality filtering");
        println!("2. Ensure BENCH_MAX_ROWS is high enough to load all data");
        println!("3. Ensure BENCH_MAX_DURATION_NS is high enough");
        println!(
            "\nOr modify load_record_batches() to apply the same filtering as load_parquet_data()"
        );
    }
}
