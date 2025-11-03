//! Utility to count rows in Parquet files
//!
//! Usage:
//!   cargo run --bin count_rows -- <path>
//!   cargo run --bin count_rows -- benches/data/parquet/
//!
//! Or with environment variable:
//!   PARQUET_PATH=benches/data/parquet/ cargo run --bin count_rows

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get path from command line args or environment variable
    let path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("PARQUET_PATH").ok())
        .unwrap_or_else(|| {
            eprintln!("Usage: count_rows <path>");
            eprintln!("  or: PARQUET_PATH=<path> count_rows");
            std::process::exit(1);
        });

    let path = Path::new(&path);

    if !path.exists() {
        eprintln!("Error: Path does not exist: {:?}", path);
        std::process::exit(1);
    }

    let parquet_files = collect_parquet_files(path)?;

    if parquet_files.is_empty() {
        eprintln!("Error: No parquet files found in {:?}", path);
        std::process::exit(1);
    }

    println!("Found {} parquet file(s)", parquet_files.len());
    println!();

    let mut total_rows = 0_usize;

    for file_path in &parquet_files {
        match count_rows_in_file(file_path) {
            Ok(rows) => {
                total_rows += rows;
                println!("{}: {} rows", file_path.display(), rows);
            }
            Err(e) => {
                eprintln!("Error reading {}: {}", file_path.display(), e);
            }
        }
    }

    println!();
    println!("Total rows: {}", total_rows);

    Ok(())
}

/// Collect all parquet files from a path (file or directory)
fn collect_parquet_files(path: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();

    if path.is_file() {
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            files.push(path.to_path_buf());
        } else {
            return Err("File is not a parquet file".into());
        }
    } else if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();

            if entry_path.is_file()
                && entry_path.extension().and_then(|s| s.to_str()) == Some("parquet")
            {
                files.push(entry_path);
            }
        }

        // Sort for consistent output
        files.sort();
    } else {
        return Err("Path is neither a file nor a directory".into());
    }

    Ok(files)
}

/// Count rows in a single parquet file
fn count_rows_in_file(path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let num_rows = metadata.file_metadata().num_rows() as usize;
    Ok(num_rows)
}
