#![allow(dead_code)]

use futures::{StreamExt, TryStreamExt, stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tag_values_cache::{TagSet, extract_tags_from_batch, streaming::SendableRecordBatchStream};

/// Configuration for benchmark data loading from environment variables
#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub input_path: PathBuf,
    pub input_type: InputType,
    pub max_rows: usize,
    pub max_duration_ns: u64,
    pub max_cardinality: Option<usize>,
    pub record_batch_rows: Option<usize>,
    /// If true, stream record batches from disk rather than pre-loading into memory
    pub stream_from_disk: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InputType {
    Parquet,
    LineProtocol,
}

impl BenchConfig {
    /// Parse a human-readable duration string into nanoseconds.
    ///
    /// Supported formats:
    /// - "1s" or "1sec" = 1 second
    /// - "30m" or "30min" = 30 minutes
    /// - "1h" or "1hr" = 1 hour
    /// - "1d" or "1day" = 1 day
    /// - "7d" = 7 days
    /// - "123456789" (just a number) = treated as nanoseconds
    ///
    /// # Examples
    /// ```
    /// assert_eq!(parse_duration("1s"), Some(1_000_000_000));
    /// assert_eq!(parse_duration("30m"), Some(30 * 60 * 1_000_000_000));
    /// assert_eq!(parse_duration("1h"), Some(60 * 60 * 1_000_000_000));
    /// assert_eq!(parse_duration("1d"), Some(24 * 60 * 60 * 1_000_000_000));
    /// ```
    fn parse_duration(s: &str) -> Option<u64> {
        let s = s.trim();

        // If it's just a number, treat it as nanoseconds
        if let Ok(ns) = s.parse::<u64>() {
            return Some(ns);
        }

        // Try to parse as human-readable duration
        let mut chars = s.chars().peekable();
        let mut number_str = String::new();

        // Collect digits
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_digit() {
                number_str.push(ch);
                chars.next();
            } else {
                break;
            }
        }

        if number_str.is_empty() {
            return None;
        }

        let number: u64 = number_str.parse().ok()?;
        let unit: String = chars.collect();
        let unit = unit.trim().to_lowercase();

        let multiplier = match unit.as_str() {
            "ns" | "nsec" | "nanosecond" | "nanoseconds" => 1,
            "us" | "usec" | "microsecond" | "microseconds" => 1_000,
            "ms" | "msec" | "millisecond" | "milliseconds" => 1_000_000,
            "s" | "sec" | "second" | "seconds" => 1_000_000_000,
            "m" | "min" | "minute" | "minutes" => 60 * 1_000_000_000,
            "h" | "hr" | "hour" | "hours" => 60 * 60 * 1_000_000_000,
            "d" | "day" | "days" => 24 * 60 * 60 * 1_000_000_000,
            "w" | "week" | "weeks" => 7 * 24 * 60 * 60 * 1_000_000_000,
            "" => 1, // No unit means nanoseconds
            _ => return None,
        };

        Some(number * multiplier)
    }

    /// Load configuration from environment variables with defaults
    pub fn from_env() -> Self {
        let input_path = std::env::var("BENCH_INPUT_PATH")
            .unwrap_or_else(|_| "benches/data/parquet".to_string());
        let input_path = PathBuf::from(input_path);

        // Auto-detect input type from path if not explicitly set
        let input_type = std::env::var("BENCH_INPUT_TYPE")
            .ok()
            .and_then(|s| match s.to_lowercase().as_str() {
                "parquet" => Some(InputType::Parquet),
                "lineprotocol" | "lp" => Some(InputType::LineProtocol),
                _ => None,
            })
            .unwrap_or_else(|| Self::detect_input_type(&input_path));

        let max_rows = std::env::var("BENCH_MAX_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(usize::MAX); // Default: unlimited

        // Parse duration with human-readable format support
        // Try BENCH_MAX_DURATION first (new human-readable format)
        // Fall back to BENCH_MAX_DURATION_NS (legacy nanoseconds)
        let max_duration_ns = std::env::var("BENCH_MAX_DURATION")
            .ok()
            .and_then(|s| Self::parse_duration(&s))
            .or_else(|| {
                std::env::var("BENCH_MAX_DURATION_NS")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(u64::MAX); // Default: unlimited

        let max_cardinality = std::env::var("BENCH_MAX_CARDINALITY")
            .ok()
            .and_then(|s| s.parse().ok());

        let record_batch_rows = std::env::var("BENCH_RECORD_BATCH_ROWS")
            .ok()
            .and_then(|s| s.parse().ok());

        let stream_from_disk = std::env::var("BENCH_STREAM_FROM_DISK")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);

        Self {
            input_path,
            input_type,
            max_rows,
            max_duration_ns,
            max_cardinality,
            record_batch_rows,
            stream_from_disk,
        }
    }

    /// Detect input type from file extension
    fn detect_input_type(path: &Path) -> InputType {
        if path.is_file() {
            if path.extension().and_then(|s| s.to_str()) == Some("lp") {
                return InputType::LineProtocol;
            }
            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                return InputType::Parquet;
            }
        } else if path.is_dir() {
            // Check if directory contains .lp or .parquet files
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if let Some(ext) = entry_path.extension().and_then(|s| s.to_str()) {
                        if ext == "lp" {
                            return InputType::LineProtocol;
                        }
                        if ext == "parquet" {
                            return InputType::Parquet;
                        }
                    }
                }
            }
        }
        // Default to parquet for backwards compatibility
        InputType::Parquet
    }

    /// Format nanoseconds as human-readable duration
    fn format_duration(ns: u64) -> String {
        const WEEK_NS: u64 = 7 * 24 * 60 * 60 * 1_000_000_000;
        const DAY_NS: u64 = 24 * 60 * 60 * 1_000_000_000;
        const HOUR_NS: u64 = 60 * 60 * 1_000_000_000;
        const MIN_NS: u64 = 60 * 1_000_000_000;
        const SEC_NS: u64 = 1_000_000_000;

        let mut remaining = ns;
        let mut parts = Vec::new();

        if remaining >= WEEK_NS {
            let weeks = remaining / WEEK_NS;
            parts.push(format!("{}w", weeks));
            remaining %= WEEK_NS;
        }
        if remaining >= DAY_NS {
            let days = remaining / DAY_NS;
            parts.push(format!("{}d", days));
            remaining %= DAY_NS;
        }
        if remaining >= HOUR_NS {
            let hours = remaining / HOUR_NS;
            parts.push(format!("{}h", hours));
            remaining %= HOUR_NS;
        }
        if remaining >= MIN_NS {
            let mins = remaining / MIN_NS;
            parts.push(format!("{}m", mins));
            remaining %= MIN_NS;
        }
        if remaining >= SEC_NS {
            let secs = remaining / SEC_NS;
            parts.push(format!("{}s", secs));
            remaining %= SEC_NS;
        }
        if remaining > 0 || parts.is_empty() {
            parts.push(format!("{}ns", remaining));
        }

        parts.join(" ")
    }

    pub fn print_config(&self) {
        println!("\n=== Benchmark Configuration ===");
        println!("Input path: {:?}", self.input_path);
        println!("Input type: {:?}", self.input_type);

        if self.max_rows == usize::MAX {
            println!("Max rows: unlimited");
        } else {
            println!("Max rows: {}", self.max_rows);
        }

        if self.max_duration_ns == u64::MAX {
            println!("Max duration: unlimited");
        } else {
            println!(
                "Max duration: {} ({})",
                Self::format_duration(self.max_duration_ns),
                self.max_duration_ns
            );
        }

        if let Some(card) = self.max_cardinality {
            println!("Max cardinality: {}", card);
        } else {
            println!("Max cardinality: unlimited");
        }
        println!("Stream from disk: {}", self.stream_from_disk);
        println!("===============================\n");
    }
}

/// File metadata extracted from parquet without loading full data
#[derive(Debug, Clone)]
struct FileMetadata {
    path: PathBuf,
    #[allow(dead_code)]
    num_rows: usize,
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
}

/// Extract min/max timestamps from parquet file metadata statistics
fn get_timestamp_range(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> (Option<i64>, Option<i64>) {
    use parquet::file::statistics::Statistics;

    let mut overall_min: Option<i64> = None;
    let mut overall_max: Option<i64> = None;

    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let col_name = column.column_descr().name().to_lowercase();
            if (col_name == "time"
                || col_name == "timestamp"
                || col_name == "_time"
                || col_name == "eventtime")
                && let Some(stats) = column.statistics()
                && let Statistics::Int64(int_stats) = stats
                && let (Some(min), Some(max)) = (int_stats.min_opt(), int_stats.max_opt())
            {
                overall_min = Some(overall_min.map_or(*min, |m| m.min(*min)));
                overall_max = Some(overall_max.map_or(*max, |m| m.max(*max)));
            }
        }
    }

    (overall_min, overall_max)
}

/// Calculate cardinality (unique tag combinations) from data
fn calculate_cardinality(data: &[(u64, TagSet)]) -> usize {
    let mut unique_combinations = HashSet::new();

    for (_, tag_set) in data {
        let mut combination = Vec::new();
        for (key, value) in tag_set {
            combination.push(format!("{}={}", key, value));
        }
        combination.sort();
        unique_combinations.insert(combination.join(","));
    }

    unique_combinations.len()
}

/// Load data from parquet files with configurable limits
fn load_parquet_data(config: &BenchConfig) -> std::io::Result<Vec<(u64, TagSet)>> {
    let path = &config.input_path;

    // Collect all parquet files
    let parquet_files: Vec<PathBuf> = if path.is_file() {
        vec![path.clone()]
    } else if path.is_dir() {
        fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let entry_path = entry.path();
                if entry_path.is_file()
                    && entry_path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    Some(entry_path)
                } else {
                    None
                }
            })
            .collect()
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path not found: {:?}", path),
        ));
    };

    if parquet_files.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No parquet files found in {:?}", path),
        ));
    }

    // Phase 1: Scan metadata from all files in parallel
    let mut file_metadata: Vec<FileMetadata> = parquet_files
        .par_iter()
        .filter_map(|path| {
            let file = File::open(path).ok()?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
            let metadata = builder.metadata();
            let num_rows = metadata.file_metadata().num_rows() as usize;
            let (min_ts, max_ts) = get_timestamp_range(metadata);

            Some(FileMetadata {
                path: path.clone(),
                num_rows,
                min_timestamp: min_ts,
                max_timestamp: max_ts,
            })
        })
        .collect();

    // Sort by min timestamp (oldest first)
    file_metadata.sort_by_key(|metadata| metadata.min_timestamp.unwrap_or(i64::MAX));

    // Phase 2: Select consecutive files based on metadata timestamps
    let mut consecutive_files = Vec::new();
    let mut prev_max_ts: Option<i64> = None;
    const MAX_GAP_NS: i64 = 25 * 60 * 60 * 1_000_000_000; // 25 hours gap tolerance

    for metadata in file_metadata {
        if let (Some(file_min), Some(file_max)) = (metadata.min_timestamp, metadata.max_timestamp) {
            let is_consecutive = if let Some(prev_max) = prev_max_ts {
                let gap = file_min.saturating_sub(prev_max);
                gap <= MAX_GAP_NS
            } else {
                true // First file is always accepted
            };

            if is_consecutive {
                consecutive_files.push(metadata.clone());
                prev_max_ts = Some(file_max);
            } else {
                break; // Gap detected - stop looking for more files
            }
        }
    }

    // Phase 3: Load consecutive files in chunks of 4
    const CHUNK_SIZE: usize = 4;
    let mut all_data = Vec::new();
    let mut overall_min_ts: Option<u64> = None;
    let mut overall_max_ts: Option<u64> = None;
    let mut cardinality_tracker: Option<HashSet<String>> =
        config.max_cardinality.map(|_| HashSet::new());

    for chunk in consecutive_files.chunks(CHUNK_SIZE) {
        // Load this chunk of files in parallel
        let chunk_results: Vec<_> = chunk
            .par_iter()
            .filter_map(|metadata| {
                let file = File::open(&metadata.path).ok()?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
                let reader = builder.build().ok()?;

                let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().ok()?;

                let file_data: Vec<(u64, TagSet)> = batches
                    .par_iter()
                    .flat_map(extract_tags_from_batch)
                    .collect();

                let file_rows = file_data.len();
                let file_min_ts = file_data.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
                let file_max_ts = file_data.iter().map(|(ts, _)| *ts).max().unwrap_or(0);

                println!(
                    "{:?}: {} rows",
                    metadata.path.file_name().unwrap_or_default(),
                    file_rows,
                );

                Some((file_data, file_min_ts, file_max_ts))
            })
            .collect();

        // Add results from this chunk and check limits
        for (file_data, file_min_ts, file_max_ts) in chunk_results {
            // Apply cardinality filter if configured
            let filtered_data = if let Some(max_card) = config.max_cardinality {
                let tracker = cardinality_tracker.as_mut().unwrap();
                file_data
                    .into_iter()
                    .filter(|(_, tag_set)| {
                        if tracker.len() >= max_card {
                            // Check if this tag combination is already seen
                            let mut combination = Vec::new();
                            for (key, value) in tag_set {
                                combination.push(format!("{}={}", key, value));
                            }
                            combination.sort();
                            let key = combination.join(",");
                            tracker.contains(&key)
                        } else {
                            // Still under limit, track this combination
                            let mut combination = Vec::new();
                            for (key, value) in tag_set {
                                combination.push(format!("{}={}", key, value));
                            }
                            combination.sort();
                            let key = combination.join(",");
                            tracker.insert(key);
                            true
                        }
                    })
                    .collect()
            } else {
                file_data
            };

            all_data.extend(filtered_data);

            // Update overall timestamp range
            overall_min_ts = Some(overall_min_ts.map_or(file_min_ts, |m| m.min(file_min_ts)));
            overall_max_ts = Some(overall_max_ts.map_or(file_max_ts, |m| m.max(file_max_ts)));

            // Check row limit
            if all_data.len() >= config.max_rows {
                all_data.truncate(config.max_rows);
                break;
            }

            // Check duration limit
            if let (Some(min), Some(max)) = (overall_min_ts, overall_max_ts) {
                let duration_ns = max.saturating_sub(min);
                if duration_ns >= config.max_duration_ns {
                    break;
                }
            }
        }

        // Break out of chunk loop if we hit a limit
        if all_data.len() >= config.max_rows {
            break;
        }

        if let (Some(min), Some(max)) = (overall_min_ts, overall_max_ts) {
            let duration_ns = max.saturating_sub(min);
            if duration_ns >= config.max_duration_ns {
                break;
            }
        }
    }

    Ok(all_data)
}

/// Parse a single line of line protocol format
fn parse_line_protocol(line: &str) -> Option<(u64, TagSet)> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 3 {
        return None;
    }

    // Parse timestamp (last part)
    let timestamp: u64 = parts[2].parse().ok()?;

    // Parse measurement and tags (first part)
    let measurement_tags = parts[0];
    let mut tag_parts = measurement_tags.split(',');
    let measurement = tag_parts.next()?;

    let mut tag_set = BTreeSet::new();

    // Add measurement as a special tag
    tag_set.insert(("_measurement".to_string(), measurement.to_string()));

    // Parse tags - include all tags
    for tag_part in tag_parts {
        if let Some((key, value)) = tag_part.split_once('=') {
            tag_set.insert((key.to_string(), value.to_string()));
        }
    }

    // Skip fields - we only want tags for cardinality calculation
    // Fields are in parts[1] but we don't include them in the TagSet

    Some((timestamp, tag_set))
}

/// Load data from line protocol files with configurable limits
fn load_line_protocol_data(config: &BenchConfig) -> std::io::Result<Vec<(u64, TagSet)>> {
    let path = &config.input_path;

    // Collect all line protocol files
    let lp_files: Vec<PathBuf> = if path.is_file() {
        vec![path.clone()]
    } else if path.is_dir() {
        fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let entry_path = entry.path();
                if entry_path.is_file()
                    && entry_path.extension().and_then(|s| s.to_str()) == Some("lp")
                {
                    Some(entry_path)
                } else {
                    None
                }
            })
            .collect()
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path not found: {:?}", path),
        ));
    };

    if lp_files.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No .lp files found in {:?}", path),
        ));
    }

    let mut all_data = Vec::new();
    let mut cardinality_tracker: Option<HashSet<String>> =
        config.max_cardinality.map(|_| HashSet::new());
    let mut overall_min_ts: Option<u64> = None;
    let mut overall_max_ts: Option<u64> = None;

    for lp_file in lp_files {
        println!("Loading {:?}...", lp_file.file_name().unwrap_or_default());

        let file = File::open(&lp_file)?;
        let reader = BufReader::new(file);

        // Read all lines first
        let lines: Vec<String> = reader.lines().collect::<Result<Vec<_>, _>>()?;

        // Parse lines in parallel
        let mut file_data: Vec<(u64, TagSet)> = lines
            .par_iter()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .filter_map(|line| parse_line_protocol(line))
            .collect();

        // Sort by timestamp for duration checking
        file_data.sort_by_key(|(ts, _)| *ts);

        // Apply cardinality filter if configured
        let filtered_data = if let Some(max_card) = config.max_cardinality {
            let tracker = cardinality_tracker.as_mut().unwrap();
            file_data
                .into_iter()
                .filter(|(_, tag_set)| {
                    if tracker.len() >= max_card {
                        let mut combination = Vec::new();
                        for (key, value) in tag_set {
                            combination.push(format!("{}={}", key, value));
                        }
                        combination.sort();
                        let key = combination.join(",");
                        tracker.contains(&key)
                    } else {
                        let mut combination = Vec::new();
                        for (key, value) in tag_set {
                            combination.push(format!("{}={}", key, value));
                        }
                        combination.sort();
                        let key = combination.join(",");
                        tracker.insert(key);
                        true
                    }
                })
                .collect()
        } else {
            file_data
        };

        println!("  {} rows parsed", filtered_data.len());

        // Update timestamp range
        if !filtered_data.is_empty() {
            let file_min_ts = filtered_data.first().map(|(ts, _)| *ts).unwrap();
            let file_max_ts = filtered_data.last().map(|(ts, _)| *ts).unwrap();

            overall_min_ts = Some(overall_min_ts.map_or(file_min_ts, |m| m.min(file_min_ts)));
            overall_max_ts = Some(overall_max_ts.map_or(file_max_ts, |m| m.max(file_max_ts)));
        }

        all_data.extend(filtered_data);

        // Check row limit
        if all_data.len() >= config.max_rows {
            all_data.truncate(config.max_rows);
            break;
        }

        // Check duration limit
        if let (Some(min), Some(max)) = (overall_min_ts, overall_max_ts) {
            let duration_ns = max.saturating_sub(min);
            if duration_ns >= config.max_duration_ns {
                break;
            }
        }
    }

    Ok(all_data)
}

/// Main entry point: Load data according to configuration
pub fn load_data(config: &BenchConfig) -> std::io::Result<Vec<(u64, TagSet)>> {
    config.print_config();

    // Check if path exists
    if !config.input_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Input path not found: {:?}\nPlease set BENCH_INPUT_PATH environment variable",
                config.input_path
            ),
        ));
    }

    let data = match config.input_type {
        InputType::Parquet => load_parquet_data(&config)?,
        InputType::LineProtocol => load_line_protocol_data(&config)?,
    };

    // Print statistics
    let cardinality = calculate_cardinality(&data);

    println!("\n=== Data Statistics ===");
    println!("Lines loaded: {}", data.len());
    println!("Total cardinality: {}", cardinality);

    if !data.is_empty() {
        let timestamps: Vec<u64> = data.iter().map(|(ts, _)| *ts).collect();
        let min_ts = *timestamps.iter().min().unwrap();
        let max_ts = *timestamps.iter().max().unwrap();

        println!("Timestamp range: {} to {}", min_ts, max_ts);

        let duration_ns = max_ts - min_ts;
        let duration_secs = duration_ns / 1_000_000_000;
        let hours = duration_secs / 3600;
        let minutes = (duration_secs % 3600) / 60;
        let seconds = duration_secs % 60;

        println!("Duration: {}h {}m {}s", hours, minutes, seconds);
    }
    println!("======================\n");

    Ok(data)
}

/// Load Parquet data as RecordBatches for streaming benchmarks
#[allow(dead_code)]
pub fn load_record_batches(config: &BenchConfig) -> Result<Vec<arrow::array::RecordBatch>, std::io::Error> {

    if config.input_type != InputType::Parquet {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Record batch loading only supports Parquet input",
        ));
    }

    let path = &config.input_path;

    // Find all parquet files
    let parquet_files: Vec<std::path::PathBuf> = if path.is_file() {
        vec![path.clone()]
    } else if path.is_dir() {
        std::fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let entry_path = entry.path();
                if entry_path.is_file()
                    && entry_path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    Some(entry_path)
                } else {
                    None
                }
            })
            .collect()
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path not found: {:?}", path),
        ));
    };

    if parquet_files.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No parquet files found in {:?}", path),
        ));
    }

    // Phase 1: Scan metadata from all files in parallel
    let mut file_metadata: Vec<FileMetadata> = parquet_files
        .par_iter()
        .filter_map(|path| {
            let file = File::open(path).ok()?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
            let metadata = builder.metadata();
            let num_rows = metadata.file_metadata().num_rows() as usize;
            let (min_ts, max_ts) = get_timestamp_range(metadata);

            Some(FileMetadata {
                path: path.clone(),
                num_rows,
                min_timestamp: min_ts,
                max_timestamp: max_ts,
            })
        })
        .collect();

    // Sort by min timestamp (oldest first)
    file_metadata.sort_by_key(|metadata| metadata.min_timestamp.unwrap_or(i64::MAX));

    // Phase 2: Select consecutive files based on metadata timestamps
    let mut consecutive_files = Vec::new();
    let mut prev_max_ts: Option<i64> = None;
    const MAX_GAP_NS: i64 = 25 * 60 * 60 * 1_000_000_000; // 25 hours gap tolerance

    for metadata in file_metadata {
        if let (Some(file_min), Some(file_max)) = (metadata.min_timestamp, metadata.max_timestamp) {
            let is_consecutive = if let Some(prev_max) = prev_max_ts {
                let gap = file_min.saturating_sub(prev_max);
                gap <= MAX_GAP_NS
            } else {
                true // First file is always accepted
            };

            if is_consecutive {
                consecutive_files.push(metadata.clone());
                prev_max_ts = Some(file_max);
            } else {
                break; // Gap detected - stop looking for more files
            }
        }
    }

    // Phase 3: Load consecutive files and collect batches
    // NOTE: This function now truncates the last batch if it would exceed max_rows
    // to ensure we return exactly max_rows of data (matching load_parquet_data behavior)
    let mut all_batches = Vec::new();
    let mut total_rows = 0;

    println!(
        "Loading RecordBatches from {} files...",
        consecutive_files.len()
    );

    for metadata in consecutive_files {
        let file = File::open(&metadata.path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("Failed to open {:?}: {}", metadata.path, e),
            )
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to read parquet from {:?}: {}", metadata.path, e),
            )
        })?;

        let builder = if let Some(batch_size) = config.record_batch_rows {
            builder.with_batch_size(batch_size)
        } else {
            builder
        };

        let reader = builder.build().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to build reader for {:?}: {}", metadata.path, e),
            )
        })?;

        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to read batches from {:?}: {}", metadata.path, e),
            )
        })?;

        for batch in batches {
            let batch_rows = batch.num_rows();
            let remaining_capacity = config.max_rows.saturating_sub(total_rows);

            if remaining_capacity == 0 {
                // Already at limit
                println!(
                    "Reached row limit: {} rows from {} batches",
                    total_rows,
                    all_batches.len()
                );
                return Ok(all_batches);
            }

            if batch_rows <= remaining_capacity {
                // Entire batch fits
                total_rows += batch_rows;
                all_batches.push(batch);
            } else {
                // Need to truncate this batch to fit exactly max_rows
                let truncated_batch = batch.slice(0, remaining_capacity);
                total_rows += remaining_capacity;
                all_batches.push(truncated_batch);

                println!(
                    "Truncated last batch to {} rows (original had {})",
                    remaining_capacity, batch_rows
                );
                println!(
                    "Reached row limit: exactly {} rows from {} batches",
                    total_rows,
                    all_batches.len()
                );
                return Ok(all_batches);
            }

            // Check if we've exactly hit the limit
            if total_rows == config.max_rows {
                println!(
                    "Reached row limit: exactly {} rows from {} batches",
                    total_rows,
                    all_batches.len()
                );
                return Ok(all_batches);
            }
        }
    }

    println!(
        "Loaded {} rows from {} batches",
        total_rows,
        all_batches.len()
    );

    Ok(all_batches)
}

/// Create a stream that reads RecordBatches from disk on-demand
///
/// This function returns a stream that lazily reads parquet files from disk,
/// avoiding the need to pre-load all data into memory. This is useful for
/// benchmarking with very large datasets that don't fit in memory.
pub fn create_stream_from_disk(config: &BenchConfig) -> Result<SendableRecordBatchStream, std::io::Error> {

    if config.input_type != InputType::Parquet {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Disk streaming only supports Parquet input",
        ));
    }

    let path = &config.input_path;

    // Find all parquet files
    let parquet_files: Vec<PathBuf> = if path.is_file() {
        vec![path.clone()]
    } else if path.is_dir() {
        fs::read_dir(path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let entry_path = entry.path();
                if entry_path.is_file()
                    && entry_path.extension().and_then(|s| s.to_str()) == Some("parquet")
                {
                    Some(entry_path)
                } else {
                    None
                }
            })
            .collect()
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path not found: {:?}", path),
        ));
    };

    if parquet_files.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("No parquet files found in {:?}", path),
        ));
    }

    // Phase 1: Scan metadata from all files in parallel
    let mut file_metadata: Vec<FileMetadata> = parquet_files
        .par_iter()
        .filter_map(|path| {
            let file = File::open(path).ok()?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
            let metadata = builder.metadata();
            let num_rows = metadata.file_metadata().num_rows() as usize;
            let (min_ts, max_ts) = get_timestamp_range(metadata);

            Some(FileMetadata {
                path: path.clone(),
                num_rows,
                min_timestamp: min_ts,
                max_timestamp: max_ts,
            })
        })
        .collect();

    // Sort by min timestamp (oldest first)
    file_metadata.sort_by_key(|metadata| metadata.min_timestamp.unwrap_or(i64::MAX));

    // Phase 2: Select consecutive files based on metadata timestamps
    let mut consecutive_files = Vec::new();
    let mut prev_max_ts: Option<i64> = None;
    const MAX_GAP_NS: i64 = 25 * 60 * 60 * 1_000_000_000; // 25 hours gap tolerance

    for metadata in file_metadata {
        if let (Some(file_min), Some(file_max)) = (metadata.min_timestamp, metadata.max_timestamp) {
            let is_consecutive = if let Some(prev_max) = prev_max_ts {
                let gap = file_min.saturating_sub(prev_max);
                gap <= MAX_GAP_NS
            } else {
                true // First file is always accepted
            };

            if is_consecutive {
                consecutive_files.push(metadata.clone());
                prev_max_ts = Some(file_max);
            } else {
                break; // Gap detected - stop looking for more files
            }
        }
    }

    println!(
        "Streaming RecordBatches from {} files...",
        consecutive_files.len()
    );

    // Phase 3: Create a stream that reads files on-demand and limits rows
    let max_rows = config.max_rows;
    let record_batch_rows = config.record_batch_rows;

    // Use a state variable to track total rows across the stream
    let total_rows_cell = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_rows_ref = total_rows_cell.clone();

    let stream = stream::iter(consecutive_files)
        .then(move |metadata| async move {
            let file = File::open(&metadata.path).map_err(|e| {
                arrow::error::ArrowError::IoError(format!("Failed to open {:?}", metadata.path), e)
            })?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| arrow::error::ArrowError::ParquetError(e.to_string()))?;

            let builder = if let Some(batch_size) = record_batch_rows {
                builder.with_batch_size(batch_size)
            } else {
                builder
            };

            let reader = builder
                .build()
                .map_err(|e| arrow::error::ArrowError::ParquetError(e.to_string()))?;

            Ok::<_, arrow::error::ArrowError>(stream::iter(reader))
        })
        .try_flatten()
        .try_filter_map(move |batch| {
            let batch_rows = batch.num_rows();
            let current_total = total_rows_ref.load(std::sync::atomic::Ordering::SeqCst);
            let remaining_capacity = max_rows.saturating_sub(current_total);

            if remaining_capacity == 0 {
                // Already at limit - filter out this batch
                futures::future::ready(Ok(None))
            } else if batch_rows <= remaining_capacity {
                // Entire batch fits
                total_rows_ref.fetch_add(batch_rows, std::sync::atomic::Ordering::SeqCst);
                futures::future::ready(Ok(Some(batch)))
            } else {
                // Need to truncate this batch to fit exactly max_rows
                let truncated_batch = batch.slice(0, remaining_capacity);
                total_rows_ref.fetch_add(remaining_capacity, std::sync::atomic::Ordering::SeqCst);
                futures::future::ready(Ok(Some(truncated_batch)))
            }
        })
        .boxed();

    Ok(stream)
}
