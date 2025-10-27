use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;
use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use tag_values_cache::{TagSet, extract_tags_from_batch};

/// Configuration for benchmark data loading from environment variables
#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub input_path: PathBuf,
    pub input_type: InputType,
    pub max_rows: usize,
    pub max_duration_ns: u64,
    pub max_cardinality: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InputType {
    Parquet,
    LineProtocol,
}

impl BenchConfig {
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
            .unwrap_or(10_000_000);

        let max_duration_ns = std::env::var("BENCH_MAX_DURATION_NS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7 * 24 * 60 * 60 * 1_000_000_000); // 7 days

        let max_cardinality = std::env::var("BENCH_MAX_CARDINALITY")
            .ok()
            .and_then(|s| s.parse().ok());

        Self {
            input_path,
            input_type,
            max_rows,
            max_duration_ns,
            max_cardinality,
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

    pub fn print_config(&self) {
        println!("\n=== Benchmark Configuration ===");
        println!("Input path: {:?}", self.input_path);
        println!("Input type: {:?}", self.input_type);
        println!("Max rows: {}", self.max_rows);
        println!(
            "Max duration: {} ns (~{} days)",
            self.max_duration_ns,
            self.max_duration_ns / (24 * 60 * 60 * 1_000_000_000)
        );
        if let Some(card) = self.max_cardinality {
            println!("Max cardinality: {}", card);
        } else {
            println!("Max cardinality: unlimited");
        }
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
            if col_name == "time"
                || col_name == "timestamp"
                || col_name == "_time"
                || col_name == "eventtime"
            {
                if let Some(stats) = column.statistics() {
                    if let Statistics::Int64(int_stats) = stats {
                        if let (Some(min), Some(max)) = (int_stats.min_opt(), int_stats.max_opt()) {
                            overall_min = Some(overall_min.map_or(*min, |m| m.min(*min)));
                            overall_max = Some(overall_max.map_or(*max, |m| m.max(*max)));
                        }
                    }
                }
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
                    .flat_map(|batch| extract_tags_from_batch(batch))
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
pub fn load_data() -> std::io::Result<Vec<(u64, TagSet)>> {
    let config = BenchConfig::from_env();
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
