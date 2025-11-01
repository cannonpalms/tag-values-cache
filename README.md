# tag-values-cache

This repository contains a series of different implementations of a tag values cache.

The final iteration of the cache is implemented in `src/bitmap_lapper_cache.rs` and uses roaring bitmaps to
efficiently store the set of unique tag value combinations (tagsets) seen for a given time range.

The cache is dictionary encoded, meaning that tag values are stored as integer IDs rather than strings. This
reduces memory usage and improves performance when working with large datasets.

## Benchmarks

There are many benchmarks in this repository but the only one relevant to the final implementation is
`benches/tag_values_cache.rs`. This benchmark demonstrates the performance of the bitmap lapper cache
when inserting and querying tag values.

To run the benchmark, first generate some sample data:

```
# generate parquet file with 1K cardinality - output will be in benches/data/by-cardinality/
cargo run --bin generate_cardinality_data_mpmc -- 1K

# sort the generated parquet file by time (unfortunately the previous step does not do this)
cargo run --bin sort_parquet_datafusion -- benches/data/by-cardinality/1K.parquet

# replace the original file with the sorted file
mv benches/data/by-cardinality/1K-sorted.parquet benches/data/by-cardinality/1K.parquet
```

Then run the benchmark:

```
cargo bench --bench tag_values_cache
```

This benchmark supports a few environment variables to customize its behavior:

- `BENCH_INPUT_PATH`: Path to the input parquet file. Default is `benches/data/by-cardinality/1K.parquet`.
- `BENCH_MAX_ROWS`: Maximum number of rows to read from the input file. Default is unlimited.
- `BENCH_MAX_DURATION`: Maximum duration (in a human readable format like 1d, 6h) of data to read from the input file. Default is unlimited.
- `BENCH_MAX_CARDINALITY`: Maximum cardinality of tag values to consider. Default is unlimited.
- `BENCH_RECORD_BATCH_ROWS`: Number of rows per record batch when reading the parquet file. Default is the arrow default of 1024.
- `BENCH_STREAM_FROM_DISK`: If set to `true`, the benchmark will stream data from disk instead of loading it all into memory. Default is `false`.
