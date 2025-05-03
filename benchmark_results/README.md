# GeeqoDB Benchmark Results

This directory contains benchmark results from various performance tests of GeeqoDB.

## Index Performance Benchmarks

The index performance benchmarks measure the performance improvement gained by using indexes for various types of queries:

1. **Point Queries**: Queries that look up a single row by a specific value (e.g., `SELECT * FROM users WHERE id = 5000`)
2. **Range Queries**: Queries that retrieve rows within a range of values (e.g., `SELECT * FROM users WHERE age BETWEEN 20 AND 30`)
3. **Join Queries**: Queries that join multiple tables (e.g., `SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 500`)

Each benchmark is run both with and without indexes to demonstrate the performance improvement.

## File Format

Benchmark results are stored in two formats:

1. **Markdown** (`*_markdown.txt`): Human-readable format with tables
2. **CSV** (`*_csv.txt`): Machine-readable format for further analysis

## Running Benchmarks

To run the benchmarks, use the following commands:

```bash
# Run all benchmarks
zig build benchmark

# Run a specific benchmark
zig build benchmark-index_query_benchmark
```

## Interpreting Results

The benchmark results include the following metrics:

- **Query Type**: The type of query being benchmarked
- **Without Index (ns)**: Average execution time in nanoseconds without using indexes
- **With Index (ns)**: Average execution time in nanoseconds using indexes
- **Speedup Factor**: The ratio of execution time without indexes to execution time with indexes (higher is better)

A higher speedup factor indicates a greater performance improvement from using indexes.

## Notes

- Benchmark results may vary depending on hardware, database size, and query complexity
- The benchmarks use a fixed random seed to ensure reproducibility
- The database is populated with a large amount of test data to simulate real-world usage
