const std = @import("std");
const geeqodb = @import("geeqodb");
const RocksDB = geeqodb.storage.rocksdb.RocksDB;
const fs = std.fs;

// Number of operations to perform in each benchmark
const NUM_OPS = 10_000;

// Size of values to use in benchmarks
const SMALL_VALUE_SIZE = 100;
const MEDIUM_VALUE_SIZE = 1_000;
const LARGE_VALUE_SIZE = 10_000;

// Helper function to clean up test directories
fn cleanupTestDir(dir_path: []const u8) !void {
    // Try to delete the directory and all its contents
    fs.cwd().deleteTree(dir_path) catch |err| {
        // Ignore errors if the directory doesn't exist
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
}

// Helper function to generate random data
fn generateRandomData(allocator: std.mem.Allocator, size: usize) ![]u8 {
    var data = try allocator.alloc(u8, size);

    // Fill with a repeating pattern instead of random data
    for (0..size) |i| {
        data[i] = @intCast((i % 95) + 32); // Printable ASCII characters
    }

    return data;
}

// Helper function to generate a key
fn generateKey(allocator: std.mem.Allocator, index: usize) ![]u8 {
    return try std.fmt.allocPrint(allocator, "key_{d}", .{index});
}

// Benchmark sequential writes
fn benchmarkSequentialWrite(allocator: std.mem.Allocator, value_size: usize) !u64 {
    const test_dir = "benchmark_sequential_write";
    try cleanupTestDir(test_dir);

    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Generate a sample value
    const value = try generateRandomData(allocator, value_size);
    defer allocator.free(value);

    // Start timing
    const start_time = std.time.milliTimestamp();

    // Perform sequential writes
    for (0..NUM_OPS) |i| {
        const key = try generateKey(allocator, i);
        defer allocator.free(key);

        try db.put(key, value);
    }

    // End timing
    const end_time = std.time.milliTimestamp();

    return @intCast(end_time - start_time);
}

// Benchmark random reads
fn benchmarkRandomRead(allocator: std.mem.Allocator, value_size: usize) !u64 {
    const test_dir = "benchmark_random_read";
    try cleanupTestDir(test_dir);

    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Generate a sample value
    const value = try generateRandomData(allocator, value_size);
    defer allocator.free(value);

    // Populate the database
    for (0..NUM_OPS) |i| {
        const key = try generateKey(allocator, i);
        defer allocator.free(key);

        try db.put(key, value);
    }

    // Generate indices for reading in a pseudo-random pattern
    var indices = try allocator.alloc(usize, NUM_OPS);
    defer allocator.free(indices);

    // Use a simple pattern that jumps around but covers all indices
    for (0..NUM_OPS) |i| {
        indices[i] = (i * 17) % NUM_OPS; // 17 is relatively prime to most values
    }

    // Start timing
    const start_time = std.time.milliTimestamp();

    // Perform random reads
    for (0..NUM_OPS) |i| {
        const key = try generateKey(allocator, indices[i]);
        defer allocator.free(key);

        const read_value = try db.get(allocator, key);
        if (read_value) |v| {
            allocator.free(v);
        }
    }

    // End timing
    const end_time = std.time.milliTimestamp();

    return @intCast(end_time - start_time);
}

// Benchmark batch writes
fn benchmarkBatchWrite(allocator: std.mem.Allocator, value_size: usize, batch_size: usize) !u64 {
    const test_dir = "benchmark_batch_write";
    try cleanupTestDir(test_dir);

    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Generate a sample value
    const value = try generateRandomData(allocator, value_size);
    defer allocator.free(value);

    // Start timing
    const start_time = std.time.milliTimestamp();

    // Perform batch writes
    var i: usize = 0;
    while (i < NUM_OPS) {
        const batch = try db.createWriteBatch();
        defer batch.deinit();

        const end = @min(i + batch_size, NUM_OPS);
        while (i < end) : (i += 1) {
            const key = try generateKey(allocator, i);
            defer allocator.free(key);

            try batch.put(key, value);
        }

        try batch.commit();
    }

    // End timing
    const end_time = std.time.milliTimestamp();

    return @intCast(end_time - start_time);
}

// Benchmark sequential scan
fn benchmarkSequentialScan(allocator: std.mem.Allocator, value_size: usize) !u64 {
    const test_dir = "benchmark_sequential_scan";
    try cleanupTestDir(test_dir);

    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Generate a sample value
    const value = try generateRandomData(allocator, value_size);
    defer allocator.free(value);

    // Populate the database
    for (0..NUM_OPS) |i| {
        const key = try generateKey(allocator, i);
        defer allocator.free(key);

        try db.put(key, value);
    }

    // Start timing
    const start_time = std.time.milliTimestamp();

    // Perform sequential scan
    const iter = try db.iterator();
    defer iter.deinit();

    iter.seekToFirst();
    var count: usize = 0;

    while (iter.isValid()) : (iter.next()) {
        _ = iter.key() catch continue;
        _ = iter.value() catch continue;
        count += 1;
    }

    // End timing
    const end_time = std.time.milliTimestamp();

    return @intCast(end_time - start_time);
}

// Run all benchmarks and print results
pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Print header
    std.debug.print("RocksDB Benchmark Results\n", .{});
    std.debug.print("========================\n", .{});
    std.debug.print("Operations: {d}\n\n", .{NUM_OPS});

    // Sequential Write Benchmarks
    {
        const small_time = try benchmarkSequentialWrite(allocator, SMALL_VALUE_SIZE);
        const medium_time = try benchmarkSequentialWrite(allocator, MEDIUM_VALUE_SIZE);
        const large_time = try benchmarkSequentialWrite(allocator, LARGE_VALUE_SIZE);

        std.debug.print("Sequential Write:\n", .{});
        std.debug.print("  Small Values ({d} bytes): {d} ms, {d:.2} ops/sec\n", .{
            SMALL_VALUE_SIZE, small_time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(small_time)) * 1000.0,
        });
        std.debug.print("  Medium Values ({d} bytes): {d} ms, {d:.2} ops/sec\n", .{
            MEDIUM_VALUE_SIZE, medium_time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(medium_time)) * 1000.0,
        });
        std.debug.print("  Large Values ({d} bytes): {d} ms, {d:.2} ops/sec\n", .{
            LARGE_VALUE_SIZE, large_time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(large_time)) * 1000.0,
        });
        std.debug.print("\n", .{});
    }

    // Random Read Benchmarks
    {
        const small_time = try benchmarkRandomRead(allocator, SMALL_VALUE_SIZE);
        const medium_time = try benchmarkRandomRead(allocator, MEDIUM_VALUE_SIZE);
        const large_time = try benchmarkRandomRead(allocator, LARGE_VALUE_SIZE);

        std.debug.print("Random Read:\n", .{});
        std.debug.print("  Small Values ({d} bytes): {d} ms, {d:.2} ops/sec\n", .{
            SMALL_VALUE_SIZE, small_time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(small_time)) * 1000.0,
        });
        std.debug.print("  Medium Values ({d} bytes): {d} ms, {d:.2} ops/sec\n", .{
            MEDIUM_VALUE_SIZE, medium_time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(medium_time)) * 1000.0,
        });
        std.debug.print("  Large Values ({d} bytes): {d} ms, {d:.2} ops/sec\n", .{
            LARGE_VALUE_SIZE, large_time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(large_time)) * 1000.0,
        });
        std.debug.print("\n", .{});
    }

    // Batch Write Benchmarks
    {
        const batch_sizes = [_]usize{ 10, 100, 1000 };

        std.debug.print("Batch Write (Medium Values, {d} bytes):\n", .{MEDIUM_VALUE_SIZE});

        for (batch_sizes) |batch_size| {
            const time = try benchmarkBatchWrite(allocator, MEDIUM_VALUE_SIZE, batch_size);
            std.debug.print("  Batch Size {d}: {d} ms, {d:.2} ops/sec\n", .{
                batch_size, time, @as(f64, @floatFromInt(NUM_OPS)) / @as(f64, @floatFromInt(time)) * 1000.0,
            });
        }
        std.debug.print("\n", .{});
    }

    // Sequential Scan Benchmarks
    {
        const small_time = try benchmarkSequentialScan(allocator, SMALL_VALUE_SIZE);
        const medium_time = try benchmarkSequentialScan(allocator, MEDIUM_VALUE_SIZE);
        const large_time = try benchmarkSequentialScan(allocator, LARGE_VALUE_SIZE);

        std.debug.print("Sequential Scan:\n", .{});
        std.debug.print("  Small Values ({d} bytes): {d} ms\n", .{ SMALL_VALUE_SIZE, small_time });
        std.debug.print("  Medium Values ({d} bytes): {d} ms\n", .{ MEDIUM_VALUE_SIZE, medium_time });
        std.debug.print("  Large Values ({d} bytes): {d} ms\n", .{ LARGE_VALUE_SIZE, large_time });
        std.debug.print("\n", .{});
    }

    std.debug.print("Benchmark completed successfully!\n", .{});
}
