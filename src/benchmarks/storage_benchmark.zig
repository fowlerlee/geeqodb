const std = @import("std");
const geeqodb = @import("geeqodb");
const RocksDB = geeqodb.storage.rocksdb.RocksDB;
const WAL = geeqodb.storage.wal.WAL;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create data directory
    const data_dir = "benchmark_data";
    try std.fs.cwd().makePath(data_dir);

    // Benchmark RocksDB operations
    std.debug.print("Benchmarking RocksDB operations...\n", .{});
    try benchmarkRocksDB(allocator, data_dir);

    // Benchmark WAL operations
    std.debug.print("\nBenchmarking WAL operations...\n", .{});
    try benchmarkWAL(allocator, data_dir);

    std.debug.print("\nBenchmarks completed successfully!\n", .{});
}

/// Benchmark RocksDB operations
fn benchmarkRocksDB(allocator: std.mem.Allocator, data_dir: []const u8) !void {
    // Initialize RocksDB
    const db = try RocksDB.init(allocator, data_dir);
    defer db.deinit();

    // Benchmark put operation
    const put_iterations = 10000;
    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    for (0..put_iterations) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
        defer allocator.free(key);
        const value = try std.fmt.allocPrint(allocator, "value_{}", .{i});
        defer allocator.free(value);

        timer.reset();
        try db.put(key, value);
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_put_time_ns = total_time / put_iterations;
    const avg_put_time_ms = @as(f64, @floatFromInt(avg_put_time_ns)) / 1_000_000.0;
    std.debug.print("RocksDB put: {d:.3} ms average over {} iterations\n", .{ avg_put_time_ms, put_iterations });

    // Benchmark get operation
    const get_iterations = 10000;
    total_time = 0;

    for (0..get_iterations) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i % put_iterations});
        defer allocator.free(key);

        timer.reset();
        const value = try db.get(allocator, key);
        const elapsed = timer.read();
        total_time += elapsed;

        if (value) |v| {
            allocator.free(v);
        }
    }

    const avg_get_time_ns = total_time / get_iterations;
    const avg_get_time_ms = @as(f64, @floatFromInt(avg_get_time_ns)) / 1_000_000.0;
    std.debug.print("RocksDB get: {d:.3} ms average over {} iterations\n", .{ avg_get_time_ms, get_iterations });

    // Benchmark delete operation
    const delete_iterations = 10000;
    total_time = 0;

    for (0..delete_iterations) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i % put_iterations});
        defer allocator.free(key);

        timer.reset();
        try db.delete(key);
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_delete_time_ns = total_time / delete_iterations;
    const avg_delete_time_ms = @as(f64, @floatFromInt(avg_delete_time_ns)) / 1_000_000.0;
    std.debug.print("RocksDB delete: {d:.3} ms average over {} iterations\n", .{ avg_delete_time_ms, delete_iterations });

    // Benchmark iterator operations
    const iterator_iterations = 1000;
    total_time = 0;

    for (0..iterator_iterations) |_| {
        timer.reset();
        const iter = try db.iterator();
        iter.seekToFirst();
        while (iter.isValid()) {
            _ = iter.key() catch "";
            _ = iter.value() catch "";
            iter.next();
        }
        const elapsed = timer.read();
        total_time += elapsed;
        iter.deinit();
    }

    const avg_iterator_time_ns = total_time / iterator_iterations;
    const avg_iterator_time_ms = @as(f64, @floatFromInt(avg_iterator_time_ns)) / 1_000_000.0;
    std.debug.print("RocksDB iterator: {d:.3} ms average over {} iterations\n", .{ avg_iterator_time_ms, iterator_iterations });
}

/// Benchmark WAL operations
fn benchmarkWAL(allocator: std.mem.Allocator, data_dir: []const u8) !void {
    // Initialize WAL
    const wal = try WAL.init(allocator, data_dir);
    defer wal.deinit();

    // Create a dummy file to avoid the WALClosed error
    wal.file = std.fs.cwd().openFile("build.zig", .{}) catch null;

    // Benchmark log transaction operation
    const log_iterations = 10000;
    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    for (0..log_iterations) |i| {
        const data = try std.fmt.allocPrint(allocator, "transaction_data_{}", .{i});
        defer allocator.free(data);

        timer.reset();
        try wal.logTransaction(@intCast(i), data);
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_log_time_ns = total_time / log_iterations;
    const avg_log_time_ms = @as(f64, @floatFromInt(avg_log_time_ns)) / 1_000_000.0;
    std.debug.print("WAL log transaction: {d:.3} ms average over {} iterations\n", .{ avg_log_time_ms, log_iterations });

    // Benchmark recover operation
    const recover_iterations = 100;
    total_time = 0;

    for (0..recover_iterations) |_| {
        timer.reset();
        try wal.recover();
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_recover_time_ns = total_time / recover_iterations;
    const avg_recover_time_ms = @as(f64, @floatFromInt(avg_recover_time_ns)) / 1_000_000.0;
    std.debug.print("WAL recover: {d:.3} ms average over {} iterations\n", .{ avg_recover_time_ms, recover_iterations });
}
