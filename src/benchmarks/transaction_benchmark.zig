const std = @import("std");
const geeqodb = @import("geeqodb");
const TransactionManager = geeqodb.transaction.manager.TransactionManager;
const Transaction = geeqodb.transaction.manager.Transaction;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Benchmark transaction operations
    std.debug.print("Benchmarking transaction operations...\n", .{});
    try benchmarkTransactions(allocator);

    std.debug.print("\nBenchmarks completed successfully!\n", .{});
}

/// Benchmark transaction operations
fn benchmarkTransactions(allocator: std.mem.Allocator) !void {
    // Initialize transaction manager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Benchmark begin transaction operation
    const begin_iterations = 10000;
    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;
    var transactions = std.ArrayList(*Transaction).init(allocator);
    defer transactions.deinit();

    for (0..begin_iterations) |_| {
        timer.reset();
        const txn = try txn_manager.beginTransaction();
        const elapsed = timer.read();
        total_time += elapsed;
        try transactions.append(txn);
    }

    const avg_begin_time_ns = total_time / begin_iterations;
    const avg_begin_time_ms = @as(f64, @floatFromInt(avg_begin_time_ns)) / 1_000_000.0;
    std.debug.print("Begin transaction: {d:.3} ms average over {} iterations\n", .{ avg_begin_time_ms, begin_iterations });

    // Benchmark get transaction operation
    const get_iterations = 10000;
    total_time = 0;

    for (0..get_iterations) |i| {
        const txn_id = @as(u64, @intCast((i % transactions.items.len) + 1));

        timer.reset();
        _ = txn_manager.getTransaction(txn_id);
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_get_time_ns = total_time / get_iterations;
    const avg_get_time_ms = @as(f64, @floatFromInt(avg_get_time_ns)) / 1_000_000.0;
    std.debug.print("Get transaction: {d:.3} ms average over {} iterations\n", .{ avg_get_time_ms, get_iterations });

    // Benchmark commit transaction operation
    const commit_iterations = transactions.items.len / 2;
    total_time = 0;

    for (0..commit_iterations) |_| {
        const txn = transactions.pop();

        timer.reset();
        try txn_manager.commitTransaction(txn.?);
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_commit_time_ns = total_time / commit_iterations;
    const avg_commit_time_ms = @as(f64, @floatFromInt(avg_commit_time_ns)) / 1_000_000.0;
    std.debug.print("Commit transaction: {d:.3} ms average over {} iterations\n", .{ avg_commit_time_ms, commit_iterations });

    // Benchmark abort transaction operation
    const abort_iterations = transactions.items.len;
    total_time = 0;

    while (transactions.items.len > 0) {
        const txn = transactions.pop();

        timer.reset();
        try txn_manager.abortTransaction(txn.?);
        const elapsed = timer.read();
        total_time += elapsed;
    }

    const avg_abort_time_ns = total_time / abort_iterations;
    const avg_abort_time_ms = @as(f64, @floatFromInt(avg_abort_time_ns)) / 1_000_000.0;
    std.debug.print("Abort transaction: {d:.3} ms average over {} iterations\n", .{ avg_abort_time_ms, abort_iterations });
}
