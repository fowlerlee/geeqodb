const std = @import("std");
const geeqodb = @import("src/main.zig");
const database = geeqodb.core;
const server = geeqodb.server;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create data directory
    const data_dir = "data";
    try std.fs.cwd().makePath(data_dir);

    // Initialize database
    std.debug.print("Initializing database...\n", .{});
    const db = try database.init(allocator, data_dir);
    defer db.deinit();
    std.debug.print("Database initialized successfully!\n", .{});

    // Run a test query
    std.debug.print("\nRunning test query: SELECT * FROM users\n", .{});
    const test_query_result = db.execute("SELECT * FROM users");
    if (test_query_result) |result_set| {
        var mutable_result_set = result_set;
        defer mutable_result_set.deinit();
        // Display the result columns
        std.debug.print("Columns: ", .{});
        for (mutable_result_set.columns) |column| {
            std.debug.print("{s} ", .{column.name});
        }
        std.debug.print("\n", .{});
        // Display the result rows
        std.debug.print("Rows: {d}\n", .{mutable_result_set.row_count});
        for (0..mutable_result_set.row_count) |row_idx| {
            std.debug.print("Row {d}: ", .{row_idx});
            for (0..mutable_result_set.columns.len) |col_idx| {
                const value = mutable_result_set.getValue(row_idx, col_idx);
                switch (value) {
                    .integer => |i| std.debug.print("{d} ", .{i}),
                    .float => |f| std.debug.print("{d:.4} ", .{f}),
                    .text => |t| std.debug.print("{s} ", .{t}),
                    .boolean => |b| std.debug.print("{} ", .{b}),
                    .null => std.debug.print("NULL ", .{}),
                }
            }
            std.debug.print("\n", .{});
        }
    } else |err| {
        if (err == error.TableNotFound) {
            std.debug.print("Test query failed: users table does not exist.\n", .{});
        } else {
            std.debug.print("Test query failed with error: {s}\n", .{@errorName(err)});
        }
    }

    // Initialize server
    const port: u16 = 5252;
    std.debug.print("\nStarting database server on port {d}...\n", .{port});
    const db_server = try server.DatabaseServer.init(allocator, db, port);
    defer db_server.deinit();

    // Start server
    try db_server.start();
    std.debug.print("Server started successfully! Press Ctrl+C to stop.\n", .{});

    // Wait for Ctrl+C
    const sigint = std.posix.SIG.INT;
    std.posix.sigaction(sigint, &std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    }, null);

    // Wait for signal
    while (true) {
        std.time.sleep(std.time.ns_per_s);
    }
}

/// Signal handler for Ctrl+C
fn handleSignal(sig: c_int) callconv(.C) void {
    std.debug.print("\nReceived signal {d}, shutting down...\n", .{sig});
    std.process.exit(0);
}
