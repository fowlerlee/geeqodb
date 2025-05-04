const std = @import("std");
const geeqodb = @import("src/main.zig");
const core = geeqodb.core;
const server = geeqodb.server;

pub fn main() !void {
    std.debug.print("GeeqoDB - Starting database server\n", .{});

    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create data directory
    const data_dir = "data";
    try std.fs.cwd().makePath(data_dir);

    // Initialize database
    std.debug.print("Initializing database...\n", .{});
    const db = try core.init(allocator, data_dir);
    defer db.deinit();
    std.debug.print("Database initialized successfully!\n", .{});

    // Initialize server
    const port: u16 = 5252;
    std.debug.print("Starting database server on port {d}...\n", .{port});
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
