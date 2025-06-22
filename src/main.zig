const std = @import("std");

// Import build options
pub const build_options = @import("build_options.zig");

pub const core = @import("core/database.zig");
pub const storage = struct {
    pub const rocksdb = @import("storage/rocksdb.zig");
    pub const wal = @import("storage/wal.zig");
    pub const index = @import("storage/index.zig");
    pub const btree_index = @import("storage/btree_index.zig");
    pub const skiplist_index = @import("storage/skiplist_index.zig");
    pub const distributed_wal = @import("storage/distributed_wal.zig");
};
pub const query = struct {
    pub const planner = @import("query/planner.zig");
    pub const executor = @import("query/executor.zig");
    pub const result = @import("query/result.zig");
    pub const advanced_planner = @import("query/advanced_planner.zig");
    pub const cost_model = @import("query/cost_model.zig");
    pub const statistics = @import("query/statistics.zig");
    pub const parallel = @import("query/parallel.zig");
};
pub const transaction = struct {
    pub const manager = @import("transaction/manager.zig");
};
pub const server = @import("server/server.zig");
pub const gpu = @import("gpu/main.zig");

pub const RocksDB = storage.rocksdb.RocksDB;

pub fn main() !void {
    std.debug.print("GeeqoDB - A high-performance OLAP database in Zig\n", .{});

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

test {
    std.testing.refAllDeclsRecursive(@This());
}
