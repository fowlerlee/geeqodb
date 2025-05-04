const std = @import("std");
const assert = @import("../build_options.zig").assert;

/// Write-Ahead Log for durability and crash recovery
pub const WAL = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    file: ?std.fs.File,
    transactions: std.AutoHashMap(u64, []const u8),
    is_recovered: bool,
    current_position: u64 = 0, // Track current position in the WAL

    /// Initialize a new WAL instance
    pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !*WAL {
        var wal = try allocator.create(WAL);
        wal.* = WAL{
            .allocator = allocator,
            .data_dir = try allocator.dupe(u8, data_dir),
            .file = null,
            .transactions = std.AutoHashMap(u64, []const u8).init(allocator),
            .is_recovered = false,
        };

        try wal.open();
        return wal;
    }

    /// Open the WAL file
    pub fn open(self: *WAL) !void {
        const wal_path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.data_dir, "wal.log" });
        defer self.allocator.free(wal_path);

        // Create the directory if it doesn't exist
        try std.fs.cwd().makePath(self.data_dir);

        // Open the WAL file with read/write access, create if doesn't exist
        self.file = try std.fs.cwd().createFile(wal_path, .{
            .read = true,
            .truncate = false,
        });

        // Recover any existing transactions
        try self.recover();
    }

    /// Close the WAL file
    pub fn close(self: *WAL) void {
        if (self.file) |file| {
            file.close();
            self.file = null;
        }
        self.is_recovered = false;
    }

    /// Deinitialize the WAL
    pub fn deinit(self: *WAL) void {
        self.close();
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.transactions.deinit();
        self.allocator.free(self.data_dir);
        self.allocator.destroy(self);
    }

    /// Get the current position in the WAL file
    pub fn getCurrentPosition(self: *WAL) !u64 {
        if (self.file == null) {
            return error.WALClosed;
        }

        // For a real implementation, this would return the current position in the WAL file
        // For now, we'll just return the tracked position
        return self.current_position;
    }

    /// Log a transaction
    pub fn logTransaction(self: *WAL, txn_id: u64, data: []const u8) !void {
        if (self.file == null) {
            return error.WALClosed;
        }

        // Store the transaction in memory
        const data_copy = try self.allocator.dupe(u8, data);
        try self.transactions.put(txn_id, data_copy);

        // Write to WAL file
        const file = self.file.?;
        var writer = file.writer();

        // Write transaction header (id and length)
        try writer.writeInt(u64, txn_id, .little);
        try writer.writeInt(u64, @as(u64, @intCast(data.len)), .little);

        // Write transaction data
        try writer.writeAll(data);

        // Update the current position
        self.current_position += 16 + data.len; // 8 bytes for txn_id, 8 bytes for data length, plus data

        // Flush to ensure data is written to disk
        try file.sync();
    }

    /// Read a transaction by ID
    pub fn readTransaction(self: *WAL, txn_id: u64) !?[]const u8 {
        if (self.file == null) {
            return error.WALClosed;
        }

        return self.transactions.get(txn_id);
    }

    /// Recover from the WAL
    pub fn recover(self: *WAL) !void {
        if (self.file == null) {
            return error.WALClosed;
        }

        if (self.is_recovered) {
            return;
        }

        const file = self.file.?;
        var reader = file.reader();

        // Clear existing transactions
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.transactions.clearRetainingCapacity();

        // Read all transactions from the WAL file
        while (true) {
            const txn_id = reader.readInt(u64, .little) catch |err| {
                if (err == error.EndOfStream) break;
                return err;
            };

            const data_len = try reader.readInt(u64, .little);
            const data = try self.allocator.alloc(u8, data_len);
            errdefer self.allocator.free(data);

            try reader.readNoEof(data);

            // Store in memory
            try self.transactions.put(txn_id, data);
        }

        self.is_recovered = true;
    }
};

test "WAL basic functionality" {
    const allocator = std.testing.allocator;
    const wal = try WAL.init(allocator, "test_data");
    defer wal.deinit();
}

test "WAL write and recover" {
    const allocator = std.testing.allocator;
    const test_dir = "test_wal";

    // Clean up any previous test data
    std.fs.cwd().deleteTree(test_dir) catch |err| {
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Initialize WAL
    const wal = try WAL.init(allocator, test_dir);
    defer wal.deinit();

    // Write some test transactions
    try wal.logTransaction(1, "transaction 1");
    try wal.logTransaction(2, "transaction 2");
    try wal.logTransaction(3, "transaction 3");

    // Close WAL to simulate crash
    wal.close();

    // Reopen WAL and recover
    const recovered_wal = try WAL.init(allocator, test_dir);
    defer recovered_wal.deinit();

    try recovered_wal.recover();

    // Verify recovered data
    const test_data = [_]struct { id: u64, data: []const u8 }{
        .{ .id = 1, .data = "transaction 1" },
        .{ .id = 2, .data = "transaction 2" },
        .{ .id = 3, .data = "transaction 3" },
    };

    // Read and verify each transaction from the recovered WAL
    for (test_data) |expected| {
        if (try recovered_wal.readTransaction(expected.id)) |transaction| {
            try std.testing.expectEqualStrings(expected.data, transaction);
        } else {
            try std.testing.expect(false); // Transaction should exist
        }
    }
}

test "WAL error handling" {
    const allocator = std.testing.allocator;
    const test_dir = "test_wal_errors";

    // Clean up any previous test data
    std.fs.cwd().deleteTree(test_dir) catch |err| {
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Test operations on closed WAL
    const wal = try WAL.init(allocator, test_dir);
    wal.close();

    // These should all return WALClosed error
    try std.testing.expectError(error.WALClosed, wal.logTransaction(1, "test"));
    try std.testing.expectError(error.WALClosed, wal.recover());

    wal.deinit();
}

test "WAL concurrent operations" {
    const allocator = std.testing.allocator;
    const test_dir = "test_wal_concurrent";

    // Clean up any previous test data
    std.fs.cwd().deleteTree(test_dir) catch |err| {
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const wal = try WAL.init(allocator, test_dir);
    defer wal.deinit();

    // Simulate multiple transactions in quick succession
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        const data = try std.fmt.allocPrint(allocator, "transaction {d}", .{i});
        defer allocator.free(data);
        try wal.logTransaction(i, data);
    }

    // Force a recovery mid-way
    try wal.recover();

    // Continue writing
    i = 100;
    while (i < 200) : (i += 1) {
        const data = try std.fmt.allocPrint(allocator, "transaction {d}", .{i});
        defer allocator.free(data);
        try wal.logTransaction(i, data);
    }
}
