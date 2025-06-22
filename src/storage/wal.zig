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

        // Check if WAL file exists and has content
        var file_exists = false;
        var file_size: usize = 0;
        if (std.fs.cwd().openFile(wal_path, .{ .mode = .read_only })) |existing_file| {
            defer existing_file.close();
            file_exists = true;
            file_size = existing_file.getEndPos() catch 0;
            std.debug.print("[WAL] open: WAL file exists, size: {}\n", .{file_size});
        } else |err| {
            if (err == error.FileNotFound) {
                std.debug.print("[WAL] open: WAL file does not exist: {s}\n", .{wal_path});
            } else {
                std.debug.print("[WAL] open: Error opening WAL file: {}\n", .{err});
            }
        }

        // Try to open the WAL file for read/write
        std.debug.print("[WAL] openFile: {s}\n", .{wal_path});
        self.file = std.fs.cwd().openFile(wal_path, .{ .mode = .read_write }) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("[WAL] createFile: {s}\n", .{wal_path});
                // Create the file if it doesn't exist
                const created = try std.fs.cwd().createFile(wal_path, .{
                    .truncate = false,
                });
                created.close();
                // Now open for read/write
                self.file = try std.fs.cwd().openFile(wal_path, .{ .mode = .read_write });
                return;
            } else {
                return err;
            }
        };

        // Recover any existing transactions
        try self.recover();
    }

    /// Close the WAL file
    pub fn close(self: *WAL) void {
        std.debug.print("WAL.close called\n", .{});
        if (self.file) |file| {
            file.close();
            self.file = null;
        }
        self.is_recovered = false;
    }

    /// Deinitialize the WAL
    pub fn deinit(self: *WAL) void {
        std.debug.print("WAL.deinit called\n", .{});
        if (self.file != null) self.close();
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        if (@hasField(@TypeOf(self.transactions), "deinit")) {
            self.transactions.deinit();
        }
        self.allocator.free(self.data_dir);
        self.data_dir = &[_]u8{};
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
            std.debug.print("[WAL] logTransaction: WAL file is null!\n", .{});
            return error.WALClosed;
        }

        // Store the transaction in memory
        const data_copy = try self.allocator.dupe(u8, data);
        try self.transactions.put(txn_id, data_copy);

        // Write to WAL file
        const file = self.file.?;
        // Seek to end before writing
        try file.seekFromEnd(0);
        var writer = file.writer();

        std.debug.print("[WAL] logTransaction: Writing txn_id {} with data '{s}' (len: {}) to WAL file\n", .{ txn_id, data, data.len });

        // Write transaction header (id and length)
        try writer.writeInt(u64, txn_id, .little);
        try writer.writeInt(u64, @as(u64, @intCast(data.len)), .little);

        // Write transaction data
        try writer.writeAll(data);

        // Update the current position
        self.current_position += 16 + data.len; // 8 bytes for txn_id, 8 bytes for data length, plus data

        // Flush to ensure data is written to disk
        try file.sync();
        const file_size = try file.getEndPos();
        std.debug.print("[WAL] logTransaction: WAL file flushed, file size now {}\n", .{file_size});
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
        std.debug.print("[WAL] recover() called\\n", .{});

        if (self.file == null) {
            std.debug.print("[WAL] recover() failed: WAL file is null\\n", .{});
            return error.WALClosed;
        }

        if (self.is_recovered) {
            std.debug.print("[WAL] recover() skipped: already recovered\\n", .{});
            return;
        }

        const file = self.file.?;

        // Seek to the beginning before reading
        try file.seekTo(0);

        // Check if file is empty
        const file_size = try file.getEndPos();
        std.debug.print("[WAL] recover() file size: {}\\n", .{file_size});

        if (file_size == 0) {
            std.debug.print("[WAL] recover() file is empty, marking as recovered\\n", .{});
            self.is_recovered = true;
            return;
        }

        var reader = file.reader();

        // Clear existing transactions
        std.debug.print("[WAL] recover() clearing existing transactions\\n", .{});
        var it = self.transactions.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.transactions.clearRetainingCapacity();

        // Read all transactions from the WAL file
        var current_pos: u64 = 0;
        var recovered_count: u64 = 0;

        std.debug.print("[WAL] recover() reading transactions from file\\n", .{});

        while (current_pos < file_size) {
            const txn_id = reader.readInt(u64, .little) catch |err| {
                if (err == error.EndOfStream) {
                    std.debug.print("[WAL] recover() reached end of stream at position {}\\n", .{current_pos});
                    break;
                }
                // If we can't read the transaction ID, the file might be corrupted
                // Just stop reading and mark as recovered
                std.debug.print("[WAL] recover() error reading txn_id: {}\\n", .{err});
                break;
            };

            const data_len = reader.readInt(u64, .little) catch |err| {
                if (err == error.EndOfStream) {
                    std.debug.print("[WAL] recover() reached end of stream after reading txn_id\\n", .{});
                    break;
                }
                // If we can't read the data length, the file might be corrupted
                std.debug.print("[WAL] recover() error reading data_len: {}\\n", .{err});
                break;
            };

            std.debug.print("[WAL] recover() reading transaction {} with data length {}\\n", .{ txn_id, data_len });

            // Validate data length to prevent excessive memory allocation
            if (data_len > 1024 * 1024) { // 1MB limit
                std.debug.print("[WAL] recover() data length {} exceeds 1MB limit, skipping\\n", .{data_len});
                break;
            }

            const data = self.allocator.alloc(u8, data_len) catch {
                // If we can't allocate memory, skip this transaction
                std.debug.print("[WAL] recover() failed to allocate memory for data length {}\\n", .{data_len});
                break;
            };
            errdefer self.allocator.free(data);

            reader.readNoEof(data) catch {
                // If we can't read the data, skip this transaction
                std.debug.print("[WAL] recover() failed to read data for transaction {}\\n", .{txn_id});
                break;
            };

            std.debug.print("[WAL] recover() recovered transaction {}: {s}\\n", .{ txn_id, data });

            // Store in memory
            self.transactions.put(txn_id, data) catch {
                // If we can't store the transaction, skip it
                std.debug.print("[WAL] recover() failed to store transaction {} in memory\\n", .{txn_id});
                break;
            };

            recovered_count += 1;

            // Update position (8 bytes for txn_id + 8 bytes for data_len + data_len)
            current_pos += 16 + data_len;
        }

        std.debug.print("[WAL] recover() completed, recovered {} transactions\\n", .{recovered_count});
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
