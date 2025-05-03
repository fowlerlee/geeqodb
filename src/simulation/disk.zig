const std = @import("std");
const Scheduler = @import("scheduler.zig").Scheduler;

/// Simulated disk for deterministic I/O operations
pub const SimulatedDisk = struct {
    allocator: std.mem.Allocator,
    scheduler: *Scheduler,
    data: std.StringHashMap([]u8),
    read_delay_min: u64,
    read_delay_max: u64,
    write_delay_min: u64,
    write_delay_max: u64,
    read_error_probability: f64,
    write_error_probability: f64,
    corruption_probability: f64,

    pub const DiskOperation = enum {
        Read,
        Write,
    };

    pub const DiskCallback = *const fn (
        op: DiskOperation,
        path: []const u8,
        data: ?[]const u8,
        error_code: ?anyerror,
        context: ?*anyopaque,
    ) void;

    pub const DiskContext = struct {
        disk: *SimulatedDisk,
        operation: DiskOperation,
        path: []const u8,
        data: ?[]const u8,
        callback: DiskCallback,
        user_context: ?*anyopaque,
    };

    /// Initialize a new simulated disk
    pub fn init(allocator: std.mem.Allocator, scheduler: *Scheduler) !*SimulatedDisk {
        const disk = try allocator.create(SimulatedDisk);

        disk.* = SimulatedDisk{
            .allocator = allocator,
            .scheduler = scheduler,
            .data = std.StringHashMap([]u8).init(allocator),
            .read_delay_min = 1,
            .read_delay_max = 5,
            .write_delay_min = 5,
            .write_delay_max = 10,
            .read_error_probability = 0.0,
            .write_error_probability = 0.0,
            .corruption_probability = 0.0,
        };

        return disk;
    }

    /// Deinitialize the disk
    pub fn deinit(self: *SimulatedDisk) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.data.deinit();
        self.allocator.destroy(self);
    }

    /// Read data from the disk
    pub fn read(
        self: *SimulatedDisk,
        path: []const u8,
        callback: DiskCallback,
        context: ?*anyopaque,
    ) !void {
        // Determine read delay
        const delay_range = self.read_delay_max - self.read_delay_min;
        const delay = if (delay_range == 0)
            self.read_delay_min
        else
            self.read_delay_min + self.scheduler.getRandomInt(delay_range);

        // Create disk context
        const ctx = try self.allocator.create(DiskContext);
        ctx.* = DiskContext{
            .disk = self,
            .operation = .Read,
            .path = try self.allocator.dupe(u8, path),
            .data = null,
            .callback = callback,
            .user_context = context,
        };

        // Schedule read operation
        _ = try self.scheduler.scheduleAfter(delay, 0, diskOperationCallback, ctx);
    }

    /// Write data to the disk
    pub fn write(
        self: *SimulatedDisk,
        path: []const u8,
        data: []const u8,
        callback: DiskCallback,
        context: ?*anyopaque,
    ) !void {
        // Determine write delay
        const delay_range = self.write_delay_max - self.write_delay_min;
        const delay = if (delay_range == 0)
            self.write_delay_min
        else
            self.write_delay_min + self.scheduler.getRandomInt(delay_range);

        // Create disk context
        const ctx = try self.allocator.create(DiskContext);
        ctx.* = DiskContext{
            .disk = self,
            .operation = .Write,
            .path = try self.allocator.dupe(u8, path),
            .data = try self.allocator.dupe(u8, data),
            .callback = callback,
            .user_context = context,
        };

        // Schedule write operation
        _ = try self.scheduler.scheduleAfter(delay, 0, diskOperationCallback, ctx);
    }

    /// Delete a file from the disk
    pub fn delete(self: *SimulatedDisk, path: []const u8) bool {
        if (self.data.getKey(path)) |key| {
            const value = self.data.get(key).?;
            self.allocator.free(value);
            _ = self.data.remove(key);
            self.allocator.free(key);
            return true;
        }
        return false;
    }

    /// Set read delay range
    pub fn setReadDelay(self: *SimulatedDisk, min: u64, max: u64) void {
        self.read_delay_min = min;
        self.read_delay_max = max;
    }

    /// Set write delay range
    pub fn setWriteDelay(self: *SimulatedDisk, min: u64, max: u64) void {
        self.write_delay_min = min;
        self.write_delay_max = max;
    }

    /// Set read error probability (0.0 - 1.0)
    pub fn setReadErrorProbability(self: *SimulatedDisk, probability: f64) void {
        self.read_error_probability = std.math.clamp(probability, 0.0, 1.0);
    }

    /// Set write error probability (0.0 - 1.0)
    pub fn setWriteErrorProbability(self: *SimulatedDisk, probability: f64) void {
        self.write_error_probability = std.math.clamp(probability, 0.0, 1.0);
    }

    /// Set corruption probability (0.0 - 1.0)
    pub fn setCorruptionProbability(self: *SimulatedDisk, probability: f64) void {
        self.corruption_probability = std.math.clamp(probability, 0.0, 1.0);
    }

    /// Inject corruption into a specific file
    pub fn injectCorruption(self: *SimulatedDisk, path: []const u8) !void {
        if (self.data.get(path)) |data| {
            if (data.len == 0) {
                return;
            }

            const pos = @as(usize, @intCast(self.scheduler.getRandomInt(@as(u64, @intCast(data.len)))));
            data[pos] ^= 0xFF; // Flip all bits at this position
        } else {
            return error.FileNotFound;
        }
    }
};

/// Callback for disk operations
fn diskOperationCallback(context: ?*anyopaque) void {
    const ctx = @as(*SimulatedDisk.DiskContext, @ptrCast(@alignCast(context.?)));
    defer {
        ctx.disk.allocator.free(ctx.path);
        if (ctx.data) |data| {
            ctx.disk.allocator.free(data);
        }
        ctx.disk.allocator.destroy(ctx);
    }

    switch (ctx.operation) {
        .Read => {
            // Check for read error
            if (ctx.disk.scheduler.getRandomFloat() < ctx.disk.read_error_probability) {
                ctx.callback(.Read, ctx.path, null, error.DiskReadError, ctx.user_context);
                return;
            }

            // Check if file exists
            if (ctx.disk.data.get(ctx.path)) |data| {
                // Check for corruption
                if (ctx.disk.scheduler.getRandomFloat() < ctx.disk.corruption_probability) {
                    // Create a corrupted copy
                    var corrupted_data = ctx.disk.allocator.dupe(u8, data) catch {
                        ctx.callback(.Read, ctx.path, null, error.OutOfMemory, ctx.user_context);
                        return;
                    };

                    // Corrupt a random byte
                    if (corrupted_data.len > 0) {
                        const pos = @as(usize, @intCast(ctx.disk.scheduler.getRandomInt(@as(u64, @intCast(corrupted_data.len)))));
                        corrupted_data[pos] ^= 0xFF; // Flip all bits at this position
                    }

                    ctx.callback(.Read, ctx.path, corrupted_data, null, ctx.user_context);
                    ctx.disk.allocator.free(corrupted_data);
                } else {
                    // Return clean data
                    ctx.callback(.Read, ctx.path, data, null, ctx.user_context);
                }
            } else {
                ctx.callback(.Read, ctx.path, null, error.FileNotFound, ctx.user_context);
            }
        },
        .Write => {
            // Check for write error
            if (ctx.disk.scheduler.getRandomFloat() < ctx.disk.write_error_probability) {
                ctx.callback(.Write, ctx.path, null, error.DiskWriteError, ctx.user_context);
                return;
            }

            // Store the data
            if (ctx.disk.data.getKey(ctx.path)) |existing_key| {
                // Free the old data
                const old_data = ctx.disk.data.get(existing_key).?;
                ctx.disk.allocator.free(old_data);

                // Store the new data
                const data_copy = ctx.disk.allocator.dupe(u8, ctx.data.?) catch {
                    ctx.callback(.Write, ctx.path, null, error.OutOfMemory, ctx.user_context);
                    return;
                };

                ctx.disk.data.put(existing_key, data_copy) catch {
                    ctx.disk.allocator.free(data_copy);
                    ctx.callback(.Write, ctx.path, null, error.OutOfMemory, ctx.user_context);
                    return;
                };
            } else {
                // Create new entry
                const key_copy = ctx.disk.allocator.dupe(u8, ctx.path) catch {
                    ctx.callback(.Write, ctx.path, null, error.OutOfMemory, ctx.user_context);
                    return;
                };

                const data_copy = ctx.disk.allocator.dupe(u8, ctx.data.?) catch {
                    ctx.disk.allocator.free(key_copy);
                    ctx.callback(.Write, ctx.path, null, error.OutOfMemory, ctx.user_context);
                    return;
                };

                ctx.disk.data.put(key_copy, data_copy) catch {
                    ctx.disk.allocator.free(key_copy);
                    ctx.disk.allocator.free(data_copy);
                    ctx.callback(.Write, ctx.path, null, error.OutOfMemory, ctx.user_context);
                    return;
                };
            }

            ctx.callback(.Write, ctx.path, ctx.data, null, ctx.user_context);
        },
    }
}

test "SimulatedDisk basic functionality" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var disk = try SimulatedDisk.init(allocator, scheduler);
    defer disk.deinit();

    // Set up test context
    var read_success = false;
    var write_success = false;
    var test_ctx = TestContext{
        .read_success = &read_success,
        .write_success = &write_success,
    };

    // Write data
    try disk.write("test.txt", "Hello, World!", testDiskCallback, &test_ctx);

    // Run the scheduler to process the write
    try scheduler.run(null);

    // Check that write succeeded
    try std.testing.expect(write_success);

    // Reset flags
    write_success = false;

    // Read data
    try disk.read("test.txt", testDiskCallback, &test_ctx);

    // Run the scheduler to process the read
    try scheduler.run(null);

    // Check that read succeeded
    try std.testing.expect(read_success);

    // Delete the file
    const deleted = disk.delete("test.txt");
    try std.testing.expect(deleted);

    // Reset flags
    read_success = false;

    // Try to read deleted file
    try disk.read("test.txt", testDiskCallback, &test_ctx);

    // Run the scheduler to process the read
    try scheduler.run(null);

    // Check that read failed
    try std.testing.expect(!read_success);
}

test "SimulatedDisk with errors" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var disk = try SimulatedDisk.init(allocator, scheduler);
    defer disk.deinit();

    // Set error probabilities to 100%
    disk.setWriteErrorProbability(1.0);

    // Set up test context
    var read_success = false;
    var write_success = false;
    var test_ctx = TestContext{
        .read_success = &read_success,
        .write_success = &write_success,
    };

    // Write data (should fail)
    try disk.write("test.txt", "Hello, World!", testDiskCallback, &test_ctx);

    // Run the scheduler to process the write
    try scheduler.run(null);

    // Check that write failed
    try std.testing.expect(!write_success);

    // Reset error probabilities
    disk.setWriteErrorProbability(0.0);

    // Write data (should succeed now)
    try disk.write("test.txt", "Hello, World!", testDiskCallback, &test_ctx);

    // Run the scheduler to process the write
    try scheduler.run(null);

    // Check that write succeeded
    try std.testing.expect(write_success);

    // Set read error probability to 100%
    disk.setReadErrorProbability(1.0);

    // Reset flags
    read_success = false;

    // Read data (should fail)
    try disk.read("test.txt", testDiskCallback, &test_ctx);

    // Run the scheduler to process the read
    try scheduler.run(null);

    // Check that read failed
    try std.testing.expect(!read_success);
}

test "SimulatedDisk with corruption" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var disk = try SimulatedDisk.init(allocator, scheduler);
    defer disk.deinit();

    // Write data
    try disk.write("test.txt", "Hello, World!", testDiskCallback, null);

    // Run the scheduler to process the write
    try scheduler.run(null);

    // Inject corruption
    try disk.injectCorruption("test.txt");

    // Set up test context for corruption check
    var corruption_detected = false;
    var test_ctx = TestCorruptionContext{
        .original_data = "Hello, World!",
        .corruption_detected = &corruption_detected,
    };

    // Read data and check for corruption
    try disk.read("test.txt", testCorruptionCallback, &test_ctx);

    // Run the scheduler to process the read
    try scheduler.run(null);

    // Check that corruption was detected
    try std.testing.expect(corruption_detected);
}

const TestContext = struct {
    read_success: *bool,
    write_success: *bool,
};

fn testDiskCallback(
    op: SimulatedDisk.DiskOperation,
    path: []const u8,
    data: ?[]const u8,
    error_code: ?anyerror,
    context: ?*anyopaque,
) void {
    _ = path;

    if (context) |ctx| {
        const test_ctx = @as(*TestContext, @ptrCast(@alignCast(ctx)));

        switch (op) {
            .Read => {
                if (error_code == null and data != null) {
                    test_ctx.read_success.* = true;
                }
            },
            .Write => {
                if (error_code == null) {
                    test_ctx.write_success.* = true;
                }
            },
        }
    }
}

const TestCorruptionContext = struct {
    original_data: []const u8,
    corruption_detected: *bool,
};

fn testCorruptionCallback(
    op: SimulatedDisk.DiskOperation,
    path: []const u8,
    data: ?[]const u8,
    error_code: ?anyerror,
    context: ?*anyopaque,
) void {
    _ = path;
    _ = op;

    if (error_code != null) {
        return;
    }

    if (context) |ctx| {
        const test_ctx = @as(*TestCorruptionContext, @ptrCast(@alignCast(ctx)));

        if (data) |actual_data| {
            // Check if data is different from original
            if (actual_data.len == test_ctx.original_data.len) {
                var is_different = false;
                for (actual_data, 0..) |byte, i| {
                    if (byte != test_ctx.original_data[i]) {
                        is_different = true;
                        break;
                    }
                }
                test_ctx.corruption_detected.* = is_different;
            } else {
                test_ctx.corruption_detected.* = true;
            }
        }
    }
}
