const std = @import("std");
const Simulation = @import("../simulation.zig").Simulation;
const SimulatedDisk = @import("../disk.zig").SimulatedDisk;

/// A simple integration test that simulates a GeeqoDB database with simulated disk I/O
pub fn runDatabaseIntegrationScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();
    
    // Create a simulated node for the database
    try simulation.createClock("db_node", 1.0);
    try simulation.createDisk("db_node");
    
    // Get the simulated disk
    var disk = simulation.getDisk("db_node").?;
    
    // Set up test context
    var read_success = false;
    var write_success = false;
    var test_ctx = TestContext{
        .read_success = &read_success,
        .write_success = &write_success,
    };
    
    // Simulate writing a database file
    try disk.write("database.dat", "GeeqoDB Database File v1.0", testDiskCallback, &test_ctx);
    
    // Run the simulation to process the write
    try simulation.run(100);
    
    // Check that write succeeded
    try std.testing.expect(write_success);
    
    // Reset flags
    write_success = false;
    
    // Simulate reading the database file
    try disk.read("database.dat", testDiskCallback, &test_ctx);
    
    // Run the simulation to process the read
    try simulation.run(200);
    
    // Check that read succeeded
    try std.testing.expect(read_success);
    
    // Simulate disk corruption
    try disk.injectCorruption("database.dat");
    
    // Set up corruption detection context
    var corruption_detected = false;
    var corruption_ctx = TestCorruptionContext{
        .original_data = "GeeqoDB Database File v1.0",
        .corruption_detected = &corruption_detected,
    };
    
    // Read the corrupted file
    try disk.read("database.dat", testCorruptionCallback, &corruption_ctx);
    
    // Run the simulation to process the read
    try simulation.run(300);
    
    // Check that corruption was detected
    try std.testing.expect(corruption_detected);
    
    std.debug.print("Database integration scenario completed successfully!\n", .{});
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

test "Database integration scenario" {
    try runDatabaseIntegrationScenario(std.testing.allocator);
}
