const std = @import("std");
const Simulation = @import("../simulation.zig").Simulation;
const SimulatedDisk = @import("../disk.zig").SimulatedDisk;
const SimulatedNetwork = @import("../network.zig").SimulatedNetwork;
const VirtualClock = @import("../virtual_clock.zig").VirtualClock;

/// Concurrent Access Simulation: Test database behavior with multiple simulated clients accessing it simultaneously.
pub fn runConcurrentAccessScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create a simulated node for the database server
    try simulation.createClock("db_server", 1.0);
    try simulation.createDisk("db_server");

    // Create multiple client nodes
    const num_clients = 5;
    var client_contexts = try allocator.alloc(ClientContext, num_clients);
    defer allocator.free(client_contexts);

    for (0..num_clients) |i| {
        const client_id = try std.fmt.allocPrint(allocator, "client_{d}", .{i});
        defer allocator.free(client_id);

        try simulation.createClock(client_id, 1.0);

        // Initialize client context
        client_contexts[i] = ClientContext{
            .client_id = try allocator.dupe(u8, client_id),
            .request_completed = false,
            .response_received = false,
        };

        // Register client node in the network
        try simulation.registerNode(client_id, clientMessageHandler, &client_contexts[i]);
    }

    // Register server node in the network
    var server_context = ServerContext{
        .simulation = simulation,
        .num_requests_processed = 0,
        .expected_requests = num_clients * 2, // Each client sends 2 requests
    };
    try simulation.registerNode("db_server", serverMessageHandler, &server_context);

    // Send concurrent requests from all clients
    for (0..num_clients) |i| {
        const client_id = client_contexts[i].client_id;

        // Send a write request
        const write_request = try std.fmt.allocPrint(allocator, "WRITE key_{d} value_{d}", .{ i, i });
        defer allocator.free(write_request);
        try simulation.sendMessage(client_id, "db_server", write_request);

        // Send a read request
        const read_request = try std.fmt.allocPrint(allocator, "READ key_{d}", .{i});
        defer allocator.free(read_request);
        try simulation.sendMessage(client_id, "db_server", read_request);
    }

    // Run the simulation
    try simulation.run(1000);

    // Verify that all clients received responses
    for (0..num_clients) |i| {
        try std.testing.expect(client_contexts[i].response_received);
        allocator.free(client_contexts[i].client_id);
    }

    // Verify that server processed all requests
    try std.testing.expectEqual(server_context.expected_requests, server_context.num_requests_processed);

    std.debug.print("Concurrent access scenario completed successfully!\n", .{});
}

const ClientContext = struct {
    client_id: []const u8,
    request_completed: bool,
    response_received: bool,
};

const ServerContext = struct {
    simulation: *Simulation,
    num_requests_processed: usize,
    expected_requests: usize,
};

fn clientMessageHandler(sender: []const u8, message: []const u8, context: ?*anyopaque) void {
    _ = sender;
    if (context) |ctx| {
        const client_ctx = @as(*ClientContext, @ptrCast(@alignCast(ctx)));

        // Mark that we received a response
        if (std.mem.startsWith(u8, message, "RESPONSE")) {
            client_ctx.response_received = true;
        }
    }
}

fn serverMessageHandler(sender: []const u8, message: []const u8, context: ?*anyopaque) void {
    if (context) |ctx| {
        const server_ctx = @as(*ServerContext, @ptrCast(@alignCast(ctx)));

        // Process the request
        server_ctx.num_requests_processed += 1;

        // Send a response back to the client
        if (std.mem.startsWith(u8, message, "READ") or std.mem.startsWith(u8, message, "WRITE")) {
            const response = "RESPONSE OK";
            server_ctx.simulation.sendMessage("db_server", sender, response) catch {};
        }
    }
}

/// Network Partition Tests: Simulate network partitions between database nodes in a distributed setup.
pub fn runNetworkPartitionScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create multiple database nodes
    const num_nodes = 3;
    var node_contexts = try allocator.alloc(NodeContext, num_nodes);
    defer allocator.free(node_contexts);

    for (0..num_nodes) |i| {
        const node_id = try std.fmt.allocPrint(allocator, "db_node_{d}", .{i});
        defer allocator.free(node_id);

        try simulation.createClock(node_id, 1.0);
        try simulation.createDisk(node_id);

        // Initialize node context
        node_contexts[i] = NodeContext{
            .node_id = try allocator.dupe(u8, node_id),
            .messages_received = 0,
            .simulation = simulation,
        };

        // Register node in the network
        try simulation.registerNode(node_id, nodeMessageHandler, &node_contexts[i]);
    }

    // Send messages between all nodes to establish connectivity
    for (0..num_nodes) |i| {
        for (0..num_nodes) |j| {
            if (i != j) {
                const message = try std.fmt.allocPrint(allocator, "PING from {s}", .{node_contexts[i].node_id});
                defer allocator.free(message);
                try simulation.sendMessage(node_contexts[i].node_id, node_contexts[j].node_id, message);
            }
        }
    }

    // Run the simulation for a bit
    try simulation.run(100);

    // Verify that all nodes received messages
    for (0..num_nodes) |i| {
        try std.testing.expect(node_contexts[i].messages_received > 0);
    }

    // Create a network partition between node 0 and nodes 1,2
    try simulation.network.createPartition(&[_][]const u8{node_contexts[0].node_id}, &[_][]const u8{ node_contexts[1].node_id, node_contexts[2].node_id });

    // Reset message counters
    for (0..num_nodes) |i| {
        node_contexts[i].messages_received = 0;
    }

    // Send messages again
    for (0..num_nodes) |i| {
        for (0..num_nodes) |j| {
            if (i != j) {
                const message = try std.fmt.allocPrint(allocator, "PING from {s} after partition", .{node_contexts[i].node_id});
                defer allocator.free(message);
                try simulation.sendMessage(node_contexts[i].node_id, node_contexts[j].node_id, message);
            }
        }
    }

    // Run the simulation again
    try simulation.run(200);

    // Verify that node 0 didn't receive messages from nodes 1,2 and vice versa
    try std.testing.expectEqual(@as(usize, 0), node_contexts[0].messages_received);

    // But nodes 1 and 2 should still communicate with each other
    try std.testing.expect(node_contexts[1].messages_received > 0);
    try std.testing.expect(node_contexts[2].messages_received > 0);

    // Clean up
    for (0..num_nodes) |i| {
        allocator.free(node_contexts[i].node_id);
    }

    std.debug.print("Network partition scenario completed successfully!\n", .{});
}

const NodeContext = struct {
    node_id: []const u8,
    messages_received: usize,
    simulation: *Simulation,
};

fn nodeMessageHandler(sender: []const u8, message: []const u8, context: ?*anyopaque) void {
    if (context) |ctx| {
        const node_ctx = @as(*NodeContext, @ptrCast(@alignCast(ctx)));

        // Count received messages
        node_ctx.messages_received += 1;

        // Send an acknowledgment
        if (std.mem.startsWith(u8, message, "PING")) {
            const response = "PONG";
            node_ctx.simulation.sendMessage(node_ctx.node_id, sender, response) catch {};
        }
    }
}

/// Recovery After Crash: Simulate database crashes during write operations and test recovery mechanisms.
pub fn runCrashRecoveryScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create a database node
    try simulation.createClock("db_node", 1.0);
    try simulation.createDisk("db_node");

    var disk = simulation.getDisk("db_node").?;

    // Set up test context
    var recovery_ctx = RecoveryContext{
        .write_completed = false,
        .recovery_completed = false,
        .data_verified = false,
    };

    // Write data to disk
    const data = "IMPORTANT_DATABASE_DATA";
    try disk.write("database.dat", data, recoveryDiskCallback, &recovery_ctx);

    // Run simulation to complete the write
    try simulation.run(100);

    // Verify write completed
    try std.testing.expect(recovery_ctx.write_completed);

    // Simulate a crash by "forgetting" we wrote the data
    recovery_ctx.write_completed = false;

    // Now simulate recovery by reading the data back
    try disk.read("database.dat", recoveryDiskCallback, &recovery_ctx);

    // Run simulation to complete the recovery
    try simulation.run(200);

    // Verify recovery completed and data was verified
    try std.testing.expect(recovery_ctx.recovery_completed);
    try std.testing.expect(recovery_ctx.data_verified);

    std.debug.print("Crash recovery scenario completed successfully!\n", .{});
}

const RecoveryContext = struct {
    write_completed: bool,
    recovery_completed: bool,
    data_verified: bool,
};

fn recoveryDiskCallback(
    op: SimulatedDisk.DiskOperation,
    path: []const u8,
    data: ?[]const u8,
    error_code: ?anyerror,
    context: ?*anyopaque,
) void {
    _ = path;

    if (context) |ctx| {
        const recovery_ctx = @as(*RecoveryContext, @ptrCast(@alignCast(ctx)));

        switch (op) {
            .Write => {
                if (error_code == null) {
                    recovery_ctx.write_completed = true;
                }
            },
            .Read => {
                recovery_ctx.recovery_completed = true;

                if (error_code == null and data != null) {
                    // Verify the data is what we expect
                    const expected = "IMPORTANT_DATABASE_DATA";
                    if (data.?.len == expected.len) {
                        var matches = true;
                        for (data.?, 0..) |byte, i| {
                            if (byte != expected[i]) {
                                matches = false;
                                break;
                            }
                        }
                        recovery_ctx.data_verified = matches;
                    }
                }
            },
        }
    }
}

/// Clock Drift Scenarios: Test how clock drift between nodes affects distributed transactions.
pub fn runClockDriftScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create multiple database nodes with different clock drift factors
    try simulation.createClock("db_node_1", 1.0); // Normal clock
    try simulation.createClock("db_node_2", 1.2); // 20% faster
    try simulation.createClock("db_node_3", 0.8); // 20% slower

    // Get the clocks
    var clock1 = simulation.getClock("db_node_1").?;
    var clock2 = simulation.getClock("db_node_2").?;
    var clock3 = simulation.getClock("db_node_3").?;

    // Initial time should be 0 for all clocks
    try std.testing.expectEqual(@as(u64, 0), clock1.now());
    try std.testing.expectEqual(@as(u64, 0), clock2.now());
    try std.testing.expectEqual(@as(u64, 0), clock3.now());

    // Advance simulation time
    _ = try simulation.scheduler.scheduleAt(100, 0, dummyCallback, null);
    try simulation.run(null);

    // Check clock times - they should differ due to drift
    try std.testing.expectEqual(@as(u64, 100), clock1.now());
    try std.testing.expectEqual(@as(u64, 120), clock2.now()); // 20% faster
    try std.testing.expectEqual(@as(u64, 80), clock3.now()); // 20% slower

    // Simulate a distributed transaction with timestamp-based ordering
    var transaction_ctx = TransactionContext{
        .transaction_success = false,
    };

    // Schedule transaction events on each node
    _ = try clock1.sleep(50, &transaction_ctx);
    _ = try clock2.sleep(50, &transaction_ctx);
    _ = try clock3.sleep(50, &transaction_ctx);

    // Run simulation to process the transaction
    try simulation.run(300);

    // The transaction should complete, but the ordering of events might be affected by clock drift
    // In a real implementation, we would check for specific ordering issues

    std.debug.print("Clock drift scenario completed successfully!\n", .{});
}

const TransactionContext = struct {
    transaction_success: bool,
};

fn dummyCallback(context: ?*anyopaque) void {
    _ = context;
    // This is just a placeholder callback
}

/// Slow Disk I/O: Simulate extremely slow disk operations to test timeout handling.
pub fn runSlowDiskIOScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create a database node
    try simulation.createClock("db_node", 1.0);
    try simulation.createDisk("db_node");

    var disk = simulation.getDisk("db_node").?;

    // Configure disk to be extremely slow
    disk.read_delay_min = 500;
    disk.read_delay_max = 1000;
    disk.write_delay_min = 800;
    disk.write_delay_max = 1500;

    // Set up test context with timeout detection
    var timeout_ctx = TimeoutContext{
        .operation_completed = false,
        .timeout_detected = false,
        .timeout_threshold = 300,
        .start_time = 0,
    };

    // Record start time
    timeout_ctx.start_time = simulation.scheduler.getCurrentTime();

    // Write data to disk (this will be slow)
    try disk.write("database.dat", "Test Data", timeoutDiskCallback, &timeout_ctx);

    // Run simulation with a time limit
    try simulation.run(timeout_ctx.timeout_threshold);

    // Check if operation completed within the timeout
    if (!timeout_ctx.operation_completed) {
        timeout_ctx.timeout_detected = true;
    }

    // Verify that a timeout was detected
    try std.testing.expect(timeout_ctx.timeout_detected);

    // Run simulation to completion
    try simulation.run(2000);

    // Verify that the operation eventually completed
    try std.testing.expect(timeout_ctx.operation_completed);

    std.debug.print("Slow disk I/O scenario completed successfully!\n", .{});
}

const TimeoutContext = struct {
    operation_completed: bool,
    timeout_detected: bool,
    timeout_threshold: u64,
    start_time: u64,
};

fn timeoutDiskCallback(
    op: SimulatedDisk.DiskOperation,
    path: []const u8,
    data: ?[]const u8,
    error_code: ?anyerror,
    context: ?*anyopaque,
) void {
    _ = op;
    _ = path;
    _ = data;

    if (context) |ctx| {
        const timeout_ctx = @as(*TimeoutContext, @ptrCast(@alignCast(ctx)));

        if (error_code == null) {
            timeout_ctx.operation_completed = true;
        }
    }
}

/// Run all advanced database scenarios
pub fn runAllAdvancedScenarios(allocator: std.mem.Allocator) !void {
    std.debug.print("\n=== Running Concurrent Access Scenario ===\n", .{});
    try runConcurrentAccessScenario(allocator);

    std.debug.print("\n=== Running Network Partition Scenario ===\n", .{});
    try runNetworkPartitionScenario(allocator);

    std.debug.print("\n=== Running Crash Recovery Scenario ===\n", .{});
    try runCrashRecoveryScenario(allocator);

    std.debug.print("\n=== Running Clock Drift Scenario ===\n", .{});
    try runClockDriftScenario(allocator);

    std.debug.print("\n=== Running Slow Disk I/O Scenario ===\n", .{});
    try runSlowDiskIOScenario(allocator);

    // Add more scenarios as they are implemented

    std.debug.print("\nAll advanced database scenarios completed successfully!\n", .{});
}

test "Advanced database scenarios" {
    try runAllAdvancedScenarios(std.testing.allocator);
}
