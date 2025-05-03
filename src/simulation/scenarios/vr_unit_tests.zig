const std = @import("std");
const testing = std.testing;
const VRNode = @import("simulation").scenarios.viewstamped_replication.VRNode;
const Simulation = @import("simulation").Simulation;

// Test suite for Viewstamped Replication implementation
// These tests focus on the core functionality without complex simulation scenarios

// Test basic initialization of a VR node
test "VRNode initialization" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    // Create a simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create a node
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up resources
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();
        node.do_view_change_msgs.clearAndFree();
        node.deinit();
    }

    // Verify initial state
    try testing.expectEqualStrings("node1", node.id);
    try testing.expectEqual(false, node.is_primary);
    try testing.expectEqual(@as(u64, 0), node.view_number);
    try testing.expectEqual(@as(u64, 0), node.op_number);
    try testing.expectEqual(@as(u64, 0), node.commit_number);
    try testing.expectEqual(true, node.active);
}

// Test starting a node as primary
test "VRNode start as primary" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    // Create a simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create a node with no peers (should become primary)
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up resources
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();
        node.do_view_change_msgs.clearAndFree();
        node.deinit();
    }

    // Start the node
    try node.start();

    // Run simulation briefly to process any events
    try simulation.run(10);

    // Verify the node became primary
    try testing.expectEqual(true, node.is_primary);
    try testing.expectEqual(@as(u64, 1), node.view_number);
}

// Test basic put operation on a primary node
test "VRNode put operation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    // Create a simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create a node with no peers
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up resources
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();
        node.do_view_change_msgs.clearAndFree();
        node.deinit();
    }

    // Start the node
    try node.start();

    // Run simulation briefly
    try simulation.run(10);

    // Process a put request
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify the state was updated
    if (node.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.KeyNotFound;
    }

    // Verify log and commit number
    try testing.expectEqual(@as(u64, 1), node.log.items.len);
    try testing.expectEqual(@as(u64, 1), node.op_number);
    try testing.expectEqual(@as(u64, 1), node.commit_number);
}

// Test error handling when trying to process a request on a non-primary node
test "VRNode error on non-primary request" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    // Create a simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create a node
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up resources
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();
        node.do_view_change_msgs.clearAndFree();
        node.deinit();
    }

    // Start the node but force it to be a backup
    try node.start();
    node.is_primary = false;

    // Try to process a request, should fail with NotPrimary error
    const result = node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });
    try testing.expectError(error.NotPrimary, result);
}

// Test error handling when trying to process a request on an inactive node
test "VRNode error on inactive node" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    // Create a simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create a node
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up resources
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();
        node.do_view_change_msgs.clearAndFree();
        node.deinit();
    }

    // Start the node but then stop it
    try node.start();
    node.stop();

    // Try to process a request, should fail with NodeInactive error
    const result = node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });
    try testing.expectError(error.NodeInactive, result);
}

// Test multiple operations on a node
test "VRNode multiple operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    // Create a simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create a node
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up resources
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();
        node.do_view_change_msgs.clearAndFree();
        node.deinit();
    }

    // Start the node
    try node.start();
    try simulation.run(10);

    // Process multiple requests
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });
    try simulation.run(10);

    try node.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });
    try simulation.run(10);

    try node.processRequest("client1", 3, .{ .Delete = .{ .key = "key1" } });
    try simulation.run(10);

    // Verify the state
    try testing.expect(!node.state.contains("key1")); // key1 should be deleted

    if (node.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
    } else {
        return error.KeyNotFound;
    }

    // Verify log and commit number
    try testing.expectEqual(@as(u64, 3), node.log.items.len);
    try testing.expectEqual(@as(u64, 3), node.op_number);
    try testing.expectEqual(@as(u64, 3), node.commit_number);
}
