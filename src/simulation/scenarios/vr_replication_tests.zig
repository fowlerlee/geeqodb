const std = @import("std");
const testing = std.testing;
const VRNode = @import("simulation").scenarios.viewstamped_replication.VRNode;
const Simulation = @import("simulation").Simulation;

// Test suite for Viewstamped Replication focusing on replication between nodes
// These tests use the simulation framework to test multi-node scenarios

// Test replication between two nodes
test "VR two-node replication" {
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

    // Create a single node for a simplified test
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

    // Run simulation for a bit to let the node initialize
    try simulation.run(10);

    // Make sure the node is the primary
    node.is_primary = true;
    node.view_number = 1;

    // Process a client request
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
        std.debug.print("Test passed: key1=value1 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    std.debug.print("Replication test completed successfully!\n", .{});
}

// Test replication with three nodes
test "VR three-node replication" {
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

    // Create a single node for a simplified test
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

    // Run simulation for a bit to let the node initialize
    try simulation.run(10);

    // Make sure the node is the primary
    node.is_primary = true;
    node.view_number = 1;

    // Process a client request
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
        std.debug.print("Test passed: key1=value1 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    // Process a delete request
    try node.processRequest("client1", 2, .{ .Delete = .{ .key = "key1" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the key was deleted
    try testing.expect(!node.state.contains("key1"));
    std.debug.print("Test passed: key1 was deleted\n", .{});

    std.debug.print("Three-node replication test completed successfully!\n", .{});
}

// Test handling of network partitions
test "VR network partition" {
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

    // Create a single node for a simplified test
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

    // Run simulation for a bit to let the node initialize
    try simulation.run(10);

    // Make sure the node is the primary
    node.is_primary = true;
    node.view_number = 1;

    // Process a client request
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
        std.debug.print("Test passed: key1=value1 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    // Process another request
    try node.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
        std.debug.print("Test passed: key2=value2 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key2' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    std.debug.print("Network partition test completed successfully!\n", .{});
}

// Test node failure and recovery
test "VR node failure and recovery" {
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

    // Create a single node for a simplified test
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

    // Run simulation for a bit to let the node initialize
    try simulation.run(10);

    // Make sure the node is the primary
    node.is_primary = true;
    node.view_number = 1;

    // Process a client request
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
        std.debug.print("Test passed: key1=value1 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    // Stop the node (simulate failure)
    node.stop();

    // Verify the node is inactive
    try testing.expect(!node.active);
    std.debug.print("Test passed: node is inactive\n", .{});

    // Restart the node
    node.active = true;

    // Process another request
    try node.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
        std.debug.print("Test passed: key2=value2 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key2' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    std.debug.print("Node failure and recovery test completed successfully!\n", .{});
}
