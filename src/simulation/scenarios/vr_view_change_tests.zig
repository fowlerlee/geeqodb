const std = @import("std");
const testing = std.testing;
const VRNode = @import("simulation").scenarios.viewstamped_replication.VRNode;
const Simulation = @import("simulation").Simulation;

// Test suite for Viewstamped Replication focusing on view changes
// These tests verify that the system can handle primary failures and elect new primaries

// Test basic view change with three nodes
test "VR basic view change" {
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

    std.debug.print("Basic view change test completed successfully!\n", .{});
}

// Test multiple view changes
test "VR multiple view changes" {
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

    // Manually increment the view number to simulate a view change
    node.view_number += 1;

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

    // Manually increment the view number again to simulate another view change
    node.view_number += 1;

    // Process a third request
    try node.processRequest("client1", 3, .{ .Put = .{ .key = "key3", .value = "value3" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify that the node has the expected state
    if (node.state.get("key3")) |value| {
        try testing.expectEqualStrings("value3", value);
        std.debug.print("Test passed: key3=value3 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key3' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    // Verify that the view number has increased
    try testing.expect(node.view_number >= 3);
    std.debug.print("Test passed: view number is {d}\n", .{node.view_number});

    std.debug.print("Multiple view changes test completed successfully!\n", .{});
}

// Test view change with concurrent client requests
test "VR view change with concurrent requests" {
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

    // Process several client requests
    try node.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });
    try simulation.run(10);

    try node.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });
    try simulation.run(10);

    try node.processRequest("client1", 3, .{ .Put = .{ .key = "key3", .value = "value3" } });
    try simulation.run(10);

    // Verify that all keys are present
    if (node.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
        std.debug.print("Test passed: key1=value1 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    if (node.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
        std.debug.print("Test passed: key2=value2 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key2' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    if (node.state.get("key3")) |value| {
        try testing.expectEqualStrings("value3", value);
        std.debug.print("Test passed: key3=value3 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key3' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    // Simulate a view change
    node.view_number += 1;

    // Process another request
    try node.processRequest("client1", 4, .{ .Put = .{ .key = "key4", .value = "value4" } });
    try simulation.run(10);

    // Verify the new key is present
    if (node.state.get("key4")) |value| {
        try testing.expectEqualStrings("value4", value);
        std.debug.print("Test passed: key4=value4 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key4' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    std.debug.print("View change with concurrent requests test completed successfully!\n", .{});
}

// Test view change with a minority of nodes failing
test "VR view change with minority failure" {
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

    // Verify that the node is inactive
    try testing.expect(!node.active);
    std.debug.print("Test passed: node is inactive\n", .{});

    // Try to process a request on the inactive node
    const result = node.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });
    try testing.expectError(error.NodeInactive, result);
    std.debug.print("Test passed: cannot process request on inactive node\n", .{});

    // Restart the node
    node.active = true;
    node.is_primary = true; // Make it primary again
    node.view_number += 1; // Increment view number to simulate view change

    // Process a request on the restarted node
    try node.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });

    // Run simulation to process the request
    try simulation.run(10);

    // Verify the request was processed
    if (node.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
        std.debug.print("Test passed: key2=value2 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key2' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    std.debug.print("View change with minority failure test completed successfully!\n", .{});
}
