const std = @import("std");
const testing = std.testing;
const Simulation = @import("simulation").Simulation;
const VRNode = @import("simulation").scenarios.viewstamped_replication.VRNode;
const ViewChangeStatus = @import("simulation").scenarios.viewstamped_replication.ViewChangeStatus;

// Unit tests for Viewstamped Replication implementation
// These tests focus on the core functionality of the VR protocol

test "VRNode initialization" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

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
    var node = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{});
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
    try testing.expectEqual(ViewChangeStatus.Normal, node.view_change_status);
}

test "VRNode becomes primary with no peers" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

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
    var node = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{});
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

test "VRNode processes request as primary" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

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
    var node = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{});
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

    // Verify log state
    try testing.expectEqual(@as(usize, 1), node.log.items.len);
    try testing.expectEqual(@as(u64, 1), node.op_number);
    try testing.expectEqual(@as(u64, 1), node.commit_number);
}

test "VRNode rejects request when not primary" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

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
    var node = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{});
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

test "VRNode rejects request when inactive" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

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
    var node = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{});
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

test "VRNode two-node replication" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create two nodes
    var node1 = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{"node2"});
    defer {
        node1.log.clearAndFree();
        node1.start_view_change_acks.clearAndFree();
        node1.do_view_change_msgs.clearAndFree();
        node1.deinit();
    }

    var node2 = try VRNode.init(allocator, &simulation, "node2", &[_][]const u8{"node1"});
    defer {
        node2.log.clearAndFree();
        node2.start_view_change_acks.clearAndFree();
        node2.do_view_change_msgs.clearAndFree();
        node2.deinit();
    }

    // Start both nodes
    try node1.start();
    try node2.start();

    // Run simulation to let nodes initialize
    try simulation.run(50);

    // Ensure node1 is primary
    node1.is_primary = true;
    node1.view_number = 1;
    node2.is_primary = false;
    node2.view_number = 1;

    // Process a request on the primary
    try node1.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to allow replication
    try simulation.run(100);

    // Verify both nodes have the update
    if (node1.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.KeyNotFoundInNode1;
    }

    if (node2.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.KeyNotFoundInNode2;
    }

    // Verify log state on both nodes
    try testing.expectEqual(@as(usize, 1), node1.log.items.len);
    try testing.expectEqual(@as(u64, 1), node1.op_number);
    try testing.expectEqual(@as(u64, 1), node1.commit_number);

    try testing.expectEqual(@as(usize, 1), node2.log.items.len);
    try testing.expectEqual(@as(u64, 1), node2.op_number);
    try testing.expectEqual(@as(u64, 1), node2.commit_number);
}

test "VRNode view change" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create three nodes
    var node1 = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{ "node2", "node3" });
    defer {
        node1.log.clearAndFree();
        node1.start_view_change_acks.clearAndFree();
        node1.do_view_change_msgs.clearAndFree();
        node1.deinit();
    }

    var node2 = try VRNode.init(allocator, &simulation, "node2", &[_][]const u8{ "node1", "node3" });
    defer {
        node2.log.clearAndFree();
        node2.start_view_change_acks.clearAndFree();
        node2.do_view_change_msgs.clearAndFree();
        node2.deinit();
    }

    var node3 = try VRNode.init(allocator, &simulation, "node3", &[_][]const u8{ "node1", "node2" });
    defer {
        node3.log.clearAndFree();
        node3.start_view_change_acks.clearAndFree();
        node3.do_view_change_msgs.clearAndFree();
        node3.deinit();
    }

    // Start all nodes
    try node1.start();
    try node2.start();
    try node3.start();

    // Run simulation to let nodes initialize
    try simulation.run(50);

    // Ensure node1 is primary
    node1.is_primary = true;
    node1.view_number = 1;
    node2.is_primary = false;
    node2.view_number = 1;
    node3.is_primary = false;
    node3.view_number = 1;

    // Process a request on the primary
    try node1.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to allow replication
    try simulation.run(100);

    // Verify all nodes have the update
    if (node1.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.KeyNotFoundInNode1;
    }

    if (node2.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.KeyNotFoundInNode2;
    }

    if (node3.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.KeyNotFoundInNode3;
    }

    // Simulate primary failure by stopping node1
    node1.stop();

    // Manually trigger view change on node2
    try node2.startViewChange();

    // Run simulation to allow view change
    try simulation.run(200);

    // Verify node2 or node3 is now primary
    try testing.expect(node2.is_primary or node3.is_primary);
    try testing.expectEqual(@as(u64, 2), node2.view_number);
    try testing.expectEqual(@as(u64, 2), node3.view_number);

    // Find the new primary
    const new_primary = if (node2.is_primary) node2 else node3;
    const backup = if (node2.is_primary) node3 else node2;

    // Process a new request on the new primary
    try new_primary.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });

    // Run simulation to allow replication
    try simulation.run(100);

    // Verify both active nodes have the new update
    if (new_primary.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
    } else {
        return error.Key2NotFoundInPrimary;
    }

    if (backup.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
    } else {
        return error.Key2NotFoundInBackup;
    }
}

test "VRNode recovery" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create three nodes
    var node1 = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{ "node2", "node3" });
    defer {
        node1.log.clearAndFree();
        node1.start_view_change_acks.clearAndFree();
        node1.do_view_change_msgs.clearAndFree();
        node1.deinit();
    }

    var node2 = try VRNode.init(allocator, &simulation, "node2", &[_][]const u8{ "node1", "node3" });
    defer {
        node2.log.clearAndFree();
        node2.start_view_change_acks.clearAndFree();
        node2.do_view_change_msgs.clearAndFree();
        node2.deinit();
    }

    var node3 = try VRNode.init(allocator, &simulation, "node3", &[_][]const u8{ "node1", "node2" });
    defer {
        node3.log.clearAndFree();
        node3.start_view_change_acks.clearAndFree();
        node3.do_view_change_msgs.clearAndFree();
        node3.deinit();
    }

    // Start all nodes
    try node1.start();
    try node2.start();
    try node3.start();

    // Run simulation to let nodes initialize
    try simulation.run(50);

    // Ensure node1 is primary
    node1.is_primary = true;
    node1.view_number = 1;
    node2.is_primary = false;
    node2.view_number = 1;
    node3.is_primary = false;
    node3.view_number = 1;

    // Process a request on the primary
    try node1.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });

    // Run simulation to allow replication
    try simulation.run(100);

    // Stop node3 (simulating a crash)
    node3.stop();

    // Process another request on the primary
    try node1.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });

    // Run simulation to allow replication
    try simulation.run(100);

    // Restart node3
    node3.active = true;
    node3.recovery_mode = true;
    node3.view_change_status = .Recovering;

    // Request state transfer from node1
    try node3.requestStateTransfer("node1");

    // Run simulation to allow state transfer
    try simulation.run(200);

    // Verify node3 has recovered both keys
    if (node3.state.get("key1")) |value| {
        try testing.expectEqualStrings("value1", value);
    } else {
        return error.Key1NotFoundInNode3;
    }

    if (node3.state.get("key2")) |value| {
        try testing.expectEqualStrings("value2", value);
    } else {
        return error.Key2NotFoundInNode3;
    }

    // Verify node3 is no longer in recovery mode
    try testing.expectEqual(false, node3.recovery_mode);
    try testing.expectEqual(ViewChangeStatus.Normal, node3.view_change_status);
}

test "VRNode multiple operations" {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer {
        // Clear all tasks manually
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }
        simulation.deinit();
    }

    // Create three nodes
    var node1 = try VRNode.init(allocator, &simulation, "node1", &[_][]const u8{ "node2", "node3" });
    defer {
        node1.log.clearAndFree();
        node1.start_view_change_acks.clearAndFree();
        node1.do_view_change_msgs.clearAndFree();
        node1.deinit();
    }

    var node2 = try VRNode.init(allocator, &simulation, "node2", &[_][]const u8{ "node1", "node3" });
    defer {
        node2.log.clearAndFree();
        node2.start_view_change_acks.clearAndFree();
        node2.do_view_change_msgs.clearAndFree();
        node2.deinit();
    }

    var node3 = try VRNode.init(allocator, &simulation, "node3", &[_][]const u8{ "node1", "node2" });
    defer {
        node3.log.clearAndFree();
        node3.start_view_change_acks.clearAndFree();
        node3.do_view_change_msgs.clearAndFree();
        node3.deinit();
    }

    // Start all nodes
    try node1.start();
    try node2.start();
    try node3.start();

    // Run simulation to let nodes initialize
    try simulation.run(50);

    // Ensure node1 is primary
    node1.is_primary = true;
    node1.view_number = 1;
    node2.is_primary = false;
    node2.view_number = 1;
    node3.is_primary = false;
    node3.view_number = 1;

    // Process multiple operations
    try node1.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });
    try simulation.run(50);

    try node1.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });
    try simulation.run(50);

    try node1.processRequest("client1", 3, .{ .Put = .{ .key = "key3", .value = "value3" } });
    try simulation.run(50);

    try node1.processRequest("client1", 4, .{ .Delete = .{ .key = "key2" } });
    try simulation.run(50);

    try node1.processRequest("client1", 5, .{ .Put = .{ .key = "key4", .value = "value4" } });
    try simulation.run(100);

    // Verify all nodes have the correct final state
    for ([_]*VRNode{ node1, node2, node3 }) |node| {
        // key1 should exist
        if (node.state.get("key1")) |value| {
            try testing.expectEqualStrings("value1", value);
        } else {
            return error.Key1NotFound;
        }

        // key2 should be deleted
        try testing.expect(node.state.get("key2") == null);

        // key3 should exist
        if (node.state.get("key3")) |value| {
            try testing.expectEqualStrings("value3", value);
        } else {
            return error.Key3NotFound;
        }

        // key4 should exist
        if (node.state.get("key4")) |value| {
            try testing.expectEqualStrings("value4", value);
        } else {
            return error.Key4NotFound;
        }

        // Verify log state
        try testing.expectEqual(@as(u64, 5), node.op_number);
        try testing.expectEqual(@as(u64, 5), node.commit_number);
    }
}
