const std = @import("std");
const Simulation = @import("simulation").Simulation;
const VRNode = @import("simulation").scenarios.viewstamped_replication.VRNode;

pub fn main() !void {
    // Create a simulation with debug allocator for better memory leak detection
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
        // Clear all tasks manually since there's no clearAllTasks method
        while (simulation.scheduler.tasks.items.len > 0) {
            _ = simulation.scheduler.tasks.pop();
        }

        // Now deinit the simulation
        simulation.deinit();
    }

    // Create a single node for a simplified test
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer {
        // Explicitly clean up any resources before deinit
        node.log.clearAndFree();
        node.start_view_change_acks.clearAndFree();

        // Clear do_view_change_msgs
        var it = node.do_view_change_msgs.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.log) |log| {
                allocator.free(log);
            }
        }
        node.do_view_change_msgs.clearAndFree();

        // Now deinit the node
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
        try std.testing.expectEqualStrings("value1", value);
        std.debug.print("Test passed: key1=value1 found in node state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in node {s}.\n", .{node.id});
        return error.KeyNotFound;
    }

    std.debug.print("Viewstamped Replication test completed successfully!\n", .{});
}
