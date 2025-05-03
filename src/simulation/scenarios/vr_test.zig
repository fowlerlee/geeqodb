const std = @import("std");
const Simulation = @import("simulation").Simulation;
const VRNode = @import("simulation").scenarios.viewstamped_replication.VRNode;

pub fn main() !void {
    // Create a simulation
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create a single node for a simplified test
    var node = try VRNode.init(allocator, simulation, "node1", &[_][]const u8{});
    defer node.deinit();

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
