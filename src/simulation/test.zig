const std = @import("std");
const Simulation = @import("simulation.zig").Simulation;
const vr_scenario = @import("scenarios/viewstamped_replication.zig");

test "Basic simulation test" {
    const allocator = std.testing.allocator;
    const seed = 42;
    
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();
    
    // Create a simple test node
    try simulation.createClock("test_node", 1.0);
    try simulation.createDisk("test_node");
    
    // Register a message handler
    var received = false;
    var ctx = TestContext{ .received = &received };
    try simulation.registerNode("test_node", testMessageHandler, &ctx);
    
    // Send a message to self
    try simulation.sendMessage("test_node", "test_node", "Hello, world!");
    
    // Run the simulation
    try simulation.run(null);
    
    // Check that the message was received
    try std.testing.expect(received);
}

test "Run Viewstamped Replication scenario" {
    try vr_scenario.runBasicScenario(std.testing.allocator);
}

const TestContext = struct {
    received: *bool,
};

fn testMessageHandler(sender: []const u8, message: []const u8, context: ?*anyopaque) void {
    _ = sender;
    _ = message;
    
    const ctx = @as(*TestContext, @ptrCast(@alignCast(context.?)));
    ctx.received.* = true;
}
