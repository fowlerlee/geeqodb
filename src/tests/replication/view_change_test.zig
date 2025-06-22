const std = @import("std");
const testing = std.testing;
const Simulation = @import("../../simulation/simulation.zig").Simulation;

// Import replica management and view change functionality
const replica_management = @import("../../simulation/scenarios/replica_management.zig");
const ReplicaState = replica_management.ReplicaState;
const ReplicaRegistry = replica_management.ReplicaRegistry;
const view_change_protocol = @import("../../simulation/scenarios/view_change_protocol.zig");
const ViewChangeProtocol = view_change_protocol.ViewChangeProtocol;

test "ViewChangeProtocol initialization" {
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

    // Create a registry
    var registry = try ReplicaRegistry.init(allocator);
    defer registry.deinit();

    // Register some nodes
    try registry.registerReplica("node1", .PRIMARY);
    try registry.registerReplica("node2", .BACKUP);
    try registry.registerReplica("node3", .BACKUP);

    // Initialize view change protocol
    var vcp = try ViewChangeProtocol.init(allocator, simulation, registry);
    vcp.heartbeat_interval = 10;
    vcp.heartbeat_timeout = 30;
    defer vcp.deinit();

    // Check initial state
    try testing.expectEqual(@as(u64, 1), vcp.current_view);
    try testing.expectEqual(@as(usize, 0), vcp.view_change_requests.count());
    try testing.expectEqual(@as(usize, 0), vcp.view_change_responses.count());
}

test "ViewChangeProtocol heartbeat mechanism" {
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

    // Create a registry
    var registry = try ReplicaRegistry.init(allocator);
    defer registry.deinit();

    // Register some nodes
    try registry.registerReplica("node1", .PRIMARY);
    try registry.registerReplica("node2", .BACKUP);
    try registry.registerReplica("node3", .BACKUP);

    // Initialize view change protocol with short timeout for testing
    var vcp = try ViewChangeProtocol.init(allocator, simulation, registry);
    vcp.heartbeat_interval = 10; // Set short interval for testing
    vcp.heartbeat_timeout = 30; // Short timeout
    defer vcp.deinit();

    // Start heartbeat mechanism
    try vcp.startHeartbeat("node1"); // Primary node starts sending heartbeats

    // Run simulation briefly to send some heartbeats
    try simulation.run(25);

    // Check that backups received heartbeats
    try testing.expect(vcp.lastHeartbeatTime("node2") > 0);
    try testing.expect(vcp.lastHeartbeatTime("node3") > 0);

    // Primary should be healthy
    try testing.expect(vcp.isPrimaryHealthy());

    // Simulate primary failure (stop sending heartbeats)
    vcp.stopHeartbeat("node1");

    // Advance time significantly
    try simulation.run(100);

    // Primary should now be considered unhealthy
    try testing.expect(!vcp.isPrimaryHealthy());
}

test "ViewChangeProtocol view change process" {
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

    // Create a registry
    var registry = try ReplicaRegistry.init(allocator);
    defer registry.deinit();

    // Register some nodes
    try registry.registerReplica("node1", .PRIMARY);
    try registry.registerReplica("node2", .BACKUP);
    try registry.registerReplica("node3", .BACKUP);

    // Initialize view change protocol with short timeout for testing
    var vcp = try ViewChangeProtocol.init(allocator, simulation, registry);
    vcp.heartbeat_interval = 10;
    vcp.heartbeat_timeout = 30;
    defer vcp.deinit();

    // Start heartbeat mechanism
    try vcp.startHeartbeat("node1");

    // Run simulation briefly
    try simulation.run(20);

    // Simulate primary failure
    vcp.stopHeartbeat("node1");
    registry.removeReplica("node1"); // Primary is gone

    // Node2 initiates view change
    try vcp.initiateViewChange("node2");

    // Run simulation to allow view change messages to propagate
    try simulation.run(50);

    // Node3 also requests view change
    try vcp.handleViewChangeRequest("node3");

    // Run simulation to complete view change
    try simulation.run(100);

    // Check that view change completed
    try testing.expectEqual(@as(u64, 2), vcp.current_view);

    // A new primary should be elected
    const primary = registry.getPrimaryNode();
    try testing.expect(std.mem.eql(u8, primary, "node2") or std.mem.eql(u8, primary, "node3"));

    // Verify states
    try testing.expectEqual(ReplicaState.PRIMARY, registry.getReplicaState(primary));
    const backup_node = if (std.mem.eql(u8, primary, "node2")) "node3" else "node2";
    try testing.expectEqual(ReplicaState.BACKUP, registry.getReplicaState(backup_node));
}

test "ViewChangeProtocol multiple view changes" {
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

    // Create a registry
    var registry = try ReplicaRegistry.init(allocator);
    defer registry.deinit();

    // Register some nodes
    try registry.registerReplica("node1", .PRIMARY);
    try registry.registerReplica("node2", .BACKUP);
    try registry.registerReplica("node3", .BACKUP);
    try registry.registerReplica("node4", .BACKUP);
    try registry.registerReplica("node5", .BACKUP);

    // Initialize view change protocol
    var vcp = try ViewChangeProtocol.init(allocator, simulation, registry);
    vcp.heartbeat_interval = 10;
    vcp.heartbeat_timeout = 30;
    defer vcp.deinit();

    // Initial heartbeat
    try vcp.startHeartbeat("node1");
    try simulation.run(20);

    // First view change: node1 fails
    vcp.stopHeartbeat("node1");
    registry.removeReplica("node1");
    try vcp.initiateViewChange("node2");
    try simulation.run(100);

    // Check view 2
    try testing.expectEqual(@as(u64, 2), vcp.current_view);
    const primary1 = registry.getPrimaryNode();
    try testing.expectEqual(ReplicaState.PRIMARY, registry.getReplicaState(primary1));

    // Restart heartbeat with new primary
    try vcp.startHeartbeat(primary1);
    try simulation.run(20);

    // Second view change: new primary fails
    vcp.stopHeartbeat(primary1);
    registry.removeReplica(primary1);

    // Find a backup to initiate the second view change
    const backups = registry.getReplicasByState(.BACKUP);
    try testing.expect(backups.len > 0);
    try vcp.initiateViewChange(backups[0]);
    try simulation.run(100);

    // Check view 3
    try testing.expectEqual(@as(u64, 3), vcp.current_view);
    const primary2 = registry.getPrimaryNode();
    try testing.expect(!std.mem.eql(u8, primary1, primary2));
    try testing.expectEqual(ReplicaState.PRIMARY, registry.getReplicaState(primary2));
}
