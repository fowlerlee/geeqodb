const std = @import("std");
const testing = std.testing;

// Import the module
const replica_management = @import("replica_management");
const ReplicaState = replica_management.ReplicaState;
const ReplicaRegistry = replica_management.ReplicaRegistry;

test "ReplicaState transitions" {
    // Test valid state transitions
    try testing.expect(replica_management.isValidStateTransition(.PRIMARY, .VIEW_CHANGE));
    try testing.expect(replica_management.isValidStateTransition(.BACKUP, .VIEW_CHANGE));
    try testing.expect(replica_management.isValidStateTransition(.BACKUP, .PRIMARY));
    try testing.expect(replica_management.isValidStateTransition(.VIEW_CHANGE, .PRIMARY));
    try testing.expect(replica_management.isValidStateTransition(.VIEW_CHANGE, .BACKUP));
    try testing.expect(replica_management.isValidStateTransition(.RECOVERING, .BACKUP));
    try testing.expect(replica_management.isValidStateTransition(.RECOVERING, .PRIMARY));

    // Test invalid state transitions
    try testing.expect(!replica_management.isValidStateTransition(.PRIMARY, .PRIMARY));
    try testing.expect(!replica_management.isValidStateTransition(.PRIMARY, .BACKUP));
    try testing.expect(!replica_management.isValidStateTransition(.PRIMARY, .RECOVERING));
    try testing.expect(!replica_management.isValidStateTransition(.BACKUP, .BACKUP));
    try testing.expect(!replica_management.isValidStateTransition(.BACKUP, .RECOVERING));
    try testing.expect(!replica_management.isValidStateTransition(.VIEW_CHANGE, .VIEW_CHANGE));
    try testing.expect(!replica_management.isValidStateTransition(.VIEW_CHANGE, .RECOVERING));
    try testing.expect(!replica_management.isValidStateTransition(.RECOVERING, .RECOVERING));
    try testing.expect(!replica_management.isValidStateTransition(.RECOVERING, .VIEW_CHANGE));
}

test "ReplicaRegistry basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    var registry = try ReplicaRegistry.init(allocator);
    defer registry.deinit();

    // Register some replicas
    try registry.registerReplica("node1", .PRIMARY);
    try registry.registerReplica("node2", .BACKUP);
    try registry.registerReplica("node3", .BACKUP);

    // Check registration worked
    try testing.expectEqual(ReplicaState.PRIMARY, registry.getReplicaState("node1"));
    try testing.expectEqual(ReplicaState.BACKUP, registry.getReplicaState("node2"));
    try testing.expectEqual(ReplicaState.BACKUP, registry.getReplicaState("node3"));

    // Try to register a duplicate primary (should fail)
    try testing.expectError(error.PrimaryAlreadyExists, registry.registerReplica("node4", .PRIMARY));

    // Get the primary
    try testing.expectEqualStrings("node1", registry.getPrimaryNode());

    // Test state transitions
    try registry.changeReplicaState("node2", .VIEW_CHANGE);
    try testing.expectEqual(ReplicaState.VIEW_CHANGE, registry.getReplicaState("node2"));

    // Invalid state transition
    try testing.expectError(error.InvalidStateTransition, registry.changeReplicaState("node1", .BACKUP));

    // Remove a replica
    registry.removeReplica("node3");
    try testing.expectError(error.ReplicaNotFound, registry.getReplicaState("node3"));

    // Get all replicas
    const replicas = registry.getAllReplicas();
    defer allocator.free(replicas);
    try testing.expectEqual(@as(usize, 2), replicas.len);

    // Check we can find nodes by state
    const backup_nodes = registry.getReplicasByState(.BACKUP);
    defer allocator.free(backup_nodes);
    try testing.expectEqual(@as(usize, 0), backup_nodes.len);

    const view_change_nodes = registry.getReplicasByState(.VIEW_CHANGE);
    defer allocator.free(view_change_nodes);
    try testing.expectEqual(@as(usize, 1), view_change_nodes.len);
    try testing.expectEqualStrings("node2", view_change_nodes[0]);
}

test "ReplicaRegistry view change" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    var registry = try ReplicaRegistry.init(allocator);
    defer registry.deinit();

    // Setup a cluster with primary and backups
    try registry.registerReplica("node1", .PRIMARY);
    try registry.registerReplica("node2", .BACKUP);
    try registry.registerReplica("node3", .BACKUP);

    // Simulate view change - primary becomes unavailable
    registry.removeReplica("node1");

    // Backup nodes transition to view change
    try registry.changeReplicaState("node2", .VIEW_CHANGE);
    try registry.changeReplicaState("node3", .VIEW_CHANGE);

    // Elect a new primary
    try registry.changeReplicaState("node2", .PRIMARY);
    try registry.changeReplicaState("node3", .BACKUP);

    // Verify new state
    try testing.expectEqual(ReplicaState.PRIMARY, registry.getReplicaState("node2"));
    try testing.expectEqual(ReplicaState.BACKUP, registry.getReplicaState("node3"));
    try testing.expectEqualStrings("node2", registry.getPrimaryNode());
}
