const std = @import("std");
const testing = std.testing;
const Simulation = @import("simulation").Simulation;

// Import the necessary modules
const distributed_wal = @import("distributed_wal");
const DistributedWAL = distributed_wal.DistributedWAL;
const PrepareOK = distributed_wal.PrepareOK;
const replica_management = @import("replica_management");
const ReplicaState = replica_management.ReplicaState;
const ReplicaRegistry = replica_management.ReplicaRegistry;

test "DistributedWAL initialization" {
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

    // Initialize the distributed WAL
    var dwal = try DistributedWAL.init(allocator, simulation, "node1", registry, "test_data");
    defer dwal.deinit();

    // Check initial state
    try testing.expectEqual(@as(u64, 0), dwal.commit_point);
    try testing.expectEqual(@as(u64, 0), dwal.last_prepared_op);
}

test "DistributedWAL log replication" {
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

    // Initialize the distributed WALs for each node
    var primary_wal = try DistributedWAL.init(allocator, simulation, "node1", registry, "test_data_primary");
    defer primary_wal.deinit();

    var backup1_wal = try DistributedWAL.init(allocator, simulation, "node2", registry, "test_data_backup1");
    defer backup1_wal.deinit();

    var backup2_wal = try DistributedWAL.init(allocator, simulation, "node3", registry, "test_data_backup2");
    defer backup2_wal.deinit();

    // Register message handlers
    try simulation.registerNode("node1", messageHandlerNoReturn, primary_wal);
    try simulation.registerNode("node2", messageHandlerNoReturn, backup1_wal);
    try simulation.registerNode("node3", messageHandlerNoReturn, backup2_wal);

    // Log a transaction on the primary
    const txn_data = "test transaction 1";
    try primary_wal.logTransaction(1, txn_data);

    // Run simulation to allow replication
    try simulation.run(100);

    // Check that both backups have received the transaction
    try testing.expect(backup1_wal.hasReceivedOperation(1));
    try testing.expect(backup2_wal.hasReceivedOperation(1));

    // Check commit point advancement
    try testing.expectEqual(@as(u64, 1), primary_wal.commit_point);
    try testing.expectEqual(@as(u64, 1), backup1_wal.commit_point);
    try testing.expectEqual(@as(u64, 1), backup2_wal.commit_point);

    // Log multiple transactions
    try primary_wal.logTransaction(2, "test transaction 2");
    try primary_wal.logTransaction(3, "test transaction 3");

    // Run simulation to allow replication
    try simulation.run(100);

    // Check all operations were replicated and committed
    try testing.expectEqual(@as(u64, 3), primary_wal.commit_point);
    try testing.expectEqual(@as(u64, 3), backup1_wal.commit_point);
    try testing.expectEqual(@as(u64, 3), backup2_wal.commit_point);
}

test "DistributedWAL operation forwarding" {
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

    // Initialize the distributed WALs
    var primary_wal = try DistributedWAL.init(allocator, simulation, "node1", registry, "test_data_primary2");
    defer primary_wal.deinit();

    var backup_wal = try DistributedWAL.init(allocator, simulation, "node2", registry, "test_data_backup2");
    defer backup_wal.deinit();

    // Register message handlers
    try simulation.registerNode("node1", messageHandlerNoReturn, primary_wal);
    try simulation.registerNode("node2", messageHandlerNoReturn, backup_wal);

    // Try to log a transaction on the backup (should forward to primary)
    const result = backup_wal.logTransaction(1, "transaction from backup");

    // Should receive NotPrimary error
    try testing.expectError(error.NotPrimary, result);

    // Run simulation to allow forwarding
    try simulation.run(50);

    // Check primary received the forwarded operation
    try testing.expect(primary_wal.hasReceivedOperation(1));

    // Check that the backup eventually received the replicated operation
    try simulation.run(50);
    try testing.expect(backup_wal.hasReceivedOperation(1));
}

// Message handler function for distributed WAL communication
fn distributeWALMessageHandler(simulation: *Simulation, target: []const u8, message: []const u8, userdata: *anyopaque) !void {
    const dwal = @as(*DistributedWAL, @alignCast(@ptrCast(userdata)));

    // Simulation parameter is required by the handler interface
    // but not directly used in our implementation
    _ = simulation;

    try dwal.handleMessage(target, message);
}

// Version without return type for simulation registration
fn messageHandlerNoReturn(target: []const u8, message: []const u8, userdata: ?*anyopaque) void {
    if (userdata) |data| {
        const dwal = @as(*DistributedWAL, @alignCast(@ptrCast(data)));
        dwal.handleMessage(target, message) catch {};
    }
}

test "DistributedWAL view change support" {
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

    // Initialize the distributed WALs
    var primary_wal = try DistributedWAL.init(allocator, simulation, "node1", registry, "test_data_vc_primary");
    defer primary_wal.deinit();

    var backup1_wal = try DistributedWAL.init(allocator, simulation, "node2", registry, "test_data_vc_backup1");
    defer backup1_wal.deinit();

    var backup2_wal = try DistributedWAL.init(allocator, simulation, "node3", registry, "test_data_vc_backup2");
    defer backup2_wal.deinit();

    // Register message handlers
    try simulation.registerNode("node1", messageHandlerNoReturn, primary_wal);
    try simulation.registerNode("node2", messageHandlerNoReturn, backup1_wal);
    try simulation.registerNode("node3", messageHandlerNoReturn, backup2_wal);

    // Log some transactions
    try primary_wal.logTransaction(1, "txn1");
    try primary_wal.logTransaction(2, "txn2");
    try primary_wal.logTransaction(3, "txn3");

    // Run simulation to allow replication
    try simulation.run(100);

    // Simulate primary failure and view change
    registry.removeReplica("node1");
    try registry.changeReplicaState("node2", .PRIMARY);
    try registry.changeReplicaState("node3", .BACKUP);

    // Update WALs to reflect the new view
    try backup1_wal.becomePrimary();
    try backup2_wal.updateView();

    // Log a new transaction on the new primary
    try backup1_wal.logTransaction(4, "txn4");

    // Run simulation to allow replication
    try simulation.run(100);

    // Check that the operation was replicated to the backup
    try testing.expect(backup2_wal.hasReceivedOperation(4));
    try testing.expectEqual(@as(u64, 4), backup1_wal.commit_point);
    try testing.expectEqual(@as(u64, 4), backup2_wal.commit_point);
}
