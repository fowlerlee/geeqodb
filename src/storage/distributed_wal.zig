const std = @import("std");
const assert = @import("../build_options.zig").assert;
const WAL = @import("wal.zig").WAL;
const Simulation = @import("../simulation/simulation.zig").Simulation;
const replica_management = @import("../simulation/scenarios/replica_management.zig");
const ReplicaRegistry = replica_management.ReplicaRegistry;
const ReplicaState = replica_management.ReplicaState;

/// Message types for distributed WAL communication
pub const DistributedMessageType = enum {
    PREPARE,
    PREPARE_OK,
    COMMIT,
    FORWARD,
};

/// PrepareOK acknowledgment message
pub const PrepareOK = struct {
    op_number: u64,
    sender: []const u8,
};

/// Message for distributed WAL communication
pub const DistributedWALMessage = struct {
    type: DistributedMessageType,
    op_number: u64,
    commit_point: u64,
    data: ?[]const u8,
    sender: []const u8,
};

/// Distributed Write-Ahead Log for replicated operations
pub const DistributedWAL = struct {
    allocator: std.mem.Allocator,
    simulation: *Simulation,
    node_id: []const u8,
    registry: *ReplicaRegistry,
    wal: *WAL,
    last_prepared_op: u64 = 0,
    commit_point: u64 = 0,

    // Track responses for replication
    prepare_responses: std.AutoHashMap(u64, std.StringHashMap(void)),
    received_operations: std.AutoHashMap(u64, []const u8),

    /// Initialize a distributed WAL
    pub fn init(allocator: std.mem.Allocator, simulation: *Simulation, node_id: []const u8, registry: *ReplicaRegistry, data_dir: []const u8) !*DistributedWAL {
        const dwal = try allocator.create(DistributedWAL);

        // Initialize the underlying WAL
        const wal = try WAL.init(allocator, data_dir);

        dwal.* = DistributedWAL{
            .allocator = allocator,
            .simulation = simulation,
            .node_id = try allocator.dupe(u8, node_id),
            .registry = registry,
            .wal = wal,
            .prepare_responses = std.AutoHashMap(u64, std.StringHashMap(void)).init(allocator),
            .received_operations = std.AutoHashMap(u64, []const u8).init(allocator),
        };

        return dwal;
    }

    /// Clean up resources
    pub fn deinit(self: *DistributedWAL) void {
        // Free prepare responses
        var it = self.prepare_responses.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.prepare_responses.deinit();

        // Free received operations
        var op_it = self.received_operations.iterator();
        while (op_it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.received_operations.deinit();

        // Clean up the node ID
        self.allocator.free(self.node_id);

        // Deinitialize underlying WAL
        self.wal.deinit();

        // Free self
        self.allocator.destroy(self);
    }

    /// Log a transaction to the distributed WAL
    pub fn logTransaction(self: *DistributedWAL, txn_id: u64, data: []const u8) !void {
        // Check if this node is the primary
        const node_state = self.registry.getReplicaState(self.node_id) catch .BACKUP;
        if (node_state != .PRIMARY) {
            // Not primary, forward operation to primary if possible
            try self.forwardOperation(txn_id, data);
            return error.NotPrimary;
        }

        // Clone the data before using it
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);

        // Store in local WAL
        try self.wal.logTransaction(txn_id, data_copy);

        // Record in received operations
        try self.received_operations.put(txn_id, data_copy);

        // Update last prepared op
        self.last_prepared_op = txn_id;

        // Prepare for replication
        try self.prepareOperation(txn_id, data_copy);

        return;
    }

    /// Forward an operation to the primary
    fn forwardOperation(self: *DistributedWAL, txn_id: u64, data: []const u8) !void {
        const primary_node = self.registry.getPrimaryNode();
        if (primary_node.len == 0) return;

        // Create a forward message
        const forward_msg = DistributedWALMessage{
            .type = .FORWARD,
            .op_number = txn_id,
            .commit_point = self.commit_point,
            .data = data,
            .sender = self.node_id,
        };

        // Serialize and send to primary
        var buf: [4096]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(forward_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        _ = try self.simulation.sendMessage(self.node_id, primary_node, msg_slice);
    }

    /// Prepare an operation for replication to backups
    fn prepareOperation(self: *DistributedWAL, op_number: u64, data: []const u8) !void {
        // Get all backup nodes
        const backups = self.registry.getReplicasByState(.BACKUP);
        defer self.allocator.free(backups);

        if (backups.len == 0) {
            // No backups, we can commit immediately
            try self.commitOperation(op_number);
            return;
        }

        // Initialize responses tracking
        var responses = std.StringHashMap(void).init(self.allocator);
        responses = responses; // Suppress unused variable warning
        try self.prepare_responses.put(op_number, responses);

        // Create a prepare message
        const prepare_msg = DistributedWALMessage{
            .type = .PREPARE,
            .op_number = op_number,
            .commit_point = self.commit_point,
            .data = data,
            .sender = self.node_id,
        };

        // Serialize and send to backups
        var buf: [4096]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(prepare_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        for (backups) |backup| {
            _ = self.simulation.sendMessage(self.node_id, backup, msg_slice) catch continue;
        }
    }

    /// Handle a PREPARE message
    fn handlePrepareMessage(self: *DistributedWAL, msg: *const DistributedWALMessage) !void {
        // Check that the sender is the primary
        const primary_node = self.registry.getPrimaryNode();
        if (!std.mem.eql(u8, primary_node, msg.sender)) {
            return error.SenderNotPrimary;
        }

        // Apply the operation locally
        if (msg.data) |data| {
            const data_copy = try self.allocator.dupe(u8, data);
            errdefer self.allocator.free(data_copy);

            // Log to local WAL
            try self.wal.logTransaction(msg.op_number, data_copy);

            // Record in received operations
            try self.received_operations.put(msg.op_number, data_copy);

            // Update last prepared op
            if (msg.op_number > self.last_prepared_op) {
                self.last_prepared_op = msg.op_number;
            }

            // Update commit point if primary is ahead
            if (msg.commit_point > self.commit_point) {
                self.commit_point = msg.commit_point;
            }

            // Send PREPARE_OK
            try self.sendPrepareOK(msg.op_number, primary_node);
        }
    }

    /// Send a PREPARE_OK acknowledgment
    fn sendPrepareOK(self: *DistributedWAL, op_number: u64, to: []const u8) !void {
        const prepare_ok_msg = DistributedWALMessage{
            .type = .PREPARE_OK,
            .op_number = op_number,
            .commit_point = self.commit_point,
            .data = null,
            .sender = self.node_id,
        };

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(prepare_ok_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        _ = try self.simulation.sendMessage(self.node_id, to, msg_slice);
    }

    /// Handle a PREPARE_OK message
    fn handlePrepareOKMessage(self: *DistributedWAL, msg: *const DistributedWALMessage) !void {
        // Must be primary to handle PREPARE_OK
        const node_state = self.registry.getReplicaState(self.node_id) catch .BACKUP;
        if (node_state != .PRIMARY) {
            return error.NotPrimary;
        }

        // Record the response
        if (self.prepare_responses.getPtr(msg.op_number)) |responses| {
            const sender_copy = try self.allocator.dupe(u8, msg.sender);
            try responses.put(sender_copy, {});

            // Check if we have enough responses to commit
            const backups = self.registry.getReplicasByState(.BACKUP);
            defer self.allocator.free(backups);

            const quorum_size = (backups.len / 2) + 1;
            if (responses.count() >= quorum_size) {
                try self.commitOperation(msg.op_number);
            }
        }
    }

    /// Commit an operation after receiving enough acknowledgments
    fn commitOperation(self: *DistributedWAL, op_number: u64) !void {
        // Update commit point
        if (op_number > self.commit_point) {
            self.commit_point = op_number;
        }

        // Send commit message to backups
        const backups = self.registry.getReplicasByState(.BACKUP);
        defer self.allocator.free(backups);

        const commit_msg = DistributedWALMessage{
            .type = .COMMIT,
            .op_number = op_number,
            .commit_point = self.commit_point,
            .data = null,
            .sender = self.node_id,
        };

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(commit_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        for (backups) |backup| {
            _ = self.simulation.sendMessage(self.node_id, backup, msg_slice) catch continue;
        }

        // Clean up responses for this operation
        if (self.prepare_responses.fetchRemove(op_number)) |entry| {
            var it = entry.value.keyIterator();
            while (it.next()) |key| {
                self.allocator.free(key.*);
            }
            // Make a mutable copy to be able to call deinit
            var value_copy = entry.value;
            value_copy.deinit();
        }
    }

    /// Handle a COMMIT message
    fn handleCommitMessage(self: *DistributedWAL, msg: *const DistributedWALMessage) !void {
        // Update commit point
        if (msg.commit_point > self.commit_point) {
            self.commit_point = msg.commit_point;
        }
    }

    /// Handle a FORWARD message
    fn handleForwardMessage(self: *DistributedWAL, msg: *const DistributedWALMessage) !void {
        // Only the primary should handle forwarded operations
        const node_state = self.registry.getReplicaState(self.node_id) catch .BACKUP;
        if (node_state != .PRIMARY) {
            return error.NotPrimary;
        }

        // Process the forwarded operation
        if (msg.data) |data| {
            try self.logTransaction(msg.op_number, data);
        }
    }

    /// Handle an incoming message
    pub fn handleMessage(self: *DistributedWAL, from: []const u8, message: []const u8) !void {
        var parsed = try std.json.parseFromSlice(DistributedWALMessage, self.allocator, message, .{});
        defer parsed.deinit();

        const msg = parsed.value;

        switch (msg.type) {
            .PREPARE => try self.handlePrepareMessage(&msg),
            .PREPARE_OK => try self.handlePrepareOKMessage(&msg),
            .COMMIT => try self.handleCommitMessage(&msg),
            .FORWARD => try self.handleForwardMessage(&msg),
        }

        // We're not using 'from' directly, but it's used to identify the sender
        // for debugging or logging purposes (which we're not implementing here)
        _ = from;
    }

    /// Check if an operation has been received
    pub fn hasReceivedOperation(self: *DistributedWAL, op_number: u64) bool {
        return self.received_operations.contains(op_number);
    }

    /// Called when this node becomes the primary after a view change
    pub fn becomePrimary(self: *DistributedWAL) !void {
        // Clear prepare responses from previous view
        var it = self.prepare_responses.iterator();
        while (it.next()) |entry| {
            var response_it = entry.value_ptr.keyIterator();
            while (response_it.next()) |key| {
                self.allocator.free(key.*);
            }
            entry.value_ptr.clearAndFree();
        }
        self.prepare_responses.clearRetainingCapacity();
    }

    /// Update view after a view change
    pub fn updateView(self: *DistributedWAL) !void {
        // Get the current primary node
        const primary_node = self.registry.getPrimaryNode();

        // If we're the primary, nothing to do
        if (std.mem.eql(u8, self.node_id, primary_node)) {
            return;
        }

        // As a backup, we need to synchronize with the primary
        const sync_msg = DistributedWALMessage{
            .type = .PREPARE,
            .op_number = self.last_prepared_op,
            .commit_point = self.commit_point,
            .data = null,
            .sender = self.node_id,
        };

        // Serialize and send to primary
        var buf: [4096]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(sync_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        _ = try self.simulation.sendMessage(self.node_id, primary_node, msg_slice);

        // Reset any pending state that might be inconsistent after view change
        var it = self.prepare_responses.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* > self.commit_point) {
                var response_it = entry.value_ptr.keyIterator();
                while (response_it.next()) |key| {
                    self.allocator.free(key.*);
                }
                entry.value_ptr.clearAndFree();
            }
        }
    }
};
