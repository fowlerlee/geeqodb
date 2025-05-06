const std = @import("std");
const Simulation = @import("simulation").Simulation;
const replica_management = @import("replica_management");
const ReplicaRegistry = replica_management.ReplicaRegistry;
const ReplicaState = replica_management.ReplicaState;

/// Message types for view change protocol
pub const ViewChangeMessageType = enum {
    HEARTBEAT,
    VIEW_CHANGE_REQUEST,
    VIEW_CHANGE_RESPONSE,
    NEW_VIEW,
};

/// View change protocol message
pub const ViewChangeMessage = struct {
    type: ViewChangeMessageType,
    view_number: u64,
    sender: []const u8,
    timestamp: u64,
};

/// Implementation of the view change protocol
pub const ViewChangeProtocol = struct {
    allocator: std.mem.Allocator,
    simulation: *Simulation,
    registry: *ReplicaRegistry,
    current_view: u64,

    // Heartbeat related
    heartbeat_interval: u64 = 100, // Time between heartbeats in simulation time units
    heartbeat_timeout: u64 = 300, // Timeout for considering primary as failed
    last_heartbeats: std.StringHashMap(u64), // Last heartbeat time for each node
    heartbeat_tasks: std.StringHashMap(u64), // Task IDs for heartbeat schedules

    // View change related
    view_change_in_progress: bool = false,
    view_change_requests: std.StringHashMap(void), // Nodes that have requested view change
    view_change_responses: std.StringHashMap(void), // Nodes that have responded to view change
    view_change_start_time: u64 = 0,
    view_change_timeout: u64 = 500, // Timeout for view change process

    /// Initialize the view change protocol
    pub fn init(allocator: std.mem.Allocator, simulation: *Simulation, registry: *ReplicaRegistry) !*ViewChangeProtocol {
        const vcp = try allocator.create(ViewChangeProtocol);
        vcp.* = ViewChangeProtocol{
            .allocator = allocator,
            .simulation = simulation,
            .registry = registry,
            .current_view = 1, // Start with view 1
            .last_heartbeats = std.StringHashMap(u64).init(allocator),
            .heartbeat_tasks = std.StringHashMap(u64).init(allocator),
            .view_change_requests = std.StringHashMap(void).init(allocator),
            .view_change_responses = std.StringHashMap(void).init(allocator),
        };
        return vcp;
    }

    /// Clean up resources
    pub fn deinit(self: *ViewChangeProtocol) void {
        // Cancel any pending tasks
        var task_it = self.heartbeat_tasks.valueIterator();
        while (task_it.next()) |task_id| {
            _ = self.simulation.scheduler.cancel(task_id.*);
        }

        // Clear the heartbeat tasks
        var entry_it = self.heartbeat_tasks.keyIterator();
        while (entry_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.heartbeat_tasks.deinit();

        // Clear last heartbeats
        entry_it = self.last_heartbeats.keyIterator();
        while (entry_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.last_heartbeats.deinit();

        // Clear view change requests
        var view_change_it = self.view_change_requests.keyIterator();
        while (view_change_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.view_change_requests.deinit();

        // Clear view change responses
        var response_it = self.view_change_responses.keyIterator();
        while (response_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.view_change_responses.deinit();

        self.allocator.destroy(self);
    }

    /// Start sending heartbeats from a node (typically the primary)
    pub fn startHeartbeat(self: *ViewChangeProtocol, node_id: []const u8) !void {
        // Check if already sending heartbeats from this node
        if (self.heartbeat_tasks.get(node_id) != null) {
            return;
        }

        // Schedule recurring heartbeat task
        const node_id_copy = try self.allocator.dupe(u8, node_id);
        errdefer self.allocator.free(node_id_copy);

        // Wrapper function that matches the scheduler's expected callback signature
        const wrapper = struct {
            fn callback(ctx: ?*anyopaque) void {
                const context = @as(*struct {
                    vcp: *ViewChangeProtocol,
                    sender: []const u8,
                }, @ptrCast(@alignCast(ctx.?)));

                context.vcp.sendHeartbeat(context.sender) catch {};
            }
        }.callback;

        const context = try self.allocator.create(struct {
            vcp: *ViewChangeProtocol,
            sender: []const u8,
        });
        context.* = .{
            .vcp = self,
            .sender = node_id_copy,
        };

        const task_id = try self.simulation.scheduler.scheduleAfter(self.heartbeat_interval, 0, wrapper, context);
        try self.heartbeat_tasks.put(node_id_copy, task_id);
    }

    /// Stop sending heartbeats from a node
    pub fn stopHeartbeat(self: *ViewChangeProtocol, node_id: []const u8) void {
        if (self.heartbeat_tasks.fetchRemove(node_id)) |entry| {
            _ = self.simulation.scheduler.cancel(entry.value);
            self.allocator.free(entry.key);
        }
    }

    /// Send a heartbeat from a node to all other nodes
    fn sendHeartbeat(self: *ViewChangeProtocol, sender: []const u8) !void {
        const peers = self.registry.getAllReplicas();
        defer self.allocator.free(peers);

        const heartbeat_msg = ViewChangeMessage{
            .type = .HEARTBEAT,
            .view_number = self.current_view,
            .sender = sender,
            .timestamp = self.simulation.getCurrentTime(),
        };

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(heartbeat_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        // Send to all peers
        for (peers) |peer| {
            if (!std.mem.eql(u8, peer, sender)) {
                _ = self.simulation.sendMessage(sender, peer, msg_slice) catch continue;
            }
        }
    }

    /// Handle received heartbeat
    pub fn handleHeartbeat(self: *ViewChangeProtocol, from: []const u8, msg: *ViewChangeMessage) !void {
        // Update last heartbeat time for the sending node
        const node_id_copy = try self.allocator.dupe(u8, from);
        try self.last_heartbeats.put(node_id_copy, msg.timestamp);
    }

    /// Get the last heartbeat time for a node
    pub fn lastHeartbeatTime(self: *ViewChangeProtocol, node_id: []const u8) u64 {
        return self.last_heartbeats.get(node_id) orelse 0;
    }

    /// Check if the primary is healthy (based on heartbeats)
    pub fn isPrimaryHealthy(self: *ViewChangeProtocol) bool {
        const primary_node = self.registry.getPrimaryNode();
        if (primary_node.len == 0) return false;

        const last_hb = self.lastHeartbeatTime(primary_node);
        const current_time = self.simulation.getCurrentTime();

        return (current_time - last_hb) < self.heartbeat_timeout;
    }

    /// Initiate a view change request (triggered by a node when it suspects primary failure)
    pub fn initiateViewChange(self: *ViewChangeProtocol, initiator: []const u8) !void {
        if (self.view_change_in_progress) return;

        // Mark view change as in progress
        self.view_change_in_progress = true;
        self.view_change_start_time = self.simulation.getCurrentTime();

        // Update initiator's state
        try self.registry.changeReplicaState(initiator, .VIEW_CHANGE);

        // Add initiator to requests
        const node_id_copy = try self.allocator.dupe(u8, initiator);
        try self.view_change_requests.put(node_id_copy, {});

        // Clear previous view change responses
        var it = self.view_change_responses.keyIterator();
        while (it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.view_change_responses.clearAndFree();

        // Broadcast view change request
        const vc_msg = ViewChangeMessage{
            .type = .VIEW_CHANGE_REQUEST,
            .view_number = self.current_view + 1, // Request for next view
            .sender = initiator,
            .timestamp = self.simulation.getCurrentTime(),
        };

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(vc_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        const peers = self.registry.getAllReplicas();
        defer self.allocator.free(peers);

        for (peers) |peer| {
            // Skip sending to self
            if (std.mem.eql(u8, peer, initiator)) continue;

            _ = self.simulation.sendMessage(initiator, peer, msg_slice) catch continue;
        }

        // Schedule a view change timeout
        // Wrapper function that matches the scheduler's expected callback signature
        const wrapper = struct {
            fn callback(ctx: ?*anyopaque) void {
                const vcp = @as(*ViewChangeProtocol, @ptrCast(@alignCast(ctx.?)));
                vcp.checkViewChangeTimeout() catch {};
            }
        }.callback;

        _ = try self.simulation.scheduler.scheduleAfter(self.view_change_timeout, 0, wrapper, self);
    }

    /// Handle a view change request from another node
    pub fn handleViewChangeRequest(self: *ViewChangeProtocol, node_id: []const u8) !void {
        const state = self.registry.getReplicaState(node_id) catch return;

        // Update node's state to VIEW_CHANGE
        if (state != .VIEW_CHANGE) {
            try self.registry.changeReplicaState(node_id, .VIEW_CHANGE);
        }

        // Add to view change requests
        const node_id_copy = try self.allocator.dupe(u8, node_id);
        try self.view_change_requests.put(node_id_copy, {});

        // Send view change response
        try self.sendViewChangeResponse(node_id);

        // Check if we have enough requests to complete the view change
        try self.checkViewChangeCompletion();
    }

    /// Send a view change response
    fn sendViewChangeResponse(self: *ViewChangeProtocol, to: []const u8) !void {
        // Get the first backup node or another node to act as sender
        const replicas = self.registry.getAllReplicas();
        defer self.allocator.free(replicas);

        if (replicas.len == 0) return;

        // Find a node that isn't the one we're sending to
        var self_id: []const u8 = replicas[0];
        for (replicas) |node| {
            if (!std.mem.eql(u8, node, to)) {
                self_id = node;
                break;
            }
        }

        const vc_msg = ViewChangeMessage{
            .type = .VIEW_CHANGE_RESPONSE,
            .view_number = self.current_view + 1,
            .sender = self_id,
            .timestamp = self.simulation.getCurrentTime(),
        };

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(vc_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        _ = try self.simulation.sendMessage(self_id, to, msg_slice);
    }

    /// Handle a view change response
    pub fn handleViewChangeResponse(self: *ViewChangeProtocol, from: []const u8) !void {
        // Add node to responses
        const node_id_copy = try self.allocator.dupe(u8, from);
        try self.view_change_responses.put(node_id_copy, {});

        // Check if we have enough responses to complete view change
        try self.checkViewChangeCompletion();
    }

    /// Check if view change can be completed
    fn checkViewChangeCompletion(self: *ViewChangeProtocol) !void {
        if (!self.view_change_in_progress) return;

        const total_nodes = self.registry.getAllReplicas().len;
        const quorum_size = (total_nodes / 2) + 1;

        // Check if we have a quorum of nodes participating in view change
        if (self.view_change_requests.count() + self.view_change_responses.count() >= quorum_size) {
            try self.completeViewChange();
        }
    }

    /// Complete the view change process
    fn completeViewChange(self: *ViewChangeProtocol) !void {
        if (!self.view_change_in_progress) return;

        // Increment view number
        self.current_view += 1;

        // Elect a new primary (simple approach: choose the node with lowest ID)
        var candidates = try self.allocator.alloc([]const u8, self.view_change_requests.count());
        defer self.allocator.free(candidates);

        var i: usize = 0;
        var it = self.view_change_requests.keyIterator();
        while (it.next()) |key| {
            if (i < candidates.len) {
                candidates[i] = key.*;
                i += 1;
            }
        }

        if (candidates.len == 0) return; // No candidates, cannot complete view change

        // Find candidate with lowest ID (simple election policy)
        var new_primary = candidates[0];
        for (candidates[1..]) |candidate| {
            if (std.mem.lessThan(u8, candidate, new_primary)) {
                new_primary = candidate;
            }
        }

        // Update new primary's state
        try self.registry.changeReplicaState(new_primary, .PRIMARY);

        // Update other nodes to BACKUP state
        var node_it = self.registry.replicas.keyIterator();
        while (node_it.next()) |node| {
            if (!std.mem.eql(u8, node.*, new_primary)) {
                const state = self.registry.getReplicaState(node.*) catch continue;
                if (state == .VIEW_CHANGE) {
                    try self.registry.changeReplicaState(node.*, .BACKUP);
                }
            }
        }

        // Send NEW_VIEW message to all nodes
        try self.broadcastNewView(new_primary);

        // Start heartbeat from new primary
        try self.startHeartbeat(new_primary);

        // Reset view change state
        self.view_change_in_progress = false;

        // Clear requests and responses
        var clear_it = self.view_change_requests.keyIterator();
        while (clear_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.view_change_requests.clearAndFree();

        clear_it = self.view_change_responses.keyIterator();
        while (clear_it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.view_change_responses.clearAndFree();
    }

    /// Broadcast NEW_VIEW message
    fn broadcastNewView(self: *ViewChangeProtocol, new_primary: []const u8) !void {
        const new_view_msg = ViewChangeMessage{
            .type = .NEW_VIEW,
            .view_number = self.current_view,
            .sender = new_primary,
            .timestamp = self.simulation.getCurrentTime(),
        };

        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        try std.json.stringify(new_view_msg, .{}, fbs.writer());
        const msg_slice = fbs.getWritten();

        const peers = self.registry.getAllReplicas();
        defer self.allocator.free(peers);

        for (peers) |peer| {
            if (!std.mem.eql(u8, peer, new_primary)) {
                _ = self.simulation.sendMessage(new_primary, peer, msg_slice) catch continue;
            }
        }
    }

    /// Handle a NEW_VIEW message
    pub fn handleNewView(self: *ViewChangeProtocol, msg: *ViewChangeMessage) !void {
        // Update view number
        self.current_view = msg.view_number;

        // Update local state based on message
        // Get the node ID from the first node in the registry that isn't the sender
        const replicas = self.registry.getAllReplicas();
        defer self.allocator.free(replicas);

        // Default to first node in case we can't determine
        var self_id: []const u8 = if (replicas.len > 0) replicas[0] else msg.sender;

        // Find a node that isn't the sender
        for (replicas) |node| {
            if (!std.mem.eql(u8, node, msg.sender)) {
                self_id = node;
                break;
            }
        }

        const sender_is_primary = std.mem.eql(u8, msg.sender, self.registry.getPrimaryNode());

        if (sender_is_primary) {
            // Sender is already registered as primary
            if (self.registry.getReplicaState(self_id) catch .BACKUP == .VIEW_CHANGE) {
                try self.registry.changeReplicaState(self_id, .BACKUP);
            }
        } else {
            // Sender claims to be primary but isn't registered as such
            // This could happen during network partitions or other edge cases
            // For simplicity, we'll accept the claim and update our registry

            // Update sender to PRIMARY
            try self.registry.changeReplicaState(msg.sender, .PRIMARY);

            // If we thought we were primary, change to BACKUP
            if (self.registry.getReplicaState(self_id) catch .BACKUP == .PRIMARY) {
                try self.registry.changeReplicaState(self_id, .BACKUP);
            } else if (self.registry.getReplicaState(self_id) catch .BACKUP == .VIEW_CHANGE) {
                try self.registry.changeReplicaState(self_id, .BACKUP);
            }
        }

        // Reset view change state
        self.view_change_in_progress = false;
    }

    /// Check if view change has timed out
    fn checkViewChangeTimeout(self: *ViewChangeProtocol) !void {
        if (!self.view_change_in_progress) return;

        const current_time = self.simulation.getCurrentTime();
        if (current_time - self.view_change_start_time >= self.view_change_timeout) {
            // View change timed out, force completion with available nodes
            try self.completeViewChange();
        }
    }
};
