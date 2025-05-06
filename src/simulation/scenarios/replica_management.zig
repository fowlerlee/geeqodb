const std = @import("std");

/// Replica state for distributed system
pub const ReplicaState = enum {
    PRIMARY, // Node is the primary replica, handles writes
    BACKUP, // Node is a backup replica, receives replicated operations
    RECOVERING, // Node is in recovery mode after failure/restart
    VIEW_CHANGE, // Node is participating in a view change
};

/// Check if a state transition is valid
pub fn isValidStateTransition(from: ReplicaState, to: ReplicaState) bool {
    return switch (from) {
        .PRIMARY => switch (to) {
            .VIEW_CHANGE => true,
            else => false,
        },
        .BACKUP => switch (to) {
            .VIEW_CHANGE, .PRIMARY => true,
            else => false,
        },
        .RECOVERING => switch (to) {
            .BACKUP, .PRIMARY => true,
            else => false,
        },
        .VIEW_CHANGE => switch (to) {
            .PRIMARY, .BACKUP => true,
            else => false,
        },
    };
}

/// Error set for replica operations
pub const ReplicaError = error{
    PrimaryAlreadyExists,
    ReplicaNotFound,
    InvalidStateTransition,
};

/// Registry for tracking replica nodes and their states
pub const ReplicaRegistry = struct {
    allocator: std.mem.Allocator,
    replicas: std.StringHashMap(ReplicaState),
    primary_node: ?[]const u8,

    /// Initialize a new replica registry
    pub fn init(allocator: std.mem.Allocator) !*ReplicaRegistry {
        const registry = try allocator.create(ReplicaRegistry);
        registry.* = ReplicaRegistry{
            .allocator = allocator,
            .replicas = std.StringHashMap(ReplicaState).init(allocator),
            .primary_node = null,
        };
        return registry;
    }

    /// Clean up resources
    pub fn deinit(self: *ReplicaRegistry) void {
        var it = self.replicas.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.replicas.deinit();
        if (self.primary_node) |node| {
            self.allocator.free(node);
        }
        self.allocator.destroy(self);
    }

    /// Register a new replica with the given state
    pub fn registerReplica(self: *ReplicaRegistry, node_id: []const u8, state: ReplicaState) !void {
        // Check if this would create a duplicate primary
        if (state == .PRIMARY) {
            if (self.primary_node != null) {
                return ReplicaError.PrimaryAlreadyExists;
            }
            // Store primary node ID
            self.primary_node = try self.allocator.dupe(u8, node_id);
        }

        // Store node ID and state
        const node_id_copy = try self.allocator.dupe(u8, node_id);
        errdefer self.allocator.free(node_id_copy);

        try self.replicas.put(node_id_copy, state);
    }

    /// Get state of a replica
    pub fn getReplicaState(self: *ReplicaRegistry, node_id: []const u8) !ReplicaState {
        const state = self.replicas.get(node_id) orelse return ReplicaError.ReplicaNotFound;
        return state;
    }

    /// Change state of a replica
    pub fn changeReplicaState(self: *ReplicaRegistry, node_id: []const u8, new_state: ReplicaState) !void {
        const current_state = try self.getReplicaState(node_id);

        // Check if transition is valid
        if (!isValidStateTransition(current_state, new_state)) {
            return ReplicaError.InvalidStateTransition;
        }

        // Update primary_node reference if needed
        if (new_state == .PRIMARY) {
            if (self.primary_node) |prev_primary| {
                self.allocator.free(prev_primary);
            }
            self.primary_node = try self.allocator.dupe(u8, node_id);
        } else if (current_state == .PRIMARY) {
            // If transitioning from primary to non-primary, clear the primary reference
            if (self.primary_node) |prev_primary| {
                if (std.mem.eql(u8, prev_primary, node_id)) {
                    self.allocator.free(prev_primary);
                    self.primary_node = null;
                }
            }
        }

        // Update state
        try self.replicas.put(node_id, new_state);
    }

    /// Remove a replica from the registry
    pub fn removeReplica(self: *ReplicaRegistry, node_id: []const u8) void {
        // Check if removing the primary
        if (self.primary_node) |primary| {
            if (std.mem.eql(u8, primary, node_id)) {
                self.allocator.free(primary);
                self.primary_node = null;
            }
        }

        // Remove from registry and free key
        if (self.replicas.fetchRemove(node_id)) |entry| {
            self.allocator.free(entry.key);
        }
    }

    /// Get the ID of the primary node
    pub fn getPrimaryNode(self: *ReplicaRegistry) []const u8 {
        return self.primary_node orelse "";
    }

    /// Get all replicas - caller must free the returned slice
    pub fn getAllReplicas(self: *ReplicaRegistry) [][]const u8 {
        var result = std.ArrayList([]const u8).init(self.allocator);
        defer result.deinit();

        var it = self.replicas.keyIterator();
        while (it.next()) |key| {
            result.append(key.*) catch continue;
        }

        // Return an empty slice in case of error
        return result.toOwnedSlice() catch &[_][]const u8{};
    }

    /// Get replica nodes with a specific state - caller must free the returned slice
    pub fn getReplicasByState(self: *ReplicaRegistry, state: ReplicaState) [][]const u8 {
        var result = std.ArrayList([]const u8).init(self.allocator);
        defer result.deinit();

        var it = self.replicas.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* == state) {
                result.append(entry.key_ptr.*) catch continue;
            }
        }

        // Return an empty slice in case of error
        return result.toOwnedSlice() catch &[_][]const u8{};
    }
};
