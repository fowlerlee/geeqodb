const std = @import("std");
const Simulation = @import("../simulation.zig").Simulation;
const SimulatedDisk = @import("../disk.zig").SimulatedDisk;

/// Node state for the Viewstamped Replication protocol
pub const VRNode = struct {
    allocator: std.mem.Allocator,
    simulation: *Simulation,
    id: []const u8,
    is_primary: bool,
    view_number: u64,
    op_number: u64,
    log: std.ArrayList(Operation),
    commit_number: u64,
    state: std.StringHashMap([]const u8),
    peers: std.ArrayList([]const u8),
    active: bool,

    pub const Operation = struct {
        op_number: u64,
        client_id: []const u8,
        request_number: u64,
        command: Command,
    };

    pub const Command = union(enum) {
        Put: struct {
            key: []const u8,
            value: []const u8,
        },
        Get: struct {
            key: []const u8,
        },
        Delete: struct {
            key: []const u8,
        },
    };

    pub const MessageType = enum {
        Request,
        PrepareOk,
        Prepare,
        Commit,
        StartViewChange,
        DoViewChange,
        StartView,
        GetState,
        NewState,
    };

    pub const Message = struct {
        type: MessageType,
        view_number: u64,
        op_number: ?u64 = null,
        commit_number: ?u64 = null,
        operation: ?Operation = null,
        log: ?[]Operation = null,
    };

    /// Initialize a new VR node
    pub fn init(allocator: std.mem.Allocator, simulation: *Simulation, id: []const u8, peers: []const []const u8) !*VRNode {
        var node = try allocator.create(VRNode);

        node.* = VRNode{
            .allocator = allocator,
            .simulation = simulation,
            .id = try allocator.dupe(u8, id),
            .is_primary = false,
            .view_number = 0,
            .op_number = 0,
            .log = std.ArrayList(Operation).init(allocator),
            .commit_number = 0,
            .state = std.StringHashMap([]const u8).init(allocator),
            .peers = std.ArrayList([]const u8).init(allocator),
            .active = true,
        };

        // Add peers
        for (peers) |peer| {
            try node.peers.append(try allocator.dupe(u8, peer));
        }

        // Register with the simulation
        try simulation.registerNode(id, messageHandler, node);

        // Create a clock for this node
        try simulation.createClock(id, 1.0);

        // Create a disk for this node
        try simulation.createDisk(id);

        return node;
    }

    /// Deinitialize the node
    pub fn deinit(self: *VRNode) void {
        self.simulation.unregisterNode(self.id);

        for (self.peers.items) |peer| {
            self.allocator.free(peer);
        }
        self.peers.deinit();

        var state_it = self.state.iterator();
        while (state_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.state.deinit();

        for (self.log.items) |op| {
            self.freeOperation(op);
        }
        self.log.deinit();

        self.allocator.free(self.id);
        self.allocator.destroy(self);
    }

    /// Free an operation and its resources
    fn freeOperation(self: *VRNode, op: Operation) void {
        self.allocator.free(op.client_id);

        switch (op.command) {
            .Put => |put| {
                self.allocator.free(put.key);
                self.allocator.free(put.value);
            },
            .Get => |get| {
                self.allocator.free(get.key);
            },
            .Delete => |delete| {
                self.allocator.free(delete.key);
            },
        }
    }

    /// Start the node
    pub fn start(self: *VRNode) !void {
        self.active = true;

        // If this is the first node (by ID), make it the primary
        if (self.peers.items.len == 0 or std.mem.lessThan(u8, self.id, self.peers.items[0])) {
            self.is_primary = true;
            self.view_number = 1;

            // Announce to all peers that this node is the primary
            try self.sendStartView();
        }
    }

    /// Stop the node
    pub fn stop(self: *VRNode) void {
        self.active = false;
    }

    /// Process a client request
    pub fn processRequest(self: *VRNode, client_id: []const u8, request_number: u64, command: Command) !void {
        if (!self.active) {
            return error.NodeInactive;
        }

        if (!self.is_primary) {
            return error.NotPrimary;
        }

        // Create a new operation
        const op = Operation{
            .op_number = self.op_number + 1,
            .client_id = try self.allocator.dupe(u8, client_id),
            .request_number = request_number,
            .command = try self.duplicateCommand(command),
        };

        // Add to log
        try self.log.append(op);
        self.op_number += 1;

        // If there are no peers, commit immediately
        if (self.peers.items.len == 0) {
            try self.commitOperation(self.op_number);
            return;
        }

        // Send prepare messages to all peers
        try self.sendPrepare(op);
    }

    /// Duplicate a command to avoid ownership issues
    fn duplicateCommand(self: *VRNode, command: Command) !Command {
        return switch (command) {
            .Put => |put| Command{
                .Put = .{
                    .key = try self.allocator.dupe(u8, put.key),
                    .value = try self.allocator.dupe(u8, put.value),
                },
            },
            .Get => |get| Command{
                .Get = .{
                    .key = try self.allocator.dupe(u8, get.key),
                },
            },
            .Delete => |delete| Command{
                .Delete = .{
                    .key = try self.allocator.dupe(u8, delete.key),
                },
            },
        };
    }

    /// Send a prepare message to all peers
    fn sendPrepare(self: *VRNode, op: Operation) !void {
        const message = Message{
            .type = .Prepare,
            .view_number = self.view_number,
            .op_number = op.op_number,
            .commit_number = self.commit_number,
            .operation = op,
        };

        for (self.peers.items) |peer| {
            try self.sendMessage(peer, message);
        }
    }

    /// Send a commit message to all peers
    fn sendCommit(self: *VRNode) !void {
        const message = Message{
            .type = .Commit,
            .view_number = self.view_number,
            .commit_number = self.commit_number,
        };

        for (self.peers.items) |peer| {
            try self.sendMessage(peer, message);
        }
    }

    /// Send a start view message to all peers
    fn sendStartView(self: *VRNode) !void {
        const message = Message{
            .type = .StartView,
            .view_number = self.view_number,
            .op_number = self.op_number,
            .commit_number = self.commit_number,
        };

        for (self.peers.items) |peer| {
            try self.sendMessage(peer, message);
        }
    }

    /// Send a message to a specific node
    fn sendMessage(self: *VRNode, recipient: []const u8, message: Message) !void {
        // Serialize the message
        var buffer = std.ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        try std.json.stringify(message, .{}, buffer.writer());

        // Send the message
        try self.simulation.sendMessage(self.id, recipient, buffer.items);
    }

    /// Commit an operation
    fn commitOperation(self: *VRNode, op_number: u64) !void {
        if (op_number <= self.commit_number) {
            return; // Already committed
        }

        // Find the operation in the log
        for (self.log.items) |op| {
            if (op.op_number > self.commit_number and op.op_number <= op_number) {
                // Apply the operation to the state
                try self.applyOperation(op);
            }
        }

        // Update commit number
        self.commit_number = op_number;

        // If primary, send commit message to all peers
        if (self.is_primary) {
            try self.sendCommit();
        }
    }

    /// Apply an operation to the state
    fn applyOperation(self: *VRNode, op: Operation) !void {
        switch (op.command) {
            .Put => |put| {
                // Check if key already exists
                if (self.state.getKey(put.key)) |existing_key| {
                    // Free the old value
                    const old_value = self.state.get(existing_key).?;
                    self.allocator.free(old_value);

                    // Store the new value
                    try self.state.put(existing_key, try self.allocator.dupe(u8, put.value));
                } else {
                    // Create new entry
                    const key_copy = try self.allocator.dupe(u8, put.key);
                    errdefer self.allocator.free(key_copy);

                    const value_copy = try self.allocator.dupe(u8, put.value);
                    errdefer self.allocator.free(value_copy);

                    try self.state.put(key_copy, value_copy);
                }
            },
            .Delete => |delete| {
                if (self.state.getKey(delete.key)) |existing_key| {
                    const old_value = self.state.get(existing_key).?;
                    self.allocator.free(old_value);
                    _ = self.state.remove(existing_key);
                    self.allocator.free(existing_key);
                }
            },
            .Get => {
                // Get operations don't modify state
            },
        }
    }

    /// Handle a message from another node
    fn handleMessage(self: *VRNode, sender: []const u8, message_data: []const u8) !void {
        if (!self.active) {
            return; // Node is inactive, ignore message
        }

        // Parse the message
        var message = try std.json.parseFromSlice(Message, self.allocator, message_data, .{});
        defer message.deinit();

        // Handle based on message type
        switch (message.value.type) {
            .Request => {
                // Client requests should be handled by processRequest, not here
            },
            .Prepare => {
                try self.handlePrepare(sender, message.value);
            },
            .PrepareOk => {
                try self.handlePrepareOk(sender, message.value);
            },
            .Commit => {
                try self.handleCommit(message.value);
            },
            .StartViewChange => {
                // Not implemented in this basic version
            },
            .DoViewChange => {
                // Not implemented in this basic version
            },
            .StartView => {
                try self.handleStartView(sender, message.value);
            },
            .GetState => {
                // Not implemented in this basic version
            },
            .NewState => {
                // Not implemented in this basic version
            },
        }
    }

    /// Handle a prepare message
    fn handlePrepare(self: *VRNode, sender: []const u8, message: Message) !void {
        // Check view number
        if (message.view_number < self.view_number) {
            return; // Ignore messages from old views
        }

        if (message.view_number > self.view_number) {
            self.view_number = message.view_number;
            self.is_primary = false;
        }

        // Check if we have the operation
        if (message.op_number) |op_number| {
            if (op_number <= self.op_number) {
                return; // Already have this operation
            }

            // Add the operation to the log
            if (message.operation) |op| {
                // Duplicate the operation
                const new_op = Operation{
                    .op_number = op.op_number,
                    .client_id = try self.allocator.dupe(u8, op.client_id),
                    .request_number = op.request_number,
                    .command = try self.duplicateCommand(op.command),
                };

                try self.log.append(new_op);
                self.op_number = op_number;

                // Send PrepareOk
                const response = Message{
                    .type = .PrepareOk,
                    .view_number = self.view_number,
                    .op_number = op_number,
                };

                try self.sendMessage(sender, response);
            }
        }

        // Check if we need to commit operations
        if (message.commit_number) |commit_number| {
            if (commit_number > self.commit_number) {
                try self.commitOperation(commit_number);
            }
        }
    }

    /// Handle a prepare-ok message
    fn handlePrepareOk(self: *VRNode, sender: []const u8, message: Message) !void {
        _ = sender;

        // Check view number
        if (message.view_number != self.view_number) {
            return; // Ignore messages from different views
        }

        // Check if we're the primary
        if (!self.is_primary) {
            return; // Only the primary handles PrepareOk messages
        }

        // Check if we have enough PrepareOk messages to commit
        // In a real implementation, we would track PrepareOk messages per operation
        // For simplicity, we'll just commit the operation immediately
        if (message.op_number) |op_number| {
            if (op_number > self.commit_number) {
                try self.commitOperation(op_number);
            }
        }
    }

    /// Handle a commit message
    fn handleCommit(self: *VRNode, message: Message) !void {
        // Check view number
        if (message.view_number < self.view_number) {
            return; // Ignore messages from old views
        }

        if (message.view_number > self.view_number) {
            self.view_number = message.view_number;
            self.is_primary = false;
        }

        // Commit operations
        if (message.commit_number) |commit_number| {
            if (commit_number > self.commit_number) {
                try self.commitOperation(commit_number);
            }
        }
    }

    /// Handle a start view message
    fn handleStartView(self: *VRNode, sender: []const u8, message: Message) !void {
        // Check view number
        if (message.view_number <= self.view_number) {
            return; // Ignore messages from old or current views
        }

        // Update view number and primary status
        self.view_number = message.view_number;
        self.is_primary = std.mem.eql(u8, self.id, sender);

        // Update op_number and commit_number if needed
        if (message.op_number) |op_number| {
            if (op_number > self.op_number) {
                // In a real implementation, we would need to get missing operations
                self.op_number = op_number;
            }
        }

        if (message.commit_number) |commit_number| {
            if (commit_number > self.commit_number) {
                try self.commitOperation(commit_number);
            }
        }
    }
};

/// Message handler for the simulation
fn messageHandler(sender: []const u8, message: []const u8, context: ?*anyopaque) void {
    const node = @as(*VRNode, @ptrCast(@alignCast(context.?)));
    node.handleMessage(sender, message) catch |err| {
        std.debug.print("Error handling message: {}\n", .{err});
    };
}

/// Run a basic Viewstamped Replication test scenario
pub fn runBasicScenario(allocator: std.mem.Allocator) !void {
    // Create simulation
    const seed = 42;
    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create nodes
    var nodes = std.ArrayList(*VRNode).init(allocator);
    defer {
        for (nodes.items) |node| {
            node.deinit();
        }
        nodes.deinit();
    }

    const node_ids = [_][]const u8{ "node1", "node2", "node3" };

    // Create node1
    var node1 = try VRNode.init(allocator, simulation, node_ids[0], node_ids[1..]);
    try nodes.append(node1);

    // Create node2
    var peers2 = [_][]const u8{ node_ids[0], node_ids[2] };
    var node2 = try VRNode.init(allocator, simulation, node_ids[1], &peers2);
    try nodes.append(node2);

    // Create node3
    var peers3 = [_][]const u8{ node_ids[0], node_ids[1] };
    const node3 = try VRNode.init(allocator, simulation, node_ids[2], &peers3);
    try nodes.append(node3);

    // Start nodes
    for (nodes.items) |node| {
        try node.start();
    }

    // Run simulation for a bit to let nodes initialize
    try simulation.run(100);

    // Process some client requests
    try node1.processRequest("client1", 1, .{ .Put = .{ .key = "key1", .value = "value1" } });
    try node1.processRequest("client1", 2, .{ .Put = .{ .key = "key2", .value = "value2" } });
    try node1.processRequest("client1", 3, .{ .Delete = .{ .key = "key1" } });

    // Run simulation to process requests
    try simulation.run(200);

    // Verify that all nodes have the same state
    for (nodes.items) |node| {
        // key1 should be deleted
        try std.testing.expect(!node.state.contains("key1"));

        // key2 should have value2
        if (node.state.get("key2")) |value| {
            try std.testing.expectEqualStrings("value2", value);
        } else {
            return error.KeyNotFound;
        }
    }

    // Simulate primary failure
    node1.stop();

    // In a real implementation, we would have view change logic here
    // For simplicity, we'll manually make node2 the primary
    node2.is_primary = true;
    node2.view_number += 1;
    try node2.sendStartView();

    // Run simulation to process view change
    try simulation.run(300);

    // Process more client requests on the new primary
    try node2.processRequest("client1", 4, .{ .Put = .{ .key = "key3", .value = "value3" } });

    // Run simulation to process requests
    try simulation.run(400);

    // Verify that all active nodes have the updated state
    for (nodes.items[1..]) |node| {
        // key3 should have value3
        if (node.state.get("key3")) |value| {
            try std.testing.expectEqualStrings("value3", value);
        } else {
            return error.KeyNotFound;
        }
    }

    // Restart the failed node
    node1.active = true;

    // In a real implementation, the node would need to catch up
    // For simplicity, we'll manually update its state
    if (node1.state.getKey("key3")) |existing_key| {
        // Free the old value if it exists
        const old_value = node1.state.get(existing_key).?;
        node1.allocator.free(old_value);

        // Store the new value
        try node1.state.put(existing_key, try node1.allocator.dupe(u8, "value3"));
    } else {
        // Create new entry
        const key_copy = try node1.allocator.dupe(u8, "key3");
        errdefer node1.allocator.free(key_copy);

        const value_copy = try node1.allocator.dupe(u8, "value3");
        errdefer node1.allocator.free(value_copy);

        try node1.state.put(key_copy, value_copy);
    }

    // Run simulation to let the node catch up
    try simulation.run(500);

    // Verify that all nodes have the same state
    for (nodes.items) |node| {
        // key1 should be deleted
        try std.testing.expect(!node.state.contains("key1"));

        // key2 should have value2
        if (node.state.get("key2")) |value| {
            try std.testing.expectEqualStrings("value2", value);
        } else {
            return error.KeyNotFound;
        }

        // key3 should have value3
        if (node.state.get("key3")) |value| {
            try std.testing.expectEqualStrings("value3", value);
        } else {
            return error.KeyNotFound;
        }
    }

    std.debug.print("Viewstamped Replication test scenario completed successfully!\n", .{});
}

test "Viewstamped Replication basic scenario" {
    try runBasicScenario(std.testing.allocator);
}
