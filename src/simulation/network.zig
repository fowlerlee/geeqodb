const std = @import("std");
const Scheduler = @import("scheduler.zig").Scheduler;

/// Simulated network for deterministic message passing between nodes
pub const SimulatedNetwork = struct {
    allocator: std.mem.Allocator,
    scheduler: *Scheduler,
    nodes: std.StringHashMap(NodeInfo),
    partitions: std.ArrayList(Partition),
    message_delay_min: u64,
    message_delay_max: u64,
    message_loss_probability: f64,

    pub const NodeInfo = struct {
        id: []const u8,
        handler: *const fn (sender: []const u8, message: []const u8, context: ?*anyopaque) void,
        context: ?*anyopaque,
    };

    pub const Partition = struct {
        nodes_a: std.ArrayList([]const u8),
        nodes_b: std.ArrayList([]const u8),
    };

    pub const MessageContext = struct {
        network: *SimulatedNetwork,
        sender: []const u8,
        recipient: []const u8,
        message: []const u8,
    };

    /// Initialize a new simulated network
    pub fn init(allocator: std.mem.Allocator, scheduler: *Scheduler) !*SimulatedNetwork {
        const network = try allocator.create(SimulatedNetwork);

        network.* = SimulatedNetwork{
            .allocator = allocator,
            .scheduler = scheduler,
            .nodes = std.StringHashMap(NodeInfo).init(allocator),
            .partitions = std.ArrayList(Partition).init(allocator),
            .message_delay_min = 1,
            .message_delay_max = 5,
            .message_loss_probability = 0.0,
        };

        return network;
    }

    /// Deinitialize the network
    pub fn deinit(self: *SimulatedNetwork) void {
        var node_it = self.nodes.iterator();
        while (node_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.nodes.deinit();

        for (self.partitions.items) |partition| {
            for (partition.nodes_a.items) |node_id| {
                self.allocator.free(node_id);
            }
            for (partition.nodes_b.items) |node_id| {
                self.allocator.free(node_id);
            }
            partition.nodes_a.deinit();
            partition.nodes_b.deinit();
        }
        self.partitions.deinit();

        self.allocator.destroy(self);
    }

    /// Register a node in the network
    pub fn registerNode(self: *SimulatedNetwork, node_id: []const u8, handler: *const fn (sender: []const u8, message: []const u8, context: ?*anyopaque) void, context: ?*anyopaque) !void {
        const id_copy = try self.allocator.dupe(u8, node_id);
        errdefer self.allocator.free(id_copy);

        const node_info = NodeInfo{
            .id = id_copy,
            .handler = handler,
            .context = context,
        };

        try self.nodes.put(id_copy, node_info);
    }

    /// Unregister a node from the network
    pub fn unregisterNode(self: *SimulatedNetwork, node_id: []const u8) void {
        if (self.nodes.getKey(node_id)) |key| {
            _ = self.nodes.remove(key);
            self.allocator.free(key);
        }
    }

    /// Send a message from one node to another
    pub fn sendMessage(self: *SimulatedNetwork, sender: []const u8, recipient: []const u8, message: []const u8) !void {
        // Check if sender exists
        if (!self.nodes.contains(sender)) {
            return error.SenderNotFound;
        }

        // Check if recipient exists
        if (!self.nodes.contains(recipient)) {
            return error.RecipientNotFound;
        }

        // Check if there's a network partition between sender and recipient
        if (self.isPartitioned(sender, recipient)) {
            return; // Message is lost due to partition
        }

        // Check for random message loss
        if (self.message_loss_probability > 0) {
            const roll = self.scheduler.getRandomFloat();
            if (roll < self.message_loss_probability) {
                return; // Message is randomly lost
            }
        }

        // Determine message delay
        const delay_range = self.message_delay_max - self.message_delay_min;
        const delay = if (delay_range == 0)
            self.message_delay_min
        else
            self.message_delay_min + self.scheduler.getRandomInt(delay_range);

        // Create message context
        const ctx = try self.allocator.create(MessageContext);
        ctx.* = MessageContext{
            .network = self,
            .sender = sender,
            .recipient = recipient,
            .message = try self.allocator.dupe(u8, message),
        };

        // Schedule message delivery
        _ = try self.scheduler.scheduleAfter(delay, 0, deliverMessageCallback, ctx);
    }

    /// Create a network partition between two sets of nodes
    pub fn createPartition(self: *SimulatedNetwork, nodes_a: []const []const u8, nodes_b: []const []const u8) !void {
        var partition = Partition{
            .nodes_a = std.ArrayList([]const u8).init(self.allocator),
            .nodes_b = std.ArrayList([]const u8).init(self.allocator),
        };

        // Copy node IDs for partition A
        for (nodes_a) |node_id| {
            const id_copy = try self.allocator.dupe(u8, node_id);
            try partition.nodes_a.append(id_copy);
        }

        // Copy node IDs for partition B
        for (nodes_b) |node_id| {
            const id_copy = try self.allocator.dupe(u8, node_id);
            try partition.nodes_b.append(id_copy);
        }

        try self.partitions.append(partition);
    }

    /// Heal all network partitions
    pub fn healPartitions(self: *SimulatedNetwork) void {
        for (self.partitions.items) |partition| {
            for (partition.nodes_a.items) |node_id| {
                self.allocator.free(node_id);
            }
            for (partition.nodes_b.items) |node_id| {
                self.allocator.free(node_id);
            }
            partition.nodes_a.deinit();
            partition.nodes_b.deinit();
        }

        self.partitions.clearRetainingCapacity();
    }

    /// Set message delay range
    pub fn setMessageDelay(self: *SimulatedNetwork, min: u64, max: u64) void {
        self.message_delay_min = min;
        self.message_delay_max = max;
    }

    /// Set message loss probability (0.0 - 1.0)
    pub fn setMessageLossProbability(self: *SimulatedNetwork, probability: f64) void {
        self.message_loss_probability = std.math.clamp(probability, 0.0, 1.0);
    }

    /// Check if two nodes are partitioned
    fn isPartitioned(self: *SimulatedNetwork, node_a: []const u8, node_b: []const u8) bool {
        for (self.partitions.items) |partition| {
            const a_in_first = containsNode(partition.nodes_a.items, node_a);
            const b_in_first = containsNode(partition.nodes_a.items, node_b);
            const a_in_second = containsNode(partition.nodes_b.items, node_a);
            const b_in_second = containsNode(partition.nodes_b.items, node_b);

            // If nodes are in different partitions, they can't communicate
            if ((a_in_first and b_in_second) or (a_in_second and b_in_first)) {
                return true;
            }
        }

        return false;
    }

    /// Check if a list of nodes contains a specific node ID
    fn containsNode(nodes: []const []const u8, node_id: []const u8) bool {
        for (nodes) |id| {
            if (std.mem.eql(u8, id, node_id)) {
                return true;
            }
        }
        return false;
    }
};

/// Callback for delivering messages
fn deliverMessageCallback(context: ?*anyopaque) void {
    const ctx = @as(*SimulatedNetwork.MessageContext, @ptrCast(@alignCast(context.?)));
    defer {
        ctx.network.allocator.free(ctx.message);
        ctx.network.allocator.destroy(ctx);
    }

    // Get recipient node info
    if (ctx.network.nodes.get(ctx.recipient)) |node_info| {
        // Deliver the message
        node_info.handler(ctx.sender, ctx.message, node_info.context);
    }
}

test "SimulatedNetwork basic functionality" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var network = try SimulatedNetwork.init(allocator, scheduler);
    defer network.deinit();

    // Set up test context
    var node1_received = false;
    var node2_received = false;

    var node1_ctx = TestContext{ .received = &node1_received };
    var node2_ctx = TestContext{ .received = &node2_received };

    // Register nodes
    try network.registerNode("node1", testMessageHandler, &node1_ctx);
    try network.registerNode("node2", testMessageHandler, &node2_ctx);

    // Send messages
    try network.sendMessage("node1", "node2", "Hello from node1");
    try network.sendMessage("node2", "node1", "Hello from node2");

    // Run the scheduler
    try scheduler.run(null);

    // Check that both nodes received messages
    try std.testing.expect(node1_received);
    try std.testing.expect(node2_received);
}

test "SimulatedNetwork with partition" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var network = try SimulatedNetwork.init(allocator, scheduler);
    defer network.deinit();

    // Set up test context
    var node1_received = false;
    var node2_received = false;
    var node3_received = false;

    var node1_ctx = TestContext{ .received = &node1_received };
    var node2_ctx = TestContext{ .received = &node2_received };
    var node3_ctx = TestContext{ .received = &node3_received };

    // Register nodes
    try network.registerNode("node1", testMessageHandler, &node1_ctx);
    try network.registerNode("node2", testMessageHandler, &node2_ctx);
    try network.registerNode("node3", testMessageHandler, &node3_ctx);

    // Create a partition between node1 and node2
    try network.createPartition(&[_][]const u8{"node1"}, &[_][]const u8{"node2"});

    // Send messages
    try network.sendMessage("node1", "node2", "Hello from node1"); // Should be lost
    try network.sendMessage("node2", "node1", "Hello from node2"); // Should be lost
    try network.sendMessage("node1", "node3", "Hello from node1 to node3"); // Should be delivered
    try network.sendMessage("node2", "node3", "Hello from node2 to node3"); // Should be delivered

    // Run the scheduler
    try scheduler.run(null);

    // Check that only node3 received messages
    try std.testing.expect(!node1_received);
    try std.testing.expect(!node2_received);
    try std.testing.expect(node3_received);

    // Heal the partition
    network.healPartitions();

    // Reset received flags
    node1_received = false;
    node2_received = false;
    node3_received = false;

    // Send messages again
    try network.sendMessage("node1", "node2", "Hello from node1");
    try network.sendMessage("node2", "node1", "Hello from node2");

    // Run the scheduler
    try scheduler.run(null);

    // Check that both nodes received messages
    try std.testing.expect(node1_received);
    try std.testing.expect(node2_received);
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
