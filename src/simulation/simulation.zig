const std = @import("std");
const Scheduler = @import("scheduler.zig").Scheduler;
const VirtualClock = @import("virtual_clock.zig").VirtualClock;
const SimulatedNetwork = @import("network.zig").SimulatedNetwork;
const SimulatedDisk = @import("disk.zig").SimulatedDisk;

/// Main simulation environment that ties together all simulation components
pub const Simulation = struct {
    allocator: std.mem.Allocator,
    scheduler: *Scheduler,
    network: *SimulatedNetwork,
    disks: std.StringHashMap(*SimulatedDisk),
    clocks: std.StringHashMap(VirtualClock),
    seed: u64,

    /// Initialize a new simulation environment
    pub fn init(allocator: std.mem.Allocator, seed: u64) !*Simulation {
        const simulation = try allocator.create(Simulation);

        var scheduler = try Scheduler.init(allocator, seed);
        errdefer scheduler.deinit();

        var network = try SimulatedNetwork.init(allocator, scheduler);
        errdefer network.deinit();

        simulation.* = Simulation{
            .allocator = allocator,
            .scheduler = scheduler,
            .network = network,
            .disks = std.StringHashMap(*SimulatedDisk).init(allocator),
            .clocks = std.StringHashMap(VirtualClock).init(allocator),
            .seed = seed,
        };

        return simulation;
    }

    /// Deinitialize the simulation environment
    pub fn deinit(self: *Simulation) void {
        // Clean up disks
        var disk_it = self.disks.iterator();
        while (disk_it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.disks.deinit();

        // Clean up clocks
        var clock_it = self.clocks.iterator();
        while (clock_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.clocks.deinit();

        // Clean up network and scheduler
        self.network.deinit();
        self.scheduler.deinit();

        self.allocator.destroy(self);
    }

    /// Create a new virtual clock for a node
    pub fn createClock(self: *Simulation, node_id: []const u8, drift_factor: f64) !void {
        const id_copy = try self.allocator.dupe(u8, node_id);
        errdefer self.allocator.free(id_copy);

        const clock = VirtualClock.init(self.scheduler, drift_factor);
        try self.clocks.put(id_copy, clock);
    }

    /// Get a virtual clock for a node
    pub fn getClock(self: *Simulation, node_id: []const u8) ?*VirtualClock {
        if (self.clocks.getPtr(node_id)) |clock_ptr| {
            return clock_ptr;
        }
        return null;
    }

    /// Create a new simulated disk for a node
    pub fn createDisk(self: *Simulation, node_id: []const u8) !void {
        const id_copy = try self.allocator.dupe(u8, node_id);
        errdefer self.allocator.free(id_copy);

        var disk = try SimulatedDisk.init(self.allocator, self.scheduler);
        errdefer disk.deinit();

        try self.disks.put(id_copy, disk);
    }

    /// Get a simulated disk for a node
    pub fn getDisk(self: *Simulation, node_id: []const u8) ?*SimulatedDisk {
        return self.disks.get(node_id);
    }

    /// Register a node in the network
    pub fn registerNode(
        self: *Simulation,
        node_id: []const u8,
        handler: *const fn (sender: []const u8, message: []const u8, context: ?*anyopaque) void,
        context: ?*anyopaque,
    ) !void {
        try self.network.registerNode(node_id, handler, context);
    }

    /// Unregister a node from the network
    pub fn unregisterNode(self: *Simulation, node_id: []const u8) void {
        self.network.unregisterNode(node_id);

        // Remove the node's clock if it exists
        if (self.clocks.getKey(node_id)) |key| {
            _ = self.clocks.remove(key);
            self.allocator.free(key);
        }

        // Remove the node's disk if it exists
        if (self.disks.getKey(node_id)) |key| {
            const disk = self.disks.get(key).?;
            disk.deinit();
            _ = self.disks.remove(key);
            self.allocator.free(key);
        }
    }

    /// Send a message from one node to another
    pub fn sendMessage(self: *Simulation, sender: []const u8, recipient: []const u8, message: []const u8) !void {
        try self.network.sendMessage(sender, recipient, message);
    }

    /// Create a network partition between two sets of nodes
    pub fn createPartition(self: *Simulation, nodes_a: []const []const u8, nodes_b: []const []const u8) !void {
        try self.network.createPartition(nodes_a, nodes_b);
    }

    /// Heal all network partitions
    pub fn healPartitions(self: *Simulation) void {
        self.network.healPartitions();
    }

    /// Run the simulation until there are no more tasks or until max_time is reached
    pub fn run(self: *Simulation, max_time: ?u64) !void {
        try self.scheduler.run(max_time);
    }

    /// Run a single step of the simulation
    pub fn step(self: *Simulation) !bool {
        return self.scheduler.step();
    }

    /// Get the current simulation time
    pub fn getCurrentTime(self: *Simulation) u64 {
        return self.scheduler.getCurrentTime();
    }

    /// Set message delay range for the network
    pub fn setNetworkMessageDelay(self: *Simulation, min: u64, max: u64) void {
        self.network.setMessageDelay(min, max);
    }

    /// Set message loss probability for the network
    pub fn setNetworkMessageLossProbability(self: *Simulation, probability: f64) void {
        self.network.setMessageLossProbability(probability);
    }

    /// Set read delay range for a node's disk
    pub fn setDiskReadDelay(self: *Simulation, node_id: []const u8, min: u64, max: u64) !void {
        if (self.disks.get(node_id)) |disk| {
            disk.setReadDelay(min, max);
        } else {
            return error.NodeNotFound;
        }
    }

    /// Set write delay range for a node's disk
    pub fn setDiskWriteDelay(self: *Simulation, node_id: []const u8, min: u64, max: u64) !void {
        if (self.disks.get(node_id)) |disk| {
            disk.setWriteDelay(min, max);
        } else {
            return error.NodeNotFound;
        }
    }

    /// Set read error probability for a node's disk
    pub fn setDiskReadErrorProbability(self: *Simulation, node_id: []const u8, probability: f64) !void {
        if (self.disks.get(node_id)) |disk| {
            disk.setReadErrorProbability(probability);
        } else {
            return error.NodeNotFound;
        }
    }

    /// Set write error probability for a node's disk
    pub fn setDiskWriteErrorProbability(self: *Simulation, node_id: []const u8, probability: f64) !void {
        if (self.disks.get(node_id)) |disk| {
            disk.setWriteErrorProbability(probability);
        } else {
            return error.NodeNotFound;
        }
    }

    /// Set corruption probability for a node's disk
    pub fn setDiskCorruptionProbability(self: *Simulation, node_id: []const u8, probability: f64) !void {
        if (self.disks.get(node_id)) |disk| {
            disk.setCorruptionProbability(probability);
        } else {
            return error.NodeNotFound;
        }
    }

    /// Inject corruption into a specific file on a node's disk
    pub fn injectDiskCorruption(self: *Simulation, node_id: []const u8, path: []const u8) !void {
        if (self.disks.get(node_id)) |disk| {
            try disk.injectCorruption(path);
        } else {
            return error.NodeNotFound;
        }
    }

    /// Set clock drift factor for a node
    pub fn setClockDriftFactor(self: *Simulation, node_id: []const u8, drift_factor: f64) !void {
        if (self.clocks.getPtr(node_id)) |clock| {
            clock.setDriftFactor(drift_factor);
        } else {
            return error.NodeNotFound;
        }
    }
};

test "Simulation basic functionality" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var simulation = try Simulation.init(allocator, seed);
    defer simulation.deinit();

    // Create clocks for nodes
    try simulation.createClock("node1", 1.0);
    try simulation.createClock("node2", 1.0);

    // Create disks for nodes
    try simulation.createDisk("node1");
    try simulation.createDisk("node2");

    // Set up test context
    var node1_received = false;
    var node2_received = false;

    var node1_ctx = TestContext{ .received = &node1_received };
    var node2_ctx = TestContext{ .received = &node2_received };

    // Register nodes
    try simulation.registerNode("node1", testMessageHandler, &node1_ctx);
    try simulation.registerNode("node2", testMessageHandler, &node2_ctx);

    // Send messages
    try simulation.sendMessage("node1", "node2", "Hello from node1");
    try simulation.sendMessage("node2", "node1", "Hello from node2");

    // Run the simulation
    try simulation.run(null);

    // Check that both nodes received messages
    try std.testing.expect(node1_received);
    try std.testing.expect(node2_received);

    // Test disk operations
    var disk1 = simulation.getDisk("node1").?;

    // Set up disk test context
    var read_success = false;
    var write_success = false;
    var disk_ctx = DiskTestContext{
        .read_success = &read_success,
        .write_success = &write_success,
    };

    // Write data to disk1
    try disk1.write("test.txt", "Hello, World!", testDiskCallback, &disk_ctx);

    // Run the simulation to process the write
    try simulation.run(null);

    // Check that write succeeded
    try std.testing.expect(write_success);

    // Reset flags
    write_success = false;

    // Read data from disk1
    try disk1.read("test.txt", testDiskCallback, &disk_ctx);

    // Run the simulation to process the read
    try simulation.run(null);

    // Check that read succeeded
    try std.testing.expect(read_success);

    // Test network partition
    try simulation.createPartition(&[_][]const u8{"node1"}, &[_][]const u8{"node2"});

    // Reset flags
    node1_received = false;
    node2_received = false;

    // Send messages (should be lost due to partition)
    try simulation.sendMessage("node1", "node2", "Hello from node1");
    try simulation.sendMessage("node2", "node1", "Hello from node2");

    // Run the simulation
    try simulation.run(null);

    // Check that neither node received messages
    try std.testing.expect(!node1_received);
    try std.testing.expect(!node2_received);

    // Heal partitions
    simulation.healPartitions();

    // Reset flags
    node1_received = false;
    node2_received = false;

    // Send messages again
    try simulation.sendMessage("node1", "node2", "Hello from node1");
    try simulation.sendMessage("node2", "node1", "Hello from node2");

    // Run the simulation
    try simulation.run(null);

    // Check that both nodes received messages
    try std.testing.expect(node1_received);
    try std.testing.expect(node2_received);

    // Test unregistering a node
    simulation.unregisterNode("node2");

    // Reset flags
    node1_received = false;

    // Send message to unregistered node (should fail)
    simulation.sendMessage("node1", "node2", "Hello from node1") catch |err| {
        try std.testing.expectEqual(error.RecipientNotFound, err);
    };

    // Check that node1's disk and clock still exist
    try std.testing.expect(simulation.getDisk("node1") != null);
    try std.testing.expect(simulation.getClock("node1") != null);

    // Check that node2's disk and clock were removed
    try std.testing.expect(simulation.getDisk("node2") == null);
    try std.testing.expect(simulation.getClock("node2") == null);
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

const DiskTestContext = struct {
    read_success: *bool,
    write_success: *bool,
};

fn testDiskCallback(
    op: SimulatedDisk.DiskOperation,
    path: []const u8,
    data: ?[]const u8,
    error_code: ?anyerror,
    context: ?*anyopaque,
) void {
    _ = path;

    if (context) |ctx| {
        const test_ctx = @as(*DiskTestContext, @ptrCast(@alignCast(ctx)));

        switch (op) {
            .Read => {
                if (error_code == null and data != null) {
                    test_ctx.read_success.* = true;
                }
            },
            .Write => {
                if (error_code == null) {
                    test_ctx.write_success.* = true;
                }
            },
        }
    }
}
