const std = @import("std");

pub const Scheduler = @import("scheduler.zig").Scheduler;
pub const VirtualClock = @import("virtual_clock.zig").VirtualClock;
pub const SimulatedNetwork = @import("network.zig").SimulatedNetwork;
pub const SimulatedDisk = @import("disk.zig").SimulatedDisk;
pub const Simulation = @import("simulation.zig").Simulation;

// Import test scenarios
pub const scenarios = struct {
    pub const viewstamped_replication = @import("scenarios/viewstamped_replication.zig");
    pub const database_integration = @import("scenarios/database_integration.zig");
};

// Import tests
test {
    std.testing.refAllDeclsRecursive(@This());
    _ = @import("test.zig");
}
