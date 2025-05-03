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
    pub const advanced_database_scenarios = @import("scenarios/advanced_database_scenarios.zig");
    pub const advanced_database_scenarios_part2 = @import("scenarios/advanced_database_scenarios_part2.zig");
};

// Import tests
test {
    std.testing.refAllDeclsRecursive(@This());
    _ = @import("test.zig");
}
