const std = @import("std");
const simulation = @import("simulation");
const Simulation = simulation.Simulation;
const vr_scenario = simulation.scenarios.viewstamped_replication;
const db_scenario = simulation.scenarios.database_integration;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command-line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Default parameters
    var scenario_name: ?[]const u8 = null;
    var seed: u64 = 42;
    var verbose = false;

    // Parse command-line arguments
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--scenario") or std.mem.eql(u8, arg, "-s")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: Missing value for --scenario\n", .{});
                return error.InvalidArguments;
            }
            scenario_name = args[i];
        } else if (std.mem.eql(u8, arg, "--seed") or std.mem.eql(u8, arg, "-r")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: Missing value for --seed\n", .{});
                return error.InvalidArguments;
            }
            seed = try std.fmt.parseInt(u64, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--verbose") or std.mem.eql(u8, arg, "-v")) {
            verbose = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            std.debug.print("Unknown argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArguments;
        }
    }

    // Run the specified scenario or all scenarios
    if (scenario_name) |name| {
        try runScenario(allocator, name, seed, verbose);
    } else {
        try runAllScenarios(allocator, seed, verbose);
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: run_simulation_tests [options]
        \\
        \\Options:
        \\  --scenario, -s <name>  Run a specific scenario
        \\  --seed, -r <number>    Set the random seed (default: 42)
        \\  --verbose, -v          Enable verbose output
        \\  --help, -h             Show this help message
        \\
        \\Available scenarios:
        \\  vr_basic               Basic Viewstamped Replication scenario
        \\  vr_network_partition   Viewstamped Replication with network partition
        \\  vr_node_failure        Viewstamped Replication with node failure
        \\  db_integration         Database integration with simulated disk I/O
        \\
        \\Example:
        \\  run_simulation_tests --scenario vr_basic --seed 123
        \\
    , .{});
}

fn runScenario(allocator: std.mem.Allocator, name: []const u8, seed: u64, verbose: bool) !void {
    if (verbose) {
        std.debug.print("Using seed: {d}\n", .{seed});
    }

    std.debug.print("Running scenario: {s}\n", .{name});

    if (std.mem.eql(u8, name, "vr_basic")) {
        try vr_scenario.runBasicScenario(allocator);
    } else if (std.mem.eql(u8, name, "vr_network_partition")) {
        std.debug.print("Scenario not implemented yet: {s}\n", .{name});
        return error.ScenarioNotImplemented;
    } else if (std.mem.eql(u8, name, "vr_node_failure")) {
        std.debug.print("Scenario not implemented yet: {s}\n", .{name});
        return error.ScenarioNotImplemented;
    } else if (std.mem.eql(u8, name, "db_integration")) {
        try db_scenario.runDatabaseIntegrationScenario(allocator);
    } else {
        std.debug.print("Unknown scenario: {s}\n", .{name});
        return error.UnknownScenario;
    }

    std.debug.print("Scenario completed successfully: {s}\n", .{name});
}

fn runAllScenarios(allocator: std.mem.Allocator, seed: u64, verbose: bool) !void {
    std.debug.print("Running all scenarios with seed {d}\n", .{seed});

    if (verbose) {
        std.debug.print("Verbose output enabled\n", .{});
    }

    // Run the basic VR scenario
    std.debug.print("\n=== Running scenario: vr_basic ===\n", .{});
    try vr_scenario.runBasicScenario(allocator);
    std.debug.print("=== Scenario completed successfully: vr_basic ===\n", .{});

    // Run the database integration scenario
    std.debug.print("\n=== Running scenario: db_integration ===\n", .{});
    try db_scenario.runDatabaseIntegrationScenario(allocator);
    std.debug.print("=== Scenario completed successfully: db_integration ===\n", .{});

    // Add more scenarios here as they are implemented

    std.debug.print("\nAll scenarios completed successfully!\n", .{});
}
