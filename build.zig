const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the geeqodb module for use in other projects
    const geeqodb_module = b.addModule("geeqodb", .{
        .root_source_file = b.path("src/main.zig"),
    });

    // Create the simulation module
    const simulation_module = b.addModule("simulation", .{
        .root_source_file = b.path("src/simulation/main.zig"),
    });

    const lib = b.addStaticLibrary(.{
        .name = "geeqodb",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(lib);

    // Main tests
    const main_tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Disable assertions for tests
    const test_options = b.addOptions();
    test_options.addOption(bool, "enable_assertions", false);
    main_tests.root_module.addOptions("build_options", test_options);

    const run_main_tests = b.addRunArtifact(main_tests);

    // Component tests
    const test_files = [_][]const u8{
        "src/tests/core/database_test.zig",
        "src/tests/storage/rocksdb_test.zig",
        "src/tests/storage/wal_test.zig",
        "src/tests/query/planner_test.zig",
        "src/tests/query/executor_test.zig",
        "src/tests/query/result_test.zig",
        "src/tests/transaction/manager_test.zig",
        "src/tests/server/server_test.zig",
    };

    const component_test_step = b.step("test-components", "Run component tests");

    for (test_files) |test_file| {
        const test_name = std.fs.path.stem(test_file);

        const component_test = b.addTest(.{
            .name = test_name,
            .root_source_file = b.path(test_file),
            .target = target,
            .optimize = optimize,
        });

        // Add the main source file as a module to the test
        component_test.root_module.addImport("geeqodb", geeqodb_module);

        // Disable assertions for tests
        component_test.root_module.addOptions("build_options", test_options);

        const run_component_test = b.addRunArtifact(component_test);
        component_test_step.dependOn(&run_component_test.step);
    }

    // Benchmark executables
    const benchmark_files = [_][]const u8{
        "src/benchmarks/database_benchmark.zig",
        "src/benchmarks/storage_benchmark.zig",
        "src/benchmarks/transaction_benchmark.zig",
        "src/benchmarks/query_benchmark.zig",
    };

    // Example executables
    const example_files = [_][]const u8{
        "src/examples/sql_client_example.zig",
    };

    const example_step = b.step("examples", "Build example executables");

    for (example_files) |example_file| {
        const example_name = std.fs.path.stem(example_file);

        const example_exe = b.addExecutable(.{
            .name = example_name,
            .root_source_file = b.path(example_file),
            .target = target,
            .optimize = optimize,
        });

        // Add the main source file as a module to the example
        example_exe.root_module.addImport("geeqodb", geeqodb_module);

        b.installArtifact(example_exe);

        const run_example = b.addRunArtifact(example_exe);
        run_example.step.dependOn(b.getInstallStep());

        // Create individual example steps
        const step_name = std.fmt.allocPrint(b.allocator, "run-{s}", .{example_name}) catch unreachable;
        const step_desc = std.fmt.allocPrint(b.allocator, "Run {s} example", .{example_name}) catch unreachable;
        const run_single_example = b.step(step_name, step_desc);
        run_single_example.dependOn(&run_example.step);

        // Add to the main example step
        example_step.dependOn(&run_example.step);
    }

    // Tool executables
    const tool_files = [_][]const u8{
        "src/tools/sql_client.zig",
        "scripts/seed_database.zig",
        "scripts/test_database.zig",
        "scripts/run_simulation_tests.zig",
    };

    const tool_step = b.step("tools", "Build tool executables");

    for (tool_files) |tool_file| {
        const tool_name = std.fs.path.stem(tool_file);

        const tool_exe = b.addExecutable(.{
            .name = tool_name,
            .root_source_file = b.path(tool_file),
            .target = target,
            .optimize = optimize,
        });

        // Add the main source file as a module to the tool
        tool_exe.root_module.addImport("geeqodb", geeqodb_module);

        // Add the simulation module if this is the simulation test runner
        if (std.mem.eql(u8, tool_name, "run_simulation_tests")) {
            tool_exe.root_module.addImport("simulation", simulation_module);
        }

        b.installArtifact(tool_exe);

        // Add to the main tool step
        tool_step.dependOn(b.getInstallStep());
    }

    // Main executable
    const exe = b.addExecutable(.{
        .name = "geeqodb",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    const run_step = b.step("run", "Run the database server");
    run_step.dependOn(&run_cmd.step);

    const benchmark_step = b.step("benchmark", "Run all benchmarks");

    for (benchmark_files) |benchmark_file| {
        const benchmark_name = std.fs.path.stem(benchmark_file);

        const benchmark_exe = b.addExecutable(.{
            .name = benchmark_name,
            .root_source_file = b.path(benchmark_file),
            .target = target,
            .optimize = .ReleaseFast, // Use ReleaseFast for benchmarks
        });

        // Add the main source file as a module to the benchmark
        benchmark_exe.root_module.addImport("geeqodb", geeqodb_module);

        b.installArtifact(benchmark_exe);

        const run_benchmark = b.addRunArtifact(benchmark_exe);
        run_benchmark.step.dependOn(b.getInstallStep());

        // Create individual benchmark steps
        const step_name = std.fmt.allocPrint(b.allocator, "benchmark-{s}", .{benchmark_name}) catch unreachable;
        const step_desc = std.fmt.allocPrint(b.allocator, "Run {s} benchmark", .{benchmark_name}) catch unreachable;
        const run_single_benchmark = b.step(step_name, step_desc);
        run_single_benchmark.dependOn(&run_benchmark.step);

        // Add to the main benchmark step
        benchmark_step.dependOn(&run_benchmark.step);
    }

    // Simulation test executable
    const simulation_test_exe = b.addExecutable(.{
        .name = "run_simulation_tests",
        .root_source_file = b.path("scripts/run_simulation_tests.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add the modules to the simulation test
    simulation_test_exe.root_module.addImport("geeqodb", geeqodb_module);
    simulation_test_exe.root_module.addImport("simulation", simulation_module);

    b.installArtifact(simulation_test_exe);

    const run_simulation_tests = b.addRunArtifact(simulation_test_exe);
    run_simulation_tests.step.dependOn(b.getInstallStep());

    const simulation_test_step = b.step("test-simulation", "Run simulation tests");
    simulation_test_step.dependOn(&run_simulation_tests.step);

    // Test steps
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_main_tests.step);
    test_step.dependOn(component_test_step);
    test_step.dependOn(simulation_test_step);

    const main_test_step = b.step("test-main", "Run main tests");
    main_test_step.dependOn(&run_main_tests.step);
}
