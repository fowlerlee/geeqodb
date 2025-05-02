const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the geeqodb module for use in other projects
    const geeqodb_module = b.addModule("geeqodb", .{
        .root_source_file = b.path("src/main.zig"),
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

    // Test steps
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_main_tests.step);
    test_step.dependOn(component_test_step);

    const main_test_step = b.step("test-main", "Run main tests");
    main_test_step.dependOn(&run_main_tests.step);
}
