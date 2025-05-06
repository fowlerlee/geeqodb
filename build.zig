const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for the main library
    const geeqodb_module = b.addModule("geeqodb", .{
        .root_source_file = b.path("src/main.zig"),
    });

    // Add CUDA wrapper
    geeqodb_module.addIncludePath(b.path("src/gpu"));
    geeqodb_module.addCSourceFile(.{
        .file = b.path("src/gpu/cuda_wrapper.c"),
        .flags = &.{"-std=c11"},
    });

    // Create modules for simulation components
    const simulation_module = b.addModule("simulation", .{
        .root_source_file = b.path("src/simulation/simulation.zig"),
    });

    const replica_management_module = b.addModule("replica_management", .{
        .root_source_file = b.path("src/simulation/scenarios/replica_management.zig"),
    });

    const view_change_module = b.addModule("view_change", .{
        .root_source_file = b.path("src/simulation/scenarios/view_change_protocol.zig"),
    });

    const distributed_wal_module = b.addModule("distributed_wal", .{
        .root_source_file = b.path("src/storage/distributed_wal.zig"),
    });

    // Add dependencies between modules
    view_change_module.addImport("replica_management", replica_management_module);
    view_change_module.addImport("simulation", simulation_module);

    distributed_wal_module.addImport("simulation", simulation_module);
    distributed_wal_module.addImport("replica_management", replica_management_module);

    // Add RocksDB dependency
    const rocksdb_lib = b.addStaticLibrary(.{
        .name = "rocksdb",
        .root_source_file = b.path("src/storage/rocksdb.zig"),
        .target = target,
        .optimize = optimize,
    });
    rocksdb_lib.linkSystemLibrary("rocksdb");
    rocksdb_lib.addIncludePath(b.path("deps/rocksdb/include"));
    rocksdb_lib.addLibraryPath(b.path("deps/rocksdb/lib"));

    // Add tests for individual modules
    const planner_test = b.addTest(.{
        .root_source_file = b.path("src/tests/query/planner_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    planner_test.root_module.addImport("geeqodb", geeqodb_module);
    planner_test.linkSystemLibrary("rocksdb");

    const executor_test = b.addTest(.{
        .root_source_file = b.path("src/tests/query/executor_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    executor_test.root_module.addImport("geeqodb", geeqodb_module);
    executor_test.linkSystemLibrary("rocksdb");

    const advanced_planner_test = b.addTest(.{
        .root_source_file = b.path("src/tests/query/advanced_planner_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    advanced_planner_test.root_module.addImport("geeqodb", geeqodb_module);
    advanced_planner_test.linkSystemLibrary("rocksdb");

    const gpu_test = b.addTest(.{
        .root_source_file = b.path("src/tests/gpu/gpu_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    gpu_test.root_module.addImport("geeqodb", geeqodb_module);

    const cuda_kernels_test = b.addTest(.{
        .root_source_file = b.path("src/tests/gpu/cuda_kernels_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    cuda_kernels_test.root_module.addImport("geeqodb", geeqodb_module);

    const rocksdb_test = b.addTest(.{
        .root_source_file = b.path("src/tests/storage/rocksdb_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    rocksdb_test.root_module.addImport("geeqodb", geeqodb_module);
    rocksdb_test.linkSystemLibrary("rocksdb");

    const database_test = b.addTest(.{
        .root_source_file = b.path("src/tests/core/database_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    database_test.root_module.addImport("geeqodb", geeqodb_module);
    database_test.linkSystemLibrary("rocksdb");

    // Add replication tests
    const replica_management_test = b.addTest(.{
        .root_source_file = b.path("src/tests/replication/replica_management_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    replica_management_test.root_module.addImport("geeqodb", geeqodb_module);
    replica_management_test.root_module.addImport("replica_management", replica_management_module);

    const view_change_test = b.addTest(.{
        .root_source_file = b.path("src/tests/replication/view_change_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    view_change_test.root_module.addImport("geeqodb", geeqodb_module);
    view_change_test.root_module.addImport("simulation", simulation_module);
    view_change_test.root_module.addImport("replica_management", replica_management_module);
    view_change_test.root_module.addImport("view_change", view_change_module);

    const distributed_log_test = b.addTest(.{
        .root_source_file = b.path("src/tests/replication/distributed_log_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    distributed_log_test.root_module.addImport("geeqodb", geeqodb_module);
    distributed_log_test.root_module.addImport("simulation", simulation_module);
    distributed_log_test.root_module.addImport("replica_management", replica_management_module);
    distributed_log_test.root_module.addImport("distributed_wal", distributed_wal_module);

    // Create test steps
    const run_planner_tests = b.addRunArtifact(planner_test);
    run_planner_tests.has_side_effects = true;

    const run_executor_tests = b.addRunArtifact(executor_test);
    run_executor_tests.has_side_effects = true;

    const run_advanced_planner_tests = b.addRunArtifact(advanced_planner_test);
    run_advanced_planner_tests.has_side_effects = true;

    const run_gpu_tests = b.addRunArtifact(gpu_test);
    run_gpu_tests.has_side_effects = true;

    const run_cuda_kernels_tests = b.addRunArtifact(cuda_kernels_test);
    run_cuda_kernels_tests.has_side_effects = true;

    const run_rocksdb_tests = b.addRunArtifact(rocksdb_test);
    run_rocksdb_tests.has_side_effects = true;

    const run_database_tests = b.addRunArtifact(database_test);
    run_database_tests.has_side_effects = true;

    const run_replica_management_tests = b.addRunArtifact(replica_management_test);
    run_replica_management_tests.has_side_effects = true;

    const run_view_change_tests = b.addRunArtifact(view_change_test);
    run_view_change_tests.has_side_effects = true;

    const run_distributed_log_tests = b.addRunArtifact(distributed_log_test);
    run_distributed_log_tests.has_side_effects = true;

    // Add test steps to the main test step
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_planner_tests.step);
    test_step.dependOn(&run_executor_tests.step);
    test_step.dependOn(&run_advanced_planner_tests.step);
    test_step.dependOn(&run_gpu_tests.step);
    test_step.dependOn(&run_cuda_kernels_tests.step);
    test_step.dependOn(&run_rocksdb_tests.step);
    test_step.dependOn(&run_database_tests.step);
    test_step.dependOn(&run_replica_management_tests.step);
    test_step.dependOn(&run_view_change_tests.step);
    test_step.dependOn(&run_distributed_log_tests.step);

    // Add a separate step for database tests only
    const database_test_step = b.step("test-database", "Run database tests only");
    database_test_step.dependOn(&run_database_tests.step);

    // Add a separate step for executor tests only
    const executor_test_step = b.step("test-executor", "Run executor tests only");
    executor_test_step.dependOn(&run_executor_tests.step);

    // Add a separate step for advanced planner tests only
    const advanced_planner_test_step = b.step("test-advanced-planner", "Run advanced planner tests only");
    advanced_planner_test_step.dependOn(&run_advanced_planner_tests.step);

    // Add a separate step for GPU tests only
    const gpu_test_step = b.step("test-gpu", "Run GPU tests only");
    gpu_test_step.dependOn(&run_gpu_tests.step);
    gpu_test_step.dependOn(&run_cuda_kernels_tests.step);

    // Add a separate step for replication tests only
    const replication_test_step = b.step("test-replication", "Run replication tests only");
    replication_test_step.dependOn(&run_replica_management_tests.step);
    replication_test_step.dependOn(&run_view_change_tests.step);
    replication_test_step.dependOn(&run_distributed_log_tests.step);

    const tools_step = b.step("tools", "Build all tools in the src/tools directory");

    // Build SQL client tool
    const sql_client = b.addExecutable(.{
        .name = "sql_client",
        .root_source_file = b.path("src/tools/sql_client.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_client.root_module.addImport("geeqodb", geeqodb_module);
    b.installArtifact(sql_client);
    tools_step.dependOn(b.getInstallStep());

    // Build example tools
    const query_example = b.addExecutable(.{
        .name = "query_example",
        .root_source_file = b.path("src/examples/query_example.zig"),
        .target = target,
        .optimize = optimize,
    });
    query_example.root_module.addImport("geeqodb", geeqodb_module);
    query_example.linkSystemLibrary("rocksdb");
    b.installArtifact(query_example);
    tools_step.dependOn(b.getInstallStep());

    // Build GPU benchmark tool
    const gpu_benchmark = b.addExecutable(.{
        .name = "gpu_benchmark",
        .root_source_file = b.path("src/benchmarks/gpu_benchmark.zig"),
        .target = target,
        .optimize = optimize,
    });
    gpu_benchmark.root_module.addImport("geeqodb", geeqodb_module);
    gpu_benchmark.linkSystemLibrary("rocksdb");
    b.installArtifact(gpu_benchmark);
    tools_step.dependOn(b.getInstallStep());
    // Add the tool builds to this step
}
