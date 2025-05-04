const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for the main library
    const geeqodb_module = b.addModule("geeqodb", .{
        .root_source_file = b.path("src/main.zig"),
    });

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

    // Create test steps
    const run_planner_tests = b.addRunArtifact(planner_test);
    run_planner_tests.has_side_effects = true;

    const run_rocksdb_tests = b.addRunArtifact(rocksdb_test);
    run_rocksdb_tests.has_side_effects = true;

    const run_database_tests = b.addRunArtifact(database_test);
    run_database_tests.has_side_effects = true;

    // Add test steps to the main test step
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_planner_tests.step);
    test_step.dependOn(&run_rocksdb_tests.step);
    test_step.dependOn(&run_database_tests.step);

    // Add a separate step for database tests only
    const database_test_step = b.step("test-database", "Run database tests only");
    database_test_step.dependOn(&run_database_tests.step);

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
    // Add the tool builds to this step
}
