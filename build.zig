const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create a module for the main library
    const geeqodb_module = b.addModule("geeqodb", .{
        .root_source_file = b.path("src/main.zig"),
    });

    const tests = b.addTest(.{
        .root_source_file = b.path("src/tests/storage/rocksdb_test.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add the main module to the test
    tests.root_module.addImport("geeqodb", geeqodb_module);

    // Add RocksDB dependency to tests
    tests.linkSystemLibrary("rocksdb");

    const run_tests = b.addRunArtifact(tests);
    run_tests.has_side_effects = true;

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
