const std = @import("std");

// Re-export the modules for testing
pub const core = @import("core/database.zig");
pub const storage = struct {
    pub const rocksdb = @import("storage/rocksdb.zig");
    pub const wal = @import("storage/wal.zig");
};
pub const query = struct {
    pub const planner = @import("query/planner.zig");
    pub const executor = @import("query/executor.zig");
    pub const result = @import("query/result.zig");
};
pub const transaction = struct {
    pub const manager = @import("transaction/manager.zig");
};

// Import the tests
pub const tests = struct {
    pub const core_database = @import("tests/core/database_test.zig");
    pub const storage_rocksdb = @import("tests/storage/rocksdb_test.zig");
    pub const storage_wal = @import("tests/storage/wal_test.zig");
    pub const query_planner = @import("tests/query/planner_test.zig");
    pub const query_parser = @import("tests/query/parser_test.zig");
    pub const query_executor = @import("tests/query/executor_test.zig");
    pub const query_result = @import("tests/query/result_test.zig");
    pub const transaction_manager = @import("tests/transaction/manager_test.zig");
    pub const replication_vr = @import("tests/replication/vr_test.zig");
};

test {
    std.testing.refAllDeclsRecursive(@This());
}
