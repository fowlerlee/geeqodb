const std = @import("std");

// Re-export the modules for testing
pub const core = @import("core/database.zig");
pub const storage = struct {
    pub const rocksdb = @import("storage/rocksdb.zig");
    pub const wal = @import("storage/wal.zig");
    pub const index = @import("storage/index.zig");
    pub const btree_index = @import("storage/btree_index.zig");
    pub const skiplist_index = @import("storage/skiplist_index.zig");
    pub const distributed_wal = @import("storage/distributed_wal.zig");
};
pub const query = struct {
    pub const planner = @import("query/planner.zig");
    pub const executor = @import("query/executor.zig");
    pub const result = @import("query/result.zig");
};
pub const transaction = struct {
    pub const manager = @import("transaction/manager.zig");
};
pub const replication = struct {
    pub const replica_management = @import("simulation/scenarios/replica_management.zig");
    pub const view_change_protocol = @import("simulation/scenarios/view_change_protocol.zig");
};

// Import the tests
pub const tests = struct {
    pub const core_database = @import("tests/core/database_test.zig");
    pub const storage_rocksdb = @import("tests/storage/rocksdb_test.zig");
    pub const storage_wal = @import("tests/storage/wal_test.zig");
    pub const storage_index = @import("tests/storage/index_test.zig");
    pub const query_planner = @import("tests/query/planner_test.zig");
    pub const query_parser = @import("tests/query/parser_test.zig");
    pub const query_executor = @import("tests/query/executor_test.zig");
    pub const query_result = @import("tests/query/result_test.zig");
    pub const query_index = @import("tests/query/index_query_test.zig");
    pub const transaction_manager = @import("tests/transaction/manager_test.zig");
    pub const replication_vr = @import("tests/replication/vr_test.zig");
    pub const replication_replica_management = @import("tests/replication/replica_management_test.zig");
    pub const replication_view_change = @import("tests/replication/view_change_test.zig");
    pub const replication_distributed_log = @import("tests/replication/distributed_log_test.zig");
};

test {
    std.testing.refAllDeclsRecursive(@This());
}
