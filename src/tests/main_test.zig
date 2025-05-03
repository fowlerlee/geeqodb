const std = @import("std");

test "Core Database Tests" {
    _ = @import("core/database_test.zig");
}

test "Storage RocksDB Tests" {
    _ = @import("storage/rocksdb_test.zig");
}

test "Storage WAL Tests" {
    _ = @import("storage/wal_test.zig");
}

test "Storage Index Tests" {
    _ = @import("storage/index_test.zig");
}

test "Query Planner Tests" {
    _ = @import("query/planner_test.zig");
}

test "SQL Parser Tests" {
    _ = @import("query/parser_test.zig");
}

test "Query Executor Tests" {
    _ = @import("query/executor_test.zig");
}

test "Query Result Tests" {
    _ = @import("query/result_test.zig");
}

test "Query Index Tests" {
    _ = @import("query/index_query_test.zig");
}

test "Transaction Manager Tests" {
    _ = @import("transaction/manager_test.zig");
}

test "Transaction Isolation Tests" {
    _ = @import("transaction/isolation_test.zig");
}

test "Transaction Concurrency Control Tests" {
    _ = @import("transaction/concurrency_control_test.zig");
}

test "Server Tests" {
    _ = @import("server/server_test.zig");
}

test "Replication Tests" {
    _ = @import("replication/vr_test.zig");
}

test "Backup and Recovery Tests" {
    _ = @import("storage/backup_recovery_test.zig");
}

test "Integration Tests - Index Performance" {
    _ = @import("integration/index_performance_test.zig");
}
