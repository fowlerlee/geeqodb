const std = @import("std");
const RocksDB = @import("../storage/rocksdb.zig").RocksDB;
const WAL = @import("../storage/wal.zig").WAL;
const QueryPlanner = @import("../query/planner.zig").QueryPlanner;
const QueryExecutor = @import("../query/executor.zig").QueryExecutor;
const TransactionManager = @import("../transaction/manager.zig").TransactionManager;
const ResultSet = @import("../query/result.zig").ResultSet;
const assert = @import("../build_options.zig").assert;

/// OLAP Database main structure
pub const OLAPDatabase = struct {
    allocator: std.mem.Allocator,
    storage: *RocksDB,
    wal: *WAL,
    query_planner: *QueryPlanner,
    txn_manager: *TransactionManager,

    /// Execute a SQL query and return a result set
    pub fn execute(self: *OLAPDatabase, query: []const u8) !ResultSet {
        // Validate inputs
        assert(query.len > 0); // Query should not be empty

        const ast = try self.query_planner.parse(query);
        defer ast.deinit();

        const plan = try self.query_planner.plan(ast);
        defer plan.deinit();

        const physical_plan = try self.query_planner.optimize(plan);
        defer physical_plan.deinit();

        return try QueryExecutor.execute(self.allocator, physical_plan);
    }

    /// Deinitialize the database
    pub fn deinit(self: *OLAPDatabase) void {
        self.storage.deinit();
        self.wal.deinit();
        self.query_planner.deinit();
        self.txn_manager.deinit();
        self.allocator.destroy(self);
    }
};

/// Initialize a new OLAP database
pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !*OLAPDatabase {
    var db = try allocator.create(OLAPDatabase);
    db.allocator = allocator;
    db.storage = try RocksDB.init(allocator, data_dir);
    db.wal = try WAL.init(allocator, data_dir);
    db.query_planner = try QueryPlanner.init(allocator);
    db.txn_manager = try TransactionManager.init(allocator);

    return db;
}

test "OLAPDatabase initialization" {
    const allocator = std.testing.allocator;
    const db = try init(allocator, "test_data");
    defer db.deinit();

    // Test that the database was initialized correctly
    try std.testing.expect(@intFromPtr(db.storage) != 0);
    try std.testing.expect(@intFromPtr(db.wal) != 0);
    try std.testing.expect(@intFromPtr(db.query_planner) != 0);
    try std.testing.expect(@intFromPtr(db.txn_manager) != 0);
}
