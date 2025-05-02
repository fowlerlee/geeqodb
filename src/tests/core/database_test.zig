const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const database = geeqodb.core;
const OLAPDatabase = database.OLAPDatabase;

test "Database initialization" {
    const allocator = testing.allocator;

    // Initialize the database
    const db = try database.init(allocator, "test_data");
    defer db.deinit();

    // Verify that the database was initialized correctly
    try testing.expect(@intFromPtr(db.storage) != 0);
    try testing.expect(@intFromPtr(db.wal) != 0);
    try testing.expect(@intFromPtr(db.query_planner) != 0);
    try testing.expect(@intFromPtr(db.txn_manager) != 0);
}

test "Database execute query" {
    const allocator = testing.allocator;

    // Initialize the database
    const db = try database.init(allocator, "test_data");
    defer db.deinit();

    // Execute a simple query
    var result_set = try db.execute("SELECT * FROM test");
    defer result_set.deinit();

    // Verify the result set
    try testing.expectEqual(@as(usize, 0), result_set.columns.len);
    try testing.expectEqual(@as(usize, 0), result_set.row_count);
}

test "Database initialization with empty data directory" {
    const allocator = testing.allocator;

    // Initialize the database with an empty data directory
    const db = try database.init(allocator, "");
    defer db.deinit();

    // Verify that the database was initialized correctly
    try testing.expect(@intFromPtr(db.storage) != 0);
    try testing.expect(@intFromPtr(db.wal) != 0);
    try testing.expect(@intFromPtr(db.query_planner) != 0);
    try testing.expect(@intFromPtr(db.txn_manager) != 0);
}

test "Database initialization with null-terminated data directory" {
    const allocator = testing.allocator;

    // Initialize the database with a null-terminated data directory
    const db = try database.init(allocator, "test_data\x00");
    defer db.deinit();

    // Verify that the database was initialized correctly
    try testing.expect(@intFromPtr(db.storage) != 0);
    try testing.expect(@intFromPtr(db.wal) != 0);
    try testing.expect(@intFromPtr(db.query_planner) != 0);
    try testing.expect(@intFromPtr(db.txn_manager) != 0);
}
