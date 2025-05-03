const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const database = geeqodb.core;
const OLAPDatabase = database.OLAPDatabase;
const BTreeMapIndex = geeqodb.storage.btree_index.BTreeMapIndex;
const SkipListIndex = geeqodb.storage.skiplist_index.SkipListIndex;
const DatabaseContext = geeqodb.query.executor.DatabaseContext;

// Helper function to create a test database with some data
fn createTestDatabase(allocator: std.mem.Allocator, data_dir: []const u8) !*OLAPDatabase {
    // Initialize the database
    const db = try database.init(allocator, data_dir);

    // Create a test table
    _ = try db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

    // Insert some test data
    _ = try db.execute("INSERT INTO test_table VALUES (1, 'Test 1', 100)");
    _ = try db.execute("INSERT INTO test_table VALUES (2, 'Test 2', 200)");
    _ = try db.execute("INSERT INTO test_table VALUES (3, 'Test 3', 300)");
    _ = try db.execute("INSERT INTO test_table VALUES (4, 'Test 4', 400)");
    _ = try db.execute("INSERT INTO test_table VALUES (5, 'Test 5', 500)");

    return db;
}

test "Create and use BTreeMap index" {
    const allocator = testing.allocator;
    const data_dir = "test_data_btree";

    // Create a test database
    const db = try createTestDatabase(allocator, data_dir);
    defer db.deinit();

    // Create a BTreeMap index on the test table
    const index = try BTreeMapIndex.create(allocator, "idx_test_value", "test_table", "value");
    defer index.deinit();

    // Populate the index with data from the table
    // In a real implementation, this would be done by the database engine
    try index.insert(100, 1); // value -> row_id
    try index.insert(200, 2);
    try index.insert(300, 3);
    try index.insert(400, 4);
    try index.insert(500, 5);

    // Verify the index contains the expected entries
    try testing.expectEqual(@as(usize, 5), index.count());
    try testing.expectEqual(@as(u64, 1), index.get(100).?);
    try testing.expectEqual(@as(u64, 3), index.get(300).?);
    try testing.expectEqual(@as(u64, 5), index.get(500).?);
    try testing.expectEqual(@as(?u64, null), index.get(600));

    // Simulate a query using the index
    // "SELECT id FROM test_table WHERE value = 300"
    const row_id = index.get(300) orelse return error.RowNotFound;
    try testing.expectEqual(@as(u64, 3), row_id);

    // Simulate updating a row and updating the index
    // "UPDATE test_table SET value = 350 WHERE id = 3"
    _ = index.remove(300); // Remove the old value
    try index.insert(350, 3); // Insert the new value

    // Verify the index was updated correctly
    try testing.expectEqual(@as(?u64, null), index.get(300));
    try testing.expectEqual(@as(u64, 3), index.get(350).?);
}

test "Create and use SkipList index" {
    const allocator = testing.allocator;
    const data_dir = "test_data_skiplist";

    // Create a test database
    const db = try createTestDatabase(allocator, data_dir);
    defer db.deinit();

    // Create a SkipList index on the test table
    const index = try SkipListIndex.create(allocator, "idx_test_value", "test_table", "value");
    defer index.deinit();

    // Populate the index with data from the table
    // In a real implementation, this would be done by the database engine
    try index.insert(100, 1); // value -> row_id
    try index.insert(200, 2);
    try index.insert(300, 3);
    try index.insert(400, 4);
    try index.insert(500, 5);

    // Verify the index contains the expected entries
    try testing.expectEqual(@as(usize, 5), index.count());
    try testing.expectEqual(@as(u64, 1), index.get(100).?);
    try testing.expectEqual(@as(u64, 3), index.get(300).?);
    try testing.expectEqual(@as(u64, 5), index.get(500).?);
    try testing.expectEqual(@as(?u64, null), index.get(600));

    // Simulate a query using the index
    // "SELECT id FROM test_table WHERE value = 300"
    const row_id = index.get(300) orelse return error.RowNotFound;
    try testing.expectEqual(@as(u64, 3), row_id);

    // Simulate updating a row and updating the index
    // "UPDATE test_table SET value = 350 WHERE id = 3"
    _ = index.remove(300); // Remove the old value
    try index.insert(350, 3); // Insert the new value

    // Verify the index was updated correctly
    try testing.expectEqual(@as(?u64, null), index.get(300));
    try testing.expectEqual(@as(u64, 3), index.get(350).?);
}

test "Index performance for range queries" {
    const allocator = testing.allocator;

    // Create indexes
    const btree_index = try BTreeMapIndex.create(allocator, "btree_index", "test_table", "test_column");
    defer btree_index.deinit();

    const skiplist_index = try SkipListIndex.create(allocator, "skiplist_index", "test_table", "test_column");
    defer skiplist_index.deinit();

    // Insert a large number of entries
    const num_entries = 1000;
    var i: i64 = 0;
    while (i < num_entries) : (i += 1) {
        try btree_index.insert(i, @as(u64, @intCast(i)));
        try skiplist_index.insert(i, @as(u64, @intCast(i)));
    }

    // Simulate a range query
    // "SELECT * FROM test_table WHERE test_column BETWEEN 300 AND 399"
    var count_btree: usize = 0;
    var count_skiplist: usize = 0;

    i = 300;
    while (i < 400) : (i += 1) {
        if (btree_index.get(i) != null) {
            count_btree += 1;
        }
        if (skiplist_index.get(i) != null) {
            count_skiplist += 1;
        }
    }

    try testing.expectEqual(@as(usize, 100), count_btree);
    try testing.expectEqual(@as(usize, 100), count_skiplist);
}
