const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const database = geeqodb.core;
const OLAPDatabase = database.OLAPDatabase;
const BTreeMapIndex = geeqodb.storage.btree_index.BTreeMapIndex;
const SkipListIndex = geeqodb.storage.skiplist_index.SkipListIndex;
const DatabaseContext = geeqodb.query.executor.DatabaseContext;

/// Helper function to create a test database with a large amount of data
fn createTestDatabase(allocator: std.mem.Allocator, data_dir: []const u8, num_rows: usize) !*OLAPDatabase {
    // Initialize the database
    const db = try database.init(allocator, data_dir);

    // Create a test table
    _ = try db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)");

    // Insert test data
    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        const query = try std.fmt.allocPrint(allocator, "INSERT INTO test_table VALUES ({d}, 'Test {d}', {d})", .{ i, i, i * 10 });
        defer allocator.free(query);
        _ = try db.execute(query);
    }

    return db;
}

test "Query performance with and without index" {
    const allocator = testing.allocator;
    const data_dir = "test_data_performance";
    const num_rows = 10000;

    // Create a test database with a large amount of data
    const db = try createTestDatabase(allocator, data_dir, num_rows);
    defer db.deinit();

    // Run the query multiple times to get a more accurate measurement
    const num_iterations = 10;
    var total_time_without_index: u64 = 0;
    var total_time_with_index: u64 = 0;

    // Measure query time without index
    var timer = try std.time.Timer.start();

    var i: usize = 0;
    while (i < num_iterations) : (i += 1) {
        // Execute a query that would benefit from an index
        const query = "SELECT * FROM test_table WHERE value = 5000";
        var result_set = try db.execute(query);
        result_set.deinit();
    }

    total_time_without_index = timer.read();
    const avg_time_without_index = total_time_without_index / num_iterations;

    // Create and register an index
    var index = try BTreeMapIndex.create(allocator, "idx_test_value", "test_table", "value");
    defer index.deinit();

    // Populate the index with data from the table
    i = 0;
    while (i < num_rows) : (i += 1) {
        try index.insert(@intCast(i * 10), @intCast(i)); // value -> row_id
    }

    // Register the index with the database context
    try db.db_context.registerBTreeIndex("idx_test_value", index);

    // Register the index with the query planner
    try db.query_planner.registerIndex("idx_test_value", "test_table", "value", .BTree, num_rows, num_rows);

    // Measure query time with index
    timer.reset();

    i = 0;
    while (i < num_iterations) : (i += 1) {
        // Execute the same query, now with an index
        const query = "SELECT * FROM test_table WHERE value = 5000";
        var result_set = try db.execute(query);
        result_set.deinit();
    }

    total_time_with_index = timer.read();
    const avg_time_with_index = total_time_with_index / num_iterations;

    // Print the results
    std.debug.print("\nQuery performance test results:\n", .{});
    std.debug.print("Average time without index: {d} ns\n", .{avg_time_without_index});
    std.debug.print("Average time with index: {d} ns\n", .{avg_time_with_index});
    std.debug.print("Speedup factor: {d:.2}x\n", .{@as(f64, @floatFromInt(avg_time_without_index)) / @as(f64, @floatFromInt(avg_time_with_index))});

    // Verify that the index provides a significant speedup
    try testing.expect(avg_time_with_index < avg_time_without_index);
}

test "Range query performance with and without index" {
    const allocator = testing.allocator;
    const data_dir = "test_data_range_performance";
    const num_rows = 10000;

    // Create a test database with a large amount of data
    const db = try createTestDatabase(allocator, data_dir, num_rows);
    defer db.deinit();

    // Run the query multiple times to get a more accurate measurement
    const num_iterations = 10;
    var total_time_without_index: u64 = 0;
    var total_time_with_index: u64 = 0;

    // Measure query time without index
    var timer = try std.time.Timer.start();

    var i: usize = 0;
    while (i < num_iterations) : (i += 1) {
        // Execute a range query that would benefit from an index
        const query = "SELECT * FROM test_table WHERE value BETWEEN 4000 AND 6000";
        var result_set = try db.execute(query);
        result_set.deinit();
    }

    total_time_without_index = timer.read();
    const avg_time_without_index = total_time_without_index / num_iterations;

    // Create and register an index
    var index = try BTreeMapIndex.create(allocator, "idx_test_value", "test_table", "value");
    defer index.deinit();

    // Populate the index with data from the table
    i = 0;
    while (i < num_rows) : (i += 1) {
        try index.insert(@intCast(i * 10), @intCast(i)); // value -> row_id
    }

    // Register the index with the database context
    try db.db_context.registerBTreeIndex("idx_test_value", index);

    // Register the index with the query planner
    try db.query_planner.registerIndex("idx_test_value", "test_table", "value", .BTree, num_rows, num_rows);

    // Measure query time with index
    timer.reset();

    i = 0;
    while (i < num_iterations) : (i += 1) {
        // Execute the same query, now with an index
        const query = "SELECT * FROM test_table WHERE value BETWEEN 4000 AND 6000";
        var result_set = try db.execute(query);
        result_set.deinit();
    }

    total_time_with_index = timer.read();
    const avg_time_with_index = total_time_with_index / num_iterations;

    // Print the results
    std.debug.print("\nRange query performance test results:\n", .{});
    std.debug.print("Average time without index: {d} ns\n", .{avg_time_without_index});
    std.debug.print("Average time with index: {d} ns\n", .{avg_time_with_index});
    std.debug.print("Speedup factor: {d:.2}x\n", .{@as(f64, @floatFromInt(avg_time_without_index)) / @as(f64, @floatFromInt(avg_time_with_index))});

    // Verify that the index provides a significant speedup
    try testing.expect(avg_time_with_index < avg_time_without_index);
}

test "Join query performance with and without index" {
    const allocator = testing.allocator;
    const data_dir = "test_data_join_performance";
    const num_rows = 1000; // Using fewer rows for join to avoid excessive test time

    // Initialize the database
    const db = try database.init(allocator, data_dir);
    defer db.deinit();

    // Create test tables
    _ = try db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    _ = try db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)");

    // Insert test data
    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        const user_query = try std.fmt.allocPrint(allocator, "INSERT INTO users VALUES ({d}, 'User {d}')", .{ i, i });
        defer allocator.free(user_query);
        _ = try db.execute(user_query);

        // Each user has multiple orders
        var j: usize = 0;
        while (j < 5) : (j += 1) {
            const order_id = i * 5 + j;
            const order_query = try std.fmt.allocPrint(allocator, "INSERT INTO orders VALUES ({d}, {d}, {d})", .{ order_id, i, order_id * 10 });
            defer allocator.free(order_query);
            _ = try db.execute(order_query);
        }
    }

    // Run the query multiple times to get a more accurate measurement
    const num_iterations = 5;
    var total_time_without_index: u64 = 0;
    var total_time_with_index: u64 = 0;

    // Measure join query time without index
    var timer = try std.time.Timer.start();

    i = 0;
    while (i < num_iterations) : (i += 1) {
        // Execute a join query that would benefit from an index
        const query = "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 500";
        var result_set = try db.execute(query);
        result_set.deinit();
    }

    total_time_without_index = timer.read();
    const avg_time_without_index = total_time_without_index / num_iterations;

    // Create and register indexes
    const user_index = try BTreeMapIndex.create(allocator, "idx_users_id", "users", "id");
    defer user_index.deinit();

    const order_index = try BTreeMapIndex.create(allocator, "idx_orders_user_id", "orders", "user_id");
    defer order_index.deinit();

    // Populate the indexes
    i = 0;
    while (i < num_rows) : (i += 1) {
        try user_index.insert(@intCast(i), @intCast(i)); // id -> row_id

        var j: usize = 0;
        while (j < 5) : (j += 1) {
            const order_id = i * 5 + j;
            try order_index.insert(@intCast(i), @intCast(order_id)); // user_id -> row_id
        }
    }

    // Register the indexes with the database context
    try db.db_context.registerBTreeIndex("idx_users_id", user_index);
    try db.db_context.registerBTreeIndex("idx_orders_user_id", order_index);

    // Register the indexes with the query planner
    try db.query_planner.registerIndex("idx_users_id", "users", "id", .BTree, num_rows, num_rows);
    try db.query_planner.registerIndex("idx_orders_user_id", "orders", "user_id", .BTree, num_rows * 5, num_rows);

    // Measure query time with index
    timer.reset();

    i = 0;
    while (i < num_iterations) : (i += 1) {
        // Execute the same query, now with indexes
        const query = "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = 500";
        var result_set = try db.execute(query);
        result_set.deinit();
    }

    total_time_with_index = timer.read();
    const avg_time_with_index = total_time_with_index / num_iterations;

    // Print the results
    std.debug.print("\nJoin query performance test results:\n", .{});
    std.debug.print("Average time without index: {d} ns\n", .{avg_time_without_index});
    std.debug.print("Average time with index: {d} ns\n", .{avg_time_with_index});
    std.debug.print("Speedup factor: {d:.2}x\n", .{@as(f64, @floatFromInt(avg_time_without_index)) / @as(f64, @floatFromInt(avg_time_with_index))});

    // Verify that the index provides a significant speedup
    try testing.expect(avg_time_with_index < avg_time_without_index);
}
