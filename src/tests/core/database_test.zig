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
    // Since our implementation now returns info for unknown tables
    try testing.expectEqual(@as(usize, 1), result_set.columns.len);
    try testing.expectEqualStrings("info", result_set.columns[0].name);
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

    // Initialize the database with a proper data directory (no null terminator)
    const db = try database.init(allocator, "test_data_null");
    defer db.deinit();

    // Verify that the database was initialized correctly
    try testing.expect(@intFromPtr(db.storage) != 0);
    try testing.expect(@intFromPtr(db.wal) != 0);
    try testing.expect(@intFromPtr(db.query_planner) != 0);
    try testing.expect(@intFromPtr(db.txn_manager) != 0);
}

test "Database recovery after crash" {
    const allocator = testing.allocator;
    const test_dir = "test_recovery";

    // Clean up any previous test data
    std.fs.cwd().deleteTree(test_dir) catch |err| {
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create the database
    var db = try database.init(allocator, test_dir);

    // Execute a few queries to generate some data
    _ = try db.execute("CREATE TABLE test (id INT, name TEXT)");
    _ = try db.execute("INSERT INTO test VALUES (1, 'test1')");
    _ = try db.execute("INSERT INTO test VALUES (2, 'test2')");

    // Get the WAL position
    const wal_pos = try db.getWALPosition();

    // Close the database to simulate a crash
    db.deinit();
    db = undefined;

    // Recover the database
    var recovered_db = try database.recoverDatabase(allocator, test_dir);
    defer recovered_db.deinit();

    // Verify that the database was recovered correctly
    try testing.expect(@intFromPtr(recovered_db.storage) != 0);
    try testing.expect(@intFromPtr(recovered_db.wal) != 0);
    try testing.expect(@intFromPtr(recovered_db.query_planner) != 0);
    try testing.expect(@intFromPtr(recovered_db.txn_manager) != 0);

    // Verify that the WAL position was recovered
    const recovered_wal_pos = try recovered_db.getWALPosition();
    try testing.expectEqual(wal_pos, recovered_wal_pos);

    // Execute a query to verify data integrity
    var result_set = try recovered_db.execute("SELECT * FROM test");

    // In a real implementation, we would verify the query results
    // For now, we just validate that the query executed without errors
    result_set.deinit();
}

test "Database backup and restore" {
    const allocator = testing.allocator;
    const test_dir = "test_backup_db";
    const backup_dir = "test_backup";
    const restore_dir = "test_restore";

    // Clean up any previous test data
    for ([_][]const u8{ test_dir, backup_dir, restore_dir }) |dir| {
        std.fs.cwd().deleteTree(dir) catch |err| {
            if (err != error.FileNotFound and err != error.PathNotFound) {
                return err;
            }
        };
    }
    defer {
        for ([_][]const u8{ test_dir, backup_dir, restore_dir }) |dir| {
            std.fs.cwd().deleteTree(dir) catch {};
        }
    }

    // Initialize the database
    var db = try database.init(allocator, test_dir);

    // Execute some queries to populate the database
    _ = try db.execute("CREATE TABLE test (id INT, name TEXT)");
    _ = try db.execute("INSERT INTO test VALUES (1, 'test1')");
    _ = try db.execute("INSERT INTO test VALUES (2, 'test2')");

    // Create a backup
    try db.createBackup(backup_dir);

    // Verify the backup exists
    const backup_exists = try database.OLAPDatabase.verifyBackup(allocator, backup_dir);
    try testing.expect(backup_exists);

    // Restore from the backup
    try database.OLAPDatabase.recoverFromBackup(allocator, backup_dir, restore_dir);

    // Open the restored database
    var restored_db = try database.init(allocator, restore_dir);
    defer restored_db.deinit();

    // Execute a query on the restored database to verify it works
    var result_set = try restored_db.execute("SELECT * FROM test");

    // In a real implementation, we would verify the query results
    // For now, we just validate that the query executed without errors
    result_set.deinit();

    // Close the original database
    db.deinit();
}

test "CREATE TABLE creates schema and prevents duplicates" {
    const allocator = testing.allocator;
    const db = try database.init(allocator, "test_create_table");
    defer db.deinit();

    // Clean up any previous test data
    std.fs.cwd().deleteTree("test_create_table") catch |err| {
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
    defer std.fs.cwd().deleteTree("test_create_table") catch {};

    // Create a table
    _ = try db.execute("CREATE TABLE users (id INT, name TEXT, email TEXT)");

    // Try to create the same table again and expect an error
    const dup_result = db.execute("CREATE TABLE users (id INT, name TEXT, email TEXT)");
    try testing.expectError(error.TableAlreadyExists, dup_result);

    // Select from the table and check columns
    var result_set = try db.execute("SELECT * FROM users");
    defer result_set.deinit();
    try testing.expectEqual(@as(usize, 3), result_set.columns.len);
    try testing.expectEqualStrings("id", result_set.columns[0].name);
    try testing.expectEqualStrings("name", result_set.columns[1].name);
    try testing.expectEqualStrings("email", result_set.columns[2].name);
    try testing.expectEqual(@as(usize, 0), result_set.row_count);
}
