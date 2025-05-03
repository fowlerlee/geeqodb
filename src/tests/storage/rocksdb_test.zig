const std = @import("std");
const testing = std.testing;
const fs = std.fs;
const geeqodb = @import("geeqodb");
const RocksDB = geeqodb.RocksDB;

// Helper function to clean up test directories
fn cleanupTestDir(dir_path: []const u8) !void {
    // Try to delete the directory and all its contents
    fs.cwd().deleteTree(dir_path) catch |err| {
        // Ignore errors if the directory doesn't exist
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    };
}

test "RocksDB initialization" {
    const allocator = testing.allocator;
    const test_dir = "test_init";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Verify that RocksDB was initialized correctly
    try testing.expect(db.is_open);
    try testing.expectEqualStrings(test_dir, db.data_dir);

    // Check that the options are set
    try testing.expect(db.options != null);
    try testing.expect(db.write_options != null);
    try testing.expect(db.read_options != null);
}

test "RocksDB put and get" {
    const allocator = testing.allocator;
    const test_dir = "test_put_get";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Put a key-value pair
    try db.put("test_key", "test_value");

    // Get the value
    const value = try db.get(allocator, "test_key");
    defer if (value) |v| allocator.free(v);

    // Verify the value
    try testing.expect(value != null);
    if (value) |v| {
        try testing.expectEqualStrings("test_value", v);
    }
}

test "RocksDB delete" {
    const allocator = testing.allocator;
    const test_dir = "test_delete";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Put a key-value pair
    try db.put("test_key", "test_value");

    // Delete the key-value pair
    try db.delete("test_key");

    // Get the value
    const value = try db.get(allocator, "test_key");
    defer if (value) |v| allocator.free(v);

    // Verify the key was deleted
    try testing.expectEqual(@as(?[]const u8, null), value);
}

test "RocksDB iterator" {
    const allocator = testing.allocator;
    const test_dir = "test_iterator";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Add some data
    try db.put("key1", "value1");
    try db.put("key2", "value2");
    try db.put("key3", "value3");

    // Create an iterator
    const iter = try db.iterator();
    defer iter.deinit();

    // Seek to the first key
    iter.seekToFirst();
    try testing.expect(iter.isValid());

    // Check the first key
    const key1 = try iter.key();
    const value1 = try iter.value();
    try testing.expectEqualStrings("key1", key1);
    try testing.expectEqualStrings("value1", value1);

    // Move to the next key
    iter.next();
    try testing.expect(iter.isValid());

    // Check the second key
    const key2 = try iter.key();
    const value2 = try iter.value();
    try testing.expectEqualStrings("key2", key2);
    try testing.expectEqualStrings("value2", value2);

    // Seek to a specific key
    iter.seek("key3");
    try testing.expect(iter.isValid());

    // Check the third key
    const key3 = try iter.key();
    const value3 = try iter.value();
    try testing.expectEqualStrings("key3", key3);
    try testing.expectEqualStrings("value3", value3);

    // Seek to a non-existent key
    iter.seek("key4");
    try testing.expect(!iter.isValid());
}

test "RocksDB close and reopen" {
    const allocator = testing.allocator;
    const test_dir = "test_close_reopen";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, test_dir);

    // Close the database
    db.close();
    try testing.expect(!db.is_open);

    // Try to put a key-value pair
    try testing.expectError(error.DatabaseClosed, db.put("test_key", "test_value"));

    // Try to get a value
    try testing.expectError(error.DatabaseClosed, db.get(allocator, "test_key"));

    // Try to delete a key-value pair
    try testing.expectError(error.DatabaseClosed, db.delete("test_key"));

    // Try to create an iterator
    try testing.expectError(error.DatabaseClosed, db.iterator());

    // Reopen the database
    try db.open();
    try testing.expect(db.is_open);

    // Clean up
    db.deinit();
    try cleanupTestDir(test_dir);
}

test "RocksDB batch operations" {
    const allocator = testing.allocator;
    const test_dir = "test_batch";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Create a write batch
    const batch = try db.createWriteBatch();
    defer batch.deinit();

    // Add operations to the batch
    try batch.put("key1", "value1");
    try batch.put("key2", "value2");
    try batch.put("key3", "value3");
    try batch.delete("key3"); // This key should not appear in the final state

    // Commit the batch
    try batch.commit();

    // Verify the results
    const value1 = try db.get(allocator, "key1");
    defer if (value1) |v| allocator.free(v);
    try testing.expect(value1 != null);
    if (value1) |v| {
        try testing.expectEqualStrings("value1", v);
    }

    const value2 = try db.get(allocator, "key2");
    defer if (value2) |v| allocator.free(v);
    try testing.expect(value2 != null);
    if (value2) |v| {
        try testing.expectEqualStrings("value2", v);
    }

    const value3 = try db.get(allocator, "key3");
    defer if (value3) |v| allocator.free(v);
    try testing.expectEqual(@as(?[]const u8, null), value3); // Should be deleted
}

test "RocksDB backup and restore" {
    const allocator = testing.allocator;
    const test_dir = "test_backup_db";
    const backup_dir = "test_backup_dir";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);
    try cleanupTestDir(backup_dir);

    // Initialize RocksDB
    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
        cleanupTestDir(backup_dir) catch {};
    }

    // Add some data
    try db.put("backup_key1", "backup_value1");
    try db.put("backup_key2", "backup_value2");

    // Create a backup
    try db.createBackup(backup_dir);

    // Add more data after backup
    try db.put("backup_key3", "backup_value3");

    // Verify all keys are present
    {
        const value1 = try db.get(allocator, "backup_key1");
        defer if (value1) |v| allocator.free(v);
        try testing.expect(value1 != null);
        if (value1) |v| {
            try testing.expectEqualStrings("backup_value1", v);
        }

        const value2 = try db.get(allocator, "backup_key2");
        defer if (value2) |v| allocator.free(v);
        try testing.expect(value2 != null);
        if (value2) |v| {
            try testing.expectEqualStrings("backup_value2", v);
        }

        const value3 = try db.get(allocator, "backup_key3");
        defer if (value3) |v| allocator.free(v);
        try testing.expect(value3 != null);
        if (value3) |v| {
            try testing.expectEqualStrings("backup_value3", v);
        }
    }

    // We need to close and reopen the database to test restore
    db.close();
    try db.open();

    // Restore from backup
    try db.restoreFromBackup(backup_dir);

    // Verify keys from backup are present, but not the one added after backup
    {
        const value1 = try db.get(allocator, "backup_key1");
        defer if (value1) |v| allocator.free(v);
        try testing.expect(value1 != null);
        if (value1) |v| {
            try testing.expectEqualStrings("backup_value1", v);
        }

        const value2 = try db.get(allocator, "backup_key2");
        defer if (value2) |v| allocator.free(v);
        try testing.expect(value2 != null);
        if (value2) |v| {
            try testing.expectEqualStrings("backup_value2", v);
        }

        const value3 = try db.get(allocator, "backup_key3");
        defer if (value3) |v| allocator.free(v);
        try testing.expectEqual(@as(?[]const u8, null), value3); // Should not be present after restore
    }
}

test "RocksDB basic operations" {
    const allocator = testing.allocator;
    const test_dir = "test_basic_ops";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Add data to the default column family
    try db.put("basic_key", "basic_value");

    // Verify data in default column family
    const value = try db.get(allocator, "basic_key");
    defer if (value) |v| allocator.free(v);
    try testing.expect(value != null);
    if (value) |v| {
        try testing.expectEqualStrings("basic_value", v);
    }

    // Close and reopen the database to verify data persists
    db.close();
    try db.open();

    // Verify data still exists
    const value2 = try db.get(allocator, "basic_key");
    defer if (value2) |v| allocator.free(v);
    try testing.expect(value2 != null);
    if (value2) |v| {
        try testing.expectEqualStrings("basic_value", v);
    }
}

test "RocksDB basic options" {
    const allocator = testing.allocator;
    const test_dir = "test_basic_options";

    // Clean up any previous test data
    try cleanupTestDir(test_dir);

    // Initialize RocksDB
    var db = try RocksDB.init(allocator, test_dir);
    defer {
        db.deinit();
        cleanupTestDir(test_dir) catch {};
    }

    // Add some data to verify the database works
    try db.put("options_key", "options_value");

    // Verify data
    const value = try db.get(allocator, "options_key");
    defer if (value) |v| allocator.free(v);
    try testing.expect(value != null);
    if (value) |v| {
        try testing.expectEqualStrings("options_value", v);
    }
}
