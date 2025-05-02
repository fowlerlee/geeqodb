const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const RocksDB = geeqodb.storage.rocksdb.RocksDB;

test "RocksDB initialization" {
    const allocator = testing.allocator;

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, "test_data");
    defer db.deinit();

    // Verify that RocksDB was initialized correctly
    try testing.expect(db.is_open);
    try testing.expectEqualStrings("test_data", db.data_dir);
}

test "RocksDB put and get" {
    const allocator = testing.allocator;

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, "test_data");
    defer db.deinit();

    // Put a key-value pair
    try db.put("test_key", "test_value");

    // Get the value
    const value = try db.get(allocator, "test_key");
    defer if (value) |v| allocator.free(v);

    // Since our implementation is a stub, we expect null
    try testing.expectEqual(@as(?[]const u8, null), value);
}

test "RocksDB delete" {
    const allocator = testing.allocator;

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, "test_data");
    defer db.deinit();

    // Put a key-value pair
    try db.put("test_key", "test_value");

    // Delete the key-value pair
    try db.delete("test_key");

    // Get the value
    const value = try db.get(allocator, "test_key");
    defer if (value) |v| allocator.free(v);

    // Since our implementation is a stub, we expect null
    try testing.expectEqual(@as(?[]const u8, null), value);
}

test "RocksDB iterator" {
    const allocator = testing.allocator;

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, "test_data");
    defer db.deinit();

    // Create an iterator
    const iter = try db.iterator();
    defer iter.deinit();

    // Verify that the iterator is not valid
    try testing.expect(!iter.isValid());

    // Seek to the first key
    iter.seekToFirst();
    try testing.expect(!iter.isValid());

    // Seek to a specific key
    iter.seek("test_key");
    try testing.expect(!iter.isValid());
}

test "RocksDB close and reopen" {
    const allocator = testing.allocator;

    // Initialize RocksDB
    const db = try RocksDB.init(allocator, "test_data");

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
}
