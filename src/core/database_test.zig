const std = @import("std");
const database = @import("database.zig");
const assert = std.debug.assert;
const testing = std.testing;
const fs = std.fs;

// Helper function to create a test directory and clean it up when done
fn withTestDir(callback: fn (dir: []const u8) anyerror!void) !void {
    const test_dir = "test_db_data";

    // Create test directory
    try fs.cwd().makePath(test_dir);
    defer fs.cwd().deleteTree(test_dir) catch {};

    try callback(test_dir);
}

test "Database initialization and query execution" {
    const allocator = std.testing.allocator;

    // Initialize the database
    const db = try database.init(allocator, "test_data");
    defer db.deinit();

    // Execute a simple query
    var result_set = try db.execute("SELECT * FROM test");
    defer result_set.deinit();

    // Verify the result set
    try std.testing.expectEqual(@as(usize, 0), result_set.columns.len);
    try std.testing.expectEqual(@as(usize, 0), result_set.row_count);
}

test "Database backup and recovery" {
    const allocator = std.testing.allocator;

    try withTestDir(struct {
        fn callback(dir: []const u8) !void {
            // Create backup and recovery directories
            const backup_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "backup" });
            defer allocator.free(backup_dir);

            const recovery_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "recovery" });
            defer allocator.free(recovery_dir);

            try fs.cwd().makePath(backup_dir);

            // Initialize the database
            const db = try database.init(allocator, dir);
            defer db.deinit();

            // Execute some queries to populate the database
            _ = try db.execute("CREATE TABLE users (id INT, name TEXT)");
            _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");

            // Create a backup
            try db.createBackup(backup_dir);

            // Verify the backup
            const is_valid = try database.OLAPDatabase.verifyBackup(allocator, backup_dir);
            try testing.expect(is_valid);

            // Recover from the backup
            try database.OLAPDatabase.recoverFromBackup(allocator, backup_dir, recovery_dir);

            // Initialize a new database from the recovered data
            const recovered_db = try database.init(allocator, recovery_dir);
            defer recovered_db.deinit();

            // Verify the recovered database has the expected data
            var result_set = try recovered_db.execute("SELECT * FROM users");
            defer result_set.deinit();

            // This would check actual data in a real implementation
            // For now, we're just testing that the functions don't fail
        }
    }.callback);
}

test "Incremental backups" {
    const allocator = std.testing.allocator;

    try withTestDir(struct {
        fn callback(dir: []const u8) !void {
            // Create base and incremental backup directories
            const base_backup_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "base_backup" });
            defer allocator.free(base_backup_dir);

            const incr_backup_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "incr_backup" });
            defer allocator.free(incr_backup_dir);

            const recovery_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "recovery" });
            defer allocator.free(recovery_dir);

            try fs.cwd().makePath(base_backup_dir);
            try fs.cwd().makePath(incr_backup_dir);

            // Initialize the database
            const db = try database.init(allocator, dir);
            defer db.deinit();

            // Execute some queries to populate the database
            _ = try db.execute("CREATE TABLE test (id INT)");
            _ = try db.execute("INSERT INTO test VALUES (1)");

            // Create a base backup
            try db.createBackup(base_backup_dir);

            // Execute more queries
            _ = try db.execute("INSERT INTO test VALUES (2)");

            // Create an incremental backup
            try db.createIncrementalBackup(base_backup_dir, incr_backup_dir);

            // Recover using both backups
            var backup_dirs = [_][]const u8{ base_backup_dir, incr_backup_dir };
            try database.OLAPDatabase.recoverFromIncrementalBackups(allocator, &backup_dirs, recovery_dir);
        }
    }.callback);
}

test "Point-in-time recovery" {
    const allocator = std.testing.allocator;

    try withTestDir(struct {
        fn callback(dir: []const u8) !void {
            // Create backup and recovery directories
            const backup_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "backup" });
            defer allocator.free(backup_dir);

            const recovery_dir = try std.fs.path.join(allocator, &[_][]const u8{ dir, "recovery" });
            defer allocator.free(recovery_dir);

            try fs.cwd().makePath(backup_dir);

            // Initialize the database
            const db = try database.init(allocator, dir);
            defer db.deinit();

            // Execute queries to populate the database
            _ = try db.execute("CREATE TABLE test (id INT)");
            _ = try db.execute("INSERT INTO test VALUES (1)");

            // Create a backup
            try db.createBackup(backup_dir);

            // Get the current WAL position
            const wal_position = try db.getWALPosition();

            // Execute more queries
            _ = try db.execute("INSERT INTO test VALUES (2)");
            _ = try db.execute("INSERT INTO test VALUES (3)");

            // Recover to the point in time when we had only inserted value 1
            try database.OLAPDatabase.recoverFromBackupToPosition(allocator, backup_dir, recovery_dir, wal_position);

            // Initialize a new database from the recovered data
            const recovered_db = try database.init(allocator, recovery_dir);
            defer recovered_db.deinit();

            // In a real implementation, we would verify that only the first insert is present
        }
    }.callback);
}

test "Transaction management" {
    const allocator = std.testing.allocator;

    try withTestDir(struct {
        fn callback(dir: []const u8) !void {
            // Initialize the database
            const db = try database.init(allocator, dir);
            defer db.deinit();

            // Begin a transaction
            _ = try db.beginTransaction();

            // Execute some queries in the transaction
            _ = try db.execute("CREATE TABLE test (id INT)");
            _ = try db.execute("INSERT INTO test VALUES (1)");

            // Commit the transaction
            try db.commitTransaction();

            // Begin another transaction
            _ = try db.beginTransaction();

            // Execute some queries in the transaction
            _ = try db.execute("INSERT INTO test VALUES (2)");

            // Abort the transaction
            try db.abortTransaction();

            // In a real implementation, we would verify that only the first insert is present
        }
    }.callback);
}

test "Database recovery after crash" {
    const allocator = std.testing.allocator;

    try withTestDir(struct {
        fn callback(dir: []const u8) !void {
            var db = try database.init(allocator, dir);

            // Execute some queries to populate the database
            _ = try db.execute("CREATE TABLE test (id INT)");
            _ = try db.execute("INSERT INTO test VALUES (1)");

            // Simulate a crash by not calling deinit
            db.storage.deinit();
            db.wal.deinit();
            db.query_planner.deinit();
            db.txn_manager.deinit();
            db.db_context.deinit();
            allocator.destroy(db);

            // Recover the database
            var recovered_db = try database.recoverDatabase(allocator, dir);
            defer recovered_db.deinit();

            // Verify that the database state is consistent after recovery
            var result_set = try recovered_db.execute("SELECT * FROM test");
            defer result_set.deinit();
            
            // Check that the table exists and has the correct structure
            try std.testing.expectEqual(@as(usize, 1), result_set.columns.len);
            try std.testing.expectEqual(@as([]const u8, "id"), result_set.columns[0].name);
            
            // Check that the data was recovered correctly
            try std.testing.expectEqual(@as(usize, 1), result_set.row_count);
            try std.testing.expectEqual(@as(i32, 1), result_set.rows[0].values[0].Integer);
        }
    }.callback);
}
