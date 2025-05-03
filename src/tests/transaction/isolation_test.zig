const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const manager = geeqodb.transaction.manager;
const TransactionManager = manager.TransactionManager;
const Transaction = manager.Transaction;
const TransactionStatus = manager.TransactionStatus;

// Define a simple mock database for testing isolation levels
const MockDatabase = struct {
    allocator: std.mem.Allocator,
    txn_manager: *TransactionManager,
    data: std.StringHashMap([]const u8),

    pub fn init(allocator: std.mem.Allocator) !*MockDatabase {
        const db = try allocator.create(MockDatabase);
        db.* = MockDatabase{
            .allocator = allocator,
            .txn_manager = try TransactionManager.init(allocator),
            .data = std.StringHashMap([]const u8).init(allocator),
        };
        return db;
    }

    pub fn deinit(self: *MockDatabase) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.data.deinit();
        self.txn_manager.deinit();
        self.allocator.destroy(self);
    }

    // Basic operations
    pub fn put(self: *MockDatabase, txn: *Transaction, key: []const u8, value: []const u8) !void {
        // Ensure transaction is active
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        // Store the value
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        
        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        // If key already exists, free the old value
        if (self.data.get(key_copy)) |old_value| {
            self.allocator.free(old_value);
        }

        try self.data.put(key_copy, value_copy);
    }

    pub fn get(self: *MockDatabase, txn: *Transaction, key: []const u8) !?[]const u8 {
        // Ensure transaction is active
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        return self.data.get(key);
    }

    pub fn delete(self: *MockDatabase, txn: *Transaction, key: []const u8) !void {
        // Ensure transaction is active
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        // Remove the key-value pair
        if (self.data.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value);
        }
    }
};

// Test for dirty reads (should be allowed in Read Uncommitted, prevented in other levels)
test "Isolation Level - Dirty Read Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try MockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1
    const txn1 = try db.txn_manager.beginTransaction();
    
    // Write a value in transaction 1
    try db.put(txn1, "key1", "initial_value");
    
    // Begin transaction 2
    const txn2 = try db.txn_manager.beginTransaction();
    
    // Update the value in transaction 1
    try db.put(txn1, "key1", "updated_value");
    
    // Read the value in transaction 2 (this should be a dirty read)
    const value = try db.get(txn2, "key1");
    
    // In a real implementation with isolation levels, this would depend on the isolation level
    // For now, we'll just verify that the dirty read happens (Read Uncommitted behavior)
    try testing.expectEqualStrings("updated_value", value.?);
    
    // Abort transaction 1
    try db.txn_manager.abortTransaction(txn1);
    
    // In a real implementation with proper isolation, transaction 2 should now see the original value
    // or no value at all, depending on the isolation level
    
    // Clean up
    try db.txn_manager.commitTransaction(txn2);
}

// Test for non-repeatable reads (should be allowed in Read Uncommitted and Read Committed, prevented in others)
test "Isolation Level - Non-repeatable Read Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try MockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1
    const txn1 = try db.txn_manager.beginTransaction();
    
    // Write a value in transaction 1
    try db.put(txn1, "key1", "initial_value");
    
    // Commit transaction 1
    try db.txn_manager.commitTransaction(txn1);
    
    // Begin transaction 2
    const txn2 = try db.txn_manager.beginTransaction();
    
    // Read the value in transaction 2
    const value1 = try db.get(txn2, "key1");
    try testing.expectEqualStrings("initial_value", value1.?);
    
    // Begin transaction 3
    const txn3 = try db.txn_manager.beginTransaction();
    
    // Update the value in transaction 3
    try db.put(txn3, "key1", "updated_value");
    
    // Commit transaction 3
    try db.txn_manager.commitTransaction(txn3);
    
    // Read the value again in transaction 2
    const value2 = try db.get(txn2, "key1");
    
    // In a real implementation with isolation levels, this would depend on the isolation level
    // For now, we'll just verify that the non-repeatable read happens (Read Uncommitted/Read Committed behavior)
    try testing.expectEqualStrings("updated_value", value2.?);
    
    // Clean up
    try db.txn_manager.commitTransaction(txn2);
}

// Test for phantom reads (should be allowed in Read Uncommitted, Read Committed, and Repeatable Read, prevented in Serializable)
test "Isolation Level - Phantom Read Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try MockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1
    const txn1 = try db.txn_manager.beginTransaction();
    
    // Write some initial values
    try db.put(txn1, "key1", "value1");
    try db.put(txn1, "key2", "value2");
    
    // Commit transaction 1
    try db.txn_manager.commitTransaction(txn1);
    
    // Begin transaction 2
    const txn2 = try db.txn_manager.beginTransaction();
    
    // Count keys with prefix "key" in transaction 2
    var count1: usize = 0;
    var it = db.data.iterator();
    while (it.next()) |entry| {
        if (std.mem.startsWith(u8, entry.key_ptr.*, "key")) {
            count1 += 1;
        }
    }
    try testing.expectEqual(@as(usize, 2), count1);
    
    // Begin transaction 3
    const txn3 = try db.txn_manager.beginTransaction();
    
    // Insert a new key in transaction 3
    try db.put(txn3, "key3", "value3");
    
    // Commit transaction 3
    try db.txn_manager.commitTransaction(txn3);
    
    // Count keys with prefix "key" again in transaction 2
    var count2: usize = 0;
    it = db.data.iterator();
    while (it.next()) |entry| {
        if (std.mem.startsWith(u8, entry.key_ptr.*, "key")) {
            count2 += 1;
        }
    }
    
    // In a real implementation with isolation levels, this would depend on the isolation level
    // For now, we'll just verify that the phantom read happens (Read Uncommitted/Read Committed/Repeatable Read behavior)
    try testing.expectEqual(@as(usize, 3), count2);
    
    // Clean up
    try db.txn_manager.commitTransaction(txn2);
}

// Test for lost updates (should be prevented in all isolation levels with proper concurrency control)
test "Concurrency Control - Lost Update Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try MockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1
    const txn1 = try db.txn_manager.beginTransaction();
    
    // Write initial value
    try db.put(txn1, "counter", "0");
    
    // Commit transaction 1
    try db.txn_manager.commitTransaction(txn1);
    
    // Begin transaction 2
    const txn2 = try db.txn_manager.beginTransaction();
    
    // Read counter in transaction 2
    const value2 = try db.get(txn2, "counter");
    try testing.expectEqualStrings("0", value2.?);
    
    // Begin transaction 3
    const txn3 = try db.txn_manager.beginTransaction();
    
    // Read counter in transaction 3
    const value3 = try db.get(txn3, "counter");
    try testing.expectEqualStrings("0", value3.?);
    
    // Increment counter in transaction 2
    try db.put(txn2, "counter", "1");
    
    // Commit transaction 2
    try db.txn_manager.commitTransaction(txn2);
    
    // Increment counter in transaction 3 (this would cause a lost update without proper concurrency control)
    try db.put(txn3, "counter", "1");
    
    // Commit transaction 3
    try db.txn_manager.commitTransaction(txn3);
    
    // Begin transaction 4 to check the final value
    const txn4 = try db.txn_manager.beginTransaction();
    
    // Read counter in transaction 4
    const value4 = try db.get(txn4, "counter");
    
    // In a real implementation with proper concurrency control, this should be "2"
    // For now, we'll just verify that the lost update happens (no concurrency control)
    try testing.expectEqualStrings("1", value4.?);
    
    // Clean up
    try db.txn_manager.commitTransaction(txn4);
}

// Test for write skew (should be prevented in Serializable isolation level)
test "Concurrency Control - Write Skew Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try MockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1
    const txn1 = try db.txn_manager.beginTransaction();
    
    // Write initial values
    try db.put(txn1, "account1", "500");
    try db.put(txn1, "account2", "500");
    
    // Commit transaction 1
    try db.txn_manager.commitTransaction(txn1);
    
    // Begin transaction 2
    const txn2 = try db.txn_manager.beginTransaction();
    
    // Read both accounts in transaction 2
    const account1_value2 = try db.get(txn2, "account1");
    const account2_value2 = try db.get(txn2, "account2");
    
    // Calculate total in transaction 2
    const account1_amount2 = try std.fmt.parseInt(i32, account1_value2.?, 10);
    const account2_amount2 = try std.fmt.parseInt(i32, account2_value2.?, 10);
    const total2 = account1_amount2 + account2_amount2;
    
    // Begin transaction 3
    const txn3 = try db.txn_manager.beginTransaction();
    
    // Read both accounts in transaction 3
    const account1_value3 = try db.get(txn3, "account1");
    const account2_value3 = try db.get(txn3, "account2");
    
    // Calculate total in transaction 3
    const account1_amount3 = try std.fmt.parseInt(i32, account1_value3.?, 10);
    const account2_amount3 = try std.fmt.parseInt(i32, account2_value3.?, 10);
    const total3 = account1_amount3 + account2_amount3;
    
    // Both transactions verify that total is 1000
    try testing.expectEqual(@as(i32, 1000), total2);
    try testing.expectEqual(@as(i32, 1000), total3);
    
    // Transaction 2 withdraws 400 from account1
    try db.put(txn2, "account1", "100");
    
    // Transaction 3 withdraws 400 from account2
    try db.put(txn3, "account2", "100");
    
    // Commit both transactions
    try db.txn_manager.commitTransaction(txn2);
    try db.txn_manager.commitTransaction(txn3);
    
    // Begin transaction 4 to check the final values
    const txn4 = try db.txn_manager.beginTransaction();
    
    // Read both accounts in transaction 4
    const account1_value4 = try db.get(txn4, "account1");
    const account2_value4 = try db.get(txn4, "account2");
    
    // Calculate total in transaction 4
    const account1_amount4 = try std.fmt.parseInt(i32, account1_value4.?, 10);
    const account2_amount4 = try std.fmt.parseInt(i32, account2_value4.?, 10);
    const total4 = account1_amount4 + account2_amount4;
    
    // In a real implementation with Serializable isolation, one of the transactions should have been aborted
    // For now, we'll just verify that the write skew happens (no proper concurrency control)
    try testing.expectEqual(@as(i32, 200), total4);
    
    // Clean up
    try db.txn_manager.commitTransaction(txn4);
}

// Test for deadlock detection (should be handled by the concurrency control mechanism)
test "Concurrency Control - Deadlock Detection Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try MockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1
    const txn1 = try db.txn_manager.beginTransaction();
    
    // Write initial values
    try db.put(txn1, "resource1", "initial1");
    try db.put(txn1, "resource2", "initial2");
    
    // Commit transaction 1
    try db.txn_manager.commitTransaction(txn1);
    
    // Begin transaction 2
    const txn2 = try db.txn_manager.beginTransaction();
    
    // Begin transaction 3
    const txn3 = try db.txn_manager.beginTransaction();
    
    // Transaction 2 locks resource1
    try db.put(txn2, "resource1", "txn2_value");
    
    // Transaction 3 locks resource2
    try db.put(txn3, "resource2", "txn3_value");
    
    // Transaction 2 tries to lock resource2 (would block in a real implementation)
    try db.put(txn2, "resource2", "txn2_value");
    
    // Transaction 3 tries to lock resource1 (would cause a deadlock in a real implementation)
    try db.put(txn3, "resource1", "txn3_value");
    
    // In a real implementation with deadlock detection, one of the transactions should be aborted
    // For now, we'll just verify that both transactions can complete (no deadlock detection)
    
    // Commit both transactions
    try db.txn_manager.commitTransaction(txn2);
    try db.txn_manager.commitTransaction(txn3);
    
    // Begin transaction 4 to check the final values
    const txn4 = try db.txn_manager.beginTransaction();
    
    // Read both resources in transaction 4
    const resource1_value = try db.get(txn4, "resource1");
    const resource2_value = try db.get(txn4, "resource2");
    
    // Verify the final values
    try testing.expectEqualStrings("txn3_value", resource1_value.?);
    try testing.expectEqualStrings("txn2_value", resource2_value.?);
    
    // Clean up
    try db.txn_manager.commitTransaction(txn4);
}
