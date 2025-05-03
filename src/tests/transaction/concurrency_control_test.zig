const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const manager = geeqodb.transaction.manager;
const TransactionManager = manager.TransactionManager;
const Transaction = manager.Transaction;
const TransactionStatus = manager.TransactionStatus;
const IsolationLevel = manager.IsolationLevel;

// Define a more advanced mock database with concurrency control
const ConcurrentMockDatabase = struct {
    allocator: std.mem.Allocator,
    txn_manager: *TransactionManager,
    data: std.StringHashMap(DataItem),
    locks: std.StringHashMap(LockInfo),
    version_counter: u64,

    const DataItem = struct {
        value: []const u8,
        version: u64,
        created_by_txn: u64,
        deleted_by_txn: ?u64 = null,
    };

    const LockType = enum {
        Shared,    // Read lock
        Exclusive, // Write lock
    };

    const LockInfo = struct {
        lock_type: LockType,
        txn_id: u64,
    };

    pub fn init(allocator: std.mem.Allocator) !*ConcurrentMockDatabase {
        const db = try allocator.create(ConcurrentMockDatabase);
        db.* = ConcurrentMockDatabase{
            .allocator = allocator,
            .txn_manager = try TransactionManager.init(allocator),
            .data = std.StringHashMap(DataItem).init(allocator),
            .locks = std.StringHashMap(LockInfo).init(allocator),
            .version_counter = 1,
        };
        return db;
    }

    pub fn deinit(self: *ConcurrentMockDatabase) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.value);
        }
        self.data.deinit();
        self.locks.deinit();
        self.txn_manager.deinit();
        self.allocator.destroy(self);
    }

    // Lock management
    fn acquireLock(self: *ConcurrentMockDatabase, txn: *Transaction, key: []const u8, lock_type: LockType) !void {
        // Check if the key is already locked
        if (self.locks.get(key)) |existing_lock| {
            // If the lock is already held by this transaction, upgrade if necessary
            if (existing_lock.txn_id == txn.id) {
                if (existing_lock.lock_type == .Shared and lock_type == .Exclusive) {
                    // Upgrade from shared to exclusive
                    try self.locks.put(key, .{ .lock_type = .Exclusive, .txn_id = txn.id });
                }
                return;
            }

            // If another transaction holds the lock
            switch (existing_lock.lock_type) {
                .Shared => {
                    // Shared locks can be shared, unless we want an exclusive lock
                    if (lock_type == .Exclusive) {
                        return error.LockConflict;
                    }
                },
                .Exclusive => {
                    // Exclusive locks cannot be shared
                    return error.LockConflict;
                },
            }
        }

        // Acquire the lock
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        try self.locks.put(key_copy, .{ .lock_type = lock_type, .txn_id = txn.id });
    }

    fn releaseLock(self: *ConcurrentMockDatabase, txn: *Transaction, key: []const u8) void {
        if (self.locks.get(key)) |existing_lock| {
            if (existing_lock.txn_id == txn.id) {
                _ = self.locks.remove(key);
            }
        }
    }

    fn releaseAllLocks(self: *ConcurrentMockDatabase, txn: *Transaction) void {
        var it = self.locks.iterator();
        var keys_to_remove = std.ArrayList([]const u8).init(self.allocator);
        defer keys_to_remove.deinit();

        while (it.next()) |entry| {
            if (entry.value_ptr.txn_id == txn.id) {
                keys_to_remove.append(entry.key_ptr.*) catch continue;
            }
        }

        for (keys_to_remove.items) |key| {
            _ = self.locks.remove(key);
        }
    }

    // Data access methods with concurrency control
    pub fn put(self: *ConcurrentMockDatabase, txn: *Transaction, key: []const u8, value: []const u8) !void {
        // Ensure transaction is active
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        // Apply concurrency control based on isolation level
        switch (txn.isolation_level) {
            .ReadUncommitted => {
                // No locking required for read uncommitted
            },
            .ReadCommitted, .RepeatableRead, .Serializable => {
                // Acquire exclusive lock
                try self.acquireLock(txn, key, .Exclusive);
            },
        }

        // Store the value with versioning
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        
        const value_copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(value_copy);

        const version = self.version_counter;
        self.version_counter += 1;

        // If key already exists, mark it as deleted
        if (self.data.get(key)) |old_item| {
            var updated_item = old_item;
            updated_item.deleted_by_txn = txn.id;
            try self.data.put(key_copy, updated_item);
        }

        // Create new version
        const data_item = DataItem{
            .value = value_copy,
            .version = version,
            .created_by_txn = txn.id,
        };

        try self.data.put(key_copy, data_item);
    }

    pub fn get(self: *ConcurrentMockDatabase, txn: *Transaction, key: []const u8) !?[]const u8 {
        // Ensure transaction is active
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        // Apply concurrency control based on isolation level
        switch (txn.isolation_level) {
            .ReadUncommitted => {
                // No locking required for read uncommitted
            },
            .ReadCommitted => {
                // Acquire shared lock, but release it immediately after read
                try self.acquireLock(txn, key, .Shared);
                defer self.releaseLock(txn, key);
            },
            .RepeatableRead, .Serializable => {
                // Acquire shared lock and keep it until end of transaction
                try self.acquireLock(txn, key, .Shared);
            },
        }

        // Get the data item
        const data_item = self.data.get(key) orelse return null;

        // Apply visibility rules based on isolation level
        switch (txn.isolation_level) {
            .ReadUncommitted => {
                // Can see all data, even uncommitted
                return data_item.value;
            },
            .ReadCommitted, .RepeatableRead, .Serializable => {
                // Can only see committed data or own uncommitted data
                if (data_item.created_by_txn == txn.id or self.txn_manager.getTransaction(data_item.created_by_txn) == null) {
                    // Data was created by this transaction or by a committed transaction
                    if (data_item.deleted_by_txn) |deleted_by| {
                        if (deleted_by == txn.id or self.txn_manager.getTransaction(deleted_by) == null) {
                            // Data was deleted by this transaction or by a committed transaction
                            return null;
                        }
                    }
                    return data_item.value;
                }
                return null;
            },
        }
    }

    pub fn delete(self: *ConcurrentMockDatabase, txn: *Transaction, key: []const u8) !void {
        // Ensure transaction is active
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        // Apply concurrency control based on isolation level
        switch (txn.isolation_level) {
            .ReadUncommitted => {
                // No locking required for read uncommitted
            },
            .ReadCommitted, .RepeatableRead, .Serializable => {
                // Acquire exclusive lock
                try self.acquireLock(txn, key, .Exclusive);
            },
        }

        // Mark the data item as deleted
        if (self.data.getPtr(key)) |data_item| {
            data_item.deleted_by_txn = txn.id;
        }
    }

    // Transaction management
    pub fn commitTransaction(self: *ConcurrentMockDatabase, txn: *Transaction) !void {
        // Release all locks held by this transaction
        self.releaseAllLocks(txn);

        // Commit the transaction
        try self.txn_manager.commitTransaction(txn);
    }

    pub fn abortTransaction(self: *ConcurrentMockDatabase, txn: *Transaction) !void {
        // Undo all changes made by this transaction
        var it = self.data.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.created_by_txn == txn.id) {
                // Remove data items created by this transaction
                _ = self.data.remove(entry.key_ptr.*);
            } else if (entry.value_ptr.deleted_by_txn) |deleted_by| {
                if (deleted_by == txn.id) {
                    // Undo deletions made by this transaction
                    var updated_item = entry.value_ptr.*;
                    updated_item.deleted_by_txn = null;
                    self.data.put(entry.key_ptr.*, updated_item) catch {};
                }
            }
        }

        // Release all locks held by this transaction
        self.releaseAllLocks(txn);

        // Abort the transaction
        try self.txn_manager.abortTransaction(txn);
    }
};

// Test for dirty reads with different isolation levels
test "Isolation Level - Dirty Read Test with Isolation Levels" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try ConcurrentMockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1 with Read Committed isolation
    const txn1 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Write a value in transaction 1
    try db.put(txn1, "key1", "initial_value");
    
    // Begin transaction 2 with Read Uncommitted isolation
    const txn2 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadUncommitted);
    
    // Begin transaction 3 with Read Committed isolation
    const txn3 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Update the value in transaction 1
    try db.put(txn1, "key1", "updated_value");
    
    // Read the value in transaction 2 (Read Uncommitted)
    const value2 = try db.get(txn2, "key1");
    
    // Read Uncommitted should see the dirty read
    try testing.expectEqualStrings("updated_value", value2.?);
    
    // Read the value in transaction 3 (Read Committed)
    const value3 = try db.get(txn3, "key1");
    
    // Read Committed should not see the dirty read
    try testing.expectEqualStrings("initial_value", value3.?);
    
    // Abort transaction 1
    try db.abortTransaction(txn1);
    
    // Read the value again in transaction 2 (Read Uncommitted)
    const value2_after = try db.get(txn2, "key1");
    
    // After abort, the value should be gone
    try testing.expectEqual(@as(?[]const u8, null), value2_after);
    
    // Clean up
    try db.commitTransaction(txn2);
    try db.commitTransaction(txn3);
}

// Test for non-repeatable reads with different isolation levels
test "Isolation Level - Non-repeatable Read Test with Isolation Levels" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try ConcurrentMockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1 with Read Committed isolation
    const txn1 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Write a value in transaction 1
    try db.put(txn1, "key1", "initial_value");
    
    // Commit transaction 1
    try db.commitTransaction(txn1);
    
    // Begin transaction 2 with Read Committed isolation
    const txn2 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Begin transaction 3 with Repeatable Read isolation
    const txn3 = try db.txn_manager.beginTransactionWithIsolationLevel(.RepeatableRead);
    
    // Read the value in both transactions
    const value2_before = try db.get(txn2, "key1");
    const value3_before = try db.get(txn3, "key1");
    
    try testing.expectEqualStrings("initial_value", value2_before.?);
    try testing.expectEqualStrings("initial_value", value3_before.?);
    
    // Begin transaction 4 to update the value
    const txn4 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Update the value in transaction 4
    try db.put(txn4, "key1", "updated_value");
    
    // Commit transaction 4
    try db.commitTransaction(txn4);
    
    // Read the value again in both transactions
    const value2_after = try db.get(txn2, "key1");
    const value3_after = try db.get(txn3, "key1");
    
    // Read Committed should see the new value (non-repeatable read)
    try testing.expectEqualStrings("updated_value", value2_after.?);
    
    // Repeatable Read should still see the old value
    try testing.expectEqualStrings("initial_value", value3_after.?);
    
    // Clean up
    try db.commitTransaction(txn2);
    try db.commitTransaction(txn3);
}

// Test for phantom reads with different isolation levels
test "Isolation Level - Phantom Read Test with Isolation Levels" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try ConcurrentMockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1 to set up initial data
    const txn1 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Write some initial values
    try db.put(txn1, "key1", "value1");
    try db.put(txn1, "key2", "value2");
    
    // Commit transaction 1
    try db.commitTransaction(txn1);
    
    // Begin transaction 2 with Repeatable Read isolation
    const txn2 = try db.txn_manager.beginTransactionWithIsolationLevel(.RepeatableRead);
    
    // Begin transaction 3 with Serializable isolation
    const txn3 = try db.txn_manager.beginTransactionWithIsolationLevel(.Serializable);
    
    // Count keys with prefix "key" in both transactions
    var count2_before: usize = 0;
    var count3_before: usize = 0;
    
    var it = db.data.iterator();
    while (it.next()) |entry| {
        if (std.mem.startsWith(u8, entry.key_ptr.*, "key")) {
            const value2 = try db.get(txn2, entry.key_ptr.*);
            if (value2 != null) count2_before += 1;
            
            const value3 = try db.get(txn3, entry.key_ptr.*);
            if (value3 != null) count3_before += 1;
        }
    }
    
    try testing.expectEqual(@as(usize, 2), count2_before);
    try testing.expectEqual(@as(usize, 2), count3_before);
    
    // Begin transaction 4 to insert a new key
    const txn4 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Insert a new key
    try db.put(txn4, "key3", "value3");
    
    // Commit transaction 4
    try db.commitTransaction(txn4);
    
    // Count keys with prefix "key" again in both transactions
    var count2_after: usize = 0;
    var count3_after: usize = 0;
    
    it = db.data.iterator();
    while (it.next()) |entry| {
        if (std.mem.startsWith(u8, entry.key_ptr.*, "key")) {
            const value2 = try db.get(txn2, entry.key_ptr.*);
            if (value2 != null) count2_after += 1;
            
            const value3 = try db.get(txn3, entry.key_ptr.*);
            if (value3 != null) count3_after += 1;
        }
    }
    
    // Repeatable Read should see the phantom read
    try testing.expectEqual(@as(usize, 3), count2_after);
    
    // Serializable should not see the phantom read
    try testing.expectEqual(@as(usize, 2), count3_after);
    
    // Clean up
    try db.commitTransaction(txn2);
    try db.commitTransaction(txn3);
}

// Test for lost updates with different isolation levels
test "Concurrency Control - Lost Update Test with Isolation Levels" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try ConcurrentMockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1 to set up initial data
    const txn1 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Write initial value
    try db.put(txn1, "counter", "0");
    
    // Commit transaction 1
    try db.commitTransaction(txn1);
    
    // Begin transaction 2 with Read Committed isolation
    const txn2 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Read counter in transaction 2
    const value2 = try db.get(txn2, "counter");
    try testing.expectEqualStrings("0", value2.?);
    
    // Begin transaction 3 with Serializable isolation
    const txn3 = try db.txn_manager.beginTransactionWithIsolationLevel(.Serializable);
    
    // Read counter in transaction 3
    const value3 = try db.get(txn3, "counter");
    try testing.expectEqualStrings("0", value3.?);
    
    // Increment counter in transaction 2
    try db.put(txn2, "counter", "1");
    
    // Commit transaction 2
    try db.commitTransaction(txn2);
    
    // Increment counter in transaction 3
    // This should fail with Serializable isolation due to write-write conflict
    const result = db.put(txn3, "counter", "1");
    
    // Expect a lock conflict error
    try testing.expectError(error.LockConflict, result);
    
    // Clean up
    try db.abortTransaction(txn3);
}

// Test for write skew with different isolation levels
test "Concurrency Control - Write Skew Test with Isolation Levels" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try ConcurrentMockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1 to set up initial data
    const txn1 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Write initial values
    try db.put(txn1, "account1", "500");
    try db.put(txn1, "account2", "500");
    
    // Commit transaction 1
    try db.commitTransaction(txn1);
    
    // Begin transaction 2 with Repeatable Read isolation
    const txn2 = try db.txn_manager.beginTransactionWithIsolationLevel(.RepeatableRead);
    
    // Read both accounts in transaction 2
    const account1_value2 = try db.get(txn2, "account1");
    const account2_value2 = try db.get(txn2, "account2");
    
    // Calculate total in transaction 2
    const account1_amount2 = try std.fmt.parseInt(i32, account1_value2.?, 10);
    const account2_amount2 = try std.fmt.parseInt(i32, account2_value2.?, 10);
    const total2 = account1_amount2 + account2_amount2;
    
    // Begin transaction 3 with Serializable isolation
    const txn3 = try db.txn_manager.beginTransactionWithIsolationLevel(.Serializable);
    
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
    
    // Commit transaction 2
    try db.commitTransaction(txn2);
    
    // Transaction 3 tries to withdraw 400 from account2
    // This should fail with Serializable isolation due to the constraint that total >= 500
    const result = db.put(txn3, "account2", "100");
    
    // Expect a lock conflict error
    try testing.expectError(error.LockConflict, result);
    
    // Clean up
    try db.abortTransaction(txn3);
}

// Test for deadlock detection
test "Concurrency Control - Deadlock Detection Test" {
    const allocator = testing.allocator;

    // Initialize mock database
    const db = try ConcurrentMockDatabase.init(allocator);
    defer db.deinit();

    // Begin transaction 1 to set up initial data
    const txn1 = try db.txn_manager.beginTransactionWithIsolationLevel(.ReadCommitted);
    
    // Write initial values
    try db.put(txn1, "resource1", "initial1");
    try db.put(txn1, "resource2", "initial2");
    
    // Commit transaction 1
    try db.commitTransaction(txn1);
    
    // Begin transaction 2 with Serializable isolation
    const txn2 = try db.txn_manager.beginTransactionWithIsolationLevel(.Serializable);
    
    // Begin transaction 3 with Serializable isolation
    const txn3 = try db.txn_manager.beginTransactionWithIsolationLevel(.Serializable);
    
    // Transaction 2 locks resource1
    try db.put(txn2, "resource1", "txn2_value");
    
    // Transaction 3 locks resource2
    try db.put(txn3, "resource2", "txn3_value");
    
    // Transaction 2 tries to lock resource2 (would block in a real implementation)
    const result2 = db.put(txn2, "resource2", "txn2_value");
    
    // Expect a lock conflict error
    try testing.expectError(error.LockConflict, result2);
    
    // Transaction 3 tries to lock resource1 (would cause a deadlock in a real implementation)
    const result3 = db.put(txn3, "resource1", "txn3_value");
    
    // Expect a lock conflict error
    try testing.expectError(error.LockConflict, result3);
    
    // Clean up
    try db.abortTransaction(txn2);
    try db.abortTransaction(txn3);
}
