const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const manager = geeqodb.transaction.manager;
const TransactionManager = manager.TransactionManager;
const Transaction = manager.Transaction;
const TransactionStatus = manager.TransactionStatus;

test "TransactionManager initialization" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Verify that TransactionManager was initialized correctly
    try testing.expectEqual(allocator, txn_manager.allocator);
    try testing.expectEqual(@as(u64, 1), txn_manager.next_txn_id);
    try testing.expectEqual(@as(usize, 0), txn_manager.active_txns.count());
}

test "TransactionManager begin transaction" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin a transaction
    const txn = try txn_manager.beginTransaction();

    // Verify that the transaction was created correctly
    try testing.expectEqual(@as(u64, 1), txn.id);
    try testing.expectEqual(TransactionStatus.Active, txn.status);
    try testing.expect(txn.start_time > 0);
    try testing.expectEqual(@as(?i64, null), txn.commit_time);

    // Verify that the transaction was added to the active transactions
    try testing.expectEqual(@as(usize, 1), txn_manager.active_txns.count());
    try testing.expectEqual(txn, txn_manager.active_txns.get(1).?);

    // Verify that the next transaction ID was incremented
    try testing.expectEqual(@as(u64, 2), txn_manager.next_txn_id);

    // Commit the transaction to clean up
    try txn_manager.commitTransaction(txn);
}

test "TransactionManager commit transaction" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin a transaction
    const txn = try txn_manager.beginTransaction();

    // Commit the transaction (this will free the transaction)
    try txn_manager.commitTransaction(txn);

    // We can't verify the transaction status directly since it's been freed
    // Instead, we verify that the transaction is no longer in the active transactions

    // Verify that the transaction was removed from the active transactions
    try testing.expectEqual(@as(usize, 0), txn_manager.active_txns.count());
    try testing.expectEqual(@as(?*Transaction, null), txn_manager.getTransaction(1));
}

test "TransactionManager abort transaction" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin a transaction
    const txn = try txn_manager.beginTransaction();

    // Abort the transaction (this will free the transaction)
    try txn_manager.abortTransaction(txn);

    // We can't verify the transaction status directly since it's been freed
    // Instead, we verify that the transaction is no longer in the active transactions

    // Verify that the transaction was removed from the active transactions
    try testing.expectEqual(@as(usize, 0), txn_manager.active_txns.count());
    try testing.expectEqual(@as(?*Transaction, null), txn_manager.getTransaction(1));
}

test "TransactionManager get transaction" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin a transaction
    const txn = try txn_manager.beginTransaction();

    // Get the transaction
    const retrieved_txn = txn_manager.getTransaction(1);

    // Verify that the transaction was retrieved correctly
    try testing.expectEqual(txn, retrieved_txn.?);

    // Try to get a non-existent transaction
    const non_existent_txn = txn_manager.getTransaction(999);

    // Verify that the transaction was not found
    try testing.expectEqual(@as(?*Transaction, null), non_existent_txn);

    // Commit the transaction to clean up
    try txn_manager.commitTransaction(txn);
}

test "TransactionManager commit non-active transaction" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin a transaction
    const txn = try txn_manager.beginTransaction();

    // Commit the transaction
    try txn_manager.commitTransaction(txn);

    // Try to commit the transaction again
    try testing.expectError(error.TransactionNotActive, txn_manager.commitTransaction(txn));
}

test "TransactionManager abort non-active transaction" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin a transaction
    const txn = try txn_manager.beginTransaction();

    // Abort the transaction
    try txn_manager.abortTransaction(txn);

    // Try to abort the transaction again
    try testing.expectError(error.TransactionNotActive, txn_manager.abortTransaction(txn));
}

test "TransactionManager multiple transactions" {
    const allocator = testing.allocator;

    // Initialize TransactionManager
    const txn_manager = try TransactionManager.init(allocator);
    defer txn_manager.deinit();

    // Begin multiple transactions
    const txn1 = try txn_manager.beginTransaction();
    const txn2 = try txn_manager.beginTransaction();
    const txn3 = try txn_manager.beginTransaction();

    // Verify that the transactions were created correctly
    try testing.expectEqual(@as(u64, 1), txn1.id);
    try testing.expectEqual(@as(u64, 2), txn2.id);
    try testing.expectEqual(@as(u64, 3), txn3.id);

    // Verify that the transactions were added to the active transactions
    try testing.expectEqual(@as(usize, 3), txn_manager.active_txns.count());

    // Commit the first transaction (this will free the transaction)
    try txn_manager.commitTransaction(txn1);

    // We can't verify the transaction status directly since it's been freed
    // Instead, we verify that the transaction is no longer in the active transactions

    // Verify that the first transaction was removed from the active transactions
    try testing.expectEqual(@as(usize, 2), txn_manager.active_txns.count());
    try testing.expectEqual(@as(?*Transaction, null), txn_manager.getTransaction(1));

    // Abort the second transaction (this will free the transaction)
    try txn_manager.abortTransaction(txn2);

    // We can't verify the transaction status directly since it's been freed
    // Instead, we verify that the transaction is no longer in the active transactions

    // Verify that the second transaction was removed from the active transactions
    try testing.expectEqual(@as(usize, 1), txn_manager.active_txns.count());
    try testing.expectEqual(@as(?*Transaction, null), txn_manager.getTransaction(2));

    // Commit the third transaction (this will free the transaction)
    try txn_manager.commitTransaction(txn3);

    // We can't verify the transaction status directly since it's been freed
    // Instead, we verify that the transaction is no longer in the active transactions

    // Verify that the third transaction was removed from the active transactions
    try testing.expectEqual(@as(usize, 0), txn_manager.active_txns.count());
    try testing.expectEqual(@as(?*Transaction, null), txn_manager.getTransaction(3));
}
