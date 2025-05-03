const std = @import("std");
const assert = @import("../build_options.zig").assert;

/// Transaction status
pub const TransactionStatus = enum {
    Active,
    Committed,
    Aborted,
};

/// Transaction isolation level
pub const IsolationLevel = enum {
    ReadUncommitted, // Allows dirty reads, non-repeatable reads, and phantom reads
    ReadCommitted, // Prevents dirty reads, but allows non-repeatable reads and phantom reads
    RepeatableRead, // Prevents dirty reads and non-repeatable reads, but allows phantom reads
    Serializable, // Prevents dirty reads, non-repeatable reads, and phantom reads
};

/// Transaction for managing database operations
pub const Transaction = struct {
    id: u64,
    status: TransactionStatus,
    isolation_level: IsolationLevel,
    start_time: i64,
    commit_time: ?i64,

    /// Commit the transaction
    pub fn commit(self: *Transaction) void {
        // Validate inputs
        assert(self.status == .Active); // Only active transactions can be committed

        self.status = .Committed;
        self.commit_time = std.time.milliTimestamp();

        // Validate commit
        assert(self.commit_time != null);
        assert(self.commit_time.? >= self.start_time);
    }

    /// Abort the transaction
    pub fn abort(self: *Transaction) void {
        // Validate inputs
        assert(self.status == .Active); // Only active transactions can be aborted

        self.status = .Aborted;

        // Validate abort
        assert(self.status == .Aborted);
    }
};

/// Transaction manager for handling database transactions
pub const TransactionManager = struct {
    allocator: std.mem.Allocator,
    next_txn_id: u64,
    active_txns: std.AutoHashMap(u64, *Transaction),

    /// Initialize a new transaction manager
    pub fn init(allocator: std.mem.Allocator) !*TransactionManager {
        const manager = try allocator.create(TransactionManager);
        manager.* = TransactionManager{
            .allocator = allocator,
            .next_txn_id = 1,
            .active_txns = std.AutoHashMap(u64, *Transaction).init(allocator),
        };

        return manager;
    }

    /// Deinitialize the transaction manager
    pub fn deinit(self: *TransactionManager) void {
        // Free all active transactions
        var it = self.active_txns.iterator();
        while (it.next()) |entry| {
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.active_txns.deinit();
        self.allocator.destroy(self);
    }

    /// Begin a new transaction
    pub fn beginTransaction(self: *TransactionManager) !*Transaction {
        return try self.beginTransactionWithIsolationLevel(.ReadCommitted);
    }

    /// Begin a new transaction with a specific isolation level
    pub fn beginTransactionWithIsolationLevel(self: *TransactionManager, isolation_level: IsolationLevel) !*Transaction {
        const txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        const txn = try self.allocator.create(Transaction);
        txn.* = Transaction{
            .id = txn_id,
            .status = .Active,
            .isolation_level = isolation_level,
            .start_time = std.time.milliTimestamp(),
            .commit_time = null,
        };

        // Validate initialization
        assert(txn.id == txn_id);
        assert(txn.status == .Active);
        assert(txn.start_time > 0);
        assert(txn.commit_time == null);

        try self.active_txns.put(txn_id, txn);
        return txn;
    }

    /// Commit a transaction
    pub fn commitTransaction(self: *TransactionManager, txn: *Transaction) !void {
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        txn.commit();

        // Validate commit
        assert(txn.status == .Committed);
        assert(txn.commit_time != null);

        _ = self.active_txns.remove(txn.id);

        // Free the transaction memory
        self.allocator.destroy(txn);
    }

    /// Abort a transaction
    pub fn abortTransaction(self: *TransactionManager, txn: *Transaction) !void {
        if (txn.status != .Active) {
            return error.TransactionNotActive;
        }

        txn.abort();

        // Validate abort
        assert(txn.status == .Aborted);

        _ = self.active_txns.remove(txn.id);

        // Free the transaction memory
        self.allocator.destroy(txn);
    }

    /// Get a transaction by ID
    pub fn getTransaction(self: *TransactionManager, txn_id: u64) ?*Transaction {
        // Validate inputs
        assert(txn_id > 0); // Transaction IDs start from 1

        return self.active_txns.get(txn_id);
    }
};

test "TransactionManager basic functionality" {
    const allocator = std.testing.allocator;
    const manager = try TransactionManager.init(allocator);
    defer manager.deinit();

    const txn = try manager.beginTransaction();
    try std.testing.expectEqual(TransactionStatus.Active, txn.status);

    try manager.commitTransaction(txn);
    try std.testing.expectEqual(TransactionStatus.Committed, txn.status);
}
