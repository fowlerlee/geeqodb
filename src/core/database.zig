const std = @import("std");
const RocksDB = @import("../storage/rocksdb.zig").RocksDB;
const c = @import("../storage/rocksdb_c.zig");
const WAL = @import("../storage/wal.zig").WAL;
const planner = @import("../query/planner.zig");
const QueryPlanner = planner.QueryPlanner;
const QueryExecutor = @import("../query/executor.zig").QueryExecutor;
const DatabaseContext = @import("../query/executor.zig").DatabaseContext;
const transaction_manager = @import("../transaction/manager.zig");
const TransactionManager = transaction_manager.TransactionManager;
const Transaction = transaction_manager.Transaction;
const ResultSet = @import("../query/result.zig").ResultSet;
const assert = @import("../build_options.zig").assert;

pub const TableSchema = struct {
    name: []const u8,
    columns: []ColumnSchema,
};

pub const ColumnSchema = struct {
    name: []const u8,
    data_type: []const u8, // For now, store as string (e.g., "INT", "TEXT")
};

/// OLAP Database main structure
pub const OLAPDatabase = struct {
    allocator: std.mem.Allocator,
    storage: *RocksDB,
    wal: *WAL,
    query_planner: *QueryPlanner,
    txn_manager: *TransactionManager,
    db_context: *DatabaseContext,
    table_schemas: std.StringHashMap(*TableSchema),

    pub const Error = error{
        TableNotFound,
        TableAlreadyExists,
    };

    /// Execute a SQL query and return a result set
    pub fn execute(self: *OLAPDatabase, query: []const u8) !ResultSet {
        // Validate inputs
        assert(query.len > 0); // Query should not be empty

        // Check for CREATE TABLE
        if (std.mem.startsWith(u8, std.mem.trim(u8, query, &std.ascii.whitespace), "CREATE TABLE")) {
            // Very basic parsing: CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...)
            const open_paren = std.mem.indexOf(u8, query, "(") orelse return error.InvalidSyntax;
            const close_paren = std.mem.lastIndexOf(u8, query, ")") orelse return error.InvalidSyntax;
            const before_paren = std.mem.trim(u8, query[0..open_paren], &std.ascii.whitespace);
            const after_create = before_paren[12..]; // after "CREATE TABLE"
            const table_name = std.mem.trim(u8, after_create, &std.ascii.whitespace);
            const columns_str = query[open_paren + 1 .. close_paren];
            var col_tokens = std.mem.tokenizeSequence(u8, columns_str, ",");
            var columns = std.ArrayList(ColumnSchema).init(self.allocator);
            defer {
                // Free all allocated column names and types
                for (columns.items) |col| {
                    self.allocator.free(col.name);
                    self.allocator.free(col.data_type);
                }
                columns.deinit();
            }
            while (col_tokens.next()) |col| {
                var parts = std.mem.tokenizeSequence(u8, col, " ");
                const col_name = std.mem.trim(u8, parts.next() orelse return error.InvalidSyntax, &std.ascii.whitespace);
                const col_type = std.mem.trim(u8, parts.next() orelse return error.InvalidSyntax, &std.ascii.whitespace);
                try columns.append(ColumnSchema{
                    .name = try self.allocator.dupe(u8, col_name),
                    .data_type = try self.allocator.dupe(u8, col_type),
                });
            }
            try self.createTable(table_name, columns.items);
            // Return an empty result set
            return try ResultSet.init(self.allocator, 0, 0);
        }

        // Try to execute the query using the database context
        const result = self.db_context.executeRaw(query);
        if (result) |res| {
            return res;
        } else |err| {
            switch (err) {
                error.IndexNotFound, error.MissingTableName => return error.TableNotFound,
                else => return err,
            }
        }
    }

    /// Deinitialize the database
    pub fn deinit(self: *OLAPDatabase) void {
        std.debug.print("OLAPDatabase.deinit called\n", .{});
        // Free all table schemas
        std.debug.print("Deinit table_schemas\n", .{});
        if (@hasField(@TypeOf(self.table_schemas), "deinit") and self.table_schemas.count() > 0) {
            var it = self.table_schemas.iterator();
            while (it.next()) |entry| {
                // Free the hash map key (table name)
                self.allocator.free(entry.key_ptr.*);
                // Free the schema and its fields
                const schema = entry.value_ptr.*;
                self.allocator.free(schema.name);
                for (schema.columns) |col| {
                    self.allocator.free(col.name);
                    self.allocator.free(col.data_type);
                }
                self.allocator.free(schema.columns);
                self.allocator.destroy(schema);
            }
            self.table_schemas.deinit();
        }
        std.debug.print("Deinit RocksDB\n", .{});
        if (@intFromPtr(self.storage) != 0) self.storage.deinit();
        std.debug.print("Deinit WAL\n", .{});
        if (@intFromPtr(self.wal) != 0) self.wal.deinit();
        std.debug.print("Deinit QueryPlanner\n", .{});
        if (@intFromPtr(self.query_planner) != 0) self.query_planner.deinit();
        std.debug.print("Deinit TxnManager\n", .{});
        if (@intFromPtr(self.txn_manager) != 0) self.txn_manager.deinit();
        std.debug.print("Deinit DBContext\n", .{});
        if (@intFromPtr(self.db_context) != 0) self.db_context.deinit();
        self.allocator.destroy(self);
    }

    /// Begin a transaction
    pub fn beginTransaction(self: *OLAPDatabase) !*Transaction {
        return try self.txn_manager.beginTransaction();
    }

    /// Commit a transaction
    pub fn commitTransaction(self: *OLAPDatabase) !void {
        // Get the current transaction (in a real implementation, we would track the current transaction)
        // For now, we'll just create a dummy transaction and commit it
        const txn = try self.txn_manager.beginTransaction();
        try self.txn_manager.commitTransaction(txn);
    }

    /// Abort a transaction
    pub fn abortTransaction(self: *OLAPDatabase) !void {
        // Get the current transaction (in a real implementation, we would track the current transaction)
        // For now, we'll just create a dummy transaction and abort it
        const txn = try self.txn_manager.beginTransaction();
        try self.txn_manager.abortTransaction(txn);
    }

    /// Get the current WAL position
    pub fn getWALPosition(self: *OLAPDatabase) !u64 {
        // Get the current WAL position from the WAL subsystem
        return try self.wal.getCurrentPosition();
    }

    /// Create a backup of the database
    pub fn createBackup(self: *OLAPDatabase, backup_dir: []const u8) !void {
        // Validate inputs
        if (backup_dir.len == 0) return error.EmptyBackupDir;

        // Create the backup directory if it doesn't exist
        std.fs.cwd().makePath(backup_dir) catch |err| {
            return err;
        };

        // Create a null-terminated copy of the backup directory path
        const backup_dir_c = try self.allocator.alloc(u8, backup_dir.len + 1);
        defer self.allocator.free(backup_dir_c);
        @memcpy(backup_dir_c[0..backup_dir.len], backup_dir);
        backup_dir_c[backup_dir.len] = 0;

        // Create the backup engine directly
        var err_ptr: ?[*:0]u8 = null;
        const backup_engine = c.rocksdb_backup_engine_open(self.storage.options, @as([*:0]const u8, @ptrCast(backup_dir_c.ptr)), &err_ptr);

        if (err_ptr != null) {
            const err_msg = std.mem.span(err_ptr.?);
            c.rocksdb_free(err_ptr);
            std.log.err("Failed to open backup engine: {s}", .{err_msg});
            return error.RocksDBBackupEngineFailed;
        }

        if (backup_engine == null) {
            std.log.err("Failed to open backup engine: null pointer returned", .{});
            return error.RocksDBBackupEngineFailed;
        }

        defer c.rocksdb_backup_engine_close(backup_engine);

        // Reset error pointer before next operation
        err_ptr = null;

        // Create the backup
        c.rocksdb_backup_engine_create_new_backup(backup_engine, self.storage.db, &err_ptr);

        if (err_ptr != null) {
            const err_msg = std.mem.span(err_ptr.?);
            c.rocksdb_free(err_ptr);
            std.log.err("Failed to create backup: {s}", .{err_msg});
            return error.RocksDBBackupFailed;
        }

        // Also save WAL position for point-in-time recovery
        const wal_position = try self.wal.getCurrentPosition();
        const metadata_path = try std.fs.path.join(self.allocator, &[_][]const u8{ backup_dir, "metadata.json" });
        defer self.allocator.free(metadata_path);

        var file = try std.fs.cwd().createFile(metadata_path, .{});
        defer file.close();

        // Write the WAL position to the metadata file
        const metadata = try std.fmt.allocPrint(self.allocator, "{{\"wal_position\": {}}}", .{wal_position});
        defer self.allocator.free(metadata);
        try file.writeAll(metadata);
    }

    /// Create an incremental backup of the database
    pub fn createIncrementalBackup(self: *OLAPDatabase, base_backup_dir: []const u8, incr_backup_dir: []const u8) !void {
        // Validate inputs
        if (base_backup_dir.len == 0) return error.EmptyBaseBackupDir;
        if (incr_backup_dir.len == 0) return error.EmptyIncrBackupDir;

        // Create the incremental backup directory if it doesn't exist
        std.fs.cwd().makePath(incr_backup_dir) catch |err| {
            return err;
        };

        // In a real implementation, this would create an incremental backup of the database
        // For now, we just create an empty file to simulate a backup
        const metadata_path = try std.fs.path.join(self.allocator, &[_][]const u8{ incr_backup_dir, "metadata.json" });
        defer self.allocator.free(metadata_path);

        var file = try std.fs.cwd().createFile(metadata_path, .{});
        defer file.close();
        try file.writeAll("{}");
    }

    /// Verify a backup
    pub fn verifyBackup(allocator: std.mem.Allocator, backup_dir: []const u8) !bool {
        // Validate inputs
        if (backup_dir.len == 0) return error.EmptyBackupDir;

        // Check if the backup directory exists
        var dir = std.fs.cwd().openDir(backup_dir, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return false;
            }
            return err;
        };
        dir.close();

        // Check if the metadata file exists
        const metadata_path = try std.fs.path.join(allocator, &[_][]const u8{ backup_dir, "metadata.json" });
        defer allocator.free(metadata_path);

        var file = std.fs.cwd().openFile(metadata_path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return false;
            }
            return err;
        };
        file.close();

        // In a real implementation, this would verify the backup integrity
        return true;
    }

    /// Recover from a backup
    pub fn recoverFromBackup(allocator: std.mem.Allocator, backup_dir: []const u8, recovery_dir: []const u8) !void {
        // Validate inputs
        if (backup_dir.len == 0) return error.EmptyBackupDir;
        if (recovery_dir.len == 0) return error.EmptyRecoveryDir;

        // Check if the backup directory exists
        var dir = std.fs.cwd().openDir(backup_dir, .{}) catch |err| {
            return err;
        };
        dir.close();

        // Check if the metadata file exists
        const metadata_path = try std.fs.path.join(allocator, &[_][]const u8{ backup_dir, "metadata.json" });
        defer allocator.free(metadata_path);

        var file = std.fs.cwd().openFile(metadata_path, .{}) catch |err| {
            if (err == error.FileNotFound) {
                return error.BackupCorrupted;
            }
            return err;
        };

        // Read the metadata file to verify it's not corrupted
        var buffer: [1024]u8 = undefined;
        const bytes_read = try file.readAll(&buffer);
        file.close();

        if (bytes_read == 0) {
            return error.BackupCorrupted;
        }

        // Check if the metadata is valid JSON
        const is_valid = std.json.validate(allocator, buffer[0..bytes_read]) catch {
            return error.BackupCorrupted;
        };

        if (!is_valid) {
            return error.BackupCorrupted;
        }

        // Create the recovery directory if it doesn't exist
        std.fs.cwd().makePath(recovery_dir) catch |err| {
            return err;
        };

        // In a real implementation, this would restore the database from the backup
        // For now, we just create an empty file to simulate a recovery
        const recovery_metadata_path = try std.fs.path.join(allocator, &[_][]const u8{ recovery_dir, "metadata.json" });
        defer allocator.free(recovery_metadata_path);

        var recovery_file = try std.fs.cwd().createFile(recovery_metadata_path, .{});
        defer recovery_file.close();
        try recovery_file.writeAll(buffer[0..bytes_read]);
    }

    /// Recover from a backup to a specific WAL position
    pub fn recoverFromBackupToPosition(allocator: std.mem.Allocator, backup_dir: []const u8, recovery_dir: []const u8, wal_position: u64) !void {
        // First perform a normal recovery
        try recoverFromBackup(allocator, backup_dir, recovery_dir);

        // In a real implementation, this would apply WAL entries up to the specified position
        // For now, we just ignore the wal_position parameter
        _ = wal_position;
    }

    /// Recover from incremental backups
    pub fn recoverFromIncrementalBackups(allocator: std.mem.Allocator, backup_dirs: []const []const u8, recovery_dir: []const u8) !void {
        // Validate inputs
        if (backup_dirs.len == 0) return error.EmptyBackupDirs;
        if (recovery_dir.len == 0) return error.EmptyRecoveryDir;

        // First recover from the base backup
        try recoverFromBackup(allocator, backup_dirs[0], recovery_dir);

        // In a real implementation, this would apply incremental backups
        // For now, we just ignore the additional backups
    }

    /// Create a table in the database
    pub fn createTable(self: *OLAPDatabase, table_name: []const u8, columns: []ColumnSchema) !void {
        if (self.table_schemas.get(table_name) != null) {
            return error.TableAlreadyExists;
        }
        const schema = try self.allocator.create(TableSchema);
        schema.* = TableSchema{
            .name = try self.allocator.dupe(u8, table_name),
            .columns = try self.allocator.dupe(ColumnSchema, columns),
        };
        // Store the table name as a key in the hash map (dupe it for the map)
        const key = try self.allocator.dupe(u8, table_name);
        try self.table_schemas.put(key, schema);
    }
};

/// Initialize a new OLAP database
pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !*OLAPDatabase {
    var db = try allocator.create(OLAPDatabase);
    errdefer allocator.destroy(db);

    db.allocator = allocator;

    // If data_dir is empty, use a default directory
    const actual_data_dir = if (data_dir.len == 0) "data" else data_dir;

    // Create the data directory if it doesn't exist
    try std.fs.cwd().makePath(actual_data_dir);

    db.storage = try RocksDB.init(allocator, actual_data_dir);
    errdefer db.storage.deinit();

    db.wal = try WAL.init(allocator, actual_data_dir);
    errdefer db.wal.deinit();

    db.query_planner = try QueryPlanner.init(allocator);
    errdefer db.query_planner.deinit();

    db.txn_manager = try TransactionManager.init(allocator);
    errdefer db.txn_manager.deinit();

    db.db_context = try DatabaseContext.init(allocator);
    errdefer db.db_context.deinit();

    db.table_schemas = std.StringHashMap(*TableSchema).init(allocator);
    errdefer db.table_schemas.deinit();

    db.db_context.setTableSchemas(&db.table_schemas);

    return db;
}

/// Recover a database after a crash
pub fn recoverDatabase(allocator: std.mem.Allocator, data_dir: []const u8) !*OLAPDatabase {
    // First initialize a new database
    var db = try init(allocator, data_dir);

    // Then recover from the WAL
    try db.wal.recover();

    return db;
}

test "OLAPDatabase initialization" {
    const allocator = std.testing.allocator;
    const db = try init(allocator, "test_data");
    defer db.deinit();

    // Test that the database was initialized correctly
    try std.testing.expect(@intFromPtr(db.storage) != 0);
    try std.testing.expect(@intFromPtr(db.wal) != 0);
    try std.testing.expect(@intFromPtr(db.query_planner) != 0);
    try std.testing.expect(@intFromPtr(db.txn_manager) != 0);
    try std.testing.expect(@intFromPtr(db.db_context) != 0);
}

test "query on non-existent table returns error" {
    const allocator = std.testing.allocator;
    const db = try init(allocator, "test_data");
    defer db.deinit();

    const result = db.execute("SELECT * FROM users");
    try std.testing.expectError(error.TableNotFound, result);
}
