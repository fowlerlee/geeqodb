const std = @import("std");
const assert = @import("../build_options.zig").assert;
const c = @cImport({
    // Include RocksDB C API
    @cDefine("ROCKSDB_PLATFORM_POSIX", "1");
    @cDefine("ROCKSDB_LIB_IO_POSIX", "1");
    @cInclude("rocksdb/c.h");
});

pub const RocksDBError = error{
    RocksDBOpenFailed,
    RocksDBPutFailed,
    RocksDBGetFailed,
    RocksDBDeleteFailed,
    RocksDBWriteFailed,
    DatabaseClosed,
    DatabaseNotInitialized,
    EmptyKey,
    BatchNotInitialized,
    OptionsNotInitialized,
    InvalidIterator,
    IteratorKeyFailed,
    IteratorValueFailed,
    IteratorNotInitialized,
    RocksDBBackupEngineFailed,
    RocksDBBackupFailed,
    RocksDBRestoreFailed,
    RocksDBCreateColumnFamilyFailed,
};

/// RocksDB storage engine for the database
pub const RocksDB = struct {
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    is_open: bool,
    db: ?*c.rocksdb_t,
    options: ?*c.rocksdb_options_t,
    write_options: ?*c.rocksdb_writeoptions_t,
    read_options: ?*c.rocksdb_readoptions_t,

    /// Initialize a new RocksDB instance
    pub fn init(allocator: std.mem.Allocator, data_dir: []const u8) !*RocksDB {
        var db = try allocator.create(RocksDB);
        errdefer allocator.destroy(db);

        db.* = RocksDB{
            .allocator = allocator,
            .data_dir = try allocator.dupe(u8, data_dir),
            .is_open = false,
            .db = null,
            .options = null,
            .write_options = null,
            .read_options = null,
        };
        errdefer allocator.free(db.data_dir);

        try db.open();
        return db;
    }

    /// Open the database
    pub fn open(self: *RocksDB) !void {
        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization

        // Create options
        self.options = c.rocksdb_options_create();
        if (self.options == null) return error.RocksDBOpenFailed;
        errdefer if (self.options) |options| c.rocksdb_options_destroy(options);

        c.rocksdb_options_set_create_if_missing(self.options, 1);

        // Create read/write options
        self.write_options = c.rocksdb_writeoptions_create();
        if (self.write_options == null) return error.RocksDBOpenFailed;
        errdefer if (self.write_options) |write_options| c.rocksdb_writeoptions_destroy(write_options);

        self.read_options = c.rocksdb_readoptions_create();
        if (self.read_options == null) return error.RocksDBOpenFailed;
        errdefer if (self.read_options) |read_options| c.rocksdb_readoptions_destroy(read_options);

        // Open database
        self.db = c.rocksdb_open(self.options, @ptrCast(self.data_dir.ptr), err);

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBOpenFailed;
        }

        if (self.db == null) return error.RocksDBOpenFailed;

        self.is_open = true;
    }

    /// Close the database
    pub fn close(self: *RocksDB) void {
        if (self.is_open) {
            if (self.db) |db| {
                c.rocksdb_close(db);
                self.db = null;
            }

            if (self.options) |options| {
                c.rocksdb_options_destroy(options);
                self.options = null;
            }

            if (self.write_options) |write_options| {
                c.rocksdb_writeoptions_destroy(write_options);
                self.write_options = null;
            }

            if (self.read_options) |read_options| {
                c.rocksdb_readoptions_destroy(read_options);
                self.read_options = null;
            }

            self.is_open = false;
        }
    }

    /// Deinitialize the database
    pub fn deinit(self: *RocksDB) void {
        self.close();
        self.allocator.free(self.data_dir);
        self.allocator.destroy(self);
    }

    /// Put a key-value pair into the database
    pub fn put(self: *RocksDB, key: []const u8, value: []const u8) !void {
        // Validate inputs
        if (key.len == 0) return error.EmptyKey;
        if (!self.is_open) return error.DatabaseClosed;
        if (self.db == null) return error.DatabaseNotInitialized;
        if (self.write_options == null) return error.DatabaseNotInitialized;

        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization

        c.rocksdb_put(
            self.db.?,
            self.write_options.?,
            @ptrCast(key.ptr),
            key.len,
            @ptrCast(value.ptr),
            value.len,
            err,
        );

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBPutFailed;
        }
    }

    /// Get a value from the database
    pub fn get(self: *RocksDB, allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
        // Validate inputs
        if (key.len == 0) return error.EmptyKey;
        if (!self.is_open) return error.DatabaseClosed;
        if (self.db == null) return error.DatabaseNotInitialized;
        if (self.read_options == null) return error.DatabaseNotInitialized;

        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization
        var val_len: usize = 0;

        const val_ptr = c.rocksdb_get(
            self.db.?,
            self.read_options.?,
            @ptrCast(key.ptr),
            key.len,
            &val_len,
            err,
        );

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBGetFailed;
        }

        if (val_ptr == null) {
            return null; // Key not found
        }

        defer c.rocksdb_free(val_ptr);

        // Copy the value to a new buffer owned by the caller
        const value = try allocator.alloc(u8, val_len);
        errdefer allocator.free(value);

        @memcpy(value, val_ptr[0..val_len]);

        return value;
    }

    /// Delete a key-value pair from the database
    pub fn delete(self: *RocksDB, key: []const u8) !void {
        // Validate inputs
        if (key.len == 0) return error.EmptyKey;
        if (!self.is_open) return error.DatabaseClosed;
        if (self.db == null) return error.DatabaseNotInitialized;
        if (self.write_options == null) return error.DatabaseNotInitialized;

        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization

        c.rocksdb_delete(
            self.db.?,
            self.write_options.?,
            @ptrCast(key.ptr),
            key.len,
            err,
        );

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBDeleteFailed;
        }
    }

    /// Create a new iterator for the database
    pub fn iterator(self: *RocksDB) !*Iterator {
        if (!self.is_open) return error.DatabaseClosed;
        if (self.db == null) return error.DatabaseNotInitialized;

        const iter = try self.allocator.create(Iterator);
        const rocks_iter = c.rocksdb_create_iterator(self.db.?, self.read_options.?);

        iter.* = Iterator{
            .db = self,
            .iter = rocks_iter,
            .valid = false,
            .current_key = null,
            .current_value = null,
        };

        return iter;
    }

    /// Iterator for RocksDB
    pub const Iterator = struct {
        db: *RocksDB,
        iter: ?*c.rocksdb_iterator_t,
        valid: bool,
        current_key: ?[]const u8,
        current_value: ?[]const u8,

        /// Seek to the first key
        pub fn seekToFirst(self: *Iterator) void {
            if (self.iter) |iter| {
                c.rocksdb_iter_seek_to_first(iter);
                self.valid = c.rocksdb_iter_valid(iter) != 0;
                self.clearCurrentKeyValue();
            }
        }

        /// Seek to a specific key
        pub fn seek(self: *Iterator, target_key: []const u8) void {
            if (target_key.len == 0) return;

            if (self.iter) |iter| {
                c.rocksdb_iter_seek(iter, target_key.ptr, target_key.len);
                self.valid = c.rocksdb_iter_valid(iter) != 0;
                self.clearCurrentKeyValue();
            }
        }

        /// Move to the next key
        pub fn next(self: *Iterator) void {
            if (self.iter) |iter| {
                c.rocksdb_iter_next(iter);
                self.valid = c.rocksdb_iter_valid(iter) != 0;
                self.clearCurrentKeyValue();
            }
        }

        /// Check if the iterator is valid
        pub fn isValid(self: *Iterator) bool {
            return self.valid;
        }

        /// Clear cached key and value
        fn clearCurrentKeyValue(self: *Iterator) void {
            self.current_key = null;
            self.current_value = null;
        }

        /// Get the current key
        pub fn key(self: *Iterator) ![]const u8 {
            if (!self.valid) return error.InvalidIterator;

            if (self.current_key) |k| {
                return k;
            }

            if (self.iter) |iter| {
                var key_len: usize = 0;
                const key_ptr = c.rocksdb_iter_key(iter, &key_len);
                if (key_ptr == null) return error.IteratorKeyFailed;

                // This is a pointer to RocksDB's internal memory, valid until next operation
                // We don't need to free it
                self.current_key = key_ptr[0..key_len];
                return self.current_key.?;
            }

            return error.IteratorNotInitialized;
        }

        /// Get the current value
        pub fn value(self: *Iterator) ![]const u8 {
            if (!self.valid) return error.InvalidIterator;

            if (self.current_value) |v| {
                return v;
            }

            if (self.iter) |iter| {
                var value_len: usize = 0;
                const value_ptr = c.rocksdb_iter_value(iter, &value_len);
                if (value_ptr == null) return error.IteratorValueFailed;

                // This is a pointer to RocksDB's internal memory, valid until next operation
                // We don't need to free it
                self.current_value = value_ptr[0..value_len];
                return self.current_value.?;
            }

            return error.IteratorNotInitialized;
        }

        /// Deinitialize the iterator
        pub fn deinit(self: *Iterator) void {
            if (self.iter) |iter| {
                c.rocksdb_iter_destroy(iter);
            }
            self.db.allocator.destroy(self);
        }
    };

    /// Create a new write batch
    pub fn createWriteBatch(self: *RocksDB) !*WriteBatch {
        if (!self.is_open) return error.DatabaseClosed;

        const batch = try self.allocator.create(WriteBatch);
        batch.* = WriteBatch{
            .db = self,
            .batch = c.rocksdb_writebatch_create(),
        };

        return batch;
    }

    /// WriteBatch for RocksDB
    pub const WriteBatch = struct {
        db: *RocksDB,
        batch: ?*c.rocksdb_writebatch_t,

        /// Put a key-value pair into the batch
        pub fn put(self: *WriteBatch, key: []const u8, value: []const u8) !void {
            if (key.len == 0) return error.EmptyKey;
            if (self.batch == null) return error.BatchNotInitialized;

            c.rocksdb_writebatch_put(self.batch.?, key.ptr, key.len, value.ptr, value.len);
        }

        /// Delete a key-value pair from the batch
        pub fn delete(self: *WriteBatch, key: []const u8) !void {
            if (key.len == 0) return error.EmptyKey;
            if (self.batch == null) return error.BatchNotInitialized;

            c.rocksdb_writebatch_delete(self.batch.?, key.ptr, key.len);
        }

        /// Commit the batch
        pub fn commit(self: *WriteBatch) !void {
            if (!self.db.is_open) return error.DatabaseClosed;
            if (self.db.db == null) return error.DatabaseNotInitialized;
            if (self.batch == null) return error.BatchNotInitialized;

            var err: [*c][*c]u8 = null;
            @setRuntimeSafety(false); // Disable runtime safety for C interop
            _ = &err; // Mark as used to prevent optimization

            c.rocksdb_write(self.db.db.?, self.db.write_options.?, self.batch.?, err);

            if (err != null and err[0] != null) {
                c.rocksdb_free(err[0]);
                return error.RocksDBWriteFailed;
            }
        }

        /// Deinitialize the batch
        pub fn deinit(self: *WriteBatch) void {
            if (self.batch) |batch| {
                c.rocksdb_writebatch_destroy(batch);
            }
            self.db.allocator.destroy(self);
        }
    };

    /// Configure advanced options for RocksDB
    pub fn configureOptions(self: *RocksDB, config: Options) !void {
        if (self.options == null) return error.OptionsNotInitialized;

        // Apply configuration
        if (config.create_if_missing) |value| {
            c.rocksdb_options_set_create_if_missing(self.options.?, @intFromBool(value));
        }

        if (config.paranoid_checks) |value| {
            c.rocksdb_options_set_paranoid_checks(self.options.?, @intFromBool(value));
        }

        if (config.write_buffer_size) |value| {
            c.rocksdb_options_set_write_buffer_size(self.options.?, value);
        }

        if (config.max_open_files) |value| {
            c.rocksdb_options_set_max_open_files(self.options.?, @intCast(value));
        }

        if (config.compression) |value| {
            c.rocksdb_options_set_compression(self.options.?, @intFromEnum(value));
        }
    }

    /// Options for configuring RocksDB
    pub const Options = struct {
        create_if_missing: ?bool = true,
        paranoid_checks: ?bool = null,
        write_buffer_size: ?usize = null,
        max_open_files: ?i32 = null,
        compression: ?CompressionType = null,

        /// Compression types supported by RocksDB
        pub const CompressionType = enum(c_int) {
            no = 0,
            snappy = 1,
            zlib = 2,
            bzip2 = 3,
            lz4 = 4,
            lz4hc = 5,
            zstd = 6,
        };
    };

    /// Create a backup of the database
    pub fn createBackup(self: *RocksDB, backup_dir: []const u8) !void {
        if (!self.is_open) return error.DatabaseClosed;
        if (self.db == null) return error.DatabaseNotInitialized;
        if (self.options == null) return error.OptionsNotInitialized;

        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization

        const backup_engine = c.rocksdb_backup_engine_open(self.options.?, @ptrCast(backup_dir.ptr), err);

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBBackupEngineFailed;
        }

        if (backup_engine == null) return error.RocksDBBackupEngineFailed;
        defer c.rocksdb_backup_engine_close(backup_engine);

        c.rocksdb_backup_engine_create_new_backup(backup_engine, self.db.?, err);

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBBackupFailed;
        }
    }

    /// Restore from a backup
    pub fn restoreFromBackup(self: *RocksDB, backup_dir: []const u8) !void {
        // Close the database if it's open
        if (self.is_open) {
            self.close();
        }

        // We need to create options if they don't exist
        if (self.options == null) {
            self.options = c.rocksdb_options_create();
            if (self.options == null) return error.OptionsNotInitialized;
            c.rocksdb_options_set_create_if_missing(self.options.?, 1);
        }

        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization

        const backup_engine = c.rocksdb_backup_engine_open(self.options.?, @ptrCast(backup_dir.ptr), err);

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBBackupEngineFailed;
        }

        if (backup_engine == null) return error.RocksDBBackupEngineFailed;
        defer c.rocksdb_backup_engine_close(backup_engine);

        // Create restore options
        const restore_options = c.rocksdb_restore_options_create();
        if (restore_options == null) return error.RocksDBRestoreFailed;
        defer c.rocksdb_restore_options_destroy(restore_options);

        c.rocksdb_backup_engine_restore_db_from_latest_backup(
            backup_engine,
            @ptrCast(self.data_dir.ptr),
            @ptrCast(self.data_dir.ptr),
            restore_options,
            err,
        );

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBRestoreFailed;
        }

        // Reopen the database
        try self.open();
    }

    /// Create a new column family
    pub fn createColumnFamily(self: *RocksDB, name: []const u8) !void {
        if (!self.is_open) return error.DatabaseClosed;
        if (self.db == null) return error.DatabaseNotInitialized;

        var err: [*c][*c]u8 = null;
        @setRuntimeSafety(false); // Disable runtime safety for C interop
        _ = &err; // Mark as used to prevent optimization

        const cf_options = c.rocksdb_options_create();
        if (cf_options == null) return error.OptionsNotInitialized;
        defer c.rocksdb_options_destroy(cf_options);

        const cf_handle = c.rocksdb_create_column_family(self.db.?, cf_options, @ptrCast(name.ptr), err);

        if (err != null and err[0] != null) {
            c.rocksdb_free(err[0]);
            return error.RocksDBCreateColumnFamilyFailed;
        }

        if (cf_handle == null) return error.RocksDBCreateColumnFamilyFailed;
        c.rocksdb_column_family_handle_destroy(cf_handle);
    }
};

test "RocksDB basic functionality" {
    const allocator = std.testing.allocator;
    const db = try RocksDB.init(allocator, "test_data");
    defer db.deinit();

    try std.testing.expect(db.is_open);
}
