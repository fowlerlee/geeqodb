//! RocksDB C API bindings
pub const rocksdb_t = opaque {};
pub const rocksdb_options_t = opaque {};
pub const rocksdb_writeoptions_t = opaque {};
pub const rocksdb_readoptions_t = opaque {};
pub const rocksdb_iterator_t = opaque {};
pub const rocksdb_writebatch_t = opaque {};
pub const rocksdb_backup_engine_t = opaque {};
pub const rocksdb_backup_engine_options_t = opaque {};
pub const rocksdb_restore_options_t = opaque {};
pub const rocksdb_column_family_handle_t = opaque {};

// Basic RocksDB operations
pub extern "c" fn rocksdb_open(options: ?*const rocksdb_options_t, name: [*:0]const u8, errptr: ?*?[*:0]u8) ?*rocksdb_t;
pub extern "c" fn rocksdb_close(db: ?*rocksdb_t) void;
pub extern "c" fn rocksdb_put(db: ?*const rocksdb_t, options: ?*const rocksdb_writeoptions_t, key: [*]const u8, keylen: usize, val: [*]const u8, vallen: usize, errptr: ?*?[*:0]u8) void;
pub extern "c" fn rocksdb_get(db: ?*const rocksdb_t, options: ?*const rocksdb_readoptions_t, key: [*]const u8, keylen: usize, vallen: *usize, errptr: ?*?[*:0]u8) ?[*]u8;
pub extern "c" fn rocksdb_delete(db: ?*const rocksdb_t, options: ?*const rocksdb_writeoptions_t, key: [*]const u8, keylen: usize, errptr: ?*?[*:0]u8) void;
pub extern "c" fn rocksdb_free(ptr: ?[*]u8) void;

// Options
pub extern "c" fn rocksdb_options_create() ?*rocksdb_options_t;
pub extern "c" fn rocksdb_options_destroy(options: ?*rocksdb_options_t) void;
pub extern "c" fn rocksdb_options_set_create_if_missing(options: ?*rocksdb_options_t, v: u8) void;
pub extern "c" fn rocksdb_options_set_paranoid_checks(options: ?*rocksdb_options_t, v: u8) void;
pub extern "c" fn rocksdb_options_set_write_buffer_size(options: ?*rocksdb_options_t, size: usize) void;
pub extern "c" fn rocksdb_options_set_max_open_files(options: ?*rocksdb_options_t, n: c_int) void;
pub extern "c" fn rocksdb_options_set_compression(options: ?*rocksdb_options_t, t: c_int) void;

// Read/Write options
pub extern "c" fn rocksdb_readoptions_create() ?*rocksdb_readoptions_t;
pub extern "c" fn rocksdb_readoptions_destroy(options: ?*rocksdb_readoptions_t) void;
pub extern "c" fn rocksdb_writeoptions_create() ?*rocksdb_writeoptions_t;
pub extern "c" fn rocksdb_writeoptions_destroy(options: ?*rocksdb_writeoptions_t) void;

// Iterator
pub extern "c" fn rocksdb_create_iterator(db: ?*const rocksdb_t, options: ?*const rocksdb_readoptions_t) ?*rocksdb_iterator_t;
pub extern "c" fn rocksdb_iter_destroy(iter: ?*rocksdb_iterator_t) void;
pub extern "c" fn rocksdb_iter_seek_to_first(iter: ?*rocksdb_iterator_t) void;
pub extern "c" fn rocksdb_iter_seek(iter: ?*rocksdb_iterator_t, k: [*]const u8, klen: usize) void;
pub extern "c" fn rocksdb_iter_next(iter: ?*rocksdb_iterator_t) void;
pub extern "c" fn rocksdb_iter_valid(iter: ?*const rocksdb_iterator_t) u8;
pub extern "c" fn rocksdb_iter_key(iter: ?*const rocksdb_iterator_t, klen: *usize) ?[*]const u8;
pub extern "c" fn rocksdb_iter_value(iter: ?*const rocksdb_iterator_t, vlen: *usize) ?[*]const u8;

// WriteBatch
pub extern "c" fn rocksdb_writebatch_create() ?*rocksdb_writebatch_t;
pub extern "c" fn rocksdb_writebatch_destroy(batch: ?*rocksdb_writebatch_t) void;
pub extern "c" fn rocksdb_writebatch_put(batch: ?*rocksdb_writebatch_t, key: [*]const u8, keylen: usize, val: [*]const u8, vallen: usize) void;
pub extern "c" fn rocksdb_writebatch_delete(batch: ?*rocksdb_writebatch_t, key: [*]const u8, keylen: usize) void;
pub extern "c" fn rocksdb_write(db: ?*rocksdb_t, options: ?*const rocksdb_writeoptions_t, batch: ?*rocksdb_writebatch_t, errptr: ?*?[*:0]u8) void;

// Backup
pub extern "c" fn rocksdb_backup_engine_options_create(path: [*:0]const u8) ?*rocksdb_backup_engine_options_t;
pub extern "c" fn rocksdb_backup_engine_options_destroy(options: ?*rocksdb_backup_engine_options_t) void;
pub extern "c" fn rocksdb_backup_engine_open(options: ?*const rocksdb_options_t, path: [*:0]const u8, errptr: ?*?[*:0]u8) ?*rocksdb_backup_engine_t;
pub extern "c" fn rocksdb_backup_engine_close(backup_engine: ?*rocksdb_backup_engine_t) void;
pub extern "c" fn rocksdb_backup_engine_create_new_backup(backup_engine: ?*rocksdb_backup_engine_t, db: ?*rocksdb_t, errptr: ?*?[*:0]u8) void;

// Restore
pub extern "c" fn rocksdb_restore_options_create() ?*rocksdb_restore_options_t;
pub extern "c" fn rocksdb_restore_options_destroy(options: ?*rocksdb_restore_options_t) void;
pub extern "c" fn rocksdb_backup_engine_restore_db_from_latest_backup(
    backup_engine: ?*rocksdb_backup_engine_t,
    db_dir: [*:0]const u8,
    wal_dir: [*:0]const u8,
    restore_options: ?*const rocksdb_restore_options_t,
    errptr: ?*?[*:0]u8,
) void;

// Column Families
pub extern "c" fn rocksdb_create_column_family(db: ?*rocksdb_t, options: ?*const rocksdb_options_t, name: [*:0]const u8, errptr: ?*?[*:0]u8) ?*rocksdb_column_family_handle_t;
pub extern "c" fn rocksdb_column_family_handle_destroy(handle: ?*rocksdb_column_family_handle_t) void;
