const std = @import("std");

// Import build options
pub const build_options = @import("build_options.zig");

pub const core = @import("core/database.zig");
pub const storage = struct {
    pub const rocksdb = @import("storage/rocksdb.zig");
    pub const wal = @import("storage/wal.zig");
};
pub const query = struct {
    pub const planner = @import("query/planner.zig");
    pub const executor = @import("query/executor.zig");
    pub const result = @import("query/result.zig");
};
pub const transaction = struct {
    pub const manager = @import("transaction/manager.zig");
};

pub fn main() !void {
    std.debug.print("GeeqoDB - A high-performance OLAP database in Zig\n", .{});
}

test {
    std.testing.refAllDeclsRecursive(@This());
}
