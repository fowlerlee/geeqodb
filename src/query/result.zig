const std = @import("std");
const assert = @import("../build_options.zig").assert;

/// Represents a column in a result set
pub const ResultColumn = struct {
    name: []const u8,
    data_type: DataType,
    data: []const u8,
    null_bitmap: ?[]const u8,
    row_count: usize,

    pub fn deinit(self: *ResultColumn, allocator: std.mem.Allocator) void {
        // Only free if the slice is not empty and not a static array
        if (self.name.len > 0 and !isStaticArray(self.name)) {
            allocator.free(self.name);
        }
        if (self.data.len > 0 and !isStaticArray(self.data)) {
            allocator.free(self.data);
        }
        if (self.null_bitmap) |bitmap| {
            if (bitmap.len > 0 and !isStaticArray(bitmap)) {
                allocator.free(bitmap);
            }
        }
    }

    // Helper function to check if a slice is a static array
    fn isStaticArray(slice: []const u8) bool {
        return @intFromPtr(slice.ptr) < 0x1000 or slice.len == 0;
    }
};

/// Represents the data type of a column
pub const DataType = enum {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Bool,
    String,
    Date,
    Timestamp,
};

/// Represents a result set from a query execution
pub const ResultSet = struct {
    allocator: std.mem.Allocator,
    columns: []ResultColumn,
    row_count: usize,

    pub fn init(allocator: std.mem.Allocator, column_count: usize, row_count: usize) !ResultSet {
        const columns = try allocator.alloc(ResultColumn, column_count);

        // Initialize columns with empty values
        for (columns) |*column| {
            column.* = ResultColumn{
                .name = "",
                .data_type = .Int32, // Default type
                .data = &[_]u8{},
                .null_bitmap = null,
                .row_count = 0,
            };
        }

        return ResultSet{
            .allocator = allocator,
            .columns = columns,
            .row_count = row_count,
        };
    }

    pub fn deinit(self: *ResultSet) void {
        for (self.columns) |*column| {
            column.deinit(self.allocator);
        }
        self.allocator.free(self.columns);
    }

    /// Get a value from the result set
    pub fn getValue(self: ResultSet, row: usize, col: usize, comptime T: type) !T {
        if (row >= self.row_count or col >= self.columns.len) {
            return error.IndexOutOfBounds;
        }

        const column = self.columns[col];

        // Check if the value is NULL
        if (column.null_bitmap) |bitmap| {
            const byte_index = row / 8;
            const bit_index = @as(u3, @truncate(row % 8));
            if ((bitmap[byte_index] & (@as(u8, 1) << bit_index)) == 0) {
                return error.NullValue;
            }
        }

        // Get the value based on the data type
        switch (column.data_type) {
            .Int8, .Int16, .Int32, .Int64, .UInt8, .UInt16, .UInt32, .UInt64, .Float32, .Float64, .Bool => {
                const size = @sizeOf(T);
                const offset = row * size;

                if (offset + size > column.data.len) {
                    return error.IndexOutOfBounds;
                }

                return @bitCast(column.data[offset..][0..size].*);
            },
            .String, .Date, .Timestamp => {
                // For string types, the data contains offsets to the actual strings
                // This is a simplified implementation
                return error.NotImplemented;
            },
        }
    }
};

test "ResultSet basic functionality" {
    const allocator = std.testing.allocator;
    var result_set = try ResultSet.init(allocator, 2, 10);
    defer result_set.deinit();

    try std.testing.expectEqual(@as(usize, 2), result_set.columns.len);
    try std.testing.expectEqual(@as(usize, 10), result_set.row_count);
}
