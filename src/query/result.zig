const std = @import("std");
const assert = @import("../build_options.zig").assert;

/// Represents a value in a result set
pub const Value = union(enum) {
    integer: i64,
    float: f64,
    text: []const u8,
    boolean: bool,
    null: void,

    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .text => |text| {
                if (text.len > 0) {
                    allocator.free(text);
                }
            },
            else => {}, // No cleanup needed for other types
        }
    }
};

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

/// Represents a row in a result set
pub const Row = struct {
    values: []Value,

    pub fn deinit(self: *Row, allocator: std.mem.Allocator) void {
        for (self.values) |*value| {
            value.deinit(allocator);
        }
        allocator.free(self.values);
    }
};

/// Represents a result set from a query execution
pub const ResultSet = struct {
    allocator: std.mem.Allocator,
    columns: []ResultColumn,
    rows: []Row,
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

        // Allocate rows
        const rows = try allocator.alloc(Row, row_count);
        for (rows) |*row| {
            if (column_count > 0) {
                const values = try allocator.alloc(Value, column_count);
                for (values) |*value| {
                    value.* = Value{ .null = {} };
                }
                row.* = Row{ .values = values };
            } else {
                row.* = Row{ .values = &[_]Value{} };
            }
        }

        return ResultSet{
            .allocator = allocator,
            .columns = columns,
            .rows = rows,
            .row_count = row_count,
        };
    }

    pub fn deinit(self: *ResultSet) void {
        for (self.columns) |*column| {
            column.deinit(self.allocator);
        }
        self.allocator.free(self.columns);

        for (self.rows) |*row| {
            row.deinit(self.allocator);
        }
        self.allocator.free(self.rows);
    }

    /// Add a row to the result set
    pub fn addRow(self: *ResultSet, values: []const Value) !void {
        if (values.len != self.columns.len) {
            return error.ColumnMismatch;
        }

        const new_row_index = self.row_count;
        if (new_row_index >= self.rows.len) {
            // Grow the rows array
            const new_size = if (self.rows.len == 0) 1 else self.rows.len * 2;
            self.rows = try self.allocator.realloc(self.rows, new_size);

            // Initialize new rows
            for (self.rows[self.row_count..new_size]) |*row| {
                const row_values = try self.allocator.alloc(Value, self.columns.len);
                for (row_values) |*value| {
                    value.* = Value{ .null = {} };
                }
                row.* = Row{ .values = row_values };
            }
        }

        // Copy values to the new row
        for (values, 0..) |value, i| {
            self.rows[new_row_index].values[i] = value;
        }

        self.row_count += 1;
    }

    /// Get a value from the result set
    pub fn getValue(self: ResultSet, row: usize, col: usize) Value {
        if (row >= self.row_count or col >= self.columns.len) {
            return Value{ .null = {} };
        }
        return self.rows[row].values[col];
    }
};

test "ResultSet basic functionality" {
    const allocator = std.testing.allocator;
    var result_set = try ResultSet.init(allocator, 2, 0);
    defer result_set.deinit();

    // Set column names
    result_set.columns[0].name = try allocator.dupe(u8, "id");
    result_set.columns[1].name = try allocator.dupe(u8, "name");

    // Add a row
    try result_set.addRow(&[_]Value{
        Value{ .integer = 1 },
        Value{ .text = try allocator.dupe(u8, "Alice") },
    });

    // Add another row
    try result_set.addRow(&[_]Value{
        Value{ .integer = 2 },
        Value{ .text = try allocator.dupe(u8, "Bob") },
    });

    try std.testing.expectEqual(@as(usize, 2), result_set.columns.len);
    try std.testing.expectEqual(@as(usize, 2), result_set.row_count);

    // Check values
    try std.testing.expectEqual(Value{ .integer = 1 }, result_set.getValue(0, 0));
    try std.testing.expectEqualStrings("Alice", result_set.getValue(0, 1).text);
    try std.testing.expectEqual(Value{ .integer = 2 }, result_set.getValue(1, 0));
    try std.testing.expectEqualStrings("Bob", result_set.getValue(1, 1).text);
}
