const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const result = geeqodb.query.result;
const ResultSet = result.ResultSet;
const ResultColumn = result.ResultColumn;
const DataType = result.DataType;

test "ResultSet initialization" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 2, 10);
    defer result_set.deinit();

    // Verify that ResultSet was initialized correctly
    try testing.expectEqual(allocator, result_set.allocator);
    try testing.expectEqual(@as(usize, 2), result_set.columns.len);
    try testing.expectEqual(@as(usize, 10), result_set.row_count);
}

test "ResultSet with integer column" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 1);
    defer result_set.deinit();

    // Create a column with integer data
    const column_name = try allocator.dupe(u8, "id");
    const data = try allocator.alloc(u8, @sizeOf(i32));
    const value: i32 = 42;
    @memcpy(data[0..@sizeOf(i32)], std.mem.asBytes(&value));

    // Set up the column
    result_set.columns[0] = ResultColumn{
        .name = column_name,
        .data_type = .Int32,
        .data = data,
        .null_bitmap = null,
        .row_count = 1,
    };

    // Get the value
    const retrieved_value = try result_set.getValue(0, 0, i32);

    // Verify the value
    try testing.expectEqual(@as(i32, 42), retrieved_value);
}

test "ResultSet with float column" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 1);
    defer result_set.deinit();

    // Create a column with float data
    const column_name = try allocator.dupe(u8, "value");
    const data = try allocator.alloc(u8, @sizeOf(f64));
    const value: f64 = 3.14159;
    @memcpy(data[0..@sizeOf(f64)], std.mem.asBytes(&value));

    // Set up the column
    result_set.columns[0] = ResultColumn{
        .name = column_name,
        .data_type = .Float64,
        .data = data,
        .null_bitmap = null,
        .row_count = 1,
    };

    // Get the value
    const retrieved_value = try result_set.getValue(0, 0, f64);

    // Verify the value
    try testing.expectEqual(@as(f64, 3.14159), retrieved_value);
}

test "ResultSet with null values" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 2);
    defer result_set.deinit();

    // Create a column with integer data
    const column_name = try allocator.dupe(u8, "id");
    const data = try allocator.alloc(u8, 2 * @sizeOf(i32));
    const value1: i32 = 42;
    const value2: i32 = 84;
    @memcpy(data[0..@sizeOf(i32)], std.mem.asBytes(&value1));
    @memcpy(data[@sizeOf(i32) .. 2 * @sizeOf(i32)], std.mem.asBytes(&value2));

    // Create a null bitmap (first value is null, second is not)
    var null_bitmap = try allocator.alloc(u8, 1);
    null_bitmap[0] = 0b00000010; // Second bit is set (second value is not null)

    // Set up the column
    result_set.columns[0] = ResultColumn{
        .name = column_name,
        .data_type = .Int32,
        .data = data,
        .null_bitmap = null_bitmap,
        .row_count = 2,
    };

    // Try to get the first value (should be null)
    try testing.expectError(error.NullValue, result_set.getValue(0, 0, i32));

    // Get the second value
    const retrieved_value = try result_set.getValue(1, 0, i32);

    // Verify the value
    try testing.expectEqual(@as(i32, 84), retrieved_value);
}

test "ResultSet out of bounds access" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 1);
    defer result_set.deinit();

    // Create a column with integer data
    const column_name = try allocator.dupe(u8, "id");
    const data = try allocator.alloc(u8, @sizeOf(i32));
    const value: i32 = 42;
    @memcpy(data[0..@sizeOf(i32)], std.mem.asBytes(&value));

    // Set up the column
    result_set.columns[0] = ResultColumn{
        .name = column_name,
        .data_type = .Int32,
        .data = data,
        .null_bitmap = null,
        .row_count = 1,
    };

    // Try to access out of bounds row
    try testing.expectError(error.IndexOutOfBounds, result_set.getValue(1, 0, i32));

    // Try to access out of bounds column
    try testing.expectError(error.IndexOutOfBounds, result_set.getValue(0, 1, i32));
}
