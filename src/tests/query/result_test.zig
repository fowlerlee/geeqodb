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

    // Set up the column
    result_set.columns[0].name = try allocator.dupe(u8, "id");
    result_set.columns[0].data_type = .Int32;

    // Add a row with integer value
    try result_set.addRow(&[_]result.Value{
        result.Value{ .integer = 42 },
    });

    // Get the value
    const retrieved_value = result_set.getValue(0, 0);

    // Verify the value
    try testing.expectEqual(result.Value{ .integer = 42 }, retrieved_value);
}

test "ResultSet with float column" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 1);
    defer result_set.deinit();

    // Set up the column
    result_set.columns[0].name = try allocator.dupe(u8, "value");
    result_set.columns[0].data_type = .Float64;

    // Add a row with float value
    try result_set.addRow(&[_]result.Value{
        result.Value{ .float = 3.14159 },
    });

    // Get the value
    const retrieved_value = result_set.getValue(0, 0);

    // Verify the value
    try testing.expectEqual(result.Value{ .float = 3.14159 }, retrieved_value);
}

test "ResultSet with null values" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 2);
    defer result_set.deinit();

    // Set up the column
    result_set.columns[0].name = try allocator.dupe(u8, "id");
    result_set.columns[0].data_type = .Int32;

    // Add rows with values (first is null, second is not)
    try result_set.addRow(&[_]result.Value{
        result.Value{ .null = {} },
    });
    try result_set.addRow(&[_]result.Value{
        result.Value{ .integer = 84 },
    });

    // Get the first value (should be null)
    const null_value = result_set.getValue(0, 0);
    try testing.expectEqual(result.Value{ .null = {} }, null_value);

    // Get the second value
    const retrieved_value = result_set.getValue(1, 0);

    // Verify the value
    try testing.expectEqual(result.Value{ .integer = 84 }, retrieved_value);
}

test "ResultSet out of bounds access" {
    const allocator = testing.allocator;

    // Initialize ResultSet
    var result_set = try ResultSet.init(allocator, 1, 1);
    defer result_set.deinit();

    // Set up the column
    result_set.columns[0].name = try allocator.dupe(u8, "id");
    result_set.columns[0].data_type = .Int32;

    // Add a row with value
    try result_set.addRow(&[_]result.Value{
        result.Value{ .integer = 42 },
    });

    // Try to access out of bounds row (should return null)
    const out_of_bounds_value = result_set.getValue(1, 0);
    try testing.expectEqual(result.Value{ .null = {} }, out_of_bounds_value);

    // Try to access out of bounds column (should return null)
    const out_of_bounds_col_value = result_set.getValue(0, 1);
    try testing.expectEqual(result.Value{ .null = {} }, out_of_bounds_col_value);
}
