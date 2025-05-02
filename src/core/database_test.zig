const std = @import("std");
const database = @import("database.zig");

test "Database initialization and query execution" {
    const allocator = std.testing.allocator;
    
    // Initialize the database
    const db = try database.init(allocator, "test_data");
    defer db.deinit();
    
    // Execute a simple query
    var result_set = try db.execute("SELECT * FROM test");
    defer result_set.deinit();
    
    // Verify the result set
    try std.testing.expectEqual(@as(usize, 0), result_set.columns.len);
    try std.testing.expectEqual(@as(usize, 0), result_set.row_count);
}
