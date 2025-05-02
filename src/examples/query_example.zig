const std = @import("std");
const geeqodb = @import("geeqodb");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create data directory
    const data_dir = "query_test_data";
    try std.fs.cwd().makePath(data_dir);

    // Initialize database
    std.debug.print("Initializing database...\n", .{});
    const db = try geeqodb.core.init(allocator, data_dir);
    defer db.deinit();
    std.debug.print("Database initialized successfully!\n", .{});

    // Execute different types of queries
    const queries = [_][]const u8{
        "SELECT * FROM test",
        "SELECT id, name FROM users WHERE age > 18",
        "SELECT COUNT(*) FROM orders GROUP BY customer_id",
        "SELECT products.name, categories.name FROM products JOIN categories ON products.category_id = categories.id",
    };

    for (queries, 0..) |query, i| {
        std.debug.print("\nExecuting query {}: {s}\n", .{ i + 1, query });
        var result_set = try db.execute(query);
        defer result_set.deinit();
        std.debug.print("Query executed successfully!\n", .{});
        std.debug.print("Result set has {} columns and {} rows\n", .{ result_set.columns.len, result_set.row_count });
    }

    std.debug.print("\nQuery test completed successfully!\n", .{});
}
