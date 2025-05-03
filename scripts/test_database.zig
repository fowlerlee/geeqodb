const std = @import("std");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command-line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Default connection parameters
    var host: []const u8 = "127.0.0.1";
    var port: u16 = 5252;

    // Parse command-line arguments
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--host") or std.mem.eql(u8, arg, "-h")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: Missing value for --host\n", .{});
                return error.InvalidArguments;
            }
            host = args[i];
        } else if (std.mem.eql(u8, arg, "--port") or std.mem.eql(u8, arg, "-p")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: Missing value for --port\n", .{});
                return error.InvalidArguments;
            }
            port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return;
        }
    }

    // Connect to the database server
    const address = try std.net.Address.parseIp(host, port);
    var stream = std.net.tcpConnectToAddress(address) catch |err| {
        std.debug.print("Error connecting to server: {}\n", .{err});
        std.debug.print("Make sure the server is running at {s}:{d}\n", .{ host, port });
        return err;
    };
    defer stream.close();

    std.debug.print("Connected to database server at {}\n", .{address});
    std.debug.print("Running database tests...\n\n", .{});

    // Define test queries
    const test_queries = [_]struct {
        name: []const u8,
        query: []const u8,
    }{
        // Basic SELECT tests
        .{ .name = "Count users", .query = "SELECT COUNT(*) FROM users" },
        .{ .name = "Count products", .query = "SELECT COUNT(*) FROM products" },
        .{ .name = "Count orders", .query = "SELECT COUNT(*) FROM orders" },
        .{ .name = "Count order items", .query = "SELECT COUNT(*) FROM order_items" },

        // More complex SELECT tests
        .{ .name = "Select user by ID", .query = "SELECT * FROM users WHERE id = 1" },
        .{ .name = "Select product by price range", .query = "SELECT * FROM products WHERE price > 500" },
        .{ .name = "Select orders by status", .query = "SELECT * FROM orders WHERE status = 'Processing'" },

        // JOIN tests
        .{ .name = "Join users and orders", .query = "SELECT u.name, o.total_amount FROM users u JOIN orders o ON u.id = o.user_id" },
        .{ .name = "Join orders and order items", .query = "SELECT o.id, oi.product_id, oi.quantity FROM orders o JOIN order_items oi ON o.id = oi.order_id" },
        .{ .name = "Join products and order items", .query = "SELECT p.name, oi.quantity, oi.price FROM products p JOIN order_items oi ON p.id = oi.product_id" },

        // Complex JOIN with multiple tables
        .{ .name = "Complex join across all tables", .query = "SELECT u.name, p.name, oi.quantity, o.status FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id" },

        // Aggregation tests
        .{ .name = "Sum of order totals", .query = "SELECT SUM(total_amount) FROM orders" },
        .{ .name = "Average product price", .query = "SELECT AVG(price) FROM products" },
        .{ .name = "Min and max product price", .query = "SELECT MIN(price), MAX(price) FROM products" },

        // GROUP BY tests
        .{ .name = "Orders grouped by status", .query = "SELECT status, COUNT(*) FROM orders GROUP BY status" },
        .{ .name = "Order items grouped by product", .query = "SELECT product_id, SUM(quantity) FROM order_items GROUP BY product_id" },

        // ORDER BY tests
        .{ .name = "Products ordered by price", .query = "SELECT name, price FROM products ORDER BY price DESC" },
        .{ .name = "Users ordered by name", .query = "SELECT name, email FROM users ORDER BY name ASC" },

        // LIMIT tests
        .{ .name = "Top 3 most expensive products", .query = "SELECT name, price FROM products ORDER BY price DESC LIMIT 3" },

        // UPDATE test
        .{ .name = "Update product price", .query = "UPDATE products SET price = 1099.99 WHERE id = 1" },

        // Verify UPDATE worked
        .{ .name = "Verify price update", .query = "SELECT name, price FROM products WHERE id = 1" },

        // DELETE test
        .{ .name = "Delete a product", .query = "DELETE FROM products WHERE id = 5" },

        // Verify DELETE worked
        .{ .name = "Verify product deletion", .query = "SELECT COUNT(*) FROM products" },

        // Test transaction (if supported)
        .{ .name = "Begin transaction", .query = "BEGIN TRANSACTION" },
        .{ .name = "Insert in transaction", .query = "INSERT INTO products (id, name, description, price, stock) VALUES (6, 'Speaker', 'Bluetooth speaker', 79.99, 100)" },
        .{ .name = "Commit transaction", .query = "COMMIT" },

        // Verify transaction worked
        .{ .name = "Verify transaction", .query = "SELECT * FROM products WHERE id = 6" },
    };

    var passed: usize = 0;
    var failed: usize = 0;

    // Execute test queries
    for (test_queries) |test_case| {
        std.debug.print("Test: {s}\n", .{test_case.name});
        std.debug.print("Query: {s}\n", .{test_case.query});

        // Send query to server
        _ = try stream.write(test_case.query);

        // Read response from server
        var response_buffer: [4096]u8 = undefined;
        const bytes_read = try stream.read(&response_buffer);

        if (bytes_read == 0) {
            std.debug.print("Server closed the connection.\n", .{});
            return error.ConnectionClosed;
        }

        const response = response_buffer[0..bytes_read];
        std.debug.print("Response: {s}\n", .{response});

        // Check if the response indicates success
        if (std.mem.indexOf(u8, response, "SUCCESS") != null) {
            std.debug.print("Result: PASSED\n", .{});
            passed += 1;
        } else {
            std.debug.print("Result: FAILED\n", .{});
            failed += 1;
        }

        std.debug.print("\n", .{});
    }

    // Print test summary
    std.debug.print("Test Summary:\n", .{});
    std.debug.print("  Total tests: {d}\n", .{test_queries.len});
    std.debug.print("  Passed: {d}\n", .{passed});
    std.debug.print("  Failed: {d}\n", .{failed});

    if (failed == 0) {
        std.debug.print("\nAll tests passed! The database server is working correctly.\n", .{});
    } else {
        std.debug.print("\nSome tests failed. Please check the database server implementation.\n", .{});
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: test_database [options]
        \\
        \\Options:
        \\  --host, -h <host>  Server hostname or IP address (default: 127.0.0.1)
        \\  --port, -p <port>  Server port (default: 5252)
        \\  --help             Show this help message
        \\
        \\Example:
        \\  test_database --host 127.0.0.1 --port 5252
        \\
    , .{});
}
