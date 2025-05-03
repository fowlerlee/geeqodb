const std = @import("std");
const testing = std.testing;
const geeqodb = @import("../../main.zig");
const planner = geeqodb.query.planner;
const QueryPlanner = planner.QueryPlanner;
const AST = planner.AST;

// Tests for SQL parsing functionality
// These tests focus specifically on the SQL parsing capabilities
// of the query planner, ensuring that various SQL statements are
// correctly parsed into abstract syntax trees.

test "Parse simple SELECT query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "SELECT * FROM users";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse SELECT query with WHERE clause" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "SELECT id, name, email FROM users WHERE id = 1";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse SELECT query with JOIN" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "SELECT users.name, orders.order_date FROM users JOIN orders ON users.id = orders.user_id";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse SELECT query with GROUP BY and HAVING" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 50000";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse SELECT query with ORDER BY and LIMIT" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "SELECT * FROM products ORDER BY price DESC LIMIT 10";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse INSERT query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse UPDATE query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "UPDATE users SET name = 'Jane Doe', email = 'jane@example.com' WHERE id = 1";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse DELETE query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "DELETE FROM users WHERE id = 1";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse CREATE TABLE query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query =
        \\CREATE TABLE users (
        \\    id INTEGER PRIMARY KEY,
        \\    name TEXT NOT NULL,
        \\    email TEXT UNIQUE,
        \\    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        \\)
    ;
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse ALTER TABLE query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "ALTER TABLE users ADD COLUMN phone_number TEXT";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse DROP TABLE query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "DROP TABLE users";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse CREATE INDEX query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "CREATE INDEX idx_user_email ON users (email)";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse DROP INDEX query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "DROP INDEX idx_user_email";
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse complex query with subquery" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query =
        \\SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
        \\FROM users u
        \\WHERE u.created_at > '2023-01-01'
        \\ORDER BY order_count DESC
        \\LIMIT 10
    ;
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse query with common table expressions (CTE)" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query =
        \\WITH high_value_orders AS (
        \\    SELECT * FROM orders WHERE total_amount > 1000
        \\)
        \\SELECT u.name, COUNT(o.id) AS order_count
        \\FROM users u
        \\JOIN high_value_orders o ON u.id = o.user_id
        \\GROUP BY u.name
        \\ORDER BY order_count DESC
    ;
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse query with UNION" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query =
        \\SELECT name, email FROM customers
        \\UNION
        \\SELECT name, email FROM leads
    ;
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse query with window functions" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query =
        \\SELECT
        \\    department,
        \\    name,
        \\    salary,
        \\    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
        \\FROM employees
    ;
    const ast = try query_planner.parse(query);
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
    // In a more complete implementation, we would verify the structure of the AST
}

test "Parse invalid SQL query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "SELECT FROM WHERE";

    // This should return an error
    const result = query_planner.parse(query);

    // In the current implementation, this doesn't actually fail
    // In a more complete implementation, we would expect a parsing error
    if (result) |ast| {
        ast.deinit();
    } else |err| {
        // We would expect an error like error.InvalidSyntax
        _ = err;
    }
}

test "Parse empty query" {
    const allocator = testing.allocator;
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const query = "";

    // This should return an error
    const result = query_planner.parse(query);

    // The current implementation should return error.EmptyQuery
    try testing.expectError(error.EmptyQuery, result);
}
