const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const planner = geeqodb.query.planner;
const QueryPlanner = planner.QueryPlanner;
const AST = planner.AST;
const LogicalPlan = planner.LogicalPlan;
const Index = geeqodb.storage.index.Index;
const AccessMethod = geeqodb.query.planner.AccessMethod;

test "QueryPlanner initialization" {
    const allocator = testing.allocator;

    // Initialize QueryPlanner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Verify that QueryPlanner was initialized correctly
    try testing.expectEqual(allocator, query_planner.allocator);
}

test "QueryPlanner parse" {
    const allocator = testing.allocator;

    // Initialize QueryPlanner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Parse a SQL query
    const ast = try query_planner.parse("SELECT * FROM test");
    defer ast.deinit();

    // Verify that AST was created correctly
    try testing.expectEqual(allocator, ast.allocator);
}

test "QueryPlanner plan" {
    const allocator = testing.allocator;

    // Initialize QueryPlanner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Parse a SQL query
    const ast = try query_planner.parse("SELECT * FROM test");
    defer ast.deinit();

    // Plan the query
    const logical_plan = try query_planner.plan(ast);
    defer logical_plan.deinit();

    // Verify that LogicalPlan was created correctly
    try testing.expectEqual(allocator, logical_plan.allocator);
}

test "Query planner basic functionality" {
    const allocator = std.testing.allocator;
    var query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Test empty query
    try std.testing.expectError(error.EmptyQuery, query_planner.parse(""));

    // Test simple query
    const ast = try query_planner.parse("SELECT * FROM users");
    defer ast.deinit();

    // Test plan generation
    const plan = try query_planner.plan(ast);
    defer plan.deinit();

    // Verify plan structure
    try std.testing.expectEqual(plan.node_type, .Scan);
}

test "Query planner index selection" {
    const allocator = std.testing.allocator;
    var query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Add test indexes
    try query_planner.addIndex("users", "id", .BTree);
    try query_planner.addIndex("users", "email", .SkipList);

    // Test index lookup
    const indexes = try query_planner.findIndexesForColumn("users", "id");
    defer allocator.free(indexes);

    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqual(indexes[0].index_type, .BTree);

    // Test access method selection
    try std.testing.expectEqual(AccessMethod.IndexSeek, try query_planner.findBestAccessMethod("users", "id"));
    try std.testing.expectEqual(AccessMethod.TableScan, try query_planner.findBestAccessMethod("users", "name"));
}
