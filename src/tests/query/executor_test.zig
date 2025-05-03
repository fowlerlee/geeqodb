const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const planner = geeqodb.query.planner;
const executor = geeqodb.query.executor;
const result = geeqodb.query.result;
const QueryPlanner = planner.QueryPlanner;
const QueryExecutor = executor.QueryExecutor;
const DatabaseContext = executor.DatabaseContext;
const ResultSet = result.ResultSet;

test "QueryExecutor execute" {
    const allocator = testing.allocator;

    // Initialize QueryPlanner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Initialize DatabaseContext
    const db_context = try DatabaseContext.init(allocator);
    defer db_context.deinit();

    // Parse a SQL query
    const ast = try query_planner.parse("SELECT * FROM test");
    defer ast.deinit();

    // Plan the query
    const logical_plan = try query_planner.plan(ast);
    defer logical_plan.deinit();

    // Optimize the plan
    const physical_plan = try query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Execute the plan
    var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
    defer result_set.deinit();

    // Verify that ResultSet was created correctly
    try testing.expectEqual(allocator, result_set.allocator);
    try testing.expectEqual(@as(usize, 0), result_set.columns.len);
    try testing.expectEqual(@as(usize, 0), result_set.row_count);
}

test "QueryExecutor execute with complex query" {
    const allocator = testing.allocator;

    // Initialize QueryPlanner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Initialize DatabaseContext
    const db_context = try DatabaseContext.init(allocator);
    defer db_context.deinit();

    // Parse a SQL query
    const ast = try query_planner.parse("SELECT id, name FROM test WHERE id > 10 ORDER BY name");
    defer ast.deinit();

    // Plan the query
    const logical_plan = try query_planner.plan(ast);
    defer logical_plan.deinit();

    // Optimize the plan
    const physical_plan = try query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Execute the plan
    var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
    defer result_set.deinit();

    // Verify that ResultSet was created correctly
    try testing.expectEqual(allocator, result_set.allocator);
    try testing.expectEqual(@as(usize, 0), result_set.columns.len);
    try testing.expectEqual(@as(usize, 0), result_set.row_count);
}
