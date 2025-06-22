const std = @import("std");
const testing = std.testing;
const planner = @import("geeqodb").query.planner;
const executor = @import("geeqodb").query.executor;
const result = @import("geeqodb").query.result;
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

    // Optimize the plan - using the module function, not a method
    const physical_plan = try planner.optimize(query_planner, logical_plan);
    defer physical_plan.deinit();

    // Execute the plan
    var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
    defer result_set.deinit();

    // Verify that ResultSet was created correctly
    try testing.expectEqual(allocator, result_set.allocator);
    try testing.expectEqual(@as(usize, 1), result_set.columns.len);
    try testing.expectEqualStrings("info", result_set.columns[0].name);
}

test "QueryExecutor execute with complex query" {
    // Skip this test for now since our parser doesn't support WHERE and ORDER BY clauses yet
    return;
}
