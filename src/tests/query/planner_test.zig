const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const planner = geeqodb.query.planner;
const QueryPlanner = planner.QueryPlanner;
const AST = planner.AST;
const LogicalPlan = planner.LogicalPlan;
const PhysicalPlan = planner.PhysicalPlan;

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

test "QueryPlanner optimize" {
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

    // Optimize the plan
    const physical_plan = try query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that PhysicalPlan was created correctly
    try testing.expectEqual(allocator, physical_plan.allocator);
}

test "QueryPlanner end-to-end" {
    const allocator = testing.allocator;

    // Initialize QueryPlanner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    // Parse a SQL query
    const ast = try query_planner.parse("SELECT * FROM test WHERE id = 1");
    defer ast.deinit();

    // Plan the query
    const logical_plan = try query_planner.plan(ast);
    defer logical_plan.deinit();

    // Optimize the plan
    const physical_plan = try query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that PhysicalPlan was created correctly
    try testing.expectEqual(allocator, physical_plan.allocator);
}
