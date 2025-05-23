const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const planner = geeqodb.query.planner;
const advanced_planner = geeqodb.query.advanced_planner;
const AdvancedQueryPlanner = advanced_planner.AdvancedQueryPlanner;
const QueryPlanner = planner.QueryPlanner;
const AST = planner.AST;
const LogicalPlan = planner.LogicalPlan;
const PhysicalPlan = planner.PhysicalPlan;
const Index = geeqodb.storage.index.Index;
const AccessMethod = geeqodb.query.planner.AccessMethod;
const CostModel = geeqodb.query.cost_model.CostModel;
const Statistics = geeqodb.query.statistics.Statistics;
const GpuDevice = geeqodb.gpu.device.GpuDevice;

test "AdvancedQueryPlanner initialization" {
    const allocator = testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_query_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_query_planner.deinit();

    // Verify that AdvancedQueryPlanner was initialized correctly
    try testing.expectEqual(allocator, advanced_query_planner.allocator);
    try testing.expect(advanced_query_planner.cost_model != null);
    try testing.expect(advanced_query_planner.statistics != null);
}

test "AdvancedQueryPlanner cost-based optimization" {
    const allocator = testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_query_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_query_planner.deinit();

    // Create a simple logical plan
    var logical_plan = try allocator.create(LogicalPlan);
    defer allocator.destroy(logical_plan);

    logical_plan.* = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .predicates = null,
        .children = null,
    };
    defer {
        if (logical_plan.table_name) |name| allocator.free(name);
    }

    // Add a predicate for id = 2
    const pred = try allocator.alloc(planner.Predicate, 1);
    defer allocator.free(pred);

    pred[0] = planner.Predicate{
        .column = try allocator.dupe(u8, "id"),
        .op = .Eq,
        .value = planner.PlanValue{ .Integer = 2 },
    };
    defer allocator.free(pred[0].column);

    logical_plan.predicates = pred;

    // Register an index
    try advanced_query_planner.registerIndex("idx_users_id", "users", "id", .BTree, 1000, 1000);

    // Optimize the plan
    const physical_plan = try advanced_query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that the optimizer chose an index seek
    try testing.expectEqual(planner.PhysicalNodeType.IndexSeek, physical_plan.node_type);
    try testing.expectEqual(AccessMethod.IndexSeek, physical_plan.access_method);
}

test "AdvancedQueryPlanner join order optimization" {
    const allocator = testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_query_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_query_planner.deinit();

    // Create a logical plan with a join
    var logical_plan = try allocator.create(LogicalPlan);
    defer allocator.destroy(logical_plan);

    logical_plan.* = LogicalPlan{
        .allocator = allocator,
        .node_type = .Join,
        .table_name = null,
        .columns = null,
        .predicates = null,
        .children = try allocator.alloc(LogicalPlan, 2),
    };
    defer {
        // Free the children array
        if (logical_plan.children) |children| {
            // Free the table_name in each child
            if (children[0].table_name) |name| allocator.free(name);
            if (children[1].table_name) |name| allocator.free(name);
            allocator.free(children);
        }
    }

    // Create left child (users table)
    logical_plan.children.?[0] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .predicates = null,
        .children = null,
    };

    // Create right child (orders table)
    logical_plan.children.?[1] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "orders"),
        .columns = null,
        .predicates = null,
        .children = null,
    };

    // Add statistics for the tables
    try advanced_query_planner.statistics.?.addTableStatistics("users", 1000);
    try advanced_query_planner.statistics.?.addTableStatistics("orders", 10000);

    // Optimize the plan
    const physical_plan = try advanced_query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that the optimizer chose the smaller table as the left side of the join
    try testing.expectEqualStrings("users", physical_plan.children.?[0].table_name.?);
    try testing.expectEqualStrings("orders", physical_plan.children.?[1].table_name.?);
}

test "AdvancedQueryPlanner GPU acceleration" {
    const allocator = testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_query_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_query_planner.deinit();

    // Skip test if no GPU is available
    if (!advanced_query_planner.hasGpuSupport()) {
        std.debug.print("Skipping GPU test - no GPU available\n", .{});
        return;
    }

    // Create a logical plan with a large table scan (which should be GPU accelerated)
    const logical_plan = try allocator.create(LogicalPlan);
    defer allocator.destroy(logical_plan);

    logical_plan.* = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "large_table"),
        .columns = null,
        .predicates = null,
        .children = null,
    };
    defer {
        if (logical_plan.table_name) |name| allocator.free(name);
    }

    // Add statistics for the table (large table)
    try advanced_query_planner.statistics.?.addTableStatistics("large_table", 10000000);

    // Optimize the plan
    const physical_plan = try advanced_query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that the optimizer chose GPU acceleration
    try testing.expect(physical_plan.use_gpu);
}

test "AdvancedQueryPlanner predicate pushdown" {
    const allocator = testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_query_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_query_planner.deinit();

    // Create a logical plan with a join and a filter
    var logical_plan = try allocator.create(LogicalPlan);
    defer allocator.destroy(logical_plan);

    // Create a predicate for users.id = 2
    var predicates = try allocator.alloc(planner.Predicate, 1);
    predicates[0] = planner.Predicate{
        .column = try allocator.dupe(u8, "users.id"),
        .op = .Eq,
        .value = planner.PlanValue{ .Integer = 2 },
    };
    defer {
        // Free the predicate column
        allocator.free(predicates[0].column);
        allocator.free(predicates);
    }

    logical_plan.* = LogicalPlan{
        .allocator = allocator,
        .node_type = .Filter,
        .table_name = null,
        .columns = null,
        .predicates = predicates,
        .children = try allocator.alloc(LogicalPlan, 1),
    };
    defer {
        // Free the children arrays at each level
        if (logical_plan.children) |children| {
            if (children[0].children) |join_children| {
                // Free the table_name in each join child
                if (join_children[0].table_name) |name| allocator.free(name);
                if (join_children[1].table_name) |name| allocator.free(name);
                allocator.free(join_children);
            }
            allocator.free(children);
        }
    }

    // Create child (join)
    logical_plan.children.?[0] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Join,
        .table_name = null,
        .columns = null,
        .predicates = null,
        .children = try allocator.alloc(LogicalPlan, 2),
    };

    // Create join's left child (users table)
    logical_plan.children.?[0].children.?[0] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .predicates = null,
        .children = null,
    };

    // Create join's right child (orders table)
    logical_plan.children.?[0].children.?[1] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "orders"),
        .columns = null,
        .predicates = null,
        .children = null,
    };

    // Optimize the plan
    const physical_plan = try advanced_query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that the predicate was pushed down to the users table
    // The physical plan structure is:
    // PhysicalPlan (root)
    // └── children[0] (Join)
    //     ├── children[0] (users table)
    //     └── children[1] (orders table)

    // Check that the plan was created successfully
    try testing.expect(physical_plan.children != null);
    try testing.expect(physical_plan.children.?[0].children != null);

    // Check that the join node has two children
    try testing.expectEqual(@as(usize, 1), physical_plan.children.?.len);
    try testing.expectEqual(@as(usize, 2), physical_plan.children.?[0].children.?.len);

    // Check that the first child of the join is the users table
    try testing.expectEqualStrings("users", physical_plan.children.?[0].children.?[0].table_name.?);

    // Check that the users table has a predicate
    try testing.expect(physical_plan.children.?[0].children.?[0].predicates != null);

    // Check that the predicate is for the "id" column
    try testing.expectEqualStrings("id", physical_plan.children.?[0].children.?[0].predicates.?[0].column);
}

test "AdvancedQueryPlanner parallel execution planning" {
    const allocator = testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_query_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_query_planner.deinit();

    // Create a logical plan with a large table scan (which should be parallelized)
    const logical_plan = try allocator.create(LogicalPlan);
    defer allocator.destroy(logical_plan);

    logical_plan.* = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "large_table"),
        .columns = null,
        .predicates = null,
        .children = null,
    };
    defer {
        if (logical_plan.table_name) |name| allocator.free(name);
    }

    // Add statistics for the table (large table)
    try advanced_query_planner.statistics.?.addTableStatistics("large_table", 10000000);

    // Optimize the plan
    const physical_plan = try advanced_query_planner.optimize(logical_plan);
    defer physical_plan.deinit();

    // Verify that the optimizer chose parallel execution
    try testing.expect(physical_plan.parallel_degree > 1);
}
