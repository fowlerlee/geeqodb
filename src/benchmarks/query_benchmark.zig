const std = @import("std");
const geeqodb = @import("geeqodb");
const QueryPlanner = geeqodb.query.planner.QueryPlanner;
const QueryExecutor = geeqodb.query.executor.QueryExecutor;
const DatabaseContext = geeqodb.query.executor.DatabaseContext;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a database context
    var db_context = try DatabaseContext.init(allocator);
    defer db_context.deinit();

    // Benchmark query planning and execution
    std.debug.print("Benchmarking query planning and execution...\n", .{});
    try benchmarkQueryPlanning(allocator, db_context);

    std.debug.print("\nBenchmarks completed successfully!\n", .{});
}

/// Benchmark query planning and execution
fn benchmarkQueryPlanning(allocator: std.mem.Allocator, db_context: *DatabaseContext) !void {
    // Initialize query planner
    const query_planner = try QueryPlanner.init(allocator);
    defer query_planner.deinit();

    const queries = [_][]const u8{
        "SELECT * FROM test",
        "SELECT id, name FROM users WHERE age > 18",
        "SELECT COUNT(*) FROM orders GROUP BY customer_id",
        "SELECT products.name, categories.name FROM products JOIN categories ON products.category_id = categories.id",
    };

    // Benchmark parse operation
    const parse_iterations = 10000;
    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    for (0..parse_iterations) |i| {
        const query = queries[i % queries.len];

        timer.reset();
        const ast = try query_planner.parse(query);
        const elapsed = timer.read();
        total_time += elapsed;
        ast.deinit();
    }

    const avg_parse_time_ns = total_time / parse_iterations;
    const avg_parse_time_ms = @as(f64, @floatFromInt(avg_parse_time_ns)) / 1_000_000.0;
    std.debug.print("Query parse: {d:.3} ms average over {} iterations\n", .{ avg_parse_time_ms, parse_iterations });

    // Benchmark plan operation
    const plan_iterations = 10000;
    total_time = 0;

    for (0..plan_iterations) |i| {
        const query = queries[i % queries.len];
        const ast = try query_planner.parse(query);
        defer ast.deinit();

        timer.reset();
        const logical_plan = try query_planner.plan(ast);
        const elapsed = timer.read();
        total_time += elapsed;
        logical_plan.deinit();
    }

    const avg_plan_time_ns = total_time / plan_iterations;
    const avg_plan_time_ms = @as(f64, @floatFromInt(avg_plan_time_ns)) / 1_000_000.0;
    std.debug.print("Query plan: {d:.3} ms average over {} iterations\n", .{ avg_plan_time_ms, plan_iterations });

    // Benchmark optimize operation
    const optimize_iterations = 10000;
    total_time = 0;

    for (0..optimize_iterations) |i| {
        const query = queries[i % queries.len];
        const ast = try query_planner.parse(query);
        defer ast.deinit();
        const logical_plan = try query_planner.plan(ast);
        defer logical_plan.deinit();

        timer.reset();
        const physical_plan = try query_planner.optimize(logical_plan);
        const elapsed = timer.read();
        total_time += elapsed;
        physical_plan.deinit();
    }

    const avg_optimize_time_ns = total_time / optimize_iterations;
    const avg_optimize_time_ms = @as(f64, @floatFromInt(avg_optimize_time_ns)) / 1_000_000.0;
    std.debug.print("Query optimize: {d:.3} ms average over {} iterations\n", .{ avg_optimize_time_ms, optimize_iterations });

    // Benchmark execute operation
    const execute_iterations = 10000;
    total_time = 0;

    for (0..execute_iterations) |i| {
        const query = queries[i % queries.len];
        const ast = try query_planner.parse(query);
        defer ast.deinit();
        const logical_plan = try query_planner.plan(ast);
        defer logical_plan.deinit();
        const physical_plan = try query_planner.optimize(logical_plan);
        defer physical_plan.deinit();

        timer.reset();
        var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
        const elapsed = timer.read();
        total_time += elapsed;
        result_set.deinit();
    }

    const avg_execute_time_ns = total_time / execute_iterations;
    const avg_execute_time_ms = @as(f64, @floatFromInt(avg_execute_time_ns)) / 1_000_000.0;
    std.debug.print("Query execute: {d:.3} ms average over {} iterations\n", .{ avg_execute_time_ms, execute_iterations });

    // Benchmark end-to-end query operation
    const end_to_end_iterations = 1000;
    total_time = 0;

    for (0..end_to_end_iterations) |i| {
        const query = queries[i % queries.len];

        timer.reset();
        const ast = try query_planner.parse(query);
        const logical_plan = try query_planner.plan(ast);
        const physical_plan = try query_planner.optimize(logical_plan);
        var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
        const elapsed = timer.read();
        total_time += elapsed;

        // Clean up
        result_set.deinit();
        physical_plan.deinit();
        logical_plan.deinit();
        ast.deinit();
    }

    const avg_end_to_end_time_ns = total_time / end_to_end_iterations;
    const avg_end_to_end_time_ms = @as(f64, @floatFromInt(avg_end_to_end_time_ns)) / 1_000_000.0;
    std.debug.print("End-to-end query: {d:.3} ms average over {} iterations\n", .{ avg_end_to_end_time_ms, end_to_end_iterations });
}
