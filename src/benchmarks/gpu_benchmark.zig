const std = @import("std");
const geeqodb = @import("geeqodb");
const planner = geeqodb.query.planner;
const advanced_planner = geeqodb.query.advanced_planner;
const AdvancedQueryPlanner = advanced_planner.AdvancedQueryPlanner;
const QueryPlanner = planner.QueryPlanner;
const QueryExecutor = geeqodb.query.executor.QueryExecutor;
const DatabaseContext = geeqodb.query.executor.DatabaseContext;
const gpu = geeqodb.gpu;
const GpuDevice = gpu.device.GpuDevice;
const benchmark_utils = @import("benchmark_utils.zig");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a database context
    var db_context = try DatabaseContext.init(allocator);
    defer db_context.deinit();

    // Check if GPU is available
    const device_manager = try GpuDevice.init(allocator);
    defer device_manager.deinit();

    const has_gpu = device_manager.hasGpu();
    if (!has_gpu) {
        std.debug.print("No GPU available for benchmarking. Exiting.\n", .{});
        return;
    }

    // Benchmark GPU vs CPU execution
    std.debug.print("Benchmarking GPU vs CPU query execution...\n", .{});
    try benchmarkGpuVsCpu(allocator, db_context);

    // Benchmark different query types with GPU acceleration
    std.debug.print("\nBenchmarking different query types with GPU acceleration...\n", .{});
    try benchmarkQueryTypes(allocator, db_context);

    // Benchmark with different data sizes
    std.debug.print("\nBenchmarking with different data sizes...\n", .{});
    try benchmarkDataSizes(allocator, db_context);

    std.debug.print("\nBenchmarks completed successfully!\n", .{});

    // Write benchmark results to file
    try writeBenchmarkResults(allocator);
}

/// Benchmark GPU vs CPU execution for the same queries
fn benchmarkGpuVsCpu(allocator: std.mem.Allocator, db_context: *DatabaseContext) !void {
    // Initialize planners
    const standard_planner = try QueryPlanner.init(allocator);
    defer standard_planner.deinit();

    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Create test data
    try createTestData(db_context, 1000000);

    // Define test queries
    const queries = [_][]const u8{
        "SELECT * FROM test_table WHERE value > 500000",
        "SELECT COUNT(*) FROM test_table WHERE value BETWEEN 200000 AND 800000",
        "SELECT AVG(value) FROM test_table WHERE value % 2 = 0",
        "SELECT * FROM test_table t1 JOIN test_table t2 ON t1.id = t2.value WHERE t1.id < 1000",
    };

    // Results storage
    var cpu_times = std.ArrayList(u64).init(allocator);
    defer cpu_times.deinit();

    var gpu_times = std.ArrayList(u64).init(allocator);
    defer gpu_times.deinit();

    var query_names = std.ArrayList([]const u8).init(allocator);
    defer query_names.deinit();

    // Run benchmarks
    for (queries) |query| {
        try query_names.append(query);

        // CPU execution
        var timer = std.time.Timer.start() catch unreachable;
        
        const ast = try standard_planner.parse(query);
        defer ast.deinit();
        
        const logical_plan = try standard_planner.plan(ast);
        defer logical_plan.deinit();
        
        const physical_plan = try standard_planner.optimize(logical_plan);
        defer physical_plan.deinit();
        
        var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
        defer result_set.deinit();
        
        const cpu_time = timer.read();
        try cpu_times.append(cpu_time);

        // GPU execution
        timer.reset();
        
        const ast_gpu = try advanced_planner.parse(query);
        defer ast_gpu.deinit();
        
        const logical_plan_gpu = try advanced_planner.plan(ast_gpu);
        defer logical_plan_gpu.deinit();
        
        // Force GPU execution
        advanced_planner.setForceGpu(true);
        const physical_plan_gpu = try advanced_planner.optimize(logical_plan_gpu);
        defer physical_plan_gpu.deinit();
        advanced_planner.setForceGpu(false);
        
        var result_set_gpu = try QueryExecutor.execute(allocator, physical_plan_gpu, db_context);
        defer result_set_gpu.deinit();
        
        const gpu_time = timer.read();
        try gpu_times.append(gpu_time);

        // Print results
        const cpu_time_ms = @as(f64, @floatFromInt(cpu_time)) / 1_000_000.0;
        const gpu_time_ms = @as(f64, @floatFromInt(gpu_time)) / 1_000_000.0;
        const speedup = cpu_time_ms / gpu_time_ms;
        
        std.debug.print("Query: {s}\n", .{query});
        std.debug.print("  CPU time: {d:.3} ms\n", .{cpu_time_ms});
        std.debug.print("  GPU time: {d:.3} ms\n", .{gpu_time_ms});
        std.debug.print("  Speedup: {d:.2}x\n\n", .{speedup});
    }

    // Store results for later writing to file
    gpu_vs_cpu_results = .{
        .query_names = query_names.toOwnedSlice(),
        .cpu_times = cpu_times.toOwnedSlice(),
        .gpu_times = gpu_times.toOwnedSlice(),
    };
}

/// Benchmark different query types with GPU acceleration
fn benchmarkQueryTypes(allocator: std.mem.Allocator, db_context: *DatabaseContext) !void {
    // Initialize advanced planner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Force GPU execution
    advanced_planner.setForceGpu(true);
    defer advanced_planner.setForceGpu(false);

    // Define different query types
    const query_types = [_]struct { name: []const u8, query: []const u8 }{
        .{ .name = "Filter", .query = "SELECT * FROM test_table WHERE value > 500000" },
        .{ .name = "Aggregation", .query = "SELECT COUNT(*), SUM(value), AVG(value) FROM test_table" },
        .{ .name = "Join", .query = "SELECT * FROM test_table t1 JOIN test_table t2 ON t1.id = t2.value WHERE t1.id < 1000" },
        .{ .name = "GroupBy", .query = "SELECT value % 10 AS group_key, COUNT(*) FROM test_table GROUP BY group_key" },
        .{ .name = "OrderBy", .query = "SELECT * FROM test_table ORDER BY value DESC LIMIT 1000" },
    };

    // Results storage
    var query_type_names = std.ArrayList([]const u8).init(allocator);
    defer query_type_names.deinit();

    var execution_times = std.ArrayList(u64).init(allocator);
    defer execution_times.deinit();

    // Run benchmarks
    for (query_types) |query_type| {
        try query_type_names.append(query_type.name);

        var timer = std.time.Timer.start() catch unreachable;
        
        const ast = try advanced_planner.parse(query_type.query);
        defer ast.deinit();
        
        const logical_plan = try advanced_planner.plan(ast);
        defer logical_plan.deinit();
        
        const physical_plan = try advanced_planner.optimize(logical_plan);
        defer physical_plan.deinit();
        
        var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
        defer result_set.deinit();
        
        const execution_time = timer.read();
        try execution_times.append(execution_time);

        // Print results
        const execution_time_ms = @as(f64, @floatFromInt(execution_time)) / 1_000_000.0;
        std.debug.print("Query type: {s}\n", .{query_type.name});
        std.debug.print("  Execution time: {d:.3} ms\n\n", .{execution_time_ms});
    }

    // Store results for later writing to file
    query_type_results = .{
        .query_type_names = query_type_names.toOwnedSlice(),
        .execution_times = execution_times.toOwnedSlice(),
    };
}

/// Benchmark with different data sizes
fn benchmarkDataSizes(allocator: std.mem.Allocator, db_context: *DatabaseContext) !void {
    // Initialize advanced planner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Force GPU execution
    advanced_planner.setForceGpu(true);
    defer advanced_planner.setForceGpu(false);

    // Define different data sizes
    const data_sizes = [_]u32{ 10000, 100000, 1000000, 10000000 };

    // Results storage
    var data_size_values = std.ArrayList(u32).init(allocator);
    defer data_size_values.deinit();

    var execution_times = std.ArrayList(u64).init(allocator);
    defer execution_times.deinit();

    // Test query
    const query = "SELECT * FROM test_table WHERE value > value / 2";

    // Run benchmarks
    for (data_sizes) |data_size| {
        // Create test data with the current size
        try createTestData(db_context, data_size);
        try data_size_values.append(data_size);

        var timer = std.time.Timer.start() catch unreachable;
        
        const ast = try advanced_planner.parse(query);
        defer ast.deinit();
        
        const logical_plan = try advanced_planner.plan(ast);
        defer logical_plan.deinit();
        
        const physical_plan = try advanced_planner.optimize(logical_plan);
        defer physical_plan.deinit();
        
        var result_set = try QueryExecutor.execute(allocator, physical_plan, db_context);
        defer result_set.deinit();
        
        const execution_time = timer.read();
        try execution_times.append(execution_time);

        // Print results
        const execution_time_ms = @as(f64, @floatFromInt(execution_time)) / 1_000_000.0;
        std.debug.print("Data size: {d} rows\n", .{data_size});
        std.debug.print("  Execution time: {d:.3} ms\n\n", .{execution_time_ms});
    }

    // Store results for later writing to file
    data_size_results = .{
        .data_sizes = data_size_values.toOwnedSlice(),
        .execution_times = execution_times.toOwnedSlice(),
    };
}

/// Create test data for benchmarking
fn createTestData(db_context: *DatabaseContext, row_count: u32) !void {
    // Clear existing test data
    _ = try db_context.executeRaw("DROP TABLE IF EXISTS test_table");
    
    // Create test table
    _ = try db_context.executeRaw("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)");
    
    // Insert test data
    var i: u32 = 0;
    while (i < row_count) : (i += 1) {
        const query = try std.fmt.allocPrint(db_context.allocator, "INSERT INTO test_table VALUES ({d}, {d})", .{ i, i * 2 });
        defer db_context.allocator.free(query);
        _ = try db_context.executeRaw(query);
    }
}

// Structures to hold benchmark results
var gpu_vs_cpu_results: struct {
    query_names: []const []const u8,
    cpu_times: []const u64,
    gpu_times: []const u64,
} = undefined;

var query_type_results: struct {
    query_type_names: []const []const u8,
    execution_times: []const u64,
} = undefined;

var data_size_results: struct {
    data_sizes: []const u32,
    execution_times: []const u64,
} = undefined;

/// Write benchmark results to a file
fn writeBenchmarkResults(allocator: std.mem.Allocator) !void {
    var results = std.ArrayList(u8).init(allocator);
    defer results.deinit();

    const writer = results.writer();

    // Write header
    try writer.writeAll("# GeeqoDB GPU Acceleration Benchmark Results\n\n");
    
    // Write GPU vs CPU comparison
    try writer.writeAll("## GPU vs CPU Execution Comparison\n\n");
    try writer.writeAll("| Query | CPU Time (ms) | GPU Time (ms) | Speedup |\n");
    try writer.writeAll("| --- | --- | --- | --- |\n");
    
    for (gpu_vs_cpu_results.query_names, 0..) |query, i| {
        const cpu_time_ms = @as(f64, @floatFromInt(gpu_vs_cpu_results.cpu_times[i])) / 1_000_000.0;
        const gpu_time_ms = @as(f64, @floatFromInt(gpu_vs_cpu_results.gpu_times[i])) / 1_000_000.0;
        const speedup = cpu_time_ms / gpu_time_ms;
        
        try writer.print("| `{s}` | {d:.3} | {d:.3} | {d:.2}x |\n", .{
            query,
            cpu_time_ms,
            gpu_time_ms,
            speedup,
        });
    }
    
    // Write query type comparison
    try writer.writeAll("\n## Query Type Performance with GPU Acceleration\n\n");
    try writer.writeAll("| Query Type | Execution Time (ms) |\n");
    try writer.writeAll("| --- | --- |\n");
    
    for (query_type_results.query_type_names, 0..) |query_type, i| {
        const execution_time_ms = @as(f64, @floatFromInt(query_type_results.execution_times[i])) / 1_000_000.0;
        
        try writer.print("| {s} | {d:.3} |\n", .{
            query_type,
            execution_time_ms,
        });
    }
    
    // Write data size comparison
    try writer.writeAll("\n## Performance with Different Data Sizes\n\n");
    try writer.writeAll("| Data Size (rows) | Execution Time (ms) |\n");
    try writer.writeAll("| --- | --- |\n");
    
    for (data_size_results.data_sizes, 0..) |data_size, i| {
        const execution_time_ms = @as(f64, @floatFromInt(data_size_results.execution_times[i])) / 1_000_000.0;
        
        try writer.print("| {d} | {d:.3} |\n", .{
            data_size,
            execution_time_ms,
        });
    }

    // Write results to file
    try benchmark_utils.writeBenchmarkResults(allocator, "gpu_benchmark", results.items);
}
