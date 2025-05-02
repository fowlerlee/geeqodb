const std = @import("std");
const geeqodb = @import("geeqodb");
const OLAPDatabase = geeqodb.core.OLAPDatabase;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create data directory
    const data_dir = "benchmark_data";
    try std.fs.cwd().makePath(data_dir);

    // Initialize database
    std.debug.print("Initializing database...\n", .{});
    const db = try geeqodb.core.init(allocator, data_dir);
    defer db.deinit();
    std.debug.print("Database initialized successfully!\n", .{});

    // Benchmark database initialization
    std.debug.print("\nBenchmarking database initialization...\n", .{});
    try benchmarkDatabaseInit(allocator, data_dir);

    // Benchmark query execution
    std.debug.print("\nBenchmarking query execution...\n", .{});
    try benchmarkQueryExecution(db);

    std.debug.print("\nBenchmarks completed successfully!\n", .{});
}

/// Benchmark database initialization
fn benchmarkDatabaseInit(allocator: std.mem.Allocator, data_dir: []const u8) !void {
    const iterations = 100;
    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    for (0..iterations) |_| {
        timer.reset();
        const db = try geeqodb.core.init(allocator, data_dir);
        const elapsed = timer.read();
        total_time += elapsed;
        db.deinit();
    }

    const avg_time_ns = total_time / iterations;
    const avg_time_ms = @as(f64, @floatFromInt(avg_time_ns)) / 1_000_000.0;
    std.debug.print("Database initialization: {d:.3} ms average over {} iterations\n", .{ avg_time_ms, iterations });
}

/// Benchmark query execution
fn benchmarkQueryExecution(db: *OLAPDatabase) !void {
    const queries = [_][]const u8{
        "SELECT * FROM test",
        "SELECT id, name FROM users WHERE age > 18",
        "SELECT COUNT(*) FROM orders GROUP BY customer_id",
        "SELECT products.name, categories.name FROM products JOIN categories ON products.category_id = categories.id",
    };

    const iterations = 1000;

    for (queries, 0..) |query, i| {
        var timer = try std.time.Timer.start();
        var total_time: u64 = 0;

        for (0..iterations) |_| {
            timer.reset();
            var result_set = try db.execute(query);
            const elapsed = timer.read();
            total_time += elapsed;
            result_set.deinit();
        }

        const avg_time_ns = total_time / iterations;
        const avg_time_ms = @as(f64, @floatFromInt(avg_time_ns)) / 1_000_000.0;
        std.debug.print("Query {}: {d:.3} ms average over {} iterations\n", .{ i + 1, avg_time_ms, iterations });
        std.debug.print("  Query: {s}\n", .{query});
    }
}
