const std = @import("std");
const geeqodb = @import("geeqodb");
const database = geeqodb.core;
const OLAPDatabase = database.OLAPDatabase;
const BTreeMapIndex = geeqodb.storage.btree_index.BTreeMapIndex;
const SkipListIndex = geeqodb.storage.skiplist_index.SkipListIndex;
const DatabaseContext = geeqodb.query.executor.DatabaseContext;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create data directory
    const data_dir = "benchmark_data";
    try std.fs.cwd().makePath(data_dir);

    // Benchmark parameters
    const num_rows = 100_000;
    const num_queries = 1000;
    const num_range_queries = 100;
    const range_size = 1000;

    std.debug.print("Running index query benchmarks with {d} rows...\n", .{num_rows});

    // Create and seed the database
    std.debug.print("\nCreating and seeding the database...\n", .{});
    const db = try createAndSeedDatabase(allocator, data_dir, num_rows);
    defer db.deinit();

    // Benchmark 1: Point queries without index
    std.debug.print("\nBenchmark 1: Point queries without index ({d} queries)...\n", .{num_queries});
    const point_query_time_no_index = try benchmarkPointQueries(allocator, db, num_queries, false);

    // Create and register indexes
    std.debug.print("\nCreating and registering indexes...\n", .{});
    try createAndRegisterIndexes(allocator, db, num_rows);

    // Benchmark 2: Point queries with index
    std.debug.print("\nBenchmark 2: Point queries with index ({d} queries)...\n", .{num_queries});
    const point_query_time_with_index = try benchmarkPointQueries(allocator, db, num_queries, true);

    // Benchmark 3: Range queries without index
    std.debug.print("\nBenchmark 3: Range queries without index ({d} queries)...\n", .{num_range_queries});
    const range_query_time_no_index = try benchmarkRangeQueries(allocator, db, num_range_queries, range_size, false);

    // Benchmark 4: Range queries with index
    std.debug.print("\nBenchmark 4: Range queries with index ({d} queries)...\n", .{num_range_queries});
    const range_query_time_with_index = try benchmarkRangeQueries(allocator, db, num_range_queries, range_size, true);

    // Benchmark 5: Join queries without index
    std.debug.print("\nBenchmark 5: Join queries without index ({d} queries)...\n", .{num_queries / 10});
    const join_query_time_no_index = try benchmarkJoinQueries(allocator, db, num_queries / 10, false);

    // Benchmark 6: Join queries with index
    std.debug.print("\nBenchmark 6: Join queries with index ({d} queries)...\n", .{num_queries / 10});
    const join_query_time_with_index = try benchmarkJoinQueries(allocator, db, num_queries / 10, true);

    // Calculate speedups
    const point_query_speedup = @as(f64, @floatFromInt(point_query_time_no_index)) / @as(f64, @floatFromInt(point_query_time_with_index));
    const range_query_speedup = @as(f64, @floatFromInt(range_query_time_no_index)) / @as(f64, @floatFromInt(range_query_time_with_index));
    const join_query_speedup = @as(f64, @floatFromInt(join_query_time_no_index)) / @as(f64, @floatFromInt(join_query_time_with_index));

    // Create the benchmark_results directory if it doesn't exist
    try std.fs.cwd().makePath("benchmark_results");

    // Get the current timestamp
    const timestamp = std.time.timestamp();
    const timestamp_str = try std.fmt.allocPrint(allocator, "{d}", .{timestamp});
    defer allocator.free(timestamp_str);

    // Create the filename with timestamp
    const filename = try std.fmt.allocPrint(allocator, "benchmark_results/index_query_benchmark_{s}.txt", .{timestamp_str});
    defer allocator.free(filename);

    // Open the file for writing
    const file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    // Write the results to the file
    try file.writer().print("# GeeqoDB Index Performance Benchmark Results\n\n", .{});
    try file.writer().print("| Query Type | Without Index (ns) | With Index (ns) | Speedup Factor |\n", .{});
    try file.writer().print("| --- | --- | --- | --- |\n", .{});
    try file.writer().print("| Point Query | {d} | {d} | {d:.2}x |\n", .{point_query_time_no_index, point_query_time_with_index, point_query_speedup});
    try file.writer().print("| Range Query | {d} | {d} | {d:.2}x |\n", .{range_query_time_no_index, range_query_time_with_index, range_query_speedup});
    try file.writer().print("| Join Query | {d} | {d} | {d:.2}x |\n", .{join_query_time_no_index, join_query_time_with_index, join_query_speedup});

    std.debug.print("\nBenchmark results written to {s}\n", .{filename});

    // Print a summary of the results
    std.debug.print("\nBenchmark Summary:\n", .{});
    std.debug.print("Point Query Speedup: {d:.2}x\n", .{point_query_speedup});
    std.debug.print("Range Query Speedup: {d:.2}x\n", .{range_query_speedup});
    std.debug.print("Join Query Speedup: {d:.2}x\n", .{join_query_speedup});

    std.debug.print("\nBenchmarks completed successfully!\n", .{});
}

/// Create and seed the database with test data
fn createAndSeedDatabase(allocator: std.mem.Allocator, data_dir: []const u8, num_rows: usize) !*OLAPDatabase {
    // Initialize the database
    const db = try database.init(allocator, data_dir);

    // Create test tables
    _ = try db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
    _ = try db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)");

    // Insert test data into users table
    std.debug.print("Inserting {d} rows into users table...\n", .{num_rows});
    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        const age = i % 100; // Ages 0-99
        const query = try std.fmt.allocPrint(allocator, "INSERT INTO users VALUES ({d}, 'User {d}', {d})", .{ i, i, age });
        defer allocator.free(query);
        _ = try db.execute(query);

        if (i % 10000 == 0 and i > 0) {
            std.debug.print("Inserted {d} rows...\n", .{i});
        }
    }

    // Insert test data into orders table (each user has 1-5 orders)
    std.debug.print("Inserting rows into orders table...\n", .{});
    i = 0;
    var order_id: usize = 0;
    while (i < num_rows) : (i += 1) {
        const num_orders = (i % 5) + 1;
        var j: usize = 0;
        while (j < num_orders) : (j += 1) {
            const amount = (order_id % 1000) * 10; // Amounts 0-9990
            const query = try std.fmt.allocPrint(allocator, "INSERT INTO orders VALUES ({d}, {d}, {d})", .{ order_id, i, amount });
            defer allocator.free(query);
            _ = try db.execute(query);
            order_id += 1;
        }

        if (i % 10000 == 0 and i > 0) {
            std.debug.print("Processed {d} users...\n", .{i});
        }
    }

    std.debug.print("Database seeded with {d} users and {d} orders.\n", .{ num_rows, order_id });

    return db;
}

/// Create and register indexes for the database
fn createAndRegisterIndexes(allocator: std.mem.Allocator, db: *OLAPDatabase, num_rows: usize) !void {
    // Create indexes
    const user_id_index = try BTreeMapIndex.create(allocator, "idx_users_id", "users", "id");
    const user_age_index = try BTreeMapIndex.create(allocator, "idx_users_age", "users", "age");
    const order_id_index = try BTreeMapIndex.create(allocator, "idx_orders_id", "orders", "id");
    const order_user_id_index = try BTreeMapIndex.create(allocator, "idx_orders_user_id", "orders", "user_id");
    const order_amount_index = try BTreeMapIndex.create(allocator, "idx_orders_amount", "orders", "amount");

    // Populate user_id_index
    std.debug.print("Populating user_id_index...\n", .{});
    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        try user_id_index.insert(@intCast(i), @intCast(i)); // id -> row_id

        if (i % 10000 == 0 and i > 0) {
            std.debug.print("Indexed {d} user IDs...\n", .{i});
        }
    }

    // Populate user_age_index
    std.debug.print("Populating user_age_index...\n", .{});
    i = 0;
    while (i < num_rows) : (i += 1) {
        const age = i % 100; // Ages 0-99
        try user_age_index.insert(@intCast(age), @intCast(i)); // age -> row_id

        if (i % 10000 == 0 and i > 0) {
            std.debug.print("Indexed {d} user ages...\n", .{i});
        }
    }

    // Populate order indexes
    std.debug.print("Populating order indexes...\n", .{});
    i = 0;
    var order_id: usize = 0;
    while (i < num_rows) : (i += 1) {
        const num_orders = (i % 5) + 1;
        var j: usize = 0;
        while (j < num_orders) : (j += 1) {
            const amount = (order_id % 1000) * 10; // Amounts 0-9990

            try order_id_index.insert(@intCast(order_id), @intCast(order_id)); // id -> row_id
            try order_user_id_index.insert(@intCast(i), @intCast(order_id)); // user_id -> row_id
            try order_amount_index.insert(@intCast(amount), @intCast(order_id)); // amount -> row_id

            order_id += 1;
        }

        if (i % 10000 == 0 and i > 0) {
            std.debug.print("Indexed orders for {d} users...\n", .{i});
        }
    }

    // Register indexes with the database context
    try db.db_context.registerBTreeIndex("idx_users_id", user_id_index);
    try db.db_context.registerBTreeIndex("idx_users_age", user_age_index);
    try db.db_context.registerBTreeIndex("idx_orders_id", order_id_index);
    try db.db_context.registerBTreeIndex("idx_orders_user_id", order_user_id_index);
    try db.db_context.registerBTreeIndex("idx_orders_amount", order_amount_index);

    // Register indexes with the query planner
    try db.query_planner.registerIndex("idx_users_id", "users", "id", .BTree, num_rows, num_rows);
    try db.query_planner.registerIndex("idx_users_age", "users", "age", .BTree, num_rows, 100); // 100 distinct ages
    try db.query_planner.registerIndex("idx_orders_id", "orders", "id", .BTree, order_id, order_id);
    try db.query_planner.registerIndex("idx_orders_user_id", "orders", "user_id", .BTree, order_id, num_rows);
    try db.query_planner.registerIndex("idx_orders_amount", "orders", "amount", .BTree, order_id, 1000); // 1000 distinct amounts

    std.debug.print("Indexes created and registered successfully.\n", .{});
}

/// Benchmark point queries with or without indexes
fn benchmarkPointQueries(allocator: std.mem.Allocator, db: *OLAPDatabase, num_queries: usize, use_index: bool) !u64 {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    // If not using indexes, temporarily disable them
    var original_context: ?*DatabaseContext = null;
    var empty_context: ?*DatabaseContext = null;
    
    if (!use_index) {
        // Save the original context
        original_context = db.db_context;
        // Create a new empty context
        empty_context = try DatabaseContext.init(allocator);
        // Replace the context with the empty one
        db.db_context = empty_context.?;
    }
    defer {
        // Restore the original context if we saved it
        if (original_context != null) {
            const temp_context = db.db_context;
            db.db_context = original_context.?;
            if (empty_context != null) {
                empty_context.?.deinit();
            }
        }
    }

    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    var i: usize = 0;
    while (i < num_queries) : (i += 1) {
        const user_id = random.intRangeAtMost(usize, 0, 99999);
        const query = try std.fmt.allocPrint(allocator, "SELECT * FROM users WHERE id = {d}", .{user_id});
        defer allocator.free(query);

        timer.reset();
        var result_set = try db.execute(query);
        const elapsed = timer.read();
        total_time += elapsed;

        result_set.deinit();

        if (i % 100 == 0 and i > 0) {
            std.debug.print("Executed {d} point queries...\n", .{i});
        }
    }

    const avg_time = total_time / num_queries;
    std.debug.print("Average point query time: {d} ns\n", .{avg_time});

    return avg_time;
}

/// Benchmark range queries with or without indexes
fn benchmarkRangeQueries(allocator: std.mem.Allocator, db: *OLAPDatabase, num_queries: usize, range_size: usize, use_index: bool) !u64 {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    // If not using indexes, temporarily disable them
    var original_context: ?*DatabaseContext = null;
    var empty_context: ?*DatabaseContext = null;
    
    if (!use_index) {
        // Save the original context
        original_context = db.db_context;
        // Create a new empty context
        empty_context = try DatabaseContext.init(allocator);
        // Replace the context with the empty one
        db.db_context = empty_context.?;
    }
    defer {
        // Restore the original context if we saved it
        if (original_context != null) {
            const temp_context = db.db_context;
            db.db_context = original_context.?;
            if (empty_context != null) {
                empty_context.?.deinit();
            }
        }
    }

    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    var i: usize = 0;
    while (i < num_queries) : (i += 1) {
        const min_age = random.intRangeAtMost(usize, 0, 99 - range_size / 100);
        const max_age = min_age + range_size / 100;

        const query = try std.fmt.allocPrint(allocator, "SELECT * FROM users WHERE age BETWEEN {d} AND {d}", .{ min_age, max_age });
        defer allocator.free(query);

        timer.reset();
        var result_set = try db.execute(query);
        const elapsed = timer.read();
        total_time += elapsed;

        result_set.deinit();

        if (i % 10 == 0 and i > 0) {
            std.debug.print("Executed {d} range queries...\n", .{i});
        }
    }

    const avg_time = total_time / num_queries;
    std.debug.print("Average range query time: {d} ns\n", .{avg_time});

    return avg_time;
}

/// Benchmark join queries with or without indexes
fn benchmarkJoinQueries(allocator: std.mem.Allocator, db: *OLAPDatabase, num_queries: usize, use_index: bool) !u64 {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    // If not using indexes, temporarily disable them
    var original_context: ?*DatabaseContext = null;
    var empty_context: ?*DatabaseContext = null;
    
    if (!use_index) {
        // Save the original context
        original_context = db.db_context;
        // Create a new empty context
        empty_context = try DatabaseContext.init(allocator);
        // Replace the context with the empty one
        db.db_context = empty_context.?;
    }
    defer {
        // Restore the original context if we saved it
        if (original_context != null) {
            const temp_context = db.db_context;
            db.db_context = original_context.?;
            if (empty_context != null) {
                empty_context.?.deinit();
            }
        }
    }

    var timer = try std.time.Timer.start();
    var total_time: u64 = 0;

    var i: usize = 0;
    while (i < num_queries) : (i += 1) {
        const user_id = random.intRangeAtMost(usize, 0, 99999);

        const query = try std.fmt.allocPrint(allocator, "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = {d}", .{user_id});
        defer allocator.free(query);

        timer.reset();
        var result_set = try db.execute(query);
        const elapsed = timer.read();
        total_time += elapsed;

        result_set.deinit();

        if (i % 10 == 0 and i > 0) {
            std.debug.print("Executed {d} join queries...\n", .{i});
        }
    }

    const avg_time = total_time / num_queries;
    std.debug.print("Average join query time: {d} ns\n", .{avg_time});

    return avg_time;
}
