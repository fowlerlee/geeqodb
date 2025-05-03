const std = @import("std");
const geeqodb = @import("geeqodb");
const BTreeMapIndex = geeqodb.storage.btree_index.BTreeMapIndex;
const SkipListIndex = geeqodb.storage.skiplist_index.SkipListIndex;

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Benchmark parameters
    const num_entries = 100_000;
    const num_lookups = 10_000;
    const num_range_queries = 100;
    const range_size = 100;

    std.debug.print("Running index benchmarks with {d} entries...\n", .{num_entries});

    // Create indexes
    const btree_index = try BTreeMapIndex.create(allocator, "btree_benchmark", "benchmark_table", "benchmark_column");
    defer btree_index.deinit();

    const skiplist_index = try SkipListIndex.create(allocator, "skiplist_benchmark", "benchmark_table", "benchmark_column");
    defer skiplist_index.deinit();

    // Benchmark 1: Insertion
    std.debug.print("\nBenchmark 1: Insertion of {d} entries\n", .{num_entries});

    var timer = try std.time.Timer.start();
    var i: i64 = 0;
    while (i < num_entries) : (i += 1) {
        try btree_index.insert(i, @as(u64, @intCast(i)));
    }
    const btree_insert_time = timer.read();

    timer.reset();
    i = 0;
    while (i < num_entries) : (i += 1) {
        try skiplist_index.insert(i, @as(u64, @intCast(i)));
    }
    const skiplist_insert_time = timer.read();

    std.debug.print("BTreeMap insertion time: {d} ns ({d:.2} ns per entry)\n", .{ btree_insert_time, @as(f64, @floatFromInt(btree_insert_time)) / @as(f64, @floatFromInt(num_entries)) });
    std.debug.print("SkipList insertion time: {d} ns ({d:.2} ns per entry)\n", .{ skiplist_insert_time, @as(f64, @floatFromInt(skiplist_insert_time)) / @as(f64, @floatFromInt(num_entries)) });

    // Benchmark 2: Random lookups
    std.debug.print("\nBenchmark 2: {d} random lookups\n", .{num_lookups});

    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    timer.reset();
    var lookup_count: usize = 0;
    while (lookup_count < num_lookups) : (lookup_count += 1) {
        const key = random.intRangeAtMost(i64, 0, num_entries - 1);
        _ = btree_index.get(key);
    }
    const btree_lookup_time = timer.read();

    timer.reset();
    lookup_count = 0;
    while (lookup_count < num_lookups) : (lookup_count += 1) {
        const key = random.intRangeAtMost(i64, 0, num_entries - 1);
        _ = skiplist_index.get(key);
    }
    const skiplist_lookup_time = timer.read();

    std.debug.print("BTreeMap lookup time: {d} ns ({d:.2} ns per lookup)\n", .{ btree_lookup_time, @as(f64, @floatFromInt(btree_lookup_time)) / @as(f64, @floatFromInt(num_lookups)) });
    std.debug.print("SkipList lookup time: {d} ns ({d:.2} ns per lookup)\n", .{ skiplist_lookup_time, @as(f64, @floatFromInt(skiplist_lookup_time)) / @as(f64, @floatFromInt(num_lookups)) });

    // Benchmark 3: Range queries
    std.debug.print("\nBenchmark 3: {d} range queries of size {d}\n", .{ num_range_queries, range_size });

    timer.reset();
    var range_count: usize = 0;
    while (range_count < num_range_queries) : (range_count += 1) {
        const start_key = random.intRangeAtMost(i64, 0, num_entries - range_size - 1);
        const end_key = start_key + range_size;

        var count: usize = 0;
        var k = start_key;
        while (k <= end_key) : (k += 1) {
            if (btree_index.get(k) != null) {
                count += 1;
            }
        }
    }
    const btree_range_time = timer.read();

    timer.reset();
    range_count = 0;
    while (range_count < num_range_queries) : (range_count += 1) {
        const start_key = random.intRangeAtMost(i64, 0, num_entries - range_size - 1);
        const end_key = start_key + range_size;

        var count: usize = 0;
        var k = start_key;
        while (k <= end_key) : (k += 1) {
            if (skiplist_index.get(k) != null) {
                count += 1;
            }
        }
    }
    const skiplist_range_time = timer.read();

    std.debug.print("BTreeMap range query time: {d} ns ({d:.2} ns per query)\n", .{ btree_range_time, @as(f64, @floatFromInt(btree_range_time)) / @as(f64, @floatFromInt(num_range_queries)) });
    std.debug.print("SkipList range query time: {d} ns ({d:.2} ns per query)\n", .{ skiplist_range_time, @as(f64, @floatFromInt(skiplist_range_time)) / @as(f64, @floatFromInt(num_range_queries)) });

    // Benchmark 4: Deletion
    std.debug.print("\nBenchmark 4: Deletion of {d} entries\n", .{num_entries});

    timer.reset();
    i = 0;
    while (i < num_entries) : (i += 1) {
        _ = btree_index.remove(i);
    }
    const btree_delete_time = timer.read();

    timer.reset();
    i = 0;
    while (i < num_entries) : (i += 1) {
        _ = skiplist_index.remove(i);
    }
    const skiplist_delete_time = timer.read();

    std.debug.print("BTreeMap deletion time: {d} ns ({d:.2} ns per entry)\n", .{ btree_delete_time, @as(f64, @floatFromInt(btree_delete_time)) / @as(f64, @floatFromInt(num_entries)) });
    std.debug.print("SkipList deletion time: {d} ns ({d:.2} ns per entry)\n", .{ skiplist_delete_time, @as(f64, @floatFromInt(skiplist_delete_time)) / @as(f64, @floatFromInt(num_entries)) });

    std.debug.print("\nBenchmarks completed successfully!\n", .{});
}
