const std = @import("std");
const planner = @import("planner.zig");
const statistics = @import("statistics.zig");
const Statistics = statistics.Statistics;
const LogicalPlan = planner.LogicalPlan;
const PhysicalPlan = planner.PhysicalPlan;

/// Parallel execution planning
pub const ParallelPlanner = struct {
    allocator: std.mem.Allocator,
    statistics: *Statistics,

    // Configuration options
    max_parallel_degree: u8,
    min_rows_for_parallelism: u64,

    /// Initialize a new parallel planner
    pub fn init(allocator: std.mem.Allocator, stats: *Statistics) !*ParallelPlanner {
        const parallel_planner = try allocator.create(ParallelPlanner);
        parallel_planner.* = ParallelPlanner{
            .allocator = allocator,
            .statistics = stats,
            .max_parallel_degree = 8, // Default to 8 parallel threads
            .min_rows_for_parallelism = 10000, // Default to 10K rows minimum for parallelism
        };
        return parallel_planner;
    }

    /// Clean up resources
    pub fn deinit(self: *ParallelPlanner) void {
        self.allocator.destroy(self);
    }

    /// Set the maximum parallel degree
    pub fn setMaxParallelDegree(self: *ParallelPlanner, degree: u8) void {
        self.max_parallel_degree = degree;
    }

    /// Set the minimum rows for parallelism
    pub fn setMinRowsForParallelism(self: *ParallelPlanner, rows: u64) void {
        self.min_rows_for_parallelism = rows;
    }

    /// Determine the optimal parallel degree for a plan
    pub fn determineParallelDegree(self: *ParallelPlanner, plan: *PhysicalPlan) u8 {
        // Default to no parallelism
        var degree: u8 = 1;

        // Check if the plan is suitable for parallelism
        if (!self.isSuitableForParallelism(plan)) {
            return degree;
        }

        // Determine the optimal degree based on the plan type and data size
        switch (plan.node_type) {
            .TableScan => {
                if (plan.table_name) |table_name| {
                    const row_count = self.statistics.getTableRowCount(table_name) orelse 0;

                    // Scale parallelism based on data size
                    if (row_count >= self.min_rows_for_parallelism) {
                        // Calculate degree based on row count
                        const rows_per_thread = self.min_rows_for_parallelism;
                        const min_value = @min(row_count / rows_per_thread, self.max_parallel_degree);
                        const calculated_degree = @as(u8, @intCast(min_value));

                        degree = @max(calculated_degree, 1);
                    }
                }
            },
            .IndexScan, .IndexRangeScan => {
                // Index scans can also benefit from parallelism for large ranges
                if (plan.table_name) |table_name| {
                    const row_count = self.statistics.getTableRowCount(table_name) orelse 0;

                    // For index scans, use a higher threshold
                    if (row_count >= self.min_rows_for_parallelism * 2) {
                        degree = 2; // Start with modest parallelism for index scans

                        // Scale up for very large tables
                        if (row_count >= self.min_rows_for_parallelism * 10) {
                            degree = @min(4, self.max_parallel_degree);
                        }
                    }
                }
            },
            else => {
                // Other operations may not benefit as much from parallelism
                // or are handled differently
            },
        }

        return degree;
    }

    /// Check if a plan is suitable for parallelism
    fn isSuitableForParallelism(self: *ParallelPlanner, plan: *PhysicalPlan) bool {
        _ = self;

        // Check plan type
        switch (plan.node_type) {
            .TableScan, .IndexScan, .IndexRangeScan => {
                // These operations are generally parallelizable
                return true;
            },
            .IndexSeek => {
                // Point lookups are not worth parallelizing
                return false;
            },
            else => {
                // Other operations may not be parallelizable
                return false;
            },
        }
    }

    /// Apply parallelism to a physical plan
    pub fn applyParallelism(self: *ParallelPlanner, plan: *PhysicalPlan) !void {
        // Determine parallel degree
        const degree = self.determineParallelDegree(plan);

        // Set parallel degree in the plan
        plan.parallel_degree = degree;

        // Recursively apply to child plans
        if (plan.children) |children| {
            for (children) |child| {
                try self.applyParallelism(child);
            }
        }
    }

    /// Split a plan into parallel fragments
    pub fn splitPlanForParallelExecution(self: *ParallelPlanner, plan: *PhysicalPlan) ![]PhysicalPlan {
        if (plan.parallel_degree <= 1) {
            // No parallelism needed
            return &[_]PhysicalPlan{plan.*};
        }

        // Create parallel fragments
        const fragments = try self.allocator.alloc(PhysicalPlan, plan.parallel_degree);

        switch (plan.node_type) {
            .TableScan => {
                // Split table scan into parallel fragments
                if (plan.table_name) |table_name| {
                    const row_count = self.statistics.getTableRowCount(table_name) orelse 0;
                    const rows_per_fragment = row_count / plan.parallel_degree;

                    for (fragments, 0..) |*fragment, i| {
                        // Clone the plan for this fragment
                        fragment.* = plan.*;

                        // Set fragment-specific properties
                        fragment.parallel_fragment_id = @as(u8, @intCast(i));
                        fragment.parallel_fragment_count = plan.parallel_degree;

                        // Set range for this fragment
                        fragment.parallel_range_start = i * rows_per_fragment;
                        fragment.parallel_range_end = if (i == plan.parallel_degree - 1)
                            row_count // Last fragment gets any remainder
                        else
                            (i + 1) * rows_per_fragment;
                    }
                }
            },
            .IndexScan, .IndexRangeScan => {
                // Similar approach for index scans
                // In a real implementation, we would need to determine how to split the index range
                for (fragments, 0..) |*fragment, i| {
                    // Clone the plan for this fragment
                    fragment.* = plan.*;

                    // Set fragment-specific properties
                    fragment.parallel_fragment_id = @as(u8, @intCast(i));
                    fragment.parallel_fragment_count = plan.parallel_degree;

                    // For simplicity, we're not setting actual range values here
                    // In a real implementation, we would need to compute these based on the index
                }
            },
            else => {
                // Other operations would need specific splitting logic
                self.allocator.free(fragments);
                return &[_]PhysicalPlan{plan.*};
            },
        }

        return fragments;
    }
};

test "ParallelPlanner initialization" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Initialize ParallelPlanner
    const parallel_planner = try ParallelPlanner.init(allocator, stats);
    defer parallel_planner.deinit();

    // Verify initialization
    try std.testing.expectEqual(allocator, parallel_planner.allocator);
    try std.testing.expectEqual(stats, parallel_planner.statistics);
    try std.testing.expectEqual(@as(u8, 8), parallel_planner.max_parallel_degree);
    try std.testing.expectEqual(@as(u64, 10000), parallel_planner.min_rows_for_parallelism);
}

test "ParallelPlanner degree determination" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("small_table", 1000);
    try stats.addTableStatistics("medium_table", 20000);
    try stats.addTableStatistics("large_table", 100000);

    // Initialize ParallelPlanner
    const parallel_planner = try ParallelPlanner.init(allocator, stats);
    defer parallel_planner.deinit();

    // Create plans for different table sizes
    var small_plan = PhysicalPlan{
        .allocator = allocator,
        .node_type = .TableScan,
        .access_method = .TableScan,
        .table_name = try allocator.dupe(u8, "small_table"),
        .predicates = null,
        .columns = null,
        .children = null,
        .use_gpu = false,
        .parallel_degree = 1,
    };
    defer allocator.free(small_plan.table_name.?);

    var medium_plan = PhysicalPlan{
        .allocator = allocator,
        .node_type = .TableScan,
        .access_method = .TableScan,
        .table_name = try allocator.dupe(u8, "medium_table"),
        .predicates = null,
        .columns = null,
        .children = null,
        .use_gpu = false,
        .parallel_degree = 1,
    };
    defer allocator.free(medium_plan.table_name.?);

    var large_plan = PhysicalPlan{
        .allocator = allocator,
        .node_type = .TableScan,
        .access_method = .TableScan,
        .table_name = try allocator.dupe(u8, "large_table"),
        .predicates = null,
        .columns = null,
        .children = null,
        .use_gpu = false,
        .parallel_degree = 1,
    };
    defer allocator.free(large_plan.table_name.?);

    // Determine parallel degrees
    const small_degree = parallel_planner.determineParallelDegree(&small_plan);
    const medium_degree = parallel_planner.determineParallelDegree(&medium_plan);
    const large_degree = parallel_planner.determineParallelDegree(&large_plan);

    // Verify degrees
    try std.testing.expectEqual(@as(u8, 1), small_degree); // Too small for parallelism
    try std.testing.expectEqual(@as(u8, 2), medium_degree); // Some parallelism
    try std.testing.expectEqual(@as(u8, 8), large_degree); // Maximum parallelism
}

test "ParallelPlanner apply parallelism" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("large_table", 100000);

    // Initialize ParallelPlanner
    const parallel_planner = try ParallelPlanner.init(allocator, stats);
    defer parallel_planner.deinit();

    // Create a plan
    var plan = PhysicalPlan{
        .allocator = allocator,
        .node_type = .TableScan,
        .access_method = .TableScan,
        .table_name = try allocator.dupe(u8, "large_table"),
        .predicates = null,
        .columns = null,
        .children = null,
        .use_gpu = false,
        .parallel_degree = 1,
    };
    defer allocator.free(plan.table_name.?);

    // Apply parallelism
    try parallel_planner.applyParallelism(&plan);

    // Verify parallelism was applied
    try std.testing.expectEqual(@as(u8, 8), plan.parallel_degree);
}

test "ParallelPlanner split plan" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("large_table", 100000);

    // Initialize ParallelPlanner
    const parallel_planner = try ParallelPlanner.init(allocator, stats);
    defer parallel_planner.deinit();

    // Create a plan
    var plan = PhysicalPlan{
        .allocator = allocator,
        .node_type = .TableScan,
        .access_method = .TableScan,
        .table_name = try allocator.dupe(u8, "large_table"),
        .predicates = null,
        .columns = null,
        .children = null,
        .use_gpu = false,
        .parallel_degree = 4, // Set to 4 for this test
    };
    defer allocator.free(plan.table_name.?);

    // Split the plan
    const fragments = try parallel_planner.splitPlanForParallelExecution(&plan);
    defer allocator.free(fragments);

    // Verify fragments
    try std.testing.expectEqual(@as(usize, 4), fragments.len);

    // Check each fragment
    for (fragments, 0..) |fragment, i| {
        try std.testing.expectEqual(@as(u8, @intCast(i)), fragment.parallel_fragment_id);
        try std.testing.expectEqual(@as(u8, 4), fragment.parallel_fragment_count);

        // Check range
        const expected_start = i * 25000;
        const expected_end = if (i == 3) 100000 else (i + 1) * 25000;

        try std.testing.expectEqual(expected_start, fragment.parallel_range_start);
        try std.testing.expectEqual(expected_end, fragment.parallel_range_end);
    }
}
