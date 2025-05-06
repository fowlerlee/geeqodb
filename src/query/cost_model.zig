const std = @import("std");
const planner = @import("planner.zig");
const statistics = @import("statistics.zig");
const Statistics = statistics.Statistics;
const LogicalPlan = planner.LogicalPlan;
const PhysicalPlan = planner.PhysicalPlan;
const AccessMethod = planner.AccessMethod;

/// Cost model for query optimization
pub const CostModel = struct {
    allocator: std.mem.Allocator,
    statistics: *Statistics,
    weights: Weights,

    // Cost weights for different operations
    pub const Weights = struct {
        // CPU operation costs
        cpu_scan_cost_per_row: f64 = 1.0,
        cpu_index_seek_cost: f64 = 10.0,
        cpu_index_range_cost_per_row: f64 = 0.5,
        cpu_filter_cost_per_row: f64 = 0.2,
        cpu_join_cost_per_row: f64 = 2.0,
        cpu_aggregate_cost_per_row: f64 = 0.5,
        cpu_sort_cost_per_row: f64 = 0.5 * std.math.log2(100.0), // O(n log n)

        // GPU operation costs (typically lower for large datasets)
        gpu_scan_cost_per_row: f64 = 0.01,
        gpu_filter_cost_per_row: f64 = 0.005,
        gpu_join_cost_per_row: f64 = 0.05,
        gpu_aggregate_cost_per_row: f64 = 0.01,
        gpu_sort_cost_per_row: f64 = 0.02,

        // Data transfer costs
        gpu_transfer_cost_per_byte: f64 = 0.001,

        // Fixed costs
        gpu_kernel_launch_overhead: f64 = 50.0,
    };

    /// Initialize a new cost model
    pub fn init(allocator: std.mem.Allocator, stats: *Statistics) !*CostModel {
        const model = try allocator.create(CostModel);
        model.* = CostModel{
            .allocator = allocator,
            .statistics = stats,
            .weights = Weights{},
        };
        return model;
    }

    /// Clean up resources
    pub fn deinit(self: *CostModel) void {
        self.allocator.destroy(self);
    }

    /// Estimate the cost of a logical plan
    pub fn estimateLogicalPlanCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        return switch (plan.node_type) {
            .Scan => self.estimateScanCost(plan),
            .Filter => self.estimateFilterCost(plan),
            .Join => self.estimateJoinCost(plan),
            .Aggregate => self.estimateAggregateCost(plan),
            .Sort => self.estimateSortCost(plan),
            .Limit => self.estimateLimitCost(plan),
            .Project => self.estimateProjectCost(plan),
            else => 0.0, // Default cost for other node types
        };
    }

    /// Estimate the cost of a physical plan
    pub fn estimatePhysicalPlanCost(self: *CostModel, plan: *PhysicalPlan, use_gpu: bool) !f64 {
        var cost: f64 = 0.0;

        // Add node-specific costs
        cost += switch (plan.node_type) {
            .TableScan => try self.estimateTableScanCost(plan, use_gpu),
            .IndexSeek => try self.estimateIndexSeekCost(plan, use_gpu),
            .IndexRangeScan => try self.estimateIndexRangeScanCost(plan, use_gpu),
            .IndexScan => try self.estimateIndexScanCost(plan, use_gpu),
            else => 0.0, // Default cost for other node types
        };

        // Add costs for child nodes
        if (plan.children) |children| {
            for (children) |child| {
                cost += try self.estimatePhysicalPlanCost(child, use_gpu);
            }
        }

        // Add GPU transfer costs if using GPU
        if (use_gpu) {
            cost += try self.estimateGpuTransferCost(plan);
        }

        return cost;
    }

    /// Estimate the cost of a table scan
    fn estimateTableScanCost(self: *CostModel, plan: *PhysicalPlan, use_gpu: bool) !f64 {
        if (plan.table_name == null) return 0.0;

        const row_count = self.statistics.getTableRowCount(plan.table_name.?) orelse 1000;
        _ = self.statistics.getTableRowSize(plan.table_name.?) orelse 100;

        if (use_gpu) {
            return self.weights.gpu_scan_cost_per_row * @as(f64, @floatFromInt(row_count)) +
                self.weights.gpu_kernel_launch_overhead;
        } else {
            return self.weights.cpu_scan_cost_per_row * @as(f64, @floatFromInt(row_count));
        }
    }

    /// Estimate the cost of an index seek
    fn estimateIndexSeekCost(self: *CostModel, _: *PhysicalPlan, _: bool) !f64 {

        // Index seeks have a fixed cost plus the cost of fetching the row
        return self.weights.cpu_index_seek_cost;
    }

    /// Estimate the cost of an index range scan
    fn estimateIndexRangeScanCost(self: *CostModel, plan: *PhysicalPlan, use_gpu: bool) !f64 {
        _ = use_gpu; // Index range scans are typically done on CPU

        // Estimate the number of rows in the range
        var estimated_rows: u64 = 100; // Default estimate

        if (plan.predicates) |predicates| {
            for (predicates) |pred| {
                if (pred.op == .Gt or pred.op == .Lt or
                    pred.op == .Ge or pred.op == .Le)
                {
                    // Use statistics to estimate range size if available
                    if (plan.table_name != null) {
                        estimated_rows = self.statistics.estimateRangeSize(plan.table_name.?, pred.column, pred.value, null) orelse estimated_rows;
                    }
                    break;
                }
            }
        }

        return self.weights.cpu_index_range_cost_per_row * @as(f64, @floatFromInt(estimated_rows));
    }

    /// Estimate the cost of an index scan
    fn estimateIndexScanCost(self: *CostModel, plan: *PhysicalPlan, use_gpu: bool) !f64 {
        _ = use_gpu; // Index scans are typically done on CPU

        if (plan.table_name == null) return 0.0;

        const row_count = self.statistics.getTableRowCount(plan.table_name.?) orelse 1000;

        // Index scans are faster than table scans but still scan all rows
        return self.weights.cpu_scan_cost_per_row * 0.8 * @as(f64, @floatFromInt(row_count));
    }

    /// Estimate the cost of a scan operation in a logical plan
    fn estimateScanCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        if (plan.table_name == null) return 0.0;

        const row_count = self.statistics.getTableRowCount(plan.table_name.?) orelse 1000;

        return self.weights.cpu_scan_cost_per_row * @as(f64, @floatFromInt(row_count));
    }

    /// Estimate the cost of a filter operation
    fn estimateFilterCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        var cost: f64 = 0.0;

        // Cost of the input plan
        if (plan.children) |children| {
            if (children.len > 0) {
                cost += try self.estimateLogicalPlanCost(&children[0]);
            }
        }

        // Add filter cost
        if (plan.table_name) |table_name| {
            const row_count = self.statistics.getTableRowCount(table_name) orelse 1000;
            cost += self.weights.cpu_filter_cost_per_row * @as(f64, @floatFromInt(row_count));
        }

        return cost;
    }

    /// Estimate the cost of a join operation
    fn estimateJoinCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        var cost: f64 = 0.0;

        // Cost of the input plans
        if (plan.children) |children| {
            if (children.len >= 2) {
                const left_cost = try self.estimateLogicalPlanCost(&children[0]);
                const right_cost = try self.estimateLogicalPlanCost(&children[1]);
                cost += left_cost + right_cost;

                // Estimate join cost based on the sizes of the inputs
                var left_size: u64 = 1000;
                var right_size: u64 = 1000;

                if (children[0].table_name) |table_name| {
                    left_size = self.statistics.getTableRowCount(table_name) orelse left_size;
                }

                if (children[1].table_name) |table_name| {
                    right_size = self.statistics.getTableRowCount(table_name) orelse right_size;
                }

                // Join cost is proportional to the product of the input sizes
                cost += self.weights.cpu_join_cost_per_row * @as(f64, @floatFromInt(left_size * right_size));
            }
        }

        return cost;
    }

    /// Estimate the cost of an aggregation operation
    fn estimateAggregateCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        var cost: f64 = 0.0;

        // Cost of the input plan
        if (plan.children) |children| {
            if (children.len > 0) {
                cost += try self.estimateLogicalPlanCost(&children[0]);
            }
        }

        // Add aggregation cost
        if (plan.table_name) |table_name| {
            const row_count = self.statistics.getTableRowCount(table_name) orelse 1000;
            cost += self.weights.cpu_aggregate_cost_per_row * @as(f64, @floatFromInt(row_count));
        }

        return cost;
    }

    /// Estimate the cost of a sort operation
    fn estimateSortCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        var cost: f64 = 0.0;

        // Cost of the input plan
        if (plan.children) |children| {
            if (children.len > 0) {
                cost += try self.estimateLogicalPlanCost(&children[0]);
            }
        }

        // Add sort cost (O(n log n))
        if (plan.table_name) |table_name| {
            const row_count = self.statistics.getTableRowCount(table_name) orelse 1000;
            const n = @as(f64, @floatFromInt(row_count));
            cost += self.weights.cpu_sort_cost_per_row * n * std.math.log2(n);
        }

        return cost;
    }

    /// Estimate the cost of a limit operation
    fn estimateLimitCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        // Limit operations have negligible cost themselves
        // The cost is mainly from the input plan
        if (plan.children) |children| {
            if (children.len > 0) {
                return try self.estimateLogicalPlanCost(&children[0]);
            }
        }

        return 0.0;
    }

    /// Estimate the cost of a project operation
    fn estimateProjectCost(self: *CostModel, plan: *LogicalPlan) !f64 {
        // Project operations have negligible cost themselves
        // The cost is mainly from the input plan
        if (plan.children) |children| {
            if (children.len > 0) {
                return try self.estimateLogicalPlanCost(&children[0]);
            }
        }

        return 0.0;
    }

    /// Estimate the cost of transferring data to/from GPU
    fn estimateGpuTransferCost(self: *CostModel, plan: *PhysicalPlan) !f64 {
        if (plan.table_name == null) return 0.0;

        const row_count = self.statistics.getTableRowCount(plan.table_name.?) orelse 1000;
        const row_size = self.statistics.getTableRowSize(plan.table_name.?) orelse 100;

        // Estimate data size in bytes
        const data_size = row_count * row_size;

        // Cost of transferring data to GPU and back
        return self.weights.gpu_transfer_cost_per_byte * @as(f64, @floatFromInt(data_size * 2));
    }

    /// Determine if GPU acceleration would be beneficial for a plan
    pub fn shouldUseGpu(self: *CostModel, plan: *PhysicalPlan) !bool {
        const cpu_cost = try self.estimatePhysicalPlanCost(plan, false);
        const gpu_cost = try self.estimatePhysicalPlanCost(plan, true);

        // Use GPU if it's significantly faster (at least 20% improvement)
        return gpu_cost < cpu_cost * 0.8;
    }
};

test "CostModel initialization" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Initialize CostModel
    const cost_model = try CostModel.init(allocator, stats);
    defer cost_model.deinit();

    // Verify initialization
    try std.testing.expectEqual(allocator, cost_model.allocator);
    try std.testing.expectEqual(stats, cost_model.statistics);
}

test "CostModel cost estimation" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Add some table statistics
    try stats.addTableStatistics("users", 1000);
    try stats.addTableStatistics("orders", 10000);

    // Initialize CostModel
    const cost_model = try CostModel.init(allocator, stats);
    defer cost_model.deinit();

    // Create a simple logical plan
    var logical_plan = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .predicates = null,
        .children = null,
    };
    defer allocator.free(logical_plan.table_name.?);

    // Estimate cost
    const cost = try cost_model.estimateLogicalPlanCost(&logical_plan);

    // Verify cost is reasonable
    try std.testing.expect(cost > 0);
    try std.testing.expectEqual(cost_model.weights.cpu_scan_cost_per_row * 1000, cost);
}

test "CostModel GPU vs CPU cost comparison" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try statistics.Statistics.init(allocator);
    defer stats.deinit();

    // Add some table statistics
    try stats.addTableStatistics("small_table", 100);
    try stats.addTableStatistics("large_table", 1000000);

    // Initialize CostModel
    const cost_model = try CostModel.init(allocator, stats);
    defer cost_model.deinit();

    // Create a physical plan for a small table
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

    // Create a physical plan for a large table
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

    // Check if GPU should be used for small table
    const use_gpu_small = try cost_model.shouldUseGpu(&small_plan);

    // Check if GPU should be used for large table
    const use_gpu_large = try cost_model.shouldUseGpu(&large_plan);

    // GPU should not be used for small tables due to transfer overhead
    try std.testing.expect(!use_gpu_small);

    // GPU should be used for large tables
    try std.testing.expect(use_gpu_large);
}
