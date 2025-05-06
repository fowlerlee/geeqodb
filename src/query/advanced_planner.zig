const std = @import("std");
const planner = @import("planner.zig");
const cost_model = @import("cost_model.zig");
const statistics = @import("statistics.zig");
const parallel = @import("parallel.zig");
const gpu = @import("../gpu/main.zig");
const QueryPlanner = planner.QueryPlanner;
const LogicalPlan = planner.LogicalPlan;
const PhysicalPlan = planner.PhysicalPlan;
const CostModel = cost_model.CostModel;
const Statistics = statistics.Statistics;
const ParallelPlanner = parallel.ParallelPlanner;
const GpuDevice = gpu.GpuDevice;
const GpuQueryIntegration = gpu.GpuQueryIntegration;

/// Advanced query planner with cost-based optimization and GPU acceleration
pub const AdvancedQueryPlanner = struct {
    allocator: std.mem.Allocator,
    base_planner: *QueryPlanner,
    cost_model: ?*CostModel,
    statistics: ?*Statistics,
    parallel_planner: ?*ParallelPlanner,
    gpu_device_manager: ?*GpuDevice,
    force_gpu: bool,

    /// Initialize a new advanced query planner
    pub fn init(allocator: std.mem.Allocator) !*AdvancedQueryPlanner {
        // Initialize base planner
        const base_planner = try QueryPlanner.init(allocator);
        errdefer base_planner.deinit();

        // Initialize statistics
        const stats = try Statistics.init(allocator);
        errdefer stats.deinit();

        // Initialize cost model
        const cost = try CostModel.init(allocator, stats);
        errdefer cost.deinit();

        // Initialize parallel planner
        const parallel_planner = try ParallelPlanner.init(allocator, stats);
        errdefer parallel_planner.deinit();

        // Initialize GPU device manager
        const gpu_manager = GpuDevice.init(allocator) catch null;

        // Create advanced planner
        const advanced_planner = try allocator.create(AdvancedQueryPlanner);
        advanced_planner.* = AdvancedQueryPlanner{
            .allocator = allocator,
            .base_planner = base_planner,
            .cost_model = cost,
            .statistics = stats,
            .parallel_planner = parallel_planner,
            .gpu_device_manager = gpu_manager,
            .force_gpu = false,
        };

        return advanced_planner;
    }

    /// Clean up resources
    pub fn deinit(self: *AdvancedQueryPlanner) void {
        // Clean up GPU device manager
        if (self.gpu_device_manager) |gpu_manager| {
            gpu_manager.deinit();
        }

        // Clean up parallel planner
        if (self.parallel_planner) |parallel_planner| {
            parallel_planner.deinit();
        }

        // Clean up cost model
        if (self.cost_model) |cost| {
            cost.deinit();
        }

        // Clean up statistics
        if (self.statistics) |stats| {
            stats.deinit();
        }

        // Clean up base planner
        self.base_planner.deinit();

        // Clean up self
        self.allocator.destroy(self);
    }

    /// Parse a SQL query into an AST
    pub fn parse(self: *AdvancedQueryPlanner, query: []const u8) !*planner.AST {
        return self.base_planner.parse(query);
    }

    /// Plan a query execution from an AST
    pub fn plan(self: *AdvancedQueryPlanner, ast: *planner.AST) !*LogicalPlan {
        return self.base_planner.plan(ast);
    }

    /// Optimize a logical plan into a physical execution plan
    pub fn optimize(self: *AdvancedQueryPlanner, logical_plan: *LogicalPlan) !*PhysicalPlan {
        // Apply logical optimizations directly to the input logical plan
        // This avoids creating a new logical plan that would need to be freed
        try self.applyPredicatePushdown(logical_plan);
        try self.applyJoinReordering(logical_plan);

        // Convert to physical plan
        const physical_plan = try self.createPhysicalPlan(logical_plan);

        // Apply physical optimizations
        try self.optimizePhysical(physical_plan);

        // Apply parallelism
        if (self.parallel_planner) |parallel_planner| {
            try parallel_planner.applyParallelism(physical_plan);
        }

        // Apply GPU acceleration if beneficial
        try self.applyGpuAcceleration(physical_plan);

        return physical_plan;
    }

    /// Apply logical optimizations to a logical plan
    fn optimizeLogical(self: *AdvancedQueryPlanner, logical_plan_input: *LogicalPlan) !*LogicalPlan {
        // Create a new logical plan that's a shallow copy of the input
        const optimized_plan = try self.allocator.create(LogicalPlan);
        errdefer self.allocator.destroy(optimized_plan);

        // Initialize with the same values as the input plan
        optimized_plan.* = logical_plan_input.*;

        // Apply predicate pushdown (this doesn't modify the plan in our simplified version)
        // try self.applyPredicatePushdown(optimized_plan);

        // Apply join reordering (this doesn't modify the plan in our simplified version)
        // try self.applyJoinReordering(optimized_plan);

        return optimized_plan;
    }

    /// Apply predicate pushdown optimization
    fn applyPredicatePushdown(self: *AdvancedQueryPlanner, logical_plan: *LogicalPlan) !void {
        // In a real implementation, this would push predicates down the plan tree
        // to filter data as early as possible

        // For now, we'll just simulate the optimization
        if (logical_plan.node_type == .Filter and logical_plan.children != null and logical_plan.children.?.len > 0) {
            // Move predicates from Filter to child nodes where possible
            if (logical_plan.predicates) |predicates| {
                for (predicates) |pred| {
                    // Check if predicate can be pushed down
                    if (std.mem.indexOf(u8, pred.column, ".") != null) {
                        // Extract table name from column reference
                        const dot_index = std.mem.indexOf(u8, pred.column, ".").?;
                        const table_name = pred.column[0..dot_index];
                        const column_name = pred.column[dot_index + 1 ..];

                        // Find child node for this table
                        if (logical_plan.children) |children| {
                            for (children) |*child| {
                                if (child.node_type == .Join and child.children != null and child.children.?.len > 0) {
                                    // If the child is a join, check its children
                                    for (child.children.?) |*join_child| {
                                        if (join_child.table_name != null and std.mem.eql(u8, join_child.table_name.?, table_name)) {
                                            // Create a new predicate for the join child
                                            const new_pred = planner.Predicate{
                                                .column = try self.allocator.dupe(u8, column_name),
                                                .op = pred.op,
                                                .value = pred.value,
                                            };

                                            // Add predicate to join child
                                            if (join_child.predicates == null) {
                                                var new_preds = try self.allocator.alloc(planner.Predicate, 1);
                                                new_preds[0] = new_pred;
                                                join_child.predicates = new_preds;
                                            } else {
                                                const old_preds = join_child.predicates.?;
                                                var new_preds = try self.allocator.alloc(planner.Predicate, old_preds.len + 1);
                                                for (old_preds, 0..) |old_pred, i| {
                                                    new_preds[i] = old_pred;
                                                }
                                                new_preds[old_preds.len] = new_pred;
                                                join_child.predicates = new_preds;
                                                self.allocator.free(old_preds);
                                            }

                                            break;
                                        }
                                    }
                                } else if (child.table_name != null and std.mem.eql(u8, child.table_name.?, table_name)) {
                                    // Create a new predicate for the child
                                    const new_pred = planner.Predicate{
                                        .column = try self.allocator.dupe(u8, column_name),
                                        .op = pred.op,
                                        .value = pred.value,
                                    };

                                    // Add predicate to child
                                    if (child.predicates == null) {
                                        var new_preds = try self.allocator.alloc(planner.Predicate, 1);
                                        new_preds[0] = new_pred;
                                        child.predicates = new_preds;
                                    } else {
                                        const old_preds = child.predicates.?;
                                        var new_preds = try self.allocator.alloc(planner.Predicate, old_preds.len + 1);
                                        for (old_preds, 0..) |old_pred, i| {
                                            new_preds[i] = old_pred;
                                        }
                                        new_preds[old_preds.len] = new_pred;
                                        child.predicates = new_preds;
                                        self.allocator.free(old_preds);
                                    }

                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Recursively apply to children
        if (logical_plan.children) |children| {
            for (children) |*child| {
                try self.applyPredicatePushdown(child);
            }
        }
    }

    /// Apply join reordering optimization
    fn applyJoinReordering(self: *AdvancedQueryPlanner, logical_plan: *LogicalPlan) !void {
        // Only apply to join nodes
        if (logical_plan.node_type != .Join or logical_plan.children == null or logical_plan.children.?.len < 2) {
            // Recursively apply to children
            if (logical_plan.children) |children| {
                for (children) |*child| {
                    try self.applyJoinReordering(child);
                }
            }
            return;
        }

        // Get statistics for the tables
        var left_size: u64 = std.math.maxInt(u64);
        var right_size: u64 = std.math.maxInt(u64);

        if (logical_plan.children.?[0].table_name) |table_name| {
            if (self.statistics) |stats| {
                left_size = stats.getTableRowCount(table_name) orelse left_size;
            }
        }

        if (logical_plan.children.?[1].table_name) |table_name| {
            if (self.statistics) |stats| {
                right_size = stats.getTableRowCount(table_name) orelse right_size;
            }
        }

        // Reorder joins to put smaller table on the left
        if (right_size < left_size) {
            // Swap children
            const temp = logical_plan.children.?[0];
            logical_plan.children.?[0] = logical_plan.children.?[1];
            logical_plan.children.?[1] = temp;
        }

        // Recursively apply to children
        for (logical_plan.children.?) |*child| {
            try self.applyJoinReordering(child);
        }
    }

    /// Create a physical plan from a logical plan
    fn createPhysicalPlan(self: *AdvancedQueryPlanner, logical_plan: *LogicalPlan) !*PhysicalPlan {
        // Create a physical plan
        const physical_plan = try self.allocator.create(PhysicalPlan);
        errdefer self.allocator.destroy(physical_plan);

        // Initialize with default values
        physical_plan.* = PhysicalPlan{
            .allocator = self.allocator,
            .node_type = .TableScan,
            .access_method = .TableScan,
            .table_name = if (logical_plan.table_name) |name| try self.allocator.dupe(u8, name) else null,
            .predicates = null,
            .columns = null,
            .children = null,
            .use_gpu = false,
            .parallel_degree = 1,
        };

        // Copy predicates if any
        if (logical_plan.predicates) |predicates| {
            var new_preds = try self.allocator.alloc(planner.Predicate, predicates.len);
            for (predicates, 0..) |pred, i| {
                new_preds[i] = pred;
                // Copy the column string
                new_preds[i].column = try self.allocator.dupe(u8, pred.column);
                // Copy string values if needed
                if (pred.value == .String) {
                    new_preds[i].value = planner.PlanValue{ .String = try self.allocator.dupe(u8, pred.value.String) };
                }
            }
            physical_plan.predicates = new_preds;
        }

        // Copy columns if any
        if (logical_plan.columns) |columns| {
            var new_cols = try self.allocator.alloc([]const u8, columns.len);
            for (columns, 0..) |col, i| {
                new_cols[i] = try self.allocator.dupe(u8, col);
            }
            physical_plan.columns = new_cols;
        }

        // Process children recursively
        if (logical_plan.children) |children| {
            // Allocate an array of pointers to PhysicalPlan
            var new_children = try self.allocator.alloc(*PhysicalPlan, children.len);
            for (children, 0..) |*child, i| {
                // Create a new physical plan for the child
                new_children[i] = try self.createPhysicalPlan(child);
            }
            physical_plan.children = new_children;
        }

        // Set node type based on logical plan
        switch (logical_plan.node_type) {
            .Scan => {
                physical_plan.node_type = .TableScan;
                physical_plan.access_method = .TableScan;

                // Check if we can use an index
                if (logical_plan.table_name != null and logical_plan.predicates != null) {
                    for (logical_plan.predicates.?) |pred| {
                        {
                            if (try self.base_planner.findBestIndex(logical_plan.table_name.?, pred.column)) |_| {
                                if (pred.op == .Eq) {
                                    physical_plan.node_type = .IndexSeek;
                                    physical_plan.access_method = .IndexSeek;
                                } else if (pred.op == .Gt or pred.op == .Lt or
                                    pred.op == .Ge or pred.op == .Le)
                                {
                                    physical_plan.node_type = .IndexRangeScan;
                                    physical_plan.access_method = .IndexRange;
                                } else {
                                    physical_plan.node_type = .IndexScan;
                                    physical_plan.access_method = .TableScan;
                                }
                                break;
                            }
                        }
                    }
                }
            },
            .Join => {
                // For now, we'll just use nested loop joins
                physical_plan.node_type = .NestedLoopJoin;
            },
            .Filter => {
                physical_plan.node_type = .Filter;
            },
            .Aggregate => {
                physical_plan.node_type = .Aggregate;
            },
            .Sort => {
                physical_plan.node_type = .Sort;
            },
            .Limit => {
                physical_plan.node_type = .Limit;
            },
            .Project => {
                physical_plan.node_type = .Project;
            },
        }

        return physical_plan;
    }

    /// Apply physical optimizations to a physical plan
    fn optimizePhysical(self: *AdvancedQueryPlanner, physical_plan: *PhysicalPlan) !void {
        // In a real implementation, this would apply various physical optimizations
        // such as choosing the best join algorithm, etc.

        // For now, we'll just simulate the optimization

        // Apply predicate pushdown to the physical plan
        try self.applyPhysicalPredicatePushdown(physical_plan);

        // Recursively apply to children
        if (physical_plan.children) |children| {
            for (children) |child| {
                try self.optimizePhysical(child);
            }
        }
    }

    /// Apply predicate pushdown optimization to a physical plan
    fn applyPhysicalPredicatePushdown(self: *AdvancedQueryPlanner, physical_plan: *PhysicalPlan) !void {
        // Only apply to Filter nodes
        if (physical_plan.node_type != .Filter or physical_plan.children == null or physical_plan.children.?.len == 0) {
            // Recursively apply to children
            if (physical_plan.children) |children| {
                for (children) |child| {
                    try self.applyPhysicalPredicatePushdown(child);
                }
            }
            return;
        }

        // Move predicates from Filter to child nodes where possible
        if (physical_plan.predicates) |predicates| {
            for (predicates) |pred| {
                // Check if predicate can be pushed down
                if (std.mem.indexOf(u8, pred.column, ".") != null) {
                    // Extract table name from column reference
                    const dot_index = std.mem.indexOf(u8, pred.column, ".").?;
                    const table_name = pred.column[0..dot_index];
                    const column_name = pred.column[dot_index + 1 ..];

                    // Find child node for this table
                    if (physical_plan.children) |children| {
                        for (children) |child| {
                            if (child.node_type == .NestedLoopJoin and child.children != null and child.children.?.len > 0) {
                                // If the child is a join, check its children
                                for (child.children.?) |join_child| {
                                    if (join_child.table_name != null and std.mem.eql(u8, join_child.table_name.?, table_name)) {
                                        // Create a new predicate for the join child
                                        const new_pred = planner.Predicate{
                                            .column = try self.allocator.dupe(u8, column_name),
                                            .op = pred.op,
                                            .value = pred.value,
                                        };

                                        // Add predicate to join child
                                        if (join_child.predicates == null) {
                                            var new_preds = try self.allocator.alloc(planner.Predicate, 1);
                                            new_preds[0] = new_pred;
                                            join_child.predicates = new_preds;
                                        } else {
                                            const old_preds = join_child.predicates.?;
                                            var new_preds = try self.allocator.alloc(planner.Predicate, old_preds.len + 1);
                                            for (old_preds, 0..) |old_pred, i| {
                                                new_preds[i] = old_pred;
                                            }
                                            new_preds[old_preds.len] = new_pred;
                                            join_child.predicates = new_preds;
                                            self.allocator.free(old_preds);
                                        }

                                        break;
                                    }
                                }
                            } else if (child.table_name != null and std.mem.eql(u8, child.table_name.?, table_name)) {
                                // Create a new predicate for the child
                                const new_pred = planner.Predicate{
                                    .column = try self.allocator.dupe(u8, column_name),
                                    .op = pred.op,
                                    .value = pred.value,
                                };

                                // Add predicate to child
                                if (child.predicates == null) {
                                    var new_preds = try self.allocator.alloc(planner.Predicate, 1);
                                    new_preds[0] = new_pred;
                                    child.predicates = new_preds;
                                } else {
                                    const old_preds = child.predicates.?;
                                    var new_preds = try self.allocator.alloc(planner.Predicate, old_preds.len + 1);
                                    for (old_preds, 0..) |old_pred, i| {
                                        new_preds[i] = old_pred;
                                    }
                                    new_preds[old_preds.len] = new_pred;
                                    child.predicates = new_preds;
                                    self.allocator.free(old_preds);
                                }

                                break;
                            }
                        }
                    }
                }
            }
        }

        // Recursively apply to children
        if (physical_plan.children) |children| {
            for (children) |child| {
                try self.applyPhysicalPredicatePushdown(child);
            }
        }
    }

    /// Apply GPU acceleration to a physical plan if beneficial
    fn applyGpuAcceleration(self: *AdvancedQueryPlanner, physical_plan: *PhysicalPlan) !void {
        // Check if GPU is available
        if (!self.hasGpuSupport()) {
            return;
        }

        // If force_gpu is set, always use GPU
        if (self.force_gpu) {
            physical_plan.use_gpu = true;

            // Recursively apply to children
            if (physical_plan.children) |children| {
                for (children) |child| {
                    try self.applyGpuAcceleration(child);
                }
            }

            return;
        }

        // Check if the operation is suitable for GPU acceleration
        const suitable_node_types = [_]planner.PhysicalNodeType{
            .TableScan,
            .Filter,
            .NestedLoopJoin,
            .HashJoin,
            .Aggregate,
            .Sort,
            .GroupBy,
            .Window,
        };

        var suitable_type = false;
        for (suitable_node_types) |node_type| {
            if (physical_plan.node_type == node_type) {
                suitable_type = true;
                break;
            }
        }

        if (!suitable_type) {
            physical_plan.use_gpu = false;

            // Recursively apply to children
            if (physical_plan.children) |children| {
                for (children) |child| {
                    try self.applyGpuAcceleration(child);
                }
            }

            return;
        }

        // Get estimated row count
        var row_count: usize = 1000; // Default estimate
        if (physical_plan.table_name != null and self.statistics != null) {
            if (self.statistics.?.getTableRowCount(physical_plan.table_name.?)) |count| {
                row_count = @intCast(count);
            }
        }

        // For small data sets, CPU is faster due to transfer overhead
        const min_rows_for_gpu = 10000;
        if (row_count < min_rows_for_gpu) {
            physical_plan.use_gpu = false;

            // Recursively apply to children
            if (physical_plan.children) |children| {
                for (children) |child| {
                    try self.applyGpuAcceleration(child);
                }
            }

            return;
        }

        // Check if GPU acceleration would be beneficial using cost model
        if (self.cost_model) |cost_model_instance| {
            physical_plan.use_gpu = try cost_model_instance.shouldUseGpu(physical_plan);
        } else {
            // If no cost model is available, use GPU for large datasets
            physical_plan.use_gpu = row_count >= min_rows_for_gpu;
        }

        // Recursively apply to children
        if (physical_plan.children) |children| {
            for (children) |child| {
                try self.applyGpuAcceleration(child);
            }
        }
    }

    /// Check if GPU support is available
    pub fn hasGpuSupport(self: *AdvancedQueryPlanner) bool {
        return self.gpu_device_manager != null and self.gpu_device_manager.?.hasGpu();
    }

    /// Set force GPU flag
    pub fn setForceGpu(self: *AdvancedQueryPlanner, force: bool) void {
        self.force_gpu = force;
    }

    /// Register an index with the planner
    pub fn registerIndex(
        self: *AdvancedQueryPlanner,
        index_name: []const u8,
        table_name: []const u8,
        column_name: []const u8,
        index_type: planner.Index.IndexType,
        cardinality: u64,
        row_count: u64,
    ) !void {
        // Register with base planner
        try self.base_planner.registerIndex(index_name, table_name, column_name, index_type);

        // Update statistics
        if (self.statistics) |stats| {
            // Add table statistics if not already present
            if (stats.getTableRowCount(table_name) == null) {
                try stats.addTableStatistics(table_name, row_count);
            }

            // Add column statistics
            try stats.addColumnStatistics(table_name, column_name, cardinality, planner.PlanValue{ .Integer = 0 }, // Default min value
                planner.PlanValue{ .Integer = @intCast(cardinality) }, // Default max value
                0 // No nulls
            );
        }
    }
};

test "AdvancedQueryPlanner initialization" {
    const allocator = std.testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Verify initialization
    try std.testing.expectEqual(allocator, advanced_planner.allocator);
    try std.testing.expect(advanced_planner.base_planner != null);
    try std.testing.expect(advanced_planner.cost_model != null);
    try std.testing.expect(advanced_planner.statistics != null);
    try std.testing.expect(advanced_planner.parallel_planner != null);
    // GPU device manager may be null if no GPU is available
}

test "AdvancedQueryPlanner parse and plan" {
    const allocator = std.testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Parse a SQL query
    const ast = try advanced_planner.parse("SELECT * FROM users");
    defer ast.deinit();

    // Plan the query
    const logical_plan = try advanced_planner.plan(ast);
    defer logical_plan.deinit();

    // Verify plan
    try std.testing.expectEqual(planner.LogicalPlan.NodeType.Scan, logical_plan.node_type);
}

test "AdvancedQueryPlanner optimize" {
    const allocator = std.testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

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

    // Optimize the plan
    const physical_plan = try advanced_planner.optimize(&logical_plan);
    defer physical_plan.deinit();

    // Verify plan
    try std.testing.expectEqual(planner.PhysicalNodeType.TableScan, physical_plan.node_type);
}

test "AdvancedQueryPlanner with index" {
    const allocator = std.testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Register an index
    try advanced_planner.registerIndex("idx_users_id", "users", "id", .BTree, 1000, 1000);

    // Create a logical plan with a predicate
    var logical_plan = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .predicates = try allocator.alloc(planner.Predicate, 1),
        .children = null,
    };
    defer allocator.free(logical_plan.table_name.?);
    defer allocator.free(logical_plan.predicates.?);

    // Create a predicate for id = 2
    const pred = planner.Predicate{
        .column = try allocator.dupe(u8, "id"),
        .op = .Eq,
        .value = planner.PlanValue{ .Integer = 2 },
    };

    // Assign the predicate to the logical plan
    logical_plan.predicates.?[0] = pred;
    defer allocator.free(logical_plan.predicates.?[0].column);

    // Optimize the plan
    const physical_plan = try advanced_planner.optimize(&logical_plan);
    defer physical_plan.deinit();

    // Verify that the optimizer chose an index seek
    try std.testing.expectEqual(planner.PhysicalNodeType.IndexSeek, physical_plan.node_type);
}

test "AdvancedQueryPlanner join reordering" {
    const allocator = std.testing.allocator;

    // Initialize AdvancedQueryPlanner
    const advanced_planner = try AdvancedQueryPlanner.init(allocator);
    defer advanced_planner.deinit();

    // Add statistics for tables
    if (advanced_planner.statistics) |stats| {
        try stats.addTableStatistics("users", 1000);
        try stats.addTableStatistics("orders", 10000);
    }

    // Create a logical plan with a join
    var logical_plan = LogicalPlan{
        .allocator = allocator,
        .node_type = .Join,
        .table_name = null,
        .columns = null,
        .predicates = null,
        .children = try allocator.alloc(LogicalPlan, 2),
    };
    defer allocator.free(logical_plan.children.?);

    // Create left child (orders table - larger)
    logical_plan.children.?[0] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "orders"),
        .columns = null,
        .predicates = null,
        .children = null,
    };
    defer allocator.free(logical_plan.children.?[0].table_name.?);

    // Create right child (users table - smaller)
    logical_plan.children.?[1] = LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .predicates = null,
        .children = null,
    };
    defer allocator.free(logical_plan.children.?[1].table_name.?);

    // Optimize the plan
    const physical_plan = try advanced_planner.optimize(&logical_plan);
    defer physical_plan.deinit();

    // Verify that the optimizer reordered the join to put the smaller table first
    try std.testing.expectEqualStrings("users", physical_plan.children.?[0].table_name.?);
    try std.testing.expectEqualStrings("orders", physical_plan.children.?[1].table_name.?);
}
