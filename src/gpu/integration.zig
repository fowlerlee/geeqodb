const std = @import("std");
const query_executor = @import("query_executor.zig");
const device = @import("device.zig");
const cuda = @import("cuda.zig");
const memory_manager = @import("memory_manager.zig");
const hash_join = @import("hash_join.zig");
const window_functions = @import("window_functions.zig");
const planner = @import("../query/planner.zig");
const advanced_planner = @import("../query/advanced_planner.zig");
const executor = @import("../query/executor.zig");
const result = @import("../query/result.zig");

const GpuQueryExecutor = query_executor.GpuQueryExecutor;
const GpuDevice = device.GpuDevice;
const GpuMemoryManager = memory_manager.GpuMemoryManager;
const GpuHashJoin = hash_join.GpuHashJoin;
const GpuWindowFunction = window_functions.GpuWindowFunction;
const WindowFunctionType = window_functions.WindowFunctionType;
const WindowFrame = window_functions.WindowFrame;
const PhysicalPlan = planner.PhysicalPlan;
const AdvancedQueryPlanner = advanced_planner.AdvancedQueryPlanner;
const DatabaseContext = executor.DatabaseContext;
const ResultSet = result.ResultSet;

/// GPU Query Integration
pub const GpuQueryIntegration = struct {
    allocator: std.mem.Allocator,
    gpu_executor: *GpuQueryExecutor,
    memory_manager: *GpuMemoryManager,
    hash_join: *GpuHashJoin,
    window_function: *GpuWindowFunction,

    /// Initialize GPU query integration
    pub fn init(allocator: std.mem.Allocator) !*GpuQueryIntegration {
        // Initialize GPU query executor
        const gpu_executor = try GpuQueryExecutor.init(allocator);
        errdefer gpu_executor.deinit();

        // Get memory manager from executor
        const mem_manager = gpu_executor.memory_manager;

        // Initialize hash join
        const hash_join_instance = try GpuHashJoin.init(allocator, mem_manager, gpu_executor.cuda_instance);
        errdefer hash_join_instance.deinit();

        // Initialize window function
        const window_func = try GpuWindowFunction.init(allocator, mem_manager, gpu_executor.cuda_instance);
        errdefer window_func.deinit();

        const integration = try allocator.create(GpuQueryIntegration);
        integration.* = GpuQueryIntegration{
            .allocator = allocator,
            .gpu_executor = gpu_executor,
            .memory_manager = mem_manager,
            .hash_join = hash_join_instance,
            .window_function = window_func,
        };

        return integration;
    }

    /// Clean up resources
    pub fn deinit(self: *GpuQueryIntegration) void {
        self.window_function.deinit();
        self.hash_join.deinit();
        self.gpu_executor.deinit();
        self.allocator.destroy(self);
    }

    /// Clean up unused buffers
    pub fn cleanupUnusedBuffers(self: *GpuQueryIntegration, max_age_ms: i64) !void {
        try self.memory_manager.cleanupUnusedBuffers(max_age_ms);
    }

    /// Execute a physical plan on the GPU
    pub fn executePhysicalPlan(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Check if the plan is suitable for GPU execution
        if (!plan.use_gpu) {
            return error.PlanNotMarkedForGpu;
        }

        // Execute the plan based on its node type
        return switch (plan.node_type) {
            .TableScan => try self.executeTableScan(plan, context),
            .Filter => try self.executeFilter(plan, context),
            .Join => try self.executeJoin(plan, context),
            .Aggregate => try self.executeAggregate(plan, context),
            .Sort => try self.executeSort(plan, context),
            .GroupBy => try self.executeGroupBy(plan, context),
            .Window => try self.executeWindowFunction(plan, context),
            else => error.UnsupportedGpuOperation,
        };
    }

    /// Execute a table scan on the GPU
    fn executeTableScan(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Get table data from the database context
        const table_data = try context.getTableData(plan.table_name.?);
        defer self.allocator.free(table_data);

        // For now, we'll just return the data as is
        // In a real implementation, we would process the data on the GPU

        // Create a result set from the table data
        return try ResultSet.fromRawData(self.allocator, table_data);
    }

    /// Execute a filter operation on the GPU
    fn executeFilter(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Check if we have predicates
        if (plan.predicates == null or plan.predicates.?.len == 0) {
            return error.NoPredicates;
        }

        // Get input data from child plan or table
        var input_data: []const u8 = undefined;
        var should_free_input = false;
        defer if (should_free_input) self.allocator.free(input_data);

        if (plan.children != null and plan.children.?.len > 0) {
            // Execute child plan to get input data
            const child_result = try self.executePhysicalPlan(&plan.children.?[0], context);
            defer child_result.deinit();

            // Convert result set to raw data
            input_data = try child_result.toRawData(self.allocator);
            should_free_input = true;
        } else if (plan.table_name != null) {
            // Get table data directly
            input_data = try context.getTableData(plan.table_name.?);
            should_free_input = true;
        } else {
            return error.NoInputData;
        }

        // Get the first predicate (for now, we only support one)
        const predicate = plan.predicates.?[0];

        // Convert predicate to filter type
        const filter_type = switch (predicate.op) {
            .Eq => query_executor.FilterType.Equal,
            .Ne => query_executor.FilterType.NotEqual,
            .Gt => query_executor.FilterType.GreaterThan,
            .Lt => query_executor.FilterType.LessThan,
            .Ge => query_executor.FilterType.GreaterEqual,
            .Le => query_executor.FilterType.LessEqual,
            else => return error.UnsupportedFilterType,
        };

        // Execute filter on GPU
        // For now, we'll use a dummy value
        const filter_value: i32 = 100;
        const filtered_data = try self.gpu_executor.executeFilter(input_data, filter_type, filter_value);
        defer self.allocator.free(filtered_data);

        // Create a result set from the filtered data
        return try ResultSet.fromRawData(self.allocator, filtered_data);
    }

    /// Execute a join operation on the GPU
    fn executeJoin(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Check if we have two child plans
        if (plan.children == null or plan.children.?.len < 2) {
            return error.InsufficientChildPlans;
        }

        // Execute left child plan
        const left_result = try self.executePhysicalPlan(&plan.children.?[0], context);
        defer left_result.deinit();

        // Execute right child plan
        const right_result = try self.executePhysicalPlan(&plan.children.?[1], context);
        defer right_result.deinit();

        // Convert result sets to raw data
        const left_data = try left_result.toRawData(self.allocator);
        defer self.allocator.free(left_data);

        const right_data = try right_result.toRawData(self.allocator);
        defer self.allocator.free(right_data);

        // Get join information
        var join_type = query_executor.JoinType.Inner;
        if (plan.join_type) |jt| {
            join_type = switch (jt) {
                .Inner => .Inner,
                .Left => .Left,
                .Right => .Right,
                .Full => .Full,
                else => .Inner,
            };
        }

        // Get join columns
        var left_join_col: usize = 0;
        var right_join_col: usize = 0;
        if (plan.join_condition) |jc| {
            if (jc.left_col) |lc| {
                left_join_col = lc;
            }
            if (jc.right_col) |rc| {
                right_join_col = rc;
            }
        }

        // Determine data type
        const data_type = cuda.CudaDataType.Int32; // Default to Int32

        // Use hash join for better performance
        const joined_data = try self.hash_join.executeHashJoin(left_data, right_data, join_type, left_join_col, right_join_col, data_type);
        defer self.allocator.free(joined_data);

        // Create a result set from the joined data
        return try ResultSet.fromRawData(self.allocator, joined_data);
    }

    /// Execute an aggregation operation on the GPU
    fn executeAggregate(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Implementation will be added in a future update
        _ = self;
        _ = plan;
        _ = context;
        return error.NotImplemented;
    }

    /// Execute a sort operation on the GPU
    fn executeSort(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Implementation will be added in a future update
        _ = self;
        _ = plan;
        _ = context;
        return error.NotImplemented;
    }

    /// Execute a group by operation on the GPU
    fn executeGroupBy(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Implementation will be added in a future update
        _ = self;
        _ = plan;
        _ = context;
        return error.NotImplemented;
    }

    /// Execute a window function on the GPU
    fn executeWindowFunction(self: *GpuQueryIntegration, plan: *PhysicalPlan, context: *DatabaseContext) !ResultSet {
        // Check if we have a child plan
        if (plan.children == null or plan.children.?.len < 1) {
            return error.InsufficientChildPlans;
        }

        // Execute child plan
        const child_result = try self.executePhysicalPlan(&plan.children.?[0], context);
        defer child_result.deinit();

        // Convert result set to raw data
        const input_data = try child_result.toRawData(self.allocator);
        defer self.allocator.free(input_data);

        // Get window function information
        var window_func_type = WindowFunctionType.RowNumber; // Default
        if (plan.window_function) |wf| {
            window_func_type = switch (wf) {
                .RowNumber => .RowNumber,
                .Rank => .Rank,
                .DenseRank => .DenseRank,
                .Sum => .Sum,
                .Avg => .Avg,
                .Min => .Min,
                .Max => .Max,
                .Count => .Count,
                else => .RowNumber,
            };
        }

        // Get column index
        var column_index: usize = 0;
        if (plan.window_column) |wc| {
            column_index = wc;
        }

        // Get partition by columns
        var partition_by_columns: ?[]const usize = null;
        if (plan.partition_by) |pb| {
            partition_by_columns = pb;
        }

        // Get order by columns
        var order_by_columns: ?[]const usize = null;
        if (plan.order_by) |ob| {
            order_by_columns = ob;
        }

        // Get window frame
        var window_frame: ?WindowFrame = null;
        if (plan.window_frame != null) {
            window_frame = WindowFrame{
                .frame_type = .Rows, // Default
                .start_type = .UnboundedPreceding,
                .end_type = .CurrentRow,
                .start_offset = 0,
                .end_offset = 0,
            };
        }

        // Determine data type
        const data_type = cuda.CudaDataType.Int32; // Default to Int32

        // Execute window function
        const result_data = try self.window_function.executeWindowFunction(input_data, window_func_type, data_type, column_index, partition_by_columns, order_by_columns, window_frame);
        defer self.allocator.free(result_data);

        // Create a result set from the result data
        return try ResultSet.fromRawData(self.allocator, result_data);
    }
};

/// Helper function to determine if a plan should use GPU
pub fn shouldUseGpu(plan: *PhysicalPlan, row_count: usize) bool {
    // Check if the plan is suitable for GPU execution
    const suitable_node_types = [_]planner.PhysicalNodeType{
        .TableScan,
        .Filter,
        .Join,
        .Aggregate,
        .Sort,
        .GroupBy,
    };

    // Check if the node type is suitable
    var suitable_type = false;
    for (suitable_node_types) |node_type| {
        if (plan.node_type == node_type) {
            suitable_type = true;
            break;
        }
    }

    if (!suitable_type) {
        return false;
    }

    // For small data sets, CPU is faster due to transfer overhead
    const min_rows_for_gpu = 10000;
    return row_count >= min_rows_for_gpu;
}
