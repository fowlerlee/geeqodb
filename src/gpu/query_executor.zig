const std = @import("std");
const cuda = @import("cuda.zig");
const device = @import("device.zig");
const memory_manager = @import("memory_manager.zig");
const GpuDevice = device.GpuDevice;
const GpuMemoryManager = memory_manager.GpuMemoryManager;

/// GPU Query Executor
pub const GpuQueryExecutor = struct {
    allocator: std.mem.Allocator,
    cuda_instance: cuda.Cuda,
    device_id: usize,
    memory_manager: *GpuMemoryManager,

    /// Initialize GPU query executor
    pub fn init(allocator: std.mem.Allocator) !*GpuQueryExecutor {
        // Initialize CUDA
        const cuda_instance = try cuda.Cuda.init();

        // Use device 0 by default
        const device_id: usize = 0;

        // Initialize memory manager
        const mem_manager = try GpuMemoryManager.init(allocator);
        errdefer mem_manager.deinit();

        const executor = try allocator.create(GpuQueryExecutor);
        executor.* = GpuQueryExecutor{
            .allocator = allocator,
            .cuda_instance = cuda_instance,
            .device_id = device_id,
            .memory_manager = mem_manager,
        };

        return executor;
    }

    /// Initialize GPU query executor with a specific device
    pub fn initWithDevice(allocator: std.mem.Allocator, device_id: usize) !*GpuQueryExecutor {
        // Initialize CUDA
        const cuda_instance = try cuda.Cuda.init();

        // Verify device exists
        if (device_id >= cuda_instance.device_count) {
            return error.InvalidDeviceId;
        }

        // Initialize memory manager
        const mem_manager = try GpuMemoryManager.initWithDevice(allocator, device_id);
        errdefer mem_manager.deinit();

        const executor = try allocator.create(GpuQueryExecutor);
        executor.* = GpuQueryExecutor{
            .allocator = allocator,
            .cuda_instance = cuda_instance,
            .device_id = device_id,
            .memory_manager = mem_manager,
        };

        return executor;
    }

    /// Clean up resources
    pub fn deinit(self: *GpuQueryExecutor) void {
        self.memory_manager.deinit();
        self.allocator.destroy(self);
    }

    /// Clean up unused buffers
    pub fn cleanupUnusedBuffers(self: *GpuQueryExecutor, max_age_ms: i64) !void {
        try self.memory_manager.cleanupUnusedBuffers(max_age_ms);
    }

    /// Execute filter operation on GPU
    pub fn executeFilter(self: *GpuQueryExecutor, input_data: []const u8, filter_type: FilterType, filter_value: anytype) ![]u8 {
        // Generate a unique key for the input data
        const input_key = try std.fmt.allocPrint(self.allocator, "filter_input_{d}", .{@intFromPtr(input_data.ptr)});
        defer self.allocator.free(input_key);

        // Copy data to device with caching
        const input_buffer = try self.memory_manager.copyToDevice(input_key, input_data.ptr, input_data.len);
        defer self.memory_manager.releaseBuffer(input_key) catch {};

        // Generate a unique key for the output buffer
        const output_key = try std.fmt.allocPrint(self.allocator, "filter_output_{d}", .{@intFromPtr(input_data.ptr)});
        defer self.allocator.free(output_key);

        // Get or allocate output buffer
        const output_buffer = try self.memory_manager.getOrAllocateBuffer(output_key, input_data.len);
        defer self.memory_manager.releaseBuffer(output_key) catch {};

        // Execute filter kernel
        try self.cuda_instance.executeFilter(input_buffer, output_buffer, @enumFromInt(@intFromEnum(filter_type)), getDataType(filter_value), &filter_value, null);

        // Get result count
        var count: i32 = 0;
        try self.cuda_instance.copyToHost(output_buffer, &count, @sizeOf(i32));

        // Allocate result buffer
        const result_size = @as(usize, @intCast(count));
        const result = try self.allocator.alloc(u8, result_size);

        // Copy results back
        try self.cuda_instance.copyToHost(output_buffer, result.ptr, result.len);

        return result;
    }

    /// Execute join operation on GPU
    pub fn executeJoin(self: *GpuQueryExecutor, left_data: []const u8, right_data: []const u8, join_type: JoinType, left_join_col: usize, right_join_col: usize, data_type: cuda.CudaDataType) ![]u8 {
        // Generate unique keys for the input data
        const left_key = try std.fmt.allocPrint(self.allocator, "join_left_{d}", .{@intFromPtr(left_data.ptr)});
        defer self.allocator.free(left_key);

        const right_key = try std.fmt.allocPrint(self.allocator, "join_right_{d}", .{@intFromPtr(right_data.ptr)});
        defer self.allocator.free(right_key);

        // Copy data to device with caching
        const left_buffer = try self.memory_manager.copyToDevice(left_key, left_data.ptr, left_data.len);
        defer self.memory_manager.releaseBuffer(left_key) catch {};

        const right_buffer = try self.memory_manager.copyToDevice(right_key, right_data.ptr, right_data.len);
        defer self.memory_manager.releaseBuffer(right_key) catch {};

        // Generate a unique key for the output buffer
        const output_key = try std.fmt.allocPrint(self.allocator, "join_output_{d}_{d}", .{ @intFromPtr(left_data.ptr), @intFromPtr(right_data.ptr) });
        defer self.allocator.free(output_key);

        // Allocate output buffer - size is a heuristic based on input sizes
        const estimated_output_size = left_data.len + right_data.len;
        const output_buffer = try self.memory_manager.getOrAllocateBuffer(output_key, estimated_output_size);
        defer self.memory_manager.releaseBuffer(output_key) catch {};

        // Execute join kernel
        try self.cuda_instance.executeJoin(left_buffer, right_buffer, output_buffer, @enumFromInt(@intFromEnum(join_type)), @intCast(left_join_col), @intCast(right_join_col), data_type);

        // Get result count
        var count: i32 = 0;
        try self.cuda_instance.copyToHost(output_buffer, &count, @sizeOf(i32));

        // Allocate result buffer
        const result_size = @as(usize, @intCast(count));
        const result = try self.allocator.alloc(u8, result_size);

        // Copy results back
        try self.cuda_instance.copyToHost(output_buffer, result.ptr, result.len);

        return result;
    }

    /// Execute aggregation operation on GPU
    pub fn executeAggregate(self: *GpuQueryExecutor, input_data: []const u8, agg_op: AggregateType, data_type: cuda.CudaDataType, column_index: usize) ![]u8 {
        // Allocate input buffer
        const input_buffer = try self.cuda_instance.allocate(self.device_id, input_data.len);
        defer self.cuda_instance.free(input_buffer) catch {};

        // Copy data to device
        try self.cuda_instance.copyToDevice(input_data.ptr, input_buffer, input_data.len);

        // Allocate output buffer - for aggregates, this is typically small
        const output_size = 64; // Enough for any scalar result
        const output_buffer = try self.cuda_instance.allocate(self.device_id, output_size);
        defer self.cuda_instance.free(output_buffer) catch {};

        // Execute aggregate kernel
        try self.cuda_instance.executeAggregate(input_buffer, output_buffer, @enumFromInt(@intFromEnum(agg_op)), data_type, column_index);

        // Allocate result buffer
        const result = try self.allocator.alloc(u8, output_size);

        // Copy results back
        try self.cuda_instance.copyToHost(output_buffer, result.ptr, result.len);

        return result;
    }

    /// Execute sort operation on GPU
    pub fn executeSort(self: *GpuQueryExecutor, input_data: []const u8, data_type: cuda.CudaDataType, column_index: usize, ascending: bool) ![]u8 {
        // Allocate input buffer
        const input_buffer = try self.cuda_instance.allocate(self.device_id, input_data.len);
        defer self.cuda_instance.free(input_buffer) catch {};

        // Copy data to device
        try self.cuda_instance.copyToDevice(input_data.ptr, input_buffer, input_data.len);

        // Allocate output buffer
        const output_buffer = try self.cuda_instance.allocate(self.device_id, input_data.len);
        defer self.cuda_instance.free(output_buffer) catch {};

        // Execute sort kernel
        try self.cuda_instance.executeSort(input_buffer, output_buffer, data_type, column_index, ascending);

        // Allocate result buffer
        const result = try self.allocator.alloc(u8, input_data.len);

        // Copy results back
        try self.cuda_instance.copyToHost(output_buffer, result.ptr, result.len);

        return result;
    }

    /// Execute group by operation on GPU
    pub fn executeGroupBy(self: *GpuQueryExecutor, input_data: []const u8, group_type: cuda.CudaDataType, group_column: usize, agg_type: cuda.CudaDataType, agg_column: usize, agg_op: AggregateType) ![]u8 {
        // Allocate input buffer
        const input_buffer = try self.cuda_instance.allocate(self.device_id, input_data.len);
        defer self.cuda_instance.free(input_buffer) catch {};

        // Copy data to device
        try self.cuda_instance.copyToDevice(input_data.ptr, input_buffer, input_data.len);

        // Allocate output buffer - size is a heuristic
        const estimated_output_size = input_data.len / 2; // Assume 50% reduction
        const output_buffer = try self.cuda_instance.allocate(self.device_id, estimated_output_size);
        defer self.cuda_instance.free(output_buffer) catch {};

        // Execute group by kernel
        try self.cuda_instance.executeGroupBy(input_buffer, output_buffer, group_type, group_column, agg_type, agg_column, @enumFromInt(@intFromEnum(agg_op)));

        // Get result count
        var count: i32 = 0;
        try self.cuda_instance.copyToHost(output_buffer, &count, @sizeOf(i32));

        // Allocate result buffer
        const result_size = @as(usize, @intCast(count));
        const result = try self.allocator.alloc(u8, result_size);

        // Copy results back
        try self.cuda_instance.copyToHost(output_buffer, result.ptr, result.len);

        return result;
    }
};

/// Filter operation types
pub const FilterType = enum {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterEqual,
    LessEqual,
};

/// Join operation types
pub const JoinType = enum {
    Inner,
    Left,
    Right,
    Full,
};

/// Aggregation operation types
pub const AggregateType = enum {
    Sum,
    Count,
    Min,
    Max,
    Avg,
};

/// Get CUDA data type from Zig type
fn getDataType(value: anytype) cuda.CudaDataType {
    const T = @TypeOf(value);

    if (T == i32) return .Int32;
    if (T == i64) return .Int64;
    if (T == f32) return .Float;
    if (T == f64) return .Double;

    @compileError("Unsupported data type");
}
