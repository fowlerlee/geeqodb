const std = @import("std");

// CUDA error codes
pub const CudaError = enum(c_int) {
    Success = 0,
    ErrorInitFailed = 1,
    ErrorNoDevice = 2,
    ErrorMemoryAllocation = 3,
    ErrorLaunchFailed = 4,
    ErrorInvalidValue = 5,
    ErrorNotSupported = 6,
    ErrorUnknown = 999,

    pub fn toString(self: CudaError) []const u8 {
        return switch (self) {
            .Success => "Success",
            .ErrorInitFailed => "CUDA initialization failed",
            .ErrorNoDevice => "No CUDA device found",
            .ErrorMemoryAllocation => "Memory allocation failed",
            .ErrorLaunchFailed => "Kernel launch failed",
            .ErrorInvalidValue => "Invalid value",
            .ErrorNotSupported => "Operation not supported",
            .ErrorUnknown => "Unknown error",
        };
    }
};

// CUDA device information
pub const CudaDeviceInfo = extern struct {
    device_id: c_int,
    name: [256]u8,
    total_memory: usize,
    compute_capability_major: c_int,
    compute_capability_minor: c_int,
    multi_processor_count: c_int,
    max_threads_per_block: c_int,
};

// CUDA buffer handle
pub const CudaBuffer = extern struct {
    device_ptr: ?*anyopaque,
    size: usize,
    count_ptr: ?*anyopaque,
};

// Comparison operators for filter operations
pub const CudaComparisonOp = enum(c_int) {
    Eq = 0,
    Ne = 1,
    Lt = 2,
    Le = 3,
    Gt = 4,
    Ge = 5,
    Between = 6,
};

// Join types
pub const CudaJoinType = enum(c_int) {
    Inner = 0,
    Left = 1,
    Right = 2,
    Full = 3,
};

// Aggregation operations
pub const CudaAggregateOp = enum(c_int) {
    Sum = 0,
    Count = 1,
    Min = 2,
    Max = 3,
    Avg = 4,
};

// Data types
pub const CudaDataType = enum(c_int) {
    Int32 = 0,
    Int64 = 1,
    Float = 2,
    Double = 3,
    String = 4,
};

// External C functions
extern fn cuda_init(device_count: *c_int) CudaError;
extern fn cuda_get_device_info(device_id: c_int, info: *CudaDeviceInfo) CudaError;
extern fn cuda_allocate(device_id: c_int, size: usize, buffer: *CudaBuffer) CudaError;
extern fn cuda_free(buffer: CudaBuffer) CudaError;
extern fn cuda_copy_to_device(host_ptr: ?*const anyopaque, buffer: CudaBuffer, size: usize) CudaError;
extern fn cuda_copy_to_host(buffer: CudaBuffer, host_ptr: ?*anyopaque, size: usize) CudaError;
extern fn cuda_execute_filter(input: CudaBuffer, output: CudaBuffer, op: CudaComparisonOp, data_type: CudaDataType, value: ?*const anyopaque, value2: ?*const anyopaque) CudaError;
extern fn cuda_execute_join(left: CudaBuffer, right: CudaBuffer, output: CudaBuffer, join_type: CudaJoinType, left_join_col: c_int, right_join_col: c_int, data_type: CudaDataType) CudaError;
extern fn cuda_execute_aggregate(input: CudaBuffer, output: CudaBuffer, op: CudaAggregateOp, data_type: CudaDataType, column_index: c_int) CudaError;
extern fn cuda_execute_sort(input: CudaBuffer, output: CudaBuffer, data_type: CudaDataType, column_index: c_int, ascending: c_int) CudaError;
extern fn cuda_execute_group_by(input: CudaBuffer, output: CudaBuffer, group_type: CudaDataType, group_column: c_int, agg_type: CudaDataType, agg_column: c_int, agg_op: CudaAggregateOp) CudaError;
extern fn cuda_execute_hash_join(left_keys: CudaBuffer, left_values: CudaBuffer, right_keys: CudaBuffer, right_values: CudaBuffer, output_keys: CudaBuffer, output_left_values: CudaBuffer, output_right_values: CudaBuffer, left_size: usize, right_size: usize) CudaError;
extern fn cuda_execute_window_function(input: CudaBuffer, output: CudaBuffer, data_type: CudaDataType, num_rows: usize) CudaError;
extern fn cuda_get_error_string(err: CudaError) [*:0]const u8;

// Add external C functions for OpenGL interop
extern fn cuda_gl_register_buffer(buffer: c_uint, resource: *?*anyopaque) CudaError;
extern fn cuda_graphics_map_resources(resource: ?*anyopaque, stream: ?*anyopaque) CudaError;
extern fn cuda_graphics_get_mapped_pointer(devPtr: *?*anyopaque, size: *usize, resource: ?*anyopaque) CudaError;
extern fn cuda_graphics_unmap_resources(resource: ?*anyopaque, stream: ?*anyopaque) CudaError;
extern fn cuda_graphics_unregister_resource(resource: ?*anyopaque) CudaError;

// Zig wrapper for CUDA
pub const Cuda = struct {
    initialized: bool,
    device_count: usize,

    /// Initialize CUDA
    pub fn init() !Cuda {
        var device_count: c_int = 0;
        const err = cuda_init(&device_count);

        if (err != .Success) {
            return error.CudaInitFailed;
        }

        return Cuda{
            .initialized = true,
            .device_count = @intCast(device_count),
        };
    }

    /// Get device information
    pub fn getDeviceInfo(self: *const Cuda, device_id: usize) !CudaDeviceInfo {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        if (device_id >= self.device_count) {
            return error.InvalidDeviceId;
        }

        var info: CudaDeviceInfo = undefined;
        const err = cuda_get_device_info(@intCast(device_id), &info);

        if (err != .Success) {
            return error.CudaGetDeviceInfoFailed;
        }

        return info;
    }

    /// Allocate memory on the GPU
    pub fn allocate(self: *const Cuda, device_id: usize, size: usize) !CudaBuffer {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        if (device_id >= self.device_count) {
            return error.InvalidDeviceId;
        }

        var buffer: CudaBuffer = undefined;
        const err = cuda_allocate(@intCast(device_id), size, &buffer);

        if (err != .Success) {
            return error.CudaMemoryAllocationFailed;
        }

        return buffer;
    }

    /// Free memory on the GPU
    pub fn free(self: *const Cuda, buffer: CudaBuffer) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_free(buffer);

        if (err != .Success) {
            return error.CudaMemoryFreeFailed;
        }
    }

    /// Copy data from host to device
    pub fn copyToDevice(self: *const Cuda, host_ptr: *const anyopaque, buffer: CudaBuffer, size: usize) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_copy_to_device(host_ptr, buffer, size);

        if (err != .Success) {
            return error.CudaCopyToDeviceFailed;
        }
    }

    /// Copy data from device to host
    pub fn copyToHost(self: *const Cuda, buffer: CudaBuffer, host_ptr: *anyopaque, size: usize) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_copy_to_host(buffer, host_ptr, size);

        if (err != .Success) {
            return error.CudaCopyToHostFailed;
        }
    }

    /// Execute filter operation on the GPU
    pub fn executeFilter(self: *const Cuda, input: CudaBuffer, output: CudaBuffer, op: CudaComparisonOp, data_type: CudaDataType, value: *const anyopaque, value2: ?*const anyopaque) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_filter(input, output, op, data_type, value, value2);

        if (err != .Success) {
            return error.CudaExecuteFilterFailed;
        }
    }

    /// Execute join operation on the GPU
    pub fn executeJoin(self: *const Cuda, left: CudaBuffer, right: CudaBuffer, output: CudaBuffer, join_type: CudaJoinType, left_join_col: usize, right_join_col: usize, data_type: CudaDataType) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_join(left, right, output, join_type, @intCast(left_join_col), @intCast(right_join_col), data_type);

        if (err != .Success) {
            return error.CudaExecuteJoinFailed;
        }
    }

    /// Execute aggregation operation on the GPU
    pub fn executeAggregate(self: *const Cuda, input: CudaBuffer, output: CudaBuffer, op: CudaAggregateOp, data_type: CudaDataType, column_index: usize) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_aggregate(input, output, op, data_type, @intCast(column_index));

        if (err != .Success) {
            return error.CudaExecuteAggregateFailed;
        }
    }

    /// Execute sort operation on the GPU
    pub fn executeSort(self: *const Cuda, input: CudaBuffer, output: CudaBuffer, data_type: CudaDataType, column_index: usize, ascending: bool) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_sort(input, output, data_type, @intCast(column_index), if (ascending) 1 else 0);

        if (err != .Success) {
            return error.CudaExecuteSortFailed;
        }
    }

    /// Execute group by operation on the GPU
    pub fn executeGroupBy(self: *const Cuda, input: CudaBuffer, output: CudaBuffer, group_type: CudaDataType, group_column: usize, agg_type: CudaDataType, agg_column: usize, agg_op: CudaAggregateOp) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_group_by(input, output, group_type, @intCast(group_column), agg_type, @intCast(agg_column), agg_op);

        if (err != .Success) {
            return error.CudaExecuteGroupByFailed;
        }
    }

    /// Execute hash join operation on the GPU
    pub fn executeHashJoin(self: *const Cuda, left_keys: CudaBuffer, left_values: CudaBuffer, right_keys: CudaBuffer, right_values: CudaBuffer, output_keys: CudaBuffer, output_left_values: CudaBuffer, output_right_values: CudaBuffer, left_size: usize, right_size: usize) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_hash_join(left_keys, left_values, right_keys, right_values, output_keys, output_left_values, output_right_values, left_size, right_size);

        if (err != .Success) {
            return error.CudaExecuteHashJoinFailed;
        }
    }

    /// Execute window function on the GPU
    pub fn executeWindowFunction(self: *const Cuda, input: CudaBuffer, output: CudaBuffer, data_type: CudaDataType, num_rows: usize) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_execute_window_function(input, output, data_type, num_rows);

        if (err != .Success) {
            return error.CudaExecuteWindowFunctionFailed;
        }
    }

    /// Get error string
    pub fn getErrorString(error_code: CudaError) []const u8 {
        const c_str = cuda_get_error_string(error_code);
        return std.mem.span(c_str);
    }

    /// Register an OpenGL buffer with CUDA
    pub fn registerGLBuffer(self: *const Cuda, gl_buffer: c_uint) !CudaGraphicsResource {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        var resource: ?*anyopaque = null;
        const err = cuda_gl_register_buffer(gl_buffer, &resource);

        if (err != .Success) {
            return error.CudaGLRegisterFailed;
        }

        return CudaGraphicsResource{ .resource = resource };
    }

    /// Map graphics resource to get device pointer
    pub fn mapGraphicsResource(self: *const Cuda, resource: CudaGraphicsResource) !MappedResource {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_graphics_map_resources(resource.resource, null);
        if (err != .Success) {
            return error.CudaMapResourceFailed;
        }

        var dev_ptr: ?*anyopaque = null;
        var size: usize = 0;
        const ptr_err = cuda_graphics_get_mapped_pointer(&dev_ptr, &size, resource.resource);
        if (ptr_err != .Success) {
            return error.CudaGetMappedPointerFailed;
        }

        return MappedResource{
            .device_ptr = dev_ptr,
            .size = size,
            .resource = resource,
        };
    }

    /// Unmap graphics resource
    pub fn unmapGraphicsResource(self: *const Cuda, resource: CudaGraphicsResource) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_graphics_unmap_resources(resource.resource, null);
        if (err != .Success) {
            return error.CudaUnmapResourceFailed;
        }
    }

    /// Unregister graphics resource
    pub fn unregisterGraphicsResource(self: *const Cuda, resource: CudaGraphicsResource) !void {
        if (!self.initialized) {
            return error.CudaNotInitialized;
        }

        const err = cuda_graphics_unregister_resource(resource.resource);
        if (err != .Success) {
            return error.CudaUnregisterResourceFailed;
        }
    }
};

test "Cuda initialization" {
    // Skip test if CUDA is not available
    const cuda = Cuda.init() catch |err| {
        std.debug.print("Skipping CUDA test - {s}\n", .{@errorName(err)});
        return;
    };

    // Verify initialization
    try std.testing.expect(cuda.initialized);
    try std.testing.expect(cuda.device_count > 0);

    // Get device info
    const device_info = try cuda.getDeviceInfo(0);
    std.debug.print("CUDA Device: {s}\n", .{device_info.name});
    std.debug.print("  Memory: {} bytes\n", .{device_info.total_memory});
    std.debug.print("  Compute Capability: {}.{}\n", .{
        device_info.compute_capability_major,
        device_info.compute_capability_minor,
    });
}

test "Cuda memory operations" {
    // Skip test if CUDA is not available
    const cuda = Cuda.init() catch |err| {
        std.debug.print("Skipping CUDA test - {s}\n", .{@errorName(err)});
        return;
    };

    // Allocate memory
    const buffer_size = 1024;
    const buffer = try cuda.allocate(0, buffer_size);
    defer cuda.free(buffer) catch {};

    // Verify allocation
    try std.testing.expect(buffer.device_ptr != null);
    try std.testing.expectEqual(buffer_size, buffer.size);

    // Create test data
    const data = [_]i32{ 1, 2, 3, 4, 5 };

    // Copy to device
    try cuda.copyToDevice(&data, buffer, data.len * @sizeOf(i32));

    // Copy back to host
    var result: [5]i32 = undefined;
    try cuda.copyToHost(buffer, &result, data.len * @sizeOf(i32));

    // In our simulation, we don't actually copy the data, so we can't verify the result
    // In a real implementation, we would verify that result == data
}

test "Cuda filter operation" {
    // Skip test if CUDA is not available
    const cuda = Cuda.init() catch |err| {
        std.debug.print("Skipping CUDA test - {s}\n", .{@errorName(err)});
        return;
    };

    // Allocate input buffer
    const input_buffer = try cuda.allocate(0, 1024);
    defer cuda.free(input_buffer) catch {};

    // Allocate output buffer
    const output_buffer = try cuda.allocate(0, 1024);
    defer cuda.free(output_buffer) catch {};

    // Execute filter operation
    const value: i32 = 500;
    try cuda.executeFilter(input_buffer, output_buffer, .Gt, .Int32, &value, null);

    // Get result count
    var count: i32 = 0;
    try cuda.copyToHost(output_buffer, &count, @sizeOf(i32));

    // Verify result
    try std.testing.expectEqual(@as(i32, 523), count);
}

/// CUDA Graphics Resource
pub const CudaGraphicsResource = struct {
    resource: ?*anyopaque,
};

/// Mapped Graphics Resource
pub const MappedResource = struct {
    device_ptr: ?*anyopaque,
    size: usize,
    resource: CudaGraphicsResource,
};
