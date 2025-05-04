const std = @import("std");
const device = @import("device.zig");
const cuda = @import("cuda.zig");
const GpuDevice = device.GpuDevice;

/// GPU memory management
pub const GpuMemory = struct {
    allocator: std.mem.Allocator,
    device: GpuDevice.Device,
    cuda_instance: cuda.Cuda,
    buffers: std.ArrayList(Buffer),

    /// GPU buffer
    pub const Buffer = struct {
        id: u32,
        size: usize,
        cuda_buffer: cuda.CudaBuffer,
    };

    /// Initialize GPU memory manager
    pub fn init(allocator: std.mem.Allocator, dev: GpuDevice.Device) !*GpuMemory {
        // Initialize CUDA
        const cuda_instance = try cuda.Cuda.init();

        const memory = try allocator.create(GpuMemory);
        memory.* = GpuMemory{
            .allocator = allocator,
            .device = dev,
            .cuda_instance = cuda_instance,
            .buffers = std.ArrayList(Buffer).init(allocator),
        };

        return memory;
    }

    /// Clean up resources
    pub fn deinit(self: *GpuMemory) void {
        // Free all allocated buffers
        for (self.buffers.items) |buffer| {
            // Free the buffer using CUDA
            self.cuda_instance.free(buffer.cuda_buffer) catch |err| {
                std.debug.print("Error freeing CUDA buffer: {s}\n", .{@errorName(err)});
            };
        }

        self.buffers.deinit();
        self.allocator.destroy(self);
    }

    /// Allocate a GPU buffer
    pub fn allocate(self: *GpuMemory, size: usize) !Buffer {
        // Check if we have enough memory
        if (!try self.checkMemoryAvailable(size)) {
            return error.OutOfGpuMemory;
        }

        // Allocate a buffer ID
        const id = @as(u32, @intCast(self.buffers.items.len));

        // Allocate GPU memory using CUDA
        const cuda_buffer = try self.cuda_instance.allocate(0, size);

        // Create buffer
        const buffer = Buffer{
            .id = id,
            .size = size,
            .cuda_buffer = cuda_buffer,
        };

        // Add to buffer list
        try self.buffers.append(buffer);

        return buffer;
    }

    /// Allocate a GPU buffer and copy data to it
    pub fn allocateAndCopy(self: *GpuMemory, comptime T: type, data: []const T) !Buffer {
        const size = data.len * @sizeOf(T);
        const buffer = try self.allocate(size);

        // Copy data to GPU
        try self.cuda_instance.copyToDevice(data.ptr, buffer.cuda_buffer, size);

        return buffer;
    }

    /// Free a GPU buffer
    pub fn free(self: *GpuMemory, buffer: Buffer) void {
        // Free GPU memory using CUDA
        self.cuda_instance.free(buffer.cuda_buffer) catch |err| {
            std.debug.print("Error freeing CUDA buffer: {s}\n", .{@errorName(err)});
        };

        // Remove from our list
        for (self.buffers.items, 0..) |buf, i| {
            if (buf.id == buffer.id) {
                // Remove from list
                _ = self.buffers.orderedRemove(i);
                break;
            }
        }
    }

    /// Copy data from host to device
    pub fn copyToDevice(self: *GpuMemory, src: *const anyopaque, buffer: Buffer, size: usize) !void {
        try self.cuda_instance.copyToDevice(src, buffer.cuda_buffer, size);
    }

    /// Copy data from device to host
    pub fn copyToHost(self: *GpuMemory, dst: *anyopaque, buffer: Buffer, size: usize) !void {
        try self.cuda_instance.copyToHost(buffer.cuda_buffer, dst, size);
    }

    /// Check if enough memory is available on the GPU
    fn checkMemoryAvailable(self: *GpuMemory, size: usize) !bool {
        // Calculate total allocated memory
        var total_allocated: usize = 0;
        for (self.buffers.items) |buffer| {
            total_allocated += buffer.size;
        }

        // Get device info to check memory
        const device_info = try self.cuda_instance.getDeviceInfo(0);

        // Check if we have enough memory
        const memory_headroom = 0.8; // Use up to 80% of device memory
        const usable_memory = @as(u64, @intFromFloat(@as(f64, @floatFromInt(device_info.total_memory)) * memory_headroom));

        return total_allocated + size <= usable_memory;
    }
};

test "GpuMemory initialization" {
    const allocator = std.testing.allocator;

    // Create a test device
    const dev = GpuDevice.Device{
        .id = 0,
        .name = "Test Device",
        .memory_size = 8 * 1024 * 1024 * 1024, // 8GB
        .compute_capability = .{
            .major = 8,
            .minor = 0,
        },
    };

    // Initialize GpuMemory
    const memory = try GpuMemory.init(allocator, dev);
    defer memory.deinit();

    // Verify initialization
    try std.testing.expectEqual(allocator, memory.allocator);
    try std.testing.expectEqualStrings("Test Device", memory.device.name);
    try std.testing.expectEqual(@as(usize, 0), memory.buffers.items.len);
}

test "GpuMemory allocation" {
    const allocator = std.testing.allocator;

    // Create a test device
    const dev = GpuDevice.Device{
        .id = 0,
        .name = "Test Device",
        .memory_size = 8 * 1024 * 1024 * 1024, // 8GB
        .compute_capability = .{
            .major = 8,
            .minor = 0,
        },
    };

    // Initialize GpuMemory
    const memory = try GpuMemory.init(allocator, dev);
    defer memory.deinit();

    // Allocate a buffer
    const buffer_size = 1024 * 1024; // 1MB
    const buffer = try memory.allocate(buffer_size);

    // Verify allocation
    try std.testing.expectEqual(@as(u32, 0), buffer.id);
    try std.testing.expectEqual(buffer_size, buffer.size);
    try std.testing.expect(buffer.data_buffer != null);
    try std.testing.expect(buffer.count_buffer != null);

    // Verify buffer was added to the list
    try std.testing.expectEqual(@as(usize, 1), memory.buffers.items.len);

    // Free the buffer
    memory.free(buffer);

    // Verify buffer was removed from the list
    try std.testing.expectEqual(@as(usize, 0), memory.buffers.items.len);
}

test "GpuMemory allocate and copy" {
    const allocator = std.testing.allocator;

    // Create a test device
    const dev = GpuDevice.Device{
        .id = 0,
        .name = "Test Device",
        .memory_size = 8 * 1024 * 1024 * 1024, // 8GB
        .compute_capability = .{
            .major = 8,
            .minor = 0,
        },
    };

    // Initialize GpuMemory
    const memory = try GpuMemory.init(allocator, dev);
    defer memory.deinit();

    // Create test data
    const data = [_]i32{ 1, 2, 3, 4, 5 };

    // Allocate and copy
    const buffer = try memory.allocateAndCopy(i32, &data);
    defer memory.free(buffer);

    // Verify allocation
    try std.testing.expectEqual(@as(u32, 0), buffer.id);
    try std.testing.expectEqual(data.len * @sizeOf(i32), buffer.size);
    try std.testing.expect(buffer.data_buffer != null);
    try std.testing.expect(buffer.count_buffer != null);

    // Verify count
    var count: u32 = 0;
    try memory.copyToHost(&count, buffer.count_buffer);
    try std.testing.expectEqual(@as(u32, data.len), count);
}

test "GpuMemory out of memory" {
    const allocator = std.testing.allocator;

    // Create a test device with limited memory
    const dev = GpuDevice.Device{
        .id = 0,
        .name = "Test Device",
        .memory_size = 1024 * 1024, // 1MB
        .compute_capability = .{
            .major = 8,
            .minor = 0,
        },
    };

    // Initialize GpuMemory
    const memory = try GpuMemory.init(allocator, dev);
    defer memory.deinit();

    // Allocate a buffer that fits
    const small_buffer = try memory.allocate(512 * 1024); // 512KB
    defer memory.free(small_buffer);

    // Try to allocate a buffer that's too large
    const large_size = 2 * 1024 * 1024; // 2MB
    const large_buffer = memory.allocate(large_size);

    // Verify allocation fails
    try std.testing.expectError(error.OutOfGpuMemory, large_buffer);
}
