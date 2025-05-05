const std = @import("std");
const cuda = @import("cuda.zig");
const device = @import("device.zig");

const Cuda = cuda.Cuda;
const CudaBuffer = cuda.CudaBuffer;
const GpuDevice = device.GpuDevice;

/// GPU Memory Manager for optimizing data transfers
pub const GpuMemoryManager = struct {
    allocator: std.mem.Allocator,
    cuda_instance: Cuda,
    device_id: usize,
    buffer_cache: std.StringHashMap(CachedBuffer),
    pinned_memory: std.StringHashMap(PinnedMemory),
    
    /// Cached buffer information
    const CachedBuffer = struct {
        buffer: CudaBuffer,
        last_used: i64,
        size: usize,
        reference_count: usize,
    };
    
    /// Pinned memory information
    const PinnedMemory = struct {
        host_ptr: [*]u8,
        size: usize,
        reference_count: usize,
    };
    
    /// Initialize GPU memory manager
    pub fn init(allocator: std.mem.Allocator) !*GpuMemoryManager {
        // Initialize CUDA
        const cuda_instance = try Cuda.init();
        
        // Use device 0 by default
        const device_id: usize = 0;
        
        // Create memory manager
        const manager = try allocator.create(GpuMemoryManager);
        manager.* = GpuMemoryManager{
            .allocator = allocator,
            .cuda_instance = cuda_instance,
            .device_id = device_id,
            .buffer_cache = std.StringHashMap(CachedBuffer).init(allocator),
            .pinned_memory = std.StringHashMap(PinnedMemory).init(allocator),
        };
        
        return manager;
    }
    
    /// Initialize GPU memory manager with a specific device
    pub fn initWithDevice(allocator: std.mem.Allocator, device_id: usize) !*GpuMemoryManager {
        // Initialize CUDA
        const cuda_instance = try Cuda.init();
        
        // Verify device exists
        if (device_id >= cuda_instance.device_count) {
            return error.InvalidDeviceId;
        }
        
        // Create memory manager
        const manager = try allocator.create(GpuMemoryManager);
        manager.* = GpuMemoryManager{
            .allocator = allocator,
            .cuda_instance = cuda_instance,
            .device_id = device_id,
            .buffer_cache = std.StringHashMap(CachedBuffer).init(allocator),
            .pinned_memory = std.StringHashMap(PinnedMemory).init(allocator),
        };
        
        return manager;
    }
    
    /// Clean up resources
    pub fn deinit(self: *GpuMemoryManager) void {
        // Free all cached buffers
        var buffer_it = self.buffer_cache.iterator();
        while (buffer_it.next()) |entry| {
            self.cuda_instance.free(entry.value_ptr.buffer) catch {};
        }
        self.buffer_cache.deinit();
        
        // Free all pinned memory
        var pinned_it = self.pinned_memory.iterator();
        while (pinned_it.next()) |entry| {
            self.freePinnedMemory(entry.value_ptr.host_ptr) catch {};
        }
        self.pinned_memory.deinit();
        
        self.allocator.destroy(self);
    }
    
    /// Get or allocate a GPU buffer
    pub fn getOrAllocateBuffer(self: *GpuMemoryManager, key: []const u8, size: usize) !CudaBuffer {
        // Check if buffer exists in cache
        if (self.buffer_cache.get(key)) |cached| {
            // Check if buffer is large enough
            if (cached.size >= size) {
                // Update last used time and reference count
                var entry = self.buffer_cache.getEntry(key).?;
                entry.value_ptr.last_used = std.time.milliTimestamp();
                entry.value_ptr.reference_count += 1;
                
                return cached.buffer;
            } else {
                // Buffer is too small, remove it from cache
                const buffer = self.buffer_cache.get(key).?.buffer;
                _ = self.buffer_cache.remove(key);
                self.cuda_instance.free(buffer) catch {};
            }
        }
        
        // Allocate new buffer
        const buffer = try self.cuda_instance.allocate(self.device_id, size);
        
        // Add to cache
        const key_owned = try self.allocator.dupe(u8, key);
        try self.buffer_cache.put(key_owned, .{
            .buffer = buffer,
            .last_used = std.time.milliTimestamp(),
            .size = size,
            .reference_count = 1,
        });
        
        return buffer;
    }
    
    /// Release a buffer (decrement reference count)
    pub fn releaseBuffer(self: *GpuMemoryManager, key: []const u8) !void {
        // Check if buffer exists in cache
        if (self.buffer_cache.getEntry(key)) |entry| {
            // Decrement reference count
            if (entry.value_ptr.reference_count > 0) {
                entry.value_ptr.reference_count -= 1;
            }
        }
    }
    
    /// Allocate pinned memory for faster transfers
    pub fn allocatePinnedMemory(self: *GpuMemoryManager, key: []const u8, size: usize) ![*]u8 {
        // Check if pinned memory exists
        if (self.pinned_memory.get(key)) |pinned| {
            // Check if memory is large enough
            if (pinned.size >= size) {
                // Update reference count
                var entry = self.pinned_memory.getEntry(key).?;
                entry.value_ptr.reference_count += 1;
                
                return pinned.host_ptr;
            } else {
                // Memory is too small, remove it
                const host_ptr = self.pinned_memory.get(key).?.host_ptr;
                _ = self.pinned_memory.remove(key);
                self.freePinnedMemory(host_ptr) catch {};
            }
        }
        
        // Allocate new pinned memory
        const host_ptr = try self.allocator.alloc(u8, size);
        
        // Add to cache
        const key_owned = try self.allocator.dupe(u8, key);
        try self.pinned_memory.put(key_owned, .{
            .host_ptr = host_ptr.ptr,
            .size = size,
            .reference_count = 1,
        });
        
        return host_ptr.ptr;
    }
    
    /// Free pinned memory
    fn freePinnedMemory(self: *GpuMemoryManager, host_ptr: [*]u8) !void {
        // In a real implementation, this would call cudaFreeHost
        // For now, we'll just free the memory
        const slice = @as([*]u8, @ptrCast(host_ptr))[0..0];
        self.allocator.free(slice);
    }
    
    /// Copy data from host to device with caching
    pub fn copyToDevice(self: *GpuMemoryManager, key: []const u8, host_ptr: *const anyopaque, size: usize) !CudaBuffer {
        // Get or allocate buffer
        const buffer = try self.getOrAllocateBuffer(key, size);
        
        // Copy data to device
        try self.cuda_instance.copyToDevice(host_ptr, buffer, size);
        
        return buffer;
    }
    
    /// Copy data from device to host
    pub fn copyToHost(self: *GpuMemoryManager, buffer: CudaBuffer, host_ptr: *anyopaque, size: usize) !void {
        // Copy data from device to host
        try self.cuda_instance.copyToHost(buffer, host_ptr, size);
    }
    
    /// Clean up unused buffers
    pub fn cleanupUnusedBuffers(self: *GpuMemoryManager, max_age_ms: i64) !void {
        const current_time = std.time.milliTimestamp();
        
        // Find buffers to remove
        var to_remove = std.ArrayList([]const u8).init(self.allocator);
        defer to_remove.deinit();
        
        var it = self.buffer_cache.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.reference_count == 0 and current_time - entry.value_ptr.last_used > max_age_ms) {
                try to_remove.append(entry.key_ptr.*);
            }
        }
        
        // Remove buffers
        for (to_remove.items) |key| {
            const buffer = self.buffer_cache.get(key).?.buffer;
            _ = self.buffer_cache.remove(key);
            self.cuda_instance.free(buffer) catch {};
            self.allocator.free(key);
        }
    }
};

test "GpuMemoryManager initialization" {
    const allocator = std.testing.allocator;
    
    // Initialize GPU memory manager
    const manager = GpuMemoryManager.init(allocator) catch |err| {
        std.debug.print("Skipping GPU test - {s}\n", .{@errorName(err)});
        return;
    };
    defer manager.deinit();
    
    // Verify initialization
    try std.testing.expect(manager.cuda_instance.initialized);
    try std.testing.expectEqual(@as(usize, 0), manager.buffer_cache.count());
    try std.testing.expectEqual(@as(usize, 0), manager.pinned_memory.count());
}

test "GpuMemoryManager buffer caching" {
    const allocator = std.testing.allocator;
    
    // Initialize GPU memory manager
    const manager = GpuMemoryManager.init(allocator) catch |err| {
        std.debug.print("Skipping GPU test - {s}\n", .{@errorName(err)});
        return;
    };
    defer manager.deinit();
    
    // Allocate a buffer
    const buffer1 = try manager.getOrAllocateBuffer("test_buffer", 1024);
    
    // Verify buffer was cached
    try std.testing.expectEqual(@as(usize, 1), manager.buffer_cache.count());
    
    // Get the same buffer again
    const buffer2 = try manager.getOrAllocateBuffer("test_buffer", 1024);
    
    // Verify it's the same buffer
    try std.testing.expectEqual(buffer1.device_ptr, buffer2.device_ptr);
    
    // Release the buffer
    try manager.releaseBuffer("test_buffer");
    try manager.releaseBuffer("test_buffer");
    
    // Clean up unused buffers
    try manager.cleanupUnusedBuffers(0);
    
    // Verify buffer was removed
    try std.testing.expectEqual(@as(usize, 0), manager.buffer_cache.count());
}
