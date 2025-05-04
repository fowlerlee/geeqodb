const std = @import("std");
const cuda = @import("cuda.zig");

/// GPU device detection and management
pub const GpuDevice = struct {
    allocator: std.mem.Allocator,
    devices: std.ArrayList(Device),

    /// GPU device information
    pub const Device = struct {
        id: u32,
        name: []const u8,
        memory_size: u64,
        compute_capability: ComputeCapability,

        pub const ComputeCapability = struct {
            major: u32,
            minor: u32,
        };
    };

    /// Initialize GPU device manager
    pub fn init(allocator: std.mem.Allocator) !*GpuDevice {
        const manager = try allocator.create(GpuDevice);
        manager.* = GpuDevice{
            .allocator = allocator,
            .devices = std.ArrayList(Device).init(allocator),
        };

        // Detect available GPU devices
        try manager.detectDevices();

        return manager;
    }

    /// Clean up resources
    pub fn deinit(self: *GpuDevice) void {
        // Free device names
        for (self.devices.items) |device| {
            self.allocator.free(device.name);
        }

        self.devices.deinit();
        self.allocator.destroy(self);
    }

    /// Detect available GPU devices
    fn detectDevices(self: *GpuDevice) !void {
        // Try to initialize CUDA
        const cuda_instance = cuda.Cuda.init() catch |err| {
            std.debug.print("CUDA initialization failed: {s}\n", .{@errorName(err)});
            return;
        };

        // Add CUDA devices
        for (0..cuda_instance.device_count) |i| {
            const cuda_device_info = cuda_instance.getDeviceInfo(i) catch {
                continue;
            };

            const device = Device{
                .id = @intCast(i),
                .name = try std.fmt.allocPrint(self.allocator, "{s}", .{cuda_device_info.name}),
                .memory_size = cuda_device_info.total_memory,
                .compute_capability = .{
                    .major = @intCast(cuda_device_info.compute_capability_major),
                    .minor = @intCast(cuda_device_info.compute_capability_minor),
                },
            };

            try self.devices.append(device);
        }

        // If no CUDA devices were found, try other APIs
        if (self.devices.items.len == 0) {
            // Check if OpenCL is available
            if (try self.detectOpenCLDevices()) {
                return;
            }

            // Check if Metal is available (macOS)
            if (try self.detectMetalDevices()) {
                return;
            }
        }
    }

    /// Detect CUDA devices
    fn detectCudaDevices(self: *GpuDevice) !bool {
        // In a real implementation, this would use the CUDA API
        // to detect and query CUDA-capable devices

        // For now, we'll simulate CUDA device detection
        // based on environment variables for testing

        const cuda_device_count = std.process.getEnvVarOwned(
            self.allocator,
            "GEEQODB_CUDA_DEVICE_COUNT",
        ) catch |err| {
            if (err == error.EnvironmentVariableNotFound) {
                return false;
            }
            return err;
        };
        defer self.allocator.free(cuda_device_count);

        const count = std.fmt.parseInt(u32, cuda_device_count, 10) catch {
            return false;
        };

        if (count == 0) {
            return false;
        }

        // Add simulated CUDA devices
        for (0..count) |i| {
            const device = Device{
                .id = @intCast(i),
                .name = try std.fmt.allocPrint(self.allocator, "CUDA Device {d}", .{i}),
                .memory_size = 8 * 1024 * 1024 * 1024, // 8GB
                .compute_capability = .{
                    .major = 8,
                    .minor = 0,
                },
            };

            try self.devices.append(device);
        }

        return true;
    }

    /// Detect OpenCL devices
    fn detectOpenCLDevices(self: *GpuDevice) !bool {
        // In a real implementation, this would use the OpenCL API
        // to detect and query OpenCL-capable devices

        // For now, we'll simulate OpenCL device detection
        // based on environment variables for testing

        const opencl_device_count = std.process.getEnvVarOwned(
            self.allocator,
            "GEEQODB_OPENCL_DEVICE_COUNT",
        ) catch |err| {
            if (err == error.EnvironmentVariableNotFound) {
                return false;
            }
            return err;
        };
        defer self.allocator.free(opencl_device_count);

        const count = std.fmt.parseInt(u32, opencl_device_count, 10) catch {
            return false;
        };

        if (count == 0) {
            return false;
        }

        // Add simulated OpenCL devices
        for (0..count) |i| {
            const device = Device{
                .id = @intCast(i),
                .name = try std.fmt.allocPrint(self.allocator, "OpenCL Device {d}", .{i}),
                .memory_size = 4 * 1024 * 1024 * 1024, // 4GB
                .compute_capability = .{
                    .major = 2,
                    .minor = 0,
                },
            };

            try self.devices.append(device);
        }

        return true;
    }

    /// Detect Metal devices (macOS)
    fn detectMetalDevices(self: *GpuDevice) !bool {
        // In a real implementation, this would use the Metal API
        // to detect and query Metal-capable devices

        // For now, we'll simulate Metal device detection
        // based on environment variables for testing

        const metal_device_count = std.process.getEnvVarOwned(
            self.allocator,
            "GEEQODB_METAL_DEVICE_COUNT",
        ) catch |err| {
            if (err == error.EnvironmentVariableNotFound) {
                return false;
            }
            return err;
        };
        defer self.allocator.free(metal_device_count);

        const count = std.fmt.parseInt(u32, metal_device_count, 10) catch {
            return false;
        };

        if (count == 0) {
            return false;
        }

        // Add simulated Metal devices
        for (0..count) |i| {
            const device = Device{
                .id = @intCast(i),
                .name = try std.fmt.allocPrint(self.allocator, "Metal Device {d}", .{i}),
                .memory_size = 6 * 1024 * 1024 * 1024, // 6GB
                .compute_capability = .{
                    .major = 2,
                    .minor = 0,
                },
            };

            try self.devices.append(device);
        }

        return true;
    }

    /// Check if any GPU devices are available
    pub fn hasGpu(self: *GpuDevice) bool {
        return self.devices.items.len > 0;
    }

    /// Get the number of available GPU devices
    pub fn getDeviceCount(self: *GpuDevice) usize {
        return self.devices.items.len;
    }

    /// Get a specific GPU device
    pub fn getDevice(self: *GpuDevice, index: usize) !Device {
        if (index >= self.devices.items.len) {
            return error.DeviceNotFound;
        }

        return self.devices.items[index];
    }

    /// Get the best available GPU device
    pub fn getBestDevice(self: *GpuDevice) !Device {
        if (self.devices.items.len == 0) {
            return error.NoDevicesAvailable;
        }

        // In a real implementation, we would select the best device
        // based on compute capability, memory size, etc.

        // For now, just return the first device
        return self.devices.items[0];
    }

    /// Check if a specific GPU device is suitable for a given memory requirement
    pub fn isDeviceSuitable(self: *GpuDevice, device_index: usize, required_memory: u64) !bool {
        const device = try self.getDevice(device_index);

        // Check if the device has enough memory
        // In practice, we would want to leave some headroom
        const memory_headroom = 0.8; // Use up to 80% of device memory
        const usable_memory = @as(u64, @intFromFloat(@as(f64, @floatFromInt(device.memory_size)) * memory_headroom));

        return required_memory <= usable_memory;
    }
};

test "GpuDevice initialization" {
    const allocator = std.testing.allocator;

    // Set environment variables for testing
    try std.process.setEnvVar("GEEQODB_CUDA_DEVICE_COUNT", "2");
    defer std.process.unsetEnvVar("GEEQODB_CUDA_DEVICE_COUNT") catch {};

    // Initialize GpuDevice
    const device_manager = try GpuDevice.init(allocator);
    defer device_manager.deinit();

    // Verify initialization
    try std.testing.expectEqual(allocator, device_manager.allocator);
    try std.testing.expectEqual(@as(usize, 2), device_manager.getDeviceCount());
}

test "GpuDevice detection" {
    const allocator = std.testing.allocator;

    // Test with no devices
    try std.process.setEnvVar("GEEQODB_CUDA_DEVICE_COUNT", "0");
    defer std.process.unsetEnvVar("GEEQODB_CUDA_DEVICE_COUNT") catch {};

    try std.process.setEnvVar("GEEQODB_OPENCL_DEVICE_COUNT", "0");
    defer std.process.unsetEnvVar("GEEQODB_OPENCL_DEVICE_COUNT") catch {};

    try std.process.setEnvVar("GEEQODB_METAL_DEVICE_COUNT", "0");
    defer std.process.unsetEnvVar("GEEQODB_METAL_DEVICE_COUNT") catch {};

    // Initialize GpuDevice
    const device_manager = try GpuDevice.init(allocator);
    defer device_manager.deinit();

    // Verify no devices were detected
    try std.testing.expectEqual(@as(usize, 0), device_manager.getDeviceCount());
    try std.testing.expect(!device_manager.hasGpu());
}

test "GpuDevice with OpenCL" {
    const allocator = std.testing.allocator;

    // Set environment variables for testing
    try std.process.setEnvVar("GEEQODB_CUDA_DEVICE_COUNT", "0");
    defer std.process.unsetEnvVar("GEEQODB_CUDA_DEVICE_COUNT") catch {};

    try std.process.setEnvVar("GEEQODB_OPENCL_DEVICE_COUNT", "1");
    defer std.process.unsetEnvVar("GEEQODB_OPENCL_DEVICE_COUNT") catch {};

    // Initialize GpuDevice
    const device_manager = try GpuDevice.init(allocator);
    defer device_manager.deinit();

    // Verify OpenCL device was detected
    try std.testing.expectEqual(@as(usize, 1), device_manager.getDeviceCount());
    try std.testing.expect(device_manager.hasGpu());

    // Get the device
    const device = try device_manager.getDevice(0);

    // Verify device properties
    try std.testing.expectEqualStrings("OpenCL Device 0", device.name);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024 * 1024), device.memory_size);
}

test "GpuDevice suitability check" {
    const allocator = std.testing.allocator;

    // Set environment variables for testing
    try std.process.setEnvVar("GEEQODB_CUDA_DEVICE_COUNT", "1");
    defer std.process.unsetEnvVar("GEEQODB_CUDA_DEVICE_COUNT") catch {};

    // Initialize GpuDevice
    const device_manager = try GpuDevice.init(allocator);
    defer device_manager.deinit();

    // Check suitability for different memory requirements
    const small_memory = 1 * 1024 * 1024 * 1024; // 1GB
    const large_memory = 10 * 1024 * 1024 * 1024; // 10GB

    const suitable_small = try device_manager.isDeviceSuitable(0, small_memory);
    const suitable_large = try device_manager.isDeviceSuitable(0, large_memory);

    // Verify suitability
    try std.testing.expect(suitable_small);
    try std.testing.expect(!suitable_large);
}
