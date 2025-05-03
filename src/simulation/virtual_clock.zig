const std = @import("std");
const Scheduler = @import("scheduler.zig").Scheduler;

/// VirtualClock provides a simulated clock for deterministic time-based operations
pub const VirtualClock = struct {
    scheduler: *Scheduler,
    drift_factor: f64, // Simulated clock drift (1.0 = no drift)
    
    /// Initialize a new virtual clock
    pub fn init(scheduler: *Scheduler, drift_factor: f64) VirtualClock {
        return VirtualClock{
            .scheduler = scheduler,
            .drift_factor = drift_factor,
        };
    }
    
    /// Get the current time from the virtual clock
    pub fn now(self: *VirtualClock) u64 {
        const base_time = self.scheduler.getCurrentTime();
        if (self.drift_factor == 1.0) {
            return base_time;
        }
        
        // Apply drift factor
        const drifted_time = @as(f64, @floatFromInt(base_time)) * self.drift_factor;
        return @as(u64, @intFromFloat(drifted_time));
    }
    
    /// Sleep for a specified duration
    pub fn sleep(self: *VirtualClock, duration: u64, context: ?*anyopaque) !u64 {
        // Calculate the actual sleep duration with drift
        const actual_duration = if (self.drift_factor == 1.0)
            duration
        else
            @as(u64, @intFromFloat(@as(f64, @floatFromInt(duration)) * self.drift_factor));
            
        // Schedule a wake-up task
        return self.scheduler.scheduleAfter(actual_duration, 0, sleepCallback, context);
    }
    
    /// Set the drift factor for this clock
    pub fn setDriftFactor(self: *VirtualClock, drift_factor: f64) void {
        self.drift_factor = drift_factor;
    }
};

fn sleepCallback(context: ?*anyopaque) void {
    // This is just a placeholder for the sleep callback
    // In a real implementation, this might signal a condition variable or similar
    _ = context;
}

test "VirtualClock basic functionality" {
    const allocator = std.testing.allocator;
    const seed = 42;
    
    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();
    
    var clock = VirtualClock.init(scheduler, 1.0);
    
    // Initial time should be 0
    try std.testing.expectEqual(@as(u64, 0), clock.now());
    
    // Advance the scheduler time
    _ = try scheduler.scheduleAt(100, 0, dummyCallback, null);
    try scheduler.run(null);
    
    // Clock time should now be 100
    try std.testing.expectEqual(@as(u64, 100), clock.now());
}

test "VirtualClock with drift" {
    const allocator = std.testing.allocator;
    const seed = 42;
    
    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();
    
    // Clock running 20% faster
    var clock = VirtualClock.init(scheduler, 1.2);
    
    // Initial time should still be 0
    try std.testing.expectEqual(@as(u64, 0), clock.now());
    
    // Advance the scheduler time
    _ = try scheduler.scheduleAt(100, 0, dummyCallback, null);
    try scheduler.run(null);
    
    // Clock time should now be 120 (100 * 1.2)
    try std.testing.expectEqual(@as(u64, 120), clock.now());
}

test "VirtualClock sleep" {
    const allocator = std.testing.allocator;
    const seed = 42;
    
    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();
    
    var clock = VirtualClock.init(scheduler, 1.0);
    var context: i32 = 0;
    
    // Schedule a sleep
    _ = try clock.sleep(50, &context);
    
    // Run the scheduler
    try scheduler.run(null);
    
    // Time should have advanced by 50
    try std.testing.expectEqual(@as(u64, 50), scheduler.getCurrentTime());
}

fn dummyCallback(context: ?*anyopaque) void {
    _ = context;
}
