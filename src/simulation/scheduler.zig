const std = @import("std");
const assert = std.debug.assert;

/// Deterministic Scheduler for simulation testing
/// Controls the execution order of tasks in the simulation
pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    tasks: std.ArrayList(Task),
    current_time: u64,
    seed: u64,
    task_id_counter: u64,

    pub const Task = struct {
        id: u64,
        scheduled_time: u64,
        priority: u8,
        callback: *const fn (context: ?*anyopaque) void,
        context: ?*anyopaque,
    };

    /// Initialize a new scheduler
    pub fn init(allocator: std.mem.Allocator, seed: u64) !*Scheduler {
        const scheduler = try allocator.create(Scheduler);

        scheduler.* = Scheduler{
            .allocator = allocator,
            .tasks = std.ArrayList(Task).init(allocator),
            .current_time = 0,
            .seed = seed,
            .task_id_counter = 0,
        };

        return scheduler;
    }

    /// Deinitialize the scheduler
    pub fn deinit(self: *Scheduler) void {
        self.tasks.deinit();
        self.allocator.destroy(self);
    }

    /// Schedule a task to be executed at a specific time
    pub fn scheduleAt(self: *Scheduler, time: u64, priority: u8, callback: *const fn (context: ?*anyopaque) void, context: ?*anyopaque) !u64 {
        const task_id = self.task_id_counter;
        self.task_id_counter += 1;

        const task = Task{
            .id = task_id,
            .scheduled_time = time,
            .priority = priority,
            .callback = callback,
            .context = context,
        };

        try self.tasks.append(task);
        return task_id;
    }

    /// Schedule a task to be executed after a delay from the current time
    pub fn scheduleAfter(self: *Scheduler, delay: u64, priority: u8, callback: *const fn (context: ?*anyopaque) void, context: ?*anyopaque) !u64 {
        return self.scheduleAt(self.current_time + delay, priority, callback, context);
    }

    /// Cancel a scheduled task
    pub fn cancel(self: *Scheduler, task_id: u64) bool {
        for (self.tasks.items, 0..) |task, i| {
            if (task.id == task_id) {
                _ = self.tasks.orderedRemove(i);
                return true;
            }
        }
        return false;
    }

    /// Run the simulation until there are no more tasks or until max_time is reached
    pub fn run(self: *Scheduler, max_time: ?u64) !void {
        while (self.tasks.items.len > 0) {
            // Sort tasks by time, then by priority
            std.mem.sort(Task, self.tasks.items, {}, taskCompare);

            const next_task = self.tasks.orderedRemove(0);

            // Check if we've reached the max time
            if (max_time) |max| {
                if (next_task.scheduled_time > max) {
                    // Put the task back and exit
                    try self.tasks.append(next_task);
                    break;
                }
            }

            // Update current time
            self.current_time = next_task.scheduled_time;

            // Execute the task
            next_task.callback(next_task.context);
        }
    }

    /// Run a single step of the simulation
    pub fn step(self: *Scheduler) !bool {
        if (self.tasks.items.len == 0) {
            return false;
        }

        // Sort tasks by time, then by priority
        std.mem.sort(Task, self.tasks.items, {}, taskCompare);

        const next_task = self.tasks.orderedRemove(0);

        // Update current time
        self.current_time = next_task.scheduled_time;

        // Execute the task
        next_task.callback(next_task.context);

        return true;
    }

    /// Get the current simulation time
    pub fn getCurrentTime(self: *Scheduler) u64 {
        return self.current_time;
    }

    /// Get a random number in the range [0, max)
    pub fn getRandomInt(self: *Scheduler, max: u64) u64 {
        // Simple LCG random number generator
        const a: u64 = 6364136223846793005;
        const c: u64 = 1442695040888963407;

        // Update the seed
        self.seed = (a *% self.seed +% c);

        // Return a value in the range [0, max)
        return self.seed % max;
    }

    /// Get a random float in the range [0.0, 1.0)
    pub fn getRandomFloat(self: *Scheduler) f64 {
        const max_u64: u64 = std.math.maxInt(u64);
        const random_u64 = self.getRandomInt(max_u64);
        return @as(f64, @floatFromInt(random_u64)) / @as(f64, @floatFromInt(max_u64));
    }

    /// Compare tasks for sorting (by time, then by priority)
    fn taskCompare(_: void, a: Task, b: Task) bool {
        if (a.scheduled_time < b.scheduled_time) {
            return true;
        }
        if (a.scheduled_time > b.scheduled_time) {
            return false;
        }
        return a.priority < b.priority;
    }
};

test "Scheduler basic functionality" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var context1: i32 = 0;
    var context2: i32 = 0;

    _ = try scheduler.scheduleAt(10, 0, testCallback, &context1);
    const task2_id = try scheduler.scheduleAt(20, 0, testCallback, &context2);
    _ = task2_id;

    try scheduler.run(null);

    try std.testing.expectEqual(@as(i32, 1), context1);
    try std.testing.expectEqual(@as(i32, 1), context2);
    try std.testing.expectEqual(@as(u64, 20), scheduler.getCurrentTime());
}

test "Scheduler cancel task" {
    const allocator = std.testing.allocator;
    const seed = 42;

    var scheduler = try Scheduler.init(allocator, seed);
    defer scheduler.deinit();

    var context1: i32 = 0;
    var context2: i32 = 0;

    _ = try scheduler.scheduleAt(10, 0, testCallback, &context1);
    const task2_id = try scheduler.scheduleAt(20, 0, testCallback, &context2);

    // Cancel the second task
    const canceled = scheduler.cancel(task2_id);
    try std.testing.expect(canceled);

    try scheduler.run(null);

    try std.testing.expectEqual(@as(i32, 1), context1);
    try std.testing.expectEqual(@as(i32, 0), context2); // Should not be executed
    try std.testing.expectEqual(@as(u64, 10), scheduler.getCurrentTime());
}

fn testCallback(context: ?*anyopaque) void {
    if (context) |ctx| {
        const value = @as(*i32, @ptrCast(@alignCast(ctx)));
        value.* += 1;
    }
}
