const std = @import("std");

pub fn main() !void {
    std.debug.print("Testing basic allocator functionality\n", .{});

    const allocator = std.heap.page_allocator;

    // Try a simple allocation
    const memory = try allocator.alloc(u8, 100);
    defer allocator.free(memory);

    std.debug.print("Allocation successful!\n", .{});
}
