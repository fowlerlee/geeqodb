const std = @import("std");

pub fn main() !void {
    // Create a simple key-value store
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var state = std.StringHashMap([]const u8).init(allocator);
    defer {
        var it = state.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        state.deinit();
    }

    // Add a key-value pair
    const key = try allocator.dupe(u8, "key1");
    errdefer allocator.free(key);
    const value = try allocator.dupe(u8, "value1");
    errdefer allocator.free(value);

    try state.put(key, value);

    // Verify that the key-value pair was added
    if (state.get("key1")) |stored_value| {
        try std.testing.expectEqualStrings("value1", stored_value);
        std.debug.print("Test passed: key1=value1 found in state\n", .{});
    } else {
        std.debug.print("Key 'key1' not found in state.\n", .{});
        return error.KeyNotFound;
    }

    std.debug.print("Simple test completed successfully!\n", .{});
}
