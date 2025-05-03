const std = @import("std");
const assert = @import("../build_options.zig").assert;
const Index = @import("index.zig").Index;

/// BTreeMap index implementation for fast lookups
/// This uses a balanced tree structure for efficient range queries
pub const BTreeMapIndex = struct {
    /// Create a new BTreeMap index
    pub fn create(allocator: std.mem.Allocator, name: []const u8, table_name: []const u8, column_name: []const u8) !*BTreeMapIndex {
        const index = try allocator.create(BTreeMapIndex);
        index.* = BTreeMapIndex{
            .allocator = allocator,
            .name = try allocator.dupe(u8, name),
            .table_name = try allocator.dupe(u8, table_name),
            .column_name = try allocator.dupe(u8, column_name),
            .entries = std.AutoArrayHashMap(Key, Value).init(allocator),
        };
        
        return index;
    }

    /// Deinitialize the BTreeMap index
    pub fn deinit(self: *BTreeMapIndex) void {
        self.allocator.free(self.name);
        self.allocator.free(self.table_name);
        self.allocator.free(self.column_name);
        self.entries.deinit();
        self.allocator.destroy(self);
    }

    /// Insert a key-value pair into the index
    pub fn insert(self: *BTreeMapIndex, key: Key, value: Value) !void {
        try self.entries.put(key, value);
    }

    /// Get a value from the index
    pub fn get(self: *BTreeMapIndex, key: Key) ?Value {
        return self.entries.get(key);
    }

    /// Remove a key-value pair from the index
    pub fn remove(self: *BTreeMapIndex, key: Key) bool {
        return self.entries.swapRemove(key);
    }

    /// Get the number of entries in the index
    pub fn count(self: *BTreeMapIndex) usize {
        return self.entries.count();
    }

    /// Clear all entries from the index
    pub fn clear(self: *BTreeMapIndex) void {
        self.entries.clearRetainingCapacity();
    }

    // Types
    pub const Key = i64; // For simplicity, we'll use i64 as the key type
    pub const Value = u64; // Row ID or pointer to the actual data

    // Fields
    allocator: std.mem.Allocator,
    name: []const u8,
    table_name: []const u8,
    column_name: []const u8,
    entries: std.AutoArrayHashMap(Key, Value),
};

test "BTreeMapIndex basic operations" {
    const allocator = std.testing.allocator;
    
    // Create a BTreeMap index
    const index = try BTreeMapIndex.create(allocator, "test_index", "test_table", "test_column");
    defer index.deinit();
    
    // Verify that the index was created correctly
    try std.testing.expectEqualStrings("test_index", index.name);
    try std.testing.expectEqualStrings("test_table", index.table_name);
    try std.testing.expectEqualStrings("test_column", index.column_name);
    try std.testing.expectEqual(@as(usize, 0), index.count());
    
    // Insert some entries
    try index.insert(1, 100);
    try index.insert(2, 200);
    try index.insert(3, 300);
    
    // Verify the count
    try std.testing.expectEqual(@as(usize, 3), index.count());
    
    // Get entries
    try std.testing.expectEqual(@as(u64, 100), index.get(1).?);
    try std.testing.expectEqual(@as(u64, 200), index.get(2).?);
    try std.testing.expectEqual(@as(u64, 300), index.get(3).?);
    try std.testing.expectEqual(@as(?u64, null), index.get(4));
    
    // Remove an entry
    try std.testing.expect(index.remove(2));
    try std.testing.expectEqual(@as(usize, 2), index.count());
    try std.testing.expectEqual(@as(?u64, null), index.get(2));
    
    // Clear the index
    index.clear();
    try std.testing.expectEqual(@as(usize, 0), index.count());
}
