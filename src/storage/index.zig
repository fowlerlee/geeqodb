const std = @import("std");
const assert = @import("../build_options.zig").assert;

/// Index interface for database indexes
/// This defines the common operations that all index implementations must support
pub const Index = struct {
    /// The type of the index
    pub const IndexType = enum {
        BTree,
        SkipList,
    };

    /// Create a new index
    pub fn create(allocator: std.mem.Allocator, index_type: IndexType, name: []const u8) !*Index {
        _ = name; // Will be used in the future
        
        const index = try allocator.create(Index);
        index.* = Index{
            .allocator = allocator,
            .index_type = index_type,
        };
        
        return index;
    }

    /// Deinitialize the index
    pub fn deinit(self: *Index) void {
        self.allocator.destroy(self);
    }

    // Fields
    allocator: std.mem.Allocator,
    index_type: IndexType,
};

/// BTreeMap index implementation
pub const BTreeMapIndex = struct {
    /// Create a new BTreeMap index
    pub fn create(allocator: std.mem.Allocator, name: []const u8) !*BTreeMapIndex {
        const index = try allocator.create(BTreeMapIndex);
        index.* = BTreeMapIndex{
            .allocator = allocator,
            .name = try allocator.dupe(u8, name),
        };
        
        return index;
    }

    /// Deinitialize the BTreeMap index
    pub fn deinit(self: *BTreeMapIndex) void {
        self.allocator.free(self.name);
        self.allocator.destroy(self);
    }

    // Fields
    allocator: std.mem.Allocator,
    name: []const u8,
};

/// SkipList index implementation
pub const SkipListIndex = struct {
    /// Create a new SkipList index
    pub fn create(allocator: std.mem.Allocator, name: []const u8) !*SkipListIndex {
        const index = try allocator.create(SkipListIndex);
        index.* = SkipListIndex{
            .allocator = allocator,
            .name = try allocator.dupe(u8, name),
        };
        
        return index;
    }

    /// Deinitialize the SkipList index
    pub fn deinit(self: *SkipListIndex) void {
        self.allocator.free(self.name);
        self.allocator.destroy(self);
    }

    // Fields
    allocator: std.mem.Allocator,
    name: []const u8,
};

test "Index creation" {
    const allocator = std.testing.allocator;
    
    // Create a BTree index
    const btree_index = try Index.create(allocator, .BTree, "test_btree_index");
    defer btree_index.deinit();
    
    // Verify that the index was created correctly
    try std.testing.expectEqual(Index.IndexType.BTree, btree_index.index_type);
    
    // Create a SkipList index
    const skiplist_index = try Index.create(allocator, .SkipList, "test_skiplist_index");
    defer skiplist_index.deinit();
    
    // Verify that the index was created correctly
    try std.testing.expectEqual(Index.IndexType.SkipList, skiplist_index.index_type);
}
