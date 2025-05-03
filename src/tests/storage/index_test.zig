const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const BTreeMapIndex = geeqodb.storage.btree_index.BTreeMapIndex;
const SkipListIndex = geeqodb.storage.skiplist_index.SkipListIndex;
const Index = geeqodb.storage.index.Index;

test "Index creation" {
    const allocator = testing.allocator;

    // Create a BTree index
    const btree_index = try Index.create(allocator, .BTree, "test_btree_index");
    defer btree_index.deinit();

    // Verify that the index was created correctly
    try testing.expectEqual(Index.IndexType.BTree, btree_index.index_type);

    // Create a SkipList index
    const skiplist_index = try Index.create(allocator, .SkipList, "test_skiplist_index");
    defer skiplist_index.deinit();

    // Verify that the index was created correctly
    try testing.expectEqual(Index.IndexType.SkipList, skiplist_index.index_type);
}

test "BTreeMapIndex creation and basic operations" {
    const allocator = testing.allocator;

    // Create a BTreeMap index
    const index = try BTreeMapIndex.create(allocator, "test_index", "test_table", "test_column");
    defer index.deinit();

    // Verify that the index was created correctly
    try testing.expectEqualStrings("test_index", index.name);
    try testing.expectEqualStrings("test_table", index.table_name);
    try testing.expectEqualStrings("test_column", index.column_name);
    try testing.expectEqual(@as(usize, 0), index.count());

    // Insert some entries
    try index.insert(1, 100);
    try index.insert(2, 200);
    try index.insert(3, 300);

    // Verify the count
    try testing.expectEqual(@as(usize, 3), index.count());

    // Get entries
    try testing.expectEqual(@as(u64, 100), index.get(1).?);
    try testing.expectEqual(@as(u64, 200), index.get(2).?);
    try testing.expectEqual(@as(u64, 300), index.get(3).?);
    try testing.expectEqual(@as(?u64, null), index.get(4));

    // Remove an entry
    try testing.expect(index.remove(2));
    try testing.expectEqual(@as(usize, 2), index.count());
    try testing.expectEqual(@as(?u64, null), index.get(2));

    // Clear the index
    index.clear();
    try testing.expectEqual(@as(usize, 0), index.count());
}

test "SkipListIndex creation and basic operations" {
    const allocator = testing.allocator;

    // Create a SkipList index
    const index = try SkipListIndex.create(allocator, "test_index", "test_table", "test_column");
    defer index.deinit();

    // Verify that the index was created correctly
    try testing.expectEqualStrings("test_index", index.name);
    try testing.expectEqualStrings("test_table", index.table_name);
    try testing.expectEqualStrings("test_column", index.column_name);
    try testing.expectEqual(@as(usize, 0), index.count());
    try testing.expectEqual(@as(usize, 16), index.max_level);
    try testing.expectEqual(@as(usize, 0), index.current_level);

    // Insert some entries
    try index.insert(1, 100);
    try index.insert(2, 200);
    try index.insert(3, 300);

    // Verify the count
    try testing.expectEqual(@as(usize, 3), index.count());

    // Get entries
    try testing.expectEqual(@as(u64, 100), index.get(1).?);
    try testing.expectEqual(@as(u64, 200), index.get(2).?);
    try testing.expectEqual(@as(u64, 300), index.get(3).?);
    try testing.expectEqual(@as(?u64, null), index.get(4));

    // Remove an entry
    try testing.expect(index.remove(2));
    try testing.expectEqual(@as(usize, 2), index.count());
    try testing.expectEqual(@as(?u64, null), index.get(2));

    // Clear the index
    index.clear();
    try testing.expectEqual(@as(usize, 0), index.count());
    try testing.expectEqual(@as(usize, 0), index.current_level);
}

test "Index performance comparison" {
    const allocator = testing.allocator;

    // Create indexes
    const btree_index = try BTreeMapIndex.create(allocator, "btree_index", "test_table", "test_column");
    defer btree_index.deinit();

    const skiplist_index = try SkipListIndex.create(allocator, "skiplist_index", "test_table", "test_column");
    defer skiplist_index.deinit();

    // Insert a large number of entries
    const num_entries = 1000;
    var i: i64 = 0;
    while (i < num_entries) : (i += 1) {
        try btree_index.insert(i, @as(u64, @intCast(i * 100)));
        try skiplist_index.insert(i, @as(u64, @intCast(i * 100)));
    }

    // Verify counts
    try testing.expectEqual(@as(usize, num_entries), btree_index.count());
    try testing.expectEqual(@as(usize, num_entries), skiplist_index.count());

    // Perform lookups
    i = 0;
    while (i < num_entries) : (i += 1) {
        const expected_value = @as(u64, @intCast(i * 100));
        try testing.expectEqual(expected_value, btree_index.get(i).?);
        try testing.expectEqual(expected_value, skiplist_index.get(i).?);
    }

    // Test non-existent keys
    try testing.expectEqual(@as(?u64, null), btree_index.get(num_entries));
    try testing.expectEqual(@as(?u64, null), skiplist_index.get(num_entries));
}

test "Index update operations" {
    const allocator = testing.allocator;

    // Create a BTreeMap index
    const index = try BTreeMapIndex.create(allocator, "test_index", "test_table", "test_column");
    defer index.deinit();

    // Insert some entries
    try index.insert(1, 100);
    try index.insert(2, 200);

    // Update an entry
    try index.insert(1, 150); // Overwrite the existing entry

    // Verify the update
    try testing.expectEqual(@as(u64, 150), index.get(1).?);
    try testing.expectEqual(@as(u64, 200), index.get(2).?);

    // Remove and reinsert
    _ = index.remove(2);
    try index.insert(2, 250);

    // Verify the update
    try testing.expectEqual(@as(u64, 150), index.get(1).?);
    try testing.expectEqual(@as(u64, 250), index.get(2).?);
}
