const std = @import("std");
const planner = @import("planner.zig");
const result = @import("result.zig");
const assert = @import("../build_options.zig").assert;
const Index = @import("../storage/index.zig").Index;
const BTreeMapIndex = @import("../storage/btree_index.zig").BTreeMapIndex;
const SkipListIndex = @import("../storage/skiplist_index.zig").SkipListIndex;

/// Database context for query execution
pub const DatabaseContext = struct {
    allocator: std.mem.Allocator,
    indexes: std.StringHashMap(*anyopaque),

    pub fn init(allocator: std.mem.Allocator) !*DatabaseContext {
        const context = try allocator.create(DatabaseContext);
        context.* = DatabaseContext{
            .allocator = allocator,
            .indexes = std.StringHashMap(*anyopaque).init(allocator),
        };
        return context;
    }

    pub fn deinit(self: *DatabaseContext) void {
        self.indexes.deinit();
        self.allocator.destroy(self);
    }

    /// Register a BTreeMap index with the database context
    pub fn registerBTreeIndex(self: *DatabaseContext, name: []const u8, index: *BTreeMapIndex) !void {
        try self.indexes.put(name, @ptrCast(index));
    }

    /// Register a SkipList index with the database context
    pub fn registerSkipListIndex(self: *DatabaseContext, name: []const u8, index: *SkipListIndex) !void {
        try self.indexes.put(name, @ptrCast(index));
    }

    /// Get a BTreeMap index by name
    pub fn getBTreeIndex(self: *DatabaseContext, name: []const u8) ?*BTreeMapIndex {
        const index_ptr = self.indexes.get(name) orelse return null;
        return @ptrCast(@alignCast(index_ptr));
    }

    /// Get a SkipList index by name
    pub fn getSkipListIndex(self: *DatabaseContext, name: []const u8) ?*SkipListIndex {
        const index_ptr = self.indexes.get(name) orelse return null;
        return @ptrCast(@alignCast(index_ptr));
    }
};

/// Query executor for executing physical plans
pub const QueryExecutor = struct {
    /// Execute a physical plan and return a result set
    pub fn execute(allocator: std.mem.Allocator, plan: *planner.PhysicalPlan, context: *DatabaseContext) !result.ResultSet {
        // Execute the plan based on its node type
        switch (plan.node_type) {
            .IndexSeek => return try executeIndexSeek(allocator, plan, context),
            .IndexRangeScan => return try executeIndexRangeScan(allocator, plan, context),
            .IndexScan => return try executeIndexScan(allocator, plan, context),
            .TableScan => return try executeTableScan(allocator, plan, context),
            else => {
                // For other node types, we would implement specific execution strategies
                // For now, we'll just return an empty result set
                return try result.ResultSet.init(allocator, 0, 0);
            },
        }
    }

    /// Execute an index seek operation (direct lookup using an index)
    fn executeIndexSeek(allocator: std.mem.Allocator, plan: *planner.PhysicalPlan, context: *DatabaseContext) !result.ResultSet {
        // Validate that we have the necessary information
        if (plan.index_info == null or plan.predicates == null or plan.predicates.?.len == 0) {
            return error.InvalidPlan;
        }

        const index_info = plan.index_info.?;
        const predicates = plan.predicates.?;

        // Find the equality predicate for the indexed column
        var key_value: ?planner.PlanValue = null;
        for (predicates) |pred| {
            if (std.mem.eql(u8, pred.column, index_info.column_name) and pred.op == .Eq) {
                key_value = pred.value;
                break;
            }
        }

        if (key_value == null) {
            return error.MissingPredicate;
        }

        // Convert the key value to the appropriate type
        const key = switch (key_value.?) {
            .Integer => |i| i,
            else => return error.UnsupportedKeyType,
        };

        // Use the appropriate index type
        switch (index_info.index_type) {
            .BTree => {
                const index = context.getBTreeIndex(index_info.name) orelse return error.IndexNotFound;

                // Look up the row ID using the index
                const row_id = index.get(key) orelse return try result.ResultSet.init(allocator, 0, 0);

                // In a real implementation, we would fetch the row data using the row ID
                // For now, we'll just create a simple result set with the row ID
                var result_set = try result.ResultSet.init(allocator, 1, 1);

                // Create a column for the row ID
                var column = &result_set.columns[0];
                column.name = try allocator.dupe(u8, "row_id");
                column.data_type = .UInt64;

                // Allocate memory for the data
                const data = try allocator.alloc(u8, @sizeOf(u64));
                const row_id_bytes: []u8 = @as(*[8]u8, @ptrCast(@constCast(&row_id)))[0..8];
                @memcpy(data, row_id_bytes);
                column.data = data;
                column.row_count = 1;

                return result_set;
            },
            .SkipList => {
                const index = context.getSkipListIndex(index_info.name) orelse return error.IndexNotFound;

                // Look up the row ID using the index
                const row_id = index.get(key) orelse return try result.ResultSet.init(allocator, 0, 0);

                // In a real implementation, we would fetch the row data using the row ID
                // For now, we'll just create a simple result set with the row ID
                var result_set = try result.ResultSet.init(allocator, 1, 1);

                // Create a column for the row ID
                var column = &result_set.columns[0];
                column.name = try allocator.dupe(u8, "row_id");
                column.data_type = .UInt64;

                // Allocate memory for the data
                const data = try allocator.alloc(u8, @sizeOf(u64));
                const row_id_bytes: []u8 = @as(*[8]u8, @ptrCast(@constCast(&row_id)))[0..8];
                @memcpy(data, row_id_bytes);
                column.data = data;
                column.row_count = 1;

                return result_set;
            },
        }
    }

    /// Execute an index range scan operation
    fn executeIndexRangeScan(allocator: std.mem.Allocator, plan: *planner.PhysicalPlan, context: *DatabaseContext) !result.ResultSet {
        // Validate that we have the necessary information
        if (plan.index_info == null or plan.predicates == null or plan.predicates.?.len == 0) {
            return error.InvalidPlan;
        }

        // In a real implementation, we would scan the index within the specified range
        // For now, we'll just return an empty result set
        _ = context; // Would be used in a real implementation
        return try result.ResultSet.init(allocator, 0, 0);
    }

    /// Execute an index scan operation
    fn executeIndexScan(allocator: std.mem.Allocator, plan: *planner.PhysicalPlan, context: *DatabaseContext) !result.ResultSet {
        // Validate that we have the necessary information
        if (plan.index_info == null) {
            return error.InvalidPlan;
        }

        // In a real implementation, we would scan the entire index
        // For now, we'll just return an empty result set
        _ = context; // Would be used in a real implementation
        return try result.ResultSet.init(allocator, 0, 0);
    }

    /// Execute a table scan operation
    fn executeTableScan(allocator: std.mem.Allocator, plan: *planner.PhysicalPlan, context: *DatabaseContext) !result.ResultSet {
        // In a real implementation, we would scan the entire table
        // For now, we'll just return an empty result set
        _ = plan; // Would be used in a real implementation
        _ = context; // Would be used in a real implementation
        return try result.ResultSet.init(allocator, 0, 0);
    }
};

test "QueryExecutor basic functionality" {
    const allocator = std.testing.allocator;
    const planner_instance = try planner.QueryPlanner.init(allocator);
    defer planner_instance.deinit();

    // Create a database context
    const context = try DatabaseContext.init(allocator);
    defer context.deinit();

    const ast = try planner_instance.parse("SELECT * FROM test");
    defer ast.deinit();

    const logical_plan = try planner_instance.plan(ast);
    defer logical_plan.deinit();

    const physical_plan = try planner_instance.optimize(logical_plan);
    defer physical_plan.deinit();

    var result_set = try QueryExecutor.execute(allocator, physical_plan, context);
    defer result_set.deinit();
}

test "QueryExecutor with index seek" {
    const allocator = std.testing.allocator;

    // Create a database context
    const context = try DatabaseContext.init(allocator);
    defer context.deinit();

    // Create a BTreeMap index
    const btree_index = try BTreeMapIndex.create(allocator, "idx_users_id", "users", "id");
    defer btree_index.deinit();

    // Insert some data into the index
    try btree_index.insert(1, 101);
    try btree_index.insert(2, 102);
    try btree_index.insert(3, 103);

    // Register the index with the context
    try context.registerBTreeIndex("idx_users_id", btree_index);

    // Create a query planner
    const planner_instance = try planner.QueryPlanner.init(allocator);
    defer planner_instance.deinit();

    // Register the index with the planner
    try planner_instance.registerIndex("idx_users_id", "users", "id", .BTree, 3, 3);

    // Create a logical plan with a predicate that can use the index
    var logical_plan = try allocator.create(planner.LogicalPlan);
    logical_plan.* = planner.LogicalPlan{
        .allocator = allocator,
        .node_type = .Scan,
        .table_name = try allocator.dupe(u8, "users"),
        .columns = null,
        .children = null,
        .predicates = null,
    };

    // Add a predicate for id = 2
    const pred = try allocator.alloc(planner.Predicate, 1);
    pred[0] = planner.Predicate{
        .column = try allocator.dupe(u8, "id"),
        .op = .Eq,
        .value = planner.PlanValue{ .Integer = 2 },
        .value2 = null,
        .value_list = null,
    };
    logical_plan.predicates = pred;

    // Optimize the plan
    const physical_plan = try planner_instance.optimize(logical_plan);
    defer physical_plan.deinit();
    defer logical_plan.deinit();

    // Execute the plan
    var result_set = try QueryExecutor.execute(allocator, physical_plan, context);
    defer result_set.deinit();

    // Verify the result
    try std.testing.expectEqual(@as(usize, 1), result_set.columns.len);
    try std.testing.expectEqual(@as(usize, 1), result_set.row_count);
    try std.testing.expectEqualStrings("row_id", result_set.columns[0].name);

    // Get the row ID from the result
    const row_id = try result_set.getValue(0, 0, u64);
    try std.testing.expectEqual(@as(u64, 102), row_id);
}
