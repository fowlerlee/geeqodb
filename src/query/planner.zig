/// Physical plan node types
pub const PhysicalNodeType = enum {
    TableScan,
    IndexSeek,
    IndexRangeScan,
    IndexScan,
    Filter,
    Project,
    NestedLoopJoin,
    HashJoin,
    Sort,
    Limit,
    Aggregate,
};
const std = @import("std");
const assert = @import("../build_options.zig").assert;
pub const Index = @import("../storage/index.zig").Index;

/// Abstract Syntax Tree for SQL queries
pub const AST = struct {
    allocator: std.mem.Allocator,
    node_type: NodeType,

    // Node types for different SQL statements and expressions
    pub const NodeType = enum {
        Select,
        Insert,
        Update,
        Delete,
        Create,
        Drop,
        Alter,
    };

    pub fn deinit(self: *AST) void {
        self.allocator.destroy(self);
    }
};

/// Access methods for table data
pub const AccessMethod = enum {
    TableScan, // Full table scan
    IndexSeek, // Direct lookup using an index
    IndexRange, // Range scan using an index
};

/// Logical plan node types
pub const LogicalNodeType = enum {
    Scan, // Table scan
    Filter, // Filter rows
    Project, // Select columns
    Join, // Join tables
    Sort, // Sort rows
    Limit, // Limit number of rows
    Aggregate, // Aggregate functions
};

/// Logical plan for query execution
pub const LogicalPlan = struct {
    allocator: std.mem.Allocator,
    node_type: LogicalNodeType,
    table_name: ?[]const u8,
    predicates: ?[]const Predicate,
    columns: ?[]const []const u8,
    children: ?[]LogicalPlan,

    pub fn deinit(self: *LogicalPlan) void {
        if (self.table_name) |name| {
            self.allocator.free(name);
        }
        if (self.predicates) |preds| {
            for (preds) |pred| {
                self.allocator.free(pred.column);
                if (pred.value == .String) {
                    self.allocator.free(pred.value.String);
                }
            }
            self.allocator.free(preds);
        }
        if (self.columns) |cols| {
            for (cols) |col| {
                self.allocator.free(col);
            }
            self.allocator.free(cols);
        }
        if (self.children) |kids| {
            for (kids) |*child| {
                child.deinit();
            }
            self.allocator.free(kids);
        }
        self.allocator.destroy(self);
    }
};

/// Predicate for filtering rows
pub const Predicate = struct {
    column: []const u8,
    op: PredicateOp,
    value: PlanValue,
};

/// Predicate operators
pub const PredicateOp = enum {
    Eq, // Equal
    Ne, // Not equal
    Lt, // Less than
    Le, // Less than or equal
    Gt, // Greater than
    Ge, // Greater than or equal
    Like, // LIKE pattern matching
    In, // IN list of values
};

/// Value types for predicates and expressions
pub const PlanValue = union(enum) {
    String: []const u8,
    Integer: i64,
    Float: f64,
    Boolean: bool,
    Null: void,
};

pub const QueryPlanner = struct {
    allocator: std.mem.Allocator,
    available_indexes: std.ArrayList(AvailableIndex),

    pub const AvailableIndex = struct {
        table_name: []const u8,
        column_name: []const u8,
        index_type: Index.IndexType,
    };

    /// Initialize a new query planner
    pub fn init(allocator: std.mem.Allocator) !*QueryPlanner {
        const planner = try allocator.create(QueryPlanner);
        planner.* = QueryPlanner{
            .allocator = allocator,
            .available_indexes = std.ArrayList(AvailableIndex).init(allocator),
        };
        return planner;
    }

    /// Deinitialize the query planner
    pub fn deinit(self: *QueryPlanner) void {
        for (self.available_indexes.items) |index| {
            self.allocator.free(index.table_name);
            self.allocator.free(index.column_name);
        }
        self.available_indexes.deinit();
        self.allocator.destroy(self);
    }

    /// Add an index to the available indexes
    pub fn addIndex(self: *QueryPlanner, table_name: []const u8, column_name: []const u8, index_type: Index.IndexType) !void {
        const index = AvailableIndex{
            .table_name = try self.allocator.dupe(u8, table_name),
            .column_name = try self.allocator.dupe(u8, column_name),
            .index_type = index_type,
        };
        try self.available_indexes.append(index);
    }

    /// Register an index with the planner (alias for addIndex)
    pub fn registerIndex(self: *QueryPlanner, index_name: []const u8, table_name: []const u8, column_name: []const u8, index_type: Index.IndexType) !void {
        _ = index_name; // Not used in the base planner
        try self.addIndex(table_name, column_name, index_type);
    }

    /// Find indexes for a column
    pub fn findIndexesForColumn(self: *QueryPlanner, table_name: []const u8, column_name: []const u8) ![]const AvailableIndex {
        var result = std.ArrayList(AvailableIndex).init(self.allocator);
        defer result.deinit();

        for (self.available_indexes.items) |index| {
            if (std.mem.eql(u8, index.table_name, table_name) and std.mem.eql(u8, index.column_name, column_name)) {
                try result.append(index);
            }
        }

        return try result.toOwnedSlice();
    }

    /// Parse a SQL query into an AST
    pub fn parse(self: *QueryPlanner, query: []const u8) !*AST {
        // Validate inputs
        if (query.len == 0) return error.EmptyQuery;

        const ast = try self.allocator.create(AST);
        ast.* = AST{
            .allocator = self.allocator,
            .node_type = .Select, // Default to Select for now
        };

        return ast;
    }

    /// Plan a query execution from an AST
    pub fn plan(self: *QueryPlanner, _: *AST) !*LogicalPlan {
        const logical_plan = try self.allocator.create(LogicalPlan);
        logical_plan.* = LogicalPlan{
            .allocator = self.allocator,
            .node_type = .Scan,
            .table_name = null,
            .predicates = null,
            .columns = null,
            .children = null,
        };

        return logical_plan;
    }

    /// Find the best index for a predicate
    pub fn findBestIndex(self: *QueryPlanner, table_name: []const u8, column_name: []const u8) !?AvailableIndex {
        const indexes = try self.findIndexesForColumn(table_name, column_name);
        defer self.allocator.free(indexes);

        if (indexes.len == 0) {
            return null;
        }

        // For now, just return the first index
        // In a real implementation, we would use statistics to choose the best index
        return indexes[0];
    }

    /// Find the best access method for a predicate
    pub fn findBestAccessMethod(self: *QueryPlanner, table_name: []const u8, column_name: []const u8) !AccessMethod {
        if (try self.findBestIndex(table_name, column_name)) |_| {
            return .IndexSeek;
        }
        return .TableScan;
    }
};

test "Query planner initialization" {
    const allocator = std.testing.allocator;
    const planner = try QueryPlanner.init(allocator);
    defer planner.deinit();

    try std.testing.expectEqual(allocator, planner.allocator);
    try std.testing.expectEqual(@as(usize, 0), planner.available_indexes.items.len);
}

test "Query planner index management" {
    const allocator = std.testing.allocator;
    const planner = try QueryPlanner.init(allocator);
    defer planner.deinit();

    // Add test indexes
    try planner.addIndex("users", "id", .BTree);
    try planner.addIndex("users", "email", .SkipList);

    // Test index lookup
    const indexes = try planner.findIndexesForColumn("users", "id");
    defer allocator.free(indexes);

    try std.testing.expectEqual(@as(usize, 1), indexes.len);
    try std.testing.expectEqual(indexes[0].index_type, .BTree);
}

test "Query planner empty query" {
    const allocator = std.testing.allocator;
    const planner = try QueryPlanner.init(allocator);
    defer planner.deinit();

    try std.testing.expectError(error.EmptyQuery, planner.parse(""));
}

test "Query planner basic query" {
    const allocator = std.testing.allocator;
    const planner = try QueryPlanner.init(allocator);
    defer planner.deinit();

    const ast = try planner.parse("SELECT * FROM users");
    defer ast.deinit();

    try std.testing.expectEqual(AST.NodeType.Select, ast.node_type);
}

test "Query planner logical plan" {
    const allocator = std.testing.allocator;
    const planner = try QueryPlanner.init(allocator);
    defer planner.deinit();

    const ast = try planner.parse("SELECT * FROM users");
    defer ast.deinit();

    const logical_plan = try planner.plan(ast);
    defer logical_plan.deinit();

    try std.testing.expectEqual(LogicalNodeType.Scan, logical_plan.node_type);
}

test "Query planner access method selection" {
    const allocator = std.testing.allocator;
    const planner = try QueryPlanner.init(allocator);
    defer planner.deinit();

    // Test without indexes
    try std.testing.expectEqual(AccessMethod.TableScan, try planner.findBestAccessMethod("users", "id"));

    // Test with indexes
    try planner.addIndex("users", "id", .BTree);
    try std.testing.expectEqual(AccessMethod.IndexSeek, try planner.findBestAccessMethod("users", "id"));
}
/// Physical plan for query execution
pub const PhysicalPlan = struct {
    node_type: PhysicalNodeType,
    allocator: std.mem.Allocator,
    access_method: AccessMethod,
    table_name: ?[]const u8,
    predicates: ?[]const Predicate,
    columns: ?[]const []const u8,
    children: ?[]PhysicalPlan,
    use_gpu: bool = false,
    parallel_degree: u32 = 1,

    pub fn deinit(self: *PhysicalPlan) void {
        if (self.table_name) |name| {
            self.allocator.free(name);
        }
        if (self.predicates) |preds| {
            for (preds) |pred| {
                self.allocator.free(pred.column);
                if (pred.value == .String) {
                    self.allocator.free(pred.value.String);
                }
            }
            self.allocator.free(preds);
        }
        if (self.columns) |cols| {
            for (cols) |col| {
                self.allocator.free(col);
            }
            self.allocator.free(cols);
        }
        if (self.children) |kids| {
            for (kids) |*child| {
                child.deinit();
            }
            self.allocator.free(kids);
        }
        self.allocator.destroy(self);
    }
};
/// Optimize a logical plan into a physical execution plan
pub fn optimize(self: *QueryPlanner, logical_plan: *LogicalPlan) !*PhysicalPlan {
    const physical_plan = try self.allocator.create(PhysicalPlan);
    physical_plan.* = PhysicalPlan{
        .allocator = self.allocator,
        .access_method = .TableScan,
        .table_name = logical_plan.table_name,
        .predicates = logical_plan.predicates,
        .columns = logical_plan.columns,
        .children = null,
    };
    return physical_plan;
}
