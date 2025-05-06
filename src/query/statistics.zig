const std = @import("std");
const planner = @import("planner.zig");
const PlanValue = planner.PlanValue;

/// Statistics for query optimization
pub const Statistics = struct {
    allocator: std.mem.Allocator,
    table_stats: std.StringHashMap(TableStatistics),
    column_stats: std.StringHashMap(ColumnStatistics),

    /// Statistics for a table
    pub const TableStatistics = struct {
        row_count: u64,
        row_size: u64,
        last_updated: i64,
    };

    /// Statistics for a column
    pub const ColumnStatistics = struct {
        distinct_values: u64,
        min_value: PlanValue,
        max_value: PlanValue,
        null_count: u64,
        histogram: ?[]HistogramBucket,

        pub const HistogramBucket = struct {
            lower_bound: PlanValue,
            upper_bound: PlanValue,
            count: u64,
        };
    };

    /// Initialize a new statistics manager
    pub fn init(allocator: std.mem.Allocator) !*Statistics {
        const stats = try allocator.create(Statistics);
        stats.* = Statistics{
            .allocator = allocator,
            .table_stats = std.StringHashMap(TableStatistics).init(allocator),
            .column_stats = std.StringHashMap(ColumnStatistics).init(allocator),
        };
        return stats;
    }

    /// Clean up resources
    pub fn deinit(self: *Statistics) void {
        // Free table statistics keys
        var table_it = self.table_stats.iterator();
        while (table_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.table_stats.deinit();

        // Free column statistics keys and histograms
        var column_it = self.column_stats.iterator();
        while (column_it.next()) |entry| {
            if (entry.value_ptr.histogram) |histogram| {
                self.allocator.free(histogram);
            }
            self.allocator.free(entry.key_ptr.*);
        }
        self.column_stats.deinit();

        self.allocator.destroy(self);
    }

    /// Add statistics for a table
    pub fn addTableStatistics(self: *Statistics, table_name: []const u8, row_count: u64) !void {
        const stats = TableStatistics{
            .row_count = row_count,
            .row_size = 100, // Default row size
            .last_updated = std.time.timestamp(),
        };

        const key = try self.allocator.dupe(u8, table_name);
        errdefer self.allocator.free(key);

        // Remove old entry if it exists
        if (self.table_stats.getKey(table_name)) |old_key| {
            _ = self.table_stats.remove(old_key);
            self.allocator.free(old_key);
        }

        try self.table_stats.put(key, stats);
    }

    /// Add statistics for a column
    pub fn addColumnStatistics(self: *Statistics, table_name: []const u8, column_name: []const u8, distinct_values: u64, min_value: PlanValue, max_value: PlanValue, null_count: u64) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ table_name, column_name });
        errdefer self.allocator.free(key);

        const stats = ColumnStatistics{
            .distinct_values = distinct_values,
            .min_value = min_value,
            .max_value = max_value,
            .null_count = null_count,
            .histogram = null,
        };

        // Remove old entry if it exists
        if (self.column_stats.getKey(key)) |old_key| {
            if (self.column_stats.get(old_key).?.histogram) |histogram| {
                self.allocator.free(histogram);
            }
            _ = self.column_stats.remove(old_key);
            self.allocator.free(old_key);
        }

        try self.column_stats.put(key, stats);
    }

    /// Add a histogram for a column
    pub fn addColumnHistogram(self: *Statistics, table_name: []const u8, column_name: []const u8, buckets: []const ColumnStatistics.HistogramBucket) !void {
        const key = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ table_name, column_name });
        defer self.allocator.free(key);

        if (self.column_stats.getPtr(key)) |stats_ptr| {
            // Free old histogram if it exists
            if (stats_ptr.histogram) |old_histogram| {
                self.allocator.free(old_histogram);
            }

            // Create new histogram
            stats_ptr.histogram = try self.allocator.dupe(ColumnStatistics.HistogramBucket, buckets);
        } else {
            return error.ColumnStatsNotFound;
        }
    }

    /// Get the row count for a table
    pub fn getTableRowCount(self: *Statistics, table_name: []const u8) ?u64 {
        if (self.table_stats.get(table_name)) |stats| {
            return stats.row_count;
        }
        return null;
    }

    /// Get the row size for a table
    pub fn getTableRowSize(self: *Statistics, table_name: []const u8) ?u64 {
        if (self.table_stats.get(table_name)) |stats| {
            return stats.row_size;
        }
        return null;
    }

    /// Get statistics for a column
    pub fn getColumnStatistics(self: *Statistics, table_name: []const u8, column_name: []const u8) ?ColumnStatistics {
        const key = std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ table_name, column_name }) catch return null;
        defer self.allocator.free(key);

        return self.column_stats.get(key);
    }

    /// Estimate the selectivity of a predicate
    pub fn estimateSelectivity(self: *Statistics, table_name: []const u8, column_name: []const u8, op: planner.PredicateOp, value: PlanValue, _: ?PlanValue) f64 {
        _ = value; // Value is used in some cases but not all
        const stats = self.getColumnStatistics(table_name, column_name) orelse return 0.5;

        switch (op) {
            .Eq => {
                // Equality predicate
                if (stats.distinct_values > 0) {
                    return 1.0 / @as(f64, @floatFromInt(stats.distinct_values));
                }
                return 0.1; // Default selectivity for equality
            },
            .Ne => {
                // Not equal predicate
                if (stats.distinct_values > 0) {
                    return 1.0 - (1.0 / @as(f64, @floatFromInt(stats.distinct_values)));
                }
                return 0.9; // Default selectivity for not equal
            },
            .Lt, .Le, .Gt, .Ge => {
                // Range predicate
                return 0.3; // Default selectivity for range predicates
            },
            // Between case removed as it's not in the PredicateOp enum
            .In => {
                // In predicate - simplified since we don't have a ValueList field
                // Just return a default selectivity for IN predicates
                return 0.2; // Default selectivity for IN
            },
            .Like => {
                // Like predicate
                return 0.1; // Default selectivity for LIKE
            },
            // IsNull and IsNotNull cases removed as they're not in the PredicateOp enum
        }
    }

    /// Estimate the number of rows in a range
    pub fn estimateRangeSize(self: *Statistics, table_name: []const u8, column_name: []const u8, value1: PlanValue, value2: ?PlanValue) ?u64 {
        const table_stats = self.table_stats.get(table_name) orelse return null;
        // For range queries, we'll use a combination of Gt and Lt
        var selectivity = self.estimateSelectivity(table_name, column_name, .Gt, value1, null);

        // Only use the second value if it's provided
        if (value2) |v2| {
            selectivity *= self.estimateSelectivity(table_name, column_name, .Lt, v2, null);
        }

        return @intFromFloat(selectivity * @as(f64, @floatFromInt(table_stats.row_count)));
    }

    /// Compare two values for greater than or equal
    fn valueGreaterThanOrEqual(self: *Statistics, a: PlanValue, b: PlanValue) bool {
        _ = self;

        switch (a) {
            .Integer => |a_int| {
                switch (b) {
                    .Integer => |b_int| return a_int >= b_int,
                    else => return false,
                }
            },
            .Float => |a_float| {
                switch (b) {
                    .Float => |b_float| return a_float >= b_float,
                    .Integer => |b_int| return a_float >= @as(f64, @floatFromInt(b_int)),
                    else => return false,
                }
            },
            .String => |a_str| {
                switch (b) {
                    .String => |b_str| return std.mem.order(u8, a_str, b_str) != .lt,
                    else => return false,
                }
            },
            else => return false,
        }
    }

    /// Compare two values for less than or equal
    fn valueLessThanOrEqual(self: *Statistics, a: PlanValue, b: PlanValue) bool {
        _ = self;

        switch (a) {
            .Integer => |a_int| {
                switch (b) {
                    .Integer => |b_int| return a_int <= b_int,
                    else => return false,
                }
            },
            .Float => |a_float| {
                switch (b) {
                    .Float => |b_float| return a_float <= b_float,
                    .Integer => |b_int| return a_float <= @as(f64, @floatFromInt(b_int)),
                    else => return false,
                }
            },
            .String => |a_str| {
                switch (b) {
                    .String => |b_str| return std.mem.order(u8, a_str, b_str) != .gt,
                    else => return false,
                }
            },
            else => return false,
        }
    }

    /// Update statistics for a table by analyzing it
    pub fn analyzeTable(self: *Statistics, table_name: []const u8) !void {
        // In a real implementation, this would scan the table and collect statistics
        // For now, we'll just set some default values
        try self.addTableStatistics(table_name, 1000);
    }
};

test "Statistics initialization" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try Statistics.init(allocator);
    defer stats.deinit();

    // Verify initialization
    try std.testing.expectEqual(allocator, stats.allocator);
}

test "Table statistics" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("users", 1000);

    // Verify statistics
    const row_count = stats.getTableRowCount("users");
    try std.testing.expectEqual(@as(u64, 1000), row_count.?);

    const row_size = stats.getTableRowSize("users");
    try std.testing.expectEqual(@as(u64, 100), row_size.?);
}

test "Column statistics" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("users", 1000);

    // Add column statistics
    try stats.addColumnStatistics("users", "id", 1000, PlanValue{ .Integer = 1 }, PlanValue{ .Integer = 1000 }, 0);

    // Verify statistics
    const column_stats = stats.getColumnStatistics("users", "id");
    try std.testing.expect(column_stats != null);
    try std.testing.expectEqual(@as(u64, 1000), column_stats.?.distinct_values);
    try std.testing.expectEqual(@as(i64, 1), column_stats.?.min_value.Integer);
    try std.testing.expectEqual(@as(i64, 1000), column_stats.?.max_value.Integer);
    try std.testing.expectEqual(@as(u64, 0), column_stats.?.null_count);
}

test "Selectivity estimation" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("users", 1000);

    // Add column statistics
    try stats.addColumnStatistics("users", "id", 1000, PlanValue{ .Integer = 1 }, PlanValue{ .Integer = 1000 }, 0);

    // Estimate selectivity for equality
    const eq_selectivity = stats.estimateSelectivity("users", "id", .Eq, PlanValue{ .Integer = 500 }, null);

    // Verify selectivity
    try std.testing.expectEqual(@as(f64, 0.001), eq_selectivity);

    // Estimate selectivity for range
    const range_selectivity = stats.estimateSelectivity("users", "id", .Gt, PlanValue{ .Integer = 100 }, null) *
        stats.estimateSelectivity("users", "id", .Lt, PlanValue{ .Integer = 200 }, null);

    // Verify selectivity
    try std.testing.expectApproxEqAbs(@as(f64, 0.09), range_selectivity, 0.01);
}

test "Range size estimation" {
    const allocator = std.testing.allocator;

    // Initialize Statistics
    const stats = try Statistics.init(allocator);
    defer stats.deinit();

    // Add table statistics
    try stats.addTableStatistics("users", 1000);

    // Add column statistics
    try stats.addColumnStatistics("users", "id", 1000, PlanValue{ .Integer = 1 }, PlanValue{ .Integer = 1000 }, 0);

    // Estimate range size
    const range_size = stats.estimateRangeSize("users", "id", PlanValue{ .Integer = 100 }, PlanValue{ .Integer = 200 });

    // Verify range size
    try std.testing.expectEqual(@as(u64, 250), range_size.?);
}
