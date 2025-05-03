const std = @import("std");

/// Writes benchmark results to a file in the benchmark_results directory
pub fn writeBenchmarkResults(allocator: std.mem.Allocator, benchmark_name: []const u8, results: []const u8) !void {
    // Create the benchmark_results directory if it doesn't exist
    try std.fs.cwd().makePath("benchmark_results");

    // Get the current timestamp
    const timestamp = std.time.timestamp();
    const timestamp_str = try std.fmt.allocPrint(allocator, "{d}", .{timestamp});
    defer allocator.free(timestamp_str);

    // Create the filename with timestamp
    const filename = try std.fmt.allocPrint(allocator, "benchmark_results/{s}_{s}.txt", .{ benchmark_name, timestamp_str });
    defer allocator.free(filename);

    // Open the file for writing
    const file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    // Write the results to the file
    try file.writeAll(results);

    std.debug.print("Benchmark results written to {s}\n", .{filename});
}

/// Formats benchmark results as a markdown table
pub fn formatBenchmarkResultsMarkdown(allocator: std.mem.Allocator, title: []const u8, headers: []const []const u8, rows: []const []const []const u8) ![]const u8 {
    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();

    // Write the title
    try result.writer().print("# {s}\n\n", .{title});

    // Write the table header
    try result.writer().print("|", .{});
    for (headers) |header| {
        try result.writer().print(" {s} |", .{header});
    }
    try result.writer().print("\n", .{});

    // Write the separator
    try result.writer().print("|", .{});
    for (headers) |_| {
        try result.writer().print(" --- |", .{});
    }
    try result.writer().print("\n", .{});

    // Write the rows
    for (rows) |row| {
        try result.writer().print("|", .{});
        for (row) |cell| {
            try result.writer().print(" {s} |", .{cell});
        }
        try result.writer().print("\n", .{});
    }

    return result.toOwnedSlice();
}

/// Formats benchmark results as CSV
pub fn formatBenchmarkResultsCSV(allocator: std.mem.Allocator, headers: []const []const u8, rows: []const []const []const u8) ![]const u8 {
    var result = std.ArrayList(u8).init(allocator);
    defer result.deinit();

    // Write the header
    for (headers, 0..) |header, i| {
        if (i > 0) {
            try result.writer().print(",", .{});
        }
        try result.writer().print("{s}", .{header});
    }
    try result.writer().print("\n", .{});

    // Write the rows
    for (rows) |row| {
        for (row, 0..) |cell, i| {
            if (i > 0) {
                try result.writer().print(",", .{});
            }
            try result.writer().print("{s}", .{cell});
        }
        try result.writer().print("\n", .{});
    }

    return result.toOwnedSlice();
}
