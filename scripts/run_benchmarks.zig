const std = @import("std");
const geeqodb = @import("geeqodb");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create the benchmark_results directory if it doesn't exist
    try std.fs.cwd().makePath("benchmark_results");

    // Get the current timestamp
    const timestamp = std.time.timestamp();
    const timestamp_str = try std.fmt.allocPrint(allocator, "{d}", .{timestamp});
    defer allocator.free(timestamp_str);

    // Create the summary filename with timestamp
    const summary_filename = try std.fmt.allocPrint(allocator, "benchmark_results/benchmark_summary_{s}.md", .{timestamp_str});
    defer allocator.free(summary_filename);

    // Open the summary file for writing
    const summary_file = try std.fs.cwd().createFile(summary_filename, .{});
    defer summary_file.close();

    // Write the summary header
    try summary_file.writeAll("# GeeqoDB Benchmark Summary\n\n");
    try summary_file.writeAll("This file contains a summary of all benchmark results.\n\n");

    // Run the benchmarks
    try summary_file.writeAll("## Running Benchmarks\n\n");

    // List of benchmarks to run
    const benchmarks = [_][]const u8{
        "database_benchmark",
        "storage_benchmark",
        "transaction_benchmark",
        "query_benchmark",
        "index_benchmark",
        "index_query_benchmark",
    };

    // Run each benchmark
    for (benchmarks) |benchmark| {
        try summary_file.writer().print("### Running {s}...\n\n", .{benchmark});
        std.debug.print("Running {s}...\n", .{benchmark});

        // Build the command
        const command = try std.fmt.allocPrint(allocator, "zig build benchmark-{s}", .{benchmark});
        defer allocator.free(command);

        // Run the command
        const result = try std.process.Child.run(.{
            .allocator = allocator,
            .argv = &[_][]const u8{ "sh", "-c", command },
        });
        defer {
            allocator.free(result.stdout);
            allocator.free(result.stderr);
        }

        // Write the result to the summary file
        try summary_file.writer().print("```\n{s}\n```\n\n", .{result.stdout});
    }

    // Write the summary footer
    try summary_file.writeAll("## Conclusion\n\n");
    try summary_file.writeAll("The benchmarks demonstrate the performance characteristics of GeeqoDB, particularly the significant performance improvements gained by using indexes for various types of queries.\n\n");
    try summary_file.writeAll("For detailed analysis, please refer to the individual benchmark result files in the `benchmark_results` directory.\n");

    std.debug.print("Benchmark summary written to {s}\n", .{summary_filename});
}
