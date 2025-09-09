const std = @import("std");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command-line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Default connection parameters
    var host: []const u8 = "127.0.0.1";
    var port: u16 = 5252;

    // Parse command-line arguments
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--host") or std.mem.eql(u8, arg, "-h")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: Missing value for --host\n", .{});
                return error.InvalidArguments;
            }
            host = args[i];
        } else if (std.mem.eql(u8, arg, "--port") or std.mem.eql(u8, arg, "-p")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: Missing value for --port\n", .{});
                return error.InvalidArguments;
            }
            port = try std.fmt.parseInt(u16, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return;
        }
    }

    // Connect to the database server
    const address = try std.net.Address.parseIp(host, port);
    var stream = std.net.tcpConnectToAddress(address) catch |err| {
        std.debug.print("Error connecting to server: {}\n", .{err});
        std.debug.print("Make sure the server is running at {s}:{d}\n", .{ host, port });
        return err;
    };
    defer stream.close();

    std.debug.print("Connected to database server at {}\n", .{address});
    std.debug.print("Type SQL queries and press Enter to execute them.\n", .{});
    std.debug.print("Type 'exit' or 'quit' to exit.\n\n", .{});

    // Create stdin reader using new Zig 0.15 streaming format
    var stdin_stream = std.fs.File.stdin().readerStreaming(&.{});

    // Create buffer for user input
    var input_buffer: [4096]u8 = undefined;

    // Interactive loop
    while (true) {
        // Display prompt
        std.debug.print("sql> ", .{});

        // Read user input using new streaming interface
        const input = stdin_stream.interface.readUntilDelimiterOrEof(&input_buffer, '\n') catch |err| {
            std.debug.print("Error reading input: {}\n", .{err});
            continue;
        };
        if (input.eof()) {
            std.debug.print("End of input, exiting.\n", .{});
            break;
        }

        // Trim whitespace
        const query = std.mem.trim(u8, input.slice(), &std.ascii.whitespace);
        defer input.deinit();
        // Check for exit command
        if (std.mem.eql(u8, query, "exit") or std.mem.eql(u8, query, "quit")) {
            std.debug.print("Exiting.\n", .{});
            break;
        }

        // Skip empty queries
        if (query.len == 0) {
            continue;
        }

        // Send query to server
        _ = stream.write(query) catch |err| {
            std.debug.print("Error sending query: {}\n", .{err});
            continue;
        };

        // Read response from server
        var response_buffer: [4096]u8 = undefined;
        const bytes_read = stream.read(&response_buffer) catch |err| {
            std.debug.print("Error reading response: {}\n", .{err});
            continue;
        };

        if (bytes_read == 0) {
            std.debug.print("Server closed the connection.\n", .{});
            break;
        }

        const response = response_buffer[0..bytes_read];
        std.debug.print("{s}\n", .{response});
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: sql_client [options]
        \\
        \\Options:
        \\  --host, -h <host>  Server hostname or IP address (default: 127.0.0.1)
        \\  --port, -p <port>  Server port (default: 5252)
        \\  --help             Show this help message
        \\
        \\Example:
        \\  sql_client --host 127.0.0.1 --port 5252
        \\
    , .{});
}
