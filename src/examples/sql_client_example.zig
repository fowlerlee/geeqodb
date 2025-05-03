const std = @import("std");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // Connect to the database server
    const address = try std.net.Address.parseIp("127.0.0.1", 5252);
    var stream = try std.net.tcpConnectToAddress(address);
    defer stream.close();

    std.debug.print("Connected to database server at {}\n", .{address});

    // Execute a SQL query
    const query = "SELECT * FROM users";
    std.debug.print("Executing query: {s}\n", .{query});

    // Send query to server
    _ = try stream.write(query);

    // Read response from server
    var buffer: [4096]u8 = undefined;
    const bytes_read = try stream.read(&buffer);
    const response = buffer[0..bytes_read];

    std.debug.print("Response from server:\n{s}\n", .{response});
}
