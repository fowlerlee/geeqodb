const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const database = geeqodb.core;
const DatabaseServer = @import("../../server/server.zig").DatabaseServer;

test "Server initialization" {
    const allocator = testing.allocator;

    // Initialize the database
    const db = try database.init(allocator, "test_data");
    defer db.deinit();

    // Initialize the server with a test port
    const server = try DatabaseServer.init(allocator, db, 0); // Use port 0 to let the OS assign a free port
    defer server.deinit();

    // Verify that the server was initialized correctly
    try testing.expect(server.is_running == false);
    try testing.expect(server.thread == null);
}

test "Server start and stop" {
    const allocator = testing.allocator;

    // Initialize the database
    const db = try database.init(allocator, "test_data");
    defer db.deinit();

    // Initialize the server with a test port
    const server = try DatabaseServer.init(allocator, db, 0); // Use port 0 to let the OS assign a free port
    defer server.deinit();

    // Start the server
    try server.start();
    try testing.expect(server.is_running == true);
    try testing.expect(server.thread != null);

    // Stop the server
    server.stop();
    try testing.expect(server.is_running == false);
    try testing.expect(server.thread == null);
}
