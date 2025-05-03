const std = @import("std");
const testing = std.testing;
const geeqodb = @import("geeqodb");
const database = geeqodb.core;
const DatabaseServer = @import("../../server/server.zig").DatabaseServer;
const ResultSet = @import("../../query/result.zig").ResultSet;

/// Test client for connecting to the database server
const TestClient = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    address: std.net.Address,

    /// Initialize a new test client
    pub fn init(allocator: std.mem.Allocator, address: std.net.Address) !*TestClient {
        // Connect to the server
        var stream = try std.net.tcpConnectToAddress(address);
        errdefer stream.close();

        // Create client instance
        const client = try allocator.create(TestClient);
        client.* = TestClient{
            .allocator = allocator,
            .stream = stream,
            .address = address,
        };

        return client;
    }

    /// Send a query to the server
    pub fn sendQuery(self: *TestClient, query: []const u8) !void {
        _ = try self.stream.write(query);
    }

    /// Read response from the server
    pub fn readResponse(self: *TestClient, buffer: []u8) ![]u8 {
        const bytes_read = try self.stream.read(buffer);
        if (bytes_read == 0) {
            return error.ConnectionClosed;
        }
        return buffer[0..bytes_read];
    }

    /// Close the connection and free resources
    pub fn deinit(self: *TestClient) void {
        self.stream.close();
        self.allocator.destroy(self);
    }
};

/// Helper function to create a test database and server
fn createTestServer(allocator: std.mem.Allocator) !struct { db: *database.OLAPDatabase, server: *DatabaseServer } {
    // Initialize the database
    const db = try database.init(allocator, "test_data");
    errdefer db.deinit();

    // Initialize the server with a test port
    const server = try DatabaseServer.init(allocator, db, 0); // Use port 0 to let the OS assign a free port
    errdefer server.deinit();

    return .{ .db = db, .server = server };
}

/// Helper function to create a test client connected to the server
fn createTestClient(allocator: std.mem.Allocator, server: *DatabaseServer) !*TestClient {
    // Wait a bit for the server to start listening
    std.time.sleep(100 * std.time.ns_per_ms);

    // Get the actual port from the server
    const port = server.address.getPort();

    // Create a loopback address with the correct port
    const address = try std.net.Address.parseIp("127.0.0.1", port);

    return try TestClient.init(allocator, address);
}

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

test "Server connection handling" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Verify that the client connected successfully
    try testing.expect(@intFromPtr(&client.stream) != 0);
}

test "Server query execution" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Send a simple query
    try client.sendQuery("SELECT * FROM test");

    // Read the response
    var response_buffer: [4096]u8 = undefined;
    const response = try client.readResponse(&response_buffer);

    // Verify that we got a response
    try testing.expect(response.len > 0);

    // The response might contain an error about the table not existing,
    // but we just want to verify that the server processed the query
    std.debug.print("Response: {s}\n", .{response});
}

test "Server error handling - empty query" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Send an empty query
    try client.sendQuery("");

    // Read the response
    var response_buffer: [4096]u8 = undefined;
    const response = try client.readResponse(&response_buffer);

    // Verify that we got a response
    try testing.expect(response.len > 0);

    // Print the response for debugging
    std.debug.print("Empty query response: {s}\n", .{response});
}

test "Server error handling - malformed query" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Send a malformed query
    try client.sendQuery("SELECT * FROMM test");

    // Read the response
    var response_buffer: [4096]u8 = undefined;
    const response = try client.readResponse(&response_buffer);

    // Verify that we got a response
    try testing.expect(response.len > 0);

    // Print the response for debugging
    std.debug.print("Malformed query response: {s}\n", .{response});
}

test "Server multiple queries" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Send multiple queries
    const queries = [_][]const u8{
        "SELECT * FROM test1",
        "SELECT * FROM test2",
        "SELECT * FROM test3",
    };

    for (queries) |query| {
        // Send query
        try client.sendQuery(query);

        // Read response
        var response_buffer: [4096]u8 = undefined;
        const response = try client.readResponse(&response_buffer);

        // Verify that we got a response
        try testing.expect(response.len > 0);

        // Print the response for debugging
        std.debug.print("Query '{s}' response: {s}\n", .{ query, response });
    }
}

test "Server concurrent connections" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create multiple test clients
    const num_clients = 5;
    var clients: [num_clients]*TestClient = undefined;

    for (0..num_clients) |i| {
        clients[i] = try createTestClient(allocator, server);
    }
    defer {
        for (0..num_clients) |i| {
            clients[i].deinit();
        }
    }

    // Send a query from each client
    for (0..num_clients) |i| {
        const query = std.fmt.allocPrint(allocator, "SELECT * FROM test{d}", .{i}) catch unreachable;
        defer allocator.free(query);

        try clients[i].sendQuery(query);
    }

    // Read responses from each client
    for (0..num_clients) |i| {
        var response_buffer: [4096]u8 = undefined;
        const response = try clients[i].readResponse(&response_buffer);

        // Verify that we got a response
        try testing.expect(response.len > 0);
        try testing.expect(std.mem.indexOf(u8, response, "SUCCESS") != null);
    }
}

test "Server connection timeout handling" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);

    // Close the client without sending any queries
    client.deinit();

    // Wait a bit to ensure the server has time to handle the disconnection
    std.time.sleep(100 * std.time.ns_per_ms);

    // The server should still be running
    try testing.expect(server.is_running == true);
}

test "Server protocol parsing - valid protocol" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Send a query with the expected protocol format
    try client.sendQuery("SELECT * FROM test");

    // Read the response
    var response_buffer: [4096]u8 = undefined;
    const response = try client.readResponse(&response_buffer);

    // Verify that we got a successful response
    try testing.expect(response.len > 0);
    try testing.expect(std.mem.indexOf(u8, response, "SUCCESS") != null);
}

test "Server protocol parsing - invalid protocol" {
    const allocator = testing.allocator;

    // Create test server
    const test_setup = try createTestServer(allocator);
    const db = test_setup.db;
    const server = test_setup.server;
    defer {
        server.deinit();
        db.deinit();
    }

    // Start the server
    try server.start();
    defer server.stop();

    // Create a test client
    const client = try createTestClient(allocator, server);
    defer client.deinit();

    // Send binary data instead of a SQL query
    const invalid_data = [_]u8{ 0x00, 0x01, 0x02, 0x03, 0x04 };
    try client.sendQuery(&invalid_data);

    // Read the response
    var response_buffer: [4096]u8 = undefined;
    const response = try client.readResponse(&response_buffer);

    // Verify that we got an error response
    try testing.expect(response.len > 0);
    try testing.expect(std.mem.indexOf(u8, response, "ERROR") != null);
}
