const std = @import("std");
const OLAPDatabase = @import("../core/database.zig").OLAPDatabase;
const ResultSet = @import("../query/result.zig").ResultSet;
const assert = @import("../build_options.zig").assert;

/// Database server that listens for SQL queries over TCP
pub const DatabaseServer = struct {
    allocator: std.mem.Allocator,
    db: *OLAPDatabase,
    server: std.net.Server,
    address: std.net.Address,
    is_running: bool,
    thread: ?std.Thread,

    /// Initialize a new database server
    pub fn init(allocator: std.mem.Allocator, db: *OLAPDatabase, port: u16) !*DatabaseServer {
        // Create server address
        const address = try std.net.Address.parseIp("0.0.0.0", port);

        // Create socket
        const sock_flags = std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC;
        const sockfd = try std.posix.socket(address.any.family, sock_flags, 0);
        errdefer std.posix.close(sockfd);

        // Set socket options
        try std.posix.setsockopt(sockfd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        // Bind to address
        try std.posix.bind(sockfd, &address.any, address.getOsSockLen());

        // Get the actual bound address (important when port is 0)
        var actual_address = address;
        var addr_len = actual_address.getOsSockLen();
        try std.posix.getsockname(sockfd, &actual_address.any, &addr_len);

        // Listen for connections
        try std.posix.listen(sockfd, 128);

        // Create server
        const server = std.net.Server{
            .listen_address = actual_address,
            .stream = .{ .handle = sockfd },
        };

        // Create server instance
        const server_instance = try allocator.create(DatabaseServer);
        server_instance.* = DatabaseServer{
            .allocator = allocator,
            .db = db,
            .server = server,
            .address = actual_address,
            .is_running = false,
            .thread = null,
        };

        return server_instance;
    }

    /// Start the server in a separate thread
    pub fn start(self: *DatabaseServer) !void {
        if (self.is_running) {
            return error.ServerAlreadyRunning;
        }

        self.is_running = true;
        self.thread = try std.Thread.spawn(.{}, runServer, .{self});
    }

    /// Run the server (called in a separate thread)
    fn runServer(self: *DatabaseServer) !void {
        std.debug.print("Database server listening on {}\n", .{self.address});

        while (self.is_running) {
            // Accept connection
            const connection = self.server.accept() catch |err| {
                if (err == error.ConnectionAborted and !self.is_running) {
                    // Server is shutting down
                    break;
                }
                std.debug.print("Error accepting connection: {}\n", .{err});
                continue;
            };

            // Handle connection in a separate thread
            _ = std.Thread.spawn(.{}, handleConnection, .{ self, connection }) catch |err| {
                std.debug.print("Error spawning connection handler thread: {}\n", .{err});
                connection.stream.close();
                continue;
            };
        }
    }

    /// Handle a client connection
    fn handleConnection(self: *DatabaseServer, connection: std.net.Server.Connection) !void {
        defer connection.stream.close();

        var buffer: [4096]u8 = undefined;

        while (true) {
            // Read query from client
            const bytes_read = connection.stream.read(&buffer) catch |err| {
                std.debug.print("Error reading from client: {}\n", .{err});
                return;
            };

            if (bytes_read == 0) {
                // Client disconnected
                return;
            }

            // Extract SQL query
            const query = buffer[0..bytes_read];

            // Execute query
            var result_set = self.db.execute(query) catch |err| {
                // Send error to client
                const error_message = std.fmt.allocPrint(self.allocator, "ERROR: {s}\n", .{@errorName(err)}) catch "ERROR: Failed to format error message\n";
                defer if (@TypeOf(error_message) != *const [0:0]u8) self.allocator.free(error_message);

                _ = connection.stream.write(error_message) catch |write_err| {
                    std.debug.print("Error writing to client: {}\n", .{write_err});
                    return;
                };
                continue;
            };
            defer result_set.deinit();

            // Format result set as string
            const result_string = formatResultSet(self.allocator, result_set) catch |err| {
                std.debug.print("Error formatting result set: {}\n", .{err});
                _ = connection.stream.write("ERROR: Failed to format result set\n") catch |write_err| {
                    std.debug.print("Error writing to client: {}\n", .{write_err});
                    return;
                };
                continue;
            };
            defer self.allocator.free(result_string);

            // Send result to client
            _ = connection.stream.write(result_string) catch |err| {
                std.debug.print("Error writing to client: {}\n", .{err});
                return;
            };
        }
    }

    /// Format a result set as a string
    fn formatResultSet(allocator: std.mem.Allocator, result_set: ResultSet) ![]const u8 {
        // For now, just return a simple success message
        // In a real implementation, this would format the result set as a table
        return try std.fmt.allocPrint(allocator, "SUCCESS: Query executed successfully. Rows: {d}\n", .{result_set.row_count});
    }

    /// Stop the server
    pub fn stop(self: *DatabaseServer) void {
        if (!self.is_running) {
            return;
        }

        self.is_running = false;
        self.server.stream.close();

        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    /// Deinitialize the server
    pub fn deinit(self: *DatabaseServer) void {
        self.stop();
        self.allocator.destroy(self);
    }
};
