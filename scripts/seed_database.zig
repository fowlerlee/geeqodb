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
        std.debug.print("Make sure the server is running at {s}:{d}\n", .{host, port});
        return err;
    };
    defer stream.close();

    std.debug.print("Connected to database server at {}\n", .{address});
    std.debug.print("Seeding database with sample data...\n", .{});

    // Define seed queries
    const seed_queries = [_][]const u8{
        // Create tables
        "DROP TABLE IF EXISTS users",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at TEXT)",
        
        "DROP TABLE IF EXISTS products",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, description TEXT, price REAL, stock INTEGER)",
        
        "DROP TABLE IF EXISTS orders",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total_amount REAL, status TEXT, created_at TEXT, FOREIGN KEY(user_id) REFERENCES users(id))",
        
        "DROP TABLE IF EXISTS order_items",
        "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, price REAL, FOREIGN KEY(order_id) REFERENCES orders(id), FOREIGN KEY(product_id) REFERENCES products(id))",
        
        // Insert sample users
        "INSERT INTO users (id, name, email, created_at) VALUES (1, 'John Doe', 'john@example.com', '2023-01-01')",
        "INSERT INTO users (id, name, email, created_at) VALUES (2, 'Jane Smith', 'jane@example.com', '2023-01-02')",
        "INSERT INTO users (id, name, email, created_at) VALUES (3, 'Bob Johnson', 'bob@example.com', '2023-01-03')",
        "INSERT INTO users (id, name, email, created_at) VALUES (4, 'Alice Brown', 'alice@example.com', '2023-01-04')",
        "INSERT INTO users (id, name, email, created_at) VALUES (5, 'Charlie Wilson', 'charlie@example.com', '2023-01-05')",
        
        // Insert sample products
        "INSERT INTO products (id, name, description, price, stock) VALUES (1, 'Laptop', 'High-performance laptop', 999.99, 50)",
        "INSERT INTO products (id, name, description, price, stock) VALUES (2, 'Smartphone', 'Latest smartphone model', 699.99, 100)",
        "INSERT INTO products (id, name, description, price, stock) VALUES (3, 'Headphones', 'Noise-cancelling headphones', 199.99, 200)",
        "INSERT INTO products (id, name, description, price, stock) VALUES (4, 'Tablet', '10-inch tablet', 399.99, 75)",
        "INSERT INTO products (id, name, description, price, stock) VALUES (5, 'Smartwatch', 'Fitness tracking smartwatch', 249.99, 150)",
        
        // Insert sample orders
        "INSERT INTO orders (id, user_id, total_amount, status, created_at) VALUES (1, 1, 1199.98, 'Completed', '2023-02-01')",
        "INSERT INTO orders (id, user_id, total_amount, status, created_at) VALUES (2, 2, 699.99, 'Completed', '2023-02-02')",
        "INSERT INTO orders (id, user_id, total_amount, status, created_at) VALUES (3, 3, 599.98, 'Processing', '2023-02-03')",
        "INSERT INTO orders (id, user_id, total_amount, status, created_at) VALUES (4, 4, 999.99, 'Shipped', '2023-02-04')",
        "INSERT INTO orders (id, user_id, total_amount, status, created_at) VALUES (5, 5, 449.98, 'Processing', '2023-02-05')",
        
        // Insert sample order items
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (1, 1, 1, 1, 999.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (2, 1, 3, 1, 199.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (3, 2, 2, 1, 699.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (4, 3, 3, 1, 199.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (5, 3, 5, 1, 399.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (6, 4, 1, 1, 999.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (7, 5, 4, 1, 399.99)",
        "INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES (8, 5, 5, 1, 49.99)",
    };

    // Execute seed queries
    for (seed_queries) |query| {
        std.debug.print("Executing: {s}\n", .{query});
        
        // Send query to server
        _ = try stream.write(query);
        
        // Read response from server
        var response_buffer: [4096]u8 = undefined;
        const bytes_read = try stream.read(&response_buffer);
        
        if (bytes_read == 0) {
            std.debug.print("Server closed the connection.\n", .{});
            return error.ConnectionClosed;
        }
        
        const response = response_buffer[0..bytes_read];
        std.debug.print("Response: {s}\n", .{response});
    }

    std.debug.print("\nDatabase seeded successfully!\n", .{});
}

fn printUsage() void {
    std.debug.print(
        \\Usage: seed_database [options]
        \\
        \\Options:
        \\  --host, -h <host>  Server hostname or IP address (default: 127.0.0.1)
        \\  --port, -p <port>  Server port (default: 5252)
        \\  --help             Show this help message
        \\
        \\Example:
        \\  seed_database --host 127.0.0.1 --port 5252
        \\
    , .{});
}
