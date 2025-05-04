# GeeqoDB

A high-performance OLAP database in Zig with SQL support.

<!-- <div style="display: flex; justify-content: center;"> -->
<div style="text-align: center;">
<img src="./artwork/geeqodb.jpg"  alt="Alternative text" width="400" height="500">
</div>

## Features

- TCP server listening on port 5252
- SQL query processing
- Support for basic SQL operations (SELECT, INSERT, UPDATE, DELETE)
- Support for JOINs, GROUP BY, ORDER BY, and other SQL features
- Client tools for interacting with the database

## Building

```bash
# Build the database server and tools
zig build
zig build tools
```

## Running the Server

```bash
# Run the database server
zig build run
```

The server will start and listen on port 5252.

## Using the SQL Client

```bash
# Run the SQL client
./zig-out/bin/sql_client
```

This will connect to the database server and allow you to execute SQL queries interactively.

## Testing

```bash
# Run all tests
zig build test

# Run the full test suite (starts server, seeds database, runs tests)
./scripts/run_full_test.sh

# Run simulation tests
zig build test-simulation

# Run a specific simulation scenario
./zig-out/bin/run_simulation_tests --scenario vr_basic
```

## Scripts

- `scripts/seed_database.zig`: Seeds the database with sample data
- `scripts/test_database.zig`: Tests the database functionality
- `scripts/run_full_test.sh`: Runs a full test of the database server
- `scripts/run_simulation_tests.zig`: Runs deterministic simulation tests

## Example SQL Queries

```sql
-- Create a table
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);

-- Insert data
INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');

-- Query data
SELECT * FROM users;

-- Update data
UPDATE users SET name = 'Jane Doe' WHERE id = 1;

-- Delete data
DELETE FROM users WHERE id = 1;
```

## Project Structure

- `src/core/`: Core database functionality
- `src/storage/`: Storage engine
- `src/query/`: Query processing
- `src/transaction/`: Transaction management
- `src/server/`: Network server
- `src/tools/`: Client tools
- `scripts/`: Testing and utility scripts

## License

This project is licensed under the MIT License - see the LICENSE file for details.
