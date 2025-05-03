#!/bin/bash

# Set the port for the database server
PORT=5252

# Function to check if a process is running on the specified port
function is_port_in_use() {
  lsof -i :$1 &> /dev/null
  return $?
}

# Function to clean up on exit
function cleanup() {
  echo "Cleaning up..."
  if [ ! -z "$SERVER_PID" ]; then
    echo "Stopping database server (PID: $SERVER_PID)..."
    kill $SERVER_PID
    wait $SERVER_PID 2>/dev/null
  fi
  exit
}

# Set up trap to clean up on exit
trap cleanup EXIT INT TERM

# Build the database server, seed script, and test script
echo "Building database server and test scripts..."
zig build
zig build tools

# Check if the port is already in use
if is_port_in_use $PORT; then
  echo "Error: Port $PORT is already in use. Please stop any running database servers or use a different port."
  exit 1
fi

# Start the database server in the background
echo "Starting database server on port $PORT..."
./zig-out/bin/geeqodb &
SERVER_PID=$!

# Wait for the server to start
echo "Waiting for server to start..."
sleep 2

# Check if the server is running
if ! is_port_in_use $PORT; then
  echo "Error: Failed to start database server on port $PORT."
  exit 1
fi

echo "Database server started successfully (PID: $SERVER_PID)."

# Seed the database
echo "Seeding the database..."
./zig-out/bin/seed_database --port $PORT

# Run the tests
echo "Running database tests..."
./zig-out/bin/test_database --port $PORT

# Print success message
echo "Full test completed successfully!"
