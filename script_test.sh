# Navigate to geeqodb directory
cd /Users/leefowler/Documents/go/orchestration/geeqodb

# Build the project
zig build

# Run all tests
zig build test

# Run specific test categories
zig build test-database
zig build test-gpu
zig build test-replication

# Run a single test file
zig test src/core/database_test.zig

# Build test executable for debugging
zig test --test-no-exec -femit-bin=zig-out/bin/database_test src/core/database_test.zig

# Debug with LLDB
lldb zig-out/bin/database_test

# Performance testing
zig test --test-no-exec -femit-bin=zig-out/bin/database_test src/core/database_test.zig