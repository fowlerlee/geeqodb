// This file is included by the build system to configure build options
// It will be populated with options from build.zig

// Default to enabling assertions in debug builds
pub const enable_assertions = false;

// Helper function to conditionally use assertions
pub inline fn assert(ok: bool) void {
    if (enable_assertions) {
        if (!ok) unreachable; // assertion failure
    }
}
