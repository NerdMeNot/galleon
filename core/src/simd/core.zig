const std = @import("std");

/// SIMD vector width - 8 elements for AVX2-class performance
pub const VECTOR_WIDTH = 8;

/// Number of vectors to process per loop iteration (loop unrolling factor)
pub const UNROLL_FACTOR = 4;

/// Total elements processed per unrolled iteration
pub const CHUNK_SIZE = VECTOR_WIDTH * UNROLL_FACTOR; // 32 elements

// ============================================================================
// Thread Configuration
// ============================================================================

/// Maximum threads supported (compile-time constant for array sizing)
pub const MAX_THREADS: usize = 32;

/// Runtime configured max threads (0 = auto-detect)
var configured_max_threads: usize = 0;

/// Get the effective max threads to use
/// If configured_max_threads is 0, auto-detect from CPU count
/// Otherwise use the configured value (capped at MAX_THREADS)
pub fn getMaxThreads() usize {
    if (configured_max_threads > 0) {
        return @min(configured_max_threads, MAX_THREADS);
    }
    // Auto-detect: use CPU count
    const cpu_count = std.Thread.getCpuCount() catch 8;
    return @min(cpu_count, MAX_THREADS);
}

/// Set the maximum number of threads to use
/// Pass 0 to use auto-detection (default)
pub fn setMaxThreads(max_threads: usize) void {
    configured_max_threads = max_threads;
}

/// Get current thread configuration
pub fn getThreadConfig() struct { max_threads: usize, auto_detected: bool } {
    const auto = configured_max_threads == 0;
    return .{
        .max_threads = getMaxThreads(),
        .auto_detected = auto,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "core - thread config defaults" {
    const config = getThreadConfig();
    try std.testing.expect(config.max_threads > 0);
    try std.testing.expect(config.max_threads <= MAX_THREADS);
}

test "core - setMaxThreads" {
    const original = configured_max_threads;
    defer configured_max_threads = original;

    setMaxThreads(4);
    try std.testing.expectEqual(@as(usize, 4), getMaxThreads());

    setMaxThreads(0);
    try std.testing.expect(getThreadConfig().auto_detected);
}
