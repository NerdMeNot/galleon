//! CPU Feature Detection for Runtime SIMD Dispatch
//!
//! This module provides runtime detection of CPU SIMD capabilities.
//! It supports x86/x86_64 (SSE4, AVX2, AVX-512) and ARM (NEON).
//!
//! The detection is done once at initialization and cached for subsequent calls.
//! Users can also override the detected level for testing or compatibility.

const std = @import("std");
const builtin = @import("builtin");

/// SIMD instruction set levels supported by Galleon
pub const SimdLevel = enum(u8) {
    /// Scalar fallback (no SIMD or unknown architecture)
    scalar = 0,
    /// SSE4.1/4.2 (128-bit vectors) - x86 baseline or ARM NEON equivalent
    sse4 = 1,
    /// AVX2 + FMA (256-bit vectors)
    avx2 = 2,
    /// AVX-512F + VL + BW (512-bit vectors)
    avx512 = 3,

    /// Get the vector width in bytes for this SIMD level
    pub fn vectorBytes(self: SimdLevel) usize {
        return switch (self) {
            .scalar => 8, // Process one element at a time
            .sse4 => 16, // 128-bit
            .avx2 => 32, // 256-bit
            .avx512 => 64, // 512-bit
        };
    }

    /// Get the number of f64 elements per vector
    pub fn f64Width(self: SimdLevel) usize {
        return self.vectorBytes() / 8;
    }

    /// Get the number of f32 elements per vector
    pub fn f32Width(self: SimdLevel) usize {
        return self.vectorBytes() / 4;
    }

    /// Get the number of i64 elements per vector
    pub fn i64Width(self: SimdLevel) usize {
        return self.vectorBytes() / 8;
    }

    /// Get the number of i32 elements per vector
    pub fn i32Width(self: SimdLevel) usize {
        return self.vectorBytes() / 4;
    }

    /// Human-readable name for logging
    pub fn name(self: SimdLevel) []const u8 {
        return switch (self) {
            .scalar => "Scalar",
            .sse4 => "SSE4",
            .avx2 => "AVX2",
            .avx512 => "AVX-512",
        };
    }
};

/// Detected SIMD level - set once at initialization
var detected_level: ?SimdLevel = null;

/// Lock for thread-safe initialization
var init_lock: std.Thread.Mutex = .{};

/// Get the CPU's SIMD capability (cached after first call)
/// Thread-safe: multiple threads can call this safely
pub fn getSimdLevel() SimdLevel {
    // Fast path: already detected (use mutex for thread safety)
    init_lock.lock();
    defer init_lock.unlock();

    if (detected_level) |level| {
        return level;
    }

    // Detect and cache
    const level = detectSimdLevel();
    detected_level = level;
    return level;
}

/// Allow override for testing or user preference
/// This should be called before any SIMD operations
pub fn setSimdLevel(level: SimdLevel) void {
    init_lock.lock();
    defer init_lock.unlock();
    detected_level = level;
}

/// Reset detection (mainly for testing)
pub fn resetDetection() void {
    init_lock.lock();
    defer init_lock.unlock();
    detected_level = null;
}

/// Detect SIMD capabilities based on architecture
fn detectSimdLevel() SimdLevel {
    return switch (builtin.cpu.arch) {
        .x86_64, .x86 => detectX86SimdLevel(),
        .aarch64 => .sse4, // AArch64 always has NEON, treat as SSE4 equivalent
        .arm => detectArmSimdLevel(),
        else => .scalar,
    };
}

// ============================================================================
// x86/x86_64 Detection
// ============================================================================

/// Detect x86 SIMD level
fn detectX86SimdLevel() SimdLevel {
    // Use compile-time CPU features as a proxy for runtime detection
    // This works because we build with -Dtarget=native or specific target
    const features = builtin.cpu.features;

    // Check for AVX-512 (need F, VL, BW)
    if (std.Target.x86.featureSetHas(features, .avx512f) and
        std.Target.x86.featureSetHas(features, .avx512vl) and
        std.Target.x86.featureSetHas(features, .avx512bw))
    {
        return .avx512;
    }

    // Check for AVX2 + FMA
    if (std.Target.x86.featureSetHas(features, .avx2) and
        std.Target.x86.featureSetHas(features, .fma))
    {
        return .avx2;
    }

    // Check for SSE4.1
    if (std.Target.x86.featureSetHas(features, .sse4_1)) {
        return .sse4;
    }

    return .scalar;
}

// ============================================================================
// ARM Detection
// ============================================================================

/// Detect ARM SIMD level
fn detectArmSimdLevel() SimdLevel {
    // ARMv7 with NEON
    if (comptime std.Target.arm.featureSetHas(builtin.cpu.features, .neon)) {
        return .sse4; // NEON is roughly equivalent to SSE4 in capability
    }
    return .scalar;
}

// ============================================================================
// Logging and Diagnostics
// ============================================================================

/// Log detected SIMD level (for debugging)
pub fn logSimdInfo(writer: anytype) !void {
    const level = getSimdLevel();
    try writer.print("Galleon SIMD: {s} ({d}-bit vectors)\n", .{
        level.name(),
        level.vectorBytes() * 8,
    });
    try writer.print("  f64 width: {d} elements\n", .{level.f64Width()});
    try writer.print("  f32 width: {d} elements\n", .{level.f32Width()});
}

// ============================================================================
// Tests
// ============================================================================

test "cpuid - detection returns valid level" {
    resetDetection();
    const level = getSimdLevel();

    // Should return a valid level
    try std.testing.expect(@intFromEnum(level) <= @intFromEnum(SimdLevel.avx512));

    // Should be at least scalar
    try std.testing.expect(@intFromEnum(level) >= @intFromEnum(SimdLevel.scalar));
}

test "cpuid - detection is cached" {
    resetDetection();

    const level1 = getSimdLevel();
    const level2 = getSimdLevel();

    try std.testing.expectEqual(level1, level2);
}

test "cpuid - setSimdLevel overrides detection" {
    resetDetection();

    // Get the real level first
    const real_level = getSimdLevel();

    // Override to scalar
    setSimdLevel(.scalar);
    try std.testing.expectEqual(SimdLevel.scalar, getSimdLevel());

    // Override to AVX2
    setSimdLevel(.avx2);
    try std.testing.expectEqual(SimdLevel.avx2, getSimdLevel());

    // Reset and verify we get the real level back
    resetDetection();
    try std.testing.expectEqual(real_level, getSimdLevel());
}

test "cpuid - SimdLevel widths" {
    try std.testing.expectEqual(@as(usize, 2), SimdLevel.sse4.f64Width());
    try std.testing.expectEqual(@as(usize, 4), SimdLevel.sse4.f32Width());

    try std.testing.expectEqual(@as(usize, 4), SimdLevel.avx2.f64Width());
    try std.testing.expectEqual(@as(usize, 8), SimdLevel.avx2.f32Width());

    try std.testing.expectEqual(@as(usize, 8), SimdLevel.avx512.f64Width());
    try std.testing.expectEqual(@as(usize, 16), SimdLevel.avx512.f32Width());
}

test "cpuid - thread safety" {
    resetDetection();

    // Spawn multiple threads that all call getSimdLevel
    var threads: [8]std.Thread = undefined;
    var results: [8]SimdLevel = undefined;

    for (&threads, 0..) |*t, i| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(idx: usize, res: *[8]SimdLevel) void {
                res[idx] = getSimdLevel();
            }
        }.run, .{ i, &results }) catch unreachable;
    }

    for (&threads) |*t| {
        t.join();
    }

    // All threads should get the same result
    const expected = results[0];
    for (results) |r| {
        try std.testing.expectEqual(expected, r);
    }
}
