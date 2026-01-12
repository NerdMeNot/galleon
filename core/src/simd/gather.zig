//! Gather Operations
//!
//! Used for join result materialization - gather values by indices from source arrays.
//! Supports all numeric types through a generic implementation.

const std = @import("std");

// ============================================================================
// Generic Gather Implementation
// ============================================================================

/// Gather values by indices from source array into destination array.
/// This is the primary generic implementation used by all type-specific variants.
///
/// Parameters:
/// - T: The element type (f64, i64, i32, f32, etc.)
/// - src: Source data array
/// - indices: Row indices to gather (-1 or out of range means null/zero)
/// - dst: Destination array (same length as indices)
///
/// Example:
///   src = [10, 20, 30, 40]
///   indices = [2, 0, -1, 3]
///   dst = [30, 10, 0, 40]  // -1 becomes 0 (null value)
pub fn gather(comptime T: type, src: []const T, indices: []const i32, dst: []T) void {
    for (dst, indices) |*d, idx| {
        if (idx >= 0 and @as(usize, @intCast(idx)) < src.len) {
            d.* = src[@intCast(idx)];
        } else {
            d.* = 0; // null value
        }
    }
}

// ============================================================================
// Type-Specific Wrappers (for CGO compatibility)
// ============================================================================

/// Gather f64 values by indices
pub fn gatherF64(src: []const f64, indices: []const i32, dst: []f64) void {
    gather(f64, src, indices, dst);
}

/// Gather i64 values by indices
pub fn gatherI64(src: []const i64, indices: []const i32, dst: []i64) void {
    gather(i64, src, indices, dst);
}

/// Gather i32 values by indices
pub fn gatherI32(src: []const i32, indices: []const i32, dst: []i32) void {
    gather(i32, src, indices, dst);
}

/// Gather f32 values by indices
pub fn gatherF32(src: []const f32, indices: []const i32, dst: []f32) void {
    gather(f32, src, indices, dst);
}

// ============================================================================
// Tests
// ============================================================================

test "gather - generic f64 basic" {
    const src = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };
    const indices = [_]i32{ 2, 0, 4, 1 };
    var dst: [4]f64 = undefined;

    gather(f64, &src, &indices, &dst);

    try std.testing.expectEqual(@as(f64, 30.0), dst[0]);
    try std.testing.expectEqual(@as(f64, 10.0), dst[1]);
    try std.testing.expectEqual(@as(f64, 50.0), dst[2]);
    try std.testing.expectEqual(@as(f64, 20.0), dst[3]);
}

test "gather - generic with invalid indices" {
    const src = [_]f64{ 10.0, 20.0, 30.0 };
    const indices = [_]i32{ 0, -1, 2, 10 }; // -1 and 10 are invalid
    var dst: [4]f64 = undefined;

    gather(f64, &src, &indices, &dst);

    try std.testing.expectEqual(@as(f64, 10.0), dst[0]);
    try std.testing.expectEqual(@as(f64, 0.0), dst[1]); // null value
    try std.testing.expectEqual(@as(f64, 30.0), dst[2]);
    try std.testing.expectEqual(@as(f64, 0.0), dst[3]); // null value (out of range)
}

test "gather - gatherF64 wrapper" {
    const src = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };
    const indices = [_]i32{ 2, 0, 4, 1 };
    var dst: [4]f64 = undefined;

    gatherF64(&src, &indices, &dst);

    try std.testing.expectEqual(@as(f64, 30.0), dst[0]);
    try std.testing.expectEqual(@as(f64, 10.0), dst[1]);
    try std.testing.expectEqual(@as(f64, 50.0), dst[2]);
    try std.testing.expectEqual(@as(f64, 20.0), dst[3]);
}

test "gather - gatherI64 basic" {
    const src = [_]i64{ 100, 200, 300, 400 };
    const indices = [_]i32{ 3, 1, 0 };
    var dst: [3]i64 = undefined;

    gatherI64(&src, &indices, &dst);

    try std.testing.expectEqual(@as(i64, 400), dst[0]);
    try std.testing.expectEqual(@as(i64, 200), dst[1]);
    try std.testing.expectEqual(@as(i64, 100), dst[2]);
}

test "gather - gatherI64 with invalid indices" {
    const src = [_]i64{ 100, 200 };
    const indices = [_]i32{ 0, -1, 5 };
    var dst: [3]i64 = undefined;

    gatherI64(&src, &indices, &dst);

    try std.testing.expectEqual(@as(i64, 100), dst[0]);
    try std.testing.expectEqual(@as(i64, 0), dst[1]);
    try std.testing.expectEqual(@as(i64, 0), dst[2]);
}

test "gather - gatherI32 basic" {
    const src = [_]i32{ 1, 2, 3, 4, 5 };
    const indices = [_]i32{ 4, 2, 0 };
    var dst: [3]i32 = undefined;

    gatherI32(&src, &indices, &dst);

    try std.testing.expectEqual(@as(i32, 5), dst[0]);
    try std.testing.expectEqual(@as(i32, 3), dst[1]);
    try std.testing.expectEqual(@as(i32, 1), dst[2]);
}

test "gather - gatherF32 basic" {
    const src = [_]f32{ 1.5, 2.5, 3.5, 4.5 };
    const indices = [_]i32{ 1, 3, 0 };
    var dst: [3]f32 = undefined;

    gatherF32(&src, &indices, &dst);

    try std.testing.expectEqual(@as(f32, 2.5), dst[0]);
    try std.testing.expectEqual(@as(f32, 4.5), dst[1]);
    try std.testing.expectEqual(@as(f32, 1.5), dst[2]);
}

test "gather - empty arrays" {
    const src: []const f64 = &[_]f64{};
    const indices: []const i32 = &[_]i32{};
    var dst: [0]f64 = undefined;

    gather(f64, src, indices, &dst);
    // Should not crash with empty arrays
}
