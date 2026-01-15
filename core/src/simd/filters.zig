//! Filter and Mask Operations
//!
//! SIMD-accelerated filtering operations for selecting array elements.
//!
//! Output formats:
//! - Indices: filterGreaterThan returns u32 indices of matching elements
//! - Bool mask: filterMaskGreaterThan outputs []bool
//! - U8 mask: filterMaskU8GreaterThan outputs []u8 (0/1 values, more efficient for SIMD)
//!
//! Type variants:
//! - filterGreaterThan: Generic for all numeric types (f64, f32, i64, i32)
//! - filterGreaterThanInt: Alias for integer types (same implementation, clearer CGO naming)
//! - filterMaskU8GreaterThan: Generic for floats
//! - filterMaskU8GreaterThanInt: Alias for integers
//!
//! Note: The "Int" suffix functions exist for clearer CGO export naming,
//! but internally use the same generic implementation.
//!
//! Mask utilities:
//! - countMaskTrue: Count non-zero elements in u8 mask
//! - indicesFromMask: Extract indices where mask[i] != 0
//! - countTrue: Count true values in bool slice

const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Mask Utilities
// ============================================================================

/// Count the number of true (non-zero) values in a u8 mask using SIMD
pub fn countMaskTrue(mask: []const u8) usize {
    const len = mask.len;
    if (len == 0) return 0;

    // Use SIMD to sum bytes in chunks
    // Each non-zero byte contributes 1 when we compare != 0
    const Vec = @Vector(VECTOR_WIDTH * 4, u8); // 32 bytes at a time
    const zero_vec: Vec = @splat(0);

    var count: usize = 0;
    const chunk_size = VECTOR_WIDTH * 4;
    const aligned_len = len - (len % chunk_size);
    var i: usize = 0;

    // Process 32 bytes at a time
    while (i < aligned_len) : (i += chunk_size) {
        const chunk: Vec = mask[i..][0..chunk_size].*;
        // Compare with zero - gives 0xFF for non-zero, 0x00 for zero
        const cmp = chunk != zero_vec;
        // Convert bool mask to 1/0 values
        const ones: Vec = @select(u8, cmp, @as(Vec, @splat(1)), @as(Vec, @splat(0)));
        // Sum all bytes - use widening to avoid overflow
        var sum: u32 = 0;
        inline for (0..chunk_size) |j| {
            sum += ones[j];
        }
        count += sum;
    }

    // Scalar tail
    while (i < len) : (i += 1) {
        if (mask[i] != 0) count += 1;
    }

    return count;
}

/// Extract indices where mask[i] != 0 into out_indices
/// Returns the number of indices written
pub fn indicesFromMask(mask: []const u8, out_indices: []u32) usize {
    const len = mask.len;
    if (len == 0) return 0;

    var count: usize = 0;

    // Build indices where mask is true
    for (mask, 0..) |v, i| {
        if (v != 0) {
            if (count < out_indices.len) {
                out_indices[count] = @intCast(i);
            }
            count += 1;
        }
    }

    return count;
}

/// Count and extract indices in one pass (more efficient than separate calls)
/// Returns the number of indices written
pub fn countAndExtractIndices(mask: []const u8, out_indices: []u32) usize {
    return indicesFromMask(mask, out_indices);
}

// ============================================================================
// Filter Mask Functions (Float)
// ============================================================================

/// Create boolean mask for values greater than threshold (fast - no index extraction)
pub fn filterMaskGreaterThan(comptime T: type, data: []const T, threshold: T, out_mask: []bool) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const threshold_vec: Vec = @splat(threshold);

    // Process unrolled (32 elements at a time)
    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const mask0 = chunk0 > threshold_vec;
        const mask1 = chunk1 > threshold_vec;
        const mask2 = chunk2 > threshold_vec;
        const mask3 = chunk3 > threshold_vec;

        // Write boolean masks directly
        out_mask[i..][0..VECTOR_WIDTH].* = mask0;
        out_mask[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = mask1;
        out_mask[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = mask2;
        out_mask[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = mask3;
    }

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const mask = chunk > threshold_vec;
        out_mask[i..][0..VECTOR_WIDTH].* = mask;
    }

    // Handle tail
    while (i < data.len) : (i += 1) {
        out_mask[i] = data[i] > threshold;
    }
}

/// Create u8 mask for values greater than threshold (optimized for SIMD - 1=true, 0=false)
pub fn filterMaskU8GreaterThan(comptime T: type, data: []const T, threshold: T, out_mask: []u8) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const threshold_vec: Vec = @splat(threshold);

    // Use u8 vectors for output - more efficient than bool
    const ones: @Vector(VECTOR_WIDTH, u8) = @splat(1);
    const zeros: @Vector(VECTOR_WIDTH, u8) = @splat(0);

    // Process unrolled (32 elements at a time)
    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const cmp0 = chunk0 > threshold_vec;
        const cmp1 = chunk1 > threshold_vec;
        const cmp2 = chunk2 > threshold_vec;
        const cmp3 = chunk3 > threshold_vec;

        // Convert bool vectors to u8 vectors using select
        out_mask[i..][0..VECTOR_WIDTH].* = @select(u8, cmp0, ones, zeros);
        out_mask[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @select(u8, cmp1, ones, zeros);
        out_mask[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @select(u8, cmp2, ones, zeros);
        out_mask[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @select(u8, cmp3, ones, zeros);
    }

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const cmp = chunk > threshold_vec;
        out_mask[i..][0..VECTOR_WIDTH].* = @select(u8, cmp, ones, zeros);
    }

    // Handle tail
    while (i < data.len) : (i += 1) {
        out_mask[i] = if (data[i] > threshold) 1 else 0;
    }
}

/// Filter values greater than threshold using SIMD comparisons with loop unrolling
/// Returns indices of matching elements
pub fn filterGreaterThan(comptime T: type, data: []const T, threshold: T, out_indices: []u32) usize {
    if (data.len == 0) return 0;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const threshold_vec: Vec = @splat(threshold);
    var count: usize = 0;

    // Process 4 vectors at a time (32 elements) for better throughput
    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len and count + CHUNK_SIZE <= out_indices.len) : (i += CHUNK_SIZE) {
        // Load and compare 4 vectors
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const mask0 = chunk0 > threshold_vec;
        const mask1 = chunk1 > threshold_vec;
        const mask2 = chunk2 > threshold_vec;
        const mask3 = chunk3 > threshold_vec;

        // Extract indices from each vector - use branchless writes
        const base0: u32 = @intCast(i);
        const base1: u32 = @intCast(i + VECTOR_WIDTH);
        const base2: u32 = @intCast(i + VECTOR_WIDTH * 2);
        const base3: u32 = @intCast(i + VECTOR_WIDTH * 3);

        // Process vector 0
        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base0 + @as(u32, j);
            count += @intFromBool(mask0[j]);
        }

        // Process vector 1
        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base1 + @as(u32, j);
            count += @intFromBool(mask1[j]);
        }

        // Process vector 2
        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base2 + @as(u32, j);
            count += @intFromBool(mask2[j]);
        }

        // Process vector 3
        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base3 + @as(u32, j);
            count += @intFromBool(mask3[j]);
        }
    }

    // Handle remaining aligned elements (single vector at a time)
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len and count + VECTOR_WIDTH <= out_indices.len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const mask = chunk > threshold_vec;
        const base: u32 = @intCast(i);

        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base + @as(u32, j);
            count += @intFromBool(mask[j]);
        }
    }

    // Handle remaining elements (scalar) with bounds checking
    while (i < data.len) : (i += 1) {
        if (data[i] > threshold) {
            if (count < out_indices.len) {
                out_indices[count] = @intCast(i);
            }
            count += 1;
        }
    }

    return count;
}

// ============================================================================
// Filter Functions (Integer)
// ============================================================================

/// Filter integer values greater than threshold using SIMD comparisons
/// Returns indices of matching elements
pub fn filterGreaterThanInt(comptime T: type, data: []const T, threshold: T, out_indices: []u32) usize {
    if (data.len == 0) return 0;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const threshold_vec: Vec = @splat(threshold);
    var count: usize = 0;

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len and count + CHUNK_SIZE <= out_indices.len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const mask0 = chunk0 > threshold_vec;
        const mask1 = chunk1 > threshold_vec;
        const mask2 = chunk2 > threshold_vec;
        const mask3 = chunk3 > threshold_vec;

        const base0: u32 = @intCast(i);
        const base1: u32 = @intCast(i + VECTOR_WIDTH);
        const base2: u32 = @intCast(i + VECTOR_WIDTH * 2);
        const base3: u32 = @intCast(i + VECTOR_WIDTH * 3);

        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base0 + @as(u32, j);
            count += @intFromBool(mask0[j]);
        }

        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base1 + @as(u32, j);
            count += @intFromBool(mask1[j]);
        }

        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base2 + @as(u32, j);
            count += @intFromBool(mask2[j]);
        }

        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base3 + @as(u32, j);
            count += @intFromBool(mask3[j]);
        }
    }

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len and count + VECTOR_WIDTH <= out_indices.len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const mask = chunk > threshold_vec;
        const base: u32 = @intCast(i);

        inline for (0..VECTOR_WIDTH) |j| {
            out_indices[count] = base + @as(u32, j);
            count += @intFromBool(mask[j]);
        }
    }

    while (i < data.len) : (i += 1) {
        if (data[i] > threshold) {
            if (count < out_indices.len) {
                out_indices[count] = @intCast(i);
            }
            count += 1;
        }
    }

    return count;
}

/// Create u8 mask for integer values greater than threshold
pub fn filterMaskU8GreaterThanInt(comptime T: type, data: []const T, threshold: T, out_mask: []u8) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const threshold_vec: Vec = @splat(threshold);

    const ones: @Vector(VECTOR_WIDTH, u8) = @splat(1);
    const zeros: @Vector(VECTOR_WIDTH, u8) = @splat(0);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const cmp0 = chunk0 > threshold_vec;
        const cmp1 = chunk1 > threshold_vec;
        const cmp2 = chunk2 > threshold_vec;
        const cmp3 = chunk3 > threshold_vec;

        out_mask[i..][0..VECTOR_WIDTH].* = @select(u8, cmp0, ones, zeros);
        out_mask[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @select(u8, cmp1, ones, zeros);
        out_mask[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @select(u8, cmp2, ones, zeros);
        out_mask[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @select(u8, cmp3, ones, zeros);
    }

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const cmp = chunk > threshold_vec;
        out_mask[i..][0..VECTOR_WIDTH].* = @select(u8, cmp, ones, zeros);
    }

    while (i < data.len) : (i += 1) {
        out_mask[i] = if (data[i] > threshold) 1 else 0;
    }
}

// ============================================================================
// Boolean Operations
// ============================================================================

/// Count true values in a boolean slice
/// Count true values in bool slice using SIMD
pub fn countTrue(data: []const bool) usize {
    var count: usize = 0;
    const len = data.len;
    if (len == 0) return 0;

    // Bools are stored as u8 (0 or 1), so we can sum them directly
    // Use SIMD to process 32 bytes at a time
    const Vec = @Vector(32, u8);
    const chunk_size = 32;
    const aligned_len = len - (len % chunk_size);
    var i: usize = 0;

    while (i < aligned_len) : (i += chunk_size) {
        const b = @as(*const [chunk_size]u8, @ptrCast(data[i..].ptr));
        const chunk: Vec = b.*;
        // Sum all bytes using horizontal reduction
        count += @reduce(.Add, @as(@Vector(32, u32), chunk));
    }

    // Process remaining 8 at a time
    while (i + 8 <= len) : (i += 8) {
        const b = @as(*const [8]u8, @ptrCast(data[i..].ptr));
        count += @as(usize, b[0]) + @as(usize, b[1]) + @as(usize, b[2]) + @as(usize, b[3]) +
            @as(usize, b[4]) + @as(usize, b[5]) + @as(usize, b[6]) + @as(usize, b[7]);
    }

    // Scalar tail
    while (i < len) : (i += 1) {
        count += @intFromBool(data[i]);
    }

    return count;
}

// ============================================================================
// Tests
// ============================================================================

test "filters - countMaskTrue" {
    const mask = [_]u8{ 1, 0, 1, 1, 0, 0, 1, 0 };
    try std.testing.expectEqual(@as(usize, 4), countMaskTrue(&mask));

    const empty: []const u8 = &[_]u8{};
    try std.testing.expectEqual(@as(usize, 0), countMaskTrue(empty));
}

test "filters - indicesFromMask" {
    const mask = [_]u8{ 1, 0, 1, 1, 0, 0, 1, 0 };
    var indices: [8]u32 = undefined;

    const count = indicesFromMask(&mask, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expectEqual(@as(u32, 0), indices[0]);
    try std.testing.expectEqual(@as(u32, 2), indices[1]);
    try std.testing.expectEqual(@as(u32, 3), indices[2]);
    try std.testing.expectEqual(@as(u32, 6), indices[3]);
}

test "filters - filterGreaterThan f64" {
    const data = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0, 8.0, 4.0, 6.0 };
    var indices: [8]u32 = undefined;

    const count = filterGreaterThan(f64, &data, 4.0, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
    // Values > 4.0 are at indices 1(5.0), 3(7.0), 5(8.0), 7(6.0)
    try std.testing.expectEqual(@as(u32, 1), indices[0]);
    try std.testing.expectEqual(@as(u32, 3), indices[1]);
    try std.testing.expectEqual(@as(u32, 5), indices[2]);
    try std.testing.expectEqual(@as(u32, 7), indices[3]);
}

test "filters - filterMaskU8GreaterThan" {
    const data = [_]f64{ 1.0, 5.0, 3.0, 7.0 };
    var mask: [4]u8 = undefined;

    filterMaskU8GreaterThan(f64, &data, 3.0, &mask);
    try std.testing.expectEqual(@as(u8, 0), mask[0]); // 1.0 <= 3.0
    try std.testing.expectEqual(@as(u8, 1), mask[1]); // 5.0 > 3.0
    try std.testing.expectEqual(@as(u8, 0), mask[2]); // 3.0 <= 3.0
    try std.testing.expectEqual(@as(u8, 1), mask[3]); // 7.0 > 3.0
}

test "filters - filterGreaterThanInt" {
    const data = [_]i64{ 1, 5, 3, 7, 2, 8, 4, 6 };
    var indices: [8]u32 = undefined;

    const count = filterGreaterThanInt(i64, &data, 4, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expectEqual(@as(u32, 1), indices[0]);
    try std.testing.expectEqual(@as(u32, 3), indices[1]);
    try std.testing.expectEqual(@as(u32, 5), indices[2]);
    try std.testing.expectEqual(@as(u32, 7), indices[3]);
}

test "filters - countTrue" {
    const data = [_]bool{ true, false, true, true, false, false, true, false };
    try std.testing.expectEqual(@as(usize, 4), countTrue(&data));

    const all_true = [_]bool{ true, true, true, true };
    try std.testing.expectEqual(@as(usize, 4), countTrue(&all_true));

    const all_false = [_]bool{ false, false, false, false };
    try std.testing.expectEqual(@as(usize, 0), countTrue(&all_false));
}

// Additional tests for better coverage

test "filters - filterGreaterThan f32" {
    const data = [_]f32{ 1.0, 5.0, 3.0, 7.0, 2.0, 8.0, 4.0, 6.0 };
    var indices: [8]u32 = undefined;

    const count = filterGreaterThan(f32, &data, 4.0, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expectEqual(@as(u32, 1), indices[0]); // 5.0
    try std.testing.expectEqual(@as(u32, 3), indices[1]); // 7.0
    try std.testing.expectEqual(@as(u32, 5), indices[2]); // 8.0
    try std.testing.expectEqual(@as(u32, 7), indices[3]); // 6.0
}

test "filters - filterGreaterThanInt i32" {
    const data = [_]i32{ 1, 5, 3, 7, 2, 8, 4, 6 };
    var indices: [8]u32 = undefined;

    const count = filterGreaterThanInt(i32, &data, 4, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expectEqual(@as(u32, 1), indices[0]);
    try std.testing.expectEqual(@as(u32, 3), indices[1]);
    try std.testing.expectEqual(@as(u32, 5), indices[2]);
    try std.testing.expectEqual(@as(u32, 7), indices[3]);
}

test "filters - filterMaskU8GreaterThanInt i64" {
    const data = [_]i64{ 1, 5, 3, 7 };
    var mask: [4]u8 = undefined;

    filterMaskU8GreaterThanInt(i64, &data, 3, &mask);
    try std.testing.expectEqual(@as(u8, 0), mask[0]); // 1 <= 3
    try std.testing.expectEqual(@as(u8, 1), mask[1]); // 5 > 3
    try std.testing.expectEqual(@as(u8, 0), mask[2]); // 3 <= 3
    try std.testing.expectEqual(@as(u8, 1), mask[3]); // 7 > 3
}

test "filters - filterMaskU8GreaterThanInt i32" {
    const data = [_]i32{ 1, 5, 3, 7 };
    var mask: [4]u8 = undefined;

    filterMaskU8GreaterThanInt(i32, &data, 3, &mask);
    try std.testing.expectEqual(@as(u8, 0), mask[0]);
    try std.testing.expectEqual(@as(u8, 1), mask[1]);
    try std.testing.expectEqual(@as(u8, 0), mask[2]);
    try std.testing.expectEqual(@as(u8, 1), mask[3]);
}

test "filters - filterMaskGreaterThan f64" {
    const data = [_]f64{ 1.0, 5.0, 3.0, 7.0 };
    var mask: [4]bool = undefined;

    filterMaskGreaterThan(f64, &data, 3.0, &mask);
    try std.testing.expectEqual(false, mask[0]); // 1.0 <= 3.0
    try std.testing.expectEqual(true, mask[1]); // 5.0 > 3.0
    try std.testing.expectEqual(false, mask[2]); // 3.0 <= 3.0
    try std.testing.expectEqual(true, mask[3]); // 7.0 > 3.0
}

test "filters - filterGreaterThan empty" {
    const data = [_]f64{};
    var indices: [0]u32 = undefined;

    const count = filterGreaterThan(f64, &data, 0.0, &indices);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "filters - filterGreaterThan all pass" {
    const data = [_]f64{ 10.0, 20.0, 30.0, 40.0 };
    var indices: [4]u32 = undefined;

    const count = filterGreaterThan(f64, &data, 0.0, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
}

test "filters - filterGreaterThan none pass" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    var indices: [4]u32 = undefined;

    const count = filterGreaterThan(f64, &data, 100.0, &indices);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "filters - filterGreaterThan large SIMD path" {
    var data: [100]f64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i);
    }

    var indices: [100]u32 = undefined;
    const count = filterGreaterThan(f64, &data, 50.0, &indices);
    // Values > 50: 51, 52, ..., 99 = 49 values
    try std.testing.expectEqual(@as(usize, 49), count);

    // Verify first and last indices
    try std.testing.expectEqual(@as(u32, 51), indices[0]);
    try std.testing.expectEqual(@as(u32, 99), indices[48]);
}

test "filters - filterGreaterThanInt large SIMD path" {
    var data: [100]i64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @intCast(i);
    }

    var indices: [100]u32 = undefined;
    const count = filterGreaterThanInt(i64, &data, 50, &indices);
    try std.testing.expectEqual(@as(usize, 49), count);
}

test "filters - countMaskTrue all ones" {
    const mask = [_]u8{ 1, 1, 1, 1, 1, 1, 1, 1 };
    try std.testing.expectEqual(@as(usize, 8), countMaskTrue(&mask));
}

test "filters - countMaskTrue all zeros" {
    const mask = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectEqual(@as(usize, 0), countMaskTrue(&mask));
}

test "filters - indicesFromMask empty" {
    const mask = [_]u8{};
    var indices: [0]u32 = undefined;

    const count = indicesFromMask(&mask, &indices);
    try std.testing.expectEqual(@as(usize, 0), count);
}

test "filters - indicesFromMask all true" {
    const mask = [_]u8{ 1, 1, 1, 1 };
    var indices: [4]u32 = undefined;

    const count = indicesFromMask(&mask, &indices);
    try std.testing.expectEqual(@as(usize, 4), count);
    try std.testing.expectEqual(@as(u32, 0), indices[0]);
    try std.testing.expectEqual(@as(u32, 1), indices[1]);
    try std.testing.expectEqual(@as(u32, 2), indices[2]);
    try std.testing.expectEqual(@as(u32, 3), indices[3]);
}

test "filters - countTrue large array" {
    var data: [100]bool = undefined;
    var expected_count: usize = 0;
    for (&data, 0..) |*v, i| {
        v.* = (i % 3 == 0);
        if (v.*) expected_count += 1;
    }
    try std.testing.expectEqual(expected_count, countTrue(&data));
}
