//! SIMD-accelerated conditional operations for the Galleon DataFrame library.
//!
//! This module provides vectorized conditional operations including:
//! - select (when/then/otherwise)
//! - is_null/is_not_null detection (NaN for floats)
//! - fill_null replacement
//! - coalesce (first non-null)

const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Select Operations (when/then/otherwise)
// ============================================================================

/// SIMD select for f64: out[i] = if mask[i] then then_val[i] else else_val[i]
pub fn selectF64(mask: []const u8, then_val: []const f64, else_val: []const f64, out: []f64) void {
    const len = @min(@min(mask.len, then_val.len), @min(else_val.len, out.len));
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, f64);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    // Process 4 vectors at a time (32 elements)
    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;

            const then_vec: Vec = then_val[offset..][0..VECTOR_WIDTH].*;
            const else_vec: Vec = else_val[offset..][0..VECTOR_WIDTH].*;

            // Build mask vector from u8 array
            var result: [VECTOR_WIDTH]f64 = undefined;
            inline for (0..VECTOR_WIDTH) |k| {
                result[k] = if (mask[offset + k] != 0) then_vec[k] else else_vec[k];
            }
            out[offset..][0..VECTOR_WIDTH].* = result;
        }
    }

    // Handle aligned remainder
    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const then_vec: Vec = then_val[i..][0..VECTOR_WIDTH].*;
        const else_vec: Vec = else_val[i..][0..VECTOR_WIDTH].*;

        var result: [VECTOR_WIDTH]f64 = undefined;
        inline for (0..VECTOR_WIDTH) |k| {
            result[k] = if (mask[i + k] != 0) then_vec[k] else else_vec[k];
        }
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    // Handle scalar remainder
    while (i < len) : (i += 1) {
        out[i] = if (mask[i] != 0) then_val[i] else else_val[i];
    }
}

/// SIMD select for i64
pub fn selectI64(mask: []const u8, then_val: []const i64, else_val: []const i64, out: []i64) void {
    const len = @min(@min(mask.len, then_val.len), @min(else_val.len, out.len));
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, i64);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;

            const then_vec: Vec = then_val[offset..][0..VECTOR_WIDTH].*;
            const else_vec: Vec = else_val[offset..][0..VECTOR_WIDTH].*;

            var result: [VECTOR_WIDTH]i64 = undefined;
            inline for (0..VECTOR_WIDTH) |k| {
                result[k] = if (mask[offset + k] != 0) then_vec[k] else else_vec[k];
            }
            out[offset..][0..VECTOR_WIDTH].* = result;
        }
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const then_vec: Vec = then_val[i..][0..VECTOR_WIDTH].*;
        const else_vec: Vec = else_val[i..][0..VECTOR_WIDTH].*;

        var result: [VECTOR_WIDTH]i64 = undefined;
        inline for (0..VECTOR_WIDTH) |k| {
            result[k] = if (mask[i + k] != 0) then_vec[k] else else_vec[k];
        }
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    while (i < len) : (i += 1) {
        out[i] = if (mask[i] != 0) then_val[i] else else_val[i];
    }
}

/// SIMD select with scalar else value for f64
pub fn selectScalarF64(mask: []const u8, then_val: []const f64, else_scalar: f64, out: []f64) void {
    const len = @min(@min(mask.len, then_val.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, f64);
    const else_vec: Vec = @splat(else_scalar);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;
            const then_vec: Vec = then_val[offset..][0..VECTOR_WIDTH].*;

            var result: [VECTOR_WIDTH]f64 = undefined;
            inline for (0..VECTOR_WIDTH) |k| {
                result[k] = if (mask[offset + k] != 0) then_vec[k] else else_vec[k];
            }
            out[offset..][0..VECTOR_WIDTH].* = result;
        }
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const then_vec: Vec = then_val[i..][0..VECTOR_WIDTH].*;

        var result: [VECTOR_WIDTH]f64 = undefined;
        inline for (0..VECTOR_WIDTH) |k| {
            result[k] = if (mask[i + k] != 0) then_vec[k] else else_vec[k];
        }
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    while (i < len) : (i += 1) {
        out[i] = if (mask[i] != 0) then_val[i] else else_scalar;
    }
}

// ============================================================================
// Null Detection (NaN for floats)
// ============================================================================

/// SIMD is_null detection for f64 (checks for NaN)
/// out[i] = 1 if data[i] is NaN, else 0
pub fn isNullF64(data: []const f64, out: []u8) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, f64);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    // NaN != NaN is true, so we can detect NaN with self-inequality
    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;
            const vec: Vec = data[offset..][0..VECTOR_WIDTH].*;
            // NaN != NaN yields true
            const is_nan: BoolVec = vec != vec;
            out[offset..][0..VECTOR_WIDTH].* = @intFromBool(is_nan);
        }
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const vec: Vec = data[i..][0..VECTOR_WIDTH].*;
        const is_nan: BoolVec = vec != vec;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(is_nan);
    }

    while (i < len) : (i += 1) {
        out[i] = if (data[i] != data[i]) 1 else 0;
    }
}

/// SIMD is_not_null detection for f64 (checks for NOT NaN)
/// out[i] = 1 if data[i] is not NaN, else 0
pub fn isNotNullF64(data: []const f64, out: []u8) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, f64);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;
            const vec: Vec = data[offset..][0..VECTOR_WIDTH].*;
            // NaN == NaN yields false, so x == x is true only for non-NaN
            const is_not_nan: BoolVec = vec == vec;
            out[offset..][0..VECTOR_WIDTH].* = @intFromBool(is_not_nan);
        }
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const vec: Vec = data[i..][0..VECTOR_WIDTH].*;
        const is_not_nan: BoolVec = vec == vec;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(is_not_nan);
    }

    while (i < len) : (i += 1) {
        out[i] = if (data[i] == data[i]) 1 else 0;
    }
}

// ============================================================================
// Fill Null Operations
// ============================================================================

/// SIMD fill_null for f64: replace NaN values with fill_value
pub fn fillNullF64(data: []const f64, fill_value: f64, out: []f64) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, f64);
    const fill_vec: Vec = @splat(fill_value);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;
            const vec: Vec = data[offset..][0..VECTOR_WIDTH].*;

            // Select fill_value where NaN, otherwise original
            var result: [VECTOR_WIDTH]f64 = undefined;
            inline for (0..VECTOR_WIDTH) |k| {
                result[k] = if (vec[k] != vec[k]) fill_vec[k] else vec[k];
            }
            out[offset..][0..VECTOR_WIDTH].* = result;
        }
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const vec: Vec = data[i..][0..VECTOR_WIDTH].*;

        var result: [VECTOR_WIDTH]f64 = undefined;
        inline for (0..VECTOR_WIDTH) |k| {
            result[k] = if (vec[k] != vec[k]) fill_vec[k] else vec[k];
        }
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    while (i < len) : (i += 1) {
        out[i] = if (data[i] != data[i]) fill_value else data[i];
    }
}

/// SIMD fill_null with forward fill strategy
/// Replace NaN with the most recent non-NaN value
pub fn fillNullForwardF64(data: []const f64, out: []f64) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    var last_valid: f64 = std.math.nan(f64);

    for (0..len) |i| {
        if (data[i] == data[i]) { // Not NaN
            last_valid = data[i];
            out[i] = data[i];
        } else {
            out[i] = last_valid;
        }
    }
}

/// SIMD fill_null with backward fill strategy
/// Replace NaN with the next non-NaN value
pub fn fillNullBackwardF64(data: []const f64, out: []f64) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    var next_valid: f64 = std.math.nan(f64);

    var i = len;
    while (i > 0) {
        i -= 1;
        if (data[i] == data[i]) { // Not NaN
            next_valid = data[i];
            out[i] = data[i];
        } else {
            out[i] = next_valid;
        }
    }
}

// ============================================================================
// Coalesce Operations
// ============================================================================

/// Coalesce: return first non-NaN value from multiple arrays
/// For each position, returns the first non-NaN value across input arrays
pub fn coalesceF64(
    inputs: []const [*]const f64,
    input_lens: []const usize,
    num_inputs: usize,
    out: []f64,
    out_len: usize,
) void {
    if (num_inputs == 0 or out_len == 0) return;

    // Initialize output with NaN
    for (0..out_len) |i| {
        out[i] = std.math.nan(f64);
    }

    // For each position, find first non-NaN across inputs
    for (0..out_len) |i| {
        for (0..num_inputs) |j| {
            if (i < input_lens[j]) {
                const val = inputs[j][i];
                if (val == val) { // Not NaN
                    out[i] = val;
                    break;
                }
            }
        }
    }
}

/// Coalesce two arrays (optimized for common case)
pub fn coalesce2F64(a: []const f64, b: []const f64, out: []f64) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, f64);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;
            const a_vec: Vec = a[offset..][0..VECTOR_WIDTH].*;
            const b_vec: Vec = b[offset..][0..VECTOR_WIDTH].*;

            // Use a if not NaN, otherwise use b
            var result: [VECTOR_WIDTH]f64 = undefined;
            inline for (0..VECTOR_WIDTH) |k| {
                result[k] = if (a_vec[k] == a_vec[k]) a_vec[k] else b_vec[k];
            }
            out[offset..][0..VECTOR_WIDTH].* = result;
        }
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const a_vec: Vec = a[i..][0..VECTOR_WIDTH].*;
        const b_vec: Vec = b[i..][0..VECTOR_WIDTH].*;

        var result: [VECTOR_WIDTH]f64 = undefined;
        inline for (0..VECTOR_WIDTH) |k| {
            result[k] = if (a_vec[k] == a_vec[k]) a_vec[k] else b_vec[k];
        }
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] == a[i]) a[i] else b[i];
    }
}

// ============================================================================
// Count Null/Not Null
// ============================================================================

/// Count the number of NaN values in an array
pub fn countNullF64(data: []const f64) usize {
    const len = data.len;
    if (len == 0) return 0;

    const Vec = @Vector(VECTOR_WIDTH, f64);

    var count: usize = 0;
    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    // Accumulate counts in vector form
    var count_vec: @Vector(VECTOR_WIDTH, u8) = @splat(0);

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        inline for (0..4) |j| {
            const offset = i + j * VECTOR_WIDTH;
            const vec: Vec = data[offset..][0..VECTOR_WIDTH].*;
            const is_nan = vec != vec;
            count_vec += @intFromBool(is_nan);
        }

        // Periodically reduce to avoid overflow
        if (i % (CHUNK_SIZE * 8) == 0) {
            count += @reduce(.Add, count_vec);
            count_vec = @splat(0);
        }
    }

    count += @reduce(.Add, count_vec);

    // Handle remainder
    while (i < len) : (i += 1) {
        if (data[i] != data[i]) count += 1;
    }

    return count;
}

/// Count the number of non-NaN values in an array
pub fn countNotNullF64(data: []const f64) usize {
    return data.len - countNullF64(data);
}

// ============================================================================
// Tests
// ============================================================================

test "conditionals - selectF64 basic" {
    const mask = [_]u8{ 1, 0, 1, 0, 1, 0 };
    const then_val = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0, 60.0 };
    const else_val = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    var out: [6]f64 = undefined;

    selectF64(&mask, &then_val, &else_val, &out);

    try std.testing.expectApproxEqAbs(@as(f64, 10.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), out[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), out[2], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), out[3], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 50.0), out[4], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 6.0), out[5], 0.0001);
}

test "conditionals - selectI64 basic" {
    const mask = [_]u8{ 1, 0, 1, 0 };
    const then_val = [_]i64{ 100, 200, 300, 400 };
    const else_val = [_]i64{ 1, 2, 3, 4 };
    var out: [4]i64 = undefined;

    selectI64(&mask, &then_val, &else_val, &out);

    try std.testing.expectEqual(@as(i64, 100), out[0]);
    try std.testing.expectEqual(@as(i64, 2), out[1]);
    try std.testing.expectEqual(@as(i64, 300), out[2]);
    try std.testing.expectEqual(@as(i64, 4), out[3]);
}

test "conditionals - isNullF64" {
    const nan = std.math.nan(f64);
    const data = [_]f64{ 1.0, nan, 3.0, nan, 5.0 };
    var out: [5]u8 = undefined;

    isNullF64(&data, &out);

    try std.testing.expectEqual(@as(u8, 0), out[0]);
    try std.testing.expectEqual(@as(u8, 1), out[1]);
    try std.testing.expectEqual(@as(u8, 0), out[2]);
    try std.testing.expectEqual(@as(u8, 1), out[3]);
    try std.testing.expectEqual(@as(u8, 0), out[4]);
}

test "conditionals - isNotNullF64" {
    const nan = std.math.nan(f64);
    const data = [_]f64{ 1.0, nan, 3.0, nan, 5.0 };
    var out: [5]u8 = undefined;

    isNotNullF64(&data, &out);

    try std.testing.expectEqual(@as(u8, 1), out[0]);
    try std.testing.expectEqual(@as(u8, 0), out[1]);
    try std.testing.expectEqual(@as(u8, 1), out[2]);
    try std.testing.expectEqual(@as(u8, 0), out[3]);
    try std.testing.expectEqual(@as(u8, 1), out[4]);
}

test "conditionals - fillNullF64" {
    const nan = std.math.nan(f64);
    const data = [_]f64{ 1.0, nan, 3.0, nan, 5.0 };
    var out: [5]f64 = undefined;

    fillNullF64(&data, 0.0, &out);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), out[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), out[2], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), out[3], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), out[4], 0.0001);
}

test "conditionals - fillNullForwardF64" {
    const nan = std.math.nan(f64);
    const data = [_]f64{ 1.0, nan, nan, 4.0, nan };
    var out: [5]f64 = undefined;

    fillNullForwardF64(&data, &out);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), out[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), out[2], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), out[3], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), out[4], 0.0001);
}

test "conditionals - coalesce2F64" {
    const nan = std.math.nan(f64);
    const a = [_]f64{ 1.0, nan, 3.0, nan };
    const b = [_]f64{ 10.0, 20.0, nan, 40.0 };
    var out: [4]f64 = undefined;

    coalesce2F64(&a, &b, &out);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), out[0], 0.0001); // a not null
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), out[1], 0.0001); // a null, use b
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), out[2], 0.0001); // a not null
    try std.testing.expectApproxEqAbs(@as(f64, 40.0), out[3], 0.0001); // a null, use b
}

test "conditionals - countNullF64" {
    const nan = std.math.nan(f64);
    const data = [_]f64{ 1.0, nan, 3.0, nan, nan, 6.0 };

    const null_count = countNullF64(&data);
    try std.testing.expectEqual(@as(usize, 3), null_count);

    const not_null_count = countNotNullF64(&data);
    try std.testing.expectEqual(@as(usize, 3), not_null_count);
}

test "conditionals - selectF64 large array SIMD path" {
    var mask: [100]u8 = undefined;
    var then_val: [100]f64 = undefined;
    var else_val: [100]f64 = undefined;
    var out: [100]f64 = undefined;

    for (0..100) |i| {
        mask[i] = if (i % 2 == 0) 1 else 0;
        then_val[i] = @floatFromInt(i * 10);
        else_val[i] = @floatFromInt(i);
    }

    selectF64(&mask, &then_val, &else_val, &out);

    for (0..100) |i| {
        const expected: f64 = if (i % 2 == 0) @floatFromInt(i * 10) else @floatFromInt(i);
        try std.testing.expectApproxEqAbs(expected, out[i], 0.0001);
    }
}

test "conditionals - isNullF64 large array SIMD path" {
    const nan = std.math.nan(f64);
    var data: [100]f64 = undefined;
    var out: [100]u8 = undefined;

    for (0..100) |i| {
        data[i] = if (i % 3 == 0) nan else @floatFromInt(i);
    }

    isNullF64(&data, &out);

    for (0..100) |i| {
        const expected: u8 = if (i % 3 == 0) 1 else 0;
        try std.testing.expectEqual(expected, out[i]);
    }
}

test "conditionals - empty array" {
    const mask = [_]u8{};
    const then_val = [_]f64{};
    const else_val = [_]f64{};
    var out: [0]f64 = undefined;

    selectF64(&mask, &then_val, &else_val, &out);
    // Should not crash
}
