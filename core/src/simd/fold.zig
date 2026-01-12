//! Fold/horizontal aggregation operations with SIMD acceleration.
//!
//! This module provides operations that aggregate across multiple columns
//! for each row (horizontal operations), as opposed to aggregating down
//! a single column (vertical operations).
//!
//! Operations include:
//! - sumHorizontal: Sum values across columns for each row
//! - minHorizontal: Minimum value across columns for each row
//! - maxHorizontal: Maximum value across columns for each row
//! - meanHorizontal: Mean value across columns for each row
//! - anyHorizontal: True if any value is true (for bool columns)
//! - allHorizontal: True if all values are true (for bool columns)

const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;

// ============================================================================
// Two-Column Horizontal Operations (Most Common Case)
// ============================================================================

/// Sum two columns element-wise: out[i] = a[i] + b[i]
/// This is essentially addArrays but named for consistency with horizontal ops
pub fn sumHorizontal2(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    // Process in SIMD chunks
    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = va + vb;
    }

    // Handle remainder
    while (i < n) : (i += 1) {
        out[i] = a[i] + b[i];
    }
}

/// Min of two columns element-wise: out[i] = min(a[i], b[i])
pub fn minHorizontal2(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = @min(va, vb);
    }

    while (i < n) : (i += 1) {
        out[i] = @min(a[i], b[i]);
    }
}

/// Max of two columns element-wise: out[i] = max(a[i], b[i])
pub fn maxHorizontal2(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = @max(va, vb);
    }

    while (i < n) : (i += 1) {
        out[i] = @max(a[i], b[i]);
    }
}

// ============================================================================
// Three-Column Horizontal Operations
// ============================================================================

/// Sum three columns element-wise: out[i] = a[i] + b[i] + c[i]
pub fn sumHorizontal3(comptime T: type, a: []const T, b: []const T, c: []const T, out: []T) void {
    const n = @min(@min(a.len, b.len), c.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        const vc: Vec = c[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = va + vb + vc;
    }

    while (i < n) : (i += 1) {
        out[i] = a[i] + b[i] + c[i];
    }
}

/// Min of three columns element-wise
pub fn minHorizontal3(comptime T: type, a: []const T, b: []const T, c: []const T, out: []T) void {
    const n = @min(@min(a.len, b.len), c.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        const vc: Vec = c[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = @min(@min(va, vb), vc);
    }

    while (i < n) : (i += 1) {
        out[i] = @min(@min(a[i], b[i]), c[i]);
    }
}

/// Max of three columns element-wise
pub fn maxHorizontal3(comptime T: type, a: []const T, b: []const T, c: []const T, out: []T) void {
    const n = @min(@min(a.len, b.len), c.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        const vc: Vec = c[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = @max(@max(va, vb), vc);
    }

    while (i < n) : (i += 1) {
        out[i] = @max(@max(a[i], b[i]), c[i]);
    }
}

// ============================================================================
// N-Column Horizontal Operations (Variable Number of Columns)
// ============================================================================

/// Sum N columns element-wise using slice of column pointers
/// columns: array of pointers to column data
/// lens: array of column lengths (all should be same, we use minimum)
/// out: output array
pub fn sumHorizontalN(comptime T: type, columns: []const []const T, out: []T) void {
    if (columns.len == 0) return;

    // Find minimum length across all columns
    var n: usize = columns[0].len;
    for (columns[1..]) |col| {
        n = @min(n, col.len);
    }
    if (n == 0 or out.len < n) return;

    // Initialize output with first column
    @memcpy(out[0..n], columns[0][0..n]);

    // Add remaining columns
    for (columns[1..]) |col| {
        const Vec = @Vector(VECTOR_WIDTH, T);
        const aligned_len = n - (n % VECTOR_WIDTH);
        var i: usize = 0;

        while (i < aligned_len) : (i += VECTOR_WIDTH) {
            const vout: Vec = out[i..][0..VECTOR_WIDTH].*;
            const vcol: Vec = col[i..][0..VECTOR_WIDTH].*;
            out[i..][0..VECTOR_WIDTH].* = vout + vcol;
        }

        while (i < n) : (i += 1) {
            out[i] += col[i];
        }
    }
}

/// Min of N columns element-wise
pub fn minHorizontalN(comptime T: type, columns: []const []const T, out: []T) void {
    if (columns.len == 0) return;

    var n: usize = columns[0].len;
    for (columns[1..]) |col| {
        n = @min(n, col.len);
    }
    if (n == 0 or out.len < n) return;

    @memcpy(out[0..n], columns[0][0..n]);

    for (columns[1..]) |col| {
        const Vec = @Vector(VECTOR_WIDTH, T);
        const aligned_len = n - (n % VECTOR_WIDTH);
        var i: usize = 0;

        while (i < aligned_len) : (i += VECTOR_WIDTH) {
            const vout: Vec = out[i..][0..VECTOR_WIDTH].*;
            const vcol: Vec = col[i..][0..VECTOR_WIDTH].*;
            out[i..][0..VECTOR_WIDTH].* = @min(vout, vcol);
        }

        while (i < n) : (i += 1) {
            out[i] = @min(out[i], col[i]);
        }
    }
}

/// Max of N columns element-wise
pub fn maxHorizontalN(comptime T: type, columns: []const []const T, out: []T) void {
    if (columns.len == 0) return;

    var n: usize = columns[0].len;
    for (columns[1..]) |col| {
        n = @min(n, col.len);
    }
    if (n == 0 or out.len < n) return;

    @memcpy(out[0..n], columns[0][0..n]);

    for (columns[1..]) |col| {
        const Vec = @Vector(VECTOR_WIDTH, T);
        const aligned_len = n - (n % VECTOR_WIDTH);
        var i: usize = 0;

        while (i < aligned_len) : (i += VECTOR_WIDTH) {
            const vout: Vec = out[i..][0..VECTOR_WIDTH].*;
            const vcol: Vec = col[i..][0..VECTOR_WIDTH].*;
            out[i..][0..VECTOR_WIDTH].* = @max(vout, vcol);
        }

        while (i < n) : (i += 1) {
            out[i] = @max(out[i], col[i]);
        }
    }
}

/// Mean of N columns element-wise (float types only)
pub fn meanHorizontalN(comptime T: type, columns: []const []const T, out: []T) void {
    if (@typeInfo(T) != .float) return;
    if (columns.len == 0) return;

    // First compute sum
    sumHorizontalN(T, columns, out);

    // Then divide by number of columns
    const divisor: T = @floatFromInt(columns.len);
    const Vec = @Vector(VECTOR_WIDTH, T);
    const divisor_vec: Vec = @splat(divisor);

    var n: usize = columns[0].len;
    for (columns[1..]) |col| {
        n = @min(n, col.len);
    }

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const vout: Vec = out[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = vout / divisor_vec;
    }

    while (i < n) : (i += 1) {
        out[i] /= divisor;
    }
}

// ============================================================================
// Boolean Horizontal Operations
// ============================================================================

/// Any true across two columns: out[i] = a[i] || b[i]
pub fn anyHorizontal2(a: []const u8, b: []const u8, out: []u8) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, u8);
    const zero: Vec = @splat(0);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        // Any: if either is non-zero, result is 1
        const result = @select(u8, va != zero, @as(Vec, @splat(1)), @select(u8, vb != zero, @as(Vec, @splat(1)), zero));
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    while (i < n) : (i += 1) {
        out[i] = if (a[i] != 0 or b[i] != 0) 1 else 0;
    }
}

/// All true across two columns: out[i] = a[i] && b[i]
pub fn allHorizontal2(a: []const u8, b: []const u8, out: []u8) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, u8);
    const zero: Vec = @splat(0);
    const one: Vec = @splat(1);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        // All: both must be non-zero - use @select twice for AND behavior
        const a_nz = va != zero;
        const b_nz = vb != zero;
        // If a is non-zero, check b; otherwise 0
        const result = @select(u8, a_nz, @select(u8, b_nz, one, zero), zero);
        out[i..][0..VECTOR_WIDTH].* = result;
    }

    while (i < n) : (i += 1) {
        out[i] = if (a[i] != 0 and b[i] != 0) 1 else 0;
    }
}

// ============================================================================
// Product Horizontal (Fold with Multiplication)
// ============================================================================

/// Product of two columns element-wise: out[i] = a[i] * b[i]
pub fn productHorizontal2(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = va * vb;
    }

    while (i < n) : (i += 1) {
        out[i] = a[i] * b[i];
    }
}

/// Product of three columns element-wise
pub fn productHorizontal3(comptime T: type, a: []const T, b: []const T, c: []const T, out: []T) void {
    const n = @min(@min(a.len, b.len), c.len);
    if (n == 0 or out.len < n) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const aligned_len = n - (n % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const va: Vec = a[i..][0..VECTOR_WIDTH].*;
        const vb: Vec = b[i..][0..VECTOR_WIDTH].*;
        const vc: Vec = c[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = va * vb * vc;
    }

    while (i < n) : (i += 1) {
        out[i] = a[i] * b[i] * c[i];
    }
}

// ============================================================================
// Count Non-Null Horizontal
// ============================================================================

/// Count non-NaN values across columns for each row
pub fn countNonNullHorizontal2(a: []const f64, b: []const f64, out: []u32) void {
    const n = @min(a.len, b.len);
    if (n == 0 or out.len < n) return;

    for (0..n) |i| {
        var count: u32 = 0;
        if (!std.math.isNan(a[i])) count += 1;
        if (!std.math.isNan(b[i])) count += 1;
        out[i] = count;
    }
}

/// Count non-NaN values across three columns for each row
pub fn countNonNullHorizontal3(a: []const f64, b: []const f64, c: []const f64, out: []u32) void {
    const n = @min(@min(a.len, b.len), c.len);
    if (n == 0 or out.len < n) return;

    for (0..n) |i| {
        var count: u32 = 0;
        if (!std.math.isNan(a[i])) count += 1;
        if (!std.math.isNan(b[i])) count += 1;
        if (!std.math.isNan(c[i])) count += 1;
        out[i] = count;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "fold - sumHorizontal2" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const b = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };
    var out: [5]f64 = undefined;

    sumHorizontal2(f64, &a, &b, &out);

    const expected = [_]f64{ 11.0, 22.0, 33.0, 44.0, 55.0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - minHorizontal2" {
    const a = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };
    const b = [_]f64{ 3.0, 7.0, 4.0, 6.0, 2.0 };
    var out: [5]f64 = undefined;

    minHorizontal2(f64, &a, &b, &out);

    const expected = [_]f64{ 3.0, 2.0, 4.0, 1.0, 2.0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - maxHorizontal2" {
    const a = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };
    const b = [_]f64{ 3.0, 7.0, 4.0, 6.0, 2.0 };
    var out: [5]f64 = undefined;

    maxHorizontal2(f64, &a, &b, &out);

    const expected = [_]f64{ 5.0, 7.0, 8.0, 6.0, 9.0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - sumHorizontal3" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const b = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };
    const c = [_]f64{ 100.0, 200.0, 300.0, 400.0, 500.0 };
    var out: [5]f64 = undefined;

    sumHorizontal3(f64, &a, &b, &c, &out);

    const expected = [_]f64{ 111.0, 222.0, 333.0, 444.0, 555.0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - productHorizontal2" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const b = [_]f64{ 2.0, 3.0, 4.0, 5.0, 6.0 };
    var out: [5]f64 = undefined;

    productHorizontal2(f64, &a, &b, &out);

    const expected = [_]f64{ 2.0, 6.0, 12.0, 20.0, 30.0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - anyHorizontal2" {
    const a = [_]u8{ 0, 1, 0, 1, 0 };
    const b = [_]u8{ 0, 0, 1, 1, 0 };
    var out: [5]u8 = undefined;

    anyHorizontal2(&a, &b, &out);

    const expected = [_]u8{ 0, 1, 1, 1, 0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - allHorizontal2" {
    const a = [_]u8{ 0, 1, 0, 1, 0 };
    const b = [_]u8{ 0, 0, 1, 1, 0 };
    var out: [5]u8 = undefined;

    allHorizontal2(&a, &b, &out);

    const expected = [_]u8{ 0, 0, 0, 1, 0 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - countNonNullHorizontal2" {
    const nan = std.math.nan(f64);
    const a = [_]f64{ 1.0, nan, 3.0, nan, 5.0 };
    const b = [_]f64{ nan, 2.0, nan, 4.0, 5.0 };
    var out: [5]u32 = undefined;

    countNonNullHorizontal2(&a, &b, &out);

    const expected = [_]u32{ 1, 1, 1, 1, 2 };
    for (0..5) |i| {
        try std.testing.expectEqual(expected[i], out[i]);
    }
}

test "fold - sumHorizontalN large" {
    const allocator = std.testing.allocator;

    // Create test data
    var cols: [4][]f64 = undefined;
    for (0..4) |c| {
        cols[c] = try allocator.alloc(f64, 100);
        for (0..100) |i| {
            cols[c][i] = @as(f64, @floatFromInt(c + 1)) * @as(f64, @floatFromInt(i));
        }
    }
    defer {
        for (0..4) |c| {
            allocator.free(cols[c]);
        }
    }

    const out = try allocator.alloc(f64, 100);
    defer allocator.free(out);

    // Convert to const slices for the function call
    var const_cols: [4][]const f64 = undefined;
    for (0..4) |c| {
        const_cols[c] = cols[c];
    }

    sumHorizontalN(f64, &const_cols, out);

    // Expected: (1 + 2 + 3 + 4) * i = 10 * i
    for (0..100) |i| {
        const expected: f64 = 10.0 * @as(f64, @floatFromInt(i));
        try std.testing.expectApproxEqAbs(expected, out[i], 0.0001);
    }
}

test "fold - meanHorizontalN" {
    const allocator = std.testing.allocator;

    var cols: [4][]f64 = undefined;
    for (0..4) |c| {
        cols[c] = try allocator.alloc(f64, 10);
        for (0..10) |i| {
            cols[c][i] = @as(f64, @floatFromInt(c + 1));
        }
    }
    defer {
        for (0..4) |c| {
            allocator.free(cols[c]);
        }
    }

    const out = try allocator.alloc(f64, 10);
    defer allocator.free(out);

    var const_cols: [4][]const f64 = undefined;
    for (0..4) |c| {
        const_cols[c] = cols[c];
    }

    meanHorizontalN(f64, &const_cols, out);

    // Mean of 1, 2, 3, 4 = 2.5
    for (0..10) |i| {
        try std.testing.expectApproxEqAbs(@as(f64, 2.5), out[i], 0.0001);
    }
}
