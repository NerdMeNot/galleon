//! Element-wise Arithmetic Operations
//!
//! SIMD-accelerated arithmetic operations on arrays.
//! Includes both scalar-array and array-array operations.
//!
//! Scalar operations (in-place):
//! - addScalar: array[i] += scalar
//! - mulScalar: array[i] *= scalar
//!
//! Array operations (out-of-place):
//! - addArrays: out[i] = a[i] + b[i]
//! - subArrays: out[i] = a[i] - b[i]
//! - mulArrays: out[i] = a[i] * b[i]
//! - divArrays: out[i] = a[i] / b[i]
//!
//! All functions are generic over numeric types (f64, f32, i64, i32, etc.)

const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Scalar Operations (in-place)
// ============================================================================

/// Add a scalar to all elements in place using SIMD with loop unrolling
pub fn addScalar(comptime T: type, data: []T, scalar: T) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const scalar_vec: Vec = @splat(scalar);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        data[i..][0..VECTOR_WIDTH].* = chunk0 + scalar_vec;
        data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = chunk1 + scalar_vec;
        data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = chunk2 + scalar_vec;
        data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = chunk3 + scalar_vec;
    }

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        data[i..][0..VECTOR_WIDTH].* = chunk + scalar_vec;
    }

    while (i < data.len) : (i += 1) {
        data[i] += scalar;
    }
}

/// Multiply all elements by a scalar in place using SIMD with loop unrolling
pub fn mulScalar(comptime T: type, data: []T, scalar: T) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const scalar_vec: Vec = @splat(scalar);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        data[i..][0..VECTOR_WIDTH].* = chunk0 * scalar_vec;
        data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = chunk1 * scalar_vec;
        data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = chunk2 * scalar_vec;
        data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = chunk3 * scalar_vec;
    }

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        data[i..][0..VECTOR_WIDTH].* = chunk * scalar_vec;
    }

    while (i < data.len) : (i += 1) {
        data[i] *= scalar;
    }
}

// ============================================================================
// Array Operations
// ============================================================================

/// Add two arrays element-wise (dst += src) with loop unrolling
pub fn addArrays(comptime T: type, dst: []T, src: []const T) void {
    const len = @min(dst.len, src.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const d0: Vec = dst[i..][0..VECTOR_WIDTH].*;
        const d1: Vec = dst[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const d2: Vec = dst[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const d3: Vec = dst[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const s0: Vec = src[i..][0..VECTOR_WIDTH].*;
        const s1: Vec = src[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const s2: Vec = src[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const s3: Vec = src[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        dst[i..][0..VECTOR_WIDTH].* = d0 + s0;
        dst[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = d1 + s1;
        dst[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = d2 + s2;
        dst[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = d3 + s3;
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const d: Vec = dst[i..][0..VECTOR_WIDTH].*;
        const s: Vec = src[i..][0..VECTOR_WIDTH].*;
        dst[i..][0..VECTOR_WIDTH].* = d + s;
    }

    while (i < len) : (i += 1) {
        dst[i] += src[i];
    }
}

/// Add two arrays element-wise (out = a + b) with SIMD - output version
pub fn addArraysOut(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        out[i..][0..VECTOR_WIDTH].* = a0 + b0;
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = a1 + b1;
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = a2 + b2;
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = a3 + b3;
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = av + bv;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] + b[i];
    }
}

/// Subtract two arrays element-wise (out = a - b) with SIMD
pub fn subArrays(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        out[i..][0..VECTOR_WIDTH].* = a0 - b0;
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = a1 - b1;
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = a2 - b2;
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = a3 - b3;
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = av - bv;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] - b[i];
    }
}

/// Multiply two arrays element-wise (out = a * b) with SIMD
pub fn mulArrays(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        out[i..][0..VECTOR_WIDTH].* = a0 * b0;
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = a1 * b1;
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = a2 * b2;
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = a3 * b3;
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = av * bv;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] * b[i];
    }
}

/// Divide two arrays element-wise (out = a / b) with SIMD
pub fn divArrays(comptime T: type, a: []const T, b: []const T, out: []T) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        out[i..][0..VECTOR_WIDTH].* = a0 / b0;
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = a1 / b1;
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = a2 / b2;
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = a3 / b3;
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        out[i..][0..VECTOR_WIDTH].* = av / bv;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] / b[i];
    }
}

// ============================================================================
// Integer Scalar Operations
// ============================================================================

/// Add a scalar to all integer elements in place using SIMD
pub fn addScalarInt(comptime T: type, data: []T, scalar: T) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const scalar_vec: Vec = @splat(scalar);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        data[i..][0..VECTOR_WIDTH].* = chunk0 +% scalar_vec;
        data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = chunk1 +% scalar_vec;
        data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = chunk2 +% scalar_vec;
        data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = chunk3 +% scalar_vec;
    }

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        data[i..][0..VECTOR_WIDTH].* = chunk +% scalar_vec;
    }

    while (i < data.len) : (i += 1) {
        data[i] +%= scalar;
    }
}

/// Multiply all integer elements by a scalar in place using SIMD
pub fn mulScalarInt(comptime T: type, data: []T, scalar: T) void {
    if (data.len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const scalar_vec: Vec = @splat(scalar);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        data[i..][0..VECTOR_WIDTH].* = chunk0 *% scalar_vec;
        data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = chunk1 *% scalar_vec;
        data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = chunk2 *% scalar_vec;
        data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = chunk3 *% scalar_vec;
    }

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        data[i..][0..VECTOR_WIDTH].* = chunk *% scalar_vec;
    }

    while (i < data.len) : (i += 1) {
        data[i] *%= scalar;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "arithmetic - addScalar f64" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    addScalar(f64, &data, 10.0);
    try std.testing.expectApproxEqAbs(@as(f64, 11.0), data[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), data[4], 0.0001);
}

test "arithmetic - mulScalar f64" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    mulScalar(f64, &data, 2.0);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), data[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), data[4], 0.0001);
}

test "arithmetic - addArraysOut f64" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const b = [_]f64{ 5.0, 6.0, 7.0, 8.0 };
    var out: [4]f64 = undefined;
    addArraysOut(f64, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f64, 6.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 12.0), out[3], 0.0001);
}

test "arithmetic - subArrays f64" {
    const a = [_]f64{ 10.0, 20.0, 30.0, 40.0 };
    const b = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    var out: [4]f64 = undefined;
    subArrays(f64, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f64, 9.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 36.0), out[3], 0.0001);
}

test "arithmetic - mulArrays f64" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const b = [_]f64{ 2.0, 3.0, 4.0, 5.0 };
    var out: [4]f64 = undefined;
    mulArrays(f64, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), out[3], 0.0001);
}

test "arithmetic - divArrays f64" {
    const a = [_]f64{ 10.0, 20.0, 30.0, 40.0 };
    const b = [_]f64{ 2.0, 4.0, 5.0, 8.0 };
    var out: [4]f64 = undefined;
    divArrays(f64, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), out[3], 0.0001);
}

test "arithmetic - addScalarInt i64" {
    var data = [_]i64{ 1, 2, 3, 4, 5 };
    addScalarInt(i64, &data, 10);
    try std.testing.expectEqual(@as(i64, 11), data[0]);
    try std.testing.expectEqual(@as(i64, 15), data[4]);
}

test "arithmetic - mulScalarInt i64" {
    var data = [_]i64{ 1, 2, 3, 4, 5 };
    mulScalarInt(i64, &data, 3);
    try std.testing.expectEqual(@as(i64, 3), data[0]);
    try std.testing.expectEqual(@as(i64, 15), data[4]);
}

test "arithmetic - addArrays in-place" {
    var dst = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const src = [_]f64{ 10.0, 20.0, 30.0, 40.0 };
    addArrays(f64, &dst, &src);
    try std.testing.expectApproxEqAbs(@as(f64, 11.0), dst[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 44.0), dst[3], 0.0001);
}

// Additional tests for better coverage

test "arithmetic - addScalar f32" {
    var data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    addScalar(f32, &data, 10.0);
    try std.testing.expectApproxEqAbs(@as(f32, 11.0), data[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 15.0), data[4], 0.0001);
}

test "arithmetic - mulScalar f32" {
    var data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    mulScalar(f32, &data, 2.0);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), data[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), data[4], 0.0001);
}

test "arithmetic - addArraysOut f32" {
    const a = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const b = [_]f32{ 10.0, 20.0, 30.0, 40.0 };
    var out: [4]f32 = undefined;
    addArraysOut(f32, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 11.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 44.0), out[3], 0.0001);
}

test "arithmetic - subArrays f32" {
    const a = [_]f32{ 10.0, 20.0, 30.0, 40.0 };
    const b = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var out: [4]f32 = undefined;
    subArrays(f32, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 9.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 36.0), out[3], 0.0001);
}

test "arithmetic - mulArrays f32" {
    const a = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const b = [_]f32{ 10.0, 20.0, 30.0, 40.0 };
    var out: [4]f32 = undefined;
    mulArrays(f32, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 160.0), out[3], 0.0001);
}

test "arithmetic - divArrays f32" {
    const a = [_]f32{ 10.0, 20.0, 30.0, 40.0 };
    const b = [_]f32{ 2.0, 4.0, 5.0, 8.0 };
    var out: [4]f32 = undefined;
    divArrays(f32, &a, &b, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), out[3], 0.0001);
}

test "arithmetic - addScalarInt i32" {
    var data = [_]i32{ 1, 2, 3, 4, 5 };
    addScalarInt(i32, &data, 10);
    try std.testing.expectEqual(@as(i32, 11), data[0]);
    try std.testing.expectEqual(@as(i32, 15), data[4]);
}

test "arithmetic - mulScalarInt i32" {
    var data = [_]i32{ 1, 2, 3, 4, 5 };
    mulScalarInt(i32, &data, 3);
    try std.testing.expectEqual(@as(i32, 3), data[0]);
    try std.testing.expectEqual(@as(i32, 15), data[4]);
}

test "arithmetic - addArraysOut i64" {
    const a = [_]i64{ 1, 2, 3, 4 };
    const b = [_]i64{ 10, 20, 30, 40 };
    var out: [4]i64 = undefined;
    addArraysOut(i64, &a, &b, &out);
    try std.testing.expectEqual(@as(i64, 11), out[0]);
    try std.testing.expectEqual(@as(i64, 44), out[3]);
}

test "arithmetic - subArrays i64" {
    const a = [_]i64{ 10, 20, 30, 40 };
    const b = [_]i64{ 1, 2, 3, 4 };
    var out: [4]i64 = undefined;
    subArrays(i64, &a, &b, &out);
    try std.testing.expectEqual(@as(i64, 9), out[0]);
    try std.testing.expectEqual(@as(i64, 36), out[3]);
}

test "arithmetic - mulArrays i64" {
    const a = [_]i64{ 1, 2, 3, 4 };
    const b = [_]i64{ 10, 20, 30, 40 };
    var out: [4]i64 = undefined;
    mulArrays(i64, &a, &b, &out);
    try std.testing.expectEqual(@as(i64, 10), out[0]);
    try std.testing.expectEqual(@as(i64, 160), out[3]);
}

test "arithmetic - addArraysOut i32" {
    const a = [_]i32{ 1, 2, 3, 4 };
    const b = [_]i32{ 10, 20, 30, 40 };
    var out: [4]i32 = undefined;
    addArraysOut(i32, &a, &b, &out);
    try std.testing.expectEqual(@as(i32, 11), out[0]);
    try std.testing.expectEqual(@as(i32, 44), out[3]);
}

test "arithmetic - subArrays i32" {
    const a = [_]i32{ 10, 20, 30, 40 };
    const b = [_]i32{ 1, 2, 3, 4 };
    var out: [4]i32 = undefined;
    subArrays(i32, &a, &b, &out);
    try std.testing.expectEqual(@as(i32, 9), out[0]);
    try std.testing.expectEqual(@as(i32, 36), out[3]);
}

test "arithmetic - mulArrays i32" {
    const a = [_]i32{ 1, 2, 3, 4 };
    const b = [_]i32{ 10, 20, 30, 40 };
    var out: [4]i32 = undefined;
    mulArrays(i32, &a, &b, &out);
    try std.testing.expectEqual(@as(i32, 10), out[0]);
    try std.testing.expectEqual(@as(i32, 160), out[3]);
}

test "arithmetic - large SIMD arrays f64" {
    var a: [100]f64 = undefined;
    var b: [100]f64 = undefined;
    var out: [100]f64 = undefined;

    for (&a, &b, 0..) |*va, *vb, i| {
        va.* = @floatFromInt(i);
        vb.* = @floatFromInt(i + 1);
    }

    addArraysOut(f64, &a, &b, &out);
    // a[50] = 50, b[50] = 51, out[50] = 101
    try std.testing.expectApproxEqAbs(@as(f64, 101.0), out[50], 0.0001);
}

test "arithmetic - empty arrays" {
    var empty: [0]f64 = undefined;
    addScalar(f64, &empty, 10.0);
    mulScalar(f64, &empty, 2.0);
    // Should not crash with empty arrays
}
