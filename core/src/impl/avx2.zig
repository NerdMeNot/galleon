//! AVX2 Implementation (256-bit vectors)
//!
//! This implementation uses 256-bit vectors for 2x throughput over SSE4.
//! Requires AVX2 + FMA CPU features.
//!
//! Vector widths:
//! - f64: 4 elements per vector
//! - f32: 8 elements per vector
//! - i64: 4 elements per vector
//! - i32: 8 elements per vector

const std = @import("std");

// ============================================================================
// Configuration
// ============================================================================

const VECTOR_WIDTH_F64 = 4; // 256 bits / 64 bits
const VECTOR_WIDTH_F32 = 8; // 256 bits / 32 bits
const VECTOR_WIDTH_I64 = 4; // 256 bits / 64 bits
const VECTOR_WIDTH_I32 = 8; // 256 bits / 32 bits

const UNROLL_FACTOR = 4; // Process 4 vectors per iteration for ILP

// Vector types
const VecF64 = @Vector(VECTOR_WIDTH_F64, f64);
const VecF32 = @Vector(VECTOR_WIDTH_F32, f32);
const VecI64 = @Vector(VECTOR_WIDTH_I64, i64);
const VecI32 = @Vector(VECTOR_WIDTH_I32, i32);

// ============================================================================
// Aggregation Implementations
// ============================================================================

fn sumF64Impl(data: [*]const f64, len: usize) callconv(.c) f64 {
    if (len == 0) return 0;

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;

    var acc0: VecF64 = @splat(0);
    var acc1: VecF64 = @splat(0);
    var acc2: VecF64 = @splat(0);
    var acc3: VecF64 = @splat(0);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF64 = slice[i..][0..VECTOR_WIDTH_F64].*;
        const v1: VecF64 = slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const v2: VecF64 = slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const v3: VecF64 = slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        acc0 += v0;
        acc1 += v1;
        acc2 += v2;
        acc3 += v3;
    }

    var result = @reduce(.Add, acc0 + acc1 + acc2 + acc3);

    while (i < len) : (i += 1) {
        result += slice[i];
    }

    return result;
}

fn sumF32Impl(data: [*]const f32, len: usize) callconv(.c) f32 {
    if (len == 0) return 0;

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;

    var acc0: VecF32 = @splat(0);
    var acc1: VecF32 = @splat(0);
    var acc2: VecF32 = @splat(0);
    var acc3: VecF32 = @splat(0);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF32 = slice[i..][0..VECTOR_WIDTH_F32].*;
        const v1: VecF32 = slice[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const v2: VecF32 = slice[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const v3: VecF32 = slice[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;
        acc0 += v0;
        acc1 += v1;
        acc2 += v2;
        acc3 += v3;
    }

    var result = @reduce(.Add, acc0 + acc1 + acc2 + acc3);

    while (i < len) : (i += 1) {
        result += slice[i];
    }

    return result;
}

fn sumI64Impl(data: [*]const i64, len: usize) callconv(.c) i64 {
    if (len == 0) return 0;

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;

    var acc0: VecI64 = @splat(0);
    var acc1: VecI64 = @splat(0);
    var acc2: VecI64 = @splat(0);
    var acc3: VecI64 = @splat(0);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI64 = slice[i..][0..VECTOR_WIDTH_I64].*;
        const v1: VecI64 = slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const v2: VecI64 = slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const v3: VecI64 = slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;
        acc0 +%= v0;
        acc1 +%= v1;
        acc2 +%= v2;
        acc3 +%= v3;
    }

    var result = @reduce(.Add, acc0 +% acc1 +% acc2 +% acc3);

    while (i < len) : (i += 1) {
        result +%= slice[i];
    }

    return result;
}

fn sumI32Impl(data: [*]const i32, len: usize) callconv(.c) i32 {
    if (len == 0) return 0;

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I32 * UNROLL_FACTOR;

    var acc0: VecI32 = @splat(0);
    var acc1: VecI32 = @splat(0);
    var acc2: VecI32 = @splat(0);
    var acc3: VecI32 = @splat(0);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI32 = slice[i..][0..VECTOR_WIDTH_I32].*;
        const v1: VecI32 = slice[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const v2: VecI32 = slice[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const v3: VecI32 = slice[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;
        acc0 +%= v0;
        acc1 +%= v1;
        acc2 +%= v2;
        acc3 +%= v3;
    }

    var result = @reduce(.Add, acc0 +% acc1 +% acc2 +% acc3);

    while (i < len) : (i += 1) {
        result +%= slice[i];
    }

    return result;
}

// --- Min/Max ---

fn minF64Impl(data: [*]const f64, len: usize, valid: *bool) callconv(.c) f64 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    const init_val = slice[0];

    var min0: VecF64 = @splat(init_val);
    var min1: VecF64 = @splat(init_val);
    var min2: VecF64 = @splat(init_val);
    var min3: VecF64 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF64 = slice[i..][0..VECTOR_WIDTH_F64].*;
        const v1: VecF64 = slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const v2: VecF64 = slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const v3: VecF64 = slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        min0 = @min(min0, v0);
        min1 = @min(min1, v1);
        min2 = @min(min2, v2);
        min3 = @min(min3, v3);
    }

    var result = @reduce(.Min, @min(@min(min0, min1), @min(min2, min3)));

    while (i < len) : (i += 1) {
        if (slice[i] < result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn maxF64Impl(data: [*]const f64, len: usize, valid: *bool) callconv(.c) f64 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    const init_val = slice[0];

    var max0: VecF64 = @splat(init_val);
    var max1: VecF64 = @splat(init_val);
    var max2: VecF64 = @splat(init_val);
    var max3: VecF64 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF64 = slice[i..][0..VECTOR_WIDTH_F64].*;
        const v1: VecF64 = slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const v2: VecF64 = slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const v3: VecF64 = slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        max0 = @max(max0, v0);
        max1 = @max(max1, v1);
        max2 = @max(max2, v2);
        max3 = @max(max3, v3);
    }

    var result = @reduce(.Max, @max(@max(max0, max1), @max(max2, max3)));

    while (i < len) : (i += 1) {
        if (slice[i] > result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn minF32Impl(data: [*]const f32, len: usize, valid: *bool) callconv(.c) f32 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;
    const init_val = slice[0];

    var min0: VecF32 = @splat(init_val);
    var min1: VecF32 = @splat(init_val);
    var min2: VecF32 = @splat(init_val);
    var min3: VecF32 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF32 = slice[i..][0..VECTOR_WIDTH_F32].*;
        const v1: VecF32 = slice[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const v2: VecF32 = slice[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const v3: VecF32 = slice[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;
        min0 = @min(min0, v0);
        min1 = @min(min1, v1);
        min2 = @min(min2, v2);
        min3 = @min(min3, v3);
    }

    var result = @reduce(.Min, @min(@min(min0, min1), @min(min2, min3)));

    while (i < len) : (i += 1) {
        if (slice[i] < result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn maxF32Impl(data: [*]const f32, len: usize, valid: *bool) callconv(.c) f32 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;
    const init_val = slice[0];

    var max0: VecF32 = @splat(init_val);
    var max1: VecF32 = @splat(init_val);
    var max2: VecF32 = @splat(init_val);
    var max3: VecF32 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF32 = slice[i..][0..VECTOR_WIDTH_F32].*;
        const v1: VecF32 = slice[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const v2: VecF32 = slice[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const v3: VecF32 = slice[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;
        max0 = @max(max0, v0);
        max1 = @max(max1, v1);
        max2 = @max(max2, v2);
        max3 = @max(max3, v3);
    }

    var result = @reduce(.Max, @max(@max(max0, max1), @max(max2, max3)));

    while (i < len) : (i += 1) {
        if (slice[i] > result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn minI64Impl(data: [*]const i64, len: usize, valid: *bool) callconv(.c) i64 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    const init_val = slice[0];

    var min0: VecI64 = @splat(init_val);
    var min1: VecI64 = @splat(init_val);
    var min2: VecI64 = @splat(init_val);
    var min3: VecI64 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI64 = slice[i..][0..VECTOR_WIDTH_I64].*;
        const v1: VecI64 = slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const v2: VecI64 = slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const v3: VecI64 = slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;
        min0 = @min(min0, v0);
        min1 = @min(min1, v1);
        min2 = @min(min2, v2);
        min3 = @min(min3, v3);
    }

    var result = @reduce(.Min, @min(@min(min0, min1), @min(min2, min3)));

    while (i < len) : (i += 1) {
        if (slice[i] < result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn maxI64Impl(data: [*]const i64, len: usize, valid: *bool) callconv(.c) i64 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    const init_val = slice[0];

    var max0: VecI64 = @splat(init_val);
    var max1: VecI64 = @splat(init_val);
    var max2: VecI64 = @splat(init_val);
    var max3: VecI64 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI64 = slice[i..][0..VECTOR_WIDTH_I64].*;
        const v1: VecI64 = slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const v2: VecI64 = slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const v3: VecI64 = slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;
        max0 = @max(max0, v0);
        max1 = @max(max1, v1);
        max2 = @max(max2, v2);
        max3 = @max(max3, v3);
    }

    var result = @reduce(.Max, @max(@max(max0, max1), @max(max2, max3)));

    while (i < len) : (i += 1) {
        if (slice[i] > result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn minI32Impl(data: [*]const i32, len: usize, valid: *bool) callconv(.c) i32 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I32 * UNROLL_FACTOR;
    const init_val = slice[0];

    var min0: VecI32 = @splat(init_val);
    var min1: VecI32 = @splat(init_val);
    var min2: VecI32 = @splat(init_val);
    var min3: VecI32 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI32 = slice[i..][0..VECTOR_WIDTH_I32].*;
        const v1: VecI32 = slice[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const v2: VecI32 = slice[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const v3: VecI32 = slice[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;
        min0 = @min(min0, v0);
        min1 = @min(min1, v1);
        min2 = @min(min2, v2);
        min3 = @min(min3, v3);
    }

    var result = @reduce(.Min, @min(@min(min0, min1), @min(min2, min3)));

    while (i < len) : (i += 1) {
        if (slice[i] < result) result = slice[i];
    }

    valid.* = true;
    return result;
}

fn maxI32Impl(data: [*]const i32, len: usize, valid: *bool) callconv(.c) i32 {
    if (len == 0) {
        valid.* = false;
        return 0;
    }

    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I32 * UNROLL_FACTOR;
    const init_val = slice[0];

    var max0: VecI32 = @splat(init_val);
    var max1: VecI32 = @splat(init_val);
    var max2: VecI32 = @splat(init_val);
    var max3: VecI32 = @splat(init_val);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI32 = slice[i..][0..VECTOR_WIDTH_I32].*;
        const v1: VecI32 = slice[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const v2: VecI32 = slice[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const v3: VecI32 = slice[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;
        max0 = @max(max0, v0);
        max1 = @max(max1, v1);
        max2 = @max(max2, v2);
        max3 = @max(max3, v3);
    }

    var result = @reduce(.Max, @max(@max(max0, max1), @max(max2, max3)));

    while (i < len) : (i += 1) {
        if (slice[i] > result) result = slice[i];
    }

    valid.* = true;
    return result;
}

// ============================================================================
// Element-wise Arithmetic
// ============================================================================

fn addF64Impl(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF64 = a[i..][0..VECTOR_WIDTH_F64].*;
        const a1: VecF64 = a[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const a2: VecF64 = a[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const a3: VecF64 = a[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        const b0: VecF64 = b[i..][0..VECTOR_WIDTH_F64].*;
        const b1: VecF64 = b[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const b2: VecF64 = b[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const b3: VecF64 = b[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        out[i..][0..VECTOR_WIDTH_F64].* = a0 + b0;
        out[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = a1 + b1;
        out[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = a2 + b2;
        out[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = a3 + b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] + b[i];
    }
}

fn subF64Impl(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF64 = a[i..][0..VECTOR_WIDTH_F64].*;
        const a1: VecF64 = a[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const a2: VecF64 = a[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const a3: VecF64 = a[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        const b0: VecF64 = b[i..][0..VECTOR_WIDTH_F64].*;
        const b1: VecF64 = b[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const b2: VecF64 = b[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const b3: VecF64 = b[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        out[i..][0..VECTOR_WIDTH_F64].* = a0 - b0;
        out[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = a1 - b1;
        out[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = a2 - b2;
        out[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = a3 - b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] - b[i];
    }
}

fn mulF64Impl(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF64 = a[i..][0..VECTOR_WIDTH_F64].*;
        const a1: VecF64 = a[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const a2: VecF64 = a[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const a3: VecF64 = a[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        const b0: VecF64 = b[i..][0..VECTOR_WIDTH_F64].*;
        const b1: VecF64 = b[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const b2: VecF64 = b[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const b3: VecF64 = b[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        out[i..][0..VECTOR_WIDTH_F64].* = a0 * b0;
        out[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = a1 * b1;
        out[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = a2 * b2;
        out[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = a3 * b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] * b[i];
    }
}

fn divF64Impl(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF64 = a[i..][0..VECTOR_WIDTH_F64].*;
        const a1: VecF64 = a[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const a2: VecF64 = a[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const a3: VecF64 = a[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        const b0: VecF64 = b[i..][0..VECTOR_WIDTH_F64].*;
        const b1: VecF64 = b[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const b2: VecF64 = b[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const b3: VecF64 = b[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;

        out[i..][0..VECTOR_WIDTH_F64].* = a0 / b0;
        out[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = a1 / b1;
        out[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = a2 / b2;
        out[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = a3 / b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] / b[i];
    }
}

// f32 arithmetic
fn addF32Impl(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF32 = a[i..][0..VECTOR_WIDTH_F32].*;
        const a1: VecF32 = a[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const a2: VecF32 = a[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const a3: VecF32 = a[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        const b0: VecF32 = b[i..][0..VECTOR_WIDTH_F32].*;
        const b1: VecF32 = b[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const b2: VecF32 = b[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const b3: VecF32 = b[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        out[i..][0..VECTOR_WIDTH_F32].* = a0 + b0;
        out[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].* = a1 + b1;
        out[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].* = a2 + b2;
        out[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].* = a3 + b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] + b[i];
    }
}

fn subF32Impl(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF32 = a[i..][0..VECTOR_WIDTH_F32].*;
        const a1: VecF32 = a[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const a2: VecF32 = a[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const a3: VecF32 = a[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        const b0: VecF32 = b[i..][0..VECTOR_WIDTH_F32].*;
        const b1: VecF32 = b[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const b2: VecF32 = b[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const b3: VecF32 = b[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        out[i..][0..VECTOR_WIDTH_F32].* = a0 - b0;
        out[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].* = a1 - b1;
        out[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].* = a2 - b2;
        out[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].* = a3 - b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] - b[i];
    }
}

fn mulF32Impl(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF32 = a[i..][0..VECTOR_WIDTH_F32].*;
        const a1: VecF32 = a[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const a2: VecF32 = a[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const a3: VecF32 = a[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        const b0: VecF32 = b[i..][0..VECTOR_WIDTH_F32].*;
        const b1: VecF32 = b[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const b2: VecF32 = b[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const b3: VecF32 = b[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        out[i..][0..VECTOR_WIDTH_F32].* = a0 * b0;
        out[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].* = a1 * b1;
        out[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].* = a2 * b2;
        out[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].* = a3 * b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] * b[i];
    }
}

fn divF32Impl(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_F32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF32 = a[i..][0..VECTOR_WIDTH_F32].*;
        const a1: VecF32 = a[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const a2: VecF32 = a[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const a3: VecF32 = a[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        const b0: VecF32 = b[i..][0..VECTOR_WIDTH_F32].*;
        const b1: VecF32 = b[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].*;
        const b2: VecF32 = b[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].*;
        const b3: VecF32 = b[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].*;

        out[i..][0..VECTOR_WIDTH_F32].* = a0 / b0;
        out[i + VECTOR_WIDTH_F32 ..][0..VECTOR_WIDTH_F32].* = a1 / b1;
        out[i + VECTOR_WIDTH_F32 * 2 ..][0..VECTOR_WIDTH_F32].* = a2 / b2;
        out[i + VECTOR_WIDTH_F32 * 3 ..][0..VECTOR_WIDTH_F32].* = a3 / b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] / b[i];
    }
}

// i64 arithmetic
fn addI64Impl(a: [*]const i64, b: [*]const i64, out: [*]i64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecI64 = a[i..][0..VECTOR_WIDTH_I64].*;
        const a1: VecI64 = a[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const a2: VecI64 = a[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const a3: VecI64 = a[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;

        const b0: VecI64 = b[i..][0..VECTOR_WIDTH_I64].*;
        const b1: VecI64 = b[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const b2: VecI64 = b[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const b3: VecI64 = b[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;

        out[i..][0..VECTOR_WIDTH_I64].* = a0 +% b0;
        out[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].* = a1 +% b1;
        out[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].* = a2 +% b2;
        out[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].* = a3 +% b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] +% b[i];
    }
}

fn subI64Impl(a: [*]const i64, b: [*]const i64, out: [*]i64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecI64 = a[i..][0..VECTOR_WIDTH_I64].*;
        const a1: VecI64 = a[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const a2: VecI64 = a[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const a3: VecI64 = a[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;

        const b0: VecI64 = b[i..][0..VECTOR_WIDTH_I64].*;
        const b1: VecI64 = b[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const b2: VecI64 = b[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const b3: VecI64 = b[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;

        out[i..][0..VECTOR_WIDTH_I64].* = a0 -% b0;
        out[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].* = a1 -% b1;
        out[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].* = a2 -% b2;
        out[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].* = a3 -% b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] -% b[i];
    }
}

fn mulI64Impl(a: [*]const i64, b: [*]const i64, out: [*]i64, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecI64 = a[i..][0..VECTOR_WIDTH_I64].*;
        const a1: VecI64 = a[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const a2: VecI64 = a[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const a3: VecI64 = a[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;

        const b0: VecI64 = b[i..][0..VECTOR_WIDTH_I64].*;
        const b1: VecI64 = b[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const b2: VecI64 = b[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const b3: VecI64 = b[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;

        out[i..][0..VECTOR_WIDTH_I64].* = a0 *% b0;
        out[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].* = a1 *% b1;
        out[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].* = a2 *% b2;
        out[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].* = a3 *% b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] *% b[i];
    }
}

// i32 arithmetic
fn addI32Impl(a: [*]const i32, b: [*]const i32, out: [*]i32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_I32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecI32 = a[i..][0..VECTOR_WIDTH_I32].*;
        const a1: VecI32 = a[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const a2: VecI32 = a[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const a3: VecI32 = a[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;

        const b0: VecI32 = b[i..][0..VECTOR_WIDTH_I32].*;
        const b1: VecI32 = b[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const b2: VecI32 = b[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const b3: VecI32 = b[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;

        out[i..][0..VECTOR_WIDTH_I32].* = a0 +% b0;
        out[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].* = a1 +% b1;
        out[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].* = a2 +% b2;
        out[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].* = a3 +% b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] +% b[i];
    }
}

fn subI32Impl(a: [*]const i32, b: [*]const i32, out: [*]i32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_I32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecI32 = a[i..][0..VECTOR_WIDTH_I32].*;
        const a1: VecI32 = a[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const a2: VecI32 = a[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const a3: VecI32 = a[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;

        const b0: VecI32 = b[i..][0..VECTOR_WIDTH_I32].*;
        const b1: VecI32 = b[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const b2: VecI32 = b[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const b3: VecI32 = b[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;

        out[i..][0..VECTOR_WIDTH_I32].* = a0 -% b0;
        out[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].* = a1 -% b1;
        out[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].* = a2 -% b2;
        out[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].* = a3 -% b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] -% b[i];
    }
}

fn mulI32Impl(a: [*]const i32, b: [*]const i32, out: [*]i32, len: usize) callconv(.c) void {
    const CHUNK = VECTOR_WIDTH_I32 * UNROLL_FACTOR;
    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecI32 = a[i..][0..VECTOR_WIDTH_I32].*;
        const a1: VecI32 = a[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const a2: VecI32 = a[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const a3: VecI32 = a[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;

        const b0: VecI32 = b[i..][0..VECTOR_WIDTH_I32].*;
        const b1: VecI32 = b[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].*;
        const b2: VecI32 = b[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].*;
        const b3: VecI32 = b[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].*;

        out[i..][0..VECTOR_WIDTH_I32].* = a0 *% b0;
        out[i + VECTOR_WIDTH_I32 ..][0..VECTOR_WIDTH_I32].* = a1 *% b1;
        out[i + VECTOR_WIDTH_I32 * 2 ..][0..VECTOR_WIDTH_I32].* = a2 *% b2;
        out[i + VECTOR_WIDTH_I32 * 3 ..][0..VECTOR_WIDTH_I32].* = a3 *% b3;
    }

    while (i < len) : (i += 1) {
        out[i] = a[i] *% b[i];
    }
}

// Scalar operations
fn addScalarF64Impl(data: [*]f64, len: usize, scalar: f64) callconv(.c) void {
    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    const scalar_vec: VecF64 = @splat(scalar);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF64 = slice[i..][0..VECTOR_WIDTH_F64].*;
        const v1: VecF64 = slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const v2: VecF64 = slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const v3: VecF64 = slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        slice[i..][0..VECTOR_WIDTH_F64].* = v0 + scalar_vec;
        slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = v1 + scalar_vec;
        slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = v2 + scalar_vec;
        slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = v3 + scalar_vec;
    }

    while (i < len) : (i += 1) {
        slice[i] += scalar;
    }
}

fn mulScalarF64Impl(data: [*]f64, len: usize, scalar: f64) callconv(.c) void {
    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;
    const scalar_vec: VecF64 = @splat(scalar);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecF64 = slice[i..][0..VECTOR_WIDTH_F64].*;
        const v1: VecF64 = slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const v2: VecF64 = slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const v3: VecF64 = slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        slice[i..][0..VECTOR_WIDTH_F64].* = v0 * scalar_vec;
        slice[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = v1 * scalar_vec;
        slice[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = v2 * scalar_vec;
        slice[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = v3 * scalar_vec;
    }

    while (i < len) : (i += 1) {
        slice[i] *= scalar;
    }
}

fn addScalarI64Impl(data: [*]i64, len: usize, scalar: i64) callconv(.c) void {
    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    const scalar_vec: VecI64 = @splat(scalar);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI64 = slice[i..][0..VECTOR_WIDTH_I64].*;
        const v1: VecI64 = slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const v2: VecI64 = slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const v3: VecI64 = slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;
        slice[i..][0..VECTOR_WIDTH_I64].* = v0 +% scalar_vec;
        slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].* = v1 +% scalar_vec;
        slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].* = v2 +% scalar_vec;
        slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].* = v3 +% scalar_vec;
    }

    while (i < len) : (i += 1) {
        slice[i] +%= scalar;
    }
}

fn mulScalarI64Impl(data: [*]i64, len: usize, scalar: i64) callconv(.c) void {
    const slice = data[0..len];
    const CHUNK = VECTOR_WIDTH_I64 * UNROLL_FACTOR;
    const scalar_vec: VecI64 = @splat(scalar);

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const v0: VecI64 = slice[i..][0..VECTOR_WIDTH_I64].*;
        const v1: VecI64 = slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].*;
        const v2: VecI64 = slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].*;
        const v3: VecI64 = slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].*;
        slice[i..][0..VECTOR_WIDTH_I64].* = v0 *% scalar_vec;
        slice[i + VECTOR_WIDTH_I64 ..][0..VECTOR_WIDTH_I64].* = v1 *% scalar_vec;
        slice[i + VECTOR_WIDTH_I64 * 2 ..][0..VECTOR_WIDTH_I64].* = v2 *% scalar_vec;
        slice[i + VECTOR_WIDTH_I64 * 3 ..][0..VECTOR_WIDTH_I64].* = v3 *% scalar_vec;
    }

    while (i < len) : (i += 1) {
        slice[i] *%= scalar;
    }
}

// ============================================================================
// Delegate to SSE4 for operations that don't benefit from wider vectors
// ============================================================================

const sse4 = @import("sse4.zig");

// ============================================================================
// Dispatch Table Export
// ============================================================================

const dispatch_mod = @import("../dispatch.zig");

pub const dispatch_table: dispatch_mod.DispatchTable = .{
    // Aggregations - use AVX2 implementations
    .sum_f64 = sumF64Impl,
    .sum_f32 = sumF32Impl,
    .sum_i64 = sumI64Impl,
    .sum_i32 = sumI32Impl,

    .min_f64 = minF64Impl,
    .max_f64 = maxF64Impl,
    .min_f32 = minF32Impl,
    .max_f32 = maxF32Impl,
    .min_i64 = minI64Impl,
    .max_i64 = maxI64Impl,
    .min_i32 = minI32Impl,
    .max_i32 = maxI32Impl,

    // Element-wise arithmetic - use AVX2 implementations
    .add_f64 = addF64Impl,
    .sub_f64 = subF64Impl,
    .mul_f64 = mulF64Impl,
    .div_f64 = divF64Impl,

    .add_f32 = addF32Impl,
    .sub_f32 = subF32Impl,
    .mul_f32 = mulF32Impl,
    .div_f32 = divF32Impl,

    .add_i64 = addI64Impl,
    .sub_i64 = subI64Impl,
    .mul_i64 = mulI64Impl,

    .add_i32 = addI32Impl,
    .sub_i32 = subI32Impl,
    .mul_i32 = mulI32Impl,

    .add_scalar_f64 = addScalarF64Impl,
    .mul_scalar_f64 = mulScalarF64Impl,
    .add_scalar_i64 = addScalarI64Impl,
    .mul_scalar_i64 = mulScalarI64Impl,

    // Delegate to SSE4 for operations that don't benefit from wider vectors
    .cmp_gt_f64 = sse4.dispatch_table.cmp_gt_f64,
    .cmp_ge_f64 = sse4.dispatch_table.cmp_ge_f64,
    .cmp_lt_f64 = sse4.dispatch_table.cmp_lt_f64,
    .cmp_le_f64 = sse4.dispatch_table.cmp_le_f64,
    .cmp_eq_f64 = sse4.dispatch_table.cmp_eq_f64,
    .cmp_ne_f64 = sse4.dispatch_table.cmp_ne_f64,

    .cmp_gt_i64 = sse4.dispatch_table.cmp_gt_i64,
    .cmp_ge_i64 = sse4.dispatch_table.cmp_ge_i64,
    .cmp_lt_i64 = sse4.dispatch_table.cmp_lt_i64,
    .cmp_le_i64 = sse4.dispatch_table.cmp_le_i64,
    .cmp_eq_i64 = sse4.dispatch_table.cmp_eq_i64,
    .cmp_ne_i64 = sse4.dispatch_table.cmp_ne_i64,

    .filter_gt_f64 = sse4.dispatch_table.filter_gt_f64,
    .filter_mask_gt_f64 = sse4.dispatch_table.filter_mask_gt_f64,
    .filter_gt_i64 = sse4.dispatch_table.filter_gt_i64,
    .filter_mask_gt_i64 = sse4.dispatch_table.filter_mask_gt_i64,

    .hash_i64 = sse4.dispatch_table.hash_i64,
    .hash_i32 = sse4.dispatch_table.hash_i32,
    .hash_f64 = sse4.dispatch_table.hash_f64,
    .hash_f32 = sse4.dispatch_table.hash_f32,
    .combine_hashes = sse4.dispatch_table.combine_hashes,

    .gather_f64 = sse4.dispatch_table.gather_f64,
    .gather_f32 = sse4.dispatch_table.gather_f32,
    .gather_i64 = sse4.dispatch_table.gather_i64,
    .gather_i32 = sse4.dispatch_table.gather_i32,

    .agg_sum_by_group_f64 = sse4.dispatch_table.agg_sum_by_group_f64,
    .agg_sum_by_group_i64 = sse4.dispatch_table.agg_sum_by_group_i64,
    .agg_min_by_group_f64 = sse4.dispatch_table.agg_min_by_group_f64,
    .agg_max_by_group_f64 = sse4.dispatch_table.agg_max_by_group_f64,
    .count_by_group = sse4.dispatch_table.count_by_group,
};

// ============================================================================
// Tests
// ============================================================================

test "avx2 - sum f64" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0 };
    const result = sumF64Impl(&data, data.len);
    try std.testing.expectApproxEqAbs(@as(f64, 153.0), result, 0.0001);
}

test "avx2 - add f64" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    const b = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0 };
    var out: [8]f64 = undefined;
    addF64Impl(&a, &b, &out, 8);
    try std.testing.expectApproxEqAbs(@as(f64, 11.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 88.0), out[7], 0.0001);
}

test "avx2 - min max f64" {
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0, 0.5 };
    var valid: bool = undefined;

    const min_val = minF64Impl(&data, data.len, &valid);
    try std.testing.expect(valid);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), min_val, 0.0001);

    const max_val = maxF64Impl(&data, data.len, &valid);
    try std.testing.expect(valid);
    try std.testing.expectApproxEqAbs(@as(f64, 9.0), max_val, 0.0001);
}
