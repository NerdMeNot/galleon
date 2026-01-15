//! SSE4 Implementation (128-bit vectors)
//!
//! This is the baseline SIMD implementation using 128-bit vectors.
//! It serves as the fallback for systems without AVX2 support.
//!
//! Vector widths:
//! - f64: 2 elements per vector
//! - f32: 4 elements per vector
//! - i64: 2 elements per vector
//! - i32: 4 elements per vector

const std = @import("std");

// ============================================================================
// Configuration
// ============================================================================

const VECTOR_WIDTH_F64 = 2; // 128 bits / 64 bits
const VECTOR_WIDTH_F32 = 4; // 128 bits / 32 bits
const VECTOR_WIDTH_I64 = 2; // 128 bits / 64 bits
const VECTOR_WIDTH_I32 = 4; // 128 bits / 32 bits

const UNROLL_FACTOR = 4; // Process 4 vectors per iteration for ILP

// Vector type aliases for explicit coercion
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

    // Handle remaining elements
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
// Comparison Operations
// ============================================================================

fn cmpGtF64Impl(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) callconv(.c) void {
    const BoolVec = @Vector(VECTOR_WIDTH_F64, bool);
    const CHUNK = VECTOR_WIDTH_F64 * UNROLL_FACTOR;

    var i: usize = 0;
    const unrolled_len = len - (len % CHUNK);

    while (i < unrolled_len) : (i += CHUNK) {
        const a0: VecF64 = a[i..][0..VECTOR_WIDTH_F64].*;
        const b0: VecF64 = b[i..][0..VECTOR_WIDTH_F64].*;
        const m0: BoolVec = a0 > b0;
        out[i..][0..VECTOR_WIDTH_F64].* = @intFromBool(m0);

        const a1: VecF64 = a[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const b1: VecF64 = b[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].*;
        const m1: BoolVec = a1 > b1;
        out[i + VECTOR_WIDTH_F64 ..][0..VECTOR_WIDTH_F64].* = @intFromBool(m1);

        const a2: VecF64 = a[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const b2: VecF64 = b[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].*;
        const m2: BoolVec = a2 > b2;
        out[i + VECTOR_WIDTH_F64 * 2 ..][0..VECTOR_WIDTH_F64].* = @intFromBool(m2);

        const a3: VecF64 = a[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        const b3: VecF64 = b[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].*;
        const m3: BoolVec = a3 > b3;
        out[i + VECTOR_WIDTH_F64 * 3 ..][0..VECTOR_WIDTH_F64].* = @intFromBool(m3);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] > b[i]) 1 else 0;
    }
}

fn cmpGeF64Impl(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] >= b[i]) 1 else 0;
    }
}

fn cmpLtF64Impl(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] < b[i]) 1 else 0;
    }
}

fn cmpLeF64Impl(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] <= b[i]) 1 else 0;
    }
}

fn cmpEqF64Impl(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] == b[i]) 1 else 0;
    }
}

fn cmpNeF64Impl(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] != b[i]) 1 else 0;
    }
}

fn cmpGtI64Impl(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] > b[i]) 1 else 0;
    }
}

fn cmpGeI64Impl(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] >= b[i]) 1 else 0;
    }
}

fn cmpLtI64Impl(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] < b[i]) 1 else 0;
    }
}

fn cmpLeI64Impl(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] <= b[i]) 1 else 0;
    }
}

fn cmpEqI64Impl(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] == b[i]) 1 else 0;
    }
}

fn cmpNeI64Impl(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = if (a[i] != b[i]) 1 else 0;
    }
}

// ============================================================================
// Filter Operations
// ============================================================================

fn filterGtF64Impl(data: [*]const f64, len: usize, threshold: f64, out_indices: [*]u32, out_count: *usize) callconv(.c) void {
    var count: usize = 0;
    var i: usize = 0;

    while (i < len) : (i += 1) {
        if (data[i] > threshold) {
            out_indices[count] = @intCast(i);
            count += 1;
        }
    }

    out_count.* = count;
}

fn filterMaskGtF64Impl(data: [*]const f64, len: usize, threshold: f64, out_mask: [*]u8) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out_mask[i] = if (data[i] > threshold) 1 else 0;
    }
}

fn filterGtI64Impl(data: [*]const i64, len: usize, threshold: i64, out_indices: [*]u32, out_count: *usize) callconv(.c) void {
    var count: usize = 0;
    var i: usize = 0;

    while (i < len) : (i += 1) {
        if (data[i] > threshold) {
            out_indices[count] = @intCast(i);
            count += 1;
        }
    }

    out_count.* = count;
}

fn filterMaskGtI64Impl(data: [*]const i64, len: usize, threshold: i64, out_mask: [*]u8) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out_mask[i] = if (data[i] > threshold) 1 else 0;
    }
}

// ============================================================================
// Hashing Operations
// ============================================================================

const HASH_PRIME: u64 = 0x9E3779B97F4A7C15;

fn hashI64Impl(data: [*]const i64, out: [*]u64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const v: u64 = @bitCast(data[i]);
        out[i] = v *% HASH_PRIME;
    }
}

fn hashI32Impl(data: [*]const i32, out: [*]u64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const v: u64 = @intCast(@as(u32, @bitCast(data[i])));
        out[i] = v *% HASH_PRIME;
    }
}

fn hashF64Impl(data: [*]const f64, out: [*]u64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const v: u64 = @bitCast(data[i]);
        out[i] = v *% HASH_PRIME;
    }
}

fn hashF32Impl(data: [*]const f32, out: [*]u64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const v: u64 = @intCast(@as(u32, @bitCast(data[i])));
        out[i] = v *% HASH_PRIME;
    }
}

fn combineHashesImpl(h1: [*]const u64, h2: [*]const u64, out: [*]u64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = h1[i] ^ (h2[i] *% HASH_PRIME);
    }
}

// ============================================================================
// Gather Operations
// ============================================================================

fn gatherF64Impl(src: [*]const f64, indices: [*]const i32, dst: [*]f64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const idx = indices[i];
        if (idx >= 0) {
            dst[i] = src[@intCast(idx)];
        } else {
            dst[i] = std.math.nan(f64);
        }
    }
}

fn gatherF32Impl(src: [*]const f32, indices: [*]const i32, dst: [*]f32, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const idx = indices[i];
        if (idx >= 0) {
            dst[i] = src[@intCast(idx)];
        } else {
            dst[i] = std.math.nan(f32);
        }
    }
}

fn gatherI64Impl(src: [*]const i64, indices: [*]const i32, dst: [*]i64, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const idx = indices[i];
        if (idx >= 0) {
            dst[i] = src[@intCast(idx)];
        } else {
            dst[i] = 0;
        }
    }
}

fn gatherI32Impl(src: [*]const i32, indices: [*]const i32, dst: [*]i32, len: usize) callconv(.c) void {
    var i: usize = 0;
    while (i < len) : (i += 1) {
        const idx = indices[i];
        if (idx >= 0) {
            dst[i] = src[@intCast(idx)];
        } else {
            dst[i] = 0;
        }
    }
}

// ============================================================================
// GroupBy Aggregation Operations
// ============================================================================

fn aggSumByGroupF64Impl(data: [*]const f64, group_ids: [*]const u32, out: [*]f64, data_len: usize, num_groups: usize) callconv(.c) void {
    // Initialize output to zero
    for (0..num_groups) |g| {
        out[g] = 0;
    }

    // Accumulate
    for (0..data_len) |i| {
        const gid = group_ids[i];
        out[gid] += data[i];
    }
}

fn aggSumByGroupI64Impl(data: [*]const i64, group_ids: [*]const u32, out: [*]i64, data_len: usize, num_groups: usize) callconv(.c) void {
    for (0..num_groups) |g| {
        out[g] = 0;
    }

    for (0..data_len) |i| {
        const gid = group_ids[i];
        out[gid] +%= data[i];
    }
}

fn aggMinByGroupF64Impl(data: [*]const f64, group_ids: [*]const u32, out: [*]f64, data_len: usize, num_groups: usize) callconv(.c) void {
    for (0..num_groups) |g| {
        out[g] = std.math.inf(f64);
    }

    for (0..data_len) |i| {
        const gid = group_ids[i];
        if (data[i] < out[gid]) {
            out[gid] = data[i];
        }
    }
}

fn aggMaxByGroupF64Impl(data: [*]const f64, group_ids: [*]const u32, out: [*]f64, data_len: usize, num_groups: usize) callconv(.c) void {
    for (0..num_groups) |g| {
        out[g] = -std.math.inf(f64);
    }

    for (0..data_len) |i| {
        const gid = group_ids[i];
        if (data[i] > out[gid]) {
            out[gid] = data[i];
        }
    }
}

fn countByGroupImpl(group_ids: [*]const u32, out: [*]u64, data_len: usize, num_groups: usize) callconv(.c) void {
    for (0..num_groups) |g| {
        out[g] = 0;
    }

    for (0..data_len) |i| {
        const gid = group_ids[i];
        out[gid] += 1;
    }
}

// ============================================================================
// Dispatch Table Export
// ============================================================================

const dispatch_mod = @import("../dispatch.zig");

pub const dispatch_table: dispatch_mod.DispatchTable = .{
    // Aggregations
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

    // Element-wise arithmetic
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

    // Comparisons
    .cmp_gt_f64 = cmpGtF64Impl,
    .cmp_ge_f64 = cmpGeF64Impl,
    .cmp_lt_f64 = cmpLtF64Impl,
    .cmp_le_f64 = cmpLeF64Impl,
    .cmp_eq_f64 = cmpEqF64Impl,
    .cmp_ne_f64 = cmpNeF64Impl,

    .cmp_gt_i64 = cmpGtI64Impl,
    .cmp_ge_i64 = cmpGeI64Impl,
    .cmp_lt_i64 = cmpLtI64Impl,
    .cmp_le_i64 = cmpLeI64Impl,
    .cmp_eq_i64 = cmpEqI64Impl,
    .cmp_ne_i64 = cmpNeI64Impl,

    // Filters
    .filter_gt_f64 = filterGtF64Impl,
    .filter_mask_gt_f64 = filterMaskGtF64Impl,
    .filter_gt_i64 = filterGtI64Impl,
    .filter_mask_gt_i64 = filterMaskGtI64Impl,

    // Hashing
    .hash_i64 = hashI64Impl,
    .hash_i32 = hashI32Impl,
    .hash_f64 = hashF64Impl,
    .hash_f32 = hashF32Impl,
    .combine_hashes = combineHashesImpl,

    // Gather
    .gather_f64 = gatherF64Impl,
    .gather_f32 = gatherF32Impl,
    .gather_i64 = gatherI64Impl,
    .gather_i32 = gatherI32Impl,

    // GroupBy aggregations
    .agg_sum_by_group_f64 = aggSumByGroupF64Impl,
    .agg_sum_by_group_i64 = aggSumByGroupI64Impl,
    .agg_min_by_group_f64 = aggMinByGroupF64Impl,
    .agg_max_by_group_f64 = aggMaxByGroupF64Impl,
    .count_by_group = countByGroupImpl,
};

// ============================================================================
// Tests
// ============================================================================

test "sse4 - sum f64" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 };
    const result = sumF64Impl(&data, data.len);
    try std.testing.expectApproxEqAbs(@as(f64, 55.0), result, 0.0001);
}

test "sse4 - add f64" {
    const a = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const b = [_]f64{ 10.0, 20.0, 30.0, 40.0 };
    var out: [4]f64 = undefined;
    addF64Impl(&a, &b, &out, 4);
    try std.testing.expectApproxEqAbs(@as(f64, 11.0), out[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 22.0), out[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 33.0), out[2], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 44.0), out[3], 0.0001);
}

test "sse4 - min max f64" {
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0 };
    var valid: bool = undefined;

    const min_val = minF64Impl(&data, data.len, &valid);
    try std.testing.expect(valid);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), min_val, 0.0001);

    const max_val = maxF64Impl(&data, data.len, &valid);
    try std.testing.expect(valid);
    try std.testing.expectApproxEqAbs(@as(f64, 9.0), max_val, 0.0001);
}
