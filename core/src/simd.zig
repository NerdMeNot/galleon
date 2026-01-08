const std = @import("std");
const builtin = @import("builtin");

/// SIMD vector width - 8 elements for AVX2-class performance
const VECTOR_WIDTH = 8;

/// Number of vectors to process per loop iteration (loop unrolling factor)
const UNROLL_FACTOR = 4;

/// Total elements processed per unrolled iteration
const CHUNK_SIZE = VECTOR_WIDTH * UNROLL_FACTOR; // 32 elements

// ============================================================================
// Thread Configuration
// ============================================================================

/// Maximum threads supported (compile-time constant for array sizing)
const MAX_THREADS: usize = 32;

/// Runtime configured max threads (0 = auto-detect)
var configured_max_threads: usize = 0;

/// Get the effective max threads to use
/// If configured_max_threads is 0, auto-detect from CPU count
/// Otherwise use the configured value (capped at MAX_THREADS)
fn getMaxThreads() usize {
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

/// Sum all elements in a slice using SIMD with loop unrolling
pub fn sum(comptime T: type, data: []const T) T {
    if (data.len == 0) return 0;

    const Vec = @Vector(VECTOR_WIDTH, T);

    // Use multiple accumulators to hide latency and enable ILP
    var acc0: Vec = @splat(0);
    var acc1: Vec = @splat(0);
    var acc2: Vec = @splat(0);
    var acc3: Vec = @splat(0);

    // Process CHUNK_SIZE elements at a time (unrolled)
    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        acc0 += chunk0;
        acc1 += chunk1;
        acc2 += chunk2;
        acc3 += chunk3;
    }

    // Combine accumulators
    const combined = acc0 + acc1 + acc2 + acc3;
    var result = @reduce(.Add, combined);

    // Handle remaining elements with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        result += @reduce(.Add, chunk);
    }

    // Handle tail elements
    while (i < data.len) : (i += 1) {
        result += data[i];
    }

    return result;
}

/// Find minimum value in a slice using SIMD with loop unrolling
pub fn min(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const init_val = data[0];

    // Multiple min trackers for ILP
    var min0: Vec = @splat(init_val);
    var min1: Vec = @splat(init_val);
    var min2: Vec = @splat(init_val);
    var min3: Vec = @splat(init_val);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        min0 = @min(min0, chunk0);
        min1 = @min(min1, chunk1);
        min2 = @min(min2, chunk2);
        min3 = @min(min3, chunk3);
    }

    // Combine min vectors
    const combined = @min(@min(min0, min1), @min(min2, min3));
    var result = @reduce(.Min, combined);

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk_min = @reduce(.Min, chunk);
        if (chunk_min < result) result = chunk_min;
    }

    // Handle tail
    while (i < data.len) : (i += 1) {
        if (data[i] < result) result = data[i];
    }

    return result;
}

/// Find maximum value in a slice using SIMD with loop unrolling
pub fn max(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const init_val = data[0];

    var max0: Vec = @splat(init_val);
    var max1: Vec = @splat(init_val);
    var max2: Vec = @splat(init_val);
    var max3: Vec = @splat(init_val);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        max0 = @max(max0, chunk0);
        max1 = @max(max1, chunk1);
        max2 = @max(max2, chunk2);
        max3 = @max(max3, chunk3);
    }

    const combined = @max(@max(max0, max1), @max(max2, max3));
    var result = @reduce(.Max, combined);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk_max = @reduce(.Max, chunk);
        if (chunk_max > result) result = chunk_max;
    }

    while (i < data.len) : (i += 1) {
        if (data[i] > result) result = data[i];
    }

    return result;
}

/// Calculate mean of a slice
pub fn mean(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;
    return sum(T, data) / @as(T, @floatFromInt(data.len));
}

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

// ============================================================================
// Array Comparison Operations (a cmp b -> u8 mask)
// ============================================================================

/// Compare two arrays: out[i] = 1 if a[i] > b[i], else 0
pub fn cmpGt(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

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

        const m0: BoolVec = a0 > b0;
        const m1: BoolVec = a1 > b1;
        const m2: BoolVec = a2 > b2;
        const m3: BoolVec = a3 > b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av > bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] > b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] >= b[i], else 0
pub fn cmpGe(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

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

        const m0: BoolVec = a0 >= b0;
        const m1: BoolVec = a1 >= b1;
        const m2: BoolVec = a2 >= b2;
        const m3: BoolVec = a3 >= b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av >= bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] >= b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] < b[i], else 0
pub fn cmpLt(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

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

        const m0: BoolVec = a0 < b0;
        const m1: BoolVec = a1 < b1;
        const m2: BoolVec = a2 < b2;
        const m3: BoolVec = a3 < b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av < bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] < b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] <= b[i], else 0
pub fn cmpLe(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

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

        const m0: BoolVec = a0 <= b0;
        const m1: BoolVec = a1 <= b1;
        const m2: BoolVec = a2 <= b2;
        const m3: BoolVec = a3 <= b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av <= bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] <= b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] == b[i], else 0
pub fn cmpEq(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

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

        const m0: BoolVec = a0 == b0;
        const m1: BoolVec = a1 == b1;
        const m2: BoolVec = a2 == b2;
        const m3: BoolVec = a3 == b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av == bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] == b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] != b[i], else 0
pub fn cmpNe(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

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

        const m0: BoolVec = a0 != b0;
        const m1: BoolVec = a1 != b1;
        const m2: BoolVec = a2 != b2;
        const m3: BoolVec = a3 != b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av != bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] != b[i]) 1 else 0;
    }
}

// ============================================================================
// Mask to Indices Conversion
// ============================================================================

/// Count the number of non-zero values in a u8 mask
pub fn countMaskTrue(mask: []const u8) usize {
    const len = mask.len;
    if (len == 0) return 0;

    var count: usize = 0;

    // Simple loop - the compiler should optimize this well
    for (mask) |v| {
        if (v != 0) count += 1;
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

/// Convert f64 to a sortable u64 representation
/// This makes the bit representation sortable: negative numbers sort before positive
inline fn floatToSortable(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    // If negative (sign bit set), flip all bits
    // If positive, flip only the sign bit
    // This ensures: -inf < negative < -0 < +0 < positive < +inf
    const mask: u64 = @as(u64, @intCast(@as(i64, @bitCast(bits)) >> 63)) | (@as(u64, 1) << 63);
    return bits ^ mask;
}

/// Convert sortable u64 back to f64
inline fn sortableToFloat(bits: u64) f64 {
    // Reverse the transformation
    const mask: u64 = (~(bits >> 63) + 1) | (@as(u64, 1) << 63);
    return @bitCast(bits ^ mask);
}

/// Radix sort configuration
const RADIX_BITS: u6 = 8;
const RADIX_SIZE: usize = 1 << RADIX_BITS; // 256 buckets
const RADIX_MASK: u64 = RADIX_SIZE - 1;

/// Pair structure for cache-friendly sorting
/// Packs value and index together so comparisons don't cause cache misses
const SortPair = packed struct {
    key: u64, // sortable representation of value
    idx: u32, // original index
};

/// Convert f64 to sortable u64 representation
/// This maps floats to integers that sort in the same order:
/// - Positive floats: flip sign bit
/// - Negative floats: flip all bits
inline fn f64ToSortable(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    // If negative (sign bit set), flip all bits; otherwise flip just sign bit
    const mask: u64 = @bitCast(-@as(i64, @bitCast(bits >> 63)));
    return bits ^ (mask | (1 << 63));
}

/// ValueIndex pair for cache-friendly sorting
const ValueIndexPair = struct {
    value: f64,
    idx: u32,
};

/// SIMD-accelerated partition for quicksort on pairs
/// Returns the partition index where all elements < pivot are on the left
fn simdPartitionPairs(pairs: []ValueIndexPair, ascending: bool) usize {
    if (pairs.len <= 1) return 0;
    if (pairs.len <= 16) {
        // Small array: use simple partition
        return simplePartitionPairs(pairs, ascending);
    }

    // Median-of-three pivot selection
    const mid = pairs.len / 2;
    const last = pairs.len - 1;

    if (ascending) {
        if (pairs[0].value > pairs[mid].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[mid]);
        if (pairs[0].value > pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[last]);
        if (pairs[mid].value > pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[mid], &pairs[last]);
    } else {
        if (pairs[0].value < pairs[mid].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[mid]);
        if (pairs[0].value < pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[last]);
        if (pairs[mid].value < pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[mid], &pairs[last]);
    }

    std.mem.swap(ValueIndexPair, &pairs[mid], &pairs[last - 1]);
    const pivot = pairs[last - 1].value;

    // SIMD partition using vector comparisons
    const Vec = @Vector(4, f64);
    const pivot_vec: Vec = @splat(pivot);

    var left: usize = 0;
    var right: usize = last - 1;

    // Process 4 elements at a time from left
    while (left + 4 <= right) {
        // Load 4 values
        var vals: Vec = undefined;
        inline for (0..4) |i| {
            vals[i] = pairs[left + i].value;
        }

        // Compare with pivot
        const cmp = if (ascending) vals < pivot_vec else vals > pivot_vec;

        // Count elements that should stay on left
        var stay_count: usize = 0;
        inline for (0..4) |i| {
            if (cmp[i]) stay_count += 1;
        }

        if (stay_count == 4) {
            // All stay on left
            left += 4;
        } else if (stay_count == 0) {
            // All go to right - swap with right side
            inline for (0..4) |i| {
                right -= 1;
                std.mem.swap(ValueIndexPair, &pairs[left + i], &pairs[right]);
            }
        } else {
            // Mixed - fall back to scalar
            break;
        }
    }

    // Scalar cleanup
    while (left < right) {
        const cmp = if (ascending) pairs[left].value < pivot else pairs[left].value > pivot;
        if (cmp) {
            left += 1;
        } else {
            right -= 1;
            std.mem.swap(ValueIndexPair, &pairs[left], &pairs[right]);
        }
    }

    // Move pivot to final position
    std.mem.swap(ValueIndexPair, &pairs[left], &pairs[last - 1]);
    return left;
}

fn simplePartitionPairs(pairs: []ValueIndexPair, ascending: bool) usize {
    if (pairs.len <= 1) return 0;

    const last = pairs.len - 1;
    const pivot = pairs[last].value;
    var i: usize = 0;

    for (0..last) |j| {
        const cmp = if (ascending) pairs[j].value < pivot else pairs[j].value > pivot;
        if (cmp) {
            std.mem.swap(ValueIndexPair, &pairs[i], &pairs[j]);
            i += 1;
        }
    }
    std.mem.swap(ValueIndexPair, &pairs[i], &pairs[last]);
    return i;
}

/// Insertion sort for small arrays
fn insertionSortPairs(pairs: []ValueIndexPair, ascending: bool) void {
    if (pairs.len <= 1) return;
    for (1..pairs.len) |i| {
        const key = pairs[i];
        var j: usize = i;
        while (j > 0) {
            const cmp = if (ascending) key.value < pairs[j - 1].value else key.value > pairs[j - 1].value;
            if (!cmp) break;
            pairs[j] = pairs[j - 1];
            j -= 1;
        }
        pairs[j] = key;
    }
}

/// SIMD-accelerated quicksort on pairs
fn simdQuicksortPairs(pairs: []ValueIndexPair, ascending: bool) void {
    if (pairs.len <= 24) {
        insertionSortPairs(pairs, ascending);
        return;
    }

    const pivot_idx = simdPartitionPairs(pairs, ascending);

    // Recursively sort partitions
    if (pivot_idx > 0) simdQuicksortPairs(pairs[0..pivot_idx], ascending);
    if (pivot_idx + 1 < pairs.len) simdQuicksortPairs(pairs[pivot_idx + 1 ..], ascending);
}

/// Parallel sample sort threshold
const PARALLEL_SORT_THRESHOLD: usize = 50000;

/// Parallel sample sort for large arrays
fn parallelSampleSortPairs(pairs: []ValueIndexPair, ascending: bool) void {
    const len = pairs.len;
    const num_threads: usize = @min(getMaxThreads(), 8); // Cap at 8 threads for sort
    const num_buckets: usize = num_threads;

    if (len < PARALLEL_SORT_THRESHOLD or num_threads <= 1) {
        simdQuicksortPairs(pairs, ascending);
        return;
    }

    const allocator = std.heap.page_allocator;

    // Sample to find partition boundaries
    const sample_size = @min(num_buckets * 100, len / 10);
    var samples = allocator.alloc(f64, sample_size) catch {
        simdQuicksortPairs(pairs, ascending);
        return;
    };
    defer allocator.free(samples);

    // Take evenly spaced samples
    const step = len / sample_size;
    for (0..sample_size) |i| {
        samples[i] = pairs[i * step].value;
    }

    // Sort samples to find splitters
    if (ascending) {
        std.sort.pdq(f64, samples, {}, struct {
            fn lt(_: void, a: f64, b: f64) bool { return a < b; }
        }.lt);
    } else {
        std.sort.pdq(f64, samples, {}, struct {
            fn lt(_: void, a: f64, b: f64) bool { return a > b; }
        }.lt);
    }

    // Extract splitters (bucket boundaries)
    var splitters: [8]f64 = undefined;
    for (0..num_buckets - 1) |i| {
        splitters[i] = samples[(i + 1) * sample_size / num_buckets];
    }

    // Count elements per bucket
    var bucket_counts: [8]usize = [_]usize{0} ** 8;
    for (pairs) |pair| {
        const bucket = findBucket(pair.value, splitters[0..num_buckets - 1], ascending);
        bucket_counts[bucket] += 1;
    }

    // Calculate bucket offsets
    var bucket_offsets: [9]usize = undefined;
    bucket_offsets[0] = 0;
    for (0..num_buckets) |i| {
        bucket_offsets[i + 1] = bucket_offsets[i] + bucket_counts[i];
    }

    // Allocate temp array and distribute to buckets
    const temp = allocator.alloc(ValueIndexPair, len) catch {
        simdQuicksortPairs(pairs, ascending);
        return;
    };
    defer allocator.free(temp);

    var bucket_cursors: [8]usize = undefined;
    for (0..num_buckets) |i| {
        bucket_cursors[i] = bucket_offsets[i];
    }

    for (pairs) |pair| {
        const bucket = findBucket(pair.value, splitters[0..num_buckets - 1], ascending);
        temp[bucket_cursors[bucket]] = pair;
        bucket_cursors[bucket] += 1;
    }

    // Sort each bucket in parallel
    var threads: [8]?std.Thread = [_]?std.Thread{null} ** 8;

    for (0..num_buckets) |t| {
        const start = bucket_offsets[t];
        const end = bucket_offsets[t + 1];
        if (start >= end) continue;

        threads[t] = std.Thread.spawn(.{}, struct {
            fn work(slice: []ValueIndexPair, asc: bool) void {
                simdQuicksortPairs(slice, asc);
            }
        }.work, .{ temp[start..end], ascending }) catch null;
    }

    // Wait for all threads
    for (&threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // Copy back
    @memcpy(pairs, temp);
}

fn findBucket(value: f64, splitters: []const f64, ascending: bool) usize {
    for (splitters, 0..) |s, i| {
        if (ascending) {
            if (value < s) return i;
        } else {
            if (value > s) return i;
        }
    }
    return splitters.len;
}

/// NEW: Pair-based sort using parallel sample sort + SIMD quicksort
/// This is cache-friendly because comparisons access contiguous memory
pub fn argsortPairRadix(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    const allocator = std.heap.page_allocator;

    // Allocate pairs array
    const pairs = allocator.alloc(ValueIndexPair, len) catch {
        argsortFallback(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(pairs);

    // Initialize pairs
    for (data[0..len], 0..) |val, i| {
        pairs[i] = .{ .value = val, .idx = @intCast(i) };
    }

    // Sort pairs using parallel sample sort
    parallelSampleSortPairs(pairs, ascending);

    // Extract indices
    for (pairs, 0..) |pair, i| {
        out_indices[i] = pair.idx;
    }
}

/// Fallback simple argsort for when allocation fails
fn argsortFallback(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }
    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lt(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] < ctx[b];
            }
        }.lt);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lt(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] > ctx[b];
            }
        }.lt);
    }
}

/// Parallel radix sort for f64 argsort - much faster than comparison sort
/// Uses LSB radix sort with parallel counting and distribution
pub fn argsortRadixF64(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use simple sort
    if (len < 256) {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    }

    const allocator = std.heap.page_allocator;

    // Allocate working buffers
    const keys = allocator.alloc(u64, len) catch {
        // Fallback to simple sort on allocation failure
        argsortSmall(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(keys);

    const temp_indices = allocator.alloc(u32, len) catch {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp_indices);

    const temp_keys = allocator.alloc(u64, len) catch {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp_keys);

    // Convert floats to sortable integers and initialize indices
    for (0..len) |i| {
        keys[i] = floatToSortable(data[i]);
        out_indices[i] = @intCast(i);
    }

    // Perform radix sort passes (8 passes for 64-bit keys)
    var src_keys = keys;
    var dst_keys = temp_keys;
    var src_indices = out_indices;
    var dst_indices = temp_indices;

    // LSD radix sort: process from least significant byte to most significant
    var shift: u6 = 0;
    while (shift < 64) : (shift += RADIX_BITS) {
        // Count occurrences for this digit
        var counts: [RADIX_SIZE]usize = [_]usize{0} ** RADIX_SIZE;
        for (src_keys[0..len]) |key| {
            const digit: usize = @intCast((key >> shift) & RADIX_MASK);
            counts[digit] += 1;
        }

        // Compute prefix sums (starting positions for each bucket)
        var offsets: [RADIX_SIZE]usize = undefined;
        var total: usize = 0;
        for (0..RADIX_SIZE) |i| {
            offsets[i] = total;
            total += counts[i];
        }

        // Distribute elements to destination
        for (0..len) |i| {
            const key = src_keys[i];
            const digit: usize = @intCast((key >> shift) & RADIX_MASK);
            const dst_pos = offsets[digit];
            offsets[digit] += 1;

            dst_keys[dst_pos] = key;
            dst_indices[dst_pos] = src_indices[i];
        }

        // Swap source and destination for next pass
        const tmp_keys = src_keys;
        src_keys = dst_keys;
        dst_keys = tmp_keys;

        const tmp_indices = src_indices;
        src_indices = dst_indices;
        dst_indices = tmp_indices;
    }

    // After 8 passes (even number), result is in original buffers
    // Keys are in keys, indices are in out_indices - already correct!

    // If descending, reverse the result
    if (!ascending) {
        var left: usize = 0;
        var right: usize = len - 1;
        while (left < right) {
            const tmp = out_indices[left];
            out_indices[left] = out_indices[right];
            out_indices[right] = tmp;
            left += 1;
            right -= 1;
        }
    }
}

/// Parallel sort using divide-and-conquer with parallel merge
/// Each thread sorts its chunk, then we merge in parallel
pub fn argsortParallelMerge(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use radix sort
    if (len < 16384) {
        argsortRadixF64(data, out_indices, ascending);
        return;
    }

    const num_threads = getMaxThreads();
    const allocator = std.heap.page_allocator;

    // Allocate temp buffer for merging
    const temp_indices = allocator.alloc(u32, len) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp_indices);

    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    // Each thread sorts its chunk using radix sort
    const chunk_size = (len + num_threads - 1) / num_threads;

    // Allocate per-thread buffers for radix sort
    const thread_keys = allocator.alloc([]u64, num_threads) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(thread_keys);

    const thread_temp_keys = allocator.alloc([]u64, num_threads) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(thread_temp_keys);

    const thread_temp_indices = allocator.alloc([]u32, num_threads) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(thread_temp_indices);

    // Allocate buffers for each thread
    var alloc_failed = false;
    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) {
            thread_keys[t] = &[_]u64{};
            thread_temp_keys[t] = &[_]u64{};
            thread_temp_indices[t] = &[_]u32{};
            continue;
        }
        const csize = end - start;

        thread_keys[t] = allocator.alloc(u64, csize) catch {
            alloc_failed = true;
            break;
        };
        thread_temp_keys[t] = allocator.alloc(u64, csize) catch {
            alloc_failed = true;
            break;
        };
        thread_temp_indices[t] = allocator.alloc(u32, csize) catch {
            alloc_failed = true;
            break;
        };
    }

    if (alloc_failed) {
        // Free any allocated buffers and fallback
        for (0..num_threads) |t| {
            if (thread_keys[t].len > 0) allocator.free(thread_keys[t]);
            if (thread_temp_keys[t].len > 0) allocator.free(thread_temp_keys[t]);
            if (thread_temp_indices[t].len > 0) allocator.free(thread_temp_indices[t]);
        }
        argsortRadixF64(data, out_indices, ascending);
        return;
    }
    defer {
        for (0..num_threads) |t| {
            if (thread_keys[t].len > 0) allocator.free(thread_keys[t]);
            if (thread_temp_keys[t].len > 0) allocator.free(thread_temp_keys[t]);
            if (thread_temp_indices[t].len > 0) allocator.free(thread_temp_indices[t]);
        }
    }

    // Sort chunks in parallel
    var sort_threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;
    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) break;

        sort_threads[t] = std.Thread.spawn(.{}, struct {
            fn work(
                d: []const f64,
                indices: []u32,
                keys: []u64,
                temp_keys: []u64,
                temp_idx: []u32,
                s: usize,
                e: usize,
            ) void {
                const clen = e - s;
                const chunk_indices = indices[s..e];

                // Convert to sortable keys
                for (0..clen) |i| {
                    keys[i] = floatToSortable(d[s + i]);
                }

                // LSD Radix sort on this chunk
                var src_keys = keys;
                var dst_keys = temp_keys;
                var src_idx = chunk_indices;
                var dst_idx = temp_idx;

                var shift: u6 = 0;
                while (shift < 64) : (shift += RADIX_BITS) {
                    var counts: [RADIX_SIZE]usize = [_]usize{0} ** RADIX_SIZE;
                    for (src_keys[0..clen]) |key| {
                        const digit: usize = @intCast((key >> shift) & RADIX_MASK);
                        counts[digit] += 1;
                    }

                    var offsets: [RADIX_SIZE]usize = undefined;
                    var total: usize = 0;
                    for (0..RADIX_SIZE) |i| {
                        offsets[i] = total;
                        total += counts[i];
                    }

                    for (0..clen) |i| {
                        const key = src_keys[i];
                        const digit: usize = @intCast((key >> shift) & RADIX_MASK);
                        const dst_pos = offsets[digit];
                        offsets[digit] += 1;

                        dst_keys[dst_pos] = key;
                        dst_idx[dst_pos] = src_idx[i];
                    }

                    const tmp_k = src_keys;
                    src_keys = dst_keys;
                    dst_keys = tmp_k;

                    const tmp_i = src_idx;
                    src_idx = dst_idx;
                    dst_idx = tmp_i;
                }

                // Copy result back if needed (after 8 passes, result is in original)
                // Since we did 8 passes, result should be in keys and chunk_indices
            }
        }.work, .{ data, out_indices, thread_keys[t], thread_temp_keys[t], thread_temp_indices[t], start, end }) catch null;
    }

    // Wait for all sorts to complete
    for (&sort_threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // Now merge sorted chunks using k-way merge
    // For simplicity, do pairwise merging
    var current_chunk_size = chunk_size;
    var src = out_indices;
    var dst = temp_indices;

    while (current_chunk_size < len) {
        const merge_threads_count = (len + 2 * current_chunk_size - 1) / (2 * current_chunk_size);
        var merge_threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

        for (0..@min(merge_threads_count, num_threads)) |t| {
            const merge_start = t * 2 * current_chunk_size;
            if (merge_start >= len) break;

            const mid = @min(merge_start + current_chunk_size, len);
            const merge_end = @min(merge_start + 2 * current_chunk_size, len);

            merge_threads[t] = std.Thread.spawn(.{}, struct {
                fn work(d: []const f64, s_buf: []const u32, d_buf: []u32, left: usize, m: usize, right: usize, asc: bool) void {
                    var i = left;
                    var j = m;
                    var k = left;

                    while (i < m and j < right) {
                        const cmp = if (asc)
                            d[s_buf[i]] <= d[s_buf[j]]
                        else
                            d[s_buf[i]] >= d[s_buf[j]];

                        if (cmp) {
                            d_buf[k] = s_buf[i];
                            i += 1;
                        } else {
                            d_buf[k] = s_buf[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    while (i < m) {
                        d_buf[k] = s_buf[i];
                        i += 1;
                        k += 1;
                    }

                    while (j < right) {
                        d_buf[k] = s_buf[j];
                        j += 1;
                        k += 1;
                    }
                }
            }.work, .{ data, src, dst, merge_start, mid, merge_end, ascending }) catch null;
        }

        for (&merge_threads) |*t| {
            if (t.*) |thread| {
                thread.join();
                t.* = null;
            }
        }

        // Swap buffers
        const tmp = src;
        src = dst;
        dst = tmp;

        current_chunk_size *= 2;
    }

    // If result is in temp buffer, copy back
    if (src.ptr != out_indices.ptr) {
        @memcpy(out_indices[0..len], src[0..len]);
    }
}

/// Simple argsort for small arrays using comparison sort
fn argsortSmall(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);

    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] < ctx[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] > ctx[b];
            }
        }.lessThan);
    }
}

/// Parallel argsort using divide-and-conquer
/// Divides data into chunks, sorts each in parallel, then merges
pub fn argsortParallel(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use simple sort
    if (len < 32768) {
        argsortSmall(T, data, out_indices, ascending);
        return;
    }

    const num_threads = getMaxThreads();
    const chunk_size = (len + num_threads - 1) / num_threads;
    const allocator = std.heap.page_allocator;

    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    // Temp buffer for merging
    const temp = allocator.alloc(u32, len) catch {
        argsortSmall(T, data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp);

    // Sort chunks in parallel
    var threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) break;

        threads[t] = std.Thread.spawn(.{}, struct {
            fn work(d: []const T, indices: []u32, s: usize, e: usize, asc: bool) void {
                const chunk = indices[s..e];
                if (asc) {
                    std.mem.sort(u32, chunk, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] < ctx[b];
                        }
                    }.lt);
                } else {
                    std.mem.sort(u32, chunk, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] > ctx[b];
                        }
                    }.lt);
                }
            }
        }.work, .{ data, out_indices, start, end, ascending }) catch null;
    }

    // Wait for all sorts
    for (&threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // Merge sorted chunks (log(num_threads) levels)
    var current_size = chunk_size;
    var src = out_indices;
    var dst = temp;

    while (current_size < len) {
        const num_merges = (len + 2 * current_size - 1) / (2 * current_size);

        // Parallel merge
        var merge_threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

        for (0..@min(num_merges, num_threads)) |t| {
            const left = t * 2 * current_size;
            if (left >= len) break;

            const mid = @min(left + current_size, len);
            const right = @min(left + 2 * current_size, len);

            if (mid >= right) {
                // Only one chunk, just copy
                @memcpy(dst[left..right], src[left..right]);
                continue;
            }

            merge_threads[t] = std.Thread.spawn(.{}, struct {
                fn work(d: []const T, s_buf: []const u32, d_buf: []u32, l: usize, m: usize, r: usize, asc: bool) void {
                    var i = l;
                    var j = m;
                    var k = l;

                    while (i < m and j < r) {
                        const cmp = if (asc)
                            d[s_buf[i]] <= d[s_buf[j]]
                        else
                            d[s_buf[i]] >= d[s_buf[j]];

                        if (cmp) {
                            d_buf[k] = s_buf[i];
                            i += 1;
                        } else {
                            d_buf[k] = s_buf[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    while (i < m) : (i += 1) {
                        d_buf[k] = s_buf[i];
                        k += 1;
                    }
                    while (j < r) : (j += 1) {
                        d_buf[k] = s_buf[j];
                        k += 1;
                    }
                }
            }.work, .{ data, src, dst, left, mid, right, ascending }) catch null;
        }

        // Handle remaining merges if more than num_threads
        for (@min(num_merges, num_threads)..num_merges) |t| {
            const left = t * 2 * current_size;
            if (left >= len) break;

            const mid = @min(left + current_size, len);
            const right = @min(left + 2 * current_size, len);

            if (mid >= right) {
                @memcpy(dst[left..right], src[left..right]);
                continue;
            }

            // Sequential merge for extra chunks
            var i = left;
            var j = mid;
            var k = left;

            while (i < mid and j < right) {
                const cmp = if (ascending)
                    data[src[i]] <= data[src[j]]
                else
                    data[src[i]] >= data[src[j]];

                if (cmp) {
                    dst[k] = src[i];
                    i += 1;
                } else {
                    dst[k] = src[j];
                    j += 1;
                }
                k += 1;
            }

            while (i < mid) : (i += 1) {
                dst[k] = src[i];
                k += 1;
            }
            while (j < right) : (j += 1) {
                dst[k] = src[j];
                k += 1;
            }
        }

        for (&merge_threads) |*t| {
            if (t.*) |thread| {
                thread.join();
                t.* = null;
            }
        }

        // Swap buffers
        const tmp = src;
        src = dst;
        dst = tmp;

        current_size *= 2;
    }

    // Copy result back if needed
    if (src.ptr != out_indices.ptr) {
        @memcpy(out_indices[0..len], src[0..len]);
    }
}

/// Argsort - return indices that would sort the array
/// Uses pair-based radix sort for f64 (fastest), fallback to comparison sort for others
pub fn argsort(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For f64, use the new pair-based radix sort (O(n) and cache-friendly)
    if (T == f64) {
        argsortPairRadix(data, out_indices, ascending);
        return;
    }

    // For other types, use comparison-based sort
    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    // Use parallel sort for large arrays
    if (len >= 100000) {
        argsortParallelInPlace(T, data, out_indices[0..len], ascending);
    } else {
        // Use block sort for smaller arrays
        if (ascending) {
            std.mem.sort(u32, out_indices[0..len], data, struct {
                fn lt(ctx: []const T, a: u32, b: u32) bool {
                    return ctx[a] < ctx[b];
                }
            }.lt);
        } else {
            std.mem.sort(u32, out_indices[0..len], data, struct {
                fn lt(ctx: []const T, a: u32, b: u32) bool {
                    return ctx[a] > ctx[b];
                }
            }.lt);
        }
    }
}

/// In-place parallel sort (modifies out_indices directly)
fn argsortParallelInPlace(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = out_indices.len;
    const num_threads = getMaxThreads();
    const chunk_size = (len + num_threads - 1) / num_threads;
    const allocator = std.heap.page_allocator;

    // Sort chunks in parallel
    var threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) break;

        threads[t] = std.Thread.spawn(.{}, struct {
            fn work(d: []const T, indices: []u32, asc: bool) void {
                if (asc) {
                    std.mem.sort(u32, indices, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] < ctx[b];
                        }
                    }.lt);
                } else {
                    std.mem.sort(u32, indices, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] > ctx[b];
                        }
                    }.lt);
                }
            }
        }.work, .{ data, out_indices[start..end], ascending }) catch null;
    }

    for (&threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // K-way merge using heap
    const temp = allocator.alloc(u32, len) catch {
        // Fallback: just return partially sorted (chunks are sorted)
        return;
    };
    defer allocator.free(temp);

    // Merge pairs iteratively
    var current_size = chunk_size;
    var src = out_indices;
    var dst = temp;

    while (current_size < len) {
        var merge_idx: usize = 0;
        while (merge_idx * 2 * current_size < len) : (merge_idx += 1) {
            const left = merge_idx * 2 * current_size;
            const mid = @min(left + current_size, len);
            const right = @min(left + 2 * current_size, len);

            // Merge [left..mid] and [mid..right] into dst[left..right]
            var i = left;
            var j = mid;
            var k = left;

            while (i < mid and j < right) {
                const cmp = if (ascending)
                    data[src[i]] <= data[src[j]]
                else
                    data[src[i]] >= data[src[j]];

                if (cmp) {
                    dst[k] = src[i];
                    i += 1;
                } else {
                    dst[k] = src[j];
                    j += 1;
                }
                k += 1;
            }

            @memcpy(dst[k .. k + (mid - i)], src[i..mid]);
            k += mid - i;
            @memcpy(dst[k .. k + (right - j)], src[j..right]);
        }

        const tmp = src;
        src = dst;
        dst = tmp;
        current_size *= 2;
    }

    // Copy back if needed
    if (src.ptr != out_indices.ptr) {
        @memcpy(out_indices, src);
    }
}

/// Variance calculation (for std dev) with SIMD
pub fn variance(comptime T: type, data: []const T) ?T {
    if (data.len < 2) return null;

    const avg = mean(T, data) orelse return null;
    const Vec = @Vector(VECTOR_WIDTH, T);
    const mean_vec: Vec = @splat(avg);

    // Multiple accumulators for sum of squares
    var sum_sq0: Vec = @splat(0);
    var sum_sq1: Vec = @splat(0);
    var sum_sq2: Vec = @splat(0);
    var sum_sq3: Vec = @splat(0);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const diff0 = chunk0 - mean_vec;
        const diff1 = chunk1 - mean_vec;
        const diff2 = chunk2 - mean_vec;
        const diff3 = chunk3 - mean_vec;

        sum_sq0 += diff0 * diff0;
        sum_sq1 += diff1 * diff1;
        sum_sq2 += diff2 * diff2;
        sum_sq3 += diff3 * diff3;
    }

    const combined = sum_sq0 + sum_sq1 + sum_sq2 + sum_sq3;
    var sum_sq = @reduce(.Add, combined);

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const diff = chunk - mean_vec;
        sum_sq += @reduce(.Add, diff * diff);
    }

    // Handle tail
    while (i < data.len) : (i += 1) {
        const diff = data[i] - avg;
        sum_sq += diff * diff;
    }

    return sum_sq / @as(T, @floatFromInt(data.len - 1));
}

/// Standard deviation
pub fn stdDev(comptime T: type, data: []const T) ?T {
    const v = variance(T, data) orelse return null;
    return @sqrt(v);
}

// ============================================================================
// Integer SIMD Operations
// ============================================================================

/// Sum all elements in an integer slice using SIMD with loop unrolling
pub fn sumInt(comptime T: type, data: []const T) T {
    if (data.len == 0) return 0;

    const Vec = @Vector(VECTOR_WIDTH, T);

    var acc0: Vec = @splat(0);
    var acc1: Vec = @splat(0);
    var acc2: Vec = @splat(0);
    var acc3: Vec = @splat(0);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        acc0 +%= chunk0;
        acc1 +%= chunk1;
        acc2 +%= chunk2;
        acc3 +%= chunk3;
    }

    const combined = acc0 +% acc1 +% acc2 +% acc3;
    var result = @reduce(.Add, combined);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        result +%= @reduce(.Add, chunk);
    }

    while (i < data.len) : (i += 1) {
        result +%= data[i];
    }

    return result;
}

/// Find minimum value in an integer slice using SIMD
pub fn minInt(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const init_val = data[0];

    var min0: Vec = @splat(init_val);
    var min1: Vec = @splat(init_val);
    var min2: Vec = @splat(init_val);
    var min3: Vec = @splat(init_val);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        min0 = @min(min0, chunk0);
        min1 = @min(min1, chunk1);
        min2 = @min(min2, chunk2);
        min3 = @min(min3, chunk3);
    }

    const combined = @min(@min(min0, min1), @min(min2, min3));
    var result = @reduce(.Min, combined);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk_min = @reduce(.Min, chunk);
        if (chunk_min < result) result = chunk_min;
    }

    while (i < data.len) : (i += 1) {
        if (data[i] < result) result = data[i];
    }

    return result;
}

/// Find maximum value in an integer slice using SIMD
pub fn maxInt(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const init_val = data[0];

    var max0: Vec = @splat(init_val);
    var max1: Vec = @splat(init_val);
    var max2: Vec = @splat(init_val);
    var max3: Vec = @splat(init_val);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        max0 = @max(max0, chunk0);
        max1 = @max(max1, chunk1);
        max2 = @max(max2, chunk2);
        max3 = @max(max3, chunk3);
    }

    const combined = @max(@max(max0, max1), @max(max2, max3));
    var result = @reduce(.Max, combined);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk_max = @reduce(.Max, chunk);
        if (chunk_max > result) result = chunk_max;
    }

    while (i < data.len) : (i += 1) {
        if (data[i] > result) result = data[i];
    }

    return result;
}

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

/// Filter integer values greater than threshold using SIMD
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

/// Argsort for integer types
pub fn argsortInt(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);

    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] < ctx[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] > ctx[b];
            }
        }.lessThan);
    }
}

/// Count true values in a boolean slice
pub fn countTrue(data: []const bool) usize {
    var count: usize = 0;

    // Process 8 bools at a time by reinterpreting as bytes
    const aligned_len = data.len - (data.len % 8);
    var i: usize = 0;

    while (i < aligned_len) : (i += 8) {
        // Each bool is 1 byte, so sum them directly
        const b = @as(*const [8]u8, @ptrCast(data[i..].ptr));
        count += @as(usize, b[0]) + @as(usize, b[1]) + @as(usize, b[2]) + @as(usize, b[3]) +
            @as(usize, b[4]) + @as(usize, b[5]) + @as(usize, b[6]) + @as(usize, b[7]);
    }

    while (i < data.len) : (i += 1) {
        count += @intFromBool(data[i]);
    }

    return count;
}

// ============================================================================
// GroupBy Aggregation Functions
// ============================================================================

/// Aggregate sum by group - scatter-add pattern
/// data: source values
/// group_ids: group index for each row (0 to num_groups-1)
/// out_sums: output array of size num_groups, must be zero-initialized
pub fn aggregateSumByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_sums: []T) void {
    const len = @min(data.len, group_ids.len);
    for (0..len) |i| {
        const gid = group_ids[i];
        if (gid < out_sums.len) {
            out_sums[gid] += data[i];
        }
    }
}

/// Aggregate min by group
/// out_mins must be initialized to max values for the type
pub fn aggregateMinByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_mins: []T) void {
    const len = @min(data.len, group_ids.len);
    for (0..len) |i| {
        const gid = group_ids[i];
        if (gid < out_mins.len and data[i] < out_mins[gid]) {
            out_mins[gid] = data[i];
        }
    }
}

/// Aggregate max by group
/// out_maxs must be initialized to min values for the type
pub fn aggregateMaxByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_maxs: []T) void {
    const len = @min(data.len, group_ids.len);
    for (0..len) |i| {
        const gid = group_ids[i];
        if (gid < out_maxs.len and data[i] > out_maxs[gid]) {
            out_maxs[gid] = data[i];
        }
    }
}

/// Count elements per group
pub fn countByGroup(group_ids: []const u32, out_counts: []u64) void {
    for (group_ids) |gid| {
        if (gid < out_counts.len) {
            out_counts[gid] += 1;
        }
    }
}

// ============================================================================
// Rapidhash - Fast, high-quality hash function (evolution of wyhash)
// ============================================================================

/// Rapidhash secrets - carefully chosen constants for good mixing
const RAPID_SECRET0: u64 = 0x2d358dccaa6c78a5;
const RAPID_SECRET1: u64 = 0x8bb84b93962eacc9;
const RAPID_SECRET2: u64 = 0x4b33a62ed433d4a3;

/// Core mixing function: 64x64->128 multiply, return xor of high and low
inline fn rapidMix(a: u64, b: u64) u64 {
    const full: u128 = @as(u128, a) *% @as(u128, b);
    return @as(u64, @truncate(full)) ^ @as(u64, @truncate(full >> 64));
}

/// Hash a single 64-bit value using rapidhash
inline fn rapidHash64(val: u64) u64 {
    // Mix with secrets and combine
    const a = val ^ RAPID_SECRET0;
    const b = val ^ RAPID_SECRET1;
    return rapidMix(a, b) ^ RAPID_SECRET2;
}

/// Fast integer hash using multiply-shift
/// Faster than rapidHash64 for simple integer keys
/// Uses a prime multiplier for good bit mixing
inline fn fastIntHash(val: i64) u64 {
    const x = @as(u64, @bitCast(val));
    // Multiply by golden ratio prime, then mix high/low bits
    const h = x *% 0x9E3779B97F4A7C15;
    return h ^ (h >> 32);
}

/// Hash a single 32-bit value using rapidhash
inline fn rapidHash32(val: u32) u64 {
    // Extend to 64-bit and mix
    const extended: u64 = @as(u64, val) | (@as(u64, val) << 32);
    return rapidHash64(extended);
}

/// Hash int64 column for groupby/join using rapidhash
/// Outputs hash values that can be used for grouping
pub fn hashInt64Column(data: []const i64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    // Process 4 at a time for better ILP
    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash64(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash64(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash64(@bitCast(data[i + 3]));
    }

    // Handle remainder
    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
    }
}

/// Hash int32 column
pub fn hashInt32Column(data: []const i32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash32(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash32(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash32(@bitCast(data[i + 3]));
    }

    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
    }
}

/// Hash float64 column
pub fn hashFloat64Column(data: []const f64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash64(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash64(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash64(@bitCast(data[i + 3]));
    }

    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
    }
}

/// Hash float32 column
pub fn hashFloat32Column(data: []const f32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash32(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash32(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash32(@bitCast(data[i + 3]));
    }

    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
    }
}

/// Combine two hash columns (for multi-key groupby/join) using rapidhash mixing
pub fn combineHashes(hash1: []const u64, hash2: []const u64, out_hashes: []u64) void {
    const len = @min(@min(hash1.len, hash2.len), out_hashes.len);

    var i: usize = 0;
    while (i < len) : (i += 1) {
        // Combine using rapidhash mixing
        out_hashes[i] = rapidMix(hash1[i], hash2[i]);
    }
}

/// Hash multiple int64 columns together (for multi-key groupby/join)
/// Combines hashes from multiple columns using rapidhash
pub fn hashInt64Columns(columns: []const []const i64, out_hashes: []u64) void {
    if (columns.len == 0) return;

    const len = out_hashes.len;

    for (0..len) |row| {
        var hash: u64 = RAPID_SECRET0;

        for (columns) |col| {
            if (row < col.len) {
                const val: u64 = @bitCast(col[row]);
                // Mix each column value into the running hash
                hash = rapidMix(hash ^ val, RAPID_SECRET1);
            }
        }

        out_hashes[row] = hash ^ RAPID_SECRET2;
    }
}

// ============================================================================
// Join Helper Functions
// ============================================================================

/// Gather f64 values by indices - used for join result materialization
/// indices: source row indices (-1 or out of range means null/zero)
/// src: source data
/// dst: destination data (same length as indices)
pub fn gatherF64(src: []const f64, indices: []const i32, dst: []f64) void {
    for (dst, indices) |*d, idx| {
        if (idx >= 0 and @as(usize, @intCast(idx)) < src.len) {
            d.* = src[@intCast(idx)];
        } else {
            d.* = 0; // null value
        }
    }
}

/// Gather i64 values by indices
pub fn gatherI64(src: []const i64, indices: []const i32, dst: []i64) void {
    for (dst, indices) |*d, idx| {
        if (idx >= 0 and @as(usize, @intCast(idx)) < src.len) {
            d.* = src[@intCast(idx)];
        } else {
            d.* = 0;
        }
    }
}

/// Gather i32 values by indices
pub fn gatherI32(src: []const i32, indices: []const i32, dst: []i32) void {
    for (dst, indices) |*d, idx| {
        if (idx >= 0 and @as(usize, @intCast(idx)) < src.len) {
            d.* = src[@intCast(idx)];
        } else {
            d.* = 0;
        }
    }
}

/// Gather f32 values by indices
pub fn gatherF32(src: []const f32, indices: []const i32, dst: []f32) void {
    for (dst, indices) |*d, idx| {
        if (idx >= 0 and @as(usize, @intCast(idx)) < src.len) {
            d.* = src[@intCast(idx)];
        } else {
            d.* = 0;
        }
    }
}

/// Build hash table for join - returns group IDs for each unique hash
/// Uses a simple open addressing hash table
pub fn buildJoinHashTable(
    hashes: []const u64,
    table: []i32, // hash table: -1 = empty, otherwise row index
    next: []i32, // chain for collisions: -1 = end, otherwise next row with same hash
    table_size: u32,
) void {
    // Initialize table to -1 (empty)
    @memset(table, -1);
    @memset(next, -1);

    // Insert each row
    for (hashes, 0..) |hash, row| {
        const slot: usize = @intCast(hash % table_size);
        const row_i32: i32 = @intCast(row);

        if (table[slot] == -1) {
            // Empty slot - insert directly
            table[slot] = row_i32;
        } else {
            // Collision - chain
            next[row] = table[slot];
            table[slot] = row_i32;
        }
    }
}

/// Build hash table directly from i64 keys using fastIntHash
/// This avoids pre-computing hashes in a separate array
fn buildJoinHashTableFast(
    keys: []const i64,
    table: []i32,
    next: []i32,
    table_size: u32,
) void {
    // Initialize table to -1 (empty)
    @memset(table, -1);
    @memset(next, -1);

    // Use bitwise AND (table_size is power of 2)
    const mask: u64 = table_size - 1;

    // Insert each row
    for (keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        const row_i32: i32 = @intCast(row);

        if (table[slot] == -1) {
            table[slot] = row_i32;
        } else {
            next[row] = table[slot];
            table[slot] = row_i32;
        }
    }
}

/// Probe hash table for join - finds matching rows
/// Returns number of matches found
pub fn probeJoinHashTable(
    probe_hashes: []const u64,
    probe_keys: []const i64,
    build_keys: []const i64,
    table: []const i32,
    next: []const i32,
    table_size: u32,
    out_probe_indices: []i32,
    out_build_indices: []i32,
    max_matches: u32,
) u32 {
    var match_count: u32 = 0;

    for (probe_hashes, 0..) |hash, probe_row| {
        const slot: usize = @intCast(hash % table_size);
        var build_row = table[slot];

        while (build_row != -1 and match_count < max_matches) {
            const build_idx: usize = @intCast(build_row);

            // Verify key match (not just hash match)
            if (probe_keys[probe_row] == build_keys[build_idx]) {
                out_probe_indices[match_count] = @intCast(probe_row);
                out_build_indices[match_count] = build_row;
                match_count += 1;
            }

            build_row = next[build_idx];
        }
    }

    return match_count;
}

// ============================================================================
// End-to-End Inner Join (Single CGO Call)
// ============================================================================

/// Result of end-to-end inner join
pub const InnerJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32,
    num_matches: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *InnerJoinResult) void {
        self.allocator.free(self.left_indices);
        self.allocator.free(self.right_indices);
    }
};

/// Memory-efficient inner join for i64 keys
/// - Single pass with dynamic array growth
/// - Computes left hashes on-the-fly using fastIntHash (saves 8 bytes per left row)
/// - Uses adaptive hash table sizing
/// - Interleaved 4-key probing for better memory latency hiding
pub fn innerJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Size hash table based on adaptive cardinality estimate
    const table_size = optimalJoinTableSize(right_n, right_keys);

    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);

    // Build hash table directly using fastIntHash (no separate hash array)
    buildJoinHashTableFast(right_keys, table, next, table_size);

    // Use bitwise AND instead of modulo (table_size is power of 2)
    const mask: u64 = table_size - 1;

    // Better initial capacity: start with left_n to minimize reallocs
    var capacity: usize = @max(left_n, 4096);
    var left_indices = try allocator.alloc(i32, capacity);
    var right_indices = try allocator.alloc(i32, capacity);
    var idx: usize = 0;

    // Interleaved probing: process 4 keys at once to hide memory latency
    const BATCH_SIZE: usize = 4;
    const main_end = if (left_n > BATCH_SIZE) left_n - BATCH_SIZE else 0;

    var i: usize = 0;

    // Main loop - process 4 keys at a time with interleaved memory access
    while (i < main_end) : (i += BATCH_SIZE) {
        // Load 4 keys and compute hashes (fast integer hash)
        const key0 = left_keys[i];
        const key1 = left_keys[i + 1];
        const key2 = left_keys[i + 2];
        const key3 = left_keys[i + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch all 4 table slots
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        // Also prefetch ahead for next batch
        if (i + BATCH_SIZE + 8 < left_n) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[i + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        // Process each key's chain
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        left_indices = try allocator.realloc(left_indices, new_capacity);
                        right_indices = try allocator.realloc(right_indices, new_capacity);
                        capacity = new_capacity;
                    }
                    left_indices[idx] = @intCast(i + offset);
                    right_indices[idx] = build_row;
                    idx += 1;
                }
                build_row = next_row;
            }
        }
    }

    // Tail loop - process remaining elements one at a time
    while (i < left_n) : (i += 1) {
        const key = left_keys[i];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];
            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    left_indices = try allocator.realloc(left_indices, new_capacity);
                    right_indices = try allocator.realloc(right_indices, new_capacity);
                    capacity = new_capacity;
                }
                left_indices[idx] = @intCast(i);
                right_indices[idx] = build_row;
                idx += 1;
            }
            build_row = next_row;
        }
    }

    // Shrink to actual size if significantly over-allocated
    if (idx > 0 and idx < capacity / 2) {
        left_indices = try allocator.realloc(left_indices, idx);
        right_indices = try allocator.realloc(right_indices, idx);
    } else if (idx == 0) {
        allocator.free(left_indices);
        allocator.free(right_indices);
        left_indices = try allocator.alloc(i32, 0);
        right_indices = try allocator.alloc(i32, 0);
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(idx),
        .allocator = allocator,
    };
}

/// Single-pass inner join with prefetching for better cache performance
pub fn innerJoinI64SinglePass(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Compute hashes for both sides
    const left_hashes = try allocator.alloc(u64, left_n);
    defer allocator.free(left_hashes);
    hashInt64Column(left_keys, left_hashes);

    const right_hashes = try allocator.alloc(u64, right_n);
    defer allocator.free(right_hashes);
    hashInt64Column(right_keys, right_hashes);

    // Size hash table based on quick cardinality estimate
    const table_size = optimalJoinTableSize(right_n, right_keys);

    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);

    buildJoinHashTable(right_hashes, table, next, table_size);

    // Estimate capacity based on smaller side - common case for joins
    // Use larger estimate to minimize reallocations
    var capacity: usize = @min(left_n, right_n) * 2;
    capacity = @max(capacity, 4096);
    var left_indices = try allocator.alloc(i32, capacity);
    var right_indices = try allocator.alloc(i32, capacity);
    var idx: usize = 0;

    // Prefetch distance for cache optimization
    const PREFETCH_DIST: usize = 8;

    var i: usize = 0;
    while (i < left_n) : (i += 1) {
        // Prefetch future hash table slots
        if (i + PREFETCH_DIST < left_n) {
            const future_slot = left_hashes[i + PREFETCH_DIST] % table_size;
            @prefetch(@as([*]const i32, @ptrCast(&table[future_slot])), .{ .locality = 1 });
        }

        const hash = left_hashes[i];
        const slot: usize = @intCast(hash % table_size);
        var build_row = table[slot];

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            if (left_keys[i] == right_keys[build_idx]) {
                // Grow if needed - check less frequently with larger growth
                if (idx >= capacity) {
                    capacity = capacity + capacity / 2; // 1.5x growth
                    left_indices = try allocator.realloc(left_indices, capacity);
                    right_indices = try allocator.realloc(right_indices, capacity);
                }
                left_indices[idx] = @intCast(i);
                right_indices[idx] = build_row;
                idx += 1;
            }
            build_row = next[build_idx];
        }
    }

    // Shrink to actual size (avoid if close to capacity to reduce alloc overhead)
    if (idx > 0 and idx < capacity / 2) {
        left_indices = try allocator.realloc(left_indices, idx);
        right_indices = try allocator.realloc(right_indices, idx);
    } else if (idx == 0) {
        allocator.free(left_indices);
        allocator.free(right_indices);
        left_indices = try allocator.alloc(i32, 0);
        right_indices = try allocator.alloc(i32, 0);
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(idx),
        .allocator = allocator,
    };
}

/// Fast cardinality estimation using small sample
/// Returns multiplier to use: 2 (high cardinality), 4 (medium), or 8 (low)
fn estimateCardinalityMultiplier(keys: []const i64) usize {
    const n = keys.len;
    if (n <= 100) return 4; // Default for small

    // Quick check: sample first 64 keys for duplicates
    // This is very fast and gives us a hint about data characteristics
    const sample_size: usize = @min(64, n);
    var duplicates: usize = 0;

    // Simple duplicate detection in small sample
    for (1..sample_size) |i| {
        const key = keys[i];
        for (keys[0..i]) |prev| {
            if (prev == key) {
                duplicates += 1;
                break;
            }
        }
    }

    // High duplicates in sample -> low cardinality -> need larger table
    // No duplicates in sample -> high cardinality -> smaller table is fine
    if (duplicates == 0) {
        return 4; // High cardinality - 4x for good performance
    } else if (duplicates < sample_size / 4) {
        return 6; // Medium cardinality
    } else {
        return 8; // Low cardinality - need more space for chains
    }
}

/// Calculate optimal hash table size based on quick cardinality estimate
fn optimalJoinTableSize(num_keys: usize, keys: []const i64) u32 {
    const multiplier = estimateCardinalityMultiplier(keys);
    var size = num_keys * multiplier;

    // Cap at reasonable size
    size = @min(size, 16 * 1024 * 1024);

    return nextPowerOf2Join(@intCast(size));
}

fn nextPowerOf2Join(n: u32) u32 {
    if (n <= 1) return 2;
    var v = n - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

// ============================================================================
// Swiss Table Join Implementation
// ============================================================================

/// Swiss Table entry for join - stores key and row chain
const SwissJoinEntry = struct {
    key: i64, // Key value (stored directly for fast comparison)
    head: i32, // First row index with this key
    count: u32, // Number of rows with this key
};

/// Swiss Table for join build side
/// Uses SIMD control byte probing like groupby
pub const SwissJoinTable = struct {
    ctrl: []u8,
    entries: []SwissJoinEntry,
    next: []i32, // Chain for rows with same key: next[row] = next row with same key, or -1
    mask: usize,
    count: u32,
    allocator: std.mem.Allocator,

    const CTRL_EMPTY: u8 = 0x00;
    const CTRL_GROUP_SIZE: usize = 16;
    const LOAD_FACTOR_PERCENT: usize = 87;

    /// Extract h2 (top 7 bits with high bit set) for control byte
    inline fn h2(hash: u64) u8 {
        return @as(u8, @truncate(hash >> 57)) | 0x80;
    }

    pub fn init(allocator: std.mem.Allocator, estimated_keys: usize, num_rows: usize) !SwissJoinTable {
        // Round up to power of 2, minimum 16
        var size: usize = 16;
        while (size < estimated_keys * 100 / LOAD_FACTOR_PERCENT) {
            size *= 2;
        }

        const ctrl = try allocator.alloc(u8, size + CTRL_GROUP_SIZE);
        @memset(ctrl, CTRL_EMPTY);

        const entries = try allocator.alloc(SwissJoinEntry, size);
        const next = try allocator.alloc(i32, num_rows);
        @memset(next, -1);

        return SwissJoinTable{
            .ctrl = ctrl,
            .entries = entries,
            .next = next,
            .mask = size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SwissJoinTable) void {
        self.allocator.free(self.ctrl);
        self.allocator.free(self.entries);
        self.allocator.free(self.next);
    }

    /// Insert a row into the join table
    /// Uses linear probing with SIMD for faster lookups
    pub fn insert(self: *SwissJoinTable, hash: u64, key: i64, row_idx: i32) !void {
        const ctrl_byte = h2(hash);
        const ctrl_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(ctrl_byte);
        const empty_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(CTRL_EMPTY);

        var pos = hash & self.mask;
        const table_len = self.entries.len;
        var probe_count: usize = 0;

        while (probe_count < table_len) {
            const ctrl_group: @Vector(CTRL_GROUP_SIZE, u8) = self.ctrl[pos..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte (potential key match)
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (pos + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                // Direct key comparison
                if (entry.key == key) {
                    // Key exists - add row to chain
                    self.next[@intCast(row_idx)] = entry.head;
                    entry.head = row_idx;
                    entry.count += 1;
                    return;
                }
                match_bits &= match_bits - 1;
            }

            // Check for empty slot
            const empty_mask = ctrl_group == empty_vec;
            const empty_bits = @as(u16, @bitCast(empty_mask));

            if (empty_bits != 0) {
                const bit_pos = @ctz(empty_bits);
                const slot = (pos + bit_pos) & self.mask;

                // Insert new entry
                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[table_len + slot] = ctrl_byte;
                }

                self.entries[slot] = .{
                    .key = key,
                    .head = row_idx,
                    .count = 1,
                };
                self.count += 1;
                return;
            }

            // Linear probe to next position
            pos = (pos + 1) & self.mask;
            probe_count += 1;
        }
    }

    /// Find entry for a key, returns null if not found
    pub fn find(self: *const SwissJoinTable, hash: u64, key: i64) ?*const SwissJoinEntry {
        const ctrl_byte = h2(hash);
        const ctrl_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(ctrl_byte);
        const empty_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(CTRL_EMPTY);

        var pos = hash & self.mask;
        const table_len = self.entries.len;
        var probe_count: usize = 0;

        while (probe_count < table_len) {
            const ctrl_group: @Vector(CTRL_GROUP_SIZE, u8) = self.ctrl[pos..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (pos + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                if (entry.key == key) {
                    return entry;
                }
                match_bits &= match_bits - 1;
            }

            // Check for empty slot (key doesn't exist)
            const empty_mask = ctrl_group == empty_vec;
            if (@as(u16, @bitCast(empty_mask)) != 0) {
                return null;
            }

            // Linear probe to next position
            pos = (pos + 1) & self.mask;
            probe_count += 1;
        }
        return null;
    }
};

/// Swiss Table based inner join - faster than chained hash table
pub fn innerJoinI64Swiss(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Estimate unique keys (assume ~10% of rows are unique)
    const estimated_keys = @max(right_n / 10, 1024);

    // Build Swiss Table on right (build) side
    var table = try SwissJoinTable.init(allocator, estimated_keys, right_n);
    defer table.deinit();

    // Insert all right rows
    for (right_keys, 0..) |key, i| {
        const hash = rapidHash64(@bitCast(key));
        try table.insert(hash, key, @intCast(i));
    }

    // First pass: count matches
    var match_count: usize = 0;
    for (left_keys) |key| {
        const hash = rapidHash64(@bitCast(key));
        if (table.find(hash, key)) |entry| {
            match_count += entry.count;
        }
    }

    // Allocate result arrays
    const left_indices = try allocator.alloc(i32, match_count);
    const right_indices = try allocator.alloc(i32, match_count);

    // Second pass: fill in indices
    var idx: usize = 0;
    for (left_keys, 0..) |key, left_row| {
        const hash = rapidHash64(@bitCast(key));
        if (table.find(hash, key)) |entry| {
            // Walk the chain of right rows with this key
            var right_row = entry.head;
            while (right_row != -1) {
                left_indices[idx] = @intCast(left_row);
                right_indices[idx] = right_row;
                idx += 1;
                right_row = table.next[@intCast(right_row)];
            }
        }
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(match_count),
        .allocator = allocator,
    };
}

// ============================================================================
// Multi-threaded Operations
// ============================================================================

/// Thread context for parallel sum by group
const ParallelSumContext = struct {
    data: []const f64,
    group_ids: []const u32,
    partial_sums: []f64, // Thread-local partial sums
    num_groups: usize,
    start_idx: usize,
    end_idx: usize,
};

fn parallelSumWorker(ctx: *ParallelSumContext) void {
    const data = ctx.data;
    const group_ids = ctx.group_ids;
    const partial_sums = ctx.partial_sums;

    var i = ctx.start_idx;
    while (i < ctx.end_idx) : (i += 1) {
        const gid = group_ids[i];
        partial_sums[gid] += data[i];
    }
}

/// Parallel sum aggregation by group
/// Splits work across threads, each building partial sums, then merges
pub fn parallelAggregateSumF64ByGroup(
    allocator: std.mem.Allocator,
    data: []const f64,
    group_ids: []const u32,
    num_groups: usize,
) ![]f64 {
    const n = data.len;

    // For small data, use single-threaded version
    if (n < 10000 or num_groups < 100) {
        const out_sums = try allocator.alloc(f64, num_groups);
        @memset(out_sums, 0);
        aggregateSumByGroup(f64, data, group_ids, out_sums);
        return out_sums;
    }

    const num_threads = @min(getMaxThreads(), n / 1000);
    const chunk_size = (n + num_threads - 1) / num_threads;

    // Allocate partial sum buffers for each thread
    const thread_sums = try allocator.alloc([]f64, num_threads);
    defer allocator.free(thread_sums);

    for (thread_sums) |*ts| {
        ts.* = try allocator.alloc(f64, num_groups);
        @memset(ts.*, 0);
    }
    defer {
        for (thread_sums) |ts| {
            allocator.free(ts);
        }
    }

    // Create thread contexts
    var contexts = try allocator.alloc(ParallelSumContext, num_threads);
    defer allocator.free(contexts);

    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, n);
        contexts[t] = ParallelSumContext{
            .data = data,
            .group_ids = group_ids,
            .partial_sums = thread_sums[t],
            .num_groups = num_groups,
            .start_idx = start,
            .end_idx = end,
        };
    }

    // Spawn threads
    var threads: [MAX_THREADS]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..num_threads) |t| {
        threads[t] = std.Thread.spawn(.{}, parallelSumWorker, .{&contexts[t]}) catch {
            // If thread spawn fails, run remaining work in main thread
            for (t..num_threads) |remaining| {
                parallelSumWorker(&contexts[remaining]);
            }
            break;
        };
        spawned += 1;
    }

    // Wait for all threads
    for (threads[0..spawned]) |thread| {
        thread.join();
    }

    // Merge partial sums
    const final_sums = try allocator.alloc(f64, num_groups);
    @memset(final_sums, 0);

    for (thread_sums) |ts| {
        for (0..num_groups) |g| {
            final_sums[g] += ts[g];
        }
    }

    return final_sums;
}

/// Thread context for memory-efficient single-pass parallel join probe
/// Computes hashes on-the-fly and uses thread-local dynamic arrays
const SinglePassProbeContext = struct {
    left_keys: []const i64,
    right_keys: []const i64,
    table: []const i32,
    next: []const i32,
    table_size: u32,
    start_idx: usize,
    end_idx: usize,
    allocator: std.mem.Allocator,
    // Output - thread-local dynamic arrays
    left_indices: []i32,
    right_indices: []i32,
    count: usize,
    capacity: usize,
    alloc_failed: bool,
};

fn singlePassProbeWorker(ctx: *SinglePassProbeContext) void {
    const left_keys = ctx.left_keys;
    const right_keys = ctx.right_keys;
    const table = ctx.table;
    const next = ctx.next;
    const table_size = ctx.table_size;
    const allocator = ctx.allocator;

    // Use bitwise AND instead of modulo (table_size is power of 2)
    const mask: u64 = table_size - 1;

    const start = ctx.start_idx;
    const end = ctx.end_idx;
    const chunk_size = end - start;

    // Better initial capacity
    var capacity: usize = @max(chunk_size, 4096);
    var left_out = allocator.alloc(i32, capacity) catch {
        ctx.alloc_failed = true;
        return;
    };
    var right_out = allocator.alloc(i32, capacity) catch {
        allocator.free(left_out);
        ctx.alloc_failed = true;
        return;
    };
    var idx: usize = 0;

    // Interleaved probing: process 4 keys at once to hide memory latency
    const BATCH_SIZE: usize = 4;
    const main_end = if (end > start + BATCH_SIZE) end - BATCH_SIZE else start;

    var left_row: usize = start;

    // Main loop - process 4 keys at a time with interleaved memory access
    while (left_row < main_end) : (left_row += BATCH_SIZE) {
        // Load 4 keys and compute hashes (fast integer hash)
        const key0 = left_keys[left_row];
        const key1 = left_keys[left_row + 1];
        const key2 = left_keys[left_row + 2];
        const key3 = left_keys[left_row + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch all 4 table slots
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        // Also prefetch ahead for next batch
        if (left_row + BATCH_SIZE + 8 < end) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[left_row + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        // Process each key's chain
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        const new_left = allocator.realloc(left_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(left_out);
                            allocator.free(right_out);
                            return;
                        };
                        const new_right = allocator.realloc(right_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(new_left);
                            return;
                        };
                        left_out = new_left;
                        right_out = new_right;
                        capacity = new_capacity;
                    }
                    left_out[idx] = @intCast(left_row + offset);
                    right_out[idx] = build_row;
                    idx += 1;
                }
                build_row = next_row;
            }
        }
    }

    // Tail loop - process remaining elements one at a time
    while (left_row < end) : (left_row += 1) {
        const key = left_keys[left_row];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];
            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    const new_left = allocator.realloc(left_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(left_out);
                        allocator.free(right_out);
                        return;
                    };
                    const new_right = allocator.realloc(right_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(new_left);
                        return;
                    };
                    left_out = new_left;
                    right_out = new_right;
                    capacity = new_capacity;
                }
                left_out[idx] = @intCast(left_row);
                right_out[idx] = build_row;
                idx += 1;
            }
            build_row = next_row;
        }
    }

    ctx.left_indices = left_out;
    ctx.right_indices = right_out;
    ctx.count = idx;
    ctx.capacity = capacity;
    ctx.alloc_failed = false;
}

/// Memory-efficient parallel inner join
/// - Single pass (no count-then-fill)
/// - Computes left hashes on-the-fly (saves 8 bytes per left row)
/// - Uses adaptive hash table sizing
pub fn parallelInnerJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For small data, use single-threaded version
    if (left_n < 10000) {
        return innerJoinI64(allocator, left_keys, right_keys);
    }

    // Size hash table based on adaptive cardinality estimate
    const table_size = optimalJoinTableSize(right_n, right_keys);

    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);

    // Build hash table directly using fastIntHash (no separate hash array)
    // This saves memory and uses the same hash function as probe phase
    buildJoinHashTableFast(right_keys, table, next, table_size);

    // Single-pass parallel probe with thread-local dynamic arrays
    const num_threads = @min(getMaxThreads(), left_n / 10000);
    const actual_threads = @max(num_threads, 1);
    const chunk_size = (left_n + actual_threads - 1) / actual_threads;

    var contexts = try allocator.alloc(SinglePassProbeContext, actual_threads);
    defer allocator.free(contexts);

    // Initialize contexts
    for (0..actual_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, left_n);
        contexts[t] = SinglePassProbeContext{
            .left_keys = left_keys,
            .right_keys = right_keys,
            .table = table,
            .next = next,
            .table_size = @intCast(table_size),
            .start_idx = start,
            .end_idx = end,
            .allocator = allocator,
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .count = 0,
            .capacity = 0,
            .alloc_failed = false,
        };
    }

    // Spawn threads for single-pass probe
    var threads: [MAX_THREADS]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..actual_threads) |t| {
        threads[t] = std.Thread.spawn(.{}, singlePassProbeWorker, .{&contexts[t]}) catch {
            // Fallback to sequential if thread spawn fails
            for (t..actual_threads) |remaining| {
                singlePassProbeWorker(&contexts[remaining]);
            }
            break;
        };
        spawned += 1;
    }

    for (threads[0..spawned]) |thread| {
        thread.join();
    }

    // Check for allocation failures and calculate total matches
    var total_matches: usize = 0;
    for (contexts) |ctx| {
        if (ctx.alloc_failed) {
            // Clean up any successful allocations
            for (contexts) |c| {
                if (!c.alloc_failed and c.capacity > 0) {
                    allocator.free(c.left_indices.ptr[0..c.capacity]);
                    allocator.free(c.right_indices.ptr[0..c.capacity]);
                }
            }
            return error.OutOfMemory;
        }
        total_matches += ctx.count;
    }

    // Merge thread-local results into final arrays
    const left_indices = try allocator.alloc(i32, total_matches);
    const right_indices = try allocator.alloc(i32, total_matches);

    var offset: usize = 0;
    for (contexts) |ctx| {
        if (ctx.count > 0) {
            @memcpy(left_indices[offset .. offset + ctx.count], ctx.left_indices[0..ctx.count]);
            @memcpy(right_indices[offset .. offset + ctx.count], ctx.right_indices[0..ctx.count]);
            offset += ctx.count;
        }
        // Free thread-local arrays
        if (ctx.capacity > 0) {
            allocator.free(ctx.left_indices.ptr[0..ctx.capacity]);
            allocator.free(ctx.right_indices.ptr[0..ctx.capacity]);
        }
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(total_matches),
        .allocator = allocator,
    };
}

// ============================================================================
// Left Join Implementation
// ============================================================================

/// Result of left join - includes all left rows, unmatched have right_index = -1
pub const LeftJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32, // -1 for unmatched left rows
    num_rows: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *LeftJoinResult) void {
        self.allocator.free(self.left_indices);
        self.allocator.free(self.right_indices);
    }
};

/// Single-threaded left join with fastIntHash and interleaved probing
pub fn leftJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    // Handle empty right - all left rows with -1
    if (right_n == 0) {
        const left_indices = try allocator.alloc(i32, left_n);
        const right_indices = try allocator.alloc(i32, left_n);
        for (0..left_n) |i| {
            left_indices[i] = @intCast(i);
            right_indices[i] = -1;
        }
        return LeftJoinResult{
            .left_indices = left_indices,
            .right_indices = right_indices,
            .num_rows = @intCast(left_n),
            .allocator = allocator,
        };
    }

    if (left_n == 0) {
        return LeftJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_rows = 0,
            .allocator = allocator,
        };
    }

    // Build hash table
    const table_size = optimalJoinTableSize(right_n, right_keys);
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);
    buildJoinHashTableFast(right_keys, table, next, table_size);

    const mask: u64 = table_size - 1;

    // Capacity for results - at minimum left_n (all unmatched), could be more with duplicates
    var capacity: usize = left_n + left_n / 2;
    capacity = @max(capacity, 4096);
    var left_indices = try allocator.alloc(i32, capacity);
    var right_indices = try allocator.alloc(i32, capacity);
    var idx: usize = 0;

    // Interleaved 4-key probing
    const BATCH_SIZE: usize = 4;
    const main_end = if (left_n > BATCH_SIZE) left_n - BATCH_SIZE else 0;

    var i: usize = 0;

    while (i < main_end) : (i += BATCH_SIZE) {
        const key0 = left_keys[i];
        const key1 = left_keys[i + 1];
        const key2 = left_keys[i + 2];
        const key3 = left_keys[i + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        if (i + BATCH_SIZE + 8 < left_n) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[i + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        // Track matches for each key in batch
        var matched: [4]bool = .{ false, false, false, false };

        // Process each key
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        left_indices = try allocator.realloc(left_indices, new_capacity);
                        right_indices = try allocator.realloc(right_indices, new_capacity);
                        capacity = new_capacity;
                    }
                    left_indices[idx] = @intCast(i + offset);
                    right_indices[idx] = build_row;
                    idx += 1;
                    matched[offset] = true;
                }
                build_row = next_row;
            }
        }

        // Add unmatched rows
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            if (!matched[offset]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    left_indices = try allocator.realloc(left_indices, new_capacity);
                    right_indices = try allocator.realloc(right_indices, new_capacity);
                    capacity = new_capacity;
                }
                left_indices[idx] = @intCast(i + offset);
                right_indices[idx] = -1;
                idx += 1;
            }
        }
    }

    // Tail loop
    while (i < left_n) : (i += 1) {
        const key = left_keys[i];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];
        var matched_row = false;

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];
            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    left_indices = try allocator.realloc(left_indices, new_capacity);
                    right_indices = try allocator.realloc(right_indices, new_capacity);
                    capacity = new_capacity;
                }
                left_indices[idx] = @intCast(i);
                right_indices[idx] = build_row;
                idx += 1;
                matched_row = true;
            }
            build_row = next_row;
        }

        if (!matched_row) {
            if (idx >= capacity) {
                const new_capacity = capacity * 2;
                left_indices = try allocator.realloc(left_indices, new_capacity);
                right_indices = try allocator.realloc(right_indices, new_capacity);
                capacity = new_capacity;
            }
            left_indices[idx] = @intCast(i);
            right_indices[idx] = -1;
            idx += 1;
        }
    }

    // Shrink to actual size
    if (idx > 0 and idx < capacity / 2) {
        left_indices = try allocator.realloc(left_indices, idx);
        right_indices = try allocator.realloc(right_indices, idx);
    } else if (idx == 0) {
        allocator.free(left_indices);
        allocator.free(right_indices);
        left_indices = try allocator.alloc(i32, 0);
        right_indices = try allocator.alloc(i32, 0);
    }

    return LeftJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_rows = @intCast(idx),
        .allocator = allocator,
    };
}

/// Context for parallel left join probe workers
const LeftProbeContext = struct {
    left_keys: []const i64,
    right_keys: []const i64,
    table: []const i32,
    next: []const i32,
    table_size: u64,
    start_idx: usize,
    end_idx: usize,
    allocator: std.mem.Allocator,
    // Output
    left_indices: []i32,
    right_indices: []i32,
    count: usize,
    capacity: usize,
    alloc_failed: bool,
};

fn leftProbeWorker(ctx: *LeftProbeContext) void {
    const left_keys = ctx.left_keys;
    const right_keys = ctx.right_keys;
    const table = ctx.table;
    const next = ctx.next;
    const table_size = ctx.table_size;
    const allocator = ctx.allocator;
    const mask: u64 = table_size - 1;

    const start = ctx.start_idx;
    const end = ctx.end_idx;
    const chunk_size = end - start;

    // Initial capacity - at least chunk_size for all unmatched case
    var capacity: usize = chunk_size + chunk_size / 2;
    capacity = @max(capacity, 1024);

    var left_out = allocator.alloc(i32, capacity) catch {
        ctx.alloc_failed = true;
        return;
    };
    var right_out = allocator.alloc(i32, capacity) catch {
        allocator.free(left_out);
        ctx.alloc_failed = true;
        return;
    };
    var idx: usize = 0;

    const BATCH_SIZE: usize = 4;
    const main_end = if (end > start + BATCH_SIZE) end - BATCH_SIZE else start;

    var left_row: usize = start;

    while (left_row < main_end) : (left_row += BATCH_SIZE) {
        const key0 = left_keys[left_row];
        const key1 = left_keys[left_row + 1];
        const key2 = left_keys[left_row + 2];
        const key3 = left_keys[left_row + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        if (left_row + BATCH_SIZE + 8 < end) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[left_row + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        var matched: [4]bool = .{ false, false, false, false };

        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        const new_left = allocator.realloc(left_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(left_out);
                            allocator.free(right_out);
                            return;
                        };
                        const new_right = allocator.realloc(right_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(new_left);
                            return;
                        };
                        left_out = new_left;
                        right_out = new_right;
                        capacity = new_capacity;
                    }
                    left_out[idx] = @intCast(left_row + offset);
                    right_out[idx] = build_row;
                    idx += 1;
                    matched[offset] = true;
                }
                build_row = next_row;
            }
        }

        // Add unmatched rows
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            if (!matched[offset]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    const new_left = allocator.realloc(left_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(left_out);
                        allocator.free(right_out);
                        return;
                    };
                    const new_right = allocator.realloc(right_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(new_left);
                        return;
                    };
                    left_out = new_left;
                    right_out = new_right;
                    capacity = new_capacity;
                }
                left_out[idx] = @intCast(left_row + offset);
                right_out[idx] = -1;
                idx += 1;
            }
        }
    }

    // Tail loop
    while (left_row < end) : (left_row += 1) {
        const key = left_keys[left_row];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];
        var matched_row = false;

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];
            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    const new_left = allocator.realloc(left_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(left_out);
                        allocator.free(right_out);
                        return;
                    };
                    const new_right = allocator.realloc(right_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(new_left);
                        return;
                    };
                    left_out = new_left;
                    right_out = new_right;
                    capacity = new_capacity;
                }
                left_out[idx] = @intCast(left_row);
                right_out[idx] = build_row;
                idx += 1;
                matched_row = true;
            }
            build_row = next_row;
        }

        if (!matched_row) {
            if (idx >= capacity) {
                const new_capacity = capacity * 2;
                const new_left = allocator.realloc(left_out, new_capacity) catch {
                    ctx.alloc_failed = true;
                    allocator.free(left_out);
                    allocator.free(right_out);
                    return;
                };
                const new_right = allocator.realloc(right_out, new_capacity) catch {
                    ctx.alloc_failed = true;
                    allocator.free(new_left);
                    return;
                };
                left_out = new_left;
                right_out = new_right;
                capacity = new_capacity;
            }
            left_out[idx] = @intCast(left_row);
            right_out[idx] = -1;
            idx += 1;
        }
    }

    ctx.left_indices = left_out;
    ctx.right_indices = right_out;
    ctx.count = idx;
    ctx.capacity = capacity;
    ctx.alloc_failed = false;
}

/// Parallel left join
pub fn parallelLeftJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    // Handle empty right
    if (right_n == 0) {
        const left_indices = try allocator.alloc(i32, left_n);
        const right_indices = try allocator.alloc(i32, left_n);
        for (0..left_n) |i| {
            left_indices[i] = @intCast(i);
            right_indices[i] = -1;
        }
        return LeftJoinResult{
            .left_indices = left_indices,
            .right_indices = right_indices,
            .num_rows = @intCast(left_n),
            .allocator = allocator,
        };
    }

    if (left_n == 0) {
        return LeftJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_rows = 0,
            .allocator = allocator,
        };
    }

    // For small data, use single-threaded
    if (left_n < 10000) {
        return leftJoinI64(allocator, left_keys, right_keys);
    }

    // Build hash table
    const table_size = optimalJoinTableSize(right_n, right_keys);
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);
    buildJoinHashTableFast(right_keys, table, next, table_size);

    // Parallel probe
    const num_threads = @min(getMaxThreads(), left_n / 10000);
    const actual_threads = @max(num_threads, 1);
    const chunk_size = (left_n + actual_threads - 1) / actual_threads;

    var contexts = try allocator.alloc(LeftProbeContext, actual_threads);
    defer allocator.free(contexts);

    for (0..actual_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, left_n);
        contexts[t] = LeftProbeContext{
            .left_keys = left_keys,
            .right_keys = right_keys,
            .table = table,
            .next = next,
            .table_size = @intCast(table_size),
            .start_idx = start,
            .end_idx = end,
            .allocator = allocator,
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .count = 0,
            .capacity = 0,
            .alloc_failed = false,
        };
    }

    var threads: [MAX_THREADS]std.Thread = undefined;
    var spawned: usize = 0;

    for (0..actual_threads) |t| {
        threads[t] = std.Thread.spawn(.{}, leftProbeWorker, .{&contexts[t]}) catch {
            for (t..actual_threads) |remaining| {
                leftProbeWorker(&contexts[remaining]);
            }
            break;
        };
        spawned += 1;
    }

    for (threads[0..spawned]) |thread| {
        thread.join();
    }

    // Check for failures and count total
    var total_rows: usize = 0;
    for (contexts) |ctx| {
        if (ctx.alloc_failed) {
            for (contexts) |c| {
                if (!c.alloc_failed and c.capacity > 0) {
                    allocator.free(c.left_indices.ptr[0..c.capacity]);
                    allocator.free(c.right_indices.ptr[0..c.capacity]);
                }
            }
            return error.OutOfMemory;
        }
        total_rows += ctx.count;
    }

    // Merge results
    const left_indices = try allocator.alloc(i32, total_rows);
    const right_indices = try allocator.alloc(i32, total_rows);

    var offset: usize = 0;
    for (contexts) |ctx| {
        if (ctx.count > 0) {
            @memcpy(left_indices[offset .. offset + ctx.count], ctx.left_indices[0..ctx.count]);
            @memcpy(right_indices[offset .. offset + ctx.count], ctx.right_indices[0..ctx.count]);
            offset += ctx.count;
        }
        if (ctx.capacity > 0) {
            allocator.free(ctx.left_indices.ptr[0..ctx.capacity]);
            allocator.free(ctx.right_indices.ptr[0..ctx.capacity]);
        }
    }

    return LeftJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_rows = @intCast(total_rows),
        .allocator = allocator,
    };
}

// ============================================================================
// Lock-Free Parallel Hash Join (Polars-style)
// ============================================================================

/// Per-thread hash table for lock-free join
const PartitionedHashTable = struct {
    table: []i32,
    next: []i32,
    keys: []i64,
    hashes: []u64,
    row_indices: []u32, // Original row indices in the right table
    size: u32,
    count: u32,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator, estimated_keys: usize) !PartitionedHashTable {
        const table_size = nextPowerOf2Join(@intCast(@max(estimated_keys * 4, 16)));
        const table = try allocator.alloc(i32, table_size);
        @memset(table, -1);

        // Pre-allocate for estimated keys
        const capacity = @max(estimated_keys, 64);
        const next = try allocator.alloc(i32, capacity);
        const keys = try allocator.alloc(i64, capacity);
        const hashes = try allocator.alloc(u64, capacity);
        const row_indices = try allocator.alloc(u32, capacity);

        return PartitionedHashTable{
            .table = table,
            .next = next,
            .keys = keys,
            .hashes = hashes,
            .row_indices = row_indices,
            .size = @intCast(table_size),
            .count = 0,
            .allocator = allocator,
        };
    }

    fn deinit(self: *PartitionedHashTable) void {
        self.allocator.free(self.table);
        self.allocator.free(self.next);
        self.allocator.free(self.keys);
        self.allocator.free(self.hashes);
        self.allocator.free(self.row_indices);
    }

    fn insert(self: *PartitionedHashTable, hash: u64, key: i64, row_idx: u32) !void {
        const idx = self.count;

        // Grow arrays if needed
        if (idx >= self.next.len) {
            const new_cap = self.next.len * 2;
            self.next = try self.allocator.realloc(self.next, new_cap);
            self.keys = try self.allocator.realloc(self.keys, new_cap);
            self.hashes = try self.allocator.realloc(self.hashes, new_cap);
            self.row_indices = try self.allocator.realloc(self.row_indices, new_cap);
        }

        // Store key data
        self.keys[idx] = key;
        self.hashes[idx] = hash;
        self.row_indices[idx] = row_idx;

        // Insert into hash table (chaining)
        const slot = hash % self.size;
        self.next[idx] = self.table[slot];
        self.table[slot] = @intCast(idx);
        self.count += 1;
    }
};

/// Context for lock-free build phase
const LockFreeBuildContext = struct {
    right_keys: []const i64,
    right_hashes: []const u64,
    partition_table: *PartitionedHashTable,
    partition_id: usize,
    num_partitions: usize,
};

fn lockFreeBuildWorker(ctx: *LockFreeBuildContext) void {
    const right_keys = ctx.right_keys;
    const right_hashes = ctx.right_hashes;
    const partition_id = ctx.partition_id;
    const num_partitions = ctx.num_partitions;
    var table = ctx.partition_table;

    // Insert only keys that hash to this partition
    for (right_hashes, 0..) |hash, i| {
        if (hash % num_partitions == partition_id) {
            table.insert(hash, right_keys[i], @intCast(i)) catch {};
        }
    }
}

/// Context for lock-free probe phase
const LockFreeProbeContext = struct {
    left_keys: []const i64,
    left_hashes: []const u64,
    partition_tables: []PartitionedHashTable,
    num_partitions: usize,
    // Output - dynamically grown
    left_results: std.ArrayList(i32),
    right_results: std.ArrayList(i32),
};

fn lockFreeProbeWorker(ctx: *LockFreeProbeContext) void {
    const left_keys = ctx.left_keys;
    const left_hashes = ctx.left_hashes;
    const num_partitions = ctx.num_partitions;

    for (left_hashes, 0..) |hash, left_idx| {
        const partition_id = hash % num_partitions;
        const table = &ctx.partition_tables[partition_id];

        const slot = hash % table.size;
        var entry_idx = table.table[slot];

        while (entry_idx != -1) {
            const idx: usize = @intCast(entry_idx);
            if (table.keys[idx] == left_keys[left_idx]) {
                ctx.left_results.append(@intCast(left_idx)) catch {};
                ctx.right_results.append(@intCast(table.row_indices[idx])) catch {};
            }
            entry_idx = table.next[idx];
        }
    }
}

/// Lock-free parallel inner join with pre-partitioning
/// Step 1: Single-pass partition keys by hash % num_partitions
/// Step 2: Parallel build - each thread builds from its partition only
/// Step 3: Parallel probe - each thread probes its partition
pub fn innerJoinI64LockFree(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For small data, use single-threaded version
    if (right_n < 50000) {
        return innerJoinI64(allocator, left_keys, right_keys);
    }

    const num_partitions: usize = @min(getMaxThreads(), 8);

    // Compute hashes for both sides
    const left_hashes = try allocator.alloc(u64, left_n);
    defer allocator.free(left_hashes);
    hashInt64Column(left_keys, left_hashes);

    const right_hashes = try allocator.alloc(u64, right_n);
    defer allocator.free(right_hashes);
    hashInt64Column(right_keys, right_hashes);

    // Step 1: Count keys per partition (single pass)
    var right_partition_counts = [_]usize{0} ** MAX_THREADS;
    for (right_hashes) |hash| {
        right_partition_counts[hash % num_partitions] += 1;
    }

    // Allocate partition arrays
    var right_partitions: [MAX_THREADS][]u32 = undefined;
    var right_partition_pos = [_]usize{0} ** MAX_THREADS;
    for (0..num_partitions) |p| {
        right_partitions[p] = try allocator.alloc(u32, right_partition_counts[p]);
    }
    defer {
        for (0..num_partitions) |p| {
            allocator.free(right_partitions[p]);
        }
    }

    // Fill partitions with row indices
    for (right_hashes, 0..) |hash, i| {
        const p = hash % num_partitions;
        right_partitions[p][right_partition_pos[p]] = @intCast(i);
        right_partition_pos[p] += 1;
    }

    // Step 2: Build hash tables per partition (can be parallelized, but sequential for now)
    var partition_tables = try allocator.alloc(PartitionedHashTable, num_partitions);
    defer {
        for (partition_tables) |*t| {
            t.deinit();
        }
        allocator.free(partition_tables);
    }

    for (0..num_partitions) |p| {
        const count = right_partition_counts[p];
        partition_tables[p] = try PartitionedHashTable.init(allocator, count);

        // Build this partition's hash table
        for (right_partitions[p]) |row_idx| {
            const hash = right_hashes[row_idx];
            const key = right_keys[row_idx];
            try partition_tables[p].insert(hash, key, row_idx);
        }
    }

    // Step 3: Probe (single-threaded for now - simpler and often faster for small partitions)
    var left_results = try allocator.alloc(i32, left_n);
    var right_results = try allocator.alloc(i32, left_n);
    var result_count: usize = 0;
    var capacity: usize = left_n;

    for (left_hashes, 0..) |hash, left_idx| {
        const partition_id = hash % num_partitions;
        const table = &partition_tables[partition_id];

        const slot = hash % table.size;
        var entry_idx = table.table[slot];

        while (entry_idx != -1) {
            const idx: usize = @intCast(entry_idx);
            if (table.keys[idx] == left_keys[left_idx]) {
                if (result_count >= capacity) {
                    capacity = capacity * 2;
                    left_results = try allocator.realloc(left_results, capacity);
                    right_results = try allocator.realloc(right_results, capacity);
                }
                left_results[result_count] = @intCast(left_idx);
                right_results[result_count] = @intCast(table.row_indices[idx]);
                result_count += 1;
            }
            entry_idx = table.next[idx];
        }
    }

    // Shrink to actual size
    if (result_count < capacity) {
        left_results = try allocator.realloc(left_results, result_count);
        right_results = try allocator.realloc(right_results, result_count);
    }

    return InnerJoinResult{
        .left_indices = left_results,
        .right_indices = right_results,
        .num_matches = @intCast(result_count),
        .allocator = allocator,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "simd sum" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 };
    const result = sum(f64, &data);
    try std.testing.expectApproxEqAbs(@as(f64, 55.0), result, 0.0001);
}

test "simd sum large" {
    // Test with array larger than CHUNK_SIZE
    var data: [100]f64 = undefined;
    var expected: f64 = 0;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i + 1);
        expected += v.*;
    }
    const result = sum(f64, &data);
    try std.testing.expectApproxEqAbs(expected, result, 0.0001);
}

test "simd min/max" {
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0 };
    try std.testing.expectEqual(@as(f64, 1.0), min(f64, &data).?);
    try std.testing.expectEqual(@as(f64, 9.0), max(f64, &data).?);
}

test "simd min/max large" {
    var data: [100]f64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i + 1);
    }
    data[50] = 0.5; // Min
    data[75] = 150.0; // Max

    try std.testing.expectApproxEqAbs(@as(f64, 0.5), min(f64, &data).?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 150.0), max(f64, &data).?, 0.0001);
}

test "simd addScalar" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    addScalar(f64, &data, 10.0);
    try std.testing.expectApproxEqAbs(@as(f64, 11.0), data[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), data[4], 0.0001);
}

test "simd mulScalar" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    mulScalar(f64, &data, 2.0);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), data[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), data[4], 0.0001);
}

test "simd filter greater than" {
    const data = [_]f64{ 1.0, 5.0, 2.0, 8.0, 3.0, 9.0 };
    var indices: [6]u32 = undefined;
    const count = filterGreaterThan(f64, &data, 4.0, &indices);

    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(u32, 1), indices[0]); // 5.0
    try std.testing.expectEqual(@as(u32, 3), indices[1]); // 8.0
    try std.testing.expectEqual(@as(u32, 5), indices[2]); // 9.0
}

test "simd argsort ascending" {
    const data = [_]f64{ 3.0, 1.0, 4.0, 1.0, 5.0 };
    var indices: [5]u32 = undefined;
    argsort(f64, &data, &indices, true);

    // Indices should point to sorted order: 1, 3, 0, 2, 4 (values: 1, 1, 3, 4, 5)
    try std.testing.expectEqual(@as(u32, 1), indices[0]);
    try std.testing.expectEqual(@as(u32, 3), indices[1]);
    try std.testing.expectEqual(@as(u32, 0), indices[2]);
}

test "simd variance" {
    const data = [_]f64{ 2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0 };
    const v = variance(f64, &data).?;
    // Population variance of this data is 4.0, sample variance is 4.571...
    try std.testing.expectApproxEqAbs(@as(f64, 4.571), v, 0.01);
}
