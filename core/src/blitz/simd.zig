//! SIMD-Optimized Aggregations for Blitz
//!
//! High-performance vectorized implementations of common aggregation operations.
//! Uses multiple accumulators for instruction-level parallelism (ILP).
//!
//! These functions are used as the leaf computation in parallel reductions,
//! providing SIMD speedup within each thread's chunk.

const std = @import("std");

/// Vector width for SIMD operations (8 elements for AVX2/NEON)
pub const VECTOR_WIDTH: usize = 8;

/// Unroll factor for hiding latency
pub const UNROLL_FACTOR: usize = 4;

/// Chunk size for unrolled loops
pub const CHUNK_SIZE: usize = VECTOR_WIDTH * UNROLL_FACTOR;

// ============================================================================
// Sum
// ============================================================================

/// SIMD-optimized sum for integer types.
/// Uses 4 vector accumulators for instruction-level parallelism.
pub fn sum(comptime T: type, data: []const T) T {
    if (data.len == 0) return 0;

    // For small arrays, use scalar
    if (data.len < CHUNK_SIZE) {
        return sumScalar(T, data);
    }

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

    // Horizontal sum of vector
    var result: T = @reduce(.Add, combined);

    // Handle remainder
    for (data[unrolled_len..]) |v| {
        result += v;
    }

    return result;
}

/// Scalar sum for small arrays or remainder.
fn sumScalar(comptime T: type, data: []const T) T {
    var result: T = 0;
    for (data) |v| {
        result += v;
    }
    return result;
}

// ============================================================================
// Min
// ============================================================================

/// SIMD-optimized minimum for integer types.
pub fn min(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    if (data.len < CHUNK_SIZE) {
        return minScalar(T, data);
    }

    const Vec = @Vector(VECTOR_WIDTH, T);
    const max_val: T = std.math.maxInt(T);

    var acc0: Vec = @splat(max_val);
    var acc1: Vec = @splat(max_val);
    var acc2: Vec = @splat(max_val);
    var acc3: Vec = @splat(max_val);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        acc0 = @min(acc0, chunk0);
        acc1 = @min(acc1, chunk1);
        acc2 = @min(acc2, chunk2);
        acc3 = @min(acc3, chunk3);
    }

    // Combine accumulators
    const combined = @min(@min(acc0, acc1), @min(acc2, acc3));
    var result: T = @reduce(.Min, combined);

    // Handle remainder
    for (data[unrolled_len..]) |v| {
        if (v < result) result = v;
    }

    return result;
}

/// Scalar min for small arrays.
fn minScalar(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;
    var result: T = data[0];
    for (data[1..]) |v| {
        if (v < result) result = v;
    }
    return result;
}

// ============================================================================
// Max
// ============================================================================

/// SIMD-optimized maximum for integer types.
pub fn max(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    if (data.len < CHUNK_SIZE) {
        return maxScalar(T, data);
    }

    const Vec = @Vector(VECTOR_WIDTH, T);
    const min_val: T = std.math.minInt(T);

    var acc0: Vec = @splat(min_val);
    var acc1: Vec = @splat(min_val);
    var acc2: Vec = @splat(min_val);
    var acc3: Vec = @splat(min_val);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const chunk0: Vec = data[i..][0..VECTOR_WIDTH].*;
        const chunk1: Vec = data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const chunk2: Vec = data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const chunk3: Vec = data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        acc0 = @max(acc0, chunk0);
        acc1 = @max(acc1, chunk1);
        acc2 = @max(acc2, chunk2);
        acc3 = @max(acc3, chunk3);
    }

    // Combine accumulators
    const combined = @max(@max(acc0, acc1), @max(acc2, acc3));
    var result: T = @reduce(.Max, combined);

    // Handle remainder
    for (data[unrolled_len..]) |v| {
        if (v > result) result = v;
    }

    return result;
}

/// Scalar max for small arrays.
fn maxScalar(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;
    var result: T = data[0];
    for (data[1..]) |v| {
        if (v > result) result = v;
    }
    return result;
}

// ============================================================================
// Parallel SIMD Aggregations (combines parallelism + SIMD)
// ============================================================================

const api = @import("api.zig");
const threshold_mod = @import("threshold.zig");

/// Check if we should use parallel execution for a SIMD aggregation.
/// Uses the threshold module's cost model for dynamic calculation.
///
/// The formula considers:
/// - Number of workers (more workers = lower threshold)
/// - Operation cost (SIMD aggregations are ~2ns/element)
/// - Overhead multiplier (need 10x benefit to justify sync cost)
///
/// For SIMD aggregations, we use a higher bar because SIMD alone is very fast.
/// The threshold module gives us a base threshold, but we multiply by a
/// SIMD efficiency factor since single-threaded SIMD is already vectorized.
const SIMD_EFFICIENCY_FACTOR: usize = 100; // SIMD is ~100x faster than scalar

/// Calculate the parallel threshold for SIMD operations dynamically.
/// Returns the minimum array size where parallelism beats SIMD-only.
pub fn calculateParallelThreshold(op: threshold_mod.OpType) usize {
    const base_threshold = threshold_mod.getThreshold(op);

    // If the base returns maxInt (no workers), propagate that
    if (base_threshold == std.math.maxInt(usize)) {
        return base_threshold;
    }

    // SIMD is already vectorized, so we need more data to justify
    // the overhead of thread synchronization
    return base_threshold * SIMD_EFFICIENCY_FACTOR;
}

/// Get the parallel threshold for sum operations.
pub fn getParallelThreshold() usize {
    return calculateParallelThreshold(.sum);
}

/// Check if we should parallelize based on operation type and data size.
pub fn shouldParallelizeSimd(op: threshold_mod.OpType, len: usize) bool {
    // Memory-bound operations never benefit
    if (threshold_mod.isMemoryBound(op)) {
        return false;
    }

    // Check if pool is initialized
    if (!api.isInitialized()) {
        return false;
    }

    // Use dynamic threshold calculation
    return len >= calculateParallelThreshold(op);
}

/// Parallel SIMD sum - divides work across threads, each using SIMD.
pub fn parallelSum(comptime T: type, data: []const T) T {
    if (data.len == 0) return 0;

    // For small/medium data, just use SIMD (no parallelism overhead)
    // Uses dynamic threshold based on worker count and operation cost
    if (!shouldParallelizeSimd(.sum, data.len)) {
        return sum(T, data);
    }

    const Context = struct { slice: []const T };
    const ctx = Context{ .slice = data };

    // Use parallelReduceChunked with SIMD sum as the leaf operation
    return api.parallelReduceChunked(
        T,
        data.len,
        0,
        Context,
        ctx,
        struct {
            // Map function returns the SIMD sum of the chunk
            fn mapChunk(c: Context, start: usize, end: usize) T {
                return sum(T, c.slice[start..end]);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return a + b;
            }
        }.combine,
        // Use larger grain size since SIMD handles vectorization
        8192,
    );
}

/// Parallel SIMD max - divides work across threads, each using SIMD.
pub fn parallelMax(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    // Uses dynamic threshold based on worker count and operation cost
    if (!shouldParallelizeSimd(.max, data.len)) {
        return max(T, data);
    }

    const Context = struct { slice: []const T };
    const ctx = Context{ .slice = data };

    const result = api.parallelReduceChunked(
        T,
        data.len,
        std.math.minInt(T),
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, start: usize, end: usize) T {
                return max(T, c.slice[start..end]) orelse std.math.minInt(T);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return @max(a, b);
            }
        }.combine,
        8192,
    );

    return if (result == std.math.minInt(T)) null else result;
}

/// Parallel SIMD min - divides work across threads, each using SIMD.
pub fn parallelMin(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    // Uses dynamic threshold based on worker count and operation cost
    if (!shouldParallelizeSimd(.min, data.len)) {
        return min(T, data);
    }

    const Context = struct { slice: []const T };
    const ctx = Context{ .slice = data };

    const result = api.parallelReduceChunked(
        T,
        data.len,
        std.math.maxInt(T),
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, start: usize, end: usize) T {
                return min(T, c.slice[start..end]) orelse std.math.maxInt(T);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return @min(a, b);
            }
        }.combine,
        8192,
    );

    return if (result == std.math.maxInt(T)) null else result;
}

// ============================================================================
// Tests
// ============================================================================

test "simd sum" {
    const data = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const result = sum(i64, &data);
    try std.testing.expectEqual(@as(i64, 55), result);
}

test "simd sum large" {
    var data: [1000]i64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @intCast(i + 1);
    }
    const result = sum(i64, &data);
    // Sum of 1 to 1000 = n(n+1)/2 = 500500
    try std.testing.expectEqual(@as(i64, 500500), result);
}

test "simd min" {
    const data = [_]i64{ 5, 2, 8, 1, 9, 3, 7, 4, 6 };
    const result = min(i64, &data);
    try std.testing.expectEqual(@as(i64, 1), result.?);
}

test "simd max" {
    const data = [_]i64{ 5, 2, 8, 1, 9, 3, 7, 4, 6 };
    const result = max(i64, &data);
    try std.testing.expectEqual(@as(i64, 9), result.?);
}

test "simd empty" {
    const empty: []const i64 = &.{};
    try std.testing.expectEqual(@as(i64, 0), sum(i64, empty));
    try std.testing.expectEqual(@as(?i64, null), min(i64, empty));
    try std.testing.expectEqual(@as(?i64, null), max(i64, empty));
}
