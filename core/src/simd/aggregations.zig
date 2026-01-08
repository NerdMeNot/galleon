const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Float Aggregations
// ============================================================================

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
// Integer Aggregations
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

// ============================================================================
// Tests
// ============================================================================

test "aggregations - sum f64" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 };
    const result = sum(f64, &data);
    try std.testing.expectApproxEqAbs(@as(f64, 55.0), result, 0.0001);
}

test "aggregations - sum f64 large" {
    var data: [100]f64 = undefined;
    var expected: f64 = 0;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i + 1);
        expected += v.*;
    }
    const result = sum(f64, &data);
    try std.testing.expectApproxEqAbs(expected, result, 0.0001);
}

test "aggregations - sum f64 empty" {
    const data = [_]f64{};
    const result = sum(f64, &data);
    try std.testing.expectEqual(@as(f64, 0), result);
}

test "aggregations - min/max f64" {
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0 };
    try std.testing.expectEqual(@as(f64, 1.0), min(f64, &data).?);
    try std.testing.expectEqual(@as(f64, 9.0), max(f64, &data).?);
}

test "aggregations - min/max f64 large" {
    var data: [100]f64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i + 1);
    }
    data[50] = 0.5; // Min
    data[75] = 150.0; // Max

    try std.testing.expectApproxEqAbs(@as(f64, 0.5), min(f64, &data).?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 150.0), max(f64, &data).?, 0.0001);
}

test "aggregations - min/max empty" {
    const data = [_]f64{};
    try std.testing.expectEqual(@as(?f64, null), min(f64, &data));
    try std.testing.expectEqual(@as(?f64, null), max(f64, &data));
}

test "aggregations - mean f64" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), mean(f64, &data).?, 0.0001);
}

test "aggregations - variance f64" {
    const data = [_]f64{ 2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0 };
    const v = variance(f64, &data).?;
    try std.testing.expectApproxEqAbs(@as(f64, 4.571), v, 0.01);
}

test "aggregations - sumInt i64" {
    const data = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const result = sumInt(i64, &data);
    try std.testing.expectEqual(@as(i64, 55), result);
}

test "aggregations - minInt/maxInt i64" {
    const data = [_]i64{ 5, 2, 8, 1, 9, 3 };
    try std.testing.expectEqual(@as(i64, 1), minInt(i64, &data).?);
    try std.testing.expectEqual(@as(i64, 9), maxInt(i64, &data).?);
}

// Additional tests for better coverage

test "aggregations - sum f32" {
    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    const result = sum(f32, &data);
    try std.testing.expectApproxEqAbs(@as(f32, 36.0), result, 0.0001);
}

test "aggregations - sum f32 large SIMD path" {
    var data: [100]f32 = undefined;
    var expected: f32 = 0;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i + 1);
        expected += v.*;
    }
    const result = sum(f32, &data);
    try std.testing.expectApproxEqAbs(expected, result, 0.01);
}

test "aggregations - min/max f32" {
    const data = [_]f32{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0 };
    try std.testing.expectEqual(@as(f32, 1.0), min(f32, &data).?);
    try std.testing.expectEqual(@as(f32, 9.0), max(f32, &data).?);
}

test "aggregations - mean f32" {
    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), mean(f32, &data).?, 0.0001);
}

test "aggregations - mean empty" {
    const data = [_]f64{};
    try std.testing.expectEqual(@as(?f64, null), mean(f64, &data));
}

test "aggregations - sumInt i32" {
    const data = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    const result = sumInt(i32, &data);
    try std.testing.expectEqual(@as(i32, 55), result);
}

test "aggregations - sumInt i32 large SIMD path" {
    var data: [100]i32 = undefined;
    var expected: i32 = 0;
    for (&data, 0..) |*v, i| {
        v.* = @intCast(i + 1);
        expected += v.*;
    }
    const result = sumInt(i32, &data);
    try std.testing.expectEqual(expected, result);
}

test "aggregations - minInt/maxInt i32" {
    const data = [_]i32{ 5, 2, 8, 1, 9, 3 };
    try std.testing.expectEqual(@as(i32, 1), minInt(i32, &data).?);
    try std.testing.expectEqual(@as(i32, 9), maxInt(i32, &data).?);
}

test "aggregations - minInt/maxInt negative values" {
    const data = [_]i64{ -5, -2, -8, -1, -9, -3 };
    try std.testing.expectEqual(@as(i64, -9), minInt(i64, &data).?);
    try std.testing.expectEqual(@as(i64, -1), maxInt(i64, &data).?);
}

test "aggregations - minInt/maxInt empty" {
    const data = [_]i64{};
    try std.testing.expectEqual(@as(?i64, null), minInt(i64, &data));
    try std.testing.expectEqual(@as(?i64, null), maxInt(i64, &data));
}

test "aggregations - variance empty" {
    const data = [_]f64{};
    try std.testing.expectEqual(@as(?f64, null), variance(f64, &data));
}

test "aggregations - stdDev f64" {
    const data = [_]f64{ 2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0 };
    const sd = stdDev(f64, &data).?;
    // stdDev = sqrt(4.571) ≈ 2.138
    try std.testing.expectApproxEqAbs(@as(f64, 2.138), sd, 0.01);
}

test "aggregations - single element" {
    const data_f64 = [_]f64{42.0};
    try std.testing.expectEqual(@as(f64, 42.0), sum(f64, &data_f64));
    try std.testing.expectEqual(@as(f64, 42.0), min(f64, &data_f64).?);
    try std.testing.expectEqual(@as(f64, 42.0), max(f64, &data_f64).?);
    try std.testing.expectEqual(@as(f64, 42.0), mean(f64, &data_f64).?);

    const data_i64 = [_]i64{42};
    try std.testing.expectEqual(@as(i64, 42), sumInt(i64, &data_i64));
    try std.testing.expectEqual(@as(i64, 42), minInt(i64, &data_i64).?);
    try std.testing.expectEqual(@as(i64, 42), maxInt(i64, &data_i64).?);
}
