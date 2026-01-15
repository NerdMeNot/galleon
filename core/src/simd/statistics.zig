//! Advanced statistical operations with SIMD acceleration.
//!
//! This module provides advanced aggregation functions including:
//! - Median (Floyd-Rivest O(n) with lower constants)
//! - Quantile (percentile computation)
//! - Skewness (3rd standardized moment)
//! - Kurtosis (4th standardized moment)
//! - Correlation (Pearson correlation coefficient)

const std = @import("std");
const core = @import("core.zig");
const aggregations = @import("aggregations.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Median (Floyd-Rivest Algorithm)
// ============================================================================

/// Compute median using the Floyd-Rivest algorithm.
/// O(n) with very low constants due to sampling-based pivot selection.
/// Returns null for empty input.
pub fn median(comptime T: type, data: []const T, allocator: std.mem.Allocator) ?T {
    if (data.len == 0) return null;
    if (data.len == 1) return data[0];

    // Make a mutable copy for in-place partitioning
    const copy = allocator.alloc(T, data.len) catch return null;
    defer allocator.free(copy);
    @memcpy(copy, data);

    const n = data.len;
    if (n % 2 == 1) {
        // Odd length: return middle element
        return floydRivestSelect(T, copy, 0, n - 1, n / 2);
    } else {
        // Even length: average of two middle elements
        const lower = floydRivestSelect(T, copy, 0, n - 1, n / 2 - 1) orelse return null;
        // After selection, element at n/2-1 is in place, and all elements >= n/2 are >= lower
        // Find minimum of right partition for upper median
        var upper = copy[n / 2];
        for (copy[n / 2 + 1 ..]) |v| {
            if (v < upper) upper = v;
        }
        return (lower + upper) / 2.0;
    }
}

/// Floyd-Rivest selection algorithm.
/// Finds the k-th smallest element (0-indexed) in expected O(n) time
/// with very low constants due to sampling-based approach.
///
/// Algorithm overview:
/// 1. For small arrays (< 600), use simple selection sort
/// 2. For larger arrays, take a sample and use percentiles to narrow bounds
/// 3. Recursively select within narrowed bounds
fn floydRivestSelect(comptime T: type, data: []T, left_init: usize, right_init: usize, k: usize) ?T {
    if (data.len == 0) return null;
    if (k >= data.len) return null;

    var left = left_init;
    var right = right_init;

    while (right > left) {
        // For large arrays, use sampling to find better bounds
        if (right - left > 600) {
            const n = right - left + 1;
            const i_float = @as(T, @floatFromInt(k - left + 1));
            const n_float = @as(T, @floatFromInt(n));

            // Compute sample size (based on Floyd-Rivest paper)
            const z = @log(n_float);
            const s = 0.5 * @exp(2.0 * z / 3.0);

            // Compute standard deviation adjustment
            const sd = 0.5 * @sqrt(z * s * (n_float - s) / n_float) *
                @as(T, if (i_float - n_float / 2.0 < 0) -1.0 else 1.0);

            // Compute sample bounds
            const new_left_f = @max(
                @as(T, @floatFromInt(left)),
                @floor(@as(T, @floatFromInt(k)) - i_float * s / n_float + sd),
            );
            const new_right_f = @min(
                @as(T, @floatFromInt(right)),
                @floor(@as(T, @floatFromInt(k)) + (n_float - i_float) * s / n_float + sd),
            );

            const new_left = @as(usize, @intFromFloat(new_left_f));
            const new_right = @as(usize, @intFromFloat(new_right_f));

            // Recursively select within sample to position bounds
            _ = floydRivestSelect(T, data, new_left, new_right, k);
        }

        // Standard three-way partition around data[k]
        const pivot = data[k];
        var i = left;
        var j = right;

        // Swap pivot to left position
        std.mem.swap(T, &data[left], &data[k]);

        // Check if right element should be swapped
        if (data[right] > pivot) {
            std.mem.swap(T, &data[left], &data[right]);
        }

        // Partition
        while (i < j) {
            std.mem.swap(T, &data[i], &data[j]);
            i += 1;
            if (j == 0) break;
            j -= 1;

            while (i < data.len and data[i] < pivot) : (i += 1) {}
            while (j > 0 and data[j] > pivot) : (j -= 1) {}
        }

        // Place pivot in final position
        if (data[left] == pivot) {
            std.mem.swap(T, &data[left], &data[j]);
        } else {
            j += 1;
            std.mem.swap(T, &data[j], &data[right]);
        }

        // Narrow search range (with overflow protection)
        if (j <= k) {
            left = j + 1;
        }
        if (k <= j) {
            if (j == 0) break;
            right = j - 1;
        }
    }

    return data[k];
}

/// Simple insertion sort for very small arrays (used as base case)
fn insertionSort(comptime T: type, data: []T) void {
    if (data.len < 2) return;

    for (1..data.len) |i| {
        const key = data[i];
        var j = i;
        while (j > 0 and data[j - 1] > key) : (j -= 1) {
            data[j] = data[j - 1];
        }
        data[j] = key;
    }
}

// ============================================================================
// Quantile
// ============================================================================

/// Compute quantile (percentile / 100) using Floyd-Rivest + linear interpolation.
/// q should be in range [0, 1].
/// Returns null for empty input or invalid q.
/// Uses O(n) Floyd-Rivest selection instead of O(n log n) sort.
pub fn quantile(comptime T: type, data: []const T, q: T, allocator: std.mem.Allocator) ?T {
    if (data.len == 0) return null;
    if (q < 0 or q > 1) return null;
    if (data.len == 1) return data[0];

    // Make a mutable copy for in-place selection
    const copy = allocator.alloc(T, data.len) catch return null;
    defer allocator.free(copy);
    @memcpy(copy, data);

    // Calculate indices for interpolation
    const n = @as(T, @floatFromInt(data.len - 1));
    const idx = q * n;
    const lower_idx = @as(usize, @intFromFloat(@floor(idx)));
    const upper_idx = @min(lower_idx + 1, data.len - 1);
    const frac = idx - @floor(idx);

    // Use Floyd-Rivest to find the lower element - O(n)
    const lower_val = floydRivestSelect(T, copy, 0, data.len - 1, lower_idx) orelse return null;

    // If we need interpolation, find the minimum of remaining elements
    // After selection, all elements at index > lower_idx are >= lower_val
    // So we just need to find the min of copy[lower_idx+1..] which is O(n-k) with SIMD
    if (frac > 0 and upper_idx != lower_idx and lower_idx + 1 < data.len) {
        // Find minimum in the right partition (all >= lower_val) using SIMD
        const right_slice = copy[lower_idx + 1 ..];
        if (right_slice.len > 0) {
            const upper_val = findMinSIMD(T, right_slice);
            return lower_val * (1.0 - frac) + upper_val * frac;
        }
    }

    return lower_val;
}

/// SIMD-accelerated minimum finding for quantile interpolation
fn findMinSIMD(comptime T: type, data: []const T) T {
    if (data.len == 0) return 0;
    if (data.len == 1) return data[0];

    const Vec = @Vector(VECTOR_WIDTH, T);

    // Initialize with first element
    var min_vec: Vec = @splat(data[0]);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    var i: usize = 0;

    // Process vectors
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        min_vec = @min(min_vec, chunk);
    }

    // Reduce vector to scalar
    var result = @reduce(.Min, min_vec);

    // Handle tail
    while (i < data.len) : (i += 1) {
        if (data[i] < result) result = data[i];
    }

    return result;
}

// ============================================================================
// Skewness (3rd Standardized Moment)
// ============================================================================

/// Compute skewness using SIMD for moment calculation.
/// Uses sample skewness formula: n/((n-1)(n-2)) * sum((x-mean)³) / std³
/// Returns null for n < 3.
pub fn skewness(comptime T: type, data: []const T) ?T {
    if (data.len < 3) return null;

    const n = @as(T, @floatFromInt(data.len));
    const avg = aggregations.mean(T, data) orelse return null;
    const std_dev = aggregations.stdDev(T, data) orelse return null;

    if (std_dev == 0) return 0; // All values identical

    const Vec = @Vector(VECTOR_WIDTH, T);
    const mean_vec: Vec = @splat(avg);

    // Multiple accumulators for sum of cubes
    var sum_cube0: Vec = @splat(0);
    var sum_cube1: Vec = @splat(0);
    var sum_cube2: Vec = @splat(0);
    var sum_cube3: Vec = @splat(0);

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

        sum_cube0 += diff0 * diff0 * diff0;
        sum_cube1 += diff1 * diff1 * diff1;
        sum_cube2 += diff2 * diff2 * diff2;
        sum_cube3 += diff3 * diff3 * diff3;
    }

    const combined = sum_cube0 + sum_cube1 + sum_cube2 + sum_cube3;
    var sum_cube = @reduce(.Add, combined);

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const diff = chunk - mean_vec;
        sum_cube += @reduce(.Add, diff * diff * diff);
    }

    // Handle tail
    while (i < data.len) : (i += 1) {
        const diff = data[i] - avg;
        sum_cube += diff * diff * diff;
    }

    // Sample skewness with bias correction
    const m3 = sum_cube / n;
    const adj = @sqrt(n * (n - 1)) / (n - 2);
    return adj * m3 / (std_dev * std_dev * std_dev);
}

// ============================================================================
// Kurtosis (4th Standardized Moment)
// ============================================================================

/// Compute excess kurtosis using SIMD for moment calculation.
/// Uses Fisher's definition: (m4/m2²) - 3
/// Returns null for n < 4.
pub fn kurtosis(comptime T: type, data: []const T) ?T {
    if (data.len < 4) return null;

    const n = @as(T, @floatFromInt(data.len));
    const avg = aggregations.mean(T, data) orelse return null;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const mean_vec: Vec = @splat(avg);

    // Accumulators for 2nd and 4th moments
    var sum_sq0: Vec = @splat(0);
    var sum_sq1: Vec = @splat(0);
    var sum_sq2: Vec = @splat(0);
    var sum_sq3: Vec = @splat(0);

    var sum_quad0: Vec = @splat(0);
    var sum_quad1: Vec = @splat(0);
    var sum_quad2: Vec = @splat(0);
    var sum_quad3: Vec = @splat(0);

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

        const sq0 = diff0 * diff0;
        const sq1 = diff1 * diff1;
        const sq2 = diff2 * diff2;
        const sq3 = diff3 * diff3;

        sum_sq0 += sq0;
        sum_sq1 += sq1;
        sum_sq2 += sq2;
        sum_sq3 += sq3;

        sum_quad0 += sq0 * sq0;
        sum_quad1 += sq1 * sq1;
        sum_quad2 += sq2 * sq2;
        sum_quad3 += sq3 * sq3;
    }

    const combined_sq = sum_sq0 + sum_sq1 + sum_sq2 + sum_sq3;
    var sum_sq = @reduce(.Add, combined_sq);

    const combined_quad = sum_quad0 + sum_quad1 + sum_quad2 + sum_quad3;
    var sum_quad = @reduce(.Add, combined_quad);

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        const diff = chunk - mean_vec;
        const sq = diff * diff;
        sum_sq += @reduce(.Add, sq);
        sum_quad += @reduce(.Add, sq * sq);
    }

    // Handle tail
    while (i < data.len) : (i += 1) {
        const diff = data[i] - avg;
        const sq = diff * diff;
        sum_sq += sq;
        sum_quad += sq * sq;
    }

    const m2 = sum_sq / n;
    const m4 = sum_quad / n;

    if (m2 == 0) return 0; // All values identical

    // Excess kurtosis with bias correction (Fisher's definition)
    const raw_kurt = m4 / (m2 * m2);

    // Bias correction for sample kurtosis
    const g2 = raw_kurt - 3;
    const adj = (n - 1) / ((n - 2) * (n - 3));
    return adj * ((n + 1) * g2 + 6);
}

// ============================================================================
// Correlation (Pearson)
// ============================================================================

/// Compute Pearson correlation coefficient using SIMD.
/// Returns null if either array has zero variance or lengths differ.
pub fn correlation(comptime T: type, x: []const T, y: []const T) ?T {
    if (x.len != y.len or x.len < 2) return null;

    const n = @as(T, @floatFromInt(x.len));
    const mean_x = aggregations.mean(T, x) orelse return null;
    const mean_y = aggregations.mean(T, y) orelse return null;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const mean_x_vec: Vec = @splat(mean_x);
    const mean_y_vec: Vec = @splat(mean_y);

    // Accumulators for covariance and variances
    var sum_xy0: Vec = @splat(0);
    var sum_xy1: Vec = @splat(0);
    var sum_xy2: Vec = @splat(0);
    var sum_xy3: Vec = @splat(0);

    var sum_xx0: Vec = @splat(0);
    var sum_xx1: Vec = @splat(0);
    var sum_xx2: Vec = @splat(0);
    var sum_xx3: Vec = @splat(0);

    var sum_yy0: Vec = @splat(0);
    var sum_yy1: Vec = @splat(0);
    var sum_yy2: Vec = @splat(0);
    var sum_yy3: Vec = @splat(0);

    const unrolled_len = x.len - (x.len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const x0: Vec = x[i..][0..VECTOR_WIDTH].*;
        const x1: Vec = x[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const x2: Vec = x[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const x3: Vec = x[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const y0: Vec = y[i..][0..VECTOR_WIDTH].*;
        const y1: Vec = y[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const y2: Vec = y[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const y3: Vec = y[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const dx0 = x0 - mean_x_vec;
        const dx1 = x1 - mean_x_vec;
        const dx2 = x2 - mean_x_vec;
        const dx3 = x3 - mean_x_vec;

        const dy0 = y0 - mean_y_vec;
        const dy1 = y1 - mean_y_vec;
        const dy2 = y2 - mean_y_vec;
        const dy3 = y3 - mean_y_vec;

        sum_xy0 += dx0 * dy0;
        sum_xy1 += dx1 * dy1;
        sum_xy2 += dx2 * dy2;
        sum_xy3 += dx3 * dy3;

        sum_xx0 += dx0 * dx0;
        sum_xx1 += dx1 * dx1;
        sum_xx2 += dx2 * dx2;
        sum_xx3 += dx3 * dx3;

        sum_yy0 += dy0 * dy0;
        sum_yy1 += dy1 * dy1;
        sum_yy2 += dy2 * dy2;
        sum_yy3 += dy3 * dy3;
    }

    const combined_xy = sum_xy0 + sum_xy1 + sum_xy2 + sum_xy3;
    var sum_xy = @reduce(.Add, combined_xy);

    const combined_xx = sum_xx0 + sum_xx1 + sum_xx2 + sum_xx3;
    var sum_xx = @reduce(.Add, combined_xx);

    const combined_yy = sum_yy0 + sum_yy1 + sum_yy2 + sum_yy3;
    var sum_yy = @reduce(.Add, combined_yy);

    // Handle remaining with single vector
    const aligned_len = x.len - (x.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const xv: Vec = x[i..][0..VECTOR_WIDTH].*;
        const yv: Vec = y[i..][0..VECTOR_WIDTH].*;
        const dx = xv - mean_x_vec;
        const dy = yv - mean_y_vec;
        sum_xy += @reduce(.Add, dx * dy);
        sum_xx += @reduce(.Add, dx * dx);
        sum_yy += @reduce(.Add, dy * dy);
    }

    // Handle tail
    while (i < x.len) : (i += 1) {
        const dx = x[i] - mean_x;
        const dy = y[i] - mean_y;
        sum_xy += dx * dy;
        sum_xx += dx * dx;
        sum_yy += dy * dy;
    }

    const denom = @sqrt(sum_xx * sum_yy);
    if (denom == 0) return null; // Zero variance in one or both

    _ = n; // n not needed for Pearson r
    return sum_xy / denom;
}

// ============================================================================
// Mode (Most Frequent Value)
// ============================================================================

/// Find mode (most frequent value) for integers.
/// Returns null for empty input.
/// Note: For floats, exact equality comparison is problematic.
pub fn modeInt(comptime T: type, data: []const T, allocator: std.mem.Allocator) ?T {
    if (data.len == 0) return null;
    if (data.len == 1) return data[0];

    // Use hash map to count frequencies
    var counts = std.AutoHashMap(T, usize).init(allocator);
    defer counts.deinit();

    for (data) |val| {
        const entry = counts.getOrPut(val) catch return null;
        if (!entry.found_existing) {
            entry.value_ptr.* = 0;
        }
        entry.value_ptr.* += 1;
    }

    var max_count: usize = 0;
    var mode_val: ?T = null;

    var it = counts.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* > max_count) {
            max_count = entry.value_ptr.*;
            mode_val = entry.key_ptr.*;
        }
    }

    return mode_val;
}

// ============================================================================
// Tests
// ============================================================================

test "statistics - median odd length" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };
    const result = median(f64, &data, allocator).?;
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), result, 0.0001);
}

test "statistics - median even length" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const result = median(f64, &data, allocator).?;
    try std.testing.expectApproxEqAbs(@as(f64, 2.5), result, 0.0001);
}

test "statistics - median single element" {
    const allocator = std.testing.allocator;
    const data = [_]f64{42.0};
    const result = median(f64, &data, allocator).?;
    try std.testing.expectApproxEqAbs(@as(f64, 42.0), result, 0.0001);
}

test "statistics - median empty" {
    const allocator = std.testing.allocator;
    const data = [_]f64{};
    try std.testing.expectEqual(@as(?f64, null), median(f64, &data, allocator));
}

test "statistics - quantile 50th percentile" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const result = quantile(f64, &data, 0.5, allocator).?;
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), result, 0.0001);
}

test "statistics - quantile 25th and 75th percentile" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 };
    const q25 = quantile(f64, &data, 0.25, allocator).?;
    const q75 = quantile(f64, &data, 0.75, allocator).?;
    try std.testing.expectApproxEqAbs(@as(f64, 3.25), q25, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 7.75), q75, 0.0001);
}

test "statistics - quantile edge cases" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const q0 = quantile(f64, &data, 0.0, allocator).?;
    const q1 = quantile(f64, &data, 1.0, allocator).?;
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), q0, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), q1, 0.0001);
}

test "statistics - quantile invalid q" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0 };
    try std.testing.expectEqual(@as(?f64, null), quantile(f64, &data, -0.1, allocator));
    try std.testing.expectEqual(@as(?f64, null), quantile(f64, &data, 1.1, allocator));
}

test "statistics - skewness symmetric distribution" {
    // Symmetric data should have skewness ≈ 0
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 };
    const result = skewness(f64, &data).?;
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), result, 0.1);
}

test "statistics - skewness right skewed" {
    // Right-skewed data should have positive skewness
    const data = [_]f64{ 1.0, 1.0, 1.0, 2.0, 2.0, 3.0, 10.0 };
    const result = skewness(f64, &data).?;
    try std.testing.expect(result > 0);
}

test "statistics - skewness insufficient data" {
    const data = [_]f64{ 1.0, 2.0 };
    try std.testing.expectEqual(@as(?f64, null), skewness(f64, &data));
}

test "statistics - kurtosis normal-like distribution" {
    // This is a rough test - excess kurtosis of normal is 0
    const data = [_]f64{ -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, -1.5, 1.5, 0.0 };
    const result = kurtosis(f64, &data);
    // Just verify it returns a reasonable value
    try std.testing.expect(result != null);
}

test "statistics - kurtosis heavy tails" {
    // Data with outliers should have positive excess kurtosis
    const data = [_]f64{ -10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 10.0 };
    const result = kurtosis(f64, &data);
    try std.testing.expect(result != null);
}

test "statistics - kurtosis insufficient data" {
    const data = [_]f64{ 1.0, 2.0, 3.0 };
    try std.testing.expectEqual(@as(?f64, null), kurtosis(f64, &data));
}

test "statistics - correlation perfect positive" {
    const x = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const y = [_]f64{ 2.0, 4.0, 6.0, 8.0, 10.0 };
    const result = correlation(f64, &x, &y).?;
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), result, 0.0001);
}

test "statistics - correlation perfect negative" {
    const x = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const y = [_]f64{ 10.0, 8.0, 6.0, 4.0, 2.0 };
    const result = correlation(f64, &x, &y).?;
    try std.testing.expectApproxEqAbs(@as(f64, -1.0), result, 0.0001);
}

test "statistics - correlation no correlation" {
    const x = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const y = [_]f64{ 5.0, 3.0, 5.0, 3.0, 5.0 };
    const result = correlation(f64, &x, &y).?;
    // Should be close to 0 but not exactly
    try std.testing.expect(@abs(result) < 0.5);
}

test "statistics - correlation length mismatch" {
    const x = [_]f64{ 1.0, 2.0, 3.0 };
    const y = [_]f64{ 1.0, 2.0 };
    try std.testing.expectEqual(@as(?f64, null), correlation(f64, &x, &y));
}

test "statistics - correlation zero variance" {
    const x = [_]f64{ 1.0, 1.0, 1.0 };
    const y = [_]f64{ 1.0, 2.0, 3.0 };
    try std.testing.expectEqual(@as(?f64, null), correlation(f64, &x, &y));
}

test "statistics - modeInt basic" {
    const allocator = std.testing.allocator;
    const data = [_]i64{ 1, 2, 2, 3, 3, 3, 4 };
    const result = modeInt(i64, &data, allocator).?;
    try std.testing.expectEqual(@as(i64, 3), result);
}

test "statistics - modeInt single element" {
    const allocator = std.testing.allocator;
    const data = [_]i64{42};
    const result = modeInt(i64, &data, allocator).?;
    try std.testing.expectEqual(@as(i64, 42), result);
}

test "statistics - modeInt empty" {
    const allocator = std.testing.allocator;
    const data = [_]i64{};
    try std.testing.expectEqual(@as(?i64, null), modeInt(i64, &data, allocator));
}
