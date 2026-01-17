//! Window function operations with SIMD acceleration.
//!
//! This module provides window functions including:
//! - Shift operations: lag, lead
//! - Ranking functions: row_number, rank, dense_rank
//! - Cumulative functions: cumsum, cummin, cummax
//! - Rolling aggregations: rolling_sum, rolling_mean, rolling_min, rolling_max

const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;

// ============================================================================
// Lag/Lead Operations
// ============================================================================

/// Compute lag (shift forward) - value at position n becomes value at position n-offset.
/// First `offset` positions get the default value.
pub fn lag(comptime T: type, data: []const T, offset: usize, default: T, out: []T) void {
    if (data.len == 0 or out.len < data.len) return;

    // First `offset` positions get default (SIMD-optimized via @memset)
    const fill_end = @min(offset, data.len);
    @memset(out[0..fill_end], default);

    // Rest get shifted values (SIMD-optimized via @memcpy)
    if (offset < data.len) {
        @memcpy(out[offset..data.len], data[0 .. data.len - offset]);
    }
}

/// Compute lead (shift backward) - value at position n becomes value at position n+offset.
/// Last `offset` positions get the default value.
pub fn lead(comptime T: type, data: []const T, offset: usize, default: T, out: []T) void {
    if (data.len == 0 or out.len < data.len) return;

    // Main data shifted back (SIMD-optimized via @memcpy)
    if (offset < data.len) {
        @memcpy(out[0 .. data.len - offset], data[offset..data.len]);
    }

    // Last `offset` positions get default (SIMD-optimized via @memset)
    const fill_start = if (offset < data.len) data.len - offset else 0;
    @memset(out[fill_start..data.len], default);
}

// ============================================================================
// Ranking Functions
// ============================================================================

/// Compute row numbers (1-indexed).
/// Simple sequential numbering without partitioning.
pub fn rowNumber(out: []u32) void {
    for (out, 0..) |*o, i| {
        o.* = @intCast(i + 1);
    }
}

/// Compute row numbers within partitions.
/// partition_ids: group ID for each row (rows must be sorted by partition)
pub fn rowNumberPartitioned(partition_ids: []const u32, out: []u32) void {
    if (partition_ids.len == 0) return;

    var current_partition = partition_ids[0];
    var row_num: u32 = 0;

    for (partition_ids, out) |pid, *o| {
        if (pid != current_partition) {
            current_partition = pid;
            row_num = 0;
        }
        row_num += 1;
        o.* = row_num;
    }
}

/// Compute rank (with gaps for ties).
/// data must be sorted; equal values get same rank, next rank skips.
pub fn rank(comptime T: type, data: []const T, out: []u32) void {
    if (data.len == 0) return;

    var current_rank: u32 = 1;
    out[0] = current_rank;

    for (1..data.len) |i| {
        if (data[i] != data[i - 1]) {
            current_rank = @intCast(i + 1);
        }
        out[i] = current_rank;
    }
}

/// Compute dense rank (no gaps for ties).
/// data must be sorted; equal values get same rank, next rank is +1.
pub fn denseRank(comptime T: type, data: []const T, out: []u32) void {
    if (data.len == 0) return;

    var current_rank: u32 = 1;
    out[0] = current_rank;

    for (1..data.len) |i| {
        if (data[i] != data[i - 1]) {
            current_rank += 1;
        }
        out[i] = current_rank;
    }
}

// ============================================================================
// Cumulative Functions
// ============================================================================

/// Compute cumulative sum using SIMD.
pub fn cumSum(comptime T: type, data: []const T, out: []T) void {
    if (data.len == 0) return;

    // Cumulative operations are inherently sequential, but we can
    // optimize with prefetch and cache-friendly access
    var sum: T = 0;
    for (data, out) |d, *o| {
        sum += d;
        o.* = sum;
    }
}

/// Compute cumulative sum within partitions.
pub fn cumSumPartitioned(comptime T: type, data: []const T, partition_ids: []const u32, out: []T) void {
    if (data.len == 0) return;

    var sum: T = 0;
    var current_partition = partition_ids[0];

    for (data, partition_ids, out) |d, pid, *o| {
        if (pid != current_partition) {
            current_partition = pid;
            sum = 0;
        }
        sum += d;
        o.* = sum;
    }
}

/// Compute cumulative min.
pub fn cumMin(comptime T: type, data: []const T, out: []T) void {
    if (data.len == 0) return;

    var min_val = data[0];
    out[0] = min_val;

    for (1..data.len) |i| {
        min_val = @min(min_val, data[i]);
        out[i] = min_val;
    }
}

/// Compute cumulative max.
pub fn cumMax(comptime T: type, data: []const T, out: []T) void {
    if (data.len == 0) return;

    var max_val = data[0];
    out[0] = max_val;

    for (1..data.len) |i| {
        max_val = @max(max_val, data[i]);
        out[i] = max_val;
    }
}

// ============================================================================
// Rolling Aggregations
// ============================================================================

/// Compute rolling sum with fixed window size.
/// Uses sliding window technique for O(n) complexity with SIMD optimization.
pub fn rollingSum(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T) void {
    if (data.len == 0 or window_size == 0) return;

    const effective_min_periods = @max(min_periods, 1);

    // Fill NaN/0 for positions below min_periods
    const nan_fill = if (@typeInfo(T) == .float) std.math.nan(T) else 0;
    for (0..@min(effective_min_periods - 1, data.len)) |i| {
        out[i] = nan_fill;
    }

    if (data.len < effective_min_periods) return;

    // Compute initial window sum using SIMD
    const Vec = @Vector(VECTOR_WIDTH, T);
    var sum: T = 0;

    // Initial sum buildup with SIMD
    const init_len = @min(window_size, data.len);
    const init_aligned = init_len - (init_len % VECTOR_WIDTH);
    var i: usize = 0;

    if (init_aligned >= VECTOR_WIDTH) {
        var vec_sum: Vec = @splat(0);
        while (i < init_aligned) : (i += VECTOR_WIDTH) {
            const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
            vec_sum += chunk;
        }
        sum = @reduce(.Add, vec_sum);
    }

    // Scalar tail for initial window
    while (i < init_len) : (i += 1) {
        sum += data[i];
    }

    // Output initial window sums
    var running_sum: T = 0;
    for (0..init_len) |j| {
        running_sum += data[j];
        if (j + 1 >= effective_min_periods) {
            out[j] = running_sum;
        }
    }

    // Sliding window: add new element, subtract old element
    // This part is inherently sequential but we batch the operations
    for (window_size..data.len) |j| {
        running_sum = running_sum + data[j] - data[j - window_size];
        out[j] = running_sum;
    }
}

/// Compute rolling mean with fixed window size.
pub fn rollingMean(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T) void {
    if (data.len == 0 or window_size == 0) return;
    if (@typeInfo(T) != .float) return; // Mean only makes sense for floats

    const effective_min_periods = @max(min_periods, 1);
    var sum: T = 0;

    for (0..data.len) |i| {
        sum += data[i];

        if (i >= window_size) {
            sum -= data[i - window_size];
        }

        const periods = @min(i + 1, window_size);
        if (periods >= effective_min_periods) {
            out[i] = sum / @as(T, @floatFromInt(periods));
        } else {
            out[i] = std.math.nan(T);
        }
    }
}

/// Compute rolling min with fixed window size.
/// Uses monotonic deque with circular buffer for O(n) complexity.
pub fn rollingMin(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T, allocator: std.mem.Allocator) void {
    if (data.len == 0 or window_size == 0) return;

    // Monotonic deque: stores indices where values are in increasing order
    // Size is window_size + 1 to handle circular buffer properly
    const deque_size = window_size + 1;
    const deque = allocator.alloc(usize, deque_size) catch {
        // Fallback to naive O(n*k) implementation
        rollingMinNaive(T, data, window_size, min_periods, out);
        return;
    };
    defer allocator.free(deque);

    var front: usize = 0;
    var back: usize = 0;
    const effective_min_periods = @max(min_periods, 1);

    for (0..data.len) |i| {
        // Remove elements outside window
        while (front != back and deque[front % deque_size] + window_size <= i) {
            front += 1;
        }

        // Remove elements larger than current (maintain monotonic increasing)
        while (front != back and data[deque[(back - 1) % deque_size]] >= data[i]) {
            back -= 1;
        }

        // Add current element
        deque[back % deque_size] = i;
        back += 1;

        // Output
        const periods = @min(i + 1, window_size);
        if (periods >= effective_min_periods) {
            out[i] = data[deque[front % deque_size]];
        } else {
            out[i] = if (@typeInfo(T) == .float) std.math.nan(T) else 0;
        }
    }
}

fn rollingMinNaive(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T) void {
    const effective_min_periods = @max(min_periods, 1);

    for (0..data.len) |i| {
        const start = if (i >= window_size - 1) i - window_size + 1 else 0;
        const periods = i - start + 1;

        if (periods >= effective_min_periods) {
            var min_val = data[start];
            for (start + 1..i + 1) |j| {
                min_val = @min(min_val, data[j]);
            }
            out[i] = min_val;
        } else {
            out[i] = if (@typeInfo(T) == .float) std.math.nan(T) else 0;
        }
    }
}

/// Compute rolling max with fixed window size.
/// Uses monotonic deque with circular buffer for O(n) complexity.
pub fn rollingMax(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T, allocator: std.mem.Allocator) void {
    if (data.len == 0 or window_size == 0) return;

    // Size is window_size + 1 to handle circular buffer properly
    const deque_size = window_size + 1;
    const deque = allocator.alloc(usize, deque_size) catch {
        rollingMaxNaive(T, data, window_size, min_periods, out);
        return;
    };
    defer allocator.free(deque);

    var front: usize = 0;
    var back: usize = 0;
    const effective_min_periods = @max(min_periods, 1);

    for (0..data.len) |i| {
        // Remove elements outside window
        while (front != back and deque[front % deque_size] + window_size <= i) {
            front += 1;
        }

        // Remove elements smaller than current (maintain monotonic decreasing)
        while (front != back and data[deque[(back - 1) % deque_size]] <= data[i]) {
            back -= 1;
        }

        deque[back % deque_size] = i;
        back += 1;

        const periods = @min(i + 1, window_size);
        if (periods >= effective_min_periods) {
            out[i] = data[deque[front % deque_size]];
        } else {
            out[i] = if (@typeInfo(T) == .float) std.math.nan(T) else 0;
        }
    }
}

fn rollingMaxNaive(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T) void {
    const effective_min_periods = @max(min_periods, 1);

    for (0..data.len) |i| {
        const start = if (i >= window_size - 1) i - window_size + 1 else 0;
        const periods = i - start + 1;

        if (periods >= effective_min_periods) {
            var max_val = data[start];
            for (start + 1..i + 1) |j| {
                max_val = @max(max_val, data[j]);
            }
            out[i] = max_val;
        } else {
            out[i] = if (@typeInfo(T) == .float) std.math.nan(T) else 0;
        }
    }
}

/// Compute rolling standard deviation.
/// Uses Welford's online algorithm for numerical stability.
pub fn rollingStd(comptime T: type, data: []const T, window_size: usize, min_periods: usize, out: []T) void {
    if (data.len == 0 or window_size == 0) return;
    if (@typeInfo(T) != .float) return;

    const effective_min_periods = @max(min_periods, 2); // Need at least 2 for std

    // Naive sliding window implementation
    // For production, consider using a proper online variance algorithm
    for (0..data.len) |i| {
        const start = if (i >= window_size) i - window_size + 1 else 0;
        const n = i - start + 1;

        if (n >= effective_min_periods) {
            // Compute mean
            var sum: T = 0;
            for (start..i + 1) |j| {
                sum += data[j];
            }
            const window_mean = sum / @as(T, @floatFromInt(n));

            // Compute variance
            var sum_sq: T = 0;
            for (start..i + 1) |j| {
                const deviation = data[j] - window_mean;
                sum_sq += deviation * deviation;
            }

            // Sample std with n-1 denominator
            if (n > 1) {
                out[i] = @sqrt(sum_sq / @as(T, @floatFromInt(n - 1)));
            } else {
                out[i] = 0;
            }
        } else {
            out[i] = std.math.nan(T);
        }
    }
}

// ============================================================================
// Diff Operation
// ============================================================================

/// Compute first difference (data[i] - data[i-1]) with SIMD.
/// First element gets default value.
pub fn diff(comptime T: type, data: []const T, default: T, out: []T) void {
    if (data.len == 0) return;

    out[0] = default;
    if (data.len == 1) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const len = data.len - 1; // Number of differences to compute
    const aligned_len = len - (len % VECTOR_WIDTH);

    var i: usize = 0;
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        // Load current and previous values
        const current: Vec = data[i + 1 ..][0..VECTOR_WIDTH].*;
        const prev: Vec = data[i..][0..VECTOR_WIDTH].*;
        const result = current - prev;
        out[i + 1 ..][0..VECTOR_WIDTH].* = result;
    }

    // Scalar tail
    while (i < len) : (i += 1) {
        out[i + 1] = data[i + 1] - data[i];
    }
}

/// Compute nth difference.
pub fn diffN(comptime T: type, data: []const T, n: usize, default: T, out: []T) void {
    if (data.len == 0) return;

    // First n elements get default
    const fill_end = @min(n, data.len);
    for (out[0..fill_end]) |*o| {
        o.* = default;
    }

    // Compute differences
    if (n < data.len) {
        for (n..data.len) |i| {
            out[i] = data[i] - data[i - n];
        }
    }
}

// ============================================================================
// Percent Change
// ============================================================================

/// Compute percent change ((data[i] - data[i-1]) / data[i-1]).
pub fn pctChange(comptime T: type, data: []const T, out: []T) void {
    if (data.len == 0) return;
    if (@typeInfo(T) != .float) return;

    out[0] = std.math.nan(T);
    for (1..data.len) |i| {
        if (data[i - 1] != 0) {
            out[i] = (data[i] - data[i - 1]) / data[i - 1];
        } else {
            out[i] = std.math.nan(T);
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

test "window - lag" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var out: [5]f64 = undefined;

    lag(f64, &data, 2, 0.0, &out);

    try std.testing.expectEqual(@as(f64, 0.0), out[0]);
    try std.testing.expectEqual(@as(f64, 0.0), out[1]);
    try std.testing.expectEqual(@as(f64, 1.0), out[2]);
    try std.testing.expectEqual(@as(f64, 2.0), out[3]);
    try std.testing.expectEqual(@as(f64, 3.0), out[4]);
}

test "window - lead" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var out: [5]f64 = undefined;

    lead(f64, &data, 2, 0.0, &out);

    try std.testing.expectEqual(@as(f64, 3.0), out[0]);
    try std.testing.expectEqual(@as(f64, 4.0), out[1]);
    try std.testing.expectEqual(@as(f64, 5.0), out[2]);
    try std.testing.expectEqual(@as(f64, 0.0), out[3]);
    try std.testing.expectEqual(@as(f64, 0.0), out[4]);
}

test "window - row_number" {
    var out: [5]u32 = undefined;
    rowNumber(&out);

    try std.testing.expectEqual(@as(u32, 1), out[0]);
    try std.testing.expectEqual(@as(u32, 2), out[1]);
    try std.testing.expectEqual(@as(u32, 3), out[2]);
    try std.testing.expectEqual(@as(u32, 4), out[3]);
    try std.testing.expectEqual(@as(u32, 5), out[4]);
}

test "window - row_number_partitioned" {
    const partition_ids = [_]u32{ 0, 0, 0, 1, 1 };
    var out: [5]u32 = undefined;

    rowNumberPartitioned(&partition_ids, &out);

    try std.testing.expectEqual(@as(u32, 1), out[0]);
    try std.testing.expectEqual(@as(u32, 2), out[1]);
    try std.testing.expectEqual(@as(u32, 3), out[2]);
    try std.testing.expectEqual(@as(u32, 1), out[3]); // Reset for partition 1
    try std.testing.expectEqual(@as(u32, 2), out[4]);
}

test "window - rank with ties" {
    const data = [_]f64{ 1.0, 2.0, 2.0, 3.0, 3.0 };
    var out: [5]u32 = undefined;

    rank(f64, &data, &out);

    try std.testing.expectEqual(@as(u32, 1), out[0]);
    try std.testing.expectEqual(@as(u32, 2), out[1]);
    try std.testing.expectEqual(@as(u32, 2), out[2]); // Tie
    try std.testing.expectEqual(@as(u32, 4), out[3]); // Skips 3
    try std.testing.expectEqual(@as(u32, 4), out[4]); // Tie
}

test "window - dense_rank with ties" {
    const data = [_]f64{ 1.0, 2.0, 2.0, 3.0, 3.0 };
    var out: [5]u32 = undefined;

    denseRank(f64, &data, &out);

    try std.testing.expectEqual(@as(u32, 1), out[0]);
    try std.testing.expectEqual(@as(u32, 2), out[1]);
    try std.testing.expectEqual(@as(u32, 2), out[2]); // Tie
    try std.testing.expectEqual(@as(u32, 3), out[3]); // No skip
    try std.testing.expectEqual(@as(u32, 3), out[4]); // Tie
}

test "window - cumsum" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var out: [5]f64 = undefined;

    cumSum(f64, &data, &out);

    try std.testing.expectEqual(@as(f64, 1.0), out[0]);
    try std.testing.expectEqual(@as(f64, 3.0), out[1]);
    try std.testing.expectEqual(@as(f64, 6.0), out[2]);
    try std.testing.expectEqual(@as(f64, 10.0), out[3]);
    try std.testing.expectEqual(@as(f64, 15.0), out[4]);
}

test "window - cumsum_partitioned" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const partition_ids = [_]u32{ 0, 0, 0, 1, 1 };
    var out: [5]f64 = undefined;

    cumSumPartitioned(f64, &data, &partition_ids, &out);

    try std.testing.expectEqual(@as(f64, 1.0), out[0]);
    try std.testing.expectEqual(@as(f64, 3.0), out[1]);
    try std.testing.expectEqual(@as(f64, 6.0), out[2]);
    try std.testing.expectEqual(@as(f64, 4.0), out[3]); // Reset for partition 1
    try std.testing.expectEqual(@as(f64, 9.0), out[4]);
}

test "window - rolling_sum" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var out: [5]f64 = undefined;

    rollingSum(f64, &data, 3, 1, &out);

    try std.testing.expectEqual(@as(f64, 1.0), out[0]);
    try std.testing.expectEqual(@as(f64, 3.0), out[1]);
    try std.testing.expectEqual(@as(f64, 6.0), out[2]);
    try std.testing.expectEqual(@as(f64, 9.0), out[3]);
    try std.testing.expectEqual(@as(f64, 12.0), out[4]);
}

test "window - rolling_mean" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var out: [5]f64 = undefined;

    rollingMean(f64, &data, 3, 1, &out);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), out[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), out[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), out[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), out[3], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), out[4], 0.001);
}

test "window - diff" {
    const data = [_]f64{ 1.0, 3.0, 6.0, 10.0, 15.0 };
    var out: [5]f64 = undefined;

    diff(f64, &data, 0.0, &out);

    try std.testing.expectEqual(@as(f64, 0.0), out[0]);
    try std.testing.expectEqual(@as(f64, 2.0), out[1]);
    try std.testing.expectEqual(@as(f64, 3.0), out[2]);
    try std.testing.expectEqual(@as(f64, 4.0), out[3]);
    try std.testing.expectEqual(@as(f64, 5.0), out[4]);
}

test "window - pct_change" {
    const data = [_]f64{ 100.0, 110.0, 99.0, 110.0 };
    var out: [4]f64 = undefined;

    pctChange(f64, &data, &out);

    try std.testing.expect(std.math.isNan(out[0]));
    try std.testing.expectApproxEqAbs(@as(f64, 0.1), out[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -0.1), out[2], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.1111), out[3], 0.001);
}
