const std = @import("std");

// ============================================================================
// GroupBy Aggregation Functions
// Scatter-based aggregations for grouped data
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
// Tests
// ============================================================================

test "groupby_agg - aggregateSumByGroup f64" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const group_ids = [_]u32{ 0, 1, 0, 1, 0, 1 };
    var sums = [_]f64{ 0.0, 0.0 };

    aggregateSumByGroup(f64, &data, &group_ids, &sums);

    // Group 0: 1.0 + 3.0 + 5.0 = 9.0
    // Group 1: 2.0 + 4.0 + 6.0 = 12.0
    try std.testing.expectEqual(@as(f64, 9.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 12.0), sums[1]);
}

test "groupby_agg - aggregateSumByGroup i64" {
    const data = [_]i64{ 10, 20, 30, 40, 50 };
    const group_ids = [_]u32{ 0, 0, 1, 1, 2 };
    var sums = [_]i64{ 0, 0, 0 };

    aggregateSumByGroup(i64, &data, &group_ids, &sums);

    // Group 0: 10 + 20 = 30
    // Group 1: 30 + 40 = 70
    // Group 2: 50
    try std.testing.expectEqual(@as(i64, 30), sums[0]);
    try std.testing.expectEqual(@as(i64, 70), sums[1]);
    try std.testing.expectEqual(@as(i64, 50), sums[2]);
}

test "groupby_agg - aggregateMinByGroup" {
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 3.0, 9.0 };
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 1 };
    var mins = [_]f64{ std.math.floatMax(f64), std.math.floatMax(f64) };

    aggregateMinByGroup(f64, &data, &group_ids, &mins);

    // Group 0: min(5.0, 2.0, 8.0) = 2.0
    // Group 1: min(1.0, 3.0, 9.0) = 1.0
    try std.testing.expectEqual(@as(f64, 2.0), mins[0]);
    try std.testing.expectEqual(@as(f64, 1.0), mins[1]);
}

test "groupby_agg - aggregateMaxByGroup" {
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 3.0, 9.0 };
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 1 };
    var maxs = [_]f64{ -std.math.floatMax(f64), -std.math.floatMax(f64) };

    aggregateMaxByGroup(f64, &data, &group_ids, &maxs);

    // Group 0: max(5.0, 2.0, 8.0) = 8.0
    // Group 1: max(1.0, 3.0, 9.0) = 9.0
    try std.testing.expectEqual(@as(f64, 8.0), maxs[0]);
    try std.testing.expectEqual(@as(f64, 9.0), maxs[1]);
}

test "groupby_agg - countByGroup" {
    const group_ids = [_]u32{ 0, 1, 0, 2, 1, 0, 2, 2 };
    var counts = [_]u64{ 0, 0, 0 };

    countByGroup(&group_ids, &counts);

    // Group 0: 3 elements (indices 0, 2, 5)
    // Group 1: 2 elements (indices 1, 4)
    // Group 2: 3 elements (indices 3, 6, 7)
    try std.testing.expectEqual(@as(u64, 3), counts[0]);
    try std.testing.expectEqual(@as(u64, 2), counts[1]);
    try std.testing.expectEqual(@as(u64, 3), counts[2]);
}

test "groupby_agg - out of bounds group ids are ignored" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const group_ids = [_]u32{ 0, 5, 0, 10 }; // 5 and 10 are out of bounds
    var sums = [_]f64{ 0.0, 0.0 };

    aggregateSumByGroup(f64, &data, &group_ids, &sums);

    // Only group 0 should have values: 1.0 + 3.0 = 4.0
    try std.testing.expectEqual(@as(f64, 4.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 0.0), sums[1]);
}

test "groupby_agg - empty data" {
    const data: []const f64 = &[_]f64{};
    const group_ids: []const u32 = &[_]u32{};
    var sums = [_]f64{ 0.0, 0.0 };

    aggregateSumByGroup(f64, data, group_ids, &sums);

    // Should not crash and sums should remain unchanged
    try std.testing.expectEqual(@as(f64, 0.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 0.0), sums[1]);
}

test "groupby_agg - single group" {
    const data = [_]i64{ 1, 2, 3, 4, 5 };
    const group_ids = [_]u32{ 0, 0, 0, 0, 0 };
    var sums = [_]i64{0};

    aggregateSumByGroup(i64, &data, &group_ids, &sums);

    try std.testing.expectEqual(@as(i64, 15), sums[0]);
}
