const std = @import("std");
const blitz = @import("../blitz/mod.zig");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// GroupBy Aggregation Functions
// Scatter-based aggregations for grouped data
// ============================================================================

/// Parallel sum aggregation threshold - parallelize when data is large enough
const PARALLEL_GROUPBY_THRESHOLD: usize = 50_000;

/// Threshold for using sorted-path optimization
const SORTED_CHECK_THRESHOLD: usize = 1000;

// ============================================================================
// Pre-sorted Detection and Contiguous SIMD Aggregation
// ============================================================================

/// Check if group_ids are sorted (enables contiguous SIMD path)
/// Uses SIMD comparison for efficiency
pub fn isSorted(group_ids: []const u32) bool {
    if (group_ids.len < 2) return true;

    const Vec = @Vector(VECTOR_WIDTH, u32);
    const len = group_ids.len;

    // Check if sorted using vectorized comparison
    const aligned_len = len - 1 - ((len - 1) % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const current: Vec = group_ids[i..][0..VECTOR_WIDTH].*;
        const next: Vec = group_ids[i + 1 ..][0..VECTOR_WIDTH].*;

        // Check if any element is greater than the next
        const cmp = current > next;
        if (@reduce(.Or, cmp)) {
            return false;
        }
    }

    // Check remaining elements
    while (i < len - 1) : (i += 1) {
        if (group_ids[i] > group_ids[i + 1]) {
            return false;
        }
    }

    return true;
}

/// SIMD sum for contiguous group (sorted data path)
fn sumContiguousSIMD(comptime T: type, data: []const T) T {
    if (data.len == 0) return 0;

    const Vec = @Vector(VECTOR_WIDTH, T);

    var sum0: Vec = @splat(0);
    var sum1: Vec = @splat(0);
    var sum2: Vec = @splat(0);
    var sum3: Vec = @splat(0);

    const unrolled_len = data.len - (data.len % CHUNK_SIZE);
    var i: usize = 0;

    // Process 32 elements at a time (4 vectors)
    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        sum0 += data[i..][0..VECTOR_WIDTH].*;
        sum1 += data[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        sum2 += data[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        sum3 += data[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;
    }

    // Combine accumulators
    const combined = sum0 + sum1 + sum2 + sum3;
    var result = @reduce(.Add, combined);

    // Handle remaining with single vector
    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        result += @reduce(.Add, chunk);
    }

    // Scalar tail
    while (i < data.len) : (i += 1) {
        result += data[i];
    }

    return result;
}

/// SIMD min for contiguous group
fn minContiguousSIMD(comptime T: type, data: []const T, init_value: T) T {
    if (data.len == 0) return init_value;

    const Vec = @Vector(VECTOR_WIDTH, T);
    var min_vec: Vec = @splat(data[0]);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        min_vec = @min(min_vec, chunk);
    }

    var result = @reduce(.Min, min_vec);

    // Scalar tail
    while (i < data.len) : (i += 1) {
        if (data[i] < result) result = data[i];
    }

    return result;
}

/// SIMD max for contiguous group
fn maxContiguousSIMD(comptime T: type, data: []const T, init_value: T) T {
    if (data.len == 0) return init_value;

    const Vec = @Vector(VECTOR_WIDTH, T);
    var max_vec: Vec = @splat(data[0]);

    const aligned_len = data.len - (data.len % VECTOR_WIDTH);
    var i: usize = 0;

    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const chunk: Vec = data[i..][0..VECTOR_WIDTH].*;
        max_vec = @max(max_vec, chunk);
    }

    var result = @reduce(.Max, max_vec);

    // Scalar tail
    while (i < data.len) : (i += 1) {
        if (data[i] > result) result = data[i];
    }

    return result;
}

/// Sum by group for pre-sorted data - uses contiguous SIMD
pub fn aggregateSumByGroupSorted(comptime T: type, data: []const T, group_ids: []const u32, out_sums: []T) void {
    const len = @min(data.len, group_ids.len);
    if (len == 0) return;

    var start: usize = 0;
    var current_group = group_ids[0];

    var i: usize = 1;
    while (i <= len) : (i += 1) {
        // Check for group boundary or end of data
        const at_boundary = (i == len) or (group_ids[i] != current_group);

        if (at_boundary) {
            // Sum the contiguous group using SIMD
            if (current_group < out_sums.len) {
                out_sums[current_group] = sumContiguousSIMD(T, data[start..i]);
            }

            if (i < len) {
                start = i;
                current_group = group_ids[i];
            }
        }
    }
}

/// Min by group for pre-sorted data
pub fn aggregateMinByGroupSorted(comptime T: type, data: []const T, group_ids: []const u32, out_mins: []T) void {
    const len = @min(data.len, group_ids.len);
    if (len == 0) return;

    const init_value = if (@typeInfo(T) == .float) std.math.floatMax(T) else std.math.maxInt(T);

    var start: usize = 0;
    var current_group = group_ids[0];

    var i: usize = 1;
    while (i <= len) : (i += 1) {
        const at_boundary = (i == len) or (group_ids[i] != current_group);

        if (at_boundary) {
            if (current_group < out_mins.len) {
                out_mins[current_group] = minContiguousSIMD(T, data[start..i], init_value);
            }

            if (i < len) {
                start = i;
                current_group = group_ids[i];
            }
        }
    }
}

/// Max by group for pre-sorted data
pub fn aggregateMaxByGroupSorted(comptime T: type, data: []const T, group_ids: []const u32, out_maxs: []T) void {
    const len = @min(data.len, group_ids.len);
    if (len == 0) return;

    const init_value = if (@typeInfo(T) == .float) -std.math.floatMax(T) else std.math.minInt(T);

    var start: usize = 0;
    var current_group = group_ids[0];

    var i: usize = 1;
    while (i <= len) : (i += 1) {
        const at_boundary = (i == len) or (group_ids[i] != current_group);

        if (at_boundary) {
            if (current_group < out_maxs.len) {
                out_maxs[current_group] = maxContiguousSIMD(T, data[start..i], init_value);
            }

            if (i < len) {
                start = i;
                current_group = group_ids[i];
            }
        }
    }
}

/// Count by group for pre-sorted data (simple counting, no SIMD needed)
pub fn countByGroupSorted(group_ids: []const u32, out_counts: []u64) void {
    if (group_ids.len == 0) return;

    var start: usize = 0;
    var current_group = group_ids[0];

    var i: usize = 1;
    while (i <= group_ids.len) : (i += 1) {
        const at_boundary = (i == group_ids.len) or (group_ids[i] != current_group);

        if (at_boundary) {
            if (current_group < out_counts.len) {
                out_counts[current_group] = i - start;
            }

            if (i < group_ids.len) {
                start = i;
                current_group = group_ids[i];
            }
        }
    }
}

// ============================================================================
// Smart Aggregation Functions (Auto-detect sorted vs scatter)
// ============================================================================

/// Smart sum by group - auto-detects sorted data and uses optimal path
pub fn smartSumByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_sums: []T) void {
    const len = @min(data.len, group_ids.len);

    // For small data or when check is cheap, detect if sorted
    if (len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        aggregateSumByGroupSorted(T, data, group_ids, out_sums);
    } else if (len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelSumByGroup(T, data, group_ids, out_sums);
    } else {
        aggregateSumByGroup(T, data, group_ids, out_sums);
    }
}

/// Smart min by group - auto-detects sorted data
pub fn smartMinByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_mins: []T) void {
    const len = @min(data.len, group_ids.len);

    if (len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        aggregateMinByGroupSorted(T, data, group_ids, out_mins);
    } else if (len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelMinByGroup(T, data, group_ids, out_mins);
    } else {
        aggregateMinByGroup(T, data, group_ids, out_mins);
    }
}

/// Smart max by group - auto-detects sorted data
pub fn smartMaxByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_maxs: []T) void {
    const len = @min(data.len, group_ids.len);

    if (len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        aggregateMaxByGroupSorted(T, data, group_ids, out_maxs);
    } else if (len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelMaxByGroup(T, data, group_ids, out_maxs);
    } else {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
    }
}

/// Smart count by group - auto-detects sorted data
pub fn smartCountByGroup(group_ids: []const u32, out_counts: []u64) void {
    if (group_ids.len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        countByGroupSorted(group_ids, out_counts);
    } else if (group_ids.len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelCountByGroup(group_ids, out_counts);
    } else {
        countByGroup(group_ids, out_counts);
    }
}

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
// Radix Sort-Based GroupBy for High Cardinality
// Sort by group_id, then use contiguous SIMD aggregation
// ============================================================================

/// Threshold for using radix sort approach (when num_groups is high relative to data size)
const RADIX_SORT_MIN_SIZE: usize = 10_000;

/// Radix sort-based sum by group
/// Sorts data by group_id first, then uses contiguous SIMD aggregation
/// More efficient for high-cardinality groupby where scatter pattern has poor cache behavior
pub fn radixSortSumByGroup(
    comptime T: type,
    data: []const T,
    group_ids: []const u32,
    out_sums: []T,
    allocator: std.mem.Allocator,
) void {
    const len = @min(data.len, group_ids.len);
    if (len == 0) return;

    // Allocate temporary arrays for sorted data
    const sorted_data = allocator.alloc(T, len) catch {
        // Fall back to scatter approach
        aggregateSumByGroup(T, data, group_ids, out_sums);
        return;
    };
    defer allocator.free(sorted_data);

    const sorted_ids = allocator.alloc(u32, len) catch {
        aggregateSumByGroup(T, data, group_ids, out_sums);
        return;
    };
    defer allocator.free(sorted_ids);

    // Count sort to reorder by group_id (stable sort)
    const num_groups = out_sums.len;
    const counts = allocator.alloc(usize, num_groups + 1) catch {
        aggregateSumByGroup(T, data, group_ids, out_sums);
        return;
    };
    defer allocator.free(counts);

    // Phase 1: Count elements per group
    @memset(counts, 0);
    for (group_ids) |gid| {
        if (gid < num_groups) {
            counts[gid] += 1;
        }
    }

    // Phase 2: Compute prefix sums (starting positions)
    var total: usize = 0;
    for (0..num_groups) |g| {
        const count = counts[g];
        counts[g] = total;
        total += count;
    }
    counts[num_groups] = total;

    // Phase 3: Scatter data to sorted positions
    const offsets = allocator.alloc(usize, num_groups) catch {
        aggregateSumByGroup(T, data, group_ids, out_sums);
        return;
    };
    defer allocator.free(offsets);
    @memcpy(offsets, counts[0..num_groups]);

    for (0..len) |i| {
        const gid = group_ids[i];
        if (gid < num_groups) {
            const pos = offsets[gid];
            sorted_data[pos] = data[i];
            sorted_ids[pos] = gid;
            offsets[gid] += 1;
        }
    }

    // Phase 4: Aggregate contiguous groups with SIMD
    aggregateSumByGroupSorted(T, sorted_data, sorted_ids, out_sums);
}

/// Radix sort-based min by group
pub fn radixSortMinByGroup(
    comptime T: type,
    data: []const T,
    group_ids: []const u32,
    out_mins: []T,
    allocator: std.mem.Allocator,
) void {
    const len = @min(data.len, group_ids.len);
    if (len == 0) return;

    const sorted_data = allocator.alloc(T, len) catch {
        aggregateMinByGroup(T, data, group_ids, out_mins);
        return;
    };
    defer allocator.free(sorted_data);

    const sorted_ids = allocator.alloc(u32, len) catch {
        aggregateMinByGroup(T, data, group_ids, out_mins);
        return;
    };
    defer allocator.free(sorted_ids);

    const num_groups = out_mins.len;
    const counts = allocator.alloc(usize, num_groups + 1) catch {
        aggregateMinByGroup(T, data, group_ids, out_mins);
        return;
    };
    defer allocator.free(counts);

    @memset(counts, 0);
    for (group_ids) |gid| {
        if (gid < num_groups) counts[gid] += 1;
    }

    var total: usize = 0;
    for (0..num_groups) |g| {
        const count = counts[g];
        counts[g] = total;
        total += count;
    }
    counts[num_groups] = total;

    const offsets = allocator.alloc(usize, num_groups) catch {
        aggregateMinByGroup(T, data, group_ids, out_mins);
        return;
    };
    defer allocator.free(offsets);
    @memcpy(offsets, counts[0..num_groups]);

    for (0..len) |i| {
        const gid = group_ids[i];
        if (gid < num_groups) {
            const pos = offsets[gid];
            sorted_data[pos] = data[i];
            sorted_ids[pos] = gid;
            offsets[gid] += 1;
        }
    }

    aggregateMinByGroupSorted(T, sorted_data, sorted_ids, out_mins);
}

/// Radix sort-based max by group
pub fn radixSortMaxByGroup(
    comptime T: type,
    data: []const T,
    group_ids: []const u32,
    out_maxs: []T,
    allocator: std.mem.Allocator,
) void {
    const len = @min(data.len, group_ids.len);
    if (len == 0) return;

    const sorted_data = allocator.alloc(T, len) catch {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
        return;
    };
    defer allocator.free(sorted_data);

    const sorted_ids = allocator.alloc(u32, len) catch {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
        return;
    };
    defer allocator.free(sorted_ids);

    const num_groups = out_maxs.len;
    const counts = allocator.alloc(usize, num_groups + 1) catch {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
        return;
    };
    defer allocator.free(counts);

    @memset(counts, 0);
    for (group_ids) |gid| {
        if (gid < num_groups) counts[gid] += 1;
    }

    var total: usize = 0;
    for (0..num_groups) |g| {
        const count = counts[g];
        counts[g] = total;
        total += count;
    }
    counts[num_groups] = total;

    const offsets = allocator.alloc(usize, num_groups) catch {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
        return;
    };
    defer allocator.free(offsets);
    @memcpy(offsets, counts[0..num_groups]);

    for (0..len) |i| {
        const gid = group_ids[i];
        if (gid < num_groups) {
            const pos = offsets[gid];
            sorted_data[pos] = data[i];
            sorted_ids[pos] = gid;
            offsets[gid] += 1;
        }
    }

    aggregateMaxByGroupSorted(T, sorted_data, sorted_ids, out_maxs);
}

/// Determine if radix sort approach is beneficial
/// Returns true when num_groups is high relative to data size (poor cache locality for scatter)
fn shouldUseRadixSort(data_len: usize, num_groups: usize) bool {
    // Use radix sort when:
    // 1. Data is large enough to benefit
    // 2. High cardinality (many groups relative to data)
    // The scatter pattern has poor cache behavior when groups are spread out
    if (data_len < RADIX_SORT_MIN_SIZE) return false;

    // If ratio of groups to data is high, radix sort is better
    // Threshold: if average group size < 100, use radix sort
    const avg_group_size = data_len / @max(num_groups, 1);
    return avg_group_size < 100;
}

/// Optimal sum by group - chooses best algorithm based on data characteristics
pub fn optimalSumByGroup(
    comptime T: type,
    data: []const T,
    group_ids: []const u32,
    out_sums: []T,
    allocator: std.mem.Allocator,
) void {
    const len = @min(data.len, group_ids.len);
    const num_groups = out_sums.len;

    // Check for sorted data first (fastest path)
    if (len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        aggregateSumByGroupSorted(T, data, group_ids, out_sums);
        return;
    }

    // Check if radix sort would be beneficial
    if (shouldUseRadixSort(len, num_groups)) {
        radixSortSumByGroup(T, data, group_ids, out_sums, allocator);
        return;
    }

    // Fall back to parallel or sequential scatter
    if (len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelSumByGroup(T, data, group_ids, out_sums);
    } else {
        aggregateSumByGroup(T, data, group_ids, out_sums);
    }
}

/// Optimal min by group
pub fn optimalMinByGroup(
    comptime T: type,
    data: []const T,
    group_ids: []const u32,
    out_mins: []T,
    allocator: std.mem.Allocator,
) void {
    const len = @min(data.len, group_ids.len);
    const num_groups = out_mins.len;

    if (len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        aggregateMinByGroupSorted(T, data, group_ids, out_mins);
        return;
    }

    if (shouldUseRadixSort(len, num_groups)) {
        radixSortMinByGroup(T, data, group_ids, out_mins, allocator);
        return;
    }

    if (len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelMinByGroup(T, data, group_ids, out_mins);
    } else {
        aggregateMinByGroup(T, data, group_ids, out_mins);
    }
}

/// Optimal max by group
pub fn optimalMaxByGroup(
    comptime T: type,
    data: []const T,
    group_ids: []const u32,
    out_maxs: []T,
    allocator: std.mem.Allocator,
) void {
    const len = @min(data.len, group_ids.len);
    const num_groups = out_maxs.len;

    if (len > SORTED_CHECK_THRESHOLD and isSorted(group_ids)) {
        aggregateMaxByGroupSorted(T, data, group_ids, out_maxs);
        return;
    }

    if (shouldUseRadixSort(len, num_groups)) {
        radixSortMaxByGroup(T, data, group_ids, out_maxs, allocator);
        return;
    }

    if (len >= PARALLEL_GROUPBY_THRESHOLD) {
        parallelMaxByGroup(T, data, group_ids, out_maxs);
    } else {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
    }
}

// ============================================================================
// Parallel GroupBy Aggregations with Thread-Local Accumulators
// ============================================================================

const MAX_THREADS = 32;

/// Parallel sum by group using thread-local accumulators
/// Falls back to sequential for small data or when allocation fails
pub fn parallelSumByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_sums: []T) void {
    const len = @min(data.len, group_ids.len);
    const num_groups = out_sums.len;

    // Fall back to sequential for small data
    if (len < PARALLEL_GROUPBY_THRESHOLD or num_groups == 0) {
        aggregateSumByGroup(T, data, group_ids, out_sums);
        return;
    }

    const num_workers = blitz.numWorkers();
    if (num_workers <= 1) {
        aggregateSumByGroup(T, data, group_ids, out_sums);
        return;
    }

    // Allocate thread-local partial sums
    var local_sums: [MAX_THREADS][]T = undefined;
    var allocated_count: usize = 0;

    for (0..num_workers) |t| {
        local_sums[t] = std.heap.c_allocator.alloc(T, num_groups) catch {
            // Allocation failed, clean up and fall back to sequential
            for (0..allocated_count) |i| {
                std.heap.c_allocator.free(local_sums[i]);
            }
            aggregateSumByGroup(T, data, group_ids, out_sums);
            return;
        };
        allocated_count += 1;
        // Zero-initialize
        @memset(local_sums[t], 0);
    }
    defer {
        for (0..num_workers) |t| {
            std.heap.c_allocator.free(local_sums[t]);
        }
    }

    // Parallel accumulation into thread-local arrays
    const chunk_size = (len + num_workers - 1) / num_workers;

    const SumContext = struct {
        data: []const T,
        group_ids: []const u32,
        local_sums: *[MAX_THREADS][]T,
        chunk_size: usize,
        len: usize,
    };

    const ctx = SumContext{
        .data = data,
        .group_ids = group_ids,
        .local_sums = &local_sums,
        .chunk_size = chunk_size,
        .len = len,
    };

    blitz.parallelFor(
        num_workers,
        SumContext,
        ctx,
        struct {
            fn work(c: SumContext, worker_start: usize, worker_end: usize) void {
                for (worker_start..worker_end) |worker_id| {
                    const start = worker_id * c.chunk_size;
                    const end = @min(start + c.chunk_size, c.len);
                    const my_sums = c.local_sums[worker_id];

                    for (start..end) |i| {
                        const gid = c.group_ids[i];
                        if (gid < my_sums.len) {
                            my_sums[gid] += c.data[i];
                        }
                    }
                }
            }
        }.work,
    );

    // Merge thread-local results into output
    for (0..num_groups) |g| {
        var total: T = 0;
        for (0..num_workers) |t| {
            total += local_sums[t][g];
        }
        out_sums[g] = total;
    }
}

/// Parallel min by group using thread-local accumulators
pub fn parallelMinByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_mins: []T) void {
    const len = @min(data.len, group_ids.len);
    const num_groups = out_mins.len;

    if (len < PARALLEL_GROUPBY_THRESHOLD or num_groups == 0) {
        aggregateMinByGroup(T, data, group_ids, out_mins);
        return;
    }

    const num_workers = blitz.numWorkers();
    if (num_workers <= 1) {
        aggregateMinByGroup(T, data, group_ids, out_mins);
        return;
    }

    // Allocate thread-local partial mins
    var local_mins: [MAX_THREADS][]T = undefined;
    var allocated_count: usize = 0;

    const init_value = if (@typeInfo(T) == .float) std.math.floatMax(T) else std.math.maxInt(T);

    for (0..num_workers) |t| {
        local_mins[t] = std.heap.c_allocator.alloc(T, num_groups) catch {
            for (0..allocated_count) |i| {
                std.heap.c_allocator.free(local_mins[i]);
            }
            aggregateMinByGroup(T, data, group_ids, out_mins);
            return;
        };
        allocated_count += 1;
        @memset(local_mins[t], init_value);
    }
    defer {
        for (0..num_workers) |t| {
            std.heap.c_allocator.free(local_mins[t]);
        }
    }

    const chunk_size = (len + num_workers - 1) / num_workers;

    const MinContext = struct {
        data: []const T,
        group_ids: []const u32,
        local_mins: *[MAX_THREADS][]T,
        chunk_size: usize,
        len: usize,
    };

    const ctx = MinContext{
        .data = data,
        .group_ids = group_ids,
        .local_mins = &local_mins,
        .chunk_size = chunk_size,
        .len = len,
    };

    blitz.parallelFor(
        num_workers,
        MinContext,
        ctx,
        struct {
            fn work(c: MinContext, worker_start: usize, worker_end: usize) void {
                for (worker_start..worker_end) |worker_id| {
                    const start = worker_id * c.chunk_size;
                    const end = @min(start + c.chunk_size, c.len);
                    const my_mins = c.local_mins[worker_id];

                    for (start..end) |i| {
                        const gid = c.group_ids[i];
                        if (gid < my_mins.len and c.data[i] < my_mins[gid]) {
                            my_mins[gid] = c.data[i];
                        }
                    }
                }
            }
        }.work,
    );

    // Merge - take minimum across all thread-local results
    for (0..num_groups) |g| {
        var result: T = init_value;
        for (0..num_workers) |t| {
            if (local_mins[t][g] < result) {
                result = local_mins[t][g];
            }
        }
        out_mins[g] = result;
    }
}

/// Parallel max by group using thread-local accumulators
pub fn parallelMaxByGroup(comptime T: type, data: []const T, group_ids: []const u32, out_maxs: []T) void {
    const len = @min(data.len, group_ids.len);
    const num_groups = out_maxs.len;

    if (len < PARALLEL_GROUPBY_THRESHOLD or num_groups == 0) {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
        return;
    }

    const num_workers = blitz.numWorkers();
    if (num_workers <= 1) {
        aggregateMaxByGroup(T, data, group_ids, out_maxs);
        return;
    }

    // Allocate thread-local partial maxs
    var local_maxs: [MAX_THREADS][]T = undefined;
    var allocated_count: usize = 0;

    const init_value = if (@typeInfo(T) == .float) -std.math.floatMax(T) else std.math.minInt(T);

    for (0..num_workers) |t| {
        local_maxs[t] = std.heap.c_allocator.alloc(T, num_groups) catch {
            for (0..allocated_count) |i| {
                std.heap.c_allocator.free(local_maxs[i]);
            }
            aggregateMaxByGroup(T, data, group_ids, out_maxs);
            return;
        };
        allocated_count += 1;
        @memset(local_maxs[t], init_value);
    }
    defer {
        for (0..num_workers) |t| {
            std.heap.c_allocator.free(local_maxs[t]);
        }
    }

    const chunk_size = (len + num_workers - 1) / num_workers;

    const MaxContext = struct {
        data: []const T,
        group_ids: []const u32,
        local_maxs: *[MAX_THREADS][]T,
        chunk_size: usize,
        len: usize,
    };

    const ctx = MaxContext{
        .data = data,
        .group_ids = group_ids,
        .local_maxs = &local_maxs,
        .chunk_size = chunk_size,
        .len = len,
    };

    blitz.parallelFor(
        num_workers,
        MaxContext,
        ctx,
        struct {
            fn work(c: MaxContext, worker_start: usize, worker_end: usize) void {
                for (worker_start..worker_end) |worker_id| {
                    const start = worker_id * c.chunk_size;
                    const end = @min(start + c.chunk_size, c.len);
                    const my_maxs = c.local_maxs[worker_id];

                    for (start..end) |i| {
                        const gid = c.group_ids[i];
                        if (gid < my_maxs.len and c.data[i] > my_maxs[gid]) {
                            my_maxs[gid] = c.data[i];
                        }
                    }
                }
            }
        }.work,
    );

    // Merge - take maximum across all thread-local results
    for (0..num_groups) |g| {
        var result: T = init_value;
        for (0..num_workers) |t| {
            if (local_maxs[t][g] > result) {
                result = local_maxs[t][g];
            }
        }
        out_maxs[g] = result;
    }
}

/// Parallel count by group using thread-local accumulators
pub fn parallelCountByGroup(group_ids: []const u32, out_counts: []u64) void {
    const len = group_ids.len;
    const num_groups = out_counts.len;

    if (len < PARALLEL_GROUPBY_THRESHOLD or num_groups == 0) {
        countByGroup(group_ids, out_counts);
        return;
    }

    const num_workers = blitz.numWorkers();
    if (num_workers <= 1) {
        countByGroup(group_ids, out_counts);
        return;
    }

    // Allocate thread-local partial counts
    var local_counts: [MAX_THREADS][]u64 = undefined;
    var allocated_count: usize = 0;

    for (0..num_workers) |t| {
        local_counts[t] = std.heap.c_allocator.alloc(u64, num_groups) catch {
            for (0..allocated_count) |i| {
                std.heap.c_allocator.free(local_counts[i]);
            }
            countByGroup(group_ids, out_counts);
            return;
        };
        allocated_count += 1;
        @memset(local_counts[t], 0);
    }
    defer {
        for (0..num_workers) |t| {
            std.heap.c_allocator.free(local_counts[t]);
        }
    }

    const chunk_size = (len + num_workers - 1) / num_workers;

    const CountContext = struct {
        group_ids: []const u32,
        local_counts: *[MAX_THREADS][]u64,
        chunk_size: usize,
        len: usize,
    };

    const ctx = CountContext{
        .group_ids = group_ids,
        .local_counts = &local_counts,
        .chunk_size = chunk_size,
        .len = len,
    };

    blitz.parallelFor(
        num_workers,
        CountContext,
        ctx,
        struct {
            fn work(c: CountContext, worker_start: usize, worker_end: usize) void {
                for (worker_start..worker_end) |worker_id| {
                    const start = worker_id * c.chunk_size;
                    const end = @min(start + c.chunk_size, c.len);
                    const my_counts = c.local_counts[worker_id];

                    for (start..end) |i| {
                        const gid = c.group_ids[i];
                        if (gid < my_counts.len) {
                            my_counts[gid] += 1;
                        }
                    }
                }
            }
        }.work,
    );

    // Merge thread-local results
    for (0..num_groups) |g| {
        var total: u64 = 0;
        for (0..num_workers) |t| {
            total += local_counts[t][g];
        }
        out_counts[g] = total;
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

// ============================================================================
// Tests for sorted detection and SIMD aggregation
// ============================================================================

test "groupby_agg - isSorted detects sorted data" {
    const sorted = [_]u32{ 0, 0, 1, 1, 2, 2, 3 };
    const unsorted = [_]u32{ 0, 1, 0, 2, 1, 0, 2 };

    try std.testing.expect(isSorted(&sorted));
    try std.testing.expect(!isSorted(&unsorted));
}

test "groupby_agg - isSorted edge cases" {
    const empty: []const u32 = &[_]u32{};
    const single = [_]u32{5};
    const same = [_]u32{ 3, 3, 3, 3 };

    try std.testing.expect(isSorted(empty));
    try std.testing.expect(isSorted(&single));
    try std.testing.expect(isSorted(&same));
}

test "groupby_agg - aggregateSumByGroupSorted" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 2, 2, 2 };
    var sums = [_]f64{ 0.0, 0.0, 0.0 };

    aggregateSumByGroupSorted(f64, &data, &group_ids, &sums);

    // Group 0: 1 + 2 + 3 = 6
    // Group 1: 4 + 5 = 9
    // Group 2: 6 + 7 + 8 = 21
    try std.testing.expectEqual(@as(f64, 6.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 9.0), sums[1]);
    try std.testing.expectEqual(@as(f64, 21.0), sums[2]);
}

test "groupby_agg - aggregateMinByGroupSorted" {
    const data = [_]f64{ 3.0, 1.0, 2.0, 5.0, 4.0, 9.0, 7.0, 8.0 };
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 2, 2, 2 };
    var mins = [_]f64{ std.math.floatMax(f64), std.math.floatMax(f64), std.math.floatMax(f64) };

    aggregateMinByGroupSorted(f64, &data, &group_ids, &mins);

    try std.testing.expectEqual(@as(f64, 1.0), mins[0]);
    try std.testing.expectEqual(@as(f64, 4.0), mins[1]);
    try std.testing.expectEqual(@as(f64, 7.0), mins[2]);
}

test "groupby_agg - aggregateMaxByGroupSorted" {
    const data = [_]f64{ 3.0, 1.0, 2.0, 5.0, 4.0, 9.0, 7.0, 8.0 };
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 2, 2, 2 };
    var maxs = [_]f64{ -std.math.floatMax(f64), -std.math.floatMax(f64), -std.math.floatMax(f64) };

    aggregateMaxByGroupSorted(f64, &data, &group_ids, &maxs);

    try std.testing.expectEqual(@as(f64, 3.0), maxs[0]);
    try std.testing.expectEqual(@as(f64, 5.0), maxs[1]);
    try std.testing.expectEqual(@as(f64, 9.0), maxs[2]);
}

test "groupby_agg - countByGroupSorted" {
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 2, 2, 2, 2 };
    var counts = [_]u64{ 0, 0, 0 };

    countByGroupSorted(&group_ids, &counts);

    try std.testing.expectEqual(@as(u64, 3), counts[0]);
    try std.testing.expectEqual(@as(u64, 2), counts[1]);
    try std.testing.expectEqual(@as(u64, 4), counts[2]);
}

test "groupby_agg - radixSortSumByGroup" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const group_ids = [_]u32{ 0, 1, 0, 1, 0, 1 };
    var sums = [_]f64{ 0.0, 0.0 };

    radixSortSumByGroup(f64, &data, &group_ids, &sums, allocator);

    // Group 0: 1.0 + 3.0 + 5.0 = 9.0
    // Group 1: 2.0 + 4.0 + 6.0 = 12.0
    try std.testing.expectEqual(@as(f64, 9.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 12.0), sums[1]);
}

test "groupby_agg - smartSumByGroup selects correct path" {
    // Test with small unsorted data (should use scatter)
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const group_ids = [_]u32{ 0, 1, 0, 1, 0, 1 };
    var sums = [_]f64{ 0.0, 0.0 };

    smartSumByGroup(f64, &data, &group_ids, &sums);

    try std.testing.expectEqual(@as(f64, 9.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 12.0), sums[1]);
}

test "groupby_agg - smartSumByGroup with sorted data" {
    // Test with sorted data
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    const group_ids = [_]u32{ 0, 0, 0, 1, 1, 2, 2, 2 };
    var sums = [_]f64{ 0.0, 0.0, 0.0 };

    smartSumByGroup(f64, &data, &group_ids, &sums);

    try std.testing.expectEqual(@as(f64, 6.0), sums[0]);
    try std.testing.expectEqual(@as(f64, 9.0), sums[1]);
    try std.testing.expectEqual(@as(f64, 21.0), sums[2]);
}
