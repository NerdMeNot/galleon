//! High-Performance Sorting for Galleon
//!
//! Implements optimized radix sort with:
//! - 8-bit radix (256 buckets) = 8 passes for 64-bit data (better L1 cache fit)
//! - Skip-pass optimization for uniform digits
//! - Parallel scatter for large arrays
//! - SIMD key transformation
//! - Verge sort for pre-sorted data detection

const std = @import("std");
const blitz = @import("../blitz.zig");

// ============================================================================
// Configuration
// ============================================================================

/// 8-bit radix for 8 passes on 64-bit data
/// 256 buckets * 8 bytes = 2KB (fits in L1 cache with room to spare)
/// Better cache locality outweighs extra passes for large arrays
pub const RADIX_BITS: u5 = 8;
pub const NUM_BUCKETS: usize = 1 << RADIX_BITS; // 256
pub const RADIX_MASK: u64 = NUM_BUCKETS - 1;

/// Threshold for switching to insertion sort
const INSERTION_THRESHOLD: usize = 64;

/// Threshold for using parallel operations
/// Based on voracious_sort: parallel comparison sort wins until ~800K elements
/// Parallel radix sort only wins for very large arrays
const PARALLEL_THRESHOLD: usize = 500_000;

/// Prefetch distance in elements
const PREFETCH_DISTANCE: usize = 32;

/// SIMD vector width for key transformation
const VECTOR_WIDTH: usize = 4;

/// Get the parallel sort threshold based on system configuration.
pub fn getParallelSortThreshold() usize {
    return blitz.threshold.getThreshold(.sort);
}

/// Check if parallel sorting is beneficial for this data size.
pub fn shouldParallelSort(len: usize) bool {
    return blitz.shouldParallelize(.sort, len);
}

// ============================================================================
// Key Conversion Utilities
// ============================================================================

/// Convert f64 to sortable u64 representation
/// Maps floats to integers that sort in the same order
pub inline fn floatToSortable(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    const sign_bit = bits >> 63;
    const mask: u64 = (0 -% sign_bit) | (@as(u64, 1) << 63);
    return bits ^ mask;
}

/// Convert sortable u64 back to f64
/// - If sortable sign_bit is 0, original was negative (XOR with all 1s)
/// - If sortable sign_bit is 1, original was positive (XOR with just sign bit)
pub inline fn sortableToF64(bits: u64) f64 {
    const sign_bit = bits >> 63;
    // For sign_bit = 0: (0 -% 1) = 0xFFFFFFFFFFFFFFFF (all 1s)
    // For sign_bit = 1: (1 -% 1) = 0, then OR with sign bit = 0x8000...
    const mask: u64 = (0 -% (1 - sign_bit)) | (@as(u64, 1) << 63);
    return @bitCast(bits ^ mask);
}

/// Convert i64 to sortable u64 representation
/// Flip sign bit so negative numbers sort before positive
pub inline fn i64ToSortable(val: i64) u64 {
    const bits: u64 = @bitCast(val);
    return bits ^ (@as(u64, 1) << 63);
}

/// Convert sortable u64 back to i64
pub inline fn sortableToI64(bits: u64) i64 {
    return @bitCast(bits ^ (@as(u64, 1) << 63));
}

// ============================================================================
// SIMD Key Transformation
// ============================================================================

/// True SIMD vector width for u64 (256-bit AVX = 4 x u64)
const SIMD_WIDTH: usize = 4;
const U64Vec = @Vector(SIMD_WIDTH, u64);

/// Vectorized float-to-sortable conversion using true SIMD
fn transformKeysF64(data: []const f64, keys: []u64) void {
    const len = data.len;
    const simd_end = len - (len % SIMD_WIDTH);

    const sign_bit_vec: U64Vec = @splat(@as(u64, 1) << 63);

    var i: usize = 0;
    while (i < simd_end) : (i += SIMD_WIDTH) {
        // Load 4 f64 values as u64 bits
        const bits: U64Vec = @bitCast(data[i..][0..SIMD_WIDTH].*);
        // Extract sign bits (1 for negative, 0 for positive)
        const sign_bits = bits >> @splat(@as(u6, 63));
        // Create mask: all 1s for negative, just sign bit for positive
        // For negative: (0 - 1) | sign_bit = all_ones
        // For positive: (0 - 0) | sign_bit = sign_bit
        const neg_mask = @as(U64Vec, @splat(0)) -% sign_bits;
        const mask = neg_mask | sign_bit_vec;
        // XOR to create sortable keys
        const sortable = bits ^ mask;
        // Store result
        keys[i..][0..SIMD_WIDTH].* = sortable;
    }

    // Handle remainder
    while (i < len) : (i += 1) {
        keys[i] = floatToSortable(data[i]);
    }
}

/// Vectorized sortable-to-f64 conversion using true SIMD
fn transformKeysToF64(keys: []const u64, out: []f64) void {
    const len = @min(keys.len, out.len);
    const simd_end = len - (len % SIMD_WIDTH);

    const sign_bit_vec: U64Vec = @splat(@as(u64, 1) << 63);

    var i: usize = 0;
    while (i < simd_end) : (i += SIMD_WIDTH) {
        // Load 4 sortable keys
        const bits: U64Vec = keys[i..][0..SIMD_WIDTH].*;
        // Extract sign bits of sortable representation
        // sign_bit = 0 means original was NEGATIVE (need XOR with all 1s)
        // sign_bit = 1 means original was POSITIVE (need XOR with sign bit)
        const sign_bits = bits >> @splat(@as(u6, 63));
        // Create mask: all 1s for sign_bit=0, just sign bit for sign_bit=1
        // For sign_bit=0: (0 - 1) | sign_bit = all_ones
        // For sign_bit=1: (0 - 0) | sign_bit = sign_bit
        const neg_mask = @as(U64Vec, @splat(0)) -% (@as(U64Vec, @splat(1)) - sign_bits);
        const mask = neg_mask | sign_bit_vec;
        // XOR to recover original float bits
        const original_bits = bits ^ mask;
        // Store as f64
        out[i..][0..SIMD_WIDTH].* = @bitCast(original_bits);
    }

    // Handle remainder
    while (i < len) : (i += 1) {
        out[i] = sortableToF64(keys[i]);
    }
}

/// Vectorized sortable-to-f64 in reverse order (for descending sort)
fn transformKeysToF64Reverse(keys: []const u64, out: []f64) void {
    const len = @min(keys.len, out.len);
    if (len == 0) return;

    // Process in reverse - can't easily SIMD this due to non-contiguous access
    // But we can still use scalar with good cache behavior
    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = sortableToF64(keys[len - 1 - i]);
    }
}

/// Vectorized i64-to-sortable conversion
fn transformKeysI64(data: []const i64, keys: []u64) void {
    const len = data.len;
    const simd_end = len - (len % SIMD_WIDTH);

    const sign_bit_vec: U64Vec = @splat(@as(u64, 1) << 63);

    var i: usize = 0;
    while (i < simd_end) : (i += SIMD_WIDTH) {
        const bits: U64Vec = @bitCast(data[i..][0..SIMD_WIDTH].*);
        keys[i..][0..SIMD_WIDTH].* = bits ^ sign_bit_vec;
    }

    while (i < len) : (i += 1) {
        keys[i] = i64ToSortable(data[i]);
    }
}

/// Vectorized sortable-to-i64 conversion
fn transformKeysToI64(keys: []const u64, out: []i64) void {
    const len = @min(keys.len, out.len);
    const simd_end = len - (len % SIMD_WIDTH);

    const sign_bit_vec: U64Vec = @splat(@as(u64, 1) << 63);

    var i: usize = 0;
    while (i < simd_end) : (i += SIMD_WIDTH) {
        const bits: U64Vec = keys[i..][0..SIMD_WIDTH].*;
        out[i..][0..SIMD_WIDTH].* = @bitCast(bits ^ sign_bit_vec);
    }

    while (i < len) : (i += 1) {
        out[i] = sortableToI64(keys[i]);
    }
}

/// Vectorized sortable-to-i64 in reverse order (for descending sort)
fn transformKeysToI64Reverse(keys: []const u64, out: []i64) void {
    const len = @min(keys.len, out.len);
    if (len == 0) return;

    var i: usize = 0;
    while (i < len) : (i += 1) {
        out[i] = sortableToI64(keys[len - 1 - i]);
    }
}

// ============================================================================
// Histogram Computation
// ============================================================================

/// Result of histogram computation with skip detection
const HistogramResult = struct {
    can_skip: bool,
    min_bucket: usize,
    max_bucket: usize,
};

/// Compute histogram with skip detection
fn computeHistogramWithSkip(keys: []const u64, shift: u6, counts: []usize) HistogramResult {
    @memset(counts[0..NUM_BUCKETS], 0);

    const len = keys.len;
    const unroll_end = len - (len % 8);
    var i: usize = 0;

    while (i < unroll_end) : (i += 8) {
        if (i + PREFETCH_DISTANCE < len) {
            @prefetch(@as([*]const u8, @ptrCast(&keys[i + PREFETCH_DISTANCE])), .{});
        }

        const b0: usize = @intCast((keys[i] >> shift) & RADIX_MASK);
        const b1: usize = @intCast((keys[i + 1] >> shift) & RADIX_MASK);
        const b2: usize = @intCast((keys[i + 2] >> shift) & RADIX_MASK);
        const b3: usize = @intCast((keys[i + 3] >> shift) & RADIX_MASK);
        const b4: usize = @intCast((keys[i + 4] >> shift) & RADIX_MASK);
        const b5: usize = @intCast((keys[i + 5] >> shift) & RADIX_MASK);
        const b6: usize = @intCast((keys[i + 6] >> shift) & RADIX_MASK);
        const b7: usize = @intCast((keys[i + 7] >> shift) & RADIX_MASK);

        counts[b0] += 1;
        counts[b1] += 1;
        counts[b2] += 1;
        counts[b3] += 1;
        counts[b4] += 1;
        counts[b5] += 1;
        counts[b6] += 1;
        counts[b7] += 1;
    }

    while (i < len) : (i += 1) {
        const b: usize = @intCast((keys[i] >> shift) & RADIX_MASK);
        counts[b] += 1;
    }

    // Check for skip
    var min_bucket: usize = NUM_BUCKETS;
    var max_bucket: usize = 0;
    var non_zero_count: usize = 0;

    for (0..NUM_BUCKETS) |bucket| {
        if (counts[bucket] > 0) {
            if (min_bucket == NUM_BUCKETS) min_bucket = bucket;
            max_bucket = bucket;
            non_zero_count += 1;
        }
    }

    return .{
        .can_skip = non_zero_count == 1,
        .min_bucket = min_bucket,
        .max_bucket = max_bucket,
    };
}

// ============================================================================
// Insertion Sort (for small arrays)
// ============================================================================

fn insertionSortKeys(keys: []u64, indices: []u32) void {
    if (keys.len <= 1) return;

    for (1..keys.len) |i| {
        const key = keys[i];
        const idx = indices[i];
        var j = i;

        while (j > 0 and keys[j - 1] > key) {
            keys[j] = keys[j - 1];
            indices[j] = indices[j - 1];
            j -= 1;
        }

        keys[j] = key;
        indices[j] = idx;
    }
}

fn insertionSortKeysOnly(keys: []u64) void {
    if (keys.len <= 1) return;

    for (1..keys.len) |i| {
        const key = keys[i];
        var j = i;

        while (j > 0 and keys[j - 1] > key) {
            keys[j] = keys[j - 1];
            j -= 1;
        }

        keys[j] = key;
    }
}

fn insertionSortIndirectF64(data: []const f64, indices: []u32, ascending: bool) void {
    if (indices.len <= 1) return;

    for (1..indices.len) |i| {
        const idx = indices[i];
        const val = data[idx];
        var j = i;

        while (j > 0) {
            const cmp = if (ascending)
                val < data[indices[j - 1]]
            else
                val > data[indices[j - 1]];

            if (!cmp) break;
            indices[j] = indices[j - 1];
            j -= 1;
        }
        indices[j] = idx;
    }
}

fn insertionSortIndirectI64(data: []const i64, indices: []u32, ascending: bool) void {
    if (indices.len <= 1) return;

    for (1..indices.len) |i| {
        const idx = indices[i];
        const val = data[idx];
        var j = i;

        while (j > 0) {
            const cmp = if (ascending)
                val < data[indices[j - 1]]
            else
                val > data[indices[j - 1]];

            if (!cmp) break;
            indices[j] = indices[j - 1];
            j -= 1;
        }
        indices[j] = idx;
    }
}

// ============================================================================
// Sorted Detection (Verge Sort)
// ============================================================================

fn isSortedF64(data: []const f64, ascending: bool) bool {
    if (data.len < 2) return true;

    if (ascending) {
        for (1..data.len) |i| {
            if (data[i] < data[i - 1]) return false;
        }
    } else {
        for (1..data.len) |i| {
            if (data[i] > data[i - 1]) return false;
        }
    }
    return true;
}

fn isSortedI64(data: []const i64, ascending: bool) bool {
    if (data.len < 2) return true;

    if (ascending) {
        for (1..data.len) |i| {
            if (data[i] < data[i - 1]) return false;
        }
    } else {
        for (1..data.len) |i| {
            if (data[i] > data[i - 1]) return false;
        }
    }
    return true;
}

// ============================================================================
// Verge Sort Run Detection
// Based on voracious_sort's verge_sort_heuristic.rs
// ============================================================================

/// Compute the minimum size for a "big enough" run
/// This is n / log2(n) - determines jump distance and minimum useful run size
fn computeJumpSize(size: usize) usize {
    if (size < 4) return 1;
    // Approximate log2 using leading zeros
    const log2_size = @bitSizeOf(usize) - @clz(size);
    return size / log2_size;
}

/// Explore forward from start position looking for ascending run
/// Returns the exclusive end of the ascending run
fn exploreForwardAscF64(data: []const f64, start: usize) usize {
    if (start >= data.len - 1) return data.len;

    var i = start;
    // Unrolled check for 4 elements at a time
    const unroll_end = if (data.len >= 4) data.len - 4 else start;
    while (i < unroll_end) {
        const b0 = data[i] <= data[i + 1];
        const b1 = data[i + 1] <= data[i + 2] and b0;
        const b2 = data[i + 2] <= data[i + 3] and b1;
        const b3 = data[i + 3] <= data[i + 4] and b2;

        if (b3) {
            i += 4;
        } else if (b2) {
            return i + 4;
        } else if (b1) {
            return i + 3;
        } else if (b0) {
            return i + 2;
        } else {
            return i + 1;
        }
    }

    // Scalar cleanup
    while (i < data.len - 1) {
        if (data[i] <= data[i + 1]) {
            i += 1;
        } else {
            return i + 1;
        }
    }
    return i + 1;
}

/// Explore forward from start position looking for descending run
/// Returns the exclusive end of the descending run
fn exploreForwardDescF64(data: []const f64, start: usize) usize {
    if (start >= data.len - 1) return data.len;

    var i = start;
    const unroll_end = if (data.len >= 4) data.len - 4 else start;
    while (i < unroll_end) {
        const b0 = data[i] >= data[i + 1];
        const b1 = data[i + 1] >= data[i + 2] and b0;
        const b2 = data[i + 2] >= data[i + 3] and b1;
        const b3 = data[i + 3] >= data[i + 4] and b2;

        if (b3) {
            i += 4;
        } else if (b2) {
            return i + 4;
        } else if (b1) {
            return i + 3;
        } else if (b0) {
            return i + 2;
        } else {
            return i + 1;
        }
    }

    while (i < data.len - 1) {
        if (data[i] >= data[i + 1]) {
            i += 1;
        } else {
            return i + 1;
        }
    }
    return i + 1;
}

/// Explore backward from position looking for ascending run start
fn exploreBackwardAscF64(data: []const f64, position: usize, min_boundary: usize) usize {
    if (position <= min_boundary) return min_boundary;

    var i = position;
    while (i > min_boundary) {
        if (data[i - 1] <= data[i]) {
            i -= 1;
        } else {
            break;
        }
    }
    return i;
}

/// Explore backward from position looking for descending run start
fn exploreBackwardDescF64(data: []const f64, position: usize, min_boundary: usize) usize {
    if (position <= min_boundary) return min_boundary;

    var i = position;
    while (i > min_boundary) {
        if (data[i - 1] >= data[i]) {
            i -= 1;
        } else {
            break;
        }
    }
    return i;
}

/// Reverse a slice of f64 in-place
fn reverseF64(data: []f64) void {
    if (data.len <= 1) return;
    var left: usize = 0;
    var right: usize = data.len - 1;
    while (left < right) {
        const tmp = data[left];
        data[left] = data[right];
        data[right] = tmp;
        left += 1;
        right -= 1;
    }
}

/// Run descriptor for verge sort
const RunDescriptor = struct {
    start: usize,
    end: usize,
};

/// Maximum number of runs we track (if more, fall back to regular sort)
const MAX_RUNS: usize = 32;

/// Verge Sort preprocessing: detect sorted runs and convert descending to ascending
/// Returns the number of runs found (0 if should fall back to regular sort)
fn vergeSortPreprocessingF64(data: []f64, runs: *[MAX_RUNS]RunDescriptor) usize {
    const len = data.len;
    if (len < 2) {
        runs[0] = .{ .start = 0, .end = len };
        return 1;
    }

    const big_enough = computeJumpSize(len);
    var num_runs: usize = 0;
    var position: usize = 0;

    while (position < len) {
        if (num_runs >= MAX_RUNS) return 0; // Too many runs, fall back

        // Determine direction at this position
        if (position >= len - 1) {
            // Single element left
            runs[num_runs] = .{ .start = position, .end = len };
            num_runs += 1;
            break;
        }

        const run_start = position;
        var run_end: usize = undefined;

        if (data[position] <= data[position + 1]) {
            // Ascending run
            run_end = exploreForwardAscF64(data, position);
        } else {
            // Descending run - explore then reverse
            run_end = exploreForwardDescF64(data, position);
            reverseF64(data[run_start..run_end]);
        }

        // Only track runs that are "big enough"
        const run_size = run_end - run_start;
        if (run_size >= big_enough or num_runs == 0 or run_end == len) {
            runs[num_runs] = .{ .start = run_start, .end = run_end };
            num_runs += 1;
        } else if (num_runs > 0) {
            // Extend previous run to include this small section
            // (will need to be sorted during merge)
            runs[num_runs - 1].end = run_end;
        }

        position = run_end;
    }

    return num_runs;
}

/// Merge two adjacent sorted runs in-place
/// Uses a temporary buffer for the smaller run
fn mergeTwoRunsF64(data: []f64, temp: []f64, start: usize, middle: usize, end: usize) void {
    if (start >= middle or middle >= end) return;

    // Already sorted - check if last of left <= first of right
    if (data[middle - 1] <= data[middle]) return;

    const left_size = middle - start;
    const right_size = end - middle;

    // Copy smaller side to temp, merge from appropriate direction
    if (left_size <= right_size) {
        // Forward merge - copy left side
        @memcpy(temp[0..left_size], data[start..middle]);

        var i: usize = 0;
        var j: usize = middle;
        var pos: usize = start;

        while (i < left_size and j < end) {
            if (temp[i] <= data[j]) {
                data[pos] = temp[i];
                i += 1;
            } else {
                data[pos] = data[j];
                j += 1;
            }
            pos += 1;
        }

        // Copy remaining from temp (right side already in place)
        while (i < left_size) {
            data[pos] = temp[i];
            i += 1;
            pos += 1;
        }
    } else {
        // Backward merge - copy right side
        @memcpy(temp[0..right_size], data[middle..end]);

        var i: isize = @intCast(right_size - 1);
        var j: isize = @intCast(middle - 1);
        var pos: usize = end - 1;

        while (i >= 0 and j >= @as(isize, @intCast(start))) {
            const ii: usize = @intCast(i);
            const jj: usize = @intCast(j);
            if (temp[ii] >= data[jj]) {
                data[pos] = temp[ii];
                i -= 1;
            } else {
                data[pos] = data[jj];
                j -= 1;
            }
            if (pos > 0) pos -= 1 else break;
        }

        // Copy remaining from temp
        while (i >= 0) {
            const ii: usize = @intCast(i);
            data[pos] = temp[ii];
            i -= 1;
            if (pos > 0) pos -= 1 else break;
        }
    }
}

/// K-way merge of sorted runs using pairwise merging
fn kWayMergeF64(data: []f64, runs: []RunDescriptor, temp: []f64) void {
    var num_runs = runs.len;
    if (num_runs <= 1) return;

    // Pairwise merge until single run
    while (num_runs > 1) {
        var new_runs: usize = 0;
        var i: usize = 0;

        while (i + 1 < num_runs) {
            // Merge runs[i] and runs[i+1]
            mergeTwoRunsF64(data, temp, runs[i].start, runs[i + 1].start, runs[i + 1].end);
            runs[new_runs] = .{ .start = runs[i].start, .end = runs[i + 1].end };
            new_runs += 1;
            i += 2;
        }

        // If odd number of runs, carry last one forward
        if (i < num_runs) {
            runs[new_runs] = runs[i];
            new_runs += 1;
        }

        num_runs = new_runs;
    }
}

// ============================================================================
// Verge Sort for i64
// ============================================================================

/// Explore forward for ascending i64 run
fn exploreForwardAscI64(data: []const i64, start: usize) usize {
    if (start >= data.len - 1) return data.len;

    var i = start;
    const unroll_end = if (data.len >= 4) data.len - 4 else start;
    while (i < unroll_end) {
        const b0 = data[i] <= data[i + 1];
        const b1 = data[i + 1] <= data[i + 2] and b0;
        const b2 = data[i + 2] <= data[i + 3] and b1;
        const b3 = data[i + 3] <= data[i + 4] and b2;

        if (b3) {
            i += 4;
        } else if (b2) {
            return i + 4;
        } else if (b1) {
            return i + 3;
        } else if (b0) {
            return i + 2;
        } else {
            return i + 1;
        }
    }

    while (i < data.len - 1) {
        if (data[i] <= data[i + 1]) {
            i += 1;
        } else {
            return i + 1;
        }
    }
    return i + 1;
}

/// Explore forward for descending i64 run
fn exploreForwardDescI64(data: []const i64, start: usize) usize {
    if (start >= data.len - 1) return data.len;

    var i = start;
    const unroll_end = if (data.len >= 4) data.len - 4 else start;
    while (i < unroll_end) {
        const b0 = data[i] >= data[i + 1];
        const b1 = data[i + 1] >= data[i + 2] and b0;
        const b2 = data[i + 2] >= data[i + 3] and b1;
        const b3 = data[i + 3] >= data[i + 4] and b2;

        if (b3) {
            i += 4;
        } else if (b2) {
            return i + 4;
        } else if (b1) {
            return i + 3;
        } else if (b0) {
            return i + 2;
        } else {
            return i + 1;
        }
    }

    while (i < data.len - 1) {
        if (data[i] >= data[i + 1]) {
            i += 1;
        } else {
            return i + 1;
        }
    }
    return i + 1;
}

/// Reverse a slice of i64 in-place
fn reverseI64(data: []i64) void {
    if (data.len <= 1) return;
    var left: usize = 0;
    var right: usize = data.len - 1;
    while (left < right) {
        const tmp = data[left];
        data[left] = data[right];
        data[right] = tmp;
        left += 1;
        right -= 1;
    }
}

/// Verge Sort preprocessing for i64
fn vergeSortPreprocessingI64(data: []i64, runs: *[MAX_RUNS]RunDescriptor) usize {
    const len = data.len;
    if (len < 2) {
        runs[0] = .{ .start = 0, .end = len };
        return 1;
    }

    const big_enough = computeJumpSize(len);
    var num_runs: usize = 0;
    var position: usize = 0;

    while (position < len) {
        if (num_runs >= MAX_RUNS) return 0;

        if (position >= len - 1) {
            runs[num_runs] = .{ .start = position, .end = len };
            num_runs += 1;
            break;
        }

        const run_start = position;
        var run_end: usize = undefined;

        if (data[position] <= data[position + 1]) {
            run_end = exploreForwardAscI64(data, position);
        } else {
            run_end = exploreForwardDescI64(data, position);
            reverseI64(data[run_start..run_end]);
        }

        const run_size = run_end - run_start;
        if (run_size >= big_enough or num_runs == 0 or run_end == len) {
            runs[num_runs] = .{ .start = run_start, .end = run_end };
            num_runs += 1;
        } else if (num_runs > 0) {
            runs[num_runs - 1].end = run_end;
        }

        position = run_end;
    }

    return num_runs;
}

/// Merge two adjacent sorted i64 runs
fn mergeTwoRunsI64(data: []i64, temp: []i64, start: usize, middle: usize, end: usize) void {
    if (start >= middle or middle >= end) return;
    if (data[middle - 1] <= data[middle]) return;

    const left_size = middle - start;
    const right_size = end - middle;

    if (left_size <= right_size) {
        @memcpy(temp[0..left_size], data[start..middle]);

        var i: usize = 0;
        var j: usize = middle;
        var pos: usize = start;

        while (i < left_size and j < end) {
            if (temp[i] <= data[j]) {
                data[pos] = temp[i];
                i += 1;
            } else {
                data[pos] = data[j];
                j += 1;
            }
            pos += 1;
        }

        while (i < left_size) {
            data[pos] = temp[i];
            i += 1;
            pos += 1;
        }
    } else {
        @memcpy(temp[0..right_size], data[middle..end]);

        var i: isize = @intCast(right_size - 1);
        var j: isize = @intCast(middle - 1);
        var pos: usize = end - 1;

        while (i >= 0 and j >= @as(isize, @intCast(start))) {
            const ii: usize = @intCast(i);
            const jj: usize = @intCast(j);
            if (temp[ii] >= data[jj]) {
                data[pos] = temp[ii];
                i -= 1;
            } else {
                data[pos] = data[jj];
                j -= 1;
            }
            if (pos > 0) pos -= 1 else break;
        }

        while (i >= 0) {
            const ii: usize = @intCast(i);
            data[pos] = temp[ii];
            i -= 1;
            if (pos > 0) pos -= 1 else break;
        }
    }
}

/// K-way merge for i64
fn kWayMergeI64(data: []i64, runs: []RunDescriptor, temp: []i64) void {
    var num_runs = runs.len;
    if (num_runs <= 1) return;

    while (num_runs > 1) {
        var new_runs: usize = 0;
        var i: usize = 0;

        while (i + 1 < num_runs) {
            mergeTwoRunsI64(data, temp, runs[i].start, runs[i + 1].start, runs[i + 1].end);
            runs[new_runs] = .{ .start = runs[i].start, .end = runs[i + 1].end };
            new_runs += 1;
            i += 2;
        }

        if (i < num_runs) {
            runs[new_runs] = runs[i];
            new_runs += 1;
        }

        num_runs = new_runs;
    }
}

// ============================================================================
// Parallel Scatter
// ============================================================================

fn parallelScatter(
    src_keys: []const u64,
    src_indices: []const u32,
    dst_keys: []u64,
    dst_indices: []u32,
    shift: u6,
    global_offsets: *[NUM_BUCKETS]usize,
) void {
    const len = src_keys.len;
    const num_workers = @min(blitz.numWorkers(), 8);
    const chunk_size = (len + num_workers - 1) / num_workers;

    if (num_workers <= 1) {
        var offsets: [NUM_BUCKETS]usize = undefined;
        @memcpy(&offsets, global_offsets);

        for (0..len) |i| {
            const key = src_keys[i];
            const bucket: usize = @intCast((key >> shift) & RADIX_MASK);
            const dst_pos = offsets[bucket];
            offsets[bucket] += 1;
            dst_keys[dst_pos] = key;
            dst_indices[dst_pos] = src_indices[i];
        }
        return;
    }

    // Per-worker histograms
    var worker_counts: [8][NUM_BUCKETS]usize = undefined;
    for (&worker_counts) |*wc| {
        @memset(wc, 0);
    }

    // Phase 1: Parallel histogram
    const HistCtx = struct {
        keys: []const u64,
        worker_counts: *[8][NUM_BUCKETS]usize,
        chunk_size: usize,
        len: usize,
        shift: u6,
    };

    const hist_ctx = HistCtx{
        .keys = src_keys,
        .worker_counts = &worker_counts,
        .chunk_size = chunk_size,
        .len = len,
        .shift = shift,
    };

    blitz.parallelFor(num_workers, HistCtx, hist_ctx, struct {
        fn work(c: HistCtx, start_w: usize, end_w: usize) void {
            for (start_w..end_w) |w| {
                const start = w * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);
                const keys_slice = c.keys[start..end];

                const slice_len = keys_slice.len;
                const unroll_end = slice_len - (slice_len % 8);
                var i: usize = 0;

                while (i < unroll_end) : (i += 8) {
                    if (i + PREFETCH_DISTANCE < slice_len) {
                        @prefetch(@as([*]const u8, @ptrCast(&keys_slice[i + PREFETCH_DISTANCE])), .{});
                    }

                    inline for (0..8) |j| {
                        const b: usize = @intCast((keys_slice[i + j] >> c.shift) & RADIX_MASK);
                        c.worker_counts[w][b] += 1;
                    }
                }

                while (i < slice_len) : (i += 1) {
                    const bucket: usize = @intCast((keys_slice[i] >> c.shift) & RADIX_MASK);
                    c.worker_counts[w][bucket] += 1;
                }
            }
        }
    }.work);

    // Phase 2: Compute per-worker offsets
    var worker_offsets: [8][NUM_BUCKETS]usize = undefined;
    for (0..NUM_BUCKETS) |bucket| {
        var offset = global_offsets[bucket];
        for (0..num_workers) |w| {
            worker_offsets[w][bucket] = offset;
            offset += worker_counts[w][bucket];
        }
    }

    // Phase 3: Parallel scatter
    const ScatterCtx = struct {
        src_keys: []const u64,
        src_indices: []const u32,
        dst_keys: []u64,
        dst_indices: []u32,
        worker_offsets: *[8][NUM_BUCKETS]usize,
        chunk_size: usize,
        len: usize,
        shift: u6,
    };

    const scatter_ctx = ScatterCtx{
        .src_keys = src_keys,
        .src_indices = src_indices,
        .dst_keys = dst_keys,
        .dst_indices = dst_indices,
        .worker_offsets = &worker_offsets,
        .chunk_size = chunk_size,
        .len = len,
        .shift = shift,
    };

    blitz.parallelFor(num_workers, ScatterCtx, scatter_ctx, struct {
        fn work(c: ScatterCtx, start_w: usize, end_w: usize) void {
            for (start_w..end_w) |w| {
                const start = w * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);

                var i = start;
                while (i < end) : (i += 1) {
                    if (i + PREFETCH_DISTANCE < end) {
                        @prefetch(@as([*]const u8, @ptrCast(&c.src_keys[i + PREFETCH_DISTANCE])), .{});
                    }

                    const key = c.src_keys[i];
                    const bucket: usize = @intCast((key >> c.shift) & RADIX_MASK);
                    const dst_pos = c.worker_offsets[w][bucket];
                    c.worker_offsets[w][bucket] += 1;
                    c.dst_keys[dst_pos] = key;
                    c.dst_indices[dst_pos] = c.src_indices[i];
                }
            }
        }
    }.work);
}

/// Parallel scatter for direct sort (keys only, no indices)
fn parallelScatterKeysOnly(
    src_keys: []const u64,
    dst_keys: []u64,
    shift: u6,
    global_offsets: *[NUM_BUCKETS]usize,
) void {
    const len = src_keys.len;
    const num_workers = @min(blitz.numWorkers(), 8);
    const chunk_size = (len + num_workers - 1) / num_workers;

    if (num_workers <= 1) {
        var offsets: [NUM_BUCKETS]usize = undefined;
        @memcpy(&offsets, global_offsets);

        for (0..len) |i| {
            const key = src_keys[i];
            const bucket: usize = @intCast((key >> shift) & RADIX_MASK);
            const dst_pos = offsets[bucket];
            offsets[bucket] += 1;
            dst_keys[dst_pos] = key;
        }
        return;
    }

    var worker_counts: [8][NUM_BUCKETS]usize = undefined;
    for (&worker_counts) |*wc| {
        @memset(wc, 0);
    }

    // Phase 1: Parallel histogram
    const HistCtx = struct {
        keys: []const u64,
        worker_counts: *[8][NUM_BUCKETS]usize,
        chunk_size: usize,
        len: usize,
        shift: u6,
    };

    const hist_ctx = HistCtx{
        .keys = src_keys,
        .worker_counts = &worker_counts,
        .chunk_size = chunk_size,
        .len = len,
        .shift = shift,
    };

    blitz.parallelFor(num_workers, HistCtx, hist_ctx, struct {
        fn work(c: HistCtx, start_w: usize, end_w: usize) void {
            for (start_w..end_w) |w| {
                const start = w * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);

                for (c.keys[start..end]) |key| {
                    const bucket: usize = @intCast((key >> c.shift) & RADIX_MASK);
                    c.worker_counts[w][bucket] += 1;
                }
            }
        }
    }.work);

    // Phase 2: Compute per-worker offsets
    var worker_offsets: [8][NUM_BUCKETS]usize = undefined;
    for (0..NUM_BUCKETS) |bucket| {
        var offset = global_offsets[bucket];
        for (0..num_workers) |w| {
            worker_offsets[w][bucket] = offset;
            offset += worker_counts[w][bucket];
        }
    }

    // Phase 3: Parallel scatter
    const ScatterCtx = struct {
        src_keys: []const u64,
        dst_keys: []u64,
        worker_offsets: *[8][NUM_BUCKETS]usize,
        chunk_size: usize,
        len: usize,
        shift: u6,
    };

    const scatter_ctx = ScatterCtx{
        .src_keys = src_keys,
        .dst_keys = dst_keys,
        .worker_offsets = &worker_offsets,
        .chunk_size = chunk_size,
        .len = len,
        .shift = shift,
    };

    blitz.parallelFor(num_workers, ScatterCtx, scatter_ctx, struct {
        fn work(c: ScatterCtx, start_w: usize, end_w: usize) void {
            for (start_w..end_w) |w| {
                const start = w * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);

                for (start..end) |i| {
                    const key = c.src_keys[i];
                    const bucket: usize = @intCast((key >> c.shift) & RADIX_MASK);
                    const dst_pos = c.worker_offsets[w][bucket];
                    c.worker_offsets[w][bucket] += 1;
                    c.dst_keys[dst_pos] = key;
                }
            }
        }
    }.work);
}

// ============================================================================
// LSD Radix Sort Core
// ============================================================================

/// LSD radix sort with indices (for argsort)
fn lsdRadixSortWithIndices(
    keys: []u64,
    indices: []u32,
    temp_keys: []u64,
    temp_indices: []u32,
) void {
    const len = keys.len;

    if (len <= INSERTION_THRESHOLD) {
        insertionSortKeys(keys, indices);
        return;
    }

    var src_keys = keys;
    var dst_keys = temp_keys;
    var src_indices = indices;
    var dst_indices = temp_indices;

    const num_passes: usize = 8;
    var pass: usize = 0;

    while (pass < num_passes) : (pass += 1) {
        const shift: u6 = @intCast(pass * @as(usize, RADIX_BITS));

        var counts: [NUM_BUCKETS]usize = undefined;
        const hist_result = computeHistogramWithSkip(src_keys, shift, &counts);

        if (hist_result.can_skip) {
            continue;
        }

        var offsets: [NUM_BUCKETS]usize = undefined;
        var running: usize = 0;
        for (0..NUM_BUCKETS) |i| {
            offsets[i] = running;
            running += counts[i];
        }

        if (len >= PARALLEL_THRESHOLD) {
            parallelScatter(src_keys, src_indices, dst_keys, dst_indices, shift, &offsets);
        } else {
            var i: usize = 0;
            while (i < len) : (i += 1) {
                if (i + PREFETCH_DISTANCE < len) {
                    @prefetch(@as([*]const u8, @ptrCast(&src_keys[i + PREFETCH_DISTANCE])), .{});
                }

                const key = src_keys[i];
                const bucket: usize = @intCast((key >> shift) & RADIX_MASK);
                const dst_pos = offsets[bucket];
                offsets[bucket] += 1;
                dst_keys[dst_pos] = key;
                dst_indices[dst_pos] = src_indices[i];
            }
        }

        const tmp_k = src_keys;
        src_keys = dst_keys;
        dst_keys = tmp_k;

        const tmp_i = src_indices;
        src_indices = dst_indices;
        dst_indices = tmp_i;
    }

    if (src_keys.ptr != keys.ptr) {
        @memcpy(keys, src_keys);
        @memcpy(indices, src_indices);
    }
}

/// LSD radix sort without indices (for direct sort)
fn lsdRadixSortKeysOnly(
    keys: []u64,
    temp_keys: []u64,
) void {
    const len = keys.len;

    if (len <= INSERTION_THRESHOLD) {
        insertionSortKeysOnly(keys);
        return;
    }

    var src_keys = keys;
    var dst_keys = temp_keys;

    const num_passes: usize = 8;
    var pass: usize = 0;

    while (pass < num_passes) : (pass += 1) {
        const shift: u6 = @intCast(pass * @as(usize, RADIX_BITS));

        var counts: [NUM_BUCKETS]usize = undefined;
        const hist_result = computeHistogramWithSkip(src_keys, shift, &counts);

        if (hist_result.can_skip) {
            continue;
        }

        var offsets: [NUM_BUCKETS]usize = undefined;
        var running: usize = 0;
        for (0..NUM_BUCKETS) |i| {
            offsets[i] = running;
            running += counts[i];
        }

        if (len >= PARALLEL_THRESHOLD) {
            parallelScatterKeysOnly(src_keys, dst_keys, shift, &offsets);
        } else {
            for (0..len) |i| {
                const key = src_keys[i];
                const bucket: usize = @intCast((key >> shift) & RADIX_MASK);
                const dst_pos = offsets[bucket];
                offsets[bucket] += 1;
                dst_keys[dst_pos] = key;
            }
        }

        const tmp = src_keys;
        src_keys = dst_keys;
        dst_keys = tmp;
    }

    if (src_keys.ptr != keys.ptr) {
        @memcpy(keys, src_keys);
    }
}

// ============================================================================
// Public API: Argsort (returns indices)
// ============================================================================

/// Argsort for f64 - returns indices that would sort the array
pub fn argsortF64(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    if (len <= INSERTION_THRESHOLD) {
        insertionSortIndirectF64(data, out_indices[0..len], ascending);
        return;
    }

    // Check if already sorted (Verge sort optimization)
    if (isSortedF64(data, ascending)) {
        return;
    }

    // Check if reverse sorted
    if (isSortedF64(data, !ascending)) {
        var left: usize = 0;
        var right: usize = len - 1;
        while (left < right) {
            const tmp = out_indices[left];
            out_indices[left] = out_indices[right];
            out_indices[right] = tmp;
            left += 1;
            right -= 1;
        }
        return;
    }

    const allocator = std.heap.c_allocator;

    const keys = allocator.alloc(u64, len) catch {
        insertionSortIndirectF64(data, out_indices[0..len], ascending);
        return;
    };
    defer allocator.free(keys);

    const temp_keys = allocator.alloc(u64, len) catch {
        insertionSortIndirectF64(data, out_indices[0..len], ascending);
        return;
    };
    defer allocator.free(temp_keys);

    const temp_indices = allocator.alloc(u32, len) catch {
        insertionSortIndirectF64(data, out_indices[0..len], ascending);
        return;
    };
    defer allocator.free(temp_indices);

    transformKeysF64(data[0..len], keys);
    lsdRadixSortWithIndices(keys, out_indices[0..len], temp_keys, temp_indices);

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

/// Argsort for i64 - returns indices that would sort the array
pub fn argsortI64(data: []const i64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    if (len <= INSERTION_THRESHOLD) {
        insertionSortIndirectI64(data, out_indices[0..len], ascending);
        return;
    }

    if (isSortedI64(data, ascending)) {
        return;
    }

    if (isSortedI64(data, !ascending)) {
        var left: usize = 0;
        var right: usize = len - 1;
        while (left < right) {
            const tmp = out_indices[left];
            out_indices[left] = out_indices[right];
            out_indices[right] = tmp;
            left += 1;
            right -= 1;
        }
        return;
    }

    const allocator = std.heap.c_allocator;

    const keys = allocator.alloc(u64, len) catch {
        insertionSortIndirectI64(data, out_indices[0..len], ascending);
        return;
    };
    defer allocator.free(keys);

    const temp_keys = allocator.alloc(u64, len) catch {
        insertionSortIndirectI64(data, out_indices[0..len], ascending);
        return;
    };
    defer allocator.free(temp_keys);

    const temp_indices = allocator.alloc(u32, len) catch {
        insertionSortIndirectI64(data, out_indices[0..len], ascending);
        return;
    };
    defer allocator.free(temp_indices);

    transformKeysI64(data[0..len], keys);
    lsdRadixSortWithIndices(keys, out_indices[0..len], temp_keys, temp_indices);

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

// ============================================================================
// Public API: Direct Sort (returns sorted values)
// ============================================================================

/// Sort f64 values directly (faster than argsort, no index tracking)
/// Uses Verge Sort preprocessing to exploit existing order, then radix sort for random sections
pub fn sortF64(data: []const f64, out: []f64, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    // Check if already sorted
    if (isSortedF64(data, ascending)) {
        @memcpy(out[0..len], data[0..len]);
        return;
    }

    // Check if reverse sorted
    if (isSortedF64(data, !ascending)) {
        var i: usize = 0;
        while (i < len) : (i += 1) {
            out[i] = data[len - 1 - i];
        }
        return;
    }

    // For small arrays, use std sort (pdqsort-like)
    // Based on voracious_sort: pdqsort wins at ≤300 elements
    const SMALL_SORT_THRESHOLD: usize = 300;
    if (len <= SMALL_SORT_THRESHOLD) {
        @memcpy(out[0..len], data[0..len]);
        if (ascending) {
            std.mem.sort(f64, out[0..len], {}, std.sort.asc(f64));
        } else {
            std.mem.sort(f64, out[0..len], {}, std.sort.desc(f64));
        }
        return;
    }

    // Copy data to output buffer for in-place operations
    @memcpy(out[0..len], data[0..len]);

    const allocator = std.heap.c_allocator;

    // Try Verge Sort preprocessing: detect sorted runs
    var runs: [MAX_RUNS]RunDescriptor = undefined;
    const num_runs = vergeSortPreprocessingF64(out[0..len], &runs);

    // If single run, data is now sorted (ascending)
    if (num_runs == 1) {
        if (!ascending) {
            reverseF64(out[0..len]);
        }
        return;
    }

    // If preprocessing found runs (not too many), use k-way merge
    if (num_runs > 1 and num_runs <= MAX_RUNS) {
        // Allocate merge buffer (half the size since we always merge smaller into temp)
        const merge_buffer = allocator.alloc(f64, len / 2 + 1) catch {
            // Fallback to std sort
            if (ascending) {
                std.mem.sort(f64, out[0..len], {}, std.sort.asc(f64));
            } else {
                std.mem.sort(f64, out[0..len], {}, std.sort.desc(f64));
            }
            return;
        };
        defer allocator.free(merge_buffer);

        kWayMergeF64(out[0..len], runs[0..num_runs], merge_buffer);

        if (!ascending) {
            reverseF64(out[0..len]);
        }
        return;
    }

    // Fallback to radix sort for truly random data (many small runs)
    const out_as_u64: []u64 = @as([*]u64, @ptrCast(out.ptr))[0..len];

    const temp_keys = allocator.alloc(u64, len) catch {
        // Fallback: use std sort
        if (ascending) {
            std.mem.sort(f64, out[0..len], {}, std.sort.asc(f64));
        } else {
            std.mem.sort(f64, out[0..len], {}, std.sort.desc(f64));
        }
        return;
    };
    defer allocator.free(temp_keys);

    // Transform to sortable keys (data already in out buffer)
    for (0..len) |i| {
        out_as_u64[i] = floatToSortable(out[i]);
    }

    // Sort keys
    lsdRadixSortKeysOnly(out_as_u64, temp_keys);

    // Convert back to f64
    if (ascending) {
        transformKeysToF64InPlace(out_as_u64);
    } else {
        reverseU64(out_as_u64);
        transformKeysToF64InPlace(out_as_u64);
    }
}

/// In-place transformation from sortable keys to f64
fn transformKeysToF64InPlace(keys: []u64) void {
    const len = keys.len;
    const simd_end = len - (len % SIMD_WIDTH);

    const sign_bit_vec: U64Vec = @splat(@as(u64, 1) << 63);

    var i: usize = 0;
    while (i < simd_end) : (i += SIMD_WIDTH) {
        const bits: U64Vec = keys[i..][0..SIMD_WIDTH].*;
        const sign_bits = bits >> @splat(@as(u6, 63));
        const neg_mask = @as(U64Vec, @splat(0)) -% (@as(U64Vec, @splat(1)) - sign_bits);
        const mask = neg_mask | sign_bit_vec;
        const original_bits = bits ^ mask;
        keys[i..][0..SIMD_WIDTH].* = original_bits;
    }

    while (i < len) : (i += 1) {
        keys[i] = @bitCast(sortableToF64(keys[i]));
    }
}

/// Reverse a u64 array in-place
fn reverseU64(arr: []u64) void {
    if (arr.len <= 1) return;
    var left: usize = 0;
    var right: usize = arr.len - 1;
    while (left < right) {
        const tmp = arr[left];
        arr[left] = arr[right];
        arr[right] = tmp;
        left += 1;
        right -= 1;
    }
}

/// Sort i64 values directly (faster than argsort, no index tracking)
/// Uses Verge Sort preprocessing to exploit existing order, then radix sort for random sections
pub fn sortI64(data: []const i64, out: []i64, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    if (isSortedI64(data, ascending)) {
        @memcpy(out[0..len], data[0..len]);
        return;
    }

    if (isSortedI64(data, !ascending)) {
        var i: usize = 0;
        while (i < len) : (i += 1) {
            out[i] = data[len - 1 - i];
        }
        return;
    }

    // For small arrays, use std sort (pdqsort-like)
    const SMALL_SORT_THRESHOLD: usize = 300;
    if (len <= SMALL_SORT_THRESHOLD) {
        @memcpy(out[0..len], data[0..len]);
        if (ascending) {
            std.mem.sort(i64, out[0..len], {}, std.sort.asc(i64));
        } else {
            std.mem.sort(i64, out[0..len], {}, std.sort.desc(i64));
        }
        return;
    }

    // Copy data to output buffer for in-place operations
    @memcpy(out[0..len], data[0..len]);

    const allocator = std.heap.c_allocator;

    // Try Verge Sort preprocessing: detect sorted runs
    var runs: [MAX_RUNS]RunDescriptor = undefined;
    const num_runs = vergeSortPreprocessingI64(out[0..len], &runs);

    // If single run, data is now sorted (ascending)
    if (num_runs == 1) {
        if (!ascending) {
            reverseI64(out[0..len]);
        }
        return;
    }

    // If preprocessing found runs (not too many), use k-way merge
    if (num_runs > 1 and num_runs <= MAX_RUNS) {
        const merge_buffer = allocator.alloc(i64, len / 2 + 1) catch {
            if (ascending) {
                std.mem.sort(i64, out[0..len], {}, std.sort.asc(i64));
            } else {
                std.mem.sort(i64, out[0..len], {}, std.sort.desc(i64));
            }
            return;
        };
        defer allocator.free(merge_buffer);

        kWayMergeI64(out[0..len], runs[0..num_runs], merge_buffer);

        if (!ascending) {
            reverseI64(out[0..len]);
        }
        return;
    }

    // Fallback to radix sort for truly random data (many small runs)
    const out_as_u64: []u64 = @as([*]u64, @ptrCast(out.ptr))[0..len];

    const temp_keys = allocator.alloc(u64, len) catch {
        if (ascending) {
            std.mem.sort(i64, out[0..len], {}, std.sort.asc(i64));
        } else {
            std.mem.sort(i64, out[0..len], {}, std.sort.desc(i64));
        }
        return;
    };
    defer allocator.free(temp_keys);

    // Transform to sortable keys (data already in out buffer)
    for (0..len) |i| {
        out_as_u64[i] = i64ToSortable(out[i]);
    }

    // Sort keys
    lsdRadixSortKeysOnly(out_as_u64, temp_keys);

    // Convert back to i64
    if (ascending) {
        transformKeysToI64InPlace(out_as_u64);
    } else {
        reverseU64(out_as_u64);
        transformKeysToI64InPlace(out_as_u64);
    }
}

/// In-place transformation from sortable keys to i64
fn transformKeysToI64InPlace(keys: []u64) void {
    const len = keys.len;
    const simd_end = len - (len % SIMD_WIDTH);

    const sign_bit_vec: U64Vec = @splat(@as(u64, 1) << 63);

    var i: usize = 0;
    while (i < simd_end) : (i += SIMD_WIDTH) {
        const bits: U64Vec = keys[i..][0..SIMD_WIDTH].*;
        keys[i..][0..SIMD_WIDTH].* = bits ^ sign_bit_vec;
    }

    while (i < len) : (i += 1) {
        keys[i] = @bitCast(sortableToI64(keys[i]));
    }
}

// ============================================================================
// Public API: Gather (for reordering by indices)
// ============================================================================

/// Parallel gather for f64
pub fn gatherF64(src: []const f64, indices: []const u32, dst: []f64) void {
    const len = @min(indices.len, dst.len);
    if (len == 0) return;

    if (len < PARALLEL_THRESHOLD) {
        for (0..len) |i| {
            dst[i] = src[indices[i]];
        }
        return;
    }

    const num_workers = @min(blitz.numWorkers(), 8);
    const chunk_size = (len + num_workers - 1) / num_workers;

    const Ctx = struct {
        src: []const f64,
        indices: []const u32,
        dst: []f64,
        chunk_size: usize,
        len: usize,
    };

    const ctx = Ctx{
        .src = src,
        .indices = indices,
        .dst = dst,
        .chunk_size = chunk_size,
        .len = len,
    };

    blitz.parallelFor(num_workers, Ctx, ctx, struct {
        fn work(c: Ctx, start_w: usize, end_w: usize) void {
            for (start_w..end_w) |w| {
                const start = w * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);

                for (start..end) |i| {
                    c.dst[i] = c.src[c.indices[i]];
                }
            }
        }
    }.work);
}

/// Parallel gather for i64
pub fn gatherI64(src: []const i64, indices: []const u32, dst: []i64) void {
    const len = @min(indices.len, dst.len);
    if (len == 0) return;

    if (len < PARALLEL_THRESHOLD) {
        for (0..len) |i| {
            dst[i] = src[indices[i]];
        }
        return;
    }

    const num_workers = @min(blitz.numWorkers(), 8);
    const chunk_size = (len + num_workers - 1) / num_workers;

    const Ctx = struct {
        src: []const i64,
        indices: []const u32,
        dst: []i64,
        chunk_size: usize,
        len: usize,
    };

    const ctx = Ctx{
        .src = src,
        .indices = indices,
        .dst = dst,
        .chunk_size = chunk_size,
        .len = len,
    };

    blitz.parallelFor(num_workers, Ctx, ctx, struct {
        fn work(c: Ctx, start_w: usize, end_w: usize) void {
            for (start_w..end_w) |w| {
                const start = w * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);

                for (start..end) |i| {
                    c.dst[i] = c.src[c.indices[i]];
                }
            }
        }
    }.work);
}

// ============================================================================
// Public API: 32-bit Argsort (using std.sort, for i32/f32 types)
// ============================================================================

/// Argsort for i32 values using comparison-based sort
pub fn argsortI32(data: []const i32, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // Initialize indices
    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    // Sort indices by data values
    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const i32, a: u32, b: u32) bool {
                return d[a] < d[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const i32, a: u32, b: u32) bool {
                return d[a] > d[b];
            }
        }.lessThan);
    }
}

/// Argsort for f32 values using comparison-based sort
pub fn argsortF32(data: []const f32, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // Initialize indices
    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    // Sort indices by data values
    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const f32, a: u32, b: u32) bool {
                return d[a] < d[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const f32, a: u32, b: u32) bool {
                return d[a] > d[b];
            }
        }.lessThan);
    }
}

/// Sort i32 values directly
pub fn sortI32(data: []const i32, out: []i32, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    @memcpy(out[0..len], data[0..len]);
    if (ascending) {
        std.mem.sort(i32, out[0..len], {}, std.sort.asc(i32));
    } else {
        std.mem.sort(i32, out[0..len], {}, std.sort.desc(i32));
    }
}

/// Sort f32 values directly
pub fn sortF32(data: []const f32, out: []f32, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    @memcpy(out[0..len], data[0..len]);
    if (ascending) {
        std.mem.sort(f32, out[0..len], {}, std.sort.asc(f32));
    } else {
        std.mem.sort(f32, out[0..len], {}, std.sort.desc(f32));
    }
}

/// Argsort for u64 values using comparison-based sort
pub fn argsortU64(data: []const u64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const u64, a: u32, b: u32) bool {
                return d[a] < d[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const u64, a: u32, b: u32) bool {
                return d[a] > d[b];
            }
        }.lessThan);
    }
}

/// Argsort for u32 values using comparison-based sort
pub fn argsortU32(data: []const u32, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const u32, a: u32, b: u32) bool {
                return d[a] < d[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(d: []const u32, a: u32, b: u32) bool {
                return d[a] > d[b];
            }
        }.lessThan);
    }
}

// ============================================================================
// Utility: Check if sorted (used by joins)
// ============================================================================

/// Check if i64 array is sorted ascending
pub fn isSortedI64Keys(keys: []const i64) bool {
    return isSortedI64(keys, true);
}

// ============================================================================
// Tests
// ============================================================================

test "argsortF64 basic" {
    var data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0, 0.0 };
    var indices: [10]u32 = undefined;

    argsortF64(&data, &indices, true);

    for (0..indices.len - 1) |i| {
        try std.testing.expect(data[indices[i]] <= data[indices[i + 1]]);
    }
}

test "argsortF64 already sorted" {
    var data = [_]f64{ 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0 };
    var indices: [10]u32 = undefined;

    argsortF64(&data, &indices, true);

    for (indices, 0..) |idx, i| {
        try std.testing.expectEqual(@as(u32, @intCast(i)), idx);
    }
}

test "argsortI64 basic" {
    var data = [_]i64{ 5, -2, 8, -1, 9, 3, -7, 4, 6, 0 };
    var indices: [10]u32 = undefined;

    argsortI64(&data, &indices, true);

    for (0..indices.len - 1) |i| {
        try std.testing.expect(data[indices[i]] <= data[indices[i + 1]]);
    }
}

test "sortF64 basic" {
    var data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };
    var out: [5]f64 = undefined;

    sortF64(&data, &out, true);

    try std.testing.expectEqual(@as(f64, 1.0), out[0]);
    try std.testing.expectEqual(@as(f64, 2.0), out[1]);
    try std.testing.expectEqual(@as(f64, 5.0), out[2]);
    try std.testing.expectEqual(@as(f64, 8.0), out[3]);
    try std.testing.expectEqual(@as(f64, 9.0), out[4]);
}

test "sortI64 basic" {
    var data = [_]i64{ 5, -2, 8, -1, 9 };
    var out: [5]i64 = undefined;

    sortI64(&data, &out, true);

    try std.testing.expectEqual(@as(i64, -2), out[0]);
    try std.testing.expectEqual(@as(i64, -1), out[1]);
    try std.testing.expectEqual(@as(i64, 5), out[2]);
    try std.testing.expectEqual(@as(i64, 8), out[3]);
    try std.testing.expectEqual(@as(i64, 9), out[4]);
}

test "floatToSortable preserves order" {
    const values = [_]f64{ -std.math.inf(f64), -100.0, -1.0, -0.0, 0.0, 1.0, 100.0, std.math.inf(f64) };

    for (0..values.len - 1) |i| {
        const a = floatToSortable(values[i]);
        const b = floatToSortable(values[i + 1]);
        try std.testing.expect(a < b);
    }
}

test "i64ToSortable preserves order" {
    const values = [_]i64{ std.math.minInt(i64), -100, -1, 0, 1, 100, std.math.maxInt(i64) };

    for (0..values.len - 1) |i| {
        const a = i64ToSortable(values[i]);
        const b = i64ToSortable(values[i + 1]);
        try std.testing.expect(a < b);
    }
}
