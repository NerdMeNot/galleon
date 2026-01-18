//! High-Performance Parallel Sorting for Galleon
//!
//! Uses parallel quicksort like Polars (via Rayon's par_sort_unstable_by).
//! Key techniques from Rayon/Polars:
//! 1. BlockQuicksort - branchless block-based partitioning (128 elements)
//! 2. Algorithmic threshold based on problem size, not hard-coded
//! 3. Pattern-defeating: heapsort fallback after too many bad pivots
//! 4. Median-of-medians pivot selection for large slices

const std = @import("std");
const core = @import("core.zig");
const blitz = @import("../blitz.zig");

// ============================================================================
// Configuration - Algorithmic Thresholds
// ============================================================================

/// Block size for BlockQuicksort partitioning
const BLOCK_SIZE: usize = 128;

/// Insertion sort threshold - very small arrays
const INSERTION_THRESHOLD: usize = 24;

/// Minimum elements before considering parallel execution.
/// Derived from: parallel overhead ~1µs, sequential sort ~10ns/element
fn parallelThreshold(comptime T: type) usize {
    const elem_size = @sizeOf(T);
    if (elem_size <= 8) return 2000;
    if (elem_size <= 64) return 1000;
    return 500;
}

/// Max bad pivots before heapsort fallback = floor(log2(n)) + 1
fn maxBadPivots(len: usize) u32 {
    if (len == 0) return 0;
    return @as(u32, @intCast(@bitSizeOf(usize) - @clz(len)));
}

// ============================================================================
// Key Conversion Utilities
// ============================================================================

/// Convert f64 to sortable u64 representation
pub inline fn floatToSortable(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    const sign_bit = bits >> 63;
    const mask: u64 = (0 -% sign_bit) | (@as(u64, 1) << 63);
    return bits ^ mask;
}

/// Convert sortable u64 back to f64
pub inline fn sortableToF64(bits: u64) f64 {
    const sign_bit = bits >> 63;
    const mask: u64 = (0 -% (1 - sign_bit)) | (@as(u64, 1) << 63);
    return @bitCast(bits ^ mask);
}

/// Convert i64 to sortable u64 representation
pub inline fn i64ToSortable(val: i64) u64 {
    const bits: u64 = @bitCast(val);
    return bits ^ (@as(u64, 1) << 63);
}

/// Convert sortable u64 back to i64
pub inline fn sortableToI64(bits: u64) i64 {
    return @bitCast(bits ^ (@as(u64, 1) << 63));
}

// ============================================================================
// Sorted Check Utilities
// ============================================================================

/// Check if i64 keys are sorted (ascending)
pub fn isSortedI64Keys(keys: []const u64) bool {
    if (keys.len <= 1) return true;
    for (0..keys.len - 1) |i| {
        if (keys[i] > keys[i + 1]) return false;
    }
    return true;
}

// ============================================================================
// BlockQuicksort Partitioning (branchless, cache-friendly)
// ============================================================================

/// Partition using BlockQuicksort algorithm for f64
/// Returns the number of elements less than pivot
fn blockPartitionF64(data: []f64, pivot: f64, comptime ascending: bool) usize {
    if (data.len == 0) return 0;

    var l: usize = 0;
    var r: usize = data.len;

    // Offsets arrays for left and right blocks
    var offsets_l: [BLOCK_SIZE]u8 = undefined;
    var offsets_r: [BLOCK_SIZE]u8 = undefined;

    var num_l: usize = 0;
    var num_r: usize = 0;
    var start_l: usize = 0;
    var start_r: usize = 0;

    while (r - l > 2 * BLOCK_SIZE) {
        // Refill left block offsets if empty
        if (num_l == 0) {
            start_l = 0;
            var i: u8 = 0;
            while (i < BLOCK_SIZE) : (i += 1) {
                // Branchless: store offset if element >= pivot
                const cmp = if (ascending) !(data[l + i] < pivot) else !(data[l + i] > pivot);
                offsets_l[num_l] = i;
                num_l += @intFromBool(cmp);
            }
        }

        // Refill right block offsets if empty
        if (num_r == 0) {
            start_r = 0;
            var i: u8 = 0;
            while (i < BLOCK_SIZE) : (i += 1) {
                const idx = r - 1 - i;
                const cmp = if (ascending) data[idx] < pivot else data[idx] > pivot;
                offsets_r[num_r] = i;
                num_r += @intFromBool(cmp);
            }
        }

        // Swap elements between blocks
        const num_swaps = @min(num_l - start_l, num_r - start_r);
        for (0..num_swaps) |_| {
            const left_idx = l + offsets_l[start_l];
            const right_idx = r - 1 - offsets_r[start_r];
            std.mem.swap(f64, &data[left_idx], &data[right_idx]);
            start_l += 1;
            start_r += 1;
        }

        // Advance pointers if block is exhausted
        if (start_l == num_l) {
            l += BLOCK_SIZE;
            num_l = 0;
        }
        if (start_r == num_r) {
            r -= BLOCK_SIZE;
            num_r = 0;
        }
    }

    // Handle remaining elements with simple partition
    return simplePartitionF64(data[l..r], pivot, ascending) + l;
}

/// Simple Hoare partition for small remainders
fn simplePartitionF64(data: []f64, pivot: f64, comptime ascending: bool) usize {
    if (data.len == 0) return 0;

    var i: usize = 0;
    var j: usize = data.len - 1;

    while (true) {
        while (i < data.len and (if (ascending) data[i] < pivot else data[i] > pivot)) {
            i += 1;
        }
        while (j > 0 and (if (ascending) !(data[j] < pivot) else !(data[j] > pivot))) {
            j -= 1;
        }
        if (i >= j) return i;
        std.mem.swap(f64, &data[i], &data[j]);
        i += 1;
        if (j > 0) j -= 1;
    }
}

/// BlockQuicksort partition for i64
fn blockPartitionI64(data: []i64, pivot: i64, comptime ascending: bool) usize {
    if (data.len == 0) return 0;

    var l: usize = 0;
    var r: usize = data.len;

    var offsets_l: [BLOCK_SIZE]u8 = undefined;
    var offsets_r: [BLOCK_SIZE]u8 = undefined;

    var num_l: usize = 0;
    var num_r: usize = 0;
    var start_l: usize = 0;
    var start_r: usize = 0;

    while (r - l > 2 * BLOCK_SIZE) {
        if (num_l == 0) {
            start_l = 0;
            var i: u8 = 0;
            while (i < BLOCK_SIZE) : (i += 1) {
                const cmp = if (ascending) !(data[l + i] < pivot) else !(data[l + i] > pivot);
                offsets_l[num_l] = i;
                num_l += @intFromBool(cmp);
            }
        }

        if (num_r == 0) {
            start_r = 0;
            var i: u8 = 0;
            while (i < BLOCK_SIZE) : (i += 1) {
                const idx = r - 1 - i;
                const cmp = if (ascending) data[idx] < pivot else data[idx] > pivot;
                offsets_r[num_r] = i;
                num_r += @intFromBool(cmp);
            }
        }

        const num_swaps = @min(num_l - start_l, num_r - start_r);
        for (0..num_swaps) |_| {
            const left_idx = l + offsets_l[start_l];
            const right_idx = r - 1 - offsets_r[start_r];
            std.mem.swap(i64, &data[left_idx], &data[right_idx]);
            start_l += 1;
            start_r += 1;
        }

        if (start_l == num_l) {
            l += BLOCK_SIZE;
            num_l = 0;
        }
        if (start_r == num_r) {
            r -= BLOCK_SIZE;
            num_r = 0;
        }
    }

    return simplePartitionI64(data[l..r], pivot, ascending) + l;
}

fn simplePartitionI64(data: []i64, pivot: i64, comptime ascending: bool) usize {
    if (data.len == 0) return 0;

    var i: usize = 0;
    var j: usize = data.len - 1;

    while (true) {
        while (i < data.len and (if (ascending) data[i] < pivot else data[i] > pivot)) {
            i += 1;
        }
        while (j > 0 and (if (ascending) !(data[j] < pivot) else !(data[j] > pivot))) {
            j -= 1;
        }
        if (i >= j) return i;
        std.mem.swap(i64, &data[i], &data[j]);
        i += 1;
        if (j > 0) j -= 1;
    }
}

// ============================================================================
// Pivot Selection - Median of Medians for large slices
// ============================================================================

fn choosePivotF64(data: []f64, comptime ascending: bool) usize {
    const len = data.len;
    if (len < 8) return len / 2;

    var a = len / 4;
    var b = len / 2;
    var c = len / 4 * 3;

    // For large slices, use median of medians
    if (len >= 50) {
        a = medianOfThreeF64(data, a - 1, a, a + 1, ascending);
        b = medianOfThreeF64(data, b - 1, b, b + 1, ascending);
        c = medianOfThreeF64(data, c - 1, c, c + 1, ascending);
    }

    return medianOfThreeF64(data, a, b, c, ascending);
}

fn medianOfThreeF64(data: []f64, i: usize, j: usize, k: usize, comptime ascending: bool) usize {
    const a = data[i];
    const b = data[j];
    const c = data[k];

    if (ascending) {
        if (a < b) {
            if (b < c) return j;
            if (a < c) return k;
            return i;
        } else {
            if (a < c) return i;
            if (b < c) return k;
            return j;
        }
    } else {
        if (a > b) {
            if (b > c) return j;
            if (a > c) return k;
            return i;
        } else {
            if (a > c) return i;
            if (b > c) return k;
            return j;
        }
    }
}

fn choosePivotI64(data: []i64, comptime ascending: bool) usize {
    const len = data.len;
    if (len < 8) return len / 2;

    var a = len / 4;
    var b = len / 2;
    var c = len / 4 * 3;

    if (len >= 50) {
        a = medianOfThreeI64(data, a - 1, a, a + 1, ascending);
        b = medianOfThreeI64(data, b - 1, b, b + 1, ascending);
        c = medianOfThreeI64(data, c - 1, c, c + 1, ascending);
    }

    return medianOfThreeI64(data, a, b, c, ascending);
}

fn medianOfThreeI64(data: []i64, i: usize, j: usize, k: usize, comptime ascending: bool) usize {
    const a = data[i];
    const b = data[j];
    const c = data[k];

    if (ascending) {
        if (a < b) {
            if (b < c) return j;
            if (a < c) return k;
            return i;
        } else {
            if (a < c) return i;
            if (b < c) return k;
            return j;
        }
    } else {
        if (a > b) {
            if (b > c) return j;
            if (a > c) return k;
            return i;
        } else {
            if (a > c) return i;
            if (b > c) return k;
            return j;
        }
    }
}

// ============================================================================
// Heapsort Fallback (O(n log n) guaranteed)
// ============================================================================

fn heapsortF64(data: []f64, comptime ascending: bool) void {
    if (data.len < 2) return;

    // Build heap
    var i = data.len / 2;
    while (i > 0) {
        i -= 1;
        siftDownF64(data, i, data.len, ascending);
    }

    // Extract elements
    var end = data.len;
    while (end > 1) {
        end -= 1;
        std.mem.swap(f64, &data[0], &data[end]);
        siftDownF64(data, 0, end, ascending);
    }
}

fn siftDownF64(data: []f64, start: usize, end: usize, comptime ascending: bool) void {
    var root = start;
    while (2 * root + 1 < end) {
        var child = 2 * root + 1;

        if (child + 1 < end) {
            const should_swap = if (ascending) data[child] < data[child + 1] else data[child] > data[child + 1];
            if (should_swap) child += 1;
        }

        const root_should_move = if (ascending) data[root] < data[child] else data[root] > data[child];

        if (root_should_move) {
            std.mem.swap(f64, &data[root], &data[child]);
            root = child;
        } else {
            return;
        }
    }
}

fn heapsortI64(data: []i64, comptime ascending: bool) void {
    if (data.len < 2) return;

    var i = data.len / 2;
    while (i > 0) {
        i -= 1;
        siftDownI64(data, i, data.len, ascending);
    }

    var end = data.len;
    while (end > 1) {
        end -= 1;
        std.mem.swap(i64, &data[0], &data[end]);
        siftDownI64(data, 0, end, ascending);
    }
}

fn siftDownI64(data: []i64, start: usize, end: usize, comptime ascending: bool) void {
    var root = start;
    while (2 * root + 1 < end) {
        var child = 2 * root + 1;

        if (child + 1 < end) {
            const should_swap = if (ascending) data[child] < data[child + 1] else data[child] > data[child + 1];
            if (should_swap) child += 1;
        }

        const root_should_move = if (ascending) data[root] < data[child] else data[root] > data[child];

        if (root_should_move) {
            std.mem.swap(i64, &data[root], &data[child]);
            root = child;
        } else {
            return;
        }
    }
}

// ============================================================================
// Insertion Sort for small arrays
// ============================================================================

fn insertionSortF64(data: []f64, comptime ascending: bool) void {
    if (data.len < 2) return;

    for (1..data.len) |i| {
        const key = data[i];
        var j = i;
        while (j > 0 and (if (ascending) key < data[j - 1] else key > data[j - 1])) {
            data[j] = data[j - 1];
            j -= 1;
        }
        data[j] = key;
    }
}

fn insertionSortI64(data: []i64, comptime ascending: bool) void {
    if (data.len < 2) return;

    for (1..data.len) |i| {
        const key = data[i];
        var j = i;
        while (j > 0 and (if (ascending) key < data[j - 1] else key > data[j - 1])) {
            data[j] = data[j - 1];
            j -= 1;
        }
        data[j] = key;
    }
}

// ============================================================================
// Parallel Quicksort Implementation
// ============================================================================

/// Recursive parallel quicksort for f64
fn parallelQuicksortF64(data: []f64, comptime ascending: bool, limit: u32) void {
    const threshold = parallelThreshold(f64);

    // Base case: insertion sort for very small arrays
    if (data.len <= INSERTION_THRESHOLD) {
        insertionSortF64(data, ascending);
        return;
    }

    // Sequential threshold
    if (data.len <= threshold) {
        if (ascending) {
            std.sort.pdq(f64, data, {}, std.sort.asc(f64));
        } else {
            std.sort.pdq(f64, data, {}, std.sort.desc(f64));
        }
        return;
    }

    // Fallback to heapsort if too many bad pivots
    if (limit == 0) {
        heapsortF64(data, ascending);
        return;
    }

    // Choose pivot using median-of-medians
    const pivot_idx = choosePivotF64(data, ascending);
    const pivot = data[pivot_idx];

    // Move pivot to start temporarily
    std.mem.swap(f64, &data[0], &data[pivot_idx]);

    // Partition using BlockQuicksort
    const mid = blockPartitionF64(data[1..], pivot, ascending) + 1;

    // Move pivot to its final position
    std.mem.swap(f64, &data[0], &data[mid - 1]);

    const left = data[0 .. mid - 1];
    const right = data[mid..];

    // Check if partition was balanced
    const was_balanced = @min(left.len, right.len) >= data.len / 8;
    const new_limit = if (was_balanced) limit else limit - 1;

    // Sort both partitions in parallel using blitz
    const LeftArg = struct { slice: []f64, lim: u32 };
    const RightArg = struct { slice: []f64, lim: u32 };

    blitz.joinVoid(
        struct {
            fn sortLeft(arg: LeftArg) void {
                parallelQuicksortF64(arg.slice, ascending, arg.lim);
            }
        }.sortLeft,
        struct {
            fn sortRight(arg: RightArg) void {
                parallelQuicksortF64(arg.slice, ascending, arg.lim);
            }
        }.sortRight,
        LeftArg{ .slice = left, .lim = new_limit },
        RightArg{ .slice = right, .lim = new_limit },
    );
}

/// Recursive parallel quicksort for i64
fn parallelQuicksortI64(data: []i64, comptime ascending: bool, limit: u32) void {
    const threshold = parallelThreshold(i64);

    if (data.len <= INSERTION_THRESHOLD) {
        insertionSortI64(data, ascending);
        return;
    }

    if (data.len <= threshold) {
        if (ascending) {
            std.sort.pdq(i64, data, {}, std.sort.asc(i64));
        } else {
            std.sort.pdq(i64, data, {}, std.sort.desc(i64));
        }
        return;
    }

    if (limit == 0) {
        heapsortI64(data, ascending);
        return;
    }

    const pivot_idx = choosePivotI64(data, ascending);
    const pivot = data[pivot_idx];

    std.mem.swap(i64, &data[0], &data[pivot_idx]);

    const mid = blockPartitionI64(data[1..], pivot, ascending) + 1;

    std.mem.swap(i64, &data[0], &data[mid - 1]);

    const left = data[0 .. mid - 1];
    const right = data[mid..];

    const was_balanced = @min(left.len, right.len) >= data.len / 8;
    const new_limit = if (was_balanced) limit else limit - 1;

    const LeftArg = struct { slice: []i64, lim: u32 };
    const RightArg = struct { slice: []i64, lim: u32 };

    blitz.joinVoid(
        struct {
            fn sortLeft(arg: LeftArg) void {
                parallelQuicksortI64(arg.slice, ascending, arg.lim);
            }
        }.sortLeft,
        struct {
            fn sortRight(arg: RightArg) void {
                parallelQuicksortI64(arg.slice, ascending, arg.lim);
            }
        }.sortRight,
        LeftArg{ .slice = left, .lim = new_limit },
        RightArg{ .slice = right, .lim = new_limit },
    );
}

// ============================================================================
// Public Sort Functions
// ============================================================================

/// Sort f64 values using parallel quicksort (same approach as Polars)
pub fn sortF64(data: []const f64, out: []f64, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    @memcpy(out[0..len], data[0..len]);

    const limit = maxBadPivots(len);
    if (ascending) {
        parallelQuicksortF64(out[0..len], true, limit);
    } else {
        parallelQuicksortF64(out[0..len], false, limit);
    }
}

/// Sort i64 values using parallel quicksort (same approach as Polars)
pub fn sortI64(data: []const i64, out: []i64, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    @memcpy(out[0..len], data[0..len]);

    const limit = maxBadPivots(len);
    if (ascending) {
        parallelQuicksortI64(out[0..len], true, limit);
    } else {
        parallelQuicksortI64(out[0..len], false, limit);
    }
}

/// Sort i32 values using pdqsort
pub fn sortI32(data: []const i32, out: []i32, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    @memcpy(out[0..len], data[0..len]);
    if (ascending) {
        std.sort.pdq(i32, out[0..len], {}, std.sort.asc(i32));
    } else {
        std.sort.pdq(i32, out[0..len], {}, std.sort.desc(i32));
    }
}

/// Sort f32 values using pdqsort
pub fn sortF32(data: []const f32, out: []f32, ascending: bool) void {
    const len = @min(data.len, out.len);
    if (len == 0) return;

    @memcpy(out[0..len], data[0..len]);
    if (ascending) {
        std.sort.pdq(f32, out[0..len], {}, std.sort.asc(f32));
    } else {
        std.sort.pdq(f32, out[0..len], {}, std.sort.desc(f32));
    }
}

// ============================================================================
// Argsort Functions (Return Indices)
// ============================================================================

/// Argsort f64 - returns indices that would sort the array
pub fn argsortF64(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    const Context = struct {
        data: []const f64,
        ascending: bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            if (ctx.ascending) {
                return ctx.data[a] < ctx.data[b];
            } else {
                return ctx.data[a] > ctx.data[b];
            }
        }
    };

    const ctx = Context{ .data = data, .ascending = ascending };
    std.sort.pdq(u32, out_indices[0..len], ctx, Context.lessThan);
}

/// Argsort i64 - returns indices that would sort the array
pub fn argsortI64(data: []const i64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    const Context = struct {
        data: []const i64,
        ascending: bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            if (ctx.ascending) {
                return ctx.data[a] < ctx.data[b];
            } else {
                return ctx.data[a] > ctx.data[b];
            }
        }
    };

    const ctx = Context{ .data = data, .ascending = ascending };
    std.sort.pdq(u32, out_indices[0..len], ctx, Context.lessThan);
}

/// Argsort i32 - returns indices that would sort the array
pub fn argsortI32(data: []const i32, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    const Context = struct {
        data: []const i32,
        ascending: bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            if (ctx.ascending) {
                return ctx.data[a] < ctx.data[b];
            } else {
                return ctx.data[a] > ctx.data[b];
            }
        }
    };

    const ctx = Context{ .data = data, .ascending = ascending };
    std.sort.pdq(u32, out_indices[0..len], ctx, Context.lessThan);
}

/// Argsort f32 - returns indices that would sort the array
pub fn argsortF32(data: []const f32, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    const Context = struct {
        data: []const f32,
        ascending: bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            if (ctx.ascending) {
                return ctx.data[a] < ctx.data[b];
            } else {
                return ctx.data[a] > ctx.data[b];
            }
        }
    };

    const ctx = Context{ .data = data, .ascending = ascending };
    std.sort.pdq(u32, out_indices[0..len], ctx, Context.lessThan);
}

/// Argsort u64 - returns indices that would sort the array
pub fn argsortU64(data: []const u64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    const Context = struct {
        data: []const u64,
        ascending: bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            if (ctx.ascending) {
                return ctx.data[a] < ctx.data[b];
            } else {
                return ctx.data[a] > ctx.data[b];
            }
        }
    };

    const ctx = Context{ .data = data, .ascending = ascending };
    std.sort.pdq(u32, out_indices[0..len], ctx, Context.lessThan);
}

/// Argsort u32 - returns indices that would sort the array
pub fn argsortU32(data: []const u32, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    for (0..len) |i| {
        out_indices[i] = @intCast(i);
    }

    const Context = struct {
        data: []const u32,
        ascending: bool,

        pub fn lessThan(ctx: @This(), a: u32, b: u32) bool {
            if (ctx.ascending) {
                return ctx.data[a] < ctx.data[b];
            } else {
                return ctx.data[a] > ctx.data[b];
            }
        }
    };

    const ctx = Context{ .data = data, .ascending = ascending };
    std.sort.pdq(u32, out_indices[0..len], ctx, Context.lessThan);
}

// ============================================================================
// Gather Functions (used after argsort to reorder data)
// ============================================================================

/// Gather f64 values by indices
pub fn gatherF64(data: []const f64, indices: []const u32, out: []f64) void {
    const len = @min(indices.len, out.len);
    for (0..len) |i| {
        const idx = indices[i];
        out[i] = if (idx < data.len) data[idx] else 0;
    }
}

/// Gather i64 values by indices
pub fn gatherI64(data: []const i64, indices: []const u32, out: []i64) void {
    const len = @min(indices.len, out.len);
    for (0..len) |i| {
        const idx = indices[i];
        out[i] = if (idx < data.len) data[idx] else 0;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "sortF64 - basic ascending" {
    var data = [_]f64{ 3.0, 1.0, 4.0, 1.0, 5.0 };
    var out: [5]f64 = undefined;
    sortF64(&data, &out, true);
    try std.testing.expectEqual(@as(f64, 1.0), out[0]);
    try std.testing.expectEqual(@as(f64, 1.0), out[1]);
    try std.testing.expectEqual(@as(f64, 3.0), out[2]);
    try std.testing.expectEqual(@as(f64, 4.0), out[3]);
    try std.testing.expectEqual(@as(f64, 5.0), out[4]);
}

test "sortF64 - descending" {
    var data = [_]f64{ 3.0, 1.0, 4.0, 1.0, 5.0 };
    var out: [5]f64 = undefined;
    sortF64(&data, &out, false);
    try std.testing.expectEqual(@as(f64, 5.0), out[0]);
    try std.testing.expectEqual(@as(f64, 4.0), out[1]);
    try std.testing.expectEqual(@as(f64, 3.0), out[2]);
    try std.testing.expectEqual(@as(f64, 1.0), out[3]);
    try std.testing.expectEqual(@as(f64, 1.0), out[4]);
}

test "sortI64 - basic ascending" {
    var data = [_]i64{ 3, -1, 4, -1, 5, -9, 2, 6 };
    var out: [8]i64 = undefined;
    sortI64(&data, &out, true);
    try std.testing.expectEqual(@as(i64, -9), out[0]);
    try std.testing.expectEqual(@as(i64, -1), out[1]);
    try std.testing.expectEqual(@as(i64, -1), out[2]);
    try std.testing.expectEqual(@as(i64, 2), out[3]);
}

test "sortI64 - descending" {
    var data = [_]i64{ 3, -1, 4, -1, 5 };
    var out: [5]i64 = undefined;
    sortI64(&data, &out, false);
    try std.testing.expectEqual(@as(i64, 5), out[0]);
    try std.testing.expectEqual(@as(i64, 4), out[1]);
    try std.testing.expectEqual(@as(i64, 3), out[2]);
}

test "argsortF64 - basic" {
    const data = [_]f64{ 3.0, 1.0, 4.0, 1.0, 5.0 };
    var indices: [5]u32 = undefined;
    argsortF64(&data, &indices, true);
    try std.testing.expect(data[indices[0]] <= data[indices[1]]);
    try std.testing.expect(data[indices[1]] <= data[indices[2]]);
    try std.testing.expect(data[indices[2]] <= data[indices[3]]);
    try std.testing.expect(data[indices[3]] <= data[indices[4]]);
}

test "argsortI64 - basic" {
    const data = [_]i64{ 3, -1, 4, -1, 5 };
    var indices: [5]u32 = undefined;
    argsortI64(&data, &indices, true);
    try std.testing.expect(data[indices[0]] <= data[indices[1]]);
    try std.testing.expect(data[indices[1]] <= data[indices[2]]);
    try std.testing.expect(data[indices[2]] <= data[indices[3]]);
    try std.testing.expect(data[indices[3]] <= data[indices[4]]);
}

test "key conversion roundtrip - f64" {
    const values = [_]f64{ -std.math.inf(f64), -1000.0, -1.0, -0.0, 0.0, 1.0, 1000.0, std.math.inf(f64) };
    for (values) |v| {
        const key = floatToSortable(v);
        const back = sortableToF64(key);
        try std.testing.expectEqual(v, back);
    }
}

test "key conversion roundtrip - i64" {
    const values = [_]i64{ std.math.minInt(i64), -1000, -1, 0, 1, 1000, std.math.maxInt(i64) };
    for (values) |v| {
        const key = i64ToSortable(v);
        const back = sortableToI64(key);
        try std.testing.expectEqual(v, back);
    }
}

test "key conversion order - f64" {
    const values = [_]f64{ -std.math.inf(f64), -1000.0, -1.0, 0.0, 1.0, 1000.0, std.math.inf(f64) };
    var prev_key: u64 = 0;
    for (values, 0..) |v, i| {
        const key = floatToSortable(v);
        if (i > 0) {
            try std.testing.expect(key > prev_key);
        }
        prev_key = key;
    }
}

test "key conversion order - i64" {
    const values = [_]i64{ std.math.minInt(i64), -1000, -1, 0, 1, 1000, std.math.maxInt(i64) };
    var prev_key: u64 = 0;
    for (values, 0..) |v, i| {
        const key = i64ToSortable(v);
        if (i > 0) {
            try std.testing.expect(key > prev_key);
        }
        prev_key = key;
    }
}
