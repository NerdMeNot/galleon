//! Blitz - High-Performance Parallel Execution Library
//!
//! This module provides the unified Blitz API for Galleon, combining:
//! - Heartbeat-based work-stealing parallel execution (~10ns join overhead)
//! - SIMD-accelerated parallel aggregations (sum, min, max, etc.)
//!
//! Usage:
//! ```zig
//! const blitz = @import("blitz.zig");
//!
//! // Initialize (optional - auto-inits on first use)
//! try blitz.init();
//! defer blitz.deinit();
//!
//! // Parallel for loop
//! blitz.parallelFor(1000, void, {}, struct {
//!     fn body(_: void, start: usize, end: usize) void {
//!         for (start..end) |i| { ... }
//!     }
//! }.body);
//!
//! // SIMD-accelerated parallel sum
//! const sum = blitz.parallelSum(f64, data);
//! ```

const std = @import("std");
const simd = @import("simd.zig");

// ============================================================================
// Re-export blitz core (the independent parallel execution library)
// ============================================================================

const blitz_core = @import("blitz/mod.zig");

// Core types
pub const Job = blitz_core.Job;
pub const JobExecuteState = blitz_core.JobExecuteState;
pub const OnceLatch = blitz_core.OnceLatch;
pub const CountLatch = blitz_core.CountLatch;
pub const SpinWait = blitz_core.SpinWait;
pub const Worker = blitz_core.Worker;
pub const Task = blitz_core.Task;
pub const ThreadPool = blitz_core.ThreadPool;
pub const ThreadPoolConfig = blitz_core.ThreadPoolConfig;
pub const Future = blitz_core.Future;

// Initialization
pub const init = blitz_core.init;
pub const initWithConfig = blitz_core.initWithConfig;
pub const deinit = blitz_core.deinit;
pub const isInitialized = blitz_core.isInitialized;
pub const numWorkers = blitz_core.numWorkers;

// Core parallel primitives
pub const join = blitz_core.join;
pub const joinVoid = blitz_core.joinVoid;
pub const parallelFor = blitz_core.parallelFor;
pub const parallelForWithGrain = blitz_core.parallelForWithGrain;
pub const parallelReduce = blitz_core.parallelReduce;
pub const parallelReduceWithGrain = blitz_core.parallelReduceWithGrain;

// Threshold system
pub const threshold = blitz_core.threshold;
pub const OpType = blitz_core.OpType;
pub const shouldParallelize = blitz_core.shouldParallelize;
pub const isMemoryBound = blitz_core.isMemoryBound;

// ============================================================================
// SIMD-Accelerated Parallel Operations
// ============================================================================
// These combine blitz's parallel execution with Galleon's SIMD operations.

const CHUNK_SIZE: usize = 8192;

// --- Float Operations ---

/// Parallel sum using divide-and-conquer with SIMD leaf operations.
pub fn parallelSum(comptime T: type, data: []const T) T {
    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.sum(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    return parallelReduceWithGrain(
        T,
        num_chunks,
        0,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                return simd.sum(T, c.data[chunk_start..chunk_end]);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return a + b;
            }
        }.combine,
        1,
    );
}

/// Parallel min using divide-and-conquer with SIMD leaf operations.
pub fn parallelMin(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.min(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;
    const identity = std.math.floatMax(T);

    const result = parallelReduceWithGrain(
        T,
        num_chunks,
        identity,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                return simd.min(T, c.data[chunk_start..chunk_end]) orelse std.math.floatMax(T);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return @min(a, b);
            }
        }.combine,
        1,
    );

    return if (result == identity) null else result;
}

/// Parallel max using divide-and-conquer with SIMD leaf operations.
pub fn parallelMax(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.max(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;
    const identity = std.math.floatMin(T);

    const result = parallelReduceWithGrain(
        T,
        num_chunks,
        identity,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                return simd.max(T, c.data[chunk_start..chunk_end]) orelse std.math.floatMin(T);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return @max(a, b);
            }
        }.combine,
        1,
    );

    return if (result == identity) null else result;
}

// --- Integer Operations ---

/// Parallel sum for integer types.
pub fn parallelSumInt(comptime T: type, data: []const T) T {
    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.sumInt(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    return parallelReduceWithGrain(
        T,
        num_chunks,
        0,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                return simd.sumInt(T, c.data[chunk_start..chunk_end]);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return a + b;
            }
        }.combine,
        1,
    );
}

/// Parallel min for integer types.
pub fn parallelMinInt(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.minInt(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;
    const identity = std.math.maxInt(T);

    const result = parallelReduceWithGrain(
        T,
        num_chunks,
        identity,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                return simd.minInt(T, c.data[chunk_start..chunk_end]) orelse std.math.maxInt(T);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return @min(a, b);
            }
        }.combine,
        1,
    );

    return if (result == identity) null else result;
}

/// Parallel max for integer types.
pub fn parallelMaxInt(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.maxInt(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;
    const identity = std.math.minInt(T);

    const result = parallelReduceWithGrain(
        T,
        num_chunks,
        identity,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                return simd.maxInt(T, c.data[chunk_start..chunk_end]) orelse std.math.minInt(T);
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return @max(a, b);
            }
        }.combine,
        1,
    );

    return if (result == identity) null else result;
}

/// Parallel element-wise addition with SIMD.
pub fn parallelAdd(comptime T: type, a: []const T, b: []const T, out: []T) void {
    std.debug.assert(a.len == b.len);
    std.debug.assert(a.len == out.len);

    if (a.len <= CHUNK_SIZE or !isInitialized()) {
        simd.addArraysOut(T, a, b, out);
        return;
    }

    const Context = struct {
        a: []const T,
        b: []const T,
        out: []T,
    };
    const ctx = Context{ .a = a, .b = b, .out = out };

    parallelForWithGrain(
        a.len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                simd.addArraysOut(T, c.a[start..end], c.b[start..end], c.out[start..end]);
            }
        }.body,
        CHUNK_SIZE,
    );
}

// ============================================================================
// Ergonomic Parallel Operations (Rayon-inspired)
// ============================================================================

/// Parallel map: transform each element using a function.
/// Output is written to `out` slice.
///
/// Example:
/// ```zig
/// var data = [_]f64{1, 2, 3, 4};
/// var result: [4]f64 = undefined;
/// blitz.parallelMap(f64, f64, void, &data, &result, struct {
///     fn transform(_: void, x: f64) f64 { return x * 2; }
/// }.transform, {});
/// // result is now {2, 4, 6, 8}
/// ```
pub fn parallelMap(
    comptime T: type,
    comptime U: type,
    comptime Ctx: type,
    input: []const T,
    output: []U,
    comptime mapFn: fn (Ctx, T) U,
    ctx: Ctx,
) void {
    std.debug.assert(input.len == output.len);

    if (input.len <= CHUNK_SIZE or !isInitialized()) {
        for (input, output) |in, *out| {
            out.* = mapFn(ctx, in);
        }
        return;
    }

    const Context = struct {
        input: []const T,
        output: []U,
        user_ctx: Ctx,
    };
    const context = Context{ .input = input, .output = output, .user_ctx = ctx };

    parallelForWithGrain(
        input.len,
        Context,
        context,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                for (c.input[start..end], c.output[start..end]) |in, *out| {
                    out.* = mapFn(c.user_ctx, in);
                }
            }
        }.body,
        CHUNK_SIZE,
    );
}

/// Parallel map in-place: transform each element of a slice.
///
/// Example:
/// ```zig
/// var data = [_]f64{1, 2, 3, 4};
/// blitz.parallelMapInPlace(f64, void, &data, struct {
///     fn transform(_: void, x: f64) f64 { return x * 2; }
/// }.transform, {});
/// // data is now {2, 4, 6, 8}
/// ```
pub fn parallelMapInPlace(
    comptime T: type,
    comptime Ctx: type,
    data: []T,
    comptime mapFn: fn (Ctx, T) T,
    ctx: Ctx,
) void {
    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        for (data) |*v| {
            v.* = mapFn(ctx, v.*);
        }
        return;
    }

    const Context = struct {
        data: []T,
        user_ctx: Ctx,
    };
    const context = Context{ .data = data, .user_ctx = ctx };

    parallelForWithGrain(
        data.len,
        Context,
        context,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                for (c.data[start..end]) |*v| {
                    v.* = mapFn(c.user_ctx, v.*);
                }
            }
        }.body,
        CHUNK_SIZE,
    );
}

/// Parallel fill: fill a slice with a constant value.
pub fn parallelFill(comptime T: type, data: []T, value: T) void {
    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        @memset(data, value);
        return;
    }

    const Context = struct {
        data: []T,
        value: T,
    };
    const ctx = Context{ .data = data, .value = value };

    parallelForWithGrain(
        data.len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                @memset(c.data[start..end], c.value);
            }
        }.body,
        CHUNK_SIZE,
    );
}

/// Parallel fill with index: fill a slice using a function of the index.
///
/// Example:
/// ```zig
/// var data: [1000]f64 = undefined;
/// blitz.parallelFillIndexed(f64, void, &data, struct {
///     fn gen(_: void, i: usize) f64 { return @floatFromInt(i * 2); }
/// }.gen, {});
/// ```
pub fn parallelFillIndexed(
    comptime T: type,
    comptime Ctx: type,
    data: []T,
    comptime genFn: fn (Ctx, usize) T,
    ctx: Ctx,
) void {
    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        for (data, 0..) |*v, i| {
            v.* = genFn(ctx, i);
        }
        return;
    }

    const Context = struct {
        data: []T,
        user_ctx: Ctx,
    };
    const context = Context{ .data = data, .user_ctx = ctx };

    parallelForWithGrain(
        data.len,
        Context,
        context,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                for (c.data[start..end], start..end) |*v, i| {
                    v.* = genFn(c.user_ctx, i);
                }
            }
        }.body,
        CHUNK_SIZE,
    );
}

/// Parallel mean for floating point types.
pub fn parallelMean(comptime T: type, data: []const T) ?T {
    if (data.len == 0) return null;
    return parallelSum(T, data) / @as(T, @floatFromInt(data.len));
}

/// Parallel mean for integer types (returns float).
pub fn parallelMeanInt(comptime T: type, data: []const T) ?f64 {
    if (data.len == 0) return null;
    const sum_val: f64 = @floatFromInt(parallelSumInt(T, data));
    return sum_val / @as(f64, @floatFromInt(data.len));
}

/// Parallel any: check if any element satisfies a predicate.
/// Returns true if at least one element matches.
pub fn parallelAny(
    comptime T: type,
    comptime Ctx: type,
    data: []const T,
    comptime predFn: fn (Ctx, T) bool,
    ctx: Ctx,
) bool {
    if (data.len == 0) return false;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        for (data) |v| {
            if (predFn(ctx, v)) return true;
        }
        return false;
    }

    const Context = struct {
        data: []const T,
        user_ctx: Ctx,
    };
    const context = Context{ .data = data, .user_ctx = ctx };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Use parallelReduce with OR combiner
    return parallelReduceWithGrain(
        bool,
        num_chunks,
        false,
        Context,
        context,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) bool {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                for (c.data[chunk_start..chunk_end]) |v| {
                    if (predFn(c.user_ctx, v)) return true;
                }
                return false;
            }
        }.mapChunk,
        struct {
            fn combine(a: bool, b: bool) bool {
                return a or b;
            }
        }.combine,
        1,
    );
}

/// Parallel all: check if all elements satisfy a predicate.
/// Returns true only if every element matches.
pub fn parallelAll(
    comptime T: type,
    comptime Ctx: type,
    data: []const T,
    comptime predFn: fn (Ctx, T) bool,
    ctx: Ctx,
) bool {
    if (data.len == 0) return true;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        for (data) |v| {
            if (!predFn(ctx, v)) return false;
        }
        return true;
    }

    const Context = struct {
        data: []const T,
        user_ctx: Ctx,
    };
    const context = Context{ .data = data, .user_ctx = ctx };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Use parallelReduce with AND combiner
    return parallelReduceWithGrain(
        bool,
        num_chunks,
        true,
        Context,
        context,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) bool {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                for (c.data[chunk_start..chunk_end]) |v| {
                    if (!predFn(c.user_ctx, v)) return false;
                }
                return true;
            }
        }.mapChunk,
        struct {
            fn combine(a: bool, b: bool) bool {
                return a and b;
            }
        }.combine,
        1,
    );
}

/// Parallel count: count elements that satisfy a predicate.
pub fn parallelCount(
    comptime T: type,
    comptime Ctx: type,
    data: []const T,
    comptime predFn: fn (Ctx, T) bool,
    ctx: Ctx,
) usize {
    if (data.len == 0) return 0;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        var count: usize = 0;
        for (data) |v| {
            if (predFn(ctx, v)) count += 1;
        }
        return count;
    }

    const Context = struct {
        data: []const T,
        user_ctx: Ctx,
    };
    const context = Context{ .data = data, .user_ctx = ctx };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    return parallelReduceWithGrain(
        usize,
        num_chunks,
        0,
        Context,
        context,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) usize {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                var count: usize = 0;
                for (c.data[chunk_start..chunk_end]) |v| {
                    if (predFn(c.user_ctx, v)) count += 1;
                }
                return count;
            }
        }.mapChunk,
        struct {
            fn combine(a: usize, b: usize) usize {
                return a + b;
            }
        }.combine,
        1,
    );
}

/// Parallel product for floating point types.
pub fn parallelProduct(comptime T: type, data: []const T) T {
    if (data.len == 0) return 1;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        var result: T = 1;
        for (data) |v| {
            result *= v;
        }
        return result;
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    return parallelReduceWithGrain(
        T,
        num_chunks,
        1,
        Context,
        ctx,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) T {
                const chunk_start = chunk_idx * CHUNK_SIZE;
                const chunk_end = @min(chunk_start + CHUNK_SIZE, c.data.len);
                var result: T = 1;
                for (c.data[chunk_start..chunk_end]) |v| {
                    result *= v;
                }
                return result;
            }
        }.mapChunk,
        struct {
            fn combine(a: T, b: T) T {
                return a * b;
            }
        }.combine,
        1,
    );
}

// ============================================================================
// Chunked Operations
// ============================================================================

/// Parallel reduce over chunks of data.
/// Used by ChunkedColumn for operations like sum, min, max across all chunks.
/// Note: chunks parameter accepts anytype to handle aligned slice types.
pub fn parallelChunkReduce(
    comptime T: type,
    comptime R: type,
    chunks: anytype, // [][]T or [][]align(N) T
    chunk_sizes: []const usize,
    allocator: std.mem.Allocator,
    identity: R,
    comptime Ctx: type,
    ctx: Ctx,
    comptime processFn: fn (Ctx, []const T) R,
    comptime combineFn: fn (R, R) R,
) !R {
    const num_chunks = chunk_sizes.len;
    if (num_chunks == 0) return identity;

    // Sequential for small number of chunks or uninitialized pool
    if (num_chunks < 4 or !isInitialized()) {
        var result = identity;
        for (0..num_chunks) |i| {
            const chunk_slice: []const T = chunks[i][0..chunk_sizes[i]];
            result = combineFn(result, processFn(ctx, chunk_slice));
        }
        return result;
    }

    // For parallel execution, we need to capture chunks by pointer
    const ChunksType = @TypeOf(chunks);
    const Context = struct {
        chunks_ptr: *const ChunksType,
        chunk_sizes: []const usize,
        user_ctx: Ctx,
    };
    const context = Context{
        .chunks_ptr = &chunks,
        .chunk_sizes = chunk_sizes,
        .user_ctx = ctx,
    };

    _ = allocator; // Not needed for stack-based parallel reduce

    return parallelReduceWithGrain(
        R,
        num_chunks,
        identity,
        Context,
        context,
        struct {
            fn mapChunk(c: Context, chunk_idx: usize) R {
                const chunk_slice: []const T = c.chunks_ptr.*[chunk_idx][0..c.chunk_sizes[chunk_idx]];
                return processFn(c.user_ctx, chunk_slice);
            }
        }.mapChunk,
        combineFn,
        1, // grain size of 1 - each chunk is independent
    );
}

// ============================================================================
// Tests
// ============================================================================

test "blitz - re-exports work" {
    // Verify core functions are accessible
    _ = init;
    _ = deinit;
    _ = parallelFor;
    _ = parallelReduce;
    _ = shouldParallelize;
}

test "blitz - parallel sum" {
    var data: [100]f64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i);
    }

    const expected: f64 = 100.0 * 99.0 / 2.0;
    const result = parallelSum(f64, &data);

    try std.testing.expectApproxEqAbs(expected, result, 0.001);
}

test "blitz - parallel sum int" {
    var data: [100]i64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @intCast(i);
    }

    const expected: i64 = 100 * 99 / 2;
    const result = parallelSumInt(i64, &data);

    try std.testing.expectEqual(expected, result);
}

test "blitz - parallelMap" {
    var input = [_]f64{ 1, 2, 3, 4, 5 };
    var output: [5]f64 = undefined;

    parallelMap(f64, f64, void, &input, &output, struct {
        fn transform(_: void, x: f64) f64 {
            return x * 2;
        }
    }.transform, {});

    try std.testing.expectApproxEqAbs(@as(f64, 2), output[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 4), output[1], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), output[4], 0.001);
}

test "blitz - parallelMapInPlace" {
    var data = [_]f64{ 1, 2, 3, 4, 5 };

    parallelMapInPlace(f64, void, &data, struct {
        fn transform(_: void, x: f64) f64 {
            return x + 10;
        }
    }.transform, {});

    try std.testing.expectApproxEqAbs(@as(f64, 11), data[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 15), data[4], 0.001);
}

test "blitz - parallelFill" {
    var data: [100]f64 = undefined;
    parallelFill(f64, &data, 42.0);

    for (data) |v| {
        try std.testing.expectApproxEqAbs(@as(f64, 42), v, 0.001);
    }
}

test "blitz - parallelFillIndexed" {
    var data: [100]f64 = undefined;
    parallelFillIndexed(f64, void, &data, struct {
        fn gen(_: void, i: usize) f64 {
            return @floatFromInt(i * 2);
        }
    }.gen, {});

    try std.testing.expectApproxEqAbs(@as(f64, 0), data[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 10), data[5], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 198), data[99], 0.001);
}

test "blitz - parallelMean" {
    var data = [_]f64{ 1, 2, 3, 4, 5 };
    const result = parallelMean(f64, &data);
    try std.testing.expectApproxEqAbs(@as(f64, 3), result.?, 0.001);
}

test "blitz - parallelAny" {
    var data = [_]i32{ 1, 2, 3, 4, 5 };

    const has_gt_3 = parallelAny(i32, void, &data, struct {
        fn pred(_: void, x: i32) bool {
            return x > 3;
        }
    }.pred, {});
    try std.testing.expect(has_gt_3);

    const has_gt_10 = parallelAny(i32, void, &data, struct {
        fn pred(_: void, x: i32) bool {
            return x > 10;
        }
    }.pred, {});
    try std.testing.expect(!has_gt_10);
}

test "blitz - parallelAll" {
    var data = [_]i32{ 1, 2, 3, 4, 5 };

    const all_positive = parallelAll(i32, void, &data, struct {
        fn pred(_: void, x: i32) bool {
            return x > 0;
        }
    }.pred, {});
    try std.testing.expect(all_positive);

    const all_lt_3 = parallelAll(i32, void, &data, struct {
        fn pred(_: void, x: i32) bool {
            return x < 3;
        }
    }.pred, {});
    try std.testing.expect(!all_lt_3);
}

test "blitz - parallelCount" {
    var data = [_]i32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    const count_even = parallelCount(i32, void, &data, struct {
        fn pred(_: void, x: i32) bool {
            return @mod(x, 2) == 0;
        }
    }.pred, {});
    try std.testing.expectEqual(@as(usize, 5), count_even);
}

test "blitz - parallelProduct" {
    var data = [_]f64{ 1, 2, 3, 4, 5 };
    const result = parallelProduct(f64, &data);
    try std.testing.expectApproxEqAbs(@as(f64, 120), result, 0.001);
}
