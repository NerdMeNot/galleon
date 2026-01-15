//! Blitz - Work-Stealing Parallel Execution Library
//!
//! A Rayon-inspired work-stealing parallel execution library for Zig.
//! Provides primitives for fork-join parallelism with automatic load balancing.
//!
//! Key Features:
//! - Chase-Lev work-stealing deques for efficient task distribution
//! - Potential parallelism (runtime decides when to parallelize)
//! - Zero-allocation join() for recursive divide-and-conquer
//! - Integration with Galleon's SIMD operations
//!
//! Usage:
//! ```zig
//! const blitz = @import("blitz/mod.zig");
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
//! // Parallel reduce
//! const sum = blitz.parallelReduce(f64, data.len, 0.0, slice, struct {
//!     fn map(s: []f64, i: usize) f64 { return s[i]; }
//! }.map, struct {
//!     fn combine(a: f64, b: f64) f64 { return a + b; }
//! }.combine);
//! ```

const std = @import("std");

// Re-export core types
pub const Deque = @import("deque.zig").Deque;
pub const StealResult = @import("deque.zig").StealResult;
pub const Job = @import("job.zig").Job;
pub const CountLatch = @import("latch.zig").CountLatch;
pub const OnceLatch = @import("latch.zig").OnceLatch;
pub const Worker = @import("worker.zig").Worker;
pub const Registry = @import("registry.zig").Registry;

// Threshold module - intelligent parallelism decisions
pub const threshold = @import("threshold.zig");
pub const OpType = threshold.OpType;
pub const shouldParallelize = threshold.shouldParallelize;
pub const isMemoryBound = threshold.isMemoryBound;

const registry_mod = @import("registry.zig");
const job_mod = @import("job.zig");
const latch_mod = @import("latch.zig");

// ============================================================================
// Initialization
// ============================================================================

/// Initialize the Blitz thread pool.
/// This is optional - the pool auto-initializes on first use.
pub fn init() !void {
    _ = try registry_mod.getGlobalRegistry();
}

/// Shutdown the thread pool and release resources.
pub fn deinit() void {
    registry_mod.shutdownGlobalRegistry();
}

/// Check if the pool is initialized.
pub fn isInitialized() bool {
    return registry_mod.isGlobalRegistryInitialized();
}

/// Get the number of worker threads.
pub fn numWorkers() u32 {
    if (registry_mod.getGlobalRegistry()) |reg| {
        return reg.getNumWorkers();
    } else |_| {
        return 1;
    }
}

// ============================================================================
// Core Primitive: join()
// ============================================================================

/// Execute two tasks potentially in parallel using work-stealing.
///
/// The second task (fn_b) is pushed to the local deque where other workers
/// can steal it, while the first task (fn_a) is executed immediately.
/// This enables recursive divide-and-conquer algorithms.
///
/// If called from outside the thread pool, both tasks execute sequentially.
pub fn join(
    comptime ContextA: type,
    comptime ContextB: type,
    ctx_a: *ContextA,
    ctx_b: *ContextB,
    comptime fn_a: fn (*ContextA) void,
    comptime fn_b: fn (*ContextB) void,
) void {
    // Try to get current worker (if on worker thread)
    if (registry_mod.getCurrentWorker()) |worker| {
        // We're on a worker thread - use work-stealing
        joinOnWorker(ContextA, ContextB, ctx_a, ctx_b, fn_a, fn_b, worker);
    } else {
        // Not on worker thread - try to inject or run sequentially
        if (registry_mod.getGlobalRegistry()) |reg| {
            joinFromExternal(ContextA, ContextB, ctx_a, ctx_b, fn_a, fn_b, reg);
        } else |_| {
            // No pool available - sequential fallback
            fn_a(ctx_a);
            fn_b(ctx_b);
        }
    }
}

fn joinOnWorker(
    comptime ContextA: type,
    comptime ContextB: type,
    ctx_a: *ContextA,
    ctx_b: *ContextB,
    comptime fn_a: fn (*ContextA) void,
    comptime fn_b: fn (*ContextB) void,
    worker: *Worker,
) void {
    // Create a stack job for task B
    const JobB = job_mod.StackJob(ContextB, fn_b);
    var job_b = JobB.init(ctx_b.*);

    // Push job B to local deque
    worker.push(job_b.getJob()) catch {
        // Deque full - execute sequentially
        fn_a(ctx_a);
        fn_b(ctx_b);
        return;
    };

    // Execute task A directly
    fn_a(ctx_a);

    // Check if job B was stolen
    if (!job_b.isDone()) {
        // Try to pop job B back
        if (worker.pop()) |popped_job| {
            if (popped_job == job_b.getJob()) {
                // We got our job back - execute it
                fn_b(ctx_b);
                job_b.complete();
            } else {
                // Got a different job - push it back and wait
                worker.push(popped_job) catch {};
                job_b.wait();
            }
        } else {
            // Job was stolen - wait for completion
            job_b.wait();
        }
    }
}

fn joinFromExternal(
    comptime ContextA: type,
    comptime ContextB: type,
    ctx_a: *ContextA,
    ctx_b: *ContextB,
    comptime fn_a: fn (*ContextA) void,
    comptime fn_b: fn (*ContextB) void,
    reg: *Registry,
) void {
    _ = reg;
    // For external calls, just execute sequentially for now
    // (Injecting both and waiting adds overhead that often isn't worth it)
    fn_a(ctx_a);
    fn_b(ctx_b);
}

// ============================================================================
// Parallel For
// ============================================================================

/// Default grain size for parallel for (minimum work per task)
const DEFAULT_GRAIN_SIZE: usize = 1024;

/// Execute a function over range [0, n) with automatic parallelization.
///
/// The range is recursively split using join() until chunks are smaller
/// than the grain size, enabling work-stealing for load balancing.
pub fn parallelFor(
    n: usize,
    comptime Context: type,
    context: Context,
    comptime body_fn: fn (Context, usize, usize) void,
) void {
    parallelForWithGrain(n, Context, context, body_fn, DEFAULT_GRAIN_SIZE);
}

/// Execute a function over range [0, n) with custom grain size.
pub fn parallelForWithGrain(
    n: usize,
    comptime Context: type,
    context: Context,
    comptime body_fn: fn (Context, usize, usize) void,
    grain_size: usize,
) void {
    if (n == 0) return;

    if (n <= grain_size or !isInitialized()) {
        // Below threshold or no pool - execute sequentially
        body_fn(context, 0, n);
        return;
    }

    // Recursive parallel execution
    parallelForImpl(Context, context, body_fn, 0, n, grain_size);
}

fn parallelForImpl(
    comptime Context: type,
    context: Context,
    comptime body_fn: fn (Context, usize, usize) void,
    start: usize,
    end: usize,
    grain_size: usize,
) void {
    const len = end - start;

    if (len <= grain_size) {
        // Base case: sequential execution
        body_fn(context, start, end);
        return;
    }

    // Split and recurse with work-stealing
    const mid = start + len / 2;

    const LeftCtx = struct {
        ctx: Context,
        s: usize,
        m: usize,
        grain: usize,
    };

    const RightCtx = struct {
        ctx: Context,
        m: usize,
        e: usize,
        grain: usize,
    };

    var left = LeftCtx{ .ctx = context, .s = start, .m = mid, .grain = grain_size };
    var right = RightCtx{ .ctx = context, .m = mid, .e = end, .grain = grain_size };

    join(
        LeftCtx,
        RightCtx,
        &left,
        &right,
        struct {
            fn exec(l: *LeftCtx) void {
                parallelForImpl(Context, l.ctx, body_fn, l.s, l.m, l.grain);
            }
        }.exec,
        struct {
            fn exec(r: *RightCtx) void {
                parallelForImpl(Context, r.ctx, body_fn, r.m, r.e, r.grain);
            }
        }.exec,
    );
}

// ============================================================================
// Parallel Reduce
// ============================================================================

/// Parallel reduction with associative combine function.
///
/// Maps each index to a value, then combines values in parallel using
/// a divide-and-conquer pattern with work-stealing.
pub fn parallelReduce(
    comptime T: type,
    n: usize,
    identity: T,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, usize) T,
    comptime combine_fn: fn (T, T) T,
) T {
    return parallelReduceWithGrain(T, n, identity, Context, context, map_fn, combine_fn, DEFAULT_GRAIN_SIZE);
}

/// Parallel reduction with custom grain size.
pub fn parallelReduceWithGrain(
    comptime T: type,
    n: usize,
    identity: T,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, usize) T,
    comptime combine_fn: fn (T, T) T,
    grain_size: usize,
) T {
    if (n == 0) return identity;

    if (n <= grain_size or !isInitialized()) {
        // Sequential reduction
        var result = identity;
        for (0..n) |i| {
            result = combine_fn(result, map_fn(context, i));
        }
        return result;
    }

    return parallelReduceImpl(T, Context, context, map_fn, combine_fn, identity, 0, n, grain_size);
}

fn parallelReduceImpl(
    comptime T: type,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, usize) T,
    comptime combine_fn: fn (T, T) T,
    identity: T,
    start: usize,
    end: usize,
    grain_size: usize,
) T {
    const len = end - start;

    if (len <= grain_size) {
        // Sequential reduction
        var result = identity;
        for (start..end) |i| {
            result = combine_fn(result, map_fn(context, i));
        }
        return result;
    }

    // Split and recurse
    const mid = start + len / 2;

    const LeftCtx = struct {
        ctx: Context,
        s: usize,
        m: usize,
        grain: usize,
        identity: T,
        result: T,
    };

    const RightCtx = struct {
        ctx: Context,
        m: usize,
        e: usize,
        grain: usize,
        identity: T,
        result: T,
    };

    var left = LeftCtx{
        .ctx = context,
        .s = start,
        .m = mid,
        .grain = grain_size,
        .identity = identity,
        .result = identity,
    };

    var right = RightCtx{
        .ctx = context,
        .m = mid,
        .e = end,
        .grain = grain_size,
        .identity = identity,
        .result = identity,
    };

    join(
        LeftCtx,
        RightCtx,
        &left,
        &right,
        struct {
            fn exec(l: *LeftCtx) void {
                l.result = parallelReduceImpl(T, Context, l.ctx, map_fn, combine_fn, l.identity, l.s, l.m, l.grain);
            }
        }.exec,
        struct {
            fn exec(r: *RightCtx) void {
                r.result = parallelReduceImpl(T, Context, r.ctx, map_fn, combine_fn, r.identity, r.m, r.e, r.grain);
            }
        }.exec,
    );

    return combine_fn(left.result, right.result);
}

// ============================================================================
// Parallel SIMD Operations
// ============================================================================

/// Parallel sum using divide-and-conquer with SIMD leaf operations.
/// Uses Galleon's SIMD sum for the sequential chunks.
pub fn parallelSum(comptime T: type, data: []const T) T {
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192; // ~64KB for f64

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.sum(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };

    // Calculate number of chunks
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
        1, // Grain size of 1 chunk (each chunk is already SIMD-optimized)
    );
}

/// Parallel min using divide-and-conquer with SIMD leaf operations.
pub fn parallelMin(comptime T: type, data: []const T) ?T {
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192;

    if (data.len == 0) return null;

    if (data.len <= CHUNK_SIZE or !isInitialized()) {
        return simd.min(T, data);
    }

    const Context = struct { data: []const T };
    const ctx = Context{ .data = data };
    const num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

    // Use max value as identity for min reduction
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
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192;

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

// ============================================================================
// Chunk-Based Operations (for ChunkedColumn)
// ============================================================================

/// Process chunks in parallel with map-reduce pattern.
/// Perfect for ChunkedColumn operations where each chunk fits in L2 cache.
pub fn parallelChunkMap(
    comptime T: type,
    comptime R: type,
    chunks: [][]const T,
    chunk_sizes: []const usize,
    results: []R,
    comptime Context: type,
    context: Context,
    comptime process_fn: fn (Context, []const T) R,
) void {
    std.debug.assert(chunks.len == results.len);
    std.debug.assert(chunks.len == chunk_sizes.len);

    const n = chunks.len;
    if (n == 0) return;

    // For small number of chunks, run sequentially
    if (n < MIN_PARALLEL_CHUNKS or !isInitialized()) {
        for (0..n) |i| {
            results[i] = process_fn(context, chunks[i][0..chunk_sizes[i]]);
        }
        return;
    }

    // Process chunks in parallel
    const MapContext = struct {
        chunks: [][]const T,
        chunk_sizes: []const usize,
        results: []R,
        ctx: Context,
    };
    const map_ctx = MapContext{
        .chunks = @constCast(chunks),
        .chunk_sizes = chunk_sizes,
        .results = results,
        .ctx = context,
    };

    parallelForWithGrain(
        n,
        MapContext,
        map_ctx,
        struct {
            fn body(c: MapContext, start: usize, end: usize) void {
                for (start..end) |i| {
                    c.results[i] = process_fn(c.ctx, c.chunks[i][0..c.chunk_sizes[i]]);
                }
            }
        }.body,
        1, // Each chunk is already an optimal work unit
    );
}

/// Reduce chunk results with an associative combine function.
/// Used after parallelChunkMap to combine per-chunk results.
pub fn reduceChunkResults(
    comptime R: type,
    results: []const R,
    identity: R,
    comptime combine_fn: fn (R, R) R,
) R {
    if (results.len == 0) return identity;

    var result = identity;
    for (results) |r| {
        result = combine_fn(result, r);
    }
    return result;
}

/// Combined map-reduce over chunks - process each chunk and combine results.
/// The most common pattern for aggregations on ChunkedColumn.
pub fn parallelChunkReduce(
    comptime T: type,
    comptime R: type,
    chunks: [][]const T,
    chunk_sizes: []const usize,
    allocator: std.mem.Allocator,
    identity: R,
    comptime Context: type,
    context: Context,
    comptime process_fn: fn (Context, []const T) R,
    comptime combine_fn: fn (R, R) R,
) !R {
    const n = chunks.len;
    if (n == 0) return identity;

    // For small number of chunks, skip allocation
    if (n < MIN_PARALLEL_CHUNKS or !isInitialized()) {
        var result = identity;
        for (0..n) |i| {
            const chunk_result = process_fn(context, chunks[i][0..chunk_sizes[i]]);
            result = combine_fn(result, chunk_result);
        }
        return result;
    }

    // Allocate space for intermediate results
    const results = try allocator.alloc(R, n);
    defer allocator.free(results);

    // Process in parallel
    parallelChunkMap(T, R, chunks, chunk_sizes, results, Context, context, process_fn);

    // Combine results
    return reduceChunkResults(R, results, identity, combine_fn);
}

/// Minimum chunks needed to justify parallelism overhead
const MIN_PARALLEL_CHUNKS: usize = 2;

// ============================================================================
// Parallel Integer Operations
// ============================================================================

/// Parallel sum for integer types using divide-and-conquer with SIMD leaf operations.
pub fn parallelSumInt(comptime T: type, data: []const T) T {
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192;

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
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192;

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
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192;

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
    const simd = @import("../simd.zig");
    const CHUNK_SIZE: usize = 8192;

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
// Tests
// ============================================================================

test "parallelFor - basic" {
    var results: [1000]u32 = undefined;
    @memset(&results, 0);

    const Context = struct { results: []u32 };
    const ctx = Context{ .results = &results };

    parallelFor(1000, Context, ctx, struct {
        fn body(c: Context, start: usize, end: usize) void {
            for (start..end) |i| {
                c.results[i] = @intCast(i);
            }
        }
    }.body);

    // Verify all set correctly
    for (results, 0..) |v, i| {
        try std.testing.expectEqual(@as(u32, @intCast(i)), v);
    }
}

test "parallelFor - empty range" {
    var called = false;
    const Context = struct { called: *bool };
    const ctx = Context{ .called = &called };

    parallelFor(0, Context, ctx, struct {
        fn body(c: Context, _: usize, _: usize) void {
            c.called.* = true;
        }
    }.body);

    try std.testing.expect(!called);
}

test "parallelReduce - sum" {
    var data: [10000]f64 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @floatFromInt(i);
    }

    const expected: f64 = 10000.0 * 9999.0 / 2.0; // Sum of 0..9999

    const Context = struct { data: []f64 };
    const ctx = Context{ .data = &data };

    const result = parallelReduce(
        f64,
        data.len,
        0.0,
        Context,
        ctx,
        struct {
            fn map(c: Context, i: usize) f64 {
                return c.data[i];
            }
        }.map,
        struct {
            fn combine(a: f64, b: f64) f64 {
                return a + b;
            }
        }.combine,
    );

    try std.testing.expectApproxEqAbs(expected, result, 0.001);
}

test "parallelReduce - min" {
    var data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0 };

    const Context = struct { data: []f64 };
    const ctx = Context{ .data = &data };

    const result = parallelReduce(
        f64,
        data.len,
        std.math.floatMax(f64),
        Context,
        ctx,
        struct {
            fn map(c: Context, i: usize) f64 {
                return c.data[i];
            }
        }.map,
        struct {
            fn combine(a: f64, b: f64) f64 {
                return @min(a, b);
            }
        }.combine,
    );

    try std.testing.expectEqual(@as(f64, 1.0), result);
}

test "join - basic" {
    var a_done = false;
    var b_done = false;

    const CtxA = struct { done: *bool };
    const CtxB = struct { done: *bool };

    var ctx_a = CtxA{ .done = &a_done };
    var ctx_b = CtxB{ .done = &b_done };

    join(
        CtxA,
        CtxB,
        &ctx_a,
        &ctx_b,
        struct {
            fn exec(c: *CtxA) void {
                c.done.* = true;
            }
        }.exec,
        struct {
            fn exec(c: *CtxB) void {
                c.done.* = true;
            }
        }.exec,
    );

    try std.testing.expect(a_done);
    try std.testing.expect(b_done);
}
