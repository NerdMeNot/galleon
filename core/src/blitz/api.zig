//! High-Level API for Blitz2
//!
//! This module provides ergonomic parallel primitives built on top of
//! the heartbeat-based work-stealing runtime.
//!
//! Key functions:
//! - join(): Fork-join two tasks with return values
//! - parallelFor(): Parallel iteration over a range
//! - parallelReduce(): Parallel map-reduce with associative combine

const std = @import("std");
const Job = @import("job.zig").Job;
const Future = @import("future.zig").Future;
const Worker = @import("worker.zig").Worker;
const Task = @import("worker.zig").Task;
const ThreadPool = @import("pool.zig").ThreadPool;
const ThreadPoolConfig = @import("pool.zig").ThreadPoolConfig;
const sync = @import("sync.zig");
pub const SyncPtr = sync.SyncPtr;
pub const computeOffsetsInto = sync.computeOffsetsInto;
pub const capAndOffsets = sync.capAndOffsets;

/// Default grain size - minimum work per task.
/// Below this, we don't parallelize to avoid overhead.
/// Set high enough that parallelism overhead is amortized for simple operations.
const DEFAULT_GRAIN_SIZE: usize = 65536;

// ============================================================================
// Runtime Configuration
// ============================================================================

/// Runtime-configurable grain size. Defaults to DEFAULT_GRAIN_SIZE.
/// Uses atomic for thread-safe access.
var configured_grain_size: std.atomic.Value(usize) = std.atomic.Value(usize).init(DEFAULT_GRAIN_SIZE);

/// Get the current grain size (runtime-configurable).
pub fn getGrainSize() usize {
    return configured_grain_size.load(.monotonic);
}

/// Set the grain size for parallel operations.
/// This affects all subsequent parallel operations.
/// Pass 0 to reset to the default value.
pub fn setGrainSize(size: usize) void {
    const value = if (size == 0) DEFAULT_GRAIN_SIZE else size;
    configured_grain_size.store(value, .monotonic);
}

/// Get the default grain size (compile-time constant).
pub fn defaultGrainSize() usize {
    return DEFAULT_GRAIN_SIZE;
}

// ============================================================================
// Global Pool Management
// ============================================================================

var global_pool: ?*ThreadPool = null;
var pool_mutex: std.Thread.Mutex = .{};
var pool_allocator: std.mem.Allocator = undefined;

/// Thread-local task context for fast recursive calls.
/// When set, we're already inside a pool.call() and can use fork/join directly.
threadlocal var current_task: ?*Task = null;

/// Initialize the global thread pool.
pub fn init() !void {
    return initWithConfig(.{});
}

/// Initialize with custom configuration.
pub fn initWithConfig(config: ThreadPoolConfig) !void {
    pool_mutex.lock();
    defer pool_mutex.unlock();

    if (global_pool != null) return;

    pool_allocator = std.heap.c_allocator;
    const pool = try pool_allocator.create(ThreadPool);
    pool.* = ThreadPool.init(pool_allocator);
    pool.start(config);
    global_pool = pool;
}

/// Shutdown the global thread pool.
pub fn deinit() void {
    pool_mutex.lock();
    defer pool_mutex.unlock();

    if (global_pool) |pool| {
        pool.deinit();
        pool_allocator.destroy(pool);
        global_pool = null;
    }
}

/// Check if the pool is initialized.
pub fn isInitialized() bool {
    pool_mutex.lock();
    defer pool_mutex.unlock();
    return global_pool != null;
}

/// Get the number of worker threads.
pub fn numWorkers() u32 {
    pool_mutex.lock();
    defer pool_mutex.unlock();

    if (global_pool) |pool| {
        return @intCast(pool.numWorkers());
    }
    return 1;
}

/// Get the global pool, auto-initializing if needed.
fn getPool() ?*ThreadPool {
    pool_mutex.lock();
    defer pool_mutex.unlock();

    if (global_pool) |pool| {
        return pool;
    }

    // Auto-initialize
    pool_allocator = std.heap.c_allocator;
    const pool = pool_allocator.create(ThreadPool) catch return null;
    pool.* = ThreadPool.init(pool_allocator);
    pool.start(.{});
    global_pool = pool;
    return pool;
}

// ============================================================================
// join() - Fork-Join with Return Values
// ============================================================================

/// Execute two tasks potentially in parallel, returning both results.
///
/// The second task is pushed to the local queue where other workers can steal it,
/// while the first task is executed immediately. This enables recursive
/// divide-and-conquer algorithms.
///
/// Returns a tuple with results from both functions.
pub fn join(
    comptime RA: type,
    comptime RB: type,
    comptime fn_a: anytype,
    comptime fn_b: anytype,
    arg_a: anytype,
    arg_b: anytype,
) struct { RA, RB } {
    const ArgA = @TypeOf(arg_a);
    const ArgB = @TypeOf(arg_b);

    // Wrapper functions with correct signature
    const wrapper_a = struct {
        fn call(task: *Task, arg: ArgA) RA {
            _ = task;
            return @call(.auto, fn_a, .{arg});
        }
    }.call;

    const wrapper_b = struct {
        fn call(task: *Task, arg: ArgB) RB {
            _ = task;
            return @call(.auto, fn_b, .{arg});
        }
    }.call;

    // Fast path: if we're already inside a pool context, use fork/join directly
    if (current_task) |task| {
        var future_b = Future(ArgB, RB).init();
        future_b.fork(task, wrapper_b, arg_b);

        const result_a = task.call(RA, wrapper_a, arg_a);
        const result_b = future_b.join(task) orelse wrapper_b(task, arg_b);

        return .{ result_a, result_b };
    }

    // Get pool or fallback to sequential
    const pool = getPool() orelse {
        const result_a = fn_a(arg_a);
        const result_b = fn_b(arg_b);
        return .{ result_a, result_b };
    };

    // Slow path: create a new worker context via pool.call()
    return pool.call(struct { RA, RB }, struct {
        fn compute(task: *Task, args: struct { ArgA, ArgB }) struct { RA, RB } {
            // Set thread-local context for nested calls
            const prev_task = current_task;
            current_task = task;
            defer current_task = prev_task;

            var future_b = Future(ArgB, RB).init();
            future_b.fork(task, wrapper_b, args[1]);

            const result_a = task.call(RA, wrapper_a, args[0]);
            const result_b = future_b.join(task) orelse wrapper_b(task, args[1]);

            return .{ result_a, result_b };
        }
    }.compute, .{ arg_a, arg_b });
}

/// Execute two void tasks potentially in parallel.
pub fn joinVoid(
    comptime fn_a: anytype,
    comptime fn_b: anytype,
    arg_a: anytype,
    arg_b: anytype,
) void {
    _ = join(void, void, fn_a, fn_b, arg_a, arg_b);
}

// ============================================================================
// parallelFor() - Parallel Iteration
// ============================================================================

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
    parallelForWithGrain(n, Context, context, body_fn, getGrainSize());
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
        body_fn(context, 0, n);
        return;
    }

    // Fast path: if we're already inside a pool context, use fork/join directly
    if (current_task) |task| {
        parallelForImpl(Context, context, body_fn, 0, n, grain_size, task);
        return;
    }

    const pool = getPool() orelse {
        body_fn(context, 0, n);
        return;
    };

    // Slow path: create a new worker context via pool.call()
    _ = pool.call(void, struct {
        fn compute(task: *Task, args: struct { Context, usize, usize, usize }) void {
            // Set thread-local context for nested calls
            const prev_task = current_task;
            current_task = task;
            defer current_task = prev_task;

            const ctx = args[0];
            const start = args[1];
            const end = args[2];
            const grain = args[3];
            parallelForImpl(Context, ctx, body_fn, start, end, grain, task);
        }
    }.compute, .{ context, 0, n, grain_size });
}

fn parallelForImpl(
    comptime Context: type,
    context: Context,
    comptime body_fn: fn (Context, usize, usize) void,
    start: usize,
    end: usize,
    grain_size: usize,
    task: *Task,
) void {
    const len = end - start;

    if (len <= grain_size) {
        body_fn(context, start, end);
        return;
    }

    const mid = start + len / 2;

    const RightArgs = struct { Context, usize, usize, usize, *Task };

    var future_right = Future(RightArgs, void).init();
    future_right.fork(task, struct {
        fn call(t: *Task, args: RightArgs) void {
            parallelForImpl(Context, args[0], body_fn, args[1], args[2], args[3], t);
        }
    }.call, .{ context, mid, end, grain_size, task });

    // Execute left half directly
    parallelForImpl(Context, context, body_fn, start, mid, grain_size, task);

    // Join right half
    _ = future_right.join(task) orelse {
        parallelForImpl(Context, context, body_fn, mid, end, grain_size, task);
    };
}

// ============================================================================
// parallelReduce() - Parallel Map-Reduce
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
    return parallelReduceWithGrain(T, n, identity, Context, context, map_fn, combine_fn, getGrainSize());
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
        var result = identity;
        for (0..n) |i| {
            result = combine_fn(result, map_fn(context, i));
        }
        return result;
    }

    // Fast path: if we're already inside a pool context, use fork/join directly
    if (current_task) |task| {
        return parallelReduceImpl(T, Context, context, map_fn, combine_fn, identity, 0, n, grain_size, task);
    }

    const pool = getPool() orelse {
        var result = identity;
        for (0..n) |i| {
            result = combine_fn(result, map_fn(context, i));
        }
        return result;
    };

    // Slow path: create a new worker context via pool.call()
    return pool.call(T, struct {
        fn compute(task: *Task, args: struct { Context, usize, usize, T, usize }) T {
            // Set thread-local context for nested calls
            const prev_task = current_task;
            current_task = task;
            defer current_task = prev_task;

            const ctx = args[0];
            const start = args[1];
            const end = args[2];
            const id = args[3];
            const grain = args[4];
            return parallelReduceImpl(T, Context, ctx, map_fn, combine_fn, id, start, end, grain, task);
        }
    }.compute, .{ context, 0, n, identity, grain_size });
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
    task: *Task,
) T {
    const len = end - start;

    if (len <= grain_size) {
        var result = identity;
        for (start..end) |i| {
            result = combine_fn(result, map_fn(context, i));
        }
        return result;
    }

    const mid = start + len / 2;

    const Args = struct { Context, usize, usize, T, usize, *Task };

    var future_right = Future(Args, T).init();
    future_right.fork(task, struct {
        fn call(t: *Task, args: Args) T {
            return parallelReduceImpl(T, Context, args[0], map_fn, combine_fn, args[3], args[1], args[2], args[4], t);
        }
    }.call, .{ context, mid, end, identity, grain_size, task });

    // Compute left half directly
    const left_result = parallelReduceImpl(T, Context, context, map_fn, combine_fn, identity, start, mid, grain_size, task);

    // Join right half
    const right_result = future_right.join(task) orelse
        parallelReduceImpl(T, Context, context, map_fn, combine_fn, identity, mid, end, grain_size, task);

    return combine_fn(left_result, right_result);
}

// ============================================================================
// parallelReduceChunked() - Range-based Parallel Reduction (for SIMD)
// ============================================================================

/// Parallel reduction using a chunk-based map function.
/// This is optimized for SIMD operations where the map function processes
/// a range of elements at once rather than one element at a time.
///
/// The map_fn signature is: `fn(Context, start: usize, end: usize) T`
/// This allows SIMD-optimized implementations within each chunk.
pub fn parallelReduceChunked(
    comptime T: type,
    n: usize,
    identity: T,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, usize, usize) T,
    comptime combine_fn: fn (T, T) T,
    grain_size: usize,
) T {
    if (n == 0) return identity;

    if (n <= grain_size or !isInitialized()) {
        return map_fn(context, 0, n);
    }

    // Fast path: if we're already inside a pool context, use fork/join directly
    if (current_task) |task| {
        return parallelReduceChunkedImpl(T, Context, context, map_fn, combine_fn, identity, 0, n, grain_size, task);
    }

    const pool = getPool() orelse {
        return map_fn(context, 0, n);
    };

    return pool.call(T, struct {
        fn compute(task: *Task, args: struct { Context, usize, usize, T, usize }) T {
            const prev_task = current_task;
            current_task = task;
            defer current_task = prev_task;

            const ctx = args[0];
            const start = args[1];
            const end = args[2];
            const id = args[3];
            const grain = args[4];
            return parallelReduceChunkedImpl(T, Context, ctx, map_fn, combine_fn, id, start, end, grain, task);
        }
    }.compute, .{ context, 0, n, identity, grain_size });
}

fn parallelReduceChunkedImpl(
    comptime T: type,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, usize, usize) T,
    comptime combine_fn: fn (T, T) T,
    identity: T,
    start: usize,
    end: usize,
    grain_size: usize,
    task: *Task,
) T {
    const len = end - start;

    // Base case: use the chunked map function (SIMD-optimized)
    if (len <= grain_size) {
        return map_fn(context, start, end);
    }

    const mid = start + len / 2;

    const Args = struct { Context, usize, usize, T, usize, *Task };

    var future_right = Future(Args, T).init();
    future_right.fork(task, struct {
        fn call(t: *Task, args: Args) T {
            return parallelReduceChunkedImpl(T, Context, args[0], map_fn, combine_fn, args[3], args[1], args[2], args[4], t);
        }
    }.call, .{ context, mid, end, identity, grain_size, task });

    // Compute left half directly
    const left_result = parallelReduceChunkedImpl(T, Context, context, map_fn, combine_fn, identity, start, mid, grain_size, task);

    // Join right half
    const right_result = future_right.join(task) orelse
        parallelReduceChunkedImpl(T, Context, context, map_fn, combine_fn, identity, mid, end, grain_size, task);

    return combine_fn(left_result, right_result);
}

// ============================================================================
// parallelCollect() - Parallel Map with Result Collection
// ============================================================================

/// Parallel map that collects results into an output slice.
///
/// Maps each element of `input` through `map_fn` and stores the result
/// in `output`. Work is divided and stolen using the standard fork-join pattern.
///
/// This is equivalent to Rayon's `.par_iter().map().collect()`.
///
/// Requirements:
/// - `output.len` must equal `input.len`
/// - `map_fn` signature: `fn(Context, T) U`
pub fn parallelCollect(
    comptime T: type,
    comptime U: type,
    input: []const T,
    output: []U,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, T) U,
) void {
    parallelCollectWithGrain(T, U, input, output, Context, context, map_fn, getGrainSize());
}

/// Parallel map with custom grain size.
pub fn parallelCollectWithGrain(
    comptime T: type,
    comptime U: type,
    input: []const T,
    output: []U,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, T) U,
    grain_size: usize,
) void {
    std.debug.assert(input.len == output.len);

    if (input.len == 0) return;

    if (input.len <= grain_size or !isInitialized()) {
        for (input, output) |in_val, *out_val| {
            out_val.* = map_fn(context, in_val);
        }
        return;
    }

    const CollectContext = struct {
        input: []const T,
        output: []U,
        ctx: Context,
    };

    const collect_ctx = CollectContext{
        .input = input,
        .output = output,
        .ctx = context,
    };

    parallelForWithGrain(input.len, CollectContext, collect_ctx, struct {
        fn body(c: CollectContext, start: usize, end: usize) void {
            for (start..end) |i| {
                c.output[i] = map_fn(c.ctx, c.input[i]);
            }
        }
    }.body, grain_size);
}

/// Parallel map in-place: transform elements without allocating new storage.
pub fn parallelMapInPlace(
    comptime T: type,
    data: []T,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, T) T,
) void {
    parallelMapInPlaceWithGrain(T, data, Context, context, map_fn, getGrainSize());
}

/// Parallel map in-place with custom grain size.
pub fn parallelMapInPlaceWithGrain(
    comptime T: type,
    data: []T,
    comptime Context: type,
    context: Context,
    comptime map_fn: fn (Context, T) T,
    grain_size: usize,
) void {
    if (data.len == 0) return;

    if (data.len <= grain_size or !isInitialized()) {
        for (data) |*val| {
            val.* = map_fn(context, val.*);
        }
        return;
    }

    const MapContext = struct { data: []T, ctx: Context };
    const map_ctx = MapContext{ .data = data, .ctx = context };

    parallelForWithGrain(data.len, MapContext, map_ctx, struct {
        fn body(c: MapContext, start: usize, end: usize) void {
            for (c.data[start..end]) |*val| {
                val.* = map_fn(c.ctx, val.*);
            }
        }
    }.body, grain_size);
}

// ============================================================================
// parallelFlatten() - Parallel Flatten Nested Slices
// ============================================================================

/// Flatten nested slices into a single output slice in parallel.
///
/// This is the pattern used in Polars' `flatten_par` for combining
/// thread-local results into a single output array.
///
/// The algorithm:
/// 1. Compute offsets for each input slice (where it starts in output)
/// 2. Copy each slice to its designated output region in parallel
///
/// Requirements:
/// - `output.len` must equal the sum of all input slice lengths
/// - Pre-compute total length using `capAndOffsets` if needed
pub fn parallelFlatten(
    comptime T: type,
    slices: []const []const T,
    output: []T,
) void {
    parallelFlattenWithGrain(T, slices, output, 1); // Grain=1 since each slice is independent work
}

/// Parallel flatten with custom grain size (number of slices per task).
pub fn parallelFlattenWithGrain(
    comptime T: type,
    slices: []const []const T,
    output: []T,
    grain_size: usize,
) void {
    if (slices.len == 0) return;

    // Compute offsets
    var offsets_buf: [1024]usize = undefined;
    const offsets = if (slices.len <= 1024)
        offsets_buf[0..slices.len]
    else blk: {
        // For very large slice counts, allocate (rare case)
        const allocator = std.heap.c_allocator;
        break :blk allocator.alloc(usize, slices.len) catch @panic("OOM");
    };
    defer if (slices.len > 1024) std.heap.c_allocator.free(offsets);

    const total = capAndOffsets(T, slices, offsets);
    std.debug.assert(output.len == total);

    if (slices.len <= grain_size or !isInitialized()) {
        // Sequential path
        for (slices, offsets) |slice, offset| {
            @memcpy(output[offset..][0..slice.len], slice);
        }
        return;
    }

    // Parallel path: each task copies its assigned slices
    const out_ptr = SyncPtr(T).init(output);

    const FlattenContext = struct {
        slices: []const []const T,
        offsets: []const usize,
        out_ptr: SyncPtr(T),
    };

    const flatten_ctx = FlattenContext{
        .slices = slices,
        .offsets = offsets,
        .out_ptr = out_ptr,
    };

    parallelForWithGrain(slices.len, FlattenContext, flatten_ctx, struct {
        fn body(c: FlattenContext, start: usize, end: usize) void {
            for (start..end) |i| {
                const slice = c.slices[i];
                const offset = c.offsets[i];
                c.out_ptr.copyAt(offset, slice);
            }
        }
    }.body, grain_size);
}

/// Flatten with pre-computed offsets (avoids recomputing).
/// Use this when you've already called `capAndOffsets` to get the total size.
pub fn parallelFlattenWithOffsets(
    comptime T: type,
    slices: []const []const T,
    offsets: []const usize,
    output: []T,
) void {
    if (slices.len == 0) return;

    std.debug.assert(offsets.len >= slices.len);

    if (slices.len <= 1 or !isInitialized()) {
        for (slices, offsets[0..slices.len]) |slice, offset| {
            @memcpy(output[offset..][0..slice.len], slice);
        }
        return;
    }

    const out_ptr = SyncPtr(T).init(output);

    const FlattenContext = struct {
        slices: []const []const T,
        offsets: []const usize,
        out_ptr: SyncPtr(T),
    };

    const flatten_ctx = FlattenContext{
        .slices = slices,
        .offsets = offsets,
        .out_ptr = out_ptr,
    };

    // Use grain size of 1 - each slice is independent
    parallelForWithGrain(slices.len, FlattenContext, flatten_ctx, struct {
        fn body(c: FlattenContext, start: usize, end: usize) void {
            for (start..end) |i| {
                const slice = c.slices[i];
                const offset = c.offsets[i];
                c.out_ptr.copyAt(offset, slice);
            }
        }
    }.body, 1);
}

// ============================================================================
// parallelScatter() - Parallel Scatter with Pre-computed Offsets
// ============================================================================

/// Scatter values to output using pre-computed offsets.
///
/// This is the pattern used in Polars' BUILD phase where each thread
/// scatters its partition to pre-computed locations in the output.
///
/// Each element `values[i]` is written to `output[offsets[i]]`.
/// After completion, `offsets[i]` is incremented by 1 (for multi-value scatter).
pub fn parallelScatter(
    comptime T: type,
    values: []const T,
    indices: []const usize,
    output: []T,
) void {
    std.debug.assert(values.len == indices.len);

    if (values.len == 0) return;

    if (values.len <= getGrainSize() or !isInitialized()) {
        for (values, indices) |val, idx| {
            output[idx] = val;
        }
        return;
    }

    const out_ptr = SyncPtr(T).init(output);

    const ScatterContext = struct {
        values: []const T,
        indices: []const usize,
        out_ptr: SyncPtr(T),
    };

    const scatter_ctx = ScatterContext{
        .values = values,
        .indices = indices,
        .out_ptr = out_ptr,
    };

    parallelFor(values.len, ScatterContext, scatter_ctx, struct {
        fn body(c: ScatterContext, start: usize, end: usize) void {
            for (start..end) |i| {
                c.out_ptr.writeAt(c.indices[i], c.values[i]);
            }
        }
    }.body);
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

    const expected: f64 = 10000.0 * 9999.0 / 2.0;

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

test "parallelCollect - basic" {
    var input: [100]i32 = undefined;
    var output: [100]i64 = undefined;

    for (&input, 0..) |*v, i| {
        v.* = @intCast(i);
    }

    parallelCollect(i32, i64, &input, &output, void, {}, struct {
        fn map(_: void, x: i32) i64 {
            return @as(i64, x) * 2;
        }
    }.map);

    for (output, 0..) |v, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i * 2)), v);
    }
}

test "parallelCollect - empty" {
    var input: [0]i32 = undefined;
    var output: [0]i64 = undefined;

    parallelCollect(i32, i64, &input, &output, void, {}, struct {
        fn map(_: void, x: i32) i64 {
            return x;
        }
    }.map);
}

test "parallelMapInPlace - basic" {
    var data: [100]i32 = undefined;
    for (&data, 0..) |*v, i| {
        v.* = @intCast(i);
    }

    parallelMapInPlace(i32, &data, void, {}, struct {
        fn map(_: void, x: i32) i32 {
            return x * 3;
        }
    }.map);

    for (data, 0..) |v, i| {
        try std.testing.expectEqual(@as(i32, @intCast(i * 3)), v);
    }
}

test "parallelFlatten - basic" {
    const slice0 = [_]u32{ 1, 2, 3 };
    const slice1 = [_]u32{ 4, 5 };
    const slice2 = [_]u32{ 6, 7, 8, 9 };

    const slices = [_][]const u32{ &slice0, &slice1, &slice2 };
    var output: [9]u32 = undefined;

    parallelFlatten(u32, &slices, &output);

    const expected = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    try std.testing.expectEqualSlices(u32, &expected, &output);
}

test "parallelFlatten - empty slices" {
    const slice0 = [_]u32{ 1, 2 };
    const slice1 = [_]u32{};
    const slice2 = [_]u32{ 3 };

    const slices = [_][]const u32{ &slice0, &slice1, &slice2 };
    var output: [3]u32 = undefined;

    parallelFlatten(u32, &slices, &output);

    const expected = [_]u32{ 1, 2, 3 };
    try std.testing.expectEqualSlices(u32, &expected, &output);
}

test "parallelFlattenWithOffsets - basic" {
    const slice0 = [_]u64{ 10, 20 };
    const slice1 = [_]u64{ 30, 40, 50 };

    const slices = [_][]const u64{ &slice0, &slice1 };
    var offsets: [2]usize = undefined;
    const total = capAndOffsets(u64, &slices, &offsets);

    try std.testing.expectEqual(@as(usize, 5), total);
    try std.testing.expectEqual(@as(usize, 0), offsets[0]);
    try std.testing.expectEqual(@as(usize, 2), offsets[1]);

    var output: [5]u64 = undefined;
    parallelFlattenWithOffsets(u64, &slices, &offsets, &output);

    const expected = [_]u64{ 10, 20, 30, 40, 50 };
    try std.testing.expectEqualSlices(u64, &expected, &output);
}

test "parallelScatter - basic" {
    const values = [_]u32{ 100, 200, 300, 400 };
    const indices = [_]usize{ 3, 0, 7, 1 };
    var output: [10]u32 = undefined;
    @memset(&output, 0);

    parallelScatter(u32, &values, &indices, &output);

    try std.testing.expectEqual(@as(u32, 200), output[0]);
    try std.testing.expectEqual(@as(u32, 400), output[1]);
    try std.testing.expectEqual(@as(u32, 0), output[2]);
    try std.testing.expectEqual(@as(u32, 100), output[3]);
    try std.testing.expectEqual(@as(u32, 300), output[7]);
}

test "SyncPtr - parallel write simulation" {
    var buffer: [100]u64 = undefined;
    @memset(&buffer, 0);

    const ptr = SyncPtr(u64).init(&buffer);

    // Simulate what parallel threads would do
    // Thread 0: write to [0..25)
    for (0..25) |i| {
        ptr.writeAt(i, @intCast(i * 10));
    }

    // Thread 1: write to [25..50)
    for (25..50) |i| {
        ptr.writeAt(i, @intCast(i * 10));
    }

    // Thread 2: write to [50..75)
    for (50..75) |i| {
        ptr.writeAt(i, @intCast(i * 10));
    }

    // Thread 3: write to [75..100)
    for (75..100) |i| {
        ptr.writeAt(i, @intCast(i * 10));
    }

    // Verify all writes
    for (buffer, 0..) |v, i| {
        try std.testing.expectEqual(@as(u64, i * 10), v);
    }
}
