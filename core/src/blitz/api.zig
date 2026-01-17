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

/// Default grain size - minimum work per task.
/// Below this, we don't parallelize to avoid overhead.
const DEFAULT_GRAIN_SIZE: usize = 4096;

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
