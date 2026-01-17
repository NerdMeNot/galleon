//! Blitz - Heartbeat-Based Work-Stealing Parallel Execution Library
//!
//! A high-performance parallel execution library using heartbeat scheduling.
//! Achieves ~10ns per-join overhead compared to ~500-1000ns with traditional
//! Chase-Lev work-stealing.
//!
//! Key Features:
//! - Heartbeat scheduling: decouples coordination from execution
//! - Branch-free local job queues (no atomics for push/pop)
//! - Future(Input, Output) with comptime specialization
//! - Zero-allocation join() for recursive divide-and-conquer
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

// Core types
pub const Job = @import("job.zig").Job;
pub const JobExecuteState = @import("job.zig").JobExecuteState;

pub const OnceLatch = @import("latch.zig").OnceLatch;
pub const CountLatch = @import("latch.zig").CountLatch;
pub const SpinWait = @import("latch.zig").SpinWait;

pub const Worker = @import("worker.zig").Worker;
pub const Task = @import("worker.zig").Task;

pub const ThreadPool = @import("pool.zig").ThreadPool;
pub const ThreadPoolConfig = @import("pool.zig").ThreadPoolConfig;

pub const Future = @import("future.zig").Future;

// API functions
const api = @import("api.zig");

pub const init = api.init;
pub const initWithConfig = api.initWithConfig;
pub const deinit = api.deinit;
pub const isInitialized = api.isInitialized;
pub const numWorkers = api.numWorkers;

pub const join = api.join;
pub const joinVoid = api.joinVoid;

pub const parallelFor = api.parallelFor;
pub const parallelForWithGrain = api.parallelForWithGrain;

pub const parallelReduce = api.parallelReduce;
pub const parallelReduceWithGrain = api.parallelReduceWithGrain;

// Threshold module for intelligent parallelism decisions
pub const threshold = @import("threshold.zig");
pub const OpType = threshold.OpType;
pub const shouldParallelize = threshold.shouldParallelize;
pub const isMemoryBound = threshold.isMemoryBound;

// ============================================================================
// Tests
// ============================================================================

test "blitz - all modules compile" {
    _ = @import("job.zig");
    _ = @import("latch.zig");
    _ = @import("future.zig");
    _ = @import("worker.zig");
    _ = @import("pool.zig");
    _ = @import("api.zig");
}

test "blitz - basic parallel for" {
    var results: [100]u32 = undefined;
    @memset(&results, 0);

    const Context = struct { results: []u32 };
    const ctx = Context{ .results = &results };

    parallelFor(100, Context, ctx, struct {
        fn body(c: Context, start: usize, end: usize) void {
            for (start..end) |i| {
                c.results[i] = @intCast(i * 2);
            }
        }
    }.body);

    for (results, 0..) |v, i| {
        try std.testing.expectEqual(@as(u32, @intCast(i * 2)), v);
    }
}

test "blitz - basic parallel reduce" {
    const n: usize = 100;
    const expected: i64 = @as(i64, @intCast(n * (n - 1) / 2));

    const result = parallelReduce(
        i64,
        n,
        0,
        void,
        {},
        struct {
            fn map(_: void, i: usize) i64 {
                return @intCast(i);
            }
        }.map,
        struct {
            fn combine(a: i64, b: i64) i64 {
                return a + b;
            }
        }.combine,
    );

    try std.testing.expectEqual(expected, result);
}
