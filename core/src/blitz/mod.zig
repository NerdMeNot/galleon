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

// Runtime configuration
pub const getGrainSize = api.getGrainSize;
pub const setGrainSize = api.setGrainSize;
pub const defaultGrainSize = api.defaultGrainSize;

pub const join = api.join;
pub const joinVoid = api.joinVoid;

pub const parallelFor = api.parallelFor;
pub const parallelForWithGrain = api.parallelForWithGrain;

pub const parallelReduce = api.parallelReduce;
pub const parallelReduceWithGrain = api.parallelReduceWithGrain;
pub const parallelReduceChunked = api.parallelReduceChunked;

// New Polars-parity functions
pub const parallelCollect = api.parallelCollect;
pub const parallelCollectWithGrain = api.parallelCollectWithGrain;
pub const parallelMapInPlace = api.parallelMapInPlace;
pub const parallelMapInPlaceWithGrain = api.parallelMapInPlaceWithGrain;

pub const parallelFlatten = api.parallelFlatten;
pub const parallelFlattenWithGrain = api.parallelFlattenWithGrain;
pub const parallelFlattenWithOffsets = api.parallelFlattenWithOffsets;

pub const parallelScatter = api.parallelScatter;

// Sync primitives for lock-free parallel writes
pub const SyncPtr = api.SyncPtr;
pub const computeOffsetsInto = api.computeOffsetsInto;
pub const capAndOffsets = api.capAndOffsets;

// Also export from sync module directly
const sync = @import("sync.zig");

// Threshold module for intelligent parallelism decisions
pub const threshold = @import("threshold.zig");
pub const OpType = threshold.OpType;
pub const shouldParallelize = threshold.shouldParallelize;
pub const isMemoryBound = threshold.isMemoryBound;

// Worker count alias (for internal use)
pub const getWorkerCount = numWorkers;

// ============================================================================
// New Rayon-Parity Features
// ============================================================================

// Parallel iterators (Rayon-style composable iterators)
pub const iter_mod = @import("iter.zig");
pub const iter = iter_mod.iter;
pub const iterMut = iter_mod.iterMut;
pub const range = iter_mod.range;
pub const ParIter = iter_mod.ParIter;
pub const ParIterMut = iter_mod.ParIterMut;
pub const RangeIter = iter_mod.RangeIter;

// Scope-based parallelism (spawn arbitrary tasks)
pub const scope_mod = @import("scope.zig");
pub const scope = scope_mod.scope;
pub const scopeWithContext = scope_mod.scopeWithContext;
pub const Scope = scope_mod.Scope;
pub const join2 = scope_mod.join2;
pub const join3 = scope_mod.join3;
pub const joinN = scope_mod.joinN;
pub const parallelForRange = scope_mod.parallelForRange;
pub const parallelForRangeWithContext = scope_mod.parallelForRangeWithContext;

// Parallel algorithms
pub const algorithms = @import("algorithms.zig");
pub const parallelSort = algorithms.parallelSort;
pub const parallelSortBy = algorithms.parallelSortBy;
pub const parallelScan = algorithms.parallelScan;
pub const parallelScanExclusive = algorithms.parallelScanExclusive;
pub const parallelFind = algorithms.parallelFind;
pub const parallelFindValue = algorithms.parallelFindValue;
pub const parallelPartition = algorithms.parallelPartition;

// SIMD-optimized aggregations (parallel + vectorized)
pub const simd_mod = @import("simd.zig");
pub const simdSum = simd_mod.sum;
pub const simdMin = simd_mod.min;
pub const simdMax = simd_mod.max;
pub const parallelSumSimd = simd_mod.parallelSum;
pub const parallelMinSimd = simd_mod.parallelMin;
pub const parallelMaxSimd = simd_mod.parallelMax;

// SIMD parallel threshold (dynamic calculation based on worker count and operation cost)
pub const calculateParallelThreshold = simd_mod.calculateParallelThreshold;
pub const shouldParallelizeSimd = simd_mod.shouldParallelizeSimd;
pub const getParallelThreshold = simd_mod.getParallelThreshold;

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
    _ = @import("sync.zig");
    _ = @import("threshold.zig");
    _ = @import("iter.zig");
    _ = @import("scope.zig");
    _ = @import("algorithms.zig");
    _ = @import("simd.zig");
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
