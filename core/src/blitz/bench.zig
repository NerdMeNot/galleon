//! Blitz Parallel Execution Benchmarks
//!
//! Measures overhead and throughput for common parallel patterns.
//!
//! Run: zig build-exe bench.zig -O ReleaseFast && ./bench

const std = @import("std");
const blitz = @import("mod.zig");

// ============================================================================
// Configuration
// ============================================================================

const WARMUP_ITERATIONS = 3;
const BENCH_ITERATIONS = 10;

const SIZES = [_]usize{
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
};

// ============================================================================
// Timer Utilities
// ============================================================================

fn now() i128 {
    return std.time.nanoTimestamp();
}

fn elapsedMs(start: i128) f64 {
    return @as(f64, @floatFromInt(now() - start)) / 1_000_000.0;
}

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize blitz first
    try blitz.init();
    defer blitz.deinit();

    std.debug.print("\n", .{});
    std.debug.print("{s}\n", .{"=" ** 70});
    std.debug.print("                 Blitz Parallel Execution Benchmarks\n", .{});
    std.debug.print("{s}\n", .{"=" ** 70});
    std.debug.print("Iterations: {d} (+ {d} warmup)\n", .{ BENCH_ITERATIONS, WARMUP_ITERATIONS });
    std.debug.print("Workers: {d}\n\n", .{blitz.numWorkers()});

    try benchJoinOverhead();
    std.debug.print("\n", .{});

    for (SIZES) |size| {
        std.debug.print("{s}\n", .{"=" ** 70});
        std.debug.print("N = {d}\n", .{size});
        std.debug.print("{s}\n", .{"=" ** 70});

        try benchSum(allocator, size);
        try benchMax(allocator, size);
        try benchFor(allocator, size);

        if (size <= 1_000_000) {
            try benchSort(allocator, size);
        }

        if (size <= 1_000_000) {
            try benchScan(allocator, size);
        }

        std.debug.print("\n", .{});
    }

    std.debug.print("{s}\n", .{"=" ** 70});
    std.debug.print("Benchmark complete.\n", .{});
    std.debug.print("{s}\n", .{"=" ** 70});
}

// ============================================================================
// Join Overhead
// ============================================================================

fn benchJoinOverhead() !void {
    std.debug.print("=== Join Overhead ===\n", .{});

    const iterations: usize = 100_000;
    var count: usize = 0;

    // Warmup
    for (0..1000) |_| {
        const result = blitz.join(
            usize,
            usize,
            struct {
                fn a(_: void) usize {
                    return 1;
                }
            }.a,
            struct {
                fn b(_: void) usize {
                    return 2;
                }
            }.b,
            {},
            {},
        );
        count += result[0] + result[1];
    }

    // Benchmark
    count = 0;
    const start = now();
    for (0..iterations) |_| {
        const result = blitz.join(
            usize,
            usize,
            struct {
                fn a(_: void) usize {
                    return 1;
                }
            }.a,
            struct {
                fn b(_: void) usize {
                    return 2;
                }
            }.b,
            {},
            {},
        );
        count += result[0] + result[1];
    }
    const total_ns = @as(f64, @floatFromInt(now() - start));
    const avg_ns = total_ns / @as(f64, @floatFromInt(iterations));

    std.debug.print("Empty join: {d:.1}ns avg (count={d})\n", .{ avg_ns, count });
}

// ============================================================================
// Parallel Sum (SIMD + Parallel)
// ============================================================================

fn benchSum(allocator: std.mem.Allocator, size: usize) !void {
    const data = try allocator.alloc(i64, size);
    defer allocator.free(data);

    // Initialize
    for (data, 0..) |*v, i| {
        v.* = @intCast(i % 1000);
    }

    var seq_sum: i64 = 0;
    var par_sum: i64 = 0;

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        seq_sum = 0;
        for (data) |v| seq_sum += v;
        std.mem.doNotOptimizeAway(seq_sum);
        par_sum = blitz.iter(i64, data).sum();
        std.mem.doNotOptimizeAway(par_sum);
    }

    // Sequential
    const seq_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        seq_sum = 0;
        for (data) |v| seq_sum += v;
        std.mem.doNotOptimizeAway(seq_sum);
    }
    const seq_ms = elapsedMs(seq_start) / @as(f64, BENCH_ITERATIONS);

    // Parallel
    const par_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        par_sum = blitz.iter(i64, data).sum();
        std.mem.doNotOptimizeAway(par_sum);
    }
    const par_ms = elapsedMs(par_start) / @as(f64, BENCH_ITERATIONS);

    const speedup = if (par_ms > 0) seq_ms / par_ms else 0;
    std.debug.print("  Sum:   seq {d:>8.3}ms   par {d:>8.3}ms   {d:>5.2}x", .{ seq_ms, par_ms, speedup });

    if (seq_sum != par_sum) {
        std.debug.print("  ERROR: mismatch!\n", .{});
    } else {
        std.debug.print("\n", .{});
    }
}

// ============================================================================
// Parallel Max (SIMD + Parallel)
// ============================================================================

fn benchMax(allocator: std.mem.Allocator, size: usize) !void {
    const data = try allocator.alloc(i64, size);
    defer allocator.free(data);

    // Initialize
    for (data, 0..) |*v, i| {
        v.* = @intCast(i % 1000);
    }

    var seq_max: i64 = 0;
    var par_max: i64 = 0;

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        seq_max = std.math.minInt(i64);
        for (data) |v| if (v > seq_max) {
            seq_max = v;
        };
        std.mem.doNotOptimizeAway(seq_max);
        par_max = blitz.iter(i64, data).max() orelse 0;
        std.mem.doNotOptimizeAway(par_max);
    }

    // Sequential
    const seq_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        seq_max = std.math.minInt(i64);
        for (data) |v| if (v > seq_max) {
            seq_max = v;
        };
        std.mem.doNotOptimizeAway(seq_max);
    }
    const seq_ms = elapsedMs(seq_start) / @as(f64, BENCH_ITERATIONS);

    // Parallel
    const par_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        par_max = blitz.iter(i64, data).max() orelse 0;
        std.mem.doNotOptimizeAway(par_max);
    }
    const par_ms = elapsedMs(par_start) / @as(f64, BENCH_ITERATIONS);

    const speedup = if (par_ms > 0) seq_ms / par_ms else 0;
    std.debug.print("  Max:   seq {d:>8.3}ms   par {d:>8.3}ms   {d:>5.2}x\n", .{ seq_ms, par_ms, speedup });
}

// ============================================================================
// Parallel For (Write Indices)
// ============================================================================

fn benchFor(allocator: std.mem.Allocator, size: usize) !void {
    const data = try allocator.alloc(u64, size);
    defer allocator.free(data);

    const ForCtx = struct {
        slice: []u64,
        const Self = @This();
        pub fn body(ctx: Self, start: usize, end: usize) void {
            for (start..end) |i| {
                ctx.slice[i] = i * 2;
            }
        }
    };

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        for (data, 0..) |*v, i| v.* = i * 2;
        std.mem.doNotOptimizeAway(data.ptr);
        blitz.parallelFor(size, ForCtx, ForCtx{ .slice = data }, ForCtx.body);
        std.mem.doNotOptimizeAway(data.ptr);
    }

    // Sequential
    const seq_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        for (data, 0..) |*v, i| v.* = i * 2;
        std.mem.doNotOptimizeAway(data.ptr);
    }
    const seq_ms = elapsedMs(seq_start) / @as(f64, BENCH_ITERATIONS);

    // Parallel
    const par_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        blitz.parallelFor(size, ForCtx, ForCtx{ .slice = data }, ForCtx.body);
        std.mem.doNotOptimizeAway(data.ptr);
    }
    const par_ms = elapsedMs(par_start) / @as(f64, BENCH_ITERATIONS);

    const speedup = if (par_ms > 0) seq_ms / par_ms else 0;
    std.debug.print("  For:   seq {d:>8.3}ms   par {d:>8.3}ms   {d:>5.2}x\n", .{ seq_ms, par_ms, speedup });
}

// ============================================================================
// Parallel Sort
// ============================================================================

fn benchSort(allocator: std.mem.Allocator, size: usize) !void {
    const data = try allocator.alloc(i64, size);
    defer allocator.free(data);
    const copy = try allocator.alloc(i64, size);
    defer allocator.free(copy);
    const original = try allocator.alloc(i64, size);
    defer allocator.free(original);

    // Initialize with random data
    var rng = std.Random.Xoroshiro128.init(54321);
    for (original) |*v| {
        v.* = @intCast(rng.random().int(u32));
    }

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        @memcpy(data, original);
        std.sort.pdq(i64, data, {}, std.sort.asc(i64));
        @memcpy(copy, original);
        blitz.parallelSort(i64, copy, allocator) catch {};
    }

    // Sequential
    const seq_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        @memcpy(data, original);
        std.sort.pdq(i64, data, {}, std.sort.asc(i64));
    }
    const seq_ms = elapsedMs(seq_start) / @as(f64, BENCH_ITERATIONS);

    // Parallel
    const par_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        @memcpy(copy, original);
        blitz.parallelSort(i64, copy, allocator) catch {};
    }
    const par_ms = elapsedMs(par_start) / @as(f64, BENCH_ITERATIONS);

    // Verify
    var sorted = true;
    for (1..copy.len) |i| {
        if (copy[i - 1] > copy[i]) {
            sorted = false;
            break;
        }
    }

    const speedup = if (par_ms > 0) seq_ms / par_ms else 0;
    std.debug.print("  Sort:  seq {d:>8.3}ms   par {d:>8.3}ms   {d:>5.2}x", .{ seq_ms, par_ms, speedup });
    if (!sorted) {
        std.debug.print("  ERROR: not sorted!\n", .{});
    } else {
        std.debug.print("\n", .{});
    }
}

// ============================================================================
// Parallel Scan (Prefix Sum)
// ============================================================================

fn benchScan(allocator: std.mem.Allocator, size: usize) !void {
    const input = try allocator.alloc(i64, size);
    defer allocator.free(input);
    const seq_output = try allocator.alloc(i64, size);
    defer allocator.free(seq_output);
    const par_output = try allocator.alloc(i64, size);
    defer allocator.free(par_output);

    // Initialize
    for (input, 0..) |*v, i| {
        v.* = @intCast(i % 100);
    }

    // Warmup
    for (0..WARMUP_ITERATIONS) |_| {
        seq_output[0] = input[0];
        for (1..input.len) |i| {
            seq_output[i] = seq_output[i - 1] + input[i];
        }
        blitz.parallelScan(i64, input, par_output);
    }

    // Sequential
    const seq_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        seq_output[0] = input[0];
        for (1..input.len) |i| {
            seq_output[i] = seq_output[i - 1] + input[i];
        }
        std.mem.doNotOptimizeAway(seq_output.ptr);
    }
    const seq_ms = elapsedMs(seq_start) / @as(f64, BENCH_ITERATIONS);

    // Parallel
    const par_start = now();
    for (0..BENCH_ITERATIONS) |_| {
        blitz.parallelScan(i64, input, par_output);
        std.mem.doNotOptimizeAway(par_output.ptr);
    }
    const par_ms = elapsedMs(par_start) / @as(f64, BENCH_ITERATIONS);

    // Verify
    const correct = seq_output[size - 1] == par_output[size - 1];

    const speedup = if (par_ms > 0) seq_ms / par_ms else 0;
    std.debug.print("  Scan:  seq {d:>8.3}ms   par {d:>8.3}ms   {d:>5.2}x", .{ seq_ms, par_ms, speedup });
    if (!correct) {
        std.debug.print("  ERROR: mismatch!\n", .{});
    } else {
        std.debug.print("\n", .{});
    }
}
