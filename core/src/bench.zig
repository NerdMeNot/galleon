const std = @import("std");
const simd = @import("simd.zig");

pub fn main() !void {
    const print = std.debug.print;

    print("=== Galleon Zig Core Benchmarks ===\n\n", .{});

    // Allocate test data
    const allocator = std.heap.page_allocator;
    const n: usize = 1_000_000;
    const iterations: usize = 100;

    const data = try allocator.alloc(f64, n);
    defer allocator.free(data);

    // Initialize with random-ish data
    for (data, 0..) |*v, i| {
        v.* = @as(f64, @floatFromInt(i % 1000)) + 0.5;
    }

    print("Dataset: {d} elements\n", .{n});
    print("Iterations: {d}\n\n", .{iterations});

    // Benchmark Sum
    {
        var timer = try std.time.Timer.start();
        var result: f64 = 0;
        for (0..iterations) |_| {
            result = simd.sum(f64, data);
            std.mem.doNotOptimizeAway(&result);
        }
        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        const avg_ms = @as(f64, @floatFromInt(avg_ns)) / 1_000_000.0;
        print("Sum:        {d:.3} ms (result: {d:.2})\n", .{ avg_ms, result });
    }

    // Benchmark Min
    {
        var timer = try std.time.Timer.start();
        var result: f64 = 0;
        for (0..iterations) |_| {
            result = simd.min(f64, data) orelse 0;
            std.mem.doNotOptimizeAway(&result);
        }
        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        const avg_ms = @as(f64, @floatFromInt(avg_ns)) / 1_000_000.0;
        print("Min:        {d:.3} ms (result: {d:.2})\n", .{ avg_ms, result });
    }

    // Benchmark Max
    {
        var timer = try std.time.Timer.start();
        var result: f64 = 0;
        for (0..iterations) |_| {
            result = simd.max(f64, data) orelse 0;
            std.mem.doNotOptimizeAway(&result);
        }
        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        const avg_ms = @as(f64, @floatFromInt(avg_ns)) / 1_000_000.0;
        print("Max:        {d:.3} ms (result: {d:.2})\n", .{ avg_ms, result });
    }

    // Benchmark Mean
    {
        var timer = try std.time.Timer.start();
        var result: f64 = 0;
        for (0..iterations) |_| {
            result = simd.mean(f64, data) orelse 0;
            std.mem.doNotOptimizeAway(&result);
        }
        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        const avg_ms = @as(f64, @floatFromInt(avg_ns)) / 1_000_000.0;
        print("Mean:       {d:.3} ms (result: {d:.2})\n", .{ avg_ms, result });
    }

    // Benchmark Filter
    {
        const indices = try allocator.alloc(u32, n);
        defer allocator.free(indices);

        var timer = try std.time.Timer.start();
        var count: usize = 0;
        for (0..iterations) |_| {
            count = simd.filterGreaterThan(f64, data, 500.0, indices);
            std.mem.doNotOptimizeAway(&count);
        }
        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        const avg_ms = @as(f64, @floatFromInt(avg_ns)) / 1_000_000.0;
        print("Filter>500: {d:.3} ms (matched: {d})\n", .{ avg_ms, count });
    }

    // Benchmark Argsort (fewer iterations - expensive)
    {
        const indices = try allocator.alloc(u32, n);
        defer allocator.free(indices);

        const sort_iterations: usize = 10;
        var timer = try std.time.Timer.start();
        for (0..sort_iterations) |_| {
            simd.argsortF64(data, indices, true);
            std.mem.doNotOptimizeAway(indices.ptr);
        }
        const elapsed = timer.read();
        const avg_ns = elapsed / sort_iterations;
        const avg_ms = @as(f64, @floatFromInt(avg_ns)) / 1_000_000.0;
        print("Argsort:    {d:.3} ms\n", .{avg_ms});
    }

    print("\n=== Reference Performance ===\n", .{});
    print("Polars Sum:  ~0.09 ms\n", .{});
    print("Polars Sort: ~20 ms\n", .{});
    print("Pandas Sum:  ~0.24 ms\n", .{});
    print("Pandas Sort: ~97 ms\n", .{});
}
