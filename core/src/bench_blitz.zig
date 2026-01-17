const std = @import("std");
const blitz = @import("blitz.zig");

pub fn main() !void {
    const print = std.debug.print;

    print("=== Blitz Parallel Runtime Benchmarks ===\n\n", .{});

    // Initialize blitz
    try blitz.init();
    defer blitz.deinit();

    print("Workers: {d}\n\n", .{blitz.numWorkers()});

    // Benchmark 1: Join overhead (empty work)
    // This measures the raw overhead of fork/join without any actual work
    {
        const iterations: usize = 100_000;
        var timer = try std.time.Timer.start();

        for (0..iterations) |_| {
            const result = blitz.join(
                void,
                void,
                struct {
                    fn a(_: void) void {}
                }.a,
                struct {
                    fn b(_: void) void {}
                }.b,
                {},
                {},
            );
            std.mem.doNotOptimizeAway(&result);
        }

        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        print("Join overhead (empty):     {d} ns/call\n", .{avg_ns});
    }

    // Benchmark 2: Join overhead with minimal work
    {
        const iterations: usize = 100_000;
        var counter: u64 = 0;
        var timer = try std.time.Timer.start();

        for (0..iterations) |_| {
            const result = blitz.join(
                u64,
                u64,
                struct {
                    fn a(_: void) u64 {
                        return 1;
                    }
                }.a,
                struct {
                    fn b(_: void) u64 {
                        return 2;
                    }
                }.b,
                {},
                {},
            );
            counter += result[0] + result[1];
            std.mem.doNotOptimizeAway(&counter);
        }

        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        print("Join overhead (minimal):   {d} ns/call (sum: {d})\n", .{ avg_ns, counter });
    }

    // Benchmark 3: parallelFor with small work
    {
        const n: usize = 1000;
        const iterations: usize = 10_000;
        var results: [n]u32 = undefined;

        const Context = struct { results: []u32 };
        const ctx = Context{ .results = &results };

        var timer = try std.time.Timer.start();

        for (0..iterations) |_| {
            blitz.parallelFor(n, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (start..end) |i| {
                        c.results[i] = @intCast(i);
                    }
                }
            }.body);
            std.mem.doNotOptimizeAway(&results);
        }

        const elapsed = timer.read();
        const avg_ns = elapsed / iterations;
        const avg_us = @as(f64, @floatFromInt(avg_ns)) / 1000.0;
        print("parallelFor (1K items):    {d:.2} us/call\n", .{avg_us});
    }

    // Benchmark 4: parallelReduce - sum of 10M elements
    {
        const allocator = std.heap.page_allocator;
        const n: usize = 10_000_000;
        const iterations: usize = 20;

        const data = try allocator.alloc(i64, n);
        defer allocator.free(data);

        for (data, 0..) |*v, i| {
            v.* = @intCast(i % 1000);
        }

        // Sequential baseline
        var seq_sum: i64 = 0;
        var seq_timer = try std.time.Timer.start();
        for (0..iterations) |_| {
            seq_sum = 0;
            for (data) |v| {
                seq_sum += v;
            }
            std.mem.doNotOptimizeAway(&seq_sum);
        }
        const seq_elapsed = seq_timer.read();
        const seq_avg_ms = @as(f64, @floatFromInt(seq_elapsed / iterations)) / 1_000_000.0;

        // Parallel
        var par_sum: i64 = 0;
        var par_timer = try std.time.Timer.start();
        for (0..iterations) |_| {
            par_sum = blitz.parallelReduce(
                i64,
                n,
                0,
                []const i64,
                data,
                struct {
                    fn map(d: []const i64, i: usize) i64 {
                        return d[i];
                    }
                }.map,
                struct {
                    fn combine(a: i64, b: i64) i64 {
                        return a + b;
                    }
                }.combine,
            );
            std.mem.doNotOptimizeAway(&par_sum);
        }
        const par_elapsed = par_timer.read();
        const par_avg_ms = @as(f64, @floatFromInt(par_elapsed / iterations)) / 1_000_000.0;

        const speedup = seq_avg_ms / par_avg_ms;
        print("\nSum 10M elements:\n", .{});
        print("  Sequential:              {d:.3} ms (result: {d})\n", .{ seq_avg_ms, seq_sum });
        print("  Parallel:                {d:.3} ms (result: {d})\n", .{ par_avg_ms, par_sum });
        print("  Speedup:                 {d:.2}x\n", .{speedup});
    }

    // Benchmark 5: parallelSum using SIMD-accelerated blitz function
    {
        const allocator = std.heap.page_allocator;
        const n: usize = 10_000_000;
        const iterations: usize = 20;

        const data = try allocator.alloc(f64, n);
        defer allocator.free(data);

        for (data, 0..) |*v, i| {
            v.* = @floatFromInt(i % 1000);
        }

        var timer = try std.time.Timer.start();
        var result: f64 = 0;
        for (0..iterations) |_| {
            result = blitz.parallelSum(f64, data);
            std.mem.doNotOptimizeAway(&result);
        }
        const elapsed = timer.read();
        const avg_ms = @as(f64, @floatFromInt(elapsed / iterations)) / 1_000_000.0;

        print("\nSIMD parallelSum 10M f64:\n", .{});
        print("  Time:                    {d:.3} ms (result: {d:.0})\n", .{ avg_ms, result });
    }

    // Benchmark 6: Recursive divide-and-conquer (Fibonacci-style)
    {
        const iterations: usize = 10;

        // Sequential Fibonacci
        var seq_timer = try std.time.Timer.start();
        var seq_result: u64 = 0;
        for (0..iterations) |_| {
            seq_result = sequentialFib(30);
            std.mem.doNotOptimizeAway(&seq_result);
        }
        const seq_elapsed = seq_timer.read();
        const seq_avg_ms = @as(f64, @floatFromInt(seq_elapsed / iterations)) / 1_000_000.0;

        // Parallel Fibonacci
        var par_timer = try std.time.Timer.start();
        var par_result: u64 = 0;
        for (0..iterations) |_| {
            par_result = parallelFib(30);
            std.mem.doNotOptimizeAway(&par_result);
        }
        const par_elapsed = par_timer.read();
        const par_avg_ms = @as(f64, @floatFromInt(par_elapsed / iterations)) / 1_000_000.0;

        const speedup = seq_avg_ms / par_avg_ms;
        print("\nFibonacci(30) (recursive divide-and-conquer):\n", .{});
        print("  Sequential:              {d:.2} ms (result: {d})\n", .{ seq_avg_ms, seq_result });
        print("  Parallel:                {d:.2} ms (result: {d})\n", .{ par_avg_ms, par_result });
        print("  Speedup:                 {d:.2}x\n", .{speedup});
    }

    // Benchmark 7: Compute-bound parallelFor (demonstrates actual speedup)
    {
        const allocator = std.heap.page_allocator;
        const n: usize = 1_000_000;
        const iterations: usize = 10;

        const data = try allocator.alloc(f64, n);
        defer allocator.free(data);

        for (data, 0..) |*v, i| {
            v.* = @floatFromInt(i);
        }

        // Sequential: compute-heavy operation (sin + cos + sqrt)
        var seq_timer = try std.time.Timer.start();
        for (0..iterations) |_| {
            for (data) |*v| {
                v.* = @sin(v.*) + @cos(v.*) + @sqrt(@abs(v.*));
            }
            std.mem.doNotOptimizeAway(data.ptr);
        }
        const seq_elapsed = seq_timer.read();
        const seq_avg_ms = @as(f64, @floatFromInt(seq_elapsed / iterations)) / 1_000_000.0;

        // Reset data
        for (data, 0..) |*v, i| {
            v.* = @floatFromInt(i);
        }

        // Parallel with explicit grain size (divide work into ~10 chunks for 10 workers)
        const Context = struct { data: []f64 };
        const ctx = Context{ .data = data };
        const grain_size: usize = n / 10; // One chunk per worker

        var par_timer = try std.time.Timer.start();
        for (0..iterations) |_| {
            blitz.parallelForWithGrain(n, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (c.data[start..end]) |*v| {
                        v.* = @sin(v.*) + @cos(v.*) + @sqrt(@abs(v.*));
                    }
                }
            }.body, grain_size);
            std.mem.doNotOptimizeAway(data.ptr);
        }
        const par_elapsed = par_timer.read();
        const par_avg_ms = @as(f64, @floatFromInt(par_elapsed / iterations)) / 1_000_000.0;

        const speedup = seq_avg_ms / par_avg_ms;
        print("\nCompute-bound parallelFor (1M sin+cos+sqrt):\n", .{});
        print("  Sequential:              {d:.2} ms\n", .{seq_avg_ms});
        print("  Parallel:                {d:.2} ms\n", .{par_avg_ms});
        print("  Speedup:                 {d:.2}x ({d} workers)\n", .{ speedup, blitz.numWorkers() });
    }

    // Benchmark 8: Larger compute-bound test
    {
        const allocator = std.heap.page_allocator;
        const n: usize = 10_000_000;
        const iterations: usize = 5;

        const data = try allocator.alloc(f64, n);
        defer allocator.free(data);

        for (data, 0..) |*v, i| {
            v.* = @floatFromInt(i);
        }

        // Sequential
        var seq_timer = try std.time.Timer.start();
        for (0..iterations) |_| {
            for (data) |*v| {
                v.* = @sin(v.*) + @cos(v.*);
            }
            std.mem.doNotOptimizeAway(data.ptr);
        }
        const seq_elapsed = seq_timer.read();
        const seq_avg_ms = @as(f64, @floatFromInt(seq_elapsed / iterations)) / 1_000_000.0;

        // Reset data
        for (data, 0..) |*v, i| {
            v.* = @floatFromInt(i);
        }

        // Parallel
        const Context = struct { data: []f64 };
        const ctx = Context{ .data = data };

        var par_timer = try std.time.Timer.start();
        for (0..iterations) |_| {
            blitz.parallelForWithGrain(n, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (c.data[start..end]) |*v| {
                        v.* = @sin(v.*) + @cos(v.*);
                    }
                }
            }.body, n / 100); // 100 chunks
            std.mem.doNotOptimizeAway(data.ptr);
        }
        const par_elapsed = par_timer.read();
        const par_avg_ms = @as(f64, @floatFromInt(par_elapsed / iterations)) / 1_000_000.0;

        const speedup = seq_avg_ms / par_avg_ms;
        print("\nLarge compute-bound (10M sin+cos, 100 chunks):\n", .{});
        print("  Sequential:              {d:.2} ms\n", .{seq_avg_ms});
        print("  Parallel:                {d:.2} ms\n", .{par_avg_ms});
        print("  Speedup:                 {d:.2}x\n", .{speedup});
    }

    print("\n=== Expected Performance ===\n", .{});
    print("Join overhead target:      <50 ns (was ~500-1000ns with Chase-Lev)\n", .{});
    print("Scaling efficiency:        >80%% at 8 cores\n", .{});
}

fn parallelFib(n: u64) u64 {
    if (n < 20) {
        return sequentialFib(n);
    }

    const results = blitz.join(
        u64,
        u64,
        struct {
            fn a(val: u64) u64 {
                return parallelFib(val);
            }
        }.a,
        struct {
            fn b(val: u64) u64 {
                return parallelFib(val);
            }
        }.b,
        n - 2,
        n - 1,
    );

    return results[0] + results[1];
}

fn sequentialFib(n: u64) u64 {
    if (n <= 1) return n;
    return sequentialFib(n - 1) + sequentialFib(n - 2);
}
