//! Swiss Table Benchmarks
//!
//! Comprehensive benchmarks comparing swisstable.Table performance against:
//! - std.AutoHashMap (Zig standard library)
//!
//! Benchmark categories:
//! - Insert: Sequential and random key insertion
//! - Lookup: Hit, miss, and mixed lookup patterns
//! - Remove: Deletion performance
//! - Iteration: Key/value iteration speed
//! - Entry API: Conditional insert/update operations
//! - Set operations: Union, intersection, difference
//! - Memory: Overhead comparison
//!
//! Run with: zig build bench

const std = @import("std");
const lib = @import("lib.zig");
const set_mod = @import("set.zig");
const Table = lib.Table;
const Set = set_mod.Set;

const print = std.debug.print;
const Allocator = std.mem.Allocator;

// ============================================================================
// Configuration
// ============================================================================

const ITERATIONS = 5;
const WARMUP_ITERATIONS = 2;

// Benchmark sizes
const SIZES = [_]usize{ 1_000, 10_000, 100_000, 1_000_000 };

// ============================================================================
// Statistics helpers
// ============================================================================

const Stats = struct {
    min_ns: u64,
    max_ns: u64,
    total_ns: u64,
    count: usize,

    fn init() Stats {
        return .{
            .min_ns = std.math.maxInt(u64),
            .max_ns = 0,
            .total_ns = 0,
            .count = 0,
        };
    }

    fn add(self: *Stats, ns: u64) void {
        self.min_ns = @min(self.min_ns, ns);
        self.max_ns = @max(self.max_ns, ns);
        self.total_ns += ns;
        self.count += 1;
    }

    fn avgNs(self: Stats) u64 {
        return if (self.count > 0) self.total_ns / self.count else 0;
    }

    fn nsPerOp(self: Stats, ops: usize) f64 {
        return @as(f64, @floatFromInt(self.avgNs())) / @as(f64, @floatFromInt(ops));
    }
};

// ============================================================================
// Random number generator for benchmarks
// ============================================================================

const Rng = struct {
    state: u64,

    fn init(seed: u64) Rng {
        return .{ .state = seed };
    }

    fn next(self: *Rng) u64 {
        // xorshift64
        var x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        return x;
    }

    fn nextBounded(self: *Rng, bound: u64) u64 {
        return self.next() % bound;
    }
};

// ============================================================================
// Main
// ============================================================================

pub fn main() !void {
    try runBenchmarks();
}

pub fn runBenchmarks() !void {
    const allocator = std.heap.c_allocator;

    print("\n", .{});
    print("=" ** 80 ++ "\n", .{});
    print("                    Swiss Table Benchmark Suite\n", .{});
    print("=" ** 80 ++ "\n", .{});
    print("Iterations: {d} (+ {d} warmup)   ", .{ ITERATIONS, WARMUP_ITERATIONS });
    print("Platform: {s}\n", .{@tagName(@import("builtin").cpu.arch)});
    print("GROUP_WIDTH: {d} bytes\n\n", .{lib.GROUP_WIDTH});

    for (SIZES) |n| {
        print("=" ** 80 ++ "\n", .{});
        print("N = {d:>12}\n", .{n});
        print("=" ** 80 ++ "\n\n", .{});

        // Core operations
        try benchInsertSequential(allocator, n);
        try benchInsertRandom(allocator, n);
        try benchInsertPrealloc(allocator, n);
        try benchPutNew(allocator, n);
        print("\n", .{});

        try benchLookupHit(allocator, n);
        try benchLookupMiss(allocator, n);
        try benchLookupRandom(allocator, n);
        print("\n", .{});

        try benchRemove(allocator, n);
        try benchRemoveAndReinsert(allocator, n);
        print("\n", .{});

        // Iteration
        try benchIteration(allocator, n);
        try benchKeysIteration(allocator, n);
        print("\n", .{});

        // Entry API
        try benchGetOrInsert(allocator, n);
        try benchEntryAPI(allocator, n);
        print("\n", .{});

        // Set operations (for smaller sizes only - O(n) operations)
        if (n <= 100_000) {
            try benchSetInsert(allocator, n);
            try benchSetContains(allocator, n);
            try benchSetUnion(allocator, n);
            try benchSetIntersection(allocator, n);
            print("\n", .{});
        }

        // Memory usage
        try benchMemoryUsage(allocator, n);
        print("\n", .{});
    }

    print("=" ** 80 ++ "\n", .{});
    print("Benchmark complete.\n", .{});
    print("=" ** 80 ++ "\n\n", .{});
}

// ============================================================================
// Insert Benchmarks
// ============================================================================

fn benchInsertSequential(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Warmup + benchmark
    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var map = Table(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                _ = try map.put(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                try map.put(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Insert (seq)", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchInsertRandom(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Pre-generate random keys
    const keys = try allocator.alloc(i64, n);
    defer allocator.free(keys);
    var rng = Rng.init(12345);
    for (keys) |*k| {
        k.* = @bitCast(rng.next());
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var map = Table(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (keys) |k| {
                _ = try map.put(k, k);
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (keys) |k| {
                try map.put(k, k);
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Insert (rnd)", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchInsertPrealloc(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var map = try Table(i64, i64).initCapacity(allocator, n);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                _ = try map.put(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();
            try map.ensureTotalCapacity(@intCast(n));

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                try map.put(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Insert (pre)", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchPutNew(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table with putNew (skip existence check)
        {
            var map = try Table(i64, i64).initCapacity(allocator, n);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                try map.putNew(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap with putAssumeCapacity
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();
            try map.ensureTotalCapacity(@intCast(n));

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                map.putAssumeCapacity(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("PutNew/Assume", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

// ============================================================================
// Lookup Benchmarks
// ============================================================================

fn benchLookupHit(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Setup tables
    var swiss_map = Table(i64, i64).init(allocator);
    defer swiss_map.deinit();
    var std_map = std.AutoHashMap(i64, i64).init(allocator);
    defer std_map.deinit();

    for (0..n) |i| {
        _ = try swiss_map.put(@intCast(i), @intCast(i));
        try std_map.put(@intCast(i), @intCast(i));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var checksum: i64 = 0;
            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                if (swiss_map.get(@intCast(i))) |v| {
                    checksum +%= v.*;
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(checksum);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var checksum: i64 = 0;
            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                if (std_map.get(@intCast(i))) |v| {
                    checksum +%= v;
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(checksum);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Lookup (hit)", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchLookupMiss(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Setup tables with keys 0..n
    var swiss_map = Table(i64, i64).init(allocator);
    defer swiss_map.deinit();
    var std_map = std.AutoHashMap(i64, i64).init(allocator);
    defer std_map.deinit();

    for (0..n) |i| {
        _ = try swiss_map.put(@intCast(i), @intCast(i));
        try std_map.put(@intCast(i), @intCast(i));
    }

    // Lookup keys n..2n (all misses)
    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var miss_count: usize = 0;
            const start = std.time.nanoTimestamp();
            for (n..n * 2) |i| {
                if (swiss_map.get(@intCast(i)) == null) {
                    miss_count += 1;
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(miss_count);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var miss_count: usize = 0;
            const start = std.time.nanoTimestamp();
            for (n..n * 2) |i| {
                if (std_map.get(@intCast(i)) == null) {
                    miss_count += 1;
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(miss_count);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Lookup (miss)", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchLookupRandom(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Setup tables
    var swiss_map = Table(i64, i64).init(allocator);
    defer swiss_map.deinit();
    var std_map = std.AutoHashMap(i64, i64).init(allocator);
    defer std_map.deinit();

    for (0..n) |i| {
        _ = try swiss_map.put(@intCast(i), @intCast(i));
        try std_map.put(@intCast(i), @intCast(i));
    }

    // Pre-generate random lookup keys (50% hit, 50% miss)
    const lookup_keys = try allocator.alloc(i64, n);
    defer allocator.free(lookup_keys);
    var rng = Rng.init(54321);
    for (lookup_keys) |*k| {
        k.* = @intCast(rng.nextBounded(@as(u64, n) * 2));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var checksum: i64 = 0;
            const start = std.time.nanoTimestamp();
            for (lookup_keys) |k| {
                if (swiss_map.get(k)) |v| {
                    checksum +%= v.*;
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(checksum);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var checksum: i64 = 0;
            const start = std.time.nanoTimestamp();
            for (lookup_keys) |k| {
                if (std_map.get(k)) |v| {
                    checksum +%= v;
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(checksum);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Lookup (rnd)", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

// ============================================================================
// Remove Benchmarks
// ============================================================================

fn benchRemove(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var map = Table(i64, i64).init(allocator);
            defer map.deinit();
            for (0..n) |i| {
                _ = try map.put(@intCast(i), @intCast(i));
            }

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                _ = map.remove(@intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();
            for (0..n) |i| {
                try map.put(@intCast(i), @intCast(i));
            }

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                _ = map.remove(@intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Remove", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchRemoveAndReinsert(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table: remove half, reinsert
        {
            var map = Table(i64, i64).init(allocator);
            defer map.deinit();
            for (0..n) |i| {
                _ = try map.put(@intCast(i), @intCast(i));
            }

            const start = std.time.nanoTimestamp();
            // Remove even keys
            var i: usize = 0;
            while (i < n) : (i += 2) {
                _ = map.remove(@intCast(i));
            }
            // Reinsert them
            i = 0;
            while (i < n) : (i += 2) {
                _ = try map.put(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();
            for (0..n) |i| {
                try map.put(@intCast(i), @intCast(i));
            }

            const start = std.time.nanoTimestamp();
            var i: usize = 0;
            while (i < n) : (i += 2) {
                _ = map.remove(@intCast(i));
            }
            i = 0;
            while (i < n) : (i += 2) {
                try map.put(@intCast(i), @intCast(i));
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Remove+Reins", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

// ============================================================================
// Iteration Benchmarks
// ============================================================================

fn benchIteration(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Setup tables
    var swiss_map = Table(i64, i64).init(allocator);
    defer swiss_map.deinit();
    var std_map = std.AutoHashMap(i64, i64).init(allocator);
    defer std_map.deinit();

    for (0..n) |i| {
        _ = try swiss_map.put(@intCast(i), @intCast(i));
        try std_map.put(@intCast(i), @intCast(i));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table
        {
            var sum: i64 = 0;
            const start = std.time.nanoTimestamp();
            var it = swiss_map.iterator();
            while (it.next()) |entry| {
                sum +%= entry.key +% entry.value;
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(sum);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap
        {
            var sum: i64 = 0;
            const start = std.time.nanoTimestamp();
            var it = std_map.iterator();
            while (it.next()) |entry| {
                sum +%= entry.key_ptr.* +% entry.value_ptr.*;
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(sum);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Iterate", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchKeysIteration(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    // Setup tables
    var swiss_map = Table(i64, i64).init(allocator);
    defer swiss_map.deinit();
    var std_map = std.AutoHashMap(i64, i64).init(allocator);
    defer std_map.deinit();

    for (0..n) |i| {
        _ = try swiss_map.put(@intCast(i), @intCast(i));
        try std_map.put(@intCast(i), @intCast(i));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table keys()
        {
            var sum: i64 = 0;
            const start = std.time.nanoTimestamp();
            var it = swiss_map.keys();
            while (it.next()) |key| {
                sum +%= key;
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(sum);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap keyIterator
        {
            var sum: i64 = 0;
            const start = std.time.nanoTimestamp();
            var it = std_map.keyIterator();
            while (it.next()) |key| {
                sum +%= key.*;
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
            std.mem.doNotOptimizeAway(sum);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("Keys iter", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

// ============================================================================
// Entry API Benchmarks
// ============================================================================

fn benchGetOrInsert(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();
    var std_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table getOrInsert
        {
            var map = Table(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                const key: i64 = @intCast(i % (n / 2)); // 50% duplicates
                const ptr = try map.getOrInsert(key, 0);
                ptr.* += 1;
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }

        // std.AutoHashMap getOrPut
        {
            var map = std.AutoHashMap(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                const key: i64 = @intCast(i % (n / 2));
                const gop = try map.getOrPut(key);
                if (!gop.found_existing) {
                    gop.value_ptr.* = 0;
                }
                gop.value_ptr.* += 1;
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                std_stats.add(elapsed);
            }
        }
    }

    printResult("GetOrInsert", swiss_stats.nsPerOp(n), std_stats.nsPerOp(n));
}

fn benchEntryAPI(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        // Swiss Table Entry API
        {
            var map = Table(i64, i64).init(allocator);
            defer map.deinit();

            const start = std.time.nanoTimestamp();
            for (0..n) |i| {
                const key: i64 = @intCast(i % (n / 2));
                const e = try map.entry(key);
                switch (e) {
                    .occupied => |o| {
                        o.get().* += 1;
                    },
                    .vacant => |v| {
                        _ = v.insert(1);
                    },
                }
            }
            const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

            if (iter >= WARMUP_ITERATIONS) {
                swiss_stats.add(elapsed);
            }
        }
    }

    // No std equivalent for entry API
    print("  Entry API:   Swiss {d:>8.1}ns/op   (no std equivalent)\n", .{swiss_stats.nsPerOp(n)});
}

// ============================================================================
// Set Benchmarks
// ============================================================================

fn benchSetInsert(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        var set = Set(i64).init(allocator);
        defer set.deinit();

        const start = std.time.nanoTimestamp();
        for (0..n) |i| {
            _ = try set.insert(@intCast(i));
        }
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);

        if (iter >= WARMUP_ITERATIONS) {
            swiss_stats.add(elapsed);
        }
    }

    print("  Set insert:  Swiss {d:>8.1}ns/op\n", .{swiss_stats.nsPerOp(n)});
}

fn benchSetContains(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();

    var set = Set(i64).init(allocator);
    defer set.deinit();
    for (0..n) |i| {
        _ = try set.insert(@intCast(i));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        var count: usize = 0;
        const start = std.time.nanoTimestamp();
        for (0..n * 2) |i| {
            if (set.contains(@intCast(i))) {
                count += 1;
            }
        }
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
        std.mem.doNotOptimizeAway(count);

        if (iter >= WARMUP_ITERATIONS) {
            swiss_stats.add(elapsed);
        }
    }

    printResultSingle("Set contains", swiss_stats.nsPerOp(n * 2));
}

fn benchSetUnion(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();

    var set_a = Set(i64).init(allocator);
    defer set_a.deinit();
    var set_b = Set(i64).init(allocator);
    defer set_b.deinit();

    // 50% overlap
    for (0..n) |i| {
        _ = try set_a.insert(@intCast(i));
        _ = try set_b.insert(@intCast(i + n / 2));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        const start = std.time.nanoTimestamp();
        var result = try set_a.unionWith(&set_b);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
        result.deinit();

        if (iter >= WARMUP_ITERATIONS) {
            swiss_stats.add(elapsed);
        }
    }

    printResultSingle("Set union", swiss_stats.nsPerOp(n));
}

fn benchSetIntersection(allocator: Allocator, n: usize) !void {
    var swiss_stats = Stats.init();

    var set_a = Set(i64).init(allocator);
    defer set_a.deinit();
    var set_b = Set(i64).init(allocator);
    defer set_b.deinit();

    // 50% overlap
    for (0..n) |i| {
        _ = try set_a.insert(@intCast(i));
        _ = try set_b.insert(@intCast(i + n / 2));
    }

    for (0..WARMUP_ITERATIONS + ITERATIONS) |iter| {
        const start = std.time.nanoTimestamp();
        var result = try set_a.intersection(&set_b);
        const elapsed: u64 = @intCast(std.time.nanoTimestamp() - start);
        result.deinit();

        if (iter >= WARMUP_ITERATIONS) {
            swiss_stats.add(elapsed);
        }
    }

    printResultSingle("Set intersect", swiss_stats.nsPerOp(n));
}

// ============================================================================
// Memory Usage
// ============================================================================

fn benchMemoryUsage(allocator: Allocator, n: usize) !void {
    // Swiss Table
    var swiss_map = try Table(i64, i64).initCapacity(allocator, n);
    defer swiss_map.deinit();
    for (0..n) |i| {
        _ = try swiss_map.put(@intCast(i), @intCast(i));
    }

    // std.AutoHashMap
    var std_map = std.AutoHashMap(i64, i64).init(allocator);
    defer std_map.deinit();
    try std_map.ensureTotalCapacity(@intCast(n));
    for (0..n) |i| {
        try std_map.put(@intCast(i), @intCast(i));
    }

    // Estimate memory usage
    const swiss_capacity = swiss_map.capacity();
    const swiss_entry_size = @sizeOf(i64) * 2; // key + value
    const swiss_ctrl_size = swiss_capacity + lib.GROUP_WIDTH;
    const swiss_mem = swiss_ctrl_size + swiss_capacity * swiss_entry_size;

    const std_capacity = std_map.capacity();
    const std_entry_size = @sizeOf(i64) * 2 + @sizeOf(u32); // key + value + metadata
    const std_mem = std_capacity * std_entry_size;

    const swiss_bytes_per_entry = @as(f64, @floatFromInt(swiss_mem)) / @as(f64, @floatFromInt(n));
    const std_bytes_per_entry = @as(f64, @floatFromInt(std_mem)) / @as(f64, @floatFromInt(n));

    print("  Memory:      Swiss {d:>6.1} B/entry   std {d:>6.1} B/entry   ", .{ swiss_bytes_per_entry, std_bytes_per_entry });
    if (swiss_bytes_per_entry < std_bytes_per_entry) {
        print("{d:.0}% smaller\n", .{(1.0 - swiss_bytes_per_entry / std_bytes_per_entry) * 100});
    } else {
        print("{d:.0}% larger\n", .{(swiss_bytes_per_entry / std_bytes_per_entry - 1.0) * 100});
    }
}

// ============================================================================
// Output helpers
// ============================================================================

fn printResult(name: []const u8, swiss_ns: f64, std_ns: f64) void {
    const speedup = std_ns / swiss_ns;
    const indicator: []const u8 = if (speedup >= 1.0) "+" else "-";
    print("  {s:<14} Swiss {d:>8.1}ns/op   std {d:>8.1}ns/op   {s}{d:.2}x\n", .{ name, swiss_ns, std_ns, indicator, speedup });
}

fn printResultSingle(name: []const u8, swiss_ns: f64) void {
    print("  {s:<14} Swiss {d:>8.1}ns/op\n", .{ name, swiss_ns });
}
