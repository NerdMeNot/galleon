//! Polars-Style Parallel Hash Join Implementation
//!
//! This module implements a partitioned hash join algorithm matching Polars' design:
//!
//! BUILD Phase (fully parallel):
//! 1. Parallel count - Each thread counts partition sizes for its chunk
//! 2. Compute offsets - Build 2D offset table (thread × partition)
//! 3. Parallel scatter - Each thread scatters its chunk using SyncPtr
//! 4. Parallel build - Each partition builds its hash table independently
//!
//! PROBE Phase (fully parallel):
//! 1. Parallel count - Count result sizes per thread
//! 2. Parallel probe - Each thread probes and writes results via SyncPtr
//!
//! Key optimizations:
//! - Build the smaller relation
//! - Lock-free parallel writes with pre-computed offsets (SyncPtr)
//! - No sequential bottlenecks for large inputs
//! - IdxVec (SmallVec) avoids heap allocation for duplicates

const std = @import("std");
const Allocator = std.mem.Allocator;

// Import Swiss Table from the swisstable module (pure Swiss Table implementation)
const swisstable = @import("../swisstable/lib.zig");
const SwissTable = swisstable.Table;
const hashToPartition = swisstable.hashToPartition;
const dirtyHash = swisstable.dirtyHash;

// Import IdxVec from local module
const idx_vec_mod = @import("idx_vec.zig");
const IdxVec4 = idx_vec_mod.IdxVec4;
const IdxSize = idx_vec_mod.IdxSize;
const NULL_IDX = idx_vec_mod.NULL_IDX;

const blitz = @import("../blitz/mod.zig");

// ============================================================================
// Configuration
// ============================================================================

/// Minimum total elements to justify parallelism.
/// For workloads < 2M elements, sequential is typically faster due to lower overhead.
const MIN_TOTAL_ELEMENTS_FOR_PARALLEL: usize = 2_000_000;

/// Get number of threads/partitions
fn getNumThreads() usize {
    if (blitz.isInitialized()) {
        return @max(1, blitz.numWorkers());
    }
    return 1;
}

/// Check if we should use parallel execution
fn shouldParallelize(n: usize) bool {
    if (!blitz.isInitialized()) return false;
    return n >= MIN_TOTAL_ELEMENTS_FOR_PARALLEL;
}

// ============================================================================
// Join Result Types
// ============================================================================

pub const InnerJoinResult = struct {
    left_indices: []IdxSize,
    right_indices: []IdxSize,
    allocator: Allocator,

    pub fn deinit(self: *InnerJoinResult) void {
        self.allocator.free(self.left_indices);
        self.allocator.free(self.right_indices);
        self.* = undefined;
    }

    pub fn len(self: *const InnerJoinResult) usize {
        return self.left_indices.len;
    }
};

pub const LeftJoinResult = struct {
    left_indices: []IdxSize,
    right_indices: []IdxSize,
    allocator: Allocator,

    pub fn deinit(self: *LeftJoinResult) void {
        self.allocator.free(self.left_indices);
        self.allocator.free(self.right_indices);
        self.* = undefined;
    }

    pub fn len(self: *const LeftJoinResult) usize {
        return self.left_indices.len;
    }
};

// ============================================================================
// Hash Table Type
// ============================================================================

fn JoinHashTable(comptime K: type) type {
    return SwissTable(K, IdxVec4);
}

/// Compute hash for a key
inline fn computeHash(comptime K: type, key: K) u64 {
    if (@typeInfo(K) == .int) {
        return dirtyHash(@as(u64, @bitCast(@as(i64, @intCast(key)))));
    } else {
        var bits: u64 = 0;
        const key_bytes = std.mem.asBytes(&key);
        @memcpy(std.mem.asBytes(&bits)[0..@min(@sizeOf(K), 8)], key_bytes[0..@min(@sizeOf(K), 8)]);
        return dirtyHash(bits);
    }
}

// ============================================================================
// Sequential Implementation (for small inputs)
// ============================================================================

fn innerJoinSequential(
    comptime K: type,
    build_keys: []const K,
    probe_keys: []const K,
    allocator: Allocator,
) !InnerJoinResult {
    // Pre-size the table to avoid rehashing during build
    var table = try JoinHashTable(K).initCapacity(allocator, build_keys.len);
    defer {
        var it = table.iterator();
        while (it.next()) |entry| {
            var vec = &entry.value;
            vec.deinit();
        }
        table.deinit();
    }

    for (build_keys, 0..) |key, idx| {
        const entry_vec = try table.getOrInsertDefault(key, IdxVec4.init());
        try entry_vec.push(allocator, @intCast(idx));
    }

    var left_indices = std.ArrayListUnmanaged(IdxSize){};
    var right_indices = std.ArrayListUnmanaged(IdxSize){};
    errdefer left_indices.deinit(allocator);
    errdefer right_indices.deinit(allocator);

    try left_indices.ensureTotalCapacity(allocator, @min(build_keys.len, probe_keys.len));
    try right_indices.ensureTotalCapacity(allocator, @min(build_keys.len, probe_keys.len));

    for (probe_keys, 0..) |key, probe_idx| {
        if (table.getPtr(key)) |match_vec| {
            for (match_vec.slice()) |build_idx| {
                try left_indices.append(allocator, @intCast(probe_idx));
                try right_indices.append(allocator, build_idx);
            }
        }
    }

    return InnerJoinResult{
        .left_indices = try left_indices.toOwnedSlice(allocator),
        .right_indices = try right_indices.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

fn leftJoinSequential(
    comptime K: type,
    build_keys: []const K,
    probe_keys: []const K,
    allocator: Allocator,
) !LeftJoinResult {
    // Pre-size the table to avoid rehashing during build
    var table = try JoinHashTable(K).initCapacity(allocator, build_keys.len);
    defer {
        var it = table.iterator();
        while (it.next()) |entry| {
            var vec = &entry.value;
            vec.deinit();
        }
        table.deinit();
    }

    for (build_keys, 0..) |key, idx| {
        const entry_vec = try table.getOrInsertDefault(key, IdxVec4.init());
        try entry_vec.push(allocator, @intCast(idx));
    }

    var left_indices = std.ArrayListUnmanaged(IdxSize){};
    var right_indices = std.ArrayListUnmanaged(IdxSize){};
    errdefer left_indices.deinit(allocator);
    errdefer right_indices.deinit(allocator);

    try left_indices.ensureTotalCapacity(allocator, probe_keys.len);
    try right_indices.ensureTotalCapacity(allocator, probe_keys.len);

    for (probe_keys, 0..) |key, probe_idx| {
        if (table.getPtr(key)) |match_vec| {
            for (match_vec.slice()) |build_idx| {
                try left_indices.append(allocator, @intCast(probe_idx));
                try right_indices.append(allocator, build_idx);
            }
        } else {
            try left_indices.append(allocator, @intCast(probe_idx));
            try right_indices.append(allocator, NULL_IDX);
        }
    }

    return LeftJoinResult{
        .left_indices = try left_indices.toOwnedSlice(allocator),
        .right_indices = try right_indices.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

// ============================================================================
// Parallel BUILD Phase (Polars-style)
// ============================================================================

/// Build partitioned hash tables using full parallel pipeline
fn buildTablesParallel(
    comptime K: type,
    keys: []const K,
    n_partitions: usize,
    allocator: Allocator,
) !struct {
    tables: []JoinHashTable(K),
    scatter_indices: []IdxSize,
    partition_offsets: []usize,
} {
    const n = keys.len;
    const n_threads = n_partitions;

    // ========================================================================
    // Step 1: Parallel count partition sizes
    // ========================================================================

    const per_thread_counts = try allocator.alloc([]usize, n_threads);
    defer allocator.free(per_thread_counts);

    for (0..n_threads) |t| {
        per_thread_counts[t] = try allocator.alloc(usize, n_partitions);
        @memset(per_thread_counts[t], 0);
    }
    defer {
        for (0..n_threads) |t| {
            allocator.free(per_thread_counts[t]);
        }
    }

    const chunk_size = (n + n_threads - 1) / n_threads;

    const CountCtx = struct {
        keys: []const K,
        per_thread_counts: [][]usize,
        chunk_size: usize,
        n_partitions: usize,
        n: usize,
    };

    var count_ctx = CountCtx{
        .keys = keys,
        .per_thread_counts = per_thread_counts,
        .chunk_size = chunk_size,
        .n_partitions = n_partitions,
        .n = n,
    };

    blitz.parallelFor(n_threads, *CountCtx, &count_ctx, struct {
        fn count(ctx: *CountCtx, start_t: usize, end_t: usize) void {
            for (start_t..end_t) |t| {
                const key_start = t * ctx.chunk_size;
                const key_end = @min(key_start + ctx.chunk_size, ctx.n);
                const counts = ctx.per_thread_counts[t];

                for (ctx.keys[key_start..key_end]) |key| {
                    const hash = computeHash(K, key);
                    const p = hashToPartition(hash, ctx.n_partitions);
                    counts[p] += 1;
                }
            }
        }
    }.count);

    // ========================================================================
    // Step 2: Compute 2D offset table
    // ========================================================================

    const per_thread_offsets = try allocator.alloc(usize, n_threads * n_partitions);
    defer allocator.free(per_thread_offsets);

    const partition_offsets = try allocator.alloc(usize, n_partitions + 1);
    errdefer allocator.free(partition_offsets);

    var cum_offset: usize = 0;
    for (0..n_partitions) |p| {
        partition_offsets[p] = cum_offset;
        for (0..n_threads) |t| {
            per_thread_offsets[t * n_partitions + p] = cum_offset;
            cum_offset += per_thread_counts[t][p];
        }
    }
    partition_offsets[n_partitions] = cum_offset;

    // ========================================================================
    // Step 3: Parallel scatter using SyncPtr
    // ========================================================================

    const scatter_keys = try allocator.alloc(K, n);
    errdefer allocator.free(scatter_keys);

    const scatter_indices = try allocator.alloc(IdxSize, n);
    errdefer allocator.free(scatter_indices);

    const thread_offsets_copy = try allocator.alloc(usize, n_threads * n_partitions);
    defer allocator.free(thread_offsets_copy);
    @memcpy(thread_offsets_copy, per_thread_offsets);

    const keys_ptr = blitz.SyncPtr(K).init(scatter_keys);
    const indices_ptr = blitz.SyncPtr(IdxSize).init(scatter_indices);

    const ScatterCtx = struct {
        keys: []const K,
        keys_ptr: blitz.SyncPtr(K),
        indices_ptr: blitz.SyncPtr(IdxSize),
        thread_offsets: []usize,
        chunk_size: usize,
        n_partitions: usize,
        n: usize,
    };

    var scatter_ctx = ScatterCtx{
        .keys = keys,
        .keys_ptr = keys_ptr,
        .indices_ptr = indices_ptr,
        .thread_offsets = thread_offsets_copy,
        .chunk_size = chunk_size,
        .n_partitions = n_partitions,
        .n = n,
    };

    blitz.parallelFor(n_threads, *ScatterCtx, &scatter_ctx, struct {
        fn scatter(ctx: *ScatterCtx, start_t: usize, end_t: usize) void {
            for (start_t..end_t) |t| {
                const key_start = t * ctx.chunk_size;
                const key_end = @min(key_start + ctx.chunk_size, ctx.n);
                const offsets = ctx.thread_offsets[t * ctx.n_partitions .. (t + 1) * ctx.n_partitions];

                for (key_start..key_end) |i| {
                    const key = ctx.keys[i];
                    const hash = computeHash(K, key);
                    const p = hashToPartition(hash, ctx.n_partitions);
                    const off = offsets[p];

                    ctx.keys_ptr.writeAt(off, key);
                    ctx.indices_ptr.writeAt(off, @intCast(i));
                    offsets[p] = off + 1;
                }
            }
        }
    }.scatter);

    defer allocator.free(scatter_keys);

    // ========================================================================
    // Step 4: Parallel build hash tables
    // ========================================================================

    const tables = try allocator.alloc(JoinHashTable(K), n_partitions);
    errdefer {
        for (tables) |*t| {
            var it = t.iterator();
            while (it.next()) |entry| {
                var vec = &entry.value;
                vec.deinit();
            }
            t.deinit();
        }
        allocator.free(tables);
    }

    for (tables) |*t| {
        t.* = JoinHashTable(K).init(allocator);
    }

    const BuildCtx = struct {
        tables: []JoinHashTable(K),
        scatter_keys: []const K,
        scatter_indices: []const IdxSize,
        partition_offsets: []const usize,
        allocator: Allocator,
        had_error: bool = false,
    };

    var build_ctx = BuildCtx{
        .tables = tables,
        .scatter_keys = scatter_keys,
        .scatter_indices = scatter_indices,
        .partition_offsets = partition_offsets,
        .allocator = allocator,
    };

    blitz.parallelFor(n_partitions, *BuildCtx, &build_ctx, struct {
        fn build(ctx: *BuildCtx, start_p: usize, end_p: usize) void {
            for (start_p..end_p) |p| {
                const p_start = ctx.partition_offsets[p];
                const p_end = ctx.partition_offsets[p + 1];

                for (p_start..p_end) |i| {
                    const key = ctx.scatter_keys[i];
                    const idx = ctx.scatter_indices[i];

                    const entry_vec = ctx.tables[p].getOrInsertDefault(key, IdxVec4.init()) catch {
                        ctx.had_error = true;
                        return;
                    };
                    entry_vec.push(ctx.allocator, idx) catch {
                        ctx.had_error = true;
                        return;
                    };
                }
            }
        }
    }.build);

    if (build_ctx.had_error) {
        return error.OutOfMemory;
    }

    return .{
        .tables = tables,
        .scatter_indices = scatter_indices,
        .partition_offsets = partition_offsets,
    };
}

/// Free hash tables and their IdxVec values
fn freeTables(comptime K: type, tables: []JoinHashTable(K), allocator: Allocator) void {
    for (tables) |*t| {
        var it = t.iterator();
        while (it.next()) |entry| {
            var vec = &entry.value;
            vec.deinit();
        }
        t.deinit();
    }
    allocator.free(tables);
}

// ============================================================================
// Parallel PROBE Phase (Polars-style)
// ============================================================================

/// Thread-local probe results
const ThreadResults = struct {
    left: std.ArrayListUnmanaged(IdxSize),
    right: std.ArrayListUnmanaged(IdxSize),

    fn init() ThreadResults {
        return .{ .left = .{}, .right = .{} };
    }

    fn deinit(self: *ThreadResults, allocator: Allocator) void {
        self.left.deinit(allocator);
        self.right.deinit(allocator);
    }
};

fn probeInnerParallel(
    comptime K: type,
    probe_keys: []const K,
    tables: []JoinHashTable(K),
    n_partitions: usize,
    allocator: Allocator,
) !InnerJoinResult {
    const n = probe_keys.len;
    const n_threads = n_partitions;
    const chunk_size = (n + n_threads - 1) / n_threads;

    const thread_results = try allocator.alloc(ThreadResults, n_threads);
    defer allocator.free(thread_results);

    for (thread_results) |*r| {
        r.* = ThreadResults.init();
    }
    defer {
        for (thread_results) |*r| {
            r.deinit(allocator);
        }
    }

    const ProbeCtx = struct {
        probe_keys: []const K,
        tables: []JoinHashTable(K),
        thread_results: []ThreadResults,
        chunk_size: usize,
        n_partitions: usize,
        n: usize,
        allocator: Allocator,
        had_error: bool = false,
    };

    var probe_ctx = ProbeCtx{
        .probe_keys = probe_keys,
        .tables = tables,
        .thread_results = thread_results,
        .chunk_size = chunk_size,
        .n_partitions = n_partitions,
        .n = n,
        .allocator = allocator,
    };

    blitz.parallelFor(n_threads, *ProbeCtx, &probe_ctx, struct {
        fn probe(ctx: *ProbeCtx, start_t: usize, end_t: usize) void {
            for (start_t..end_t) |t| {
                const key_start = t * ctx.chunk_size;
                const key_end = @min(key_start + ctx.chunk_size, ctx.n);
                const result = &ctx.thread_results[t];

                result.left.ensureTotalCapacity(ctx.allocator, (key_end - key_start) / 4) catch {
                    ctx.had_error = true;
                    return;
                };
                result.right.ensureTotalCapacity(ctx.allocator, (key_end - key_start) / 4) catch {
                    ctx.had_error = true;
                    return;
                };

                for (key_start..key_end) |i| {
                    const key = ctx.probe_keys[i];
                    const hash = computeHash(K, key);
                    const p = hashToPartition(hash, ctx.n_partitions);

                    if (ctx.tables[p].getPtr(key)) |match_vec| {
                        for (match_vec.slice()) |build_idx| {
                            result.left.append(ctx.allocator, @intCast(i)) catch {
                                ctx.had_error = true;
                                return;
                            };
                            result.right.append(ctx.allocator, build_idx) catch {
                                ctx.had_error = true;
                                return;
                            };
                        }
                    }
                }
            }
        }
    }.probe);

    if (probe_ctx.had_error) {
        return error.OutOfMemory;
    }

    // Merge results
    var total: usize = 0;
    for (thread_results) |*r| {
        total += r.left.items.len;
    }

    const left_out = try allocator.alloc(IdxSize, total);
    errdefer allocator.free(left_out);
    const right_out = try allocator.alloc(IdxSize, total);
    errdefer allocator.free(right_out);

    const thread_offsets = try allocator.alloc(usize, n_threads);
    defer allocator.free(thread_offsets);

    var offset: usize = 0;
    for (0..n_threads) |t| {
        thread_offsets[t] = offset;
        offset += thread_results[t].left.items.len;
    }

    const left_ptr = blitz.SyncPtr(IdxSize).init(left_out);
    const right_ptr = blitz.SyncPtr(IdxSize).init(right_out);

    const MergeCtx = struct {
        thread_results: []ThreadResults,
        thread_offsets: []usize,
        left_ptr: blitz.SyncPtr(IdxSize),
        right_ptr: blitz.SyncPtr(IdxSize),
    };

    var merge_ctx = MergeCtx{
        .thread_results = thread_results,
        .thread_offsets = thread_offsets,
        .left_ptr = left_ptr,
        .right_ptr = right_ptr,
    };

    blitz.parallelFor(n_threads, *MergeCtx, &merge_ctx, struct {
        fn merge(ctx: *MergeCtx, start_t: usize, end_t: usize) void {
            for (start_t..end_t) |t| {
                const result = &ctx.thread_results[t];
                const off = ctx.thread_offsets[t];
                ctx.left_ptr.copyAt(off, result.left.items);
                ctx.right_ptr.copyAt(off, result.right.items);
            }
        }
    }.merge);

    return InnerJoinResult{
        .left_indices = left_out,
        .right_indices = right_out,
        .allocator = allocator,
    };
}

fn probeLeftParallel(
    comptime K: type,
    probe_keys: []const K,
    tables: []JoinHashTable(K),
    n_partitions: usize,
    allocator: Allocator,
) !LeftJoinResult {
    const n = probe_keys.len;
    const n_threads = n_partitions;
    const chunk_size = (n + n_threads - 1) / n_threads;

    const thread_results = try allocator.alloc(ThreadResults, n_threads);
    defer allocator.free(thread_results);

    for (thread_results) |*r| {
        r.* = ThreadResults.init();
    }
    defer {
        for (thread_results) |*r| {
            r.deinit(allocator);
        }
    }

    const ProbeCtx = struct {
        probe_keys: []const K,
        tables: []JoinHashTable(K),
        thread_results: []ThreadResults,
        chunk_size: usize,
        n_partitions: usize,
        n: usize,
        allocator: Allocator,
        had_error: bool = false,
    };

    var probe_ctx = ProbeCtx{
        .probe_keys = probe_keys,
        .tables = tables,
        .thread_results = thread_results,
        .chunk_size = chunk_size,
        .n_partitions = n_partitions,
        .n = n,
        .allocator = allocator,
    };

    blitz.parallelFor(n_threads, *ProbeCtx, &probe_ctx, struct {
        fn probe(ctx: *ProbeCtx, start_t: usize, end_t: usize) void {
            for (start_t..end_t) |t| {
                const key_start = t * ctx.chunk_size;
                const key_end = @min(key_start + ctx.chunk_size, ctx.n);
                const result = &ctx.thread_results[t];

                result.left.ensureTotalCapacity(ctx.allocator, key_end - key_start) catch {
                    ctx.had_error = true;
                    return;
                };
                result.right.ensureTotalCapacity(ctx.allocator, key_end - key_start) catch {
                    ctx.had_error = true;
                    return;
                };

                for (key_start..key_end) |i| {
                    const key = ctx.probe_keys[i];
                    const hash = computeHash(K, key);
                    const p = hashToPartition(hash, ctx.n_partitions);

                    if (ctx.tables[p].getPtr(key)) |match_vec| {
                        for (match_vec.slice()) |build_idx| {
                            result.left.append(ctx.allocator, @intCast(i)) catch {
                                ctx.had_error = true;
                                return;
                            };
                            result.right.append(ctx.allocator, build_idx) catch {
                                ctx.had_error = true;
                                return;
                            };
                        }
                    } else {
                        result.left.append(ctx.allocator, @intCast(i)) catch {
                            ctx.had_error = true;
                            return;
                        };
                        result.right.append(ctx.allocator, NULL_IDX) catch {
                            ctx.had_error = true;
                            return;
                        };
                    }
                }
            }
        }
    }.probe);

    if (probe_ctx.had_error) {
        return error.OutOfMemory;
    }

    // Merge results
    var total: usize = 0;
    for (thread_results) |*r| {
        total += r.left.items.len;
    }

    const left_out = try allocator.alloc(IdxSize, total);
    errdefer allocator.free(left_out);
    const right_out = try allocator.alloc(IdxSize, total);
    errdefer allocator.free(right_out);

    const thread_offsets = try allocator.alloc(usize, n_threads);
    defer allocator.free(thread_offsets);

    var offset: usize = 0;
    for (0..n_threads) |t| {
        thread_offsets[t] = offset;
        offset += thread_results[t].left.items.len;
    }

    const left_ptr = blitz.SyncPtr(IdxSize).init(left_out);
    const right_ptr = blitz.SyncPtr(IdxSize).init(right_out);

    const MergeCtx = struct {
        thread_results: []ThreadResults,
        thread_offsets: []usize,
        left_ptr: blitz.SyncPtr(IdxSize),
        right_ptr: blitz.SyncPtr(IdxSize),
    };

    var merge_ctx = MergeCtx{
        .thread_results = thread_results,
        .thread_offsets = thread_offsets,
        .left_ptr = left_ptr,
        .right_ptr = right_ptr,
    };

    blitz.parallelFor(n_threads, *MergeCtx, &merge_ctx, struct {
        fn merge(ctx: *MergeCtx, start_t: usize, end_t: usize) void {
            for (start_t..end_t) |t| {
                const result = &ctx.thread_results[t];
                const off = ctx.thread_offsets[t];
                ctx.left_ptr.copyAt(off, result.left.items);
                ctx.right_ptr.copyAt(off, result.right.items);
            }
        }
    }.merge);

    return LeftJoinResult{
        .left_indices = left_out,
        .right_indices = right_out,
        .allocator = allocator,
    };
}

// ============================================================================
// Parallel Join Entry Points
// ============================================================================

fn innerJoinParallel(
    comptime K: type,
    build_keys: []const K,
    probe_keys: []const K,
    allocator: Allocator,
) !InnerJoinResult {
    const n_partitions = getNumThreads();

    const build_result = try buildTablesParallel(K, build_keys, n_partitions, allocator);
    defer freeTables(K, build_result.tables, allocator);
    defer allocator.free(build_result.scatter_indices);
    defer allocator.free(build_result.partition_offsets);

    return probeInnerParallel(K, probe_keys, build_result.tables, n_partitions, allocator);
}

fn leftJoinParallel(
    comptime K: type,
    build_keys: []const K,
    probe_keys: []const K,
    allocator: Allocator,
) !LeftJoinResult {
    const n_partitions = getNumThreads();

    const build_result = try buildTablesParallel(K, build_keys, n_partitions, allocator);
    defer freeTables(K, build_result.tables, allocator);
    defer allocator.free(build_result.scatter_indices);
    defer allocator.free(build_result.partition_offsets);

    return probeLeftParallel(K, probe_keys, build_result.tables, n_partitions, allocator);
}

// ============================================================================
// Public API
// ============================================================================

pub fn innerJoin(
    comptime K: type,
    left_keys: []const K,
    right_keys: []const K,
    allocator: Allocator,
) !InnerJoinResult {
    // Build on smaller relation
    const swapped = right_keys.len > left_keys.len;
    const build_keys = if (swapped) left_keys else right_keys;
    const probe_keys = if (swapped) right_keys else left_keys;

    var result = if (shouldParallelize(build_keys.len + probe_keys.len))
        try innerJoinParallel(K, build_keys, probe_keys, allocator)
    else
        try innerJoinSequential(K, build_keys, probe_keys, allocator);

    if (swapped) {
        const tmp = result.left_indices;
        result.left_indices = result.right_indices;
        result.right_indices = tmp;
    }

    return result;
}

pub fn leftJoin(
    comptime K: type,
    left_keys: []const K,
    right_keys: []const K,
    allocator: Allocator,
) !LeftJoinResult {
    // For left join, always build on right
    if (shouldParallelize(right_keys.len + left_keys.len)) {
        return leftJoinParallel(K, right_keys, left_keys, allocator);
    }
    return leftJoinSequential(K, right_keys, left_keys, allocator);
}

pub fn rightJoin(
    comptime K: type,
    left_keys: []const K,
    right_keys: []const K,
    allocator: Allocator,
) !LeftJoinResult {
    var result = try leftJoin(K, right_keys, left_keys, allocator);
    const tmp = result.left_indices;
    result.left_indices = result.right_indices;
    result.right_indices = tmp;
    return result;
}

pub fn initParallel() !void {
    try blitz.init();
}

pub fn deinitParallel() void {
    blitz.deinit();
}

pub fn isParallelInitialized() bool {
    return blitz.isInitialized();
}

// ============================================================================
// Tests
// ============================================================================

test "innerJoin - basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 3, 4, 5, 6, 7 };

    var result = try innerJoin(i64, &left, &right, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.len());

    for (0..result.len()) |i| {
        const l_idx: usize = @intCast(result.left_indices[i]);
        const r_idx: usize = @intCast(result.right_indices[i]);
        try std.testing.expectEqual(left[l_idx], right[r_idx]);
    }
}

test "innerJoin - with duplicates" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 1, 2, 2 };
    const right = [_]i64{ 1, 2, 2 };

    var result = try innerJoin(i64, &left, &right, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 6), result.len());
}

test "innerJoin - no matches" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{ 4, 5, 6 };

    var result = try innerJoin(i64, &left, &right, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.len());
}

test "leftJoin - basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 3, 4, 5, 6, 7 };

    var result = try leftJoin(i64, &left, &right, allocator);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 5), result.len());
}
