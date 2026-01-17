//! Join Operations for Galleon DataFrame Library
//!
//! This module implements high-performance join algorithms using SIMD-accelerated
//! Swiss Tables. The implementation supports both single-threaded and parallel
//! execution paths, with automatic selection based on data size.
//!
//! Key features:
//! - SIMD-accelerated Swiss Table for O(1) lookups
//! - Single-pass probe with growable output (no counting pass)
//! - Partitioned parallel build for large datasets (>2M elements)
//! - Aggressive prefetching for cache efficiency

const std = @import("std");
const swiss_table = @import("swiss_table.zig");
const core = @import("core.zig");

// Re-export swiss table types
pub const JoinSwissTable = swiss_table.JoinSwissTable;
pub const IdxVec = swiss_table.IdxVec;

// ============================================================================
// Constants
// ============================================================================

/// Hash multiplier for partitioning (wyhash-style)
const DIRTY_HASH_MULT: u64 = 0x9E3779B97F4A7C15;

/// Number of partitions for parallel build
const DEFAULT_NUM_PARTITIONS: usize = 16;

/// Minimum size for parallel execution
const MIN_PARALLEL_SIZE: usize = 10_000;

/// Threshold for using partitioned build (elements)
const PARTITIONED_BUILD_THRESHOLD: usize = 2_000_000;

// ============================================================================
// Helper Functions
// ============================================================================

/// Fast hash for partitioning - just needs decent distribution
inline fn dirtyHash(key: i64) u64 {
    const k: u64 = @bitCast(key);
    return k *% DIRTY_HASH_MULT;
}

/// Map hash to partition index
inline fn hashToPartition(h: u64, num_partitions: usize) usize {
    return @as(usize, @truncate(h >> 48)) % num_partitions;
}

// ============================================================================
// Result Types
// ============================================================================

/// Result of an inner join operation
pub const InnerJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32,
    num_matches: usize,
    owns_memory: bool = true,

    pub fn deinit(self: *InnerJoinResult, allocator: std.mem.Allocator) void {
        if (self.owns_memory) {
            if (self.left_indices.len > 0) allocator.free(self.left_indices);
            if (self.right_indices.len > 0) allocator.free(self.right_indices);
        }
    }
};

/// Result of a left join operation
/// Note: right_indices uses -1 to indicate null (unmatched left rows)
pub const LeftJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32, // -1 indicates null (no match)
    num_rows: usize,
    owns_memory: bool = true,

    pub fn deinit(self: *LeftJoinResult, allocator: std.mem.Allocator) void {
        if (self.owns_memory) {
            if (self.left_indices.len > 0) allocator.free(self.left_indices);
            if (self.right_indices.len > 0) allocator.free(self.right_indices);
        }
    }
};

// ============================================================================
// Partitioned Join Tables (for parallel build)
// ============================================================================

/// Multiple Swiss Tables, one per partition
pub const PartitionedJoinTables = struct {
    tables: []JoinSwissTable,
    num_partitions: usize,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, num_partitions: usize, estimated_per_partition: usize) !Self {
        const tables = try allocator.alloc(JoinSwissTable, num_partitions);
        errdefer allocator.free(tables);

        var initialized: usize = 0;
        errdefer {
            for (tables[0..initialized]) |*t| t.deinit();
        }

        for (tables) |*t| {
            t.* = try JoinSwissTable.init(allocator, estimated_per_partition);
            initialized += 1;
        }

        return Self{
            .tables = tables,
            .num_partitions = num_partitions,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.tables) |*t| {
            t.deinit();
        }
        self.allocator.free(self.tables);
    }

    /// Get the table for a given key
    pub inline fn getTable(self: *Self, key: i64) *JoinSwissTable {
        const h = dirtyHash(key);
        const partition = hashToPartition(h, self.num_partitions);
        return &self.tables[partition];
    }

    /// Get the table (const) for a given key
    pub inline fn getTableConst(self: *const Self, key: i64) *const JoinSwissTable {
        const h = dirtyHash(key);
        const partition = hashToPartition(h, self.num_partitions);
        return &self.tables[partition];
    }

    /// Probe for a key across partitions
    pub inline fn probe(self: *const Self, key: i64) ?[]const i32 {
        const table = self.getTableConst(key);
        return table.probe(key);
    }
};

// ============================================================================
// Utility Functions
// ============================================================================

/// Check if an i64 array is sorted in ascending order
pub fn isSortedI64(data: []const i64) bool {
    if (data.len <= 1) return true;
    for (0..data.len - 1) |i| {
        if (data[i] > data[i + 1]) return false;
    }
    return true;
}

// ============================================================================
// Single-Threaded Join Implementations
// ============================================================================

/// Inner join using Swiss Table - single-threaded
/// Single-pass probe with growable output
pub fn innerJoinSwiss(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    if (left_keys.len == 0 or right_keys.len == 0) {
        return InnerJoinResult{
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .num_matches = 0,
            .owns_memory = false,
        };
    }

    // Build hash table from right side
    var table = try JoinSwissTable.init(allocator, right_keys.len);
    defer table.deinit();

    for (right_keys, 0..) |key, i| {
        try table.insertOrAppend(key, @intCast(i));
    }

    // Single-pass probe with growable output
    // Estimate: assume ~1 match per left row on average
    var capacity: usize = left_keys.len;
    var left_out = try allocator.alloc(i32, capacity);
    errdefer allocator.free(left_out);
    var right_out = try allocator.alloc(i32, capacity);
    errdefer allocator.free(right_out);

    var idx: usize = 0;
    const prefetch_distance: usize = 8;

    for (left_keys, 0..) |left_key, left_idx| {
        // Prefetch ahead
        if (left_idx + prefetch_distance < left_keys.len) {
            const future_key = left_keys[left_idx + prefetch_distance];
            _ = table.getWithPrefetch(left_key, future_key);
        }

        if (table.probe(left_key)) |right_indices| {
            // Ensure capacity
            const needed = idx + right_indices.len;
            if (needed > capacity) {
                capacity = @max(capacity * 2, needed);
                left_out = try allocator.realloc(left_out, capacity);
                right_out = try allocator.realloc(right_out, capacity);
            }

            // Write matches
            const li: i32 = @intCast(left_idx);
            for (right_indices) |ri| {
                left_out[idx] = li;
                right_out[idx] = ri;
                idx += 1;
            }
        }
    }

    // Shrink to exact size
    const final_left = if (idx < capacity and idx > 0) try allocator.realloc(left_out, idx) else left_out;
    const final_right = if (idx < capacity and idx > 0) try allocator.realloc(right_out, idx) else right_out;

    if (idx == 0) {
        allocator.free(left_out);
        allocator.free(right_out);
        return InnerJoinResult{
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .num_matches = 0,
            .owns_memory = false,
        };
    }

    return InnerJoinResult{
        .left_indices = final_left,
        .right_indices = final_right,
        .num_matches = idx,
        .owns_memory = true,
    };
}

/// Left join using Swiss Table - single-threaded
/// Single-pass probe with growable output
pub fn leftJoinSwiss(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    if (left_keys.len == 0) {
        return LeftJoinResult{
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .num_rows = 0,
            .owns_memory = false,
        };
    }

    // Build hash table from right side
    var table = try JoinSwissTable.init(allocator, @max(right_keys.len, 16));
    defer table.deinit();

    for (right_keys, 0..) |key, i| {
        try table.insertOrAppend(key, @intCast(i));
    }

    // Single-pass probe with growable output
    // Left join produces at least left_keys.len rows
    var capacity: usize = left_keys.len;
    var left_out = try allocator.alloc(i32, capacity);
    errdefer allocator.free(left_out);
    var right_out = try allocator.alloc(i32, capacity);
    errdefer allocator.free(right_out);

    var idx: usize = 0;
    const prefetch_distance: usize = 8;

    for (left_keys, 0..) |left_key, left_idx| {
        // Prefetch ahead
        if (left_idx + prefetch_distance < left_keys.len) {
            const future_key = left_keys[left_idx + prefetch_distance];
            _ = table.getWithPrefetch(left_key, future_key);
        }

        const li: i32 = @intCast(left_idx);

        if (table.probe(left_key)) |right_indices| {
            // Ensure capacity
            const needed = idx + right_indices.len;
            if (needed > capacity) {
                capacity = @max(capacity * 2, needed);
                left_out = try allocator.realloc(left_out, capacity);
                right_out = try allocator.realloc(right_out, capacity);
            }

            // Write matches
            for (right_indices) |ri| {
                left_out[idx] = li;
                right_out[idx] = ri;
                idx += 1;
            }
        } else {
            // No match - emit -1 (null indicator)
            if (idx >= capacity) {
                capacity = capacity * 2;
                left_out = try allocator.realloc(left_out, capacity);
                right_out = try allocator.realloc(right_out, capacity);
            }
            left_out[idx] = li;
            right_out[idx] = -1;
            idx += 1;
        }
    }

    // Shrink to exact size
    const final_left = if (idx < capacity) try allocator.realloc(left_out, idx) else left_out;
    const final_right = if (idx < capacity) try allocator.realloc(right_out, idx) else right_out;

    return LeftJoinResult{
        .left_indices = final_left,
        .right_indices = final_right,
        .num_rows = idx,
        .owns_memory = true,
    };
}

// ============================================================================
// Parallel Build
// ============================================================================

/// Build context for parallel table construction
const BuildContext = struct {
    keys: []const i64,
    tables: *PartitionedJoinTables,
    start: usize,
    end: usize,
    err: ?anyerror = null,
};

/// Worker function for parallel build
fn buildWorker(ctx: *BuildContext) void {
    for (ctx.start..ctx.end) |i| {
        const key = ctx.keys[i];
        const table = ctx.tables.getTable(key);
        table.insertOrAppend(key, @intCast(i)) catch |e| {
            ctx.err = e;
            return;
        };
    }
}

/// Build partitioned tables in parallel
pub fn parallelBuildPartitionedTables(
    allocator: std.mem.Allocator,
    keys: []const i64,
    num_partitions: usize,
) !PartitionedJoinTables {
    const estimated_per_partition = (keys.len / num_partitions) + 1;
    var tables = try PartitionedJoinTables.init(allocator, num_partitions, estimated_per_partition);
    errdefer tables.deinit();

    const num_threads = core.getMaxThreads();

    // For small data or single thread, use sequential build
    if (keys.len < MIN_PARALLEL_SIZE or num_threads <= 1) {
        for (keys, 0..) |key, i| {
            const table = tables.getTable(key);
            try table.insertOrAppend(key, @intCast(i));
        }
        return tables;
    }

    // Parallel build using thread pool
    const chunk_size = (keys.len + num_threads - 1) / num_threads;
    var contexts: [core.MAX_THREADS]BuildContext = undefined;
    var threads: [core.MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** core.MAX_THREADS;

    const actual_threads = @min(num_threads, (keys.len + chunk_size - 1) / chunk_size);

    // Spawn threads
    for (0..actual_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, keys.len);
        if (start >= end) break;

        contexts[t] = BuildContext{
            .keys = keys,
            .tables = &tables,
            .start = start,
            .end = end,
        };

        threads[t] = try std.Thread.spawn(.{}, buildWorker, .{&contexts[t]});
    }

    // Wait for all threads
    var had_error: ?anyerror = null;
    for (0..actual_threads) |t| {
        if (threads[t]) |thread| {
            thread.join();
            if (contexts[t].err) |e| had_error = e;
        }
    }

    if (had_error) |e| return e;
    return tables;
}

// ============================================================================
// Parallel Join Implementations
// ============================================================================

/// Probe context for parallel inner join
const InnerProbeContext = struct {
    left_keys: []const i64,
    tables: *const PartitionedJoinTables,
    left_out: []i32,
    right_out: []i32,
    start: usize,
    end: usize,
    count: usize = 0,
};

/// Worker for counting phase of parallel inner join
fn innerCountWorker(ctx: *InnerProbeContext) void {
    var count: usize = 0;
    for (ctx.start..ctx.end) |i| {
        if (ctx.tables.probe(ctx.left_keys[i])) |indices| {
            count += indices.len;
        }
    }
    ctx.count = count;
}

/// Worker for writing phase of parallel inner join
fn innerWriteWorker(ctx: *InnerProbeContext, offset: usize) void {
    var idx = offset;
    for (ctx.start..ctx.end) |i| {
        if (ctx.tables.probe(ctx.left_keys[i])) |indices| {
            const li: i32 = @intCast(i);
            for (indices) |ri| {
                ctx.left_out[idx] = li;
                ctx.right_out[idx] = ri;
                idx += 1;
            }
        }
    }
}

/// Parallel inner join using partitioned Swiss Tables
pub fn parallelInnerJoinSwiss(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    if (left_keys.len == 0 or right_keys.len == 0) {
        return InnerJoinResult{
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .num_matches = 0,
            .owns_memory = false,
        };
    }

    const num_threads = core.getMaxThreads();

    // For small data, use single-threaded version
    if (left_keys.len < MIN_PARALLEL_SIZE and right_keys.len < MIN_PARALLEL_SIZE) {
        return innerJoinSwiss(allocator, left_keys, right_keys);
    }

    // Decide build strategy based on right side size
    const use_partitioned = right_keys.len >= PARTITIONED_BUILD_THRESHOLD;
    const num_partitions: usize = if (use_partitioned) DEFAULT_NUM_PARTITIONS else 1;

    // Build phase
    var tables: PartitionedJoinTables = undefined;
    var single_table: ?JoinSwissTable = null;

    if (use_partitioned) {
        tables = try parallelBuildPartitionedTables(allocator, right_keys, num_partitions);
    } else {
        single_table = try JoinSwissTable.init(allocator, right_keys.len);
        for (right_keys, 0..) |key, i| {
            try single_table.?.insertOrAppend(key, @intCast(i));
        }
        // Wrap single table in partitioned structure for uniform probing
        tables = try PartitionedJoinTables.init(allocator, 1, 1);
        tables.tables[0].deinit();
        tables.tables[0] = single_table.?;
        single_table = null;
    }
    defer tables.deinit();

    // Parallel probe - count phase
    const chunk_size = (left_keys.len + num_threads - 1) / num_threads;
    var contexts: [core.MAX_THREADS]InnerProbeContext = undefined;
    var threads: [core.MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** core.MAX_THREADS;

    const actual_threads = @min(num_threads, (left_keys.len + chunk_size - 1) / chunk_size);

    // Count phase
    for (0..actual_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, left_keys.len);
        if (start >= end) break;

        contexts[t] = InnerProbeContext{
            .left_keys = left_keys,
            .tables = &tables,
            .left_out = &[_]i32{},
            .right_out = &[_]i32{},
            .start = start,
            .end = end,
        };

        threads[t] = try std.Thread.spawn(.{}, innerCountWorker, .{&contexts[t]});
    }

    // Wait for count phase
    for (0..actual_threads) |t| {
        if (threads[t]) |thread| thread.join();
        threads[t] = null;
    }

    // Calculate total and offsets
    var total: usize = 0;
    var offsets: [core.MAX_THREADS]usize = undefined;
    for (0..actual_threads) |t| {
        offsets[t] = total;
        total += contexts[t].count;
    }

    if (total == 0) {
        return InnerJoinResult{
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .num_matches = 0,
            .owns_memory = false,
        };
    }

    // Allocate output
    const left_out = try allocator.alloc(i32, total);
    errdefer allocator.free(left_out);
    const right_out = try allocator.alloc(i32, total);
    errdefer allocator.free(right_out);

    // Write phase
    for (0..actual_threads) |t| {
        if (contexts[t].start >= contexts[t].end) continue;
        contexts[t].left_out = left_out;
        contexts[t].right_out = right_out;
        threads[t] = try std.Thread.spawn(.{}, innerWriteWorker, .{ &contexts[t], offsets[t] });
    }

    // Wait for write phase
    for (0..actual_threads) |t| {
        if (threads[t]) |thread| thread.join();
    }

    return InnerJoinResult{
        .left_indices = left_out,
        .right_indices = right_out,
        .num_matches = total,
        .owns_memory = true,
    };
}

/// Probe context for parallel left join
const LeftProbeContext = struct {
    left_keys: []const i64,
    tables: *const PartitionedJoinTables,
    left_out: []i32,
    right_out: []i32, // -1 indicates null (no match)
    start: usize,
    end: usize,
    count: usize = 0,
};

/// Worker for counting phase of parallel left join
fn leftCountWorker(ctx: *LeftProbeContext) void {
    var count: usize = 0;
    for (ctx.start..ctx.end) |i| {
        if (ctx.tables.probe(ctx.left_keys[i])) |indices| {
            count += indices.len;
        } else {
            count += 1; // null match
        }
    }
    ctx.count = count;
}

/// Worker for writing phase of parallel left join
fn leftWriteWorker(ctx: *LeftProbeContext, offset: usize) void {
    var idx = offset;
    for (ctx.start..ctx.end) |i| {
        const li: i32 = @intCast(i);
        if (ctx.tables.probe(ctx.left_keys[i])) |indices| {
            for (indices) |ri| {
                ctx.left_out[idx] = li;
                ctx.right_out[idx] = ri;
                idx += 1;
            }
        } else {
            ctx.left_out[idx] = li;
            ctx.right_out[idx] = -1; // null indicator
            idx += 1;
        }
    }
}

/// Parallel left join using partitioned Swiss Tables
pub fn parallelLeftJoinSwiss(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    if (left_keys.len == 0) {
        return LeftJoinResult{
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .num_rows = 0,
            .owns_memory = false,
        };
    }

    const num_threads = core.getMaxThreads();

    // For small data, use single-threaded version
    if (left_keys.len < MIN_PARALLEL_SIZE and right_keys.len < MIN_PARALLEL_SIZE) {
        return leftJoinSwiss(allocator, left_keys, right_keys);
    }

    // Decide build strategy based on right side size
    const use_partitioned = right_keys.len >= PARTITIONED_BUILD_THRESHOLD;
    const num_partitions: usize = if (use_partitioned) DEFAULT_NUM_PARTITIONS else 1;

    // Build phase
    var tables: PartitionedJoinTables = undefined;
    var single_table: ?JoinSwissTable = null;

    if (use_partitioned) {
        tables = try parallelBuildPartitionedTables(allocator, right_keys, num_partitions);
    } else {
        single_table = try JoinSwissTable.init(allocator, @max(right_keys.len, 16));
        for (right_keys, 0..) |key, i| {
            try single_table.?.insertOrAppend(key, @intCast(i));
        }
        // Wrap single table in partitioned structure for uniform probing
        tables = try PartitionedJoinTables.init(allocator, 1, 1);
        tables.tables[0].deinit();
        tables.tables[0] = single_table.?;
        single_table = null;
    }
    defer tables.deinit();

    // Parallel probe - count phase
    const chunk_size = (left_keys.len + num_threads - 1) / num_threads;
    var contexts: [core.MAX_THREADS]LeftProbeContext = undefined;
    var threads: [core.MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** core.MAX_THREADS;

    const actual_threads = @min(num_threads, (left_keys.len + chunk_size - 1) / chunk_size);

    // Count phase
    for (0..actual_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, left_keys.len);
        if (start >= end) break;

        contexts[t] = LeftProbeContext{
            .left_keys = left_keys,
            .tables = &tables,
            .left_out = &[_]i32{},
            .right_out = &[_]i32{},
            .start = start,
            .end = end,
        };

        threads[t] = try std.Thread.spawn(.{}, leftCountWorker, .{&contexts[t]});
    }

    // Wait for count phase
    for (0..actual_threads) |t| {
        if (threads[t]) |thread| thread.join();
        threads[t] = null;
    }

    // Calculate total and offsets
    var total: usize = 0;
    var offsets: [core.MAX_THREADS]usize = undefined;
    for (0..actual_threads) |t| {
        offsets[t] = total;
        total += contexts[t].count;
    }

    // Allocate output
    const left_out = try allocator.alloc(i32, total);
    errdefer allocator.free(left_out);
    const right_out = try allocator.alloc(i32, total);
    errdefer allocator.free(right_out);

    // Write phase
    for (0..actual_threads) |t| {
        if (contexts[t].start >= contexts[t].end) continue;
        contexts[t].left_out = left_out;
        contexts[t].right_out = right_out;
        threads[t] = try std.Thread.spawn(.{}, leftWriteWorker, .{ &contexts[t], offsets[t] });
    }

    // Wait for write phase
    for (0..actual_threads) |t| {
        if (threads[t]) |thread| thread.join();
    }

    return LeftJoinResult{
        .left_indices = left_out,
        .right_indices = right_out,
        .num_rows = total,
        .owns_memory = true,
    };
}

// ============================================================================
// Legacy API Wrappers
// ============================================================================

/// Inner join - legacy API (delegates to Swiss Table implementation)
pub fn innerJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    return innerJoinSwiss(allocator, left_keys, right_keys);
}

/// Left join - legacy API (delegates to Swiss Table implementation)
pub fn leftJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    return leftJoinSwiss(allocator, left_keys, right_keys);
}

/// Parallel inner join - legacy API
pub fn parallelInnerJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    return parallelInnerJoinSwiss(allocator, left_keys, right_keys);
}

/// Parallel left join - legacy API
pub fn parallelLeftJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    return parallelLeftJoinSwiss(allocator, left_keys, right_keys);
}

// ============================================================================
// Tests
// ============================================================================

test "inner join - basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try innerJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.num_matches);
}

test "inner join - empty" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{};

    var result = try innerJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), result.num_matches);
}

test "inner join - duplicates" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 1, 2 };
    const right = [_]i64{ 1, 1, 1 };

    var result = try innerJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    // Each left 1 matches 3 right 1s = 6 total
    try std.testing.expectEqual(@as(usize, 6), result.num_matches);
}

test "left join - basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try leftJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), result.num_rows);

    // Check nulls (-1) for non-matching keys
    var null_count: usize = 0;
    for (result.right_indices[0..result.num_rows]) |ri| {
        if (ri == -1) null_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), null_count);
}

test "left join - empty right" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{};

    var result = try leftJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_rows);

    // All should be -1 (null indicator)
    for (result.right_indices[0..result.num_rows]) |ri| {
        try std.testing.expect(ri == -1);
    }
}

test "parallel inner join - basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try parallelInnerJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.num_matches);
}

test "parallel left join - basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try parallelLeftJoinSwiss(allocator, &left, &right);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), result.num_rows);
}

test "isSortedI64" {
    try std.testing.expect(isSortedI64(&[_]i64{}));
    try std.testing.expect(isSortedI64(&[_]i64{1}));
    try std.testing.expect(isSortedI64(&[_]i64{ 1, 2, 3, 4, 5 }));
    try std.testing.expect(isSortedI64(&[_]i64{ 1, 1, 2, 2, 3 }));
    try std.testing.expect(!isSortedI64(&[_]i64{ 1, 3, 2 }));
    try std.testing.expect(!isSortedI64(&[_]i64{ 5, 4, 3, 2, 1 }));
}
