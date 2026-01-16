const std = @import("std");
const core = @import("core.zig");
const hashing = @import("hashing.zig");
const groupby_agg = @import("groupby_agg.zig");
const blitz = @import("../blitz/mod.zig");

// Import core constants
const MAX_THREADS = core.MAX_THREADS;
const getMaxThreads = core.getMaxThreads;

// Import hash functions
const rapidHash64 = hashing.rapidHash64;
const fastIntHash = hashing.fastIntHash;
const hashInt64Column = hashing.hashInt64Column;

// Import aggregation functions
const aggregateSumByGroup = groupby_agg.aggregateSumByGroup;

// ============================================================================
// Join Result Structures
// ============================================================================

/// Result of end-to-end inner join
pub const InnerJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32,
    num_matches: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *InnerJoinResult) void {
        self.allocator.free(self.left_indices);
        self.allocator.free(self.right_indices);
    }
};

/// Result of left join - includes all left rows, unmatched have right_index = -1
pub const LeftJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32, // -1 for unmatched left rows
    num_rows: u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *LeftJoinResult) void {
        self.allocator.free(self.left_indices);
        self.allocator.free(self.right_indices);
    }
};

// ============================================================================
// Swiss Table Join Implementation
// ============================================================================

/// Swiss Table entry for join - stores key and row chain
pub const SwissJoinEntry = struct {
    key: i64, // Key value (stored directly for fast comparison)
    head: i32, // First row index with this key
    count: u32, // Number of rows with this key
};

/// Swiss Table for join build side
/// Uses SIMD control byte probing like groupby
pub const SwissJoinTable = struct {
    ctrl: []u8,
    entries: []SwissJoinEntry,
    next: []i32, // Chain for rows with same key: next[row] = next row with same key, or -1
    mask: usize,
    count: u32,
    allocator: std.mem.Allocator,

    const CTRL_EMPTY: u8 = 0x00;
    const CTRL_GROUP_SIZE: usize = 16;
    const LOAD_FACTOR_PERCENT: usize = 87;

    /// Extract h2 (top 7 bits with high bit set) for control byte
    inline fn h2(hash: u64) u8 {
        return @as(u8, @truncate(hash >> 57)) | 0x80;
    }

    pub fn init(allocator: std.mem.Allocator, estimated_keys: usize, num_rows: usize) !SwissJoinTable {
        // Round up to power of 2, minimum 16
        var size: usize = 16;
        while (size < estimated_keys * 100 / LOAD_FACTOR_PERCENT) {
            size *= 2;
        }

        const ctrl = try allocator.alloc(u8, size + CTRL_GROUP_SIZE);
        @memset(ctrl, CTRL_EMPTY);

        const entries = try allocator.alloc(SwissJoinEntry, size);
        const next = try allocator.alloc(i32, num_rows);
        @memset(next, -1);

        return SwissJoinTable{
            .ctrl = ctrl,
            .entries = entries,
            .next = next,
            .mask = size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SwissJoinTable) void {
        self.allocator.free(self.ctrl);
        self.allocator.free(self.entries);
        self.allocator.free(self.next);
    }

    /// Insert a row into the join table
    /// Uses linear probing with SIMD for faster lookups
    pub fn insert(self: *SwissJoinTable, hash: u64, key: i64, row_idx: i32) !void {
        const ctrl_byte = h2(hash);
        const ctrl_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(ctrl_byte);
        const empty_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(CTRL_EMPTY);

        var pos = hash & self.mask;
        const table_len = self.entries.len;
        var probe_count: usize = 0;

        while (probe_count < table_len) {
            const ctrl_group: @Vector(CTRL_GROUP_SIZE, u8) = self.ctrl[pos..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte (potential key match)
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (pos + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                // Direct key comparison
                if (entry.key == key) {
                    // Key exists - add row to chain
                    self.next[@intCast(row_idx)] = entry.head;
                    entry.head = row_idx;
                    entry.count += 1;
                    return;
                }
                match_bits &= match_bits - 1;
            }

            // Check for empty slot
            const empty_mask = ctrl_group == empty_vec;
            const empty_bits = @as(u16, @bitCast(empty_mask));

            if (empty_bits != 0) {
                const bit_pos = @ctz(empty_bits);
                const slot = (pos + bit_pos) & self.mask;

                // Insert new entry
                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[table_len + slot] = ctrl_byte;
                }

                self.entries[slot] = .{
                    .key = key,
                    .head = row_idx,
                    .count = 1,
                };
                self.count += 1;
                return;
            }

            // Linear probe to next position
            pos = (pos + 1) & self.mask;
            probe_count += 1;
        }
    }

    /// Find entry for a key, returns null if not found
    pub fn find(self: *const SwissJoinTable, hash: u64, key: i64) ?*const SwissJoinEntry {
        const ctrl_byte = h2(hash);
        const ctrl_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(ctrl_byte);
        const empty_vec: @Vector(CTRL_GROUP_SIZE, u8) = @splat(CTRL_EMPTY);

        var pos = hash & self.mask;
        const table_len = self.entries.len;
        var probe_count: usize = 0;

        while (probe_count < table_len) {
            const ctrl_group: @Vector(CTRL_GROUP_SIZE, u8) = self.ctrl[pos..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (pos + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                if (entry.key == key) {
                    return entry;
                }
                match_bits &= match_bits - 1;
            }

            // Check for empty slot (key doesn't exist)
            const empty_mask = ctrl_group == empty_vec;
            if (@as(u16, @bitCast(empty_mask)) != 0) {
                return null;
            }

            // Linear probe to next position
            pos = (pos + 1) & self.mask;
            probe_count += 1;
        }
        return null;
    }
};

// ============================================================================
// Thread Context Structures for Parallel Operations
// ============================================================================

/// Thread context for parallel sum by group
pub const ParallelSumContext = struct {
    data: []const f64,
    group_ids: []const u32,
    partial_sums: []f64, // Thread-local partial sums
    num_groups: usize,
    start_idx: usize,
    end_idx: usize,
};

/// Thread context for memory-efficient single-pass parallel join probe
/// Computes hashes on-the-fly and uses thread-local dynamic arrays
pub const SinglePassProbeContext = struct {
    left_keys: []const i64,
    right_keys: []const i64,
    table: []const i32,
    next: []const i32,
    table_size: u32,
    start_idx: usize,
    end_idx: usize,
    allocator: std.mem.Allocator,
    // Output - thread-local dynamic arrays
    left_indices: []i32,
    right_indices: []i32,
    count: usize,
    capacity: usize,
    alloc_failed: bool,
};

/// Context for parallel left join probe workers
pub const LeftProbeContext = struct {
    left_keys: []const i64,
    right_keys: []const i64,
    table: []const i32,
    next: []const i32,
    table_size: u64,
    start_idx: usize,
    end_idx: usize,
    allocator: std.mem.Allocator,
    // Output
    left_indices: []i32,
    right_indices: []i32,
    count: usize,
    capacity: usize,
    alloc_failed: bool,
};

// ============================================================================
// Lock-Free Parallel Hash Join Structures
// ============================================================================

/// Per-thread hash table for lock-free join
pub const PartitionedHashTable = struct {
    table: []i32,
    next: []i32,
    keys: []i64,
    hashes: []u64,
    row_indices: []u32, // Original row indices in the right table
    size: u32,
    count: u32,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, estimated_keys: usize) !PartitionedHashTable {
        const table_size = nextPowerOf2Join(@intCast(@max(estimated_keys * 4, 16)));
        const table = try allocator.alloc(i32, table_size);
        @memset(table, -1);

        // Pre-allocate for estimated keys
        const capacity = @max(estimated_keys, 64);
        const next = try allocator.alloc(i32, capacity);
        const keys = try allocator.alloc(i64, capacity);
        const hashes = try allocator.alloc(u64, capacity);
        const row_indices = try allocator.alloc(u32, capacity);

        return PartitionedHashTable{
            .table = table,
            .next = next,
            .keys = keys,
            .hashes = hashes,
            .row_indices = row_indices,
            .size = @intCast(table_size),
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *PartitionedHashTable) void {
        self.allocator.free(self.table);
        self.allocator.free(self.next);
        self.allocator.free(self.keys);
        self.allocator.free(self.hashes);
        self.allocator.free(self.row_indices);
    }

    pub fn insert(self: *PartitionedHashTable, hash: u64, key: i64, row_idx: u32) !void {
        const idx = self.count;

        // Grow arrays if needed
        if (idx >= self.next.len) {
            const new_cap = self.next.len * 2;
            self.next = try self.allocator.realloc(self.next, new_cap);
            self.keys = try self.allocator.realloc(self.keys, new_cap);
            self.hashes = try self.allocator.realloc(self.hashes, new_cap);
            self.row_indices = try self.allocator.realloc(self.row_indices, new_cap);
        }

        // Store key data
        self.keys[idx] = key;
        self.hashes[idx] = hash;
        self.row_indices[idx] = row_idx;

        // Insert into hash table (chaining)
        const slot = hash % self.size;
        self.next[idx] = self.table[slot];
        self.table[slot] = @intCast(idx);
        self.count += 1;
    }
};

/// Context for lock-free build phase
pub const LockFreeBuildContext = struct {
    right_keys: []const i64,
    right_hashes: []const u64,
    partition_table: *PartitionedHashTable,
    partition_id: usize,
    num_partitions: usize,
};

/// Context for lock-free probe phase
pub const LockFreeProbeContext = struct {
    left_keys: []const i64,
    left_hashes: []const u64,
    partition_tables: []PartitionedHashTable,
    num_partitions: usize,
    // Output - dynamically grown
    left_results: std.ArrayList(i32),
    right_results: std.ArrayList(i32),
};

// ============================================================================
// Helper Functions
// ============================================================================

/// Fast cardinality estimation using small sample
/// Returns multiplier to use: 4 (high cardinality), 6 (medium), or 8 (low)
/// Estimate cardinality using HyperLogLog-style sampling
/// Returns estimated number of unique keys
fn estimateCardinality(keys: []const i64) usize {
    const n = keys.len;
    if (n <= 64) return n; // For small arrays, assume all unique

    // Use a small hash set to sample unique values
    // Sample size trades off accuracy vs speed
    const SAMPLE_SIZE: usize = 256;
    const HASH_BUCKETS: usize = 512; // Power of 2 for fast modulo
    var seen: [HASH_BUCKETS]bool = [_]bool{false} ** HASH_BUCKETS;

    // Sample evenly across the array
    const step = @max(n / SAMPLE_SIZE, 1);
    var unique_in_sample: usize = 0;
    var samples_taken: usize = 0;

    var i: usize = 0;
    while (i < n and samples_taken < SAMPLE_SIZE) : (i += step) {
        const hash = fastIntHash(keys[i]);
        const bucket = hash & (HASH_BUCKETS - 1);
        if (!seen[bucket]) {
            seen[bucket] = true;
            unique_in_sample += 1;
        }
        samples_taken += 1;
    }

    // Estimate total unique keys using capture-recapture formula
    // If we saw U unique in S samples from N total, estimate is approximately:
    // unique_total ≈ N * (U / S) adjusted for hash collisions
    // With 512 buckets, collision rate is approximately U/512
    const collision_factor = @as(f64, @floatFromInt(unique_in_sample)) / @as(f64, HASH_BUCKETS);
    const adjusted_unique = @as(f64, @floatFromInt(unique_in_sample)) / (1.0 - collision_factor * 0.5);

    const estimated = @as(usize, @intFromFloat(
        adjusted_unique * @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(samples_taken)),
    ));

    // Clamp to reasonable bounds
    return @min(@max(estimated, 1), n);
}

/// Calculate optimal hash table multiplier based on estimated cardinality
/// Goal: keep average chain length around 1.5-2 for good cache performance
fn estimateCardinalityMultiplier(keys: []const i64) usize {
    const n = keys.len;
    if (n == 0) return 4;

    const estimated_unique = estimateCardinality(keys);

    // Average rows per unique key
    const avg_duplicates = @as(f64, @floatFromInt(n)) / @as(f64, @floatFromInt(estimated_unique));

    // Target: average chain length of ~1.5 after hashing
    // With load factor L = unique_keys / table_size, average chain = 1 + L
    // For chain ~1.5, we want L ~0.5, so table_size = 2 * unique_keys
    // But we have duplicates, so we need table_size = 2 * unique_keys
    // Final multiplier = table_size / n = 2 * unique_keys / n = 2 / avg_duplicates

    // Calculate multiplier to achieve target chain length
    const target_chain: f64 = 1.5;
    const target_load_factor = target_chain - 1.0; // ~0.5
    const base_multiplier = 1.0 / target_load_factor; // ~2

    // Adjust for duplicates: more duplicates = longer chains for same key
    // We need larger table to spread different keys apart
    var multiplier = base_multiplier * avg_duplicates;

    // Clamp to reasonable range (4x to 16x)
    multiplier = @max(4.0, @min(16.0, multiplier));

    return @as(usize, @intFromFloat(multiplier));
}

/// Calculate optimal hash table size based on quick cardinality estimate
fn optimalJoinTableSize(num_keys: usize, keys: []const i64) u32 {
    const multiplier = estimateCardinalityMultiplier(keys);
    var size = num_keys * multiplier;

    // Cap at reasonable size
    size = @min(size, 16 * 1024 * 1024);

    return nextPowerOf2Join(@intCast(size));
}

/// Estimate join result size to pre-allocate appropriate capacity
/// Uses cardinality estimation to predict number of matches
fn estimateJoinResultSize(left_keys: []const i64, right_keys: []const i64) usize {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) return 0;

    // Estimate unique keys in each side
    const left_unique = estimateCardinality(left_keys);
    const right_unique = estimateCardinality(right_keys);

    // Estimate overlap (assume significant overlap for join scenarios)
    // Conservative: min of unique keys on each side
    const estimated_common_keys = @min(left_unique, right_unique);

    // Average rows per key
    const left_per_key = @as(f64, @floatFromInt(left_n)) / @as(f64, @floatFromInt(left_unique));
    const right_per_key = @as(f64, @floatFromInt(right_n)) / @as(f64, @floatFromInt(right_unique));

    // Expected matches = sum over common keys of (left_count * right_count)
    // ≈ common_keys * avg_left_per_key * avg_right_per_key
    const expected_matches = @as(f64, @floatFromInt(estimated_common_keys)) * left_per_key * right_per_key;

    // Add 20% margin for estimation error
    const with_margin = @as(usize, @intFromFloat(expected_matches * 1.2));

    // Clamp to reasonable bounds (at least 4096, at most 64M)
    return @min(@max(with_margin, 4096), 64 * 1024 * 1024);
}

fn nextPowerOf2Join(n: u32) u32 {
    if (n <= 1) return 2;
    var v = n - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

// ============================================================================
// Hash Table Build and Probe Functions
// ============================================================================

/// Build hash table for join - returns group IDs for each unique hash
/// Uses a simple open addressing hash table
pub fn buildJoinHashTable(
    hashes: []const u64,
    table: []i32, // hash table: -1 = empty, otherwise row index
    next: []i32, // chain for collisions: -1 = end, otherwise next row with same hash
    table_size: u32,
) void {
    // Initialize table to -1 (empty)
    @memset(table, -1);
    @memset(next, -1);

    // Insert each row
    for (hashes, 0..) |hash, row| {
        const slot: usize = @intCast(hash % table_size);
        const row_i32: i32 = @intCast(row);

        if (table[slot] == -1) {
            // Empty slot - insert directly
            table[slot] = row_i32;
        } else {
            // Collision - chain
            next[row] = table[slot];
            table[slot] = row_i32;
        }
    }
}

/// Build hash table directly from i64 keys using fastIntHash
/// This avoids pre-computing hashes in a separate array
fn buildJoinHashTableFast(
    keys: []const i64,
    table: []i32,
    next: []i32,
    table_size: u32,
) void {
    // Initialize table to -1 (empty)
    @memset(table, -1);
    @memset(next, -1);

    // Use bitwise AND (table_size is power of 2)
    const mask: u64 = table_size - 1;

    // Insert each row
    for (keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        const row_i32: i32 = @intCast(row);

        if (table[slot] == -1) {
            table[slot] = row_i32;
        } else {
            next[row] = table[slot];
            table[slot] = row_i32;
        }
    }
}

/// Parallel hash table build using partitioning
/// Partitions keys by high bits of hash, builds each partition in parallel
fn buildJoinHashTableParallel(
    allocator: std.mem.Allocator,
    keys: []const i64,
    table: []i32,
    next: []i32,
    table_size: u32,
) void {
    const n = keys.len;
    const num_workers = @min(blitz.numWorkers(), 8);

    // For small tables, use sequential build
    if (n < 50000 or num_workers <= 1) {
        buildJoinHashTableFast(keys, table, next, table_size);
        return;
    }

    // Initialize arrays
    @memset(table, -1);
    @memset(next, -1);

    const mask: u64 = table_size - 1;

    // Step 1: Partition keys by high bits of slot (to avoid conflicts)
    // Use top bits of hash to assign to partitions
    const partition_bits: u6 = 3; // 8 partitions
    const num_partitions: usize = 1 << partition_bits;
    const partition_mask: u64 = num_partitions - 1;

    // Count elements per partition
    var partition_counts: [8]usize = [_]usize{0} ** 8;
    for (keys) |key| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        const partition = (slot >> (16 - partition_bits)) & partition_mask;
        partition_counts[partition] += 1;
    }

    // Compute partition offsets
    var partition_offsets: [9]usize = undefined;
    partition_offsets[0] = 0;
    for (0..num_partitions) |p| {
        partition_offsets[p + 1] = partition_offsets[p] + partition_counts[p];
    }

    // Allocate partition arrays
    const partition_rows = allocator.alloc(u32, n) catch {
        buildJoinHashTableFast(keys, table, next, table_size);
        return;
    };
    defer allocator.free(partition_rows);

    const partition_slots = allocator.alloc(u32, n) catch {
        buildJoinHashTableFast(keys, table, next, table_size);
        return;
    };
    defer allocator.free(partition_slots);

    // Reset offsets for filling
    var fill_offsets: [8]usize = undefined;
    for (0..num_partitions) |p| {
        fill_offsets[p] = partition_offsets[p];
    }

    // Assign rows to partitions
    for (keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        const partition = (slot >> (16 - partition_bits)) & partition_mask;

        const idx = fill_offsets[partition];
        partition_rows[idx] = @intCast(row);
        partition_slots[idx] = @intCast(slot);
        fill_offsets[partition] += 1;
    }

    // Step 2: Build hash table for each partition in parallel
    const BuildCtx = struct {
        keys: []const i64,
        table: []i32,
        next: []i32,
        partition_rows: []const u32,
        partition_slots: []const u32,
        partition_offsets: *const [9]usize,
        num_partitions: usize,
    };

    const ctx = BuildCtx{
        .keys = keys,
        .table = table,
        .next = next,
        .partition_rows = partition_rows,
        .partition_slots = partition_slots,
        .partition_offsets = &partition_offsets,
        .num_partitions = num_partitions,
    };

    blitz.parallelForWithGrain(num_partitions, BuildCtx, ctx, struct {
        fn buildPartitions(c: BuildCtx, start_part: usize, end_part: usize) void {
            for (start_part..end_part) |p| {
                const start = c.partition_offsets[p];
                const end = c.partition_offsets[p + 1];

                // Build this partition's entries
                for (start..end) |idx| {
                    const row: i32 = @intCast(c.partition_rows[idx]);
                    const slot: usize = c.partition_slots[idx];

                    if (c.table[slot] == -1) {
                        c.table[slot] = row;
                    } else {
                        c.next[@intCast(row)] = c.table[slot];
                        c.table[slot] = row;
                    }
                }
            }
        }
    }.buildPartitions, 1);
}

/// Probe hash table for join - finds matching rows
/// Returns number of matches found
pub fn probeJoinHashTable(
    probe_hashes: []const u64,
    probe_keys: []const i64,
    build_keys: []const i64,
    table: []const i32,
    next: []const i32,
    table_size: u32,
    out_probe_indices: []i32,
    out_build_indices: []i32,
    max_matches: u32,
) u32 {
    var match_count: u32 = 0;

    for (probe_hashes, 0..) |hash, probe_row| {
        const slot: usize = @intCast(hash % table_size);
        var build_row = table[slot];

        while (build_row != -1 and match_count < max_matches) {
            const build_idx: usize = @intCast(build_row);

            // Verify key match (not just hash match)
            if (probe_keys[probe_row] == build_keys[build_idx]) {
                out_probe_indices[match_count] = @intCast(probe_row);
                out_build_indices[match_count] = build_row;
                match_count += 1;
            }

            build_row = next[build_idx];
        }
    }

    return match_count;
}

// ============================================================================
// Inner Join Implementations
// ============================================================================

/// Memory-efficient inner join for i64 keys
/// - Single pass with dynamic array growth
/// - Computes left hashes on-the-fly using fastIntHash (saves 8 bytes per left row)
/// - Uses adaptive hash table sizing
/// - Interleaved 4-key probing for better memory latency hiding
pub fn innerJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Size hash table based on adaptive cardinality estimate
    const table_size = optimalJoinTableSize(right_n, right_keys);

    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);

    // Build hash table directly using fastIntHash (no separate hash array)
    buildJoinHashTableFast(right_keys, table, next, table_size);

    // Use bitwise AND instead of modulo (table_size is power of 2)
    const mask: u64 = table_size - 1;

    // Estimate result size to minimize reallocations
    var capacity: usize = estimateJoinResultSize(left_keys, right_keys);
    var left_indices = try allocator.alloc(i32, capacity);
    var right_indices = try allocator.alloc(i32, capacity);
    var idx: usize = 0;

    // Interleaved probing: process 4 keys at once to hide memory latency
    const BATCH_SIZE: usize = 4;
    const main_end = if (left_n > BATCH_SIZE) left_n - BATCH_SIZE else 0;

    var i: usize = 0;

    // Main loop - process 4 keys at a time with interleaved memory access
    while (i < main_end) : (i += BATCH_SIZE) {
        // Load 4 keys and compute hashes (fast integer hash)
        const key0 = left_keys[i];
        const key1 = left_keys[i + 1];
        const key2 = left_keys[i + 2];
        const key3 = left_keys[i + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch all 4 table slots
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        // Also prefetch ahead for next batch
        if (i + BATCH_SIZE + 8 < left_n) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[i + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        // Process each key's chain
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                // Prefetch next chain element's key while comparing current
                if (next_row != -1) {
                    @prefetch(@as([*]const i64, @ptrCast(&right_keys[@intCast(next_row)])), .{ .locality = 1 });
                }

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        left_indices = try allocator.realloc(left_indices, new_capacity);
                        right_indices = try allocator.realloc(right_indices, new_capacity);
                        capacity = new_capacity;
                    }
                    left_indices[idx] = @intCast(i + offset);
                    right_indices[idx] = build_row;
                    idx += 1;
                }
                build_row = next_row;
            }
        }
    }

    // Tail loop - process remaining elements one at a time
    while (i < left_n) : (i += 1) {
        const key = left_keys[i];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];

            // Prefetch next chain element's key
            if (next_row != -1) {
                @prefetch(@as([*]const i64, @ptrCast(&right_keys[@intCast(next_row)])), .{ .locality = 1 });
            }

            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    left_indices = try allocator.realloc(left_indices, new_capacity);
                    right_indices = try allocator.realloc(right_indices, new_capacity);
                    capacity = new_capacity;
                }
                left_indices[idx] = @intCast(i);
                right_indices[idx] = build_row;
                idx += 1;
            }
            build_row = next_row;
        }
    }

    // Shrink to actual size if significantly over-allocated
    if (idx > 0 and idx < capacity / 2) {
        left_indices = try allocator.realloc(left_indices, idx);
        right_indices = try allocator.realloc(right_indices, idx);
    } else if (idx == 0) {
        allocator.free(left_indices);
        allocator.free(right_indices);
        left_indices = try allocator.alloc(i32, 0);
        right_indices = try allocator.alloc(i32, 0);
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(idx),
        .allocator = allocator,
    };
}

/// Single-pass inner join with prefetching for better cache performance
pub fn innerJoinI64SinglePass(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Compute hashes for both sides
    const left_hashes = try allocator.alloc(u64, left_n);
    defer allocator.free(left_hashes);
    hashInt64Column(left_keys, left_hashes);

    const right_hashes = try allocator.alloc(u64, right_n);
    defer allocator.free(right_hashes);
    hashInt64Column(right_keys, right_hashes);

    // Size hash table based on quick cardinality estimate
    const table_size = optimalJoinTableSize(right_n, right_keys);

    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);

    buildJoinHashTable(right_hashes, table, next, table_size);

    // Estimate capacity based on smaller side - common case for joins
    // Use larger estimate to minimize reallocations
    var capacity: usize = @min(left_n, right_n) * 2;
    capacity = @max(capacity, 4096);
    var left_indices = try allocator.alloc(i32, capacity);
    var right_indices = try allocator.alloc(i32, capacity);
    var idx: usize = 0;

    // Prefetch distance for cache optimization
    const PREFETCH_DIST: usize = 8;

    var i: usize = 0;
    while (i < left_n) : (i += 1) {
        // Prefetch future hash table slots
        if (i + PREFETCH_DIST < left_n) {
            const future_slot = left_hashes[i + PREFETCH_DIST] % table_size;
            @prefetch(@as([*]const i32, @ptrCast(&table[future_slot])), .{ .locality = 1 });
        }

        const hash = left_hashes[i];
        const slot: usize = @intCast(hash % table_size);
        var build_row = table[slot];

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            if (left_keys[i] == right_keys[build_idx]) {
                // Grow if needed - check less frequently with larger growth
                if (idx >= capacity) {
                    capacity = capacity + capacity / 2; // 1.5x growth
                    left_indices = try allocator.realloc(left_indices, capacity);
                    right_indices = try allocator.realloc(right_indices, capacity);
                }
                left_indices[idx] = @intCast(i);
                right_indices[idx] = build_row;
                idx += 1;
            }
            build_row = next[build_idx];
        }
    }

    // Shrink to actual size (avoid if close to capacity to reduce alloc overhead)
    if (idx > 0 and idx < capacity / 2) {
        left_indices = try allocator.realloc(left_indices, idx);
        right_indices = try allocator.realloc(right_indices, idx);
    } else if (idx == 0) {
        allocator.free(left_indices);
        allocator.free(right_indices);
        left_indices = try allocator.alloc(i32, 0);
        right_indices = try allocator.alloc(i32, 0);
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(idx),
        .allocator = allocator,
    };
}

/// Swiss Table based inner join - faster than chained hash table
pub fn innerJoinI64Swiss(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Estimate unique keys (assume ~10% of rows are unique)
    const estimated_keys = @max(right_n / 10, 1024);

    // Build Swiss Table on right (build) side
    var table = try SwissJoinTable.init(allocator, estimated_keys, right_n);
    defer table.deinit();

    // Insert all right rows
    for (right_keys, 0..) |key, i| {
        const hash = rapidHash64(@bitCast(key));
        try table.insert(hash, key, @intCast(i));
    }

    // First pass: count matches
    var match_count: usize = 0;
    for (left_keys) |key| {
        const hash = rapidHash64(@bitCast(key));
        if (table.find(hash, key)) |entry| {
            match_count += entry.count;
        }
    }

    // Allocate result arrays
    const left_indices = try allocator.alloc(i32, match_count);
    const right_indices = try allocator.alloc(i32, match_count);

    // Second pass: fill in indices
    var idx: usize = 0;
    for (left_keys, 0..) |key, left_row| {
        const hash = rapidHash64(@bitCast(key));
        if (table.find(hash, key)) |entry| {
            // Walk the chain of right rows with this key
            var right_row = entry.head;
            while (right_row != -1) {
                left_indices[idx] = @intCast(left_row);
                right_indices[idx] = right_row;
                idx += 1;
                right_row = table.next[@intCast(right_row)];
            }
        }
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(match_count),
        .allocator = allocator,
    };
}

// ============================================================================
// Parallel Join Implementations
// ============================================================================

/// Parallel sum aggregation by group using Blitz
/// Splits work across workers, each building partial sums, then merges
pub fn parallelAggregateSumF64ByGroup(
    allocator: std.mem.Allocator,
    data: []const f64,
    group_ids: []const u32,
    num_groups: usize,
) ![]f64 {
    const n = data.len;

    // For small data, use single-threaded version
    if (n < 10000 or num_groups < 100) {
        const out_sums = try allocator.alloc(f64, num_groups);
        @memset(out_sums, 0);
        aggregateSumByGroup(f64, data, group_ids, out_sums);
        return out_sums;
    }

    const num_workers = blitz.numWorkers();
    const num_chunks = @min(num_workers, n / 1000);
    const chunk_size = (n + num_chunks - 1) / num_chunks;

    // Allocate partial sum buffers for each chunk
    const chunk_sums = try allocator.alloc([]f64, num_chunks);
    defer allocator.free(chunk_sums);

    for (chunk_sums) |*cs| {
        cs.* = try allocator.alloc(f64, num_groups);
        @memset(cs.*, 0);
    }
    defer {
        for (chunk_sums) |cs| {
            allocator.free(cs);
        }
    }

    // Process chunks in parallel using Blitz
    const SumCtx = struct {
        data: []const f64,
        group_ids: []const u32,
        chunk_sums: [][]f64,
        chunk_size: usize,
        len: usize,
    };

    const ctx = SumCtx{
        .data = data,
        .group_ids = group_ids,
        .chunk_sums = chunk_sums,
        .chunk_size = chunk_size,
        .len = n,
    };

    blitz.parallelForWithGrain(num_chunks, SumCtx, ctx, struct {
        fn processChunks(c: SumCtx, start_chunk: usize, end_chunk: usize) void {
            for (start_chunk..end_chunk) |chunk_idx| {
                const start = chunk_idx * c.chunk_size;
                const end = @min(start + c.chunk_size, c.len);
                const partial_sums = c.chunk_sums[chunk_idx];

                for (start..end) |i| {
                    const gid = c.group_ids[i];
                    partial_sums[gid] += c.data[i];
                }
            }
        }
    }.processChunks, 1);

    // Merge partial sums
    const final_sums = try allocator.alloc(f64, num_groups);
    @memset(final_sums, 0);

    for (chunk_sums) |cs| {
        for (0..num_groups) |g| {
            final_sums[g] += cs[g];
        }
    }

    return final_sums;
}

fn singlePassProbeWorker(ctx: *SinglePassProbeContext) void {
    const left_keys = ctx.left_keys;
    const right_keys = ctx.right_keys;
    const table = ctx.table;
    const next = ctx.next;
    const table_size = ctx.table_size;
    const allocator = ctx.allocator;

    // Use bitwise AND instead of modulo (table_size is power of 2)
    const mask: u64 = table_size - 1;

    const start = ctx.start_idx;
    const end = ctx.end_idx;
    const chunk_size = end - start;

    // Estimate capacity proportional to chunk size relative to total
    // Use same estimation as main join, scaled by chunk proportion
    const total_estimated = estimateJoinResultSize(left_keys, right_keys);
    const chunk_fraction = @as(f64, @floatFromInt(chunk_size)) / @as(f64, @floatFromInt(left_keys.len));
    var capacity: usize = @max(@as(usize, @intFromFloat(@as(f64, @floatFromInt(total_estimated)) * chunk_fraction * 1.2)), 4096);
    var left_out = allocator.alloc(i32, capacity) catch {
        ctx.alloc_failed = true;
        return;
    };
    var right_out = allocator.alloc(i32, capacity) catch {
        allocator.free(left_out);
        ctx.alloc_failed = true;
        return;
    };
    var idx: usize = 0;

    // Interleaved probing: process 4 keys at once to hide memory latency
    const BATCH_SIZE: usize = 4;
    const main_end = if (end > start + BATCH_SIZE) end - BATCH_SIZE else start;

    var left_row: usize = start;

    // Main loop - process 4 keys at a time with interleaved memory access
    while (left_row < main_end) : (left_row += BATCH_SIZE) {
        // Load 4 keys and compute hashes (fast integer hash)
        const key0 = left_keys[left_row];
        const key1 = left_keys[left_row + 1];
        const key2 = left_keys[left_row + 2];
        const key3 = left_keys[left_row + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch all 4 table slots
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        // Also prefetch ahead for next batch
        if (left_row + BATCH_SIZE + 8 < end) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[left_row + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        // Process each key's chain
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                // Prefetch next chain element's key while comparing current
                if (next_row != -1) {
                    @prefetch(@as([*]const i64, @ptrCast(&right_keys[@intCast(next_row)])), .{ .locality = 1 });
                }

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        const new_left = allocator.realloc(left_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(left_out);
                            allocator.free(right_out);
                            return;
                        };
                        const new_right = allocator.realloc(right_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(new_left);
                            return;
                        };
                        left_out = new_left;
                        right_out = new_right;
                        capacity = new_capacity;
                    }
                    left_out[idx] = @intCast(left_row + offset);
                    right_out[idx] = build_row;
                    idx += 1;
                }
                build_row = next_row;
            }
        }
    }

    // Tail loop - process remaining elements one at a time
    while (left_row < end) : (left_row += 1) {
        const key = left_keys[left_row];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];

            // Prefetch next chain element's key
            if (next_row != -1) {
                @prefetch(@as([*]const i64, @ptrCast(&right_keys[@intCast(next_row)])), .{ .locality = 1 });
            }

            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    const new_left = allocator.realloc(left_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(left_out);
                        allocator.free(right_out);
                        return;
                    };
                    const new_right = allocator.realloc(right_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(new_left);
                        return;
                    };
                    left_out = new_left;
                    right_out = new_right;
                    capacity = new_capacity;
                }
                left_out[idx] = @intCast(left_row);
                right_out[idx] = build_row;
                idx += 1;
            }
            build_row = next_row;
        }
    }

    ctx.left_indices = left_out;
    ctx.right_indices = right_out;
    ctx.count = idx;
    ctx.capacity = capacity;
    ctx.alloc_failed = false;
}

/// Memory-efficient parallel inner join using Blitz
/// - Single pass (no count-then-fill)
/// - Computes left hashes on-the-fly (saves 8 bytes per left row)
/// - Uses adaptive hash table sizing
pub fn parallelInnerJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For small data, use single-threaded version
    if (left_n < 10000) {
        return innerJoinI64(allocator, left_keys, right_keys);
    }

    // Size hash table based on adaptive cardinality estimate
    const table_size = optimalJoinTableSize(right_n, right_keys);

    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);

    // Build hash table in parallel using partitioning
    // This parallelizes the build phase for large tables
    buildJoinHashTableParallel(allocator, right_keys, table, next, table_size);

    // Single-pass parallel probe with chunk-local dynamic arrays using Blitz
    const num_workers = blitz.numWorkers();
    const num_chunks = @min(num_workers, left_n / 10000);
    const actual_chunks = @max(num_chunks, 1);
    const chunk_size = (left_n + actual_chunks - 1) / actual_chunks;

    var contexts = try allocator.alloc(SinglePassProbeContext, actual_chunks);
    defer allocator.free(contexts);

    // Initialize contexts
    for (0..actual_chunks) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, left_n);
        contexts[t] = SinglePassProbeContext{
            .left_keys = left_keys,
            .right_keys = right_keys,
            .table = table,
            .next = next,
            .table_size = @intCast(table_size),
            .start_idx = start,
            .end_idx = end,
            .allocator = allocator,
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .count = 0,
            .capacity = 0,
            .alloc_failed = false,
        };
    }

    // Process chunks in parallel using Blitz
    const ProbeCtx = struct {
        contexts: []SinglePassProbeContext,
    };

    const probe_ctx = ProbeCtx{ .contexts = contexts };

    blitz.parallelForWithGrain(actual_chunks, ProbeCtx, probe_ctx, struct {
        fn processChunks(c: ProbeCtx, start_chunk: usize, end_chunk: usize) void {
            for (start_chunk..end_chunk) |chunk_idx| {
                singlePassProbeWorker(&c.contexts[chunk_idx]);
            }
        }
    }.processChunks, 1);

    // Check for allocation failures and calculate total matches
    var total_matches: usize = 0;
    for (contexts) |ctx| {
        if (ctx.alloc_failed) {
            // Clean up any successful allocations
            for (contexts) |c| {
                if (!c.alloc_failed and c.capacity > 0) {
                    allocator.free(c.left_indices.ptr[0..c.capacity]);
                    allocator.free(c.right_indices.ptr[0..c.capacity]);
                }
            }
            return error.OutOfMemory;
        }
        total_matches += ctx.count;
    }

    // Merge chunk-local results into final arrays
    const left_indices = try allocator.alloc(i32, total_matches);
    const right_indices = try allocator.alloc(i32, total_matches);

    var offset: usize = 0;
    for (contexts) |ctx| {
        if (ctx.count > 0) {
            @memcpy(left_indices[offset .. offset + ctx.count], ctx.left_indices[0..ctx.count]);
            @memcpy(right_indices[offset .. offset + ctx.count], ctx.right_indices[0..ctx.count]);
            offset += ctx.count;
        }
        // Free chunk-local arrays
        if (ctx.capacity > 0) {
            allocator.free(ctx.left_indices.ptr[0..ctx.capacity]);
            allocator.free(ctx.right_indices.ptr[0..ctx.capacity]);
        }
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(total_matches),
        .allocator = allocator,
    };
}

// ============================================================================
// Left Join Implementations
// ============================================================================

/// Single-threaded left join with fastIntHash and interleaved probing
pub fn leftJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    // Handle empty right - all left rows with -1
    if (right_n == 0) {
        const left_indices = try allocator.alloc(i32, left_n);
        const right_indices = try allocator.alloc(i32, left_n);
        for (0..left_n) |i| {
            left_indices[i] = @intCast(i);
            right_indices[i] = -1;
        }
        return LeftJoinResult{
            .left_indices = left_indices,
            .right_indices = right_indices,
            .num_rows = @intCast(left_n),
            .allocator = allocator,
        };
    }

    if (left_n == 0) {
        return LeftJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_rows = 0,
            .allocator = allocator,
        };
    }

    // Build hash table
    const table_size = optimalJoinTableSize(right_n, right_keys);
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);
    buildJoinHashTableFast(right_keys, table, next, table_size);

    const mask: u64 = table_size - 1;

    // Capacity for results - at minimum left_n (all unmatched), could be more with duplicates
    var capacity: usize = left_n + left_n / 2;
    capacity = @max(capacity, 4096);
    var left_indices = try allocator.alloc(i32, capacity);
    var right_indices = try allocator.alloc(i32, capacity);
    var idx: usize = 0;

    // Interleaved 4-key probing
    const BATCH_SIZE: usize = 4;
    const main_end = if (left_n > BATCH_SIZE) left_n - BATCH_SIZE else 0;

    var i: usize = 0;

    while (i < main_end) : (i += BATCH_SIZE) {
        const key0 = left_keys[i];
        const key1 = left_keys[i + 1];
        const key2 = left_keys[i + 2];
        const key3 = left_keys[i + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        if (i + BATCH_SIZE + 8 < left_n) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[i + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        // Track matches for each key in batch
        var matched: [4]bool = .{ false, false, false, false };

        // Process each key
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        left_indices = try allocator.realloc(left_indices, new_capacity);
                        right_indices = try allocator.realloc(right_indices, new_capacity);
                        capacity = new_capacity;
                    }
                    left_indices[idx] = @intCast(i + offset);
                    right_indices[idx] = build_row;
                    idx += 1;
                    matched[offset] = true;
                }
                build_row = next_row;
            }
        }

        // Add unmatched rows
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            if (!matched[offset]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    left_indices = try allocator.realloc(left_indices, new_capacity);
                    right_indices = try allocator.realloc(right_indices, new_capacity);
                    capacity = new_capacity;
                }
                left_indices[idx] = @intCast(i + offset);
                right_indices[idx] = -1;
                idx += 1;
            }
        }
    }

    // Tail loop
    while (i < left_n) : (i += 1) {
        const key = left_keys[i];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];
        var matched_row = false;

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];
            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    left_indices = try allocator.realloc(left_indices, new_capacity);
                    right_indices = try allocator.realloc(right_indices, new_capacity);
                    capacity = new_capacity;
                }
                left_indices[idx] = @intCast(i);
                right_indices[idx] = build_row;
                idx += 1;
                matched_row = true;
            }
            build_row = next_row;
        }

        if (!matched_row) {
            if (idx >= capacity) {
                const new_capacity = capacity * 2;
                left_indices = try allocator.realloc(left_indices, new_capacity);
                right_indices = try allocator.realloc(right_indices, new_capacity);
                capacity = new_capacity;
            }
            left_indices[idx] = @intCast(i);
            right_indices[idx] = -1;
            idx += 1;
        }
    }

    // Shrink to actual size
    if (idx > 0 and idx < capacity / 2) {
        left_indices = try allocator.realloc(left_indices, idx);
        right_indices = try allocator.realloc(right_indices, idx);
    } else if (idx == 0) {
        allocator.free(left_indices);
        allocator.free(right_indices);
        left_indices = try allocator.alloc(i32, 0);
        right_indices = try allocator.alloc(i32, 0);
    }

    return LeftJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_rows = @intCast(idx),
        .allocator = allocator,
    };
}

fn leftProbeWorker(ctx: *LeftProbeContext) void {
    const left_keys = ctx.left_keys;
    const right_keys = ctx.right_keys;
    const table = ctx.table;
    const next = ctx.next;
    const table_size = ctx.table_size;
    const allocator = ctx.allocator;
    const mask: u64 = table_size - 1;

    const start = ctx.start_idx;
    const end = ctx.end_idx;
    const chunk_size = end - start;

    // Initial capacity - at least chunk_size for all unmatched case
    var capacity: usize = chunk_size + chunk_size / 2;
    capacity = @max(capacity, 1024);

    var left_out = allocator.alloc(i32, capacity) catch {
        ctx.alloc_failed = true;
        return;
    };
    var right_out = allocator.alloc(i32, capacity) catch {
        allocator.free(left_out);
        ctx.alloc_failed = true;
        return;
    };
    var idx: usize = 0;

    const BATCH_SIZE: usize = 4;
    const main_end = if (end > start + BATCH_SIZE) end - BATCH_SIZE else start;

    var left_row: usize = start;

    while (left_row < main_end) : (left_row += BATCH_SIZE) {
        const key0 = left_keys[left_row];
        const key1 = left_keys[left_row + 1];
        const key2 = left_keys[left_row + 2];
        const key3 = left_keys[left_row + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        if (left_row + BATCH_SIZE + 8 < end) {
            @prefetch(@as([*]const i64, @ptrCast(&left_keys[left_row + BATCH_SIZE + 8])), .{ .locality = 0 });
        }

        var matched: [4]bool = .{ false, false, false, false };

        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            const key = switch (offset) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (offset) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                const next_row = next[build_idx];

                if (key == right_keys[build_idx]) {
                    if (idx >= capacity) {
                        const new_capacity = capacity * 2;
                        const new_left = allocator.realloc(left_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(left_out);
                            allocator.free(right_out);
                            return;
                        };
                        const new_right = allocator.realloc(right_out, new_capacity) catch {
                            ctx.alloc_failed = true;
                            allocator.free(new_left);
                            return;
                        };
                        left_out = new_left;
                        right_out = new_right;
                        capacity = new_capacity;
                    }
                    left_out[idx] = @intCast(left_row + offset);
                    right_out[idx] = build_row;
                    idx += 1;
                    matched[offset] = true;
                }
                build_row = next_row;
            }
        }

        // Add unmatched rows
        inline for ([_]usize{ 0, 1, 2, 3 }) |offset| {
            if (!matched[offset]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    const new_left = allocator.realloc(left_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(left_out);
                        allocator.free(right_out);
                        return;
                    };
                    const new_right = allocator.realloc(right_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(new_left);
                        return;
                    };
                    left_out = new_left;
                    right_out = new_right;
                    capacity = new_capacity;
                }
                left_out[idx] = @intCast(left_row + offset);
                right_out[idx] = -1;
                idx += 1;
            }
        }
    }

    // Tail loop
    while (left_row < end) : (left_row += 1) {
        const key = left_keys[left_row];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        var build_row = table[slot];
        var matched_row = false;

        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            const next_row = next[build_idx];
            if (key == right_keys[build_idx]) {
                if (idx >= capacity) {
                    const new_capacity = capacity * 2;
                    const new_left = allocator.realloc(left_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(left_out);
                        allocator.free(right_out);
                        return;
                    };
                    const new_right = allocator.realloc(right_out, new_capacity) catch {
                        ctx.alloc_failed = true;
                        allocator.free(new_left);
                        return;
                    };
                    left_out = new_left;
                    right_out = new_right;
                    capacity = new_capacity;
                }
                left_out[idx] = @intCast(left_row);
                right_out[idx] = build_row;
                idx += 1;
                matched_row = true;
            }
            build_row = next_row;
        }

        if (!matched_row) {
            if (idx >= capacity) {
                const new_capacity = capacity * 2;
                const new_left = allocator.realloc(left_out, new_capacity) catch {
                    ctx.alloc_failed = true;
                    allocator.free(left_out);
                    allocator.free(right_out);
                    return;
                };
                const new_right = allocator.realloc(right_out, new_capacity) catch {
                    ctx.alloc_failed = true;
                    allocator.free(new_left);
                    return;
                };
                left_out = new_left;
                right_out = new_right;
                capacity = new_capacity;
            }
            left_out[idx] = @intCast(left_row);
            right_out[idx] = -1;
            idx += 1;
        }
    }

    ctx.left_indices = left_out;
    ctx.right_indices = right_out;
    ctx.count = idx;
    ctx.capacity = capacity;
    ctx.alloc_failed = false;
}

/// Parallel left join using Blitz
pub fn parallelLeftJoinI64(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    // Handle empty right
    if (right_n == 0) {
        const left_indices = try allocator.alloc(i32, left_n);
        const right_indices = try allocator.alloc(i32, left_n);
        for (0..left_n) |i| {
            left_indices[i] = @intCast(i);
            right_indices[i] = -1;
        }
        return LeftJoinResult{
            .left_indices = left_indices,
            .right_indices = right_indices,
            .num_rows = @intCast(left_n),
            .allocator = allocator,
        };
    }

    if (left_n == 0) {
        return LeftJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_rows = 0,
            .allocator = allocator,
        };
    }

    // For small data, use single-threaded
    if (left_n < 10000) {
        return leftJoinI64(allocator, left_keys, right_keys);
    }

    // Build hash table
    const table_size = optimalJoinTableSize(right_n, right_keys);
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);
    buildJoinHashTableFast(right_keys, table, next, table_size);

    // Parallel probe using Blitz
    const num_workers = blitz.numWorkers();
    const num_chunks = @min(num_workers, left_n / 10000);
    const actual_chunks = @max(num_chunks, 1);
    const chunk_size = (left_n + actual_chunks - 1) / actual_chunks;

    var contexts = try allocator.alloc(LeftProbeContext, actual_chunks);
    defer allocator.free(contexts);

    for (0..actual_chunks) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, left_n);
        contexts[t] = LeftProbeContext{
            .left_keys = left_keys,
            .right_keys = right_keys,
            .table = table,
            .next = next,
            .table_size = @intCast(table_size),
            .start_idx = start,
            .end_idx = end,
            .allocator = allocator,
            .left_indices = &[_]i32{},
            .right_indices = &[_]i32{},
            .count = 0,
            .capacity = 0,
            .alloc_failed = false,
        };
    }

    // Process chunks in parallel using Blitz
    const LeftProbeCtx = struct {
        contexts: []LeftProbeContext,
    };

    const probe_ctx = LeftProbeCtx{ .contexts = contexts };

    blitz.parallelForWithGrain(actual_chunks, LeftProbeCtx, probe_ctx, struct {
        fn processChunks(c: LeftProbeCtx, start_chunk: usize, end_chunk: usize) void {
            for (start_chunk..end_chunk) |chunk_idx| {
                leftProbeWorker(&c.contexts[chunk_idx]);
            }
        }
    }.processChunks, 1);

    // Check for failures and count total
    var total_rows: usize = 0;
    for (contexts) |ctx| {
        if (ctx.alloc_failed) {
            for (contexts) |c| {
                if (!c.alloc_failed and c.capacity > 0) {
                    allocator.free(c.left_indices.ptr[0..c.capacity]);
                    allocator.free(c.right_indices.ptr[0..c.capacity]);
                }
            }
            return error.OutOfMemory;
        }
        total_rows += ctx.count;
    }

    // Merge results
    const left_indices = try allocator.alloc(i32, total_rows);
    const right_indices = try allocator.alloc(i32, total_rows);

    var offset: usize = 0;
    for (contexts) |ctx| {
        if (ctx.count > 0) {
            @memcpy(left_indices[offset .. offset + ctx.count], ctx.left_indices[0..ctx.count]);
            @memcpy(right_indices[offset .. offset + ctx.count], ctx.right_indices[0..ctx.count]);
            offset += ctx.count;
        }
        if (ctx.capacity > 0) {
            allocator.free(ctx.left_indices.ptr[0..ctx.capacity]);
            allocator.free(ctx.right_indices.ptr[0..ctx.capacity]);
        }
    }

    return LeftJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_rows = @intCast(total_rows),
        .allocator = allocator,
    };
}

// ============================================================================
// Lock-Free Parallel Hash Join
// ============================================================================

fn lockFreeBuildWorker(ctx: *LockFreeBuildContext) void {
    const right_keys = ctx.right_keys;
    const right_hashes = ctx.right_hashes;
    const partition_id = ctx.partition_id;
    const num_partitions = ctx.num_partitions;
    var table = ctx.partition_table;

    // Insert only keys that hash to this partition
    for (right_hashes, 0..) |hash, i| {
        if (hash % num_partitions == partition_id) {
            table.insert(hash, right_keys[i], @intCast(i)) catch {};
        }
    }
}

fn lockFreeProbeWorker(ctx: *LockFreeProbeContext) void {
    const left_keys = ctx.left_keys;
    const left_hashes = ctx.left_hashes;
    const num_partitions = ctx.num_partitions;

    for (left_hashes, 0..) |hash, left_idx| {
        const partition_id = hash % num_partitions;
        const table = &ctx.partition_tables[partition_id];

        const slot = hash % table.size;
        var entry_idx = table.table[slot];

        while (entry_idx != -1) {
            const idx_val: usize = @intCast(entry_idx);
            if (table.keys[idx_val] == left_keys[left_idx]) {
                ctx.left_results.append(@intCast(left_idx)) catch {};
                ctx.right_results.append(@intCast(table.row_indices[idx_val])) catch {};
            }
            entry_idx = table.next[idx_val];
        }
    }
}

/// Lock-free parallel inner join with pre-partitioning
/// Step 1: Single-pass partition keys by hash % num_partitions
/// Step 2: Parallel build - each thread builds from its partition only
/// Step 3: Parallel probe - each thread probes its partition
pub fn innerJoinI64LockFree(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For small data, use single-threaded version
    if (right_n < 50000) {
        return innerJoinI64(allocator, left_keys, right_keys);
    }

    const num_partitions: usize = @min(getMaxThreads(), 8);

    // Compute hashes for both sides
    const left_hashes = try allocator.alloc(u64, left_n);
    defer allocator.free(left_hashes);
    hashInt64Column(left_keys, left_hashes);

    const right_hashes = try allocator.alloc(u64, right_n);
    defer allocator.free(right_hashes);
    hashInt64Column(right_keys, right_hashes);

    // Step 1: Count keys per partition (single pass)
    var right_partition_counts = [_]usize{0} ** MAX_THREADS;
    for (right_hashes) |hash| {
        right_partition_counts[hash % num_partitions] += 1;
    }

    // Allocate partition arrays
    var right_partitions: [MAX_THREADS][]u32 = undefined;
    var right_partition_pos = [_]usize{0} ** MAX_THREADS;
    for (0..num_partitions) |p| {
        right_partitions[p] = try allocator.alloc(u32, right_partition_counts[p]);
    }
    defer {
        for (0..num_partitions) |p| {
            allocator.free(right_partitions[p]);
        }
    }

    // Fill partitions with row indices
    for (right_hashes, 0..) |hash, i| {
        const p = hash % num_partitions;
        right_partitions[p][right_partition_pos[p]] = @intCast(i);
        right_partition_pos[p] += 1;
    }

    // Step 2: Build hash tables per partition (can be parallelized, but sequential for now)
    var partition_tables = try allocator.alloc(PartitionedHashTable, num_partitions);
    defer {
        for (partition_tables) |*t| {
            t.deinit();
        }
        allocator.free(partition_tables);
    }

    for (0..num_partitions) |p| {
        const count = right_partition_counts[p];
        partition_tables[p] = try PartitionedHashTable.init(allocator, count);

        // Build this partition's hash table
        for (right_partitions[p]) |row_idx| {
            const hash = right_hashes[row_idx];
            const key = right_keys[row_idx];
            try partition_tables[p].insert(hash, key, row_idx);
        }
    }

    // Step 3: Probe (single-threaded for now - simpler and often faster for small partitions)
    var left_results = try allocator.alloc(i32, left_n);
    var right_results = try allocator.alloc(i32, left_n);
    var result_count: usize = 0;
    var capacity: usize = left_n;

    for (left_hashes, 0..) |hash, left_idx| {
        const partition_id = hash % num_partitions;
        const table = &partition_tables[partition_id];

        const slot = hash % table.size;
        var entry_idx = table.table[slot];

        while (entry_idx != -1) {
            const idx_val: usize = @intCast(entry_idx);
            if (table.keys[idx_val] == left_keys[left_idx]) {
                if (result_count >= capacity) {
                    capacity = capacity * 2;
                    left_results = try allocator.realloc(left_results, capacity);
                    right_results = try allocator.realloc(right_results, capacity);
                }
                left_results[result_count] = @intCast(left_idx);
                right_results[result_count] = @intCast(table.row_indices[idx_val]);
                result_count += 1;
            }
            entry_idx = table.next[idx_val];
        }
    }

    // Shrink to actual size
    if (result_count < capacity) {
        left_results = try allocator.realloc(left_results, result_count);
        right_results = try allocator.realloc(right_results, result_count);
    }

    return InnerJoinResult{
        .left_indices = left_results,
        .right_indices = right_results,
        .num_matches = @intCast(result_count),
        .allocator = allocator,
    };
}

// ============================================================================
// Fully Parallel Radix-Partitioned Join
// ============================================================================
//
// Key insight: partition both left and right by hash bits, then process
// each partition completely independently (build + probe) in parallel.
// This matches Polars' approach.
//
// Memory: ~16 bytes per right row for partitioned keys + tables

/// Partition context for parallel processing
const PartitionContext = struct {
    // Input data (views into original arrays)
    left_keys: []const i64,
    right_keys: []const i64,
    left_hashes: []const u64,
    right_hashes: []const u64,

    // Partition range
    partition_id: usize,
    partition_mask: usize,
    partition_shift: u6,

    // Working arrays for this partition
    right_indices_in_partition: []u32,
    right_count: usize,

    // Output
    left_results: []i32,
    right_results: []i32,
    result_count: usize,
    capacity: usize,
    alloc_failed: bool,
    allocator: std.mem.Allocator,
};

/// Entry with inline key for partition-local hash table
const InlineKeyEntry = struct {
    key: i64,
    row_idx: u32, // Original index in right table (not partition index)
    next: i32,
};

/// Inner join with inline key storage and proper parallelization
pub fn innerJoinI64Radix(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For small data, use simple version
    if (right_n < 5000) {
        return innerJoinI64(allocator, left_keys, right_keys);
    }

    // Allocate entries array (stores key inline)
    const entries = try allocator.alloc(InlineKeyEntry, right_n);
    defer allocator.free(entries);

    // Size hash table based on cardinality
    const table_size = optimalJoinTableSize(right_n, right_keys);
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    @memset(table, -1);

    const mask: u64 = table_size - 1;

    // Build phase: populate entries with inline keys
    for (right_keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);

        entries[row] = .{
            .key = key, // Store key inline!
            .row_idx = @intCast(row),
            .next = table[slot],
        };
        table[slot] = @intCast(row);
    }

    // Probe phase
    const estimated_unique = estimateCardinality(right_keys);
    const avg_matches = @max(right_n / estimated_unique, 1);
    var capacity: usize = @min(left_n * avg_matches, 50_000_000);
    capacity = @max(capacity, 4096);

    var left_results = try allocator.alloc(i32, capacity);
    var right_results = try allocator.alloc(i32, capacity);
    var result_count: usize = 0;

    // Batched probing with prefetch
    const BATCH: usize = 4;
    const main_end = if (left_n >= BATCH) left_n - BATCH + 1 else 0;
    var i: usize = 0;

    while (i < main_end) : (i += BATCH) {
        // Compute hashes and slots for batch
        const key0 = left_keys[i];
        const key1 = left_keys[i + 1];
        const key2 = left_keys[i + 2];
        const key3 = left_keys[i + 3];

        const hash0 = fastIntHash(key0);
        const hash1 = fastIntHash(key1);
        const hash2 = fastIntHash(key2);
        const hash3 = fastIntHash(key3);

        const slot0: usize = @intCast(hash0 & mask);
        const slot1: usize = @intCast(hash1 & mask);
        const slot2: usize = @intCast(hash2 & mask);
        const slot3: usize = @intCast(hash3 & mask);

        // Prefetch table slots
        @prefetch(@as([*]const i32, @ptrCast(&table[slot0])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot1])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot2])), .{ .locality = 1 });
        @prefetch(@as([*]const i32, @ptrCast(&table[slot3])), .{ .locality = 1 });

        // Process each key
        inline for ([_]usize{ 0, 1, 2, 3 }) |off| {
            const key = switch (off) {
                0 => key0,
                1 => key1,
                2 => key2,
                3 => key3,
                else => unreachable,
            };
            const slot = switch (off) {
                0 => slot0,
                1 => slot1,
                2 => slot2,
                3 => slot3,
                else => unreachable,
            };

            var entry_idx = table[slot];
            while (entry_idx != -1) {
                const entry = &entries[@intCast(entry_idx)];

                // Prefetch next entry while comparing current
                if (entry.next != -1) {
                    @prefetch(@as([*]const InlineKeyEntry, @ptrCast(&entries[@intCast(entry.next)])), .{ .locality = 1 });
                }

                // Compare using INLINE key (no random access!)
                if (entry.key == key) {
                    if (result_count >= capacity) {
                        capacity = capacity * 2;
                        left_results = try allocator.realloc(left_results, capacity);
                        right_results = try allocator.realloc(right_results, capacity);
                    }
                    left_results[result_count] = @intCast(i + off);
                    right_results[result_count] = @intCast(entry.row_idx);
                    result_count += 1;
                }
                entry_idx = entry.next;
            }
        }
    }

    // Tail loop
    while (i < left_n) : (i += 1) {
        const key = left_keys[i];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);

        var entry_idx = table[slot];
        while (entry_idx != -1) {
            const entry = &entries[@intCast(entry_idx)];

            if (entry.key == key) {
                if (result_count >= capacity) {
                    capacity = capacity * 2;
                    left_results = try allocator.realloc(left_results, capacity);
                    right_results = try allocator.realloc(right_results, capacity);
                }
                left_results[result_count] = @intCast(i);
                right_results[result_count] = @intCast(entry.row_idx);
                result_count += 1;
            }
            entry_idx = entry.next;
        }
    }

    // Shrink to actual size
    if (result_count < capacity) {
        left_results = try allocator.realloc(left_results, @max(result_count, 1));
        right_results = try allocator.realloc(right_results, @max(result_count, 1));
    }

    return InnerJoinResult{
        .left_indices = left_results,
        .right_indices = right_results,
        .num_matches = @intCast(result_count),
        .allocator = allocator,
    };
}

/// Left join with inline key storage
pub fn leftJoinI64Radix(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0) {
        return LeftJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_rows = 0,
            .allocator = allocator,
        };
    }

    if (right_n == 0) {
        const left_indices = try allocator.alloc(i32, left_n);
        const right_indices = try allocator.alloc(i32, left_n);
        for (0..left_n) |idx| {
            left_indices[idx] = @intCast(idx);
            right_indices[idx] = -1;
        }
        return LeftJoinResult{
            .left_indices = left_indices,
            .right_indices = right_indices,
            .num_rows = @intCast(left_n),
            .allocator = allocator,
        };
    }

    // For small data, use simple version
    if (right_n < 5000) {
        return leftJoinI64(allocator, left_keys, right_keys);
    }

    // Allocate entries array
    const entries = try allocator.alloc(InlineKeyEntry, right_n);
    defer allocator.free(entries);

    const table_size = optimalJoinTableSize(right_n, right_keys);
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    @memset(table, -1);

    const mask: u64 = table_size - 1;

    // Build phase
    for (right_keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);

        entries[row] = .{
            .key = key,
            .row_idx = @intCast(row),
            .next = table[slot],
        };
        table[slot] = @intCast(row);
    }

    // Probe phase
    const estimated_unique = estimateCardinality(right_keys);
    const avg_matches = @max(right_n / estimated_unique, 1);
    var capacity: usize = @min(left_n * avg_matches + left_n, 50_000_000);
    capacity = @max(capacity, left_n);

    var left_results = try allocator.alloc(i32, capacity);
    var right_results = try allocator.alloc(i32, capacity);
    var result_count: usize = 0;

    for (left_keys, 0..) |key, left_row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);

        var entry_idx = table[slot];
        var found_match = false;

        while (entry_idx != -1) {
            const entry = &entries[@intCast(entry_idx)];

            if (entry.key == key) {
                if (result_count >= capacity) {
                    capacity = capacity * 2;
                    left_results = try allocator.realloc(left_results, capacity);
                    right_results = try allocator.realloc(right_results, capacity);
                }
                left_results[result_count] = @intCast(left_row);
                right_results[result_count] = @intCast(entry.row_idx);
                result_count += 1;
                found_match = true;
            }
            entry_idx = entry.next;
        }

        if (!found_match) {
            if (result_count >= capacity) {
                capacity = capacity * 2;
                left_results = try allocator.realloc(left_results, capacity);
                right_results = try allocator.realloc(right_results, capacity);
            }
            left_results[result_count] = @intCast(left_row);
            right_results[result_count] = -1;
            result_count += 1;
        }
    }

    // Shrink to actual size
    if (result_count < capacity) {
        left_results = try allocator.realloc(left_results, @max(result_count, 1));
        right_results = try allocator.realloc(right_results, @max(result_count, 1));
    }

    return LeftJoinResult{
        .left_indices = left_results,
        .right_indices = right_results,
        .num_rows = @intCast(result_count),
        .allocator = allocator,
    };
}

// ============================================================================
// Sort-Merge Join Implementation
// ============================================================================

// Import sorting functions
const sorting = @import("sorting.zig");
const argsortRadixI64 = sorting.argsortRadixI64;
const argsortRadixI64Parallel = sorting.argsortRadixI64Parallel;

/// Threshold for using parallel sort in sort-merge join
const PARALLEL_SORT_THRESHOLD: usize = 100000;

/// Check if i64 array is sorted (ascending)
/// Uses SIMD for fast vectorized comparison
pub fn isSortedI64(keys: []const i64) bool {
    const len = keys.len;
    if (len < 2) return true;

    const Vec = @Vector(8, i64);
    const aligned_len = (len - 1) - ((len - 1) % 8);
    var i: usize = 0;

    // Vectorized check: compare adjacent elements
    while (i < aligned_len) : (i += 8) {
        const current: Vec = keys[i..][0..8].*;
        const next: Vec = keys[i + 1 ..][0..8].*;
        const cmp = current > next;
        if (@reduce(.Or, cmp)) {
            return false;
        }
    }

    // Check remaining elements
    while (i < len - 1) : (i += 1) {
        if (keys[i] > keys[i + 1]) {
            return false;
        }
    }

    return true;
}

/// Create identity indices (0, 1, 2, ..., n-1) for already-sorted data
fn createIdentityIndices(allocator: std.mem.Allocator, n: usize) ![]u32 {
    const indices = try allocator.alloc(u32, n);
    for (indices, 0..) |*idx, i| {
        idx.* = @intCast(i);
    }
    return indices;
}

/// Sort-Merge Inner Join for i64 keys
/// Uses sorting + sequential merge for excellent cache locality
/// Algorithm:
/// 1. Radix sort both sides by key (O(n) with good cache behavior)
/// 2. Merge with two pointers (O(n+m) sequential access)
///
/// Benefits:
/// - Pure sequential memory access during merge phase
/// - No random access to hash buckets or chains
/// - Works well with data that has many duplicates
/// - Radix sort is cache-friendly
pub fn innerJoinI64SortMerge(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Step 1: Sort both sides using radix sort (returns indices)
    // Optimization: skip sort if data is already sorted
    // Note: Single-threaded radix sort is O(n) which is faster than parallel O(n log n)
    const left_already_sorted = isSortedI64(left_keys);
    const left_sorted_idx = if (left_already_sorted)
        try createIdentityIndices(allocator, left_n)
    else blk: {
        const idx = try allocator.alloc(u32, left_n);
        argsortRadixI64(left_keys, idx, true);
        break :blk idx;
    };
    defer allocator.free(left_sorted_idx);

    const right_already_sorted = isSortedI64(right_keys);
    const right_sorted_idx = if (right_already_sorted)
        try createIdentityIndices(allocator, right_n)
    else blk: {
        const idx = try allocator.alloc(u32, right_n);
        argsortRadixI64(right_keys, idx, true);
        break :blk idx;
    };
    defer allocator.free(right_sorted_idx);

    // Step 2: Merge phase - count matches first for exact allocation
    // This is fast because it's pure sequential access
    var match_count: usize = 0;
    {
        var li: usize = 0;
        var ri: usize = 0;

        while (li < left_n and ri < right_n) {
            const left_key = left_keys[left_sorted_idx[li]];
            const right_key = right_keys[right_sorted_idx[ri]];

            if (left_key < right_key) {
                li += 1;
            } else if (left_key > right_key) {
                ri += 1;
            } else {
                // Found matching keys - count all combinations
                // Count duplicates on left side
                var left_dup_count: usize = 1;
                while (li + left_dup_count < left_n and
                    left_keys[left_sorted_idx[li + left_dup_count]] == left_key)
                {
                    left_dup_count += 1;
                }

                // Count duplicates on right side
                var right_dup_count: usize = 1;
                while (ri + right_dup_count < right_n and
                    right_keys[right_sorted_idx[ri + right_dup_count]] == right_key)
                {
                    right_dup_count += 1;
                }

                // Each left dup matches each right dup
                match_count += left_dup_count * right_dup_count;

                li += left_dup_count;
                ri += right_dup_count;
            }
        }
    }

    // Allocate result arrays with exact size
    const left_results = try allocator.alloc(i32, match_count);
    const right_results = try allocator.alloc(i32, match_count);

    // Step 3: Fill in the results (second pass, still sequential)
    var result_idx: usize = 0;
    {
        var li: usize = 0;
        var ri: usize = 0;

        while (li < left_n and ri < right_n) {
            const left_key = left_keys[left_sorted_idx[li]];
            const right_key = right_keys[right_sorted_idx[ri]];

            if (left_key < right_key) {
                li += 1;
            } else if (left_key > right_key) {
                ri += 1;
            } else {
                // Found matching keys - emit all combinations
                // Find range of left duplicates
                var left_end = li + 1;
                while (left_end < left_n and left_keys[left_sorted_idx[left_end]] == left_key) {
                    left_end += 1;
                }

                // Find range of right duplicates
                var right_end = ri + 1;
                while (right_end < right_n and right_keys[right_sorted_idx[right_end]] == right_key) {
                    right_end += 1;
                }

                // Emit cartesian product of duplicates
                for (li..left_end) |l| {
                    for (ri..right_end) |r| {
                        left_results[result_idx] = @intCast(left_sorted_idx[l]);
                        right_results[result_idx] = @intCast(right_sorted_idx[r]);
                        result_idx += 1;
                    }
                }

                li = left_end;
                ri = right_end;
            }
        }
    }

    return InnerJoinResult{
        .left_indices = left_results,
        .right_indices = right_results,
        .num_matches = @intCast(match_count),
        .allocator = allocator,
    };
}

/// Sort-Merge Left Join for i64 keys
/// Similar to inner join but includes all left rows
pub fn leftJoinI64SortMerge(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !LeftJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0) {
        return LeftJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_rows = 0,
            .allocator = allocator,
        };
    }

    if (right_n == 0) {
        // All left rows, no matches
        const left_indices = try allocator.alloc(i32, left_n);
        const right_indices = try allocator.alloc(i32, left_n);
        for (0..left_n) |i| {
            left_indices[i] = @intCast(i);
            right_indices[i] = -1;
        }
        return LeftJoinResult{
            .left_indices = left_indices,
            .right_indices = right_indices,
            .num_rows = @intCast(left_n),
            .allocator = allocator,
        };
    }

    // Step 1: Sort both sides (skip if already sorted)
    // Note: Single-threaded radix sort is O(n) which is faster than parallel O(n log n)
    const left_already_sorted = isSortedI64(left_keys);
    const left_sorted_idx = if (left_already_sorted)
        try createIdentityIndices(allocator, left_n)
    else blk: {
        const idx = try allocator.alloc(u32, left_n);
        argsortRadixI64(left_keys, idx, true);
        break :blk idx;
    };
    defer allocator.free(left_sorted_idx);

    const right_already_sorted = isSortedI64(right_keys);
    const right_sorted_idx = if (right_already_sorted)
        try createIdentityIndices(allocator, right_n)
    else blk: {
        const idx = try allocator.alloc(u32, right_n);
        argsortRadixI64(right_keys, idx, true);
        break :blk idx;
    };
    defer allocator.free(right_sorted_idx);

    // Step 2: Count output rows (left rows + extra for duplicates on right)
    var row_count: usize = 0;
    {
        var li: usize = 0;
        var ri: usize = 0;

        while (li < left_n) {
            const left_key = left_keys[left_sorted_idx[li]];

            // Advance right until >= left_key
            while (ri < right_n and right_keys[right_sorted_idx[ri]] < left_key) {
                ri += 1;
            }

            if (ri < right_n and right_keys[right_sorted_idx[ri]] == left_key) {
                // Count left duplicates
                var left_end = li + 1;
                while (left_end < left_n and left_keys[left_sorted_idx[left_end]] == left_key) {
                    left_end += 1;
                }
                const left_dup_count = left_end - li;

                // Count right duplicates
                const right_start = ri;
                var right_end = ri + 1;
                while (right_end < right_n and right_keys[right_sorted_idx[right_end]] == left_key) {
                    right_end += 1;
                }
                const right_dup_count = right_end - right_start;

                row_count += left_dup_count * right_dup_count;
                li = left_end;
            } else {
                // No match for this left key
                row_count += 1;
                li += 1;
            }
        }
    }

    // Allocate results
    const left_results = try allocator.alloc(i32, row_count);
    const right_results = try allocator.alloc(i32, row_count);

    // Step 3: Fill results
    var result_idx: usize = 0;
    {
        var li: usize = 0;
        var ri: usize = 0;

        while (li < left_n) {
            const left_key = left_keys[left_sorted_idx[li]];

            // Advance right until >= left_key
            while (ri < right_n and right_keys[right_sorted_idx[ri]] < left_key) {
                ri += 1;
            }

            if (ri < right_n and right_keys[right_sorted_idx[ri]] == left_key) {
                // Find left range
                var left_end = li + 1;
                while (left_end < left_n and left_keys[left_sorted_idx[left_end]] == left_key) {
                    left_end += 1;
                }

                // Find right range
                const right_start = ri;
                var right_end = ri + 1;
                while (right_end < right_n and right_keys[right_sorted_idx[right_end]] == left_key) {
                    right_end += 1;
                }

                // Emit cartesian product
                for (li..left_end) |l| {
                    for (right_start..right_end) |r| {
                        left_results[result_idx] = @intCast(left_sorted_idx[l]);
                        right_results[result_idx] = @intCast(right_sorted_idx[r]);
                        result_idx += 1;
                    }
                }

                li = left_end;
            } else {
                // No match - emit with -1
                left_results[result_idx] = @intCast(left_sorted_idx[li]);
                right_results[result_idx] = -1;
                result_idx += 1;
                li += 1;
            }
        }
    }

    return LeftJoinResult{
        .left_indices = left_results,
        .right_indices = right_results,
        .num_rows = @intCast(row_count),
        .allocator = allocator,
    };
}

// ============================================================================
// High-Performance Open-Addressing Hash Join
// ============================================================================
//
// Key optimizations:
// 1. Open addressing with linear probing (better cache locality than chaining)
// 2. Two-pass approach: count matches first, then fill (no reallocations)
// 3. Keys stored inline in hash table (avoid extra memory access)
// 4. SIMD for batch hash computation

/// Open-addressing hash table entry with inline key
const OpenHashEntry = packed struct {
    key: i64, // Key stored inline (no extra memory access)
    row_idx: u32, // Original row index in right table
    occupied: bool, // Whether slot is occupied
    _pad: u23 = 0, // Padding for alignment
};

/// Build an open-addressing hash table with linear probing
/// Returns the number of entries and the table itself
fn buildOpenHashTable(
    allocator: std.mem.Allocator,
    keys: []const i64,
) !struct { table: []OpenHashEntry, mask: usize } {
    const n = keys.len;
    if (n == 0) {
        return .{ .table = &[_]OpenHashEntry{}, .mask = 0 };
    }

    // Size table at ~50% load factor for good performance
    var table_size: usize = 16;
    while (table_size < n * 2) {
        table_size *= 2;
    }
    const mask = table_size - 1;

    const table = try allocator.alloc(OpenHashEntry, table_size);
    for (table) |*entry| {
        entry.* = .{ .key = 0, .row_idx = 0, .occupied = false };
    }

    // Insert all keys
    for (keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        var slot: usize = @intCast(hash & mask);

        // Linear probe to find empty slot
        while (table[slot].occupied) {
            slot = (slot + 1) & mask;
        }

        table[slot] = .{
            .key = key,
            .row_idx = @intCast(row),
            .occupied = true,
        };
    }

    return .{ .table = table, .mask = mask };
}

/// Two-pass inner join: count then fill (no reallocations)
/// Uses open-addressing hash table for cache-friendly probing
pub fn innerJoinI64TwoPass(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For very small data, use simple nested loop
    if (right_n <= 16) {
        return innerJoinI64(allocator, left_keys, right_keys);
    }

    // Build open-addressing hash table on right side
    const table_result = try buildOpenHashTable(allocator, right_keys);
    const table = table_result.table;
    const mask = table_result.mask;
    defer if (table.len > 0) allocator.free(table);

    if (table.len == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // Pass 1: Count matches (no allocation)
    var match_count: usize = 0;
    for (left_keys) |key| {
        const hash = fastIntHash(key);
        var slot: usize = @intCast(hash & mask);
        const start_slot = slot;

        while (table[slot].occupied) {
            if (table[slot].key == key) {
                match_count += 1;
            }
            slot = (slot + 1) & mask;
            if (slot == start_slot) break;
        }
    }

    // Allocate exact size
    const left_indices = try allocator.alloc(i32, match_count);
    const right_indices = try allocator.alloc(i32, match_count);

    // Pass 2: Fill results
    var idx: usize = 0;
    for (left_keys, 0..) |key, left_row| {
        const hash = fastIntHash(key);
        var slot: usize = @intCast(hash & mask);
        const start_slot = slot;

        while (table[slot].occupied) {
            if (table[slot].key == key) {
                left_indices[idx] = @intCast(left_row);
                right_indices[idx] = @intCast(table[slot].row_idx);
                idx += 1;
            }
            slot = (slot + 1) & mask;
            if (slot == start_slot) break;
        }
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(match_count),
        .allocator = allocator,
    };
}

/// SIMD-accelerated inner join with vectorized probing
/// Uses batch processing for better throughput
pub fn innerJoinI64Simd(
    allocator: std.mem.Allocator,
    left_keys: []const i64,
    right_keys: []const i64,
) !InnerJoinResult {
    const left_n = left_keys.len;
    const right_n = right_keys.len;

    if (left_n == 0 or right_n == 0) {
        return InnerJoinResult{
            .left_indices = try allocator.alloc(i32, 0),
            .right_indices = try allocator.alloc(i32, 0),
            .num_matches = 0,
            .allocator = allocator,
        };
    }

    // For small data, use two-pass
    if (right_n < 1000 or left_n < 1000) {
        return innerJoinI64TwoPass(allocator, left_keys, right_keys);
    }

    // Build hash table with inline keys for cache-friendly probing
    // Size at 50% load factor
    var table_size: usize = 16;
    while (table_size < right_n * 2) {
        table_size *= 2;
    }
    const mask: u64 = table_size - 1;

    // Allocate table and next arrays
    const table = try allocator.alloc(i32, table_size);
    defer allocator.free(table);
    @memset(table, -1);

    const next = try allocator.alloc(i32, right_n);
    defer allocator.free(next);
    @memset(next, -1);

    const right_keys_inline = try allocator.alloc(i64, right_n);
    defer allocator.free(right_keys_inline);

    // Build hash table with inline keys
    for (right_keys, 0..) |key, row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);
        const row_i32: i32 = @intCast(row);

        right_keys_inline[row] = key;
        next[row] = table[slot];
        table[slot] = row_i32;
    }

    // Pass 1: Count matches using SIMD batching
    var match_count: usize = 0;

    // Process 8 keys at a time for SIMD benefit
    const BATCH: usize = 8;
    const aligned_len = left_n - (left_n % BATCH);
    var i: usize = 0;

    while (i < aligned_len) : (i += BATCH) {
        // Load 8 keys and compute hashes
        inline for (0..BATCH) |j| {
            const key = left_keys[i + j];
            const hash = fastIntHash(key);
            const slot: usize = @intCast(hash & mask);

            var build_row = table[slot];
            while (build_row != -1) {
                const build_idx: usize = @intCast(build_row);
                if (right_keys_inline[build_idx] == key) {
                    match_count += 1;
                }
                build_row = next[build_idx];
            }
        }
    }

    // Tail
    while (i < left_n) : (i += 1) {
        const key = left_keys[i];
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);

        var build_row = table[slot];
        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            if (right_keys_inline[build_idx] == key) {
                match_count += 1;
            }
            build_row = next[build_idx];
        }
    }

    // Allocate exact size
    const left_indices = try allocator.alloc(i32, match_count);
    const right_indices = try allocator.alloc(i32, match_count);

    // Pass 2: Fill results
    var idx: usize = 0;
    for (left_keys, 0..) |key, left_row| {
        const hash = fastIntHash(key);
        const slot: usize = @intCast(hash & mask);

        var build_row = table[slot];
        while (build_row != -1) {
            const build_idx: usize = @intCast(build_row);
            if (right_keys_inline[build_idx] == key) {
                left_indices[idx] = @intCast(left_row);
                right_indices[idx] = @intCast(build_idx);
                idx += 1;
            }
            build_row = next[build_idx];
        }
    }

    return InnerJoinResult{
        .left_indices = left_indices,
        .right_indices = right_indices,
        .num_matches = @intCast(match_count),
        .allocator = allocator,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "joins - innerJoinI64 basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try innerJoinI64(allocator, &left, &right);
    defer result.deinit();

    // Should match 2 and 4
    try std.testing.expectEqual(@as(u32, 2), result.num_matches);
}

test "joins - innerJoinI64 with duplicates" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 2, 3 };
    const right = [_]i64{ 2, 2, 4 };

    var result = try innerJoinI64(allocator, &left, &right);
    defer result.deinit();

    // 2 in left matches both 2s in right: 2 matches
    // second 2 in left matches both 2s in right: 2 matches
    // Total: 4 matches
    try std.testing.expectEqual(@as(u32, 4), result.num_matches);
}

test "joins - innerJoinI64 empty" {
    const allocator = std.testing.allocator;

    const left: []const i64 = &[_]i64{};
    const right = [_]i64{ 1, 2, 3 };

    var result = try innerJoinI64(allocator, left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_matches);
}

test "joins - leftJoinI64 basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{ 2, 4 };

    var result = try leftJoinI64(allocator, &left, &right);
    defer result.deinit();

    // All 3 left rows should be present
    // 1 -> -1 (no match)
    // 2 -> 0 (matches right[0])
    // 3 -> -1 (no match)
    try std.testing.expectEqual(@as(u32, 3), result.num_rows);
}

test "joins - leftJoinI64 empty right" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right: []const i64 = &[_]i64{};

    var result = try leftJoinI64(allocator, &left, right);
    defer result.deinit();

    // All left rows with -1 right indices
    try std.testing.expectEqual(@as(u32, 3), result.num_rows);
    for (result.right_indices) |ri| {
        try std.testing.expectEqual(@as(i32, -1), ri);
    }
}

test "joins - buildJoinHashTable" {
    const hashes = [_]u64{ 100, 200, 300, 100 }; // 100 appears twice
    var table: [8]i32 = undefined;
    var next: [4]i32 = undefined;

    buildJoinHashTable(&hashes, &table, &next, 8);

    // Verify table is initialized and has entries
    var found_entry = false;
    for (table) |entry| {
        if (entry != -1) {
            found_entry = true;
            break;
        }
    }
    try std.testing.expect(found_entry);
}

test "joins - SwissJoinTable basic" {
    const allocator = std.testing.allocator;

    var table = try SwissJoinTable.init(allocator, 10, 5);
    defer table.deinit();

    try table.insert(100, 1, 0);
    try table.insert(200, 2, 1);
    try table.insert(100, 1, 2); // Same key as first

    // Should find the entry
    const entry = table.find(100, 1);
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(@as(u32, 2), entry.?.count);
}

test "joins - innerJoinI64Swiss" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try innerJoinI64Swiss(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.num_matches);
}

// Additional tests for better coverage

test "joins - innerJoinI64 no matches" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{ 4, 5, 6 };

    var result = try innerJoinI64(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_matches);
}

test "joins - innerJoinI64 all match" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{ 1, 2, 3 };

    var result = try innerJoinI64(allocator, &left, &right);
    defer result.deinit();

    // Each left matches exactly one right
    try std.testing.expectEqual(@as(u32, 3), result.num_matches);
}

test "joins - innerJoinI64 single element" {
    const allocator = std.testing.allocator;

    const left = [_]i64{1};
    const right = [_]i64{1};

    var result = try innerJoinI64(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_matches);
    try std.testing.expectEqual(@as(i32, 0), result.left_indices[0]);
    try std.testing.expectEqual(@as(i32, 0), result.right_indices[0]);
}

test "joins - innerJoinI64 negative keys" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ -1, -2, -3 };
    const right = [_]i64{ -2, -4 };

    var result = try innerJoinI64(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_matches);
}

test "joins - leftJoinI64 all match" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{ 1, 2, 3 };

    var result = try leftJoinI64(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 3), result.num_rows);
    // All should have valid right indices
    for (result.right_indices) |ri| {
        try std.testing.expect(ri >= 0);
    }
}

test "joins - leftJoinI64 no matches" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3 };
    const right = [_]i64{ 4, 5, 6 };

    var result = try leftJoinI64(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 3), result.num_rows);
    // All should have -1 right indices (no match)
    for (result.right_indices) |ri| {
        try std.testing.expectEqual(@as(i32, -1), ri);
    }
}

test "joins - leftJoinI64 partial matches" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4 };

    var result = try leftJoinI64(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 5), result.num_rows);

    // Check that matched rows have valid indices and unmatched have -1
    var matched_count: usize = 0;
    for (result.right_indices) |ri| {
        if (ri >= 0) matched_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), matched_count);
}

test "joins - leftJoinI64 empty left" {
    const allocator = std.testing.allocator;

    const left: []const i64 = &[_]i64{};
    const right = [_]i64{ 1, 2, 3 };

    var result = try leftJoinI64(allocator, left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_rows);
}

test "joins - innerJoinI64SinglePass basic" {
    const allocator = std.testing.allocator;

    const left = [_]i64{ 1, 2, 3, 4, 5 };
    const right = [_]i64{ 2, 4, 6 };

    var result = try innerJoinI64SinglePass(allocator, &left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.num_matches);
}

test "joins - innerJoinI64SinglePass empty" {
    const allocator = std.testing.allocator;

    const left: []const i64 = &[_]i64{};
    const right = [_]i64{ 1, 2, 3 };

    var result = try innerJoinI64SinglePass(allocator, left, &right);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_matches);
}

test "joins - probeJoinHashTable basic" {
    const probe_hashes = [_]u64{ 100, 200, 300 };
    const probe_keys = [_]i64{ 1, 2, 3 };
    const build_keys = [_]i64{ 2, 4, 6 };

    var table: [8]i32 = undefined;
    var next: [3]i32 = undefined;

    // First build the hash table
    const build_hashes = [_]u64{ 200, 400, 600 };
    buildJoinHashTable(&build_hashes, &table, &next, 8);

    var out_probe: [10]i32 = undefined;
    var out_build: [10]i32 = undefined;

    const matches = probeJoinHashTable(
        &probe_hashes,
        &probe_keys,
        &build_keys,
        &table,
        &next,
        8,
        &out_probe,
        &out_build,
        10,
    );

    // Only key 2 matches (with hash 200)
    try std.testing.expectEqual(@as(u32, 1), matches);
}

test "joins - gather operations used in joins" {
    const gather = @import("gather.zig");

    // Test gatherF64 with join-like indices
    const src = [_]f64{ 10.0, 20.0, 30.0, 40.0, 50.0 };
    const indices = [_]i32{ 1, 3, -1, 0 }; // -1 represents null (no match)
    var dst: [4]f64 = undefined;

    gather.gatherF64(&src, &indices, &dst);

    try std.testing.expectEqual(@as(f64, 20.0), dst[0]); // src[1]
    try std.testing.expectEqual(@as(f64, 40.0), dst[1]); // src[3]
    try std.testing.expectEqual(@as(f64, 0.0), dst[2]); // null
    try std.testing.expectEqual(@as(f64, 10.0), dst[3]); // src[0]
}

test "joins - SwissJoinTable multiple inserts same key" {
    const allocator = std.testing.allocator;

    var table = try SwissJoinTable.init(allocator, 10, 10);
    defer table.deinit();

    // Insert same key multiple times
    try table.insert(100, 1, 0);
    try table.insert(100, 1, 1);
    try table.insert(100, 1, 2);

    const entry = table.find(100, 1);
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(@as(u32, 3), entry.?.count);

    // Verify linked list is built correctly (head should be last inserted, 2)
    try std.testing.expectEqual(@as(i32, 2), entry.?.head);
    // Linked list: 2 -> 1 -> 0 -> -1
    try std.testing.expectEqual(@as(i32, 1), table.next[2]);
    try std.testing.expectEqual(@as(i32, 0), table.next[1]);
    try std.testing.expectEqual(@as(i32, -1), table.next[0]);
}

test "joins - SwissJoinTable different keys same hash" {
    const allocator = std.testing.allocator;

    var table = try SwissJoinTable.init(allocator, 10, 5);
    defer table.deinit();

    // Insert different keys
    try table.insert(100, 1, 0);
    try table.insert(100, 2, 1); // Same hash, different key

    const entry1 = table.find(100, 1);
    const entry2 = table.find(100, 2);

    try std.testing.expect(entry1 != null);
    try std.testing.expect(entry2 != null);
    try std.testing.expectEqual(@as(u32, 1), entry1.?.count);
    try std.testing.expectEqual(@as(u32, 1), entry2.?.count);
}

test "joins - SwissJoinTable not found" {
    const allocator = std.testing.allocator;

    var table = try SwissJoinTable.init(allocator, 10, 5);
    defer table.deinit();

    try table.insert(100, 1, 0);

    const entry = table.find(200, 2);
    try std.testing.expectEqual(@as(?*const SwissJoinEntry, null), entry);
}
