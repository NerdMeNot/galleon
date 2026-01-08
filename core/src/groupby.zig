const std = @import("std");
const Allocator = std.mem.Allocator;
const simd = @import("simd.zig");

// ============================================================================
// GroupBy Hash Table
// ============================================================================

/// GroupByResult holds the result of a groupby operation
pub const GroupByResult = struct {
    /// Group ID for each row (0 to num_groups-1)
    group_ids: []u32,
    /// Number of unique groups
    num_groups: u32,
    /// Allocator used (for cleanup)
    allocator: Allocator,

    pub fn deinit(self: *GroupByResult) void {
        self.allocator.free(self.group_ids);
    }
};

/// Extended GroupByResult with firstRowIdx and groupCounts (eliminates Go loops)
pub const GroupByResultExt = struct {
    /// Group ID for each row (0 to num_groups-1)
    group_ids: []u32,
    /// Number of unique groups
    num_groups: u32,
    /// First row index for each group
    first_row_idx: []u32,
    /// Count of rows per group
    group_counts: []u32,
    /// Allocator used (for cleanup)
    allocator: Allocator,

    pub fn deinit(self: *GroupByResultExt) void {
        self.allocator.free(self.group_ids);
        self.allocator.free(self.first_row_idx);
        self.allocator.free(self.group_counts);
    }
};

/// Hash table entry for groupby
const HashEntry = struct {
    hash: u64,
    group_id: u32,
    next: i32, // -1 = end of chain
};

/// Open-addressed hash table for grouping
pub const GroupByHashTable = struct {
    allocator: Allocator,
    // Hash buckets (index into entries, -1 = empty)
    buckets: []i32,
    // Linked entries for collision handling
    entries: []HashEntry,
    num_entries: u32,
    table_size: u32,

    pub fn init(allocator: Allocator, expected_groups: usize) !GroupByHashTable {
        // Use power of 2 table size for fast modulo
        const table_size = nextPowerOf2(@intCast(expected_groups * 2));

        const buckets = try allocator.alloc(i32, table_size);
        @memset(buckets, -1);

        // Pre-allocate entries (will grow if needed)
        const entries = try allocator.alloc(HashEntry, expected_groups);

        return GroupByHashTable{
            .allocator = allocator,
            .buckets = buckets,
            .entries = entries,
            .num_entries = 0,
            .table_size = @intCast(table_size),
        };
    }

    pub fn deinit(self: *GroupByHashTable) void {
        self.allocator.free(self.buckets);
        self.allocator.free(self.entries);
    }

    /// Insert a hash and return its group ID
    pub fn insertOrGet(self: *GroupByHashTable, hash: u64) !u32 {
        const bucket_idx = hash & (self.table_size - 1);
        var entry_idx = self.buckets[@intCast(bucket_idx)];

        // Search chain for existing hash
        while (entry_idx >= 0) {
            const entry = &self.entries[@intCast(entry_idx)];
            if (entry.hash == hash) {
                return entry.group_id;
            }
            entry_idx = entry.next;
        }

        // Not found - create new entry
        const group_id = self.num_entries;

        // Grow entries array if needed
        if (self.num_entries >= self.entries.len) {
            const new_size = self.entries.len * 2;
            self.entries = try self.allocator.realloc(self.entries, new_size);
        }

        // Add new entry at front of chain
        const new_entry_idx: i32 = @intCast(self.num_entries);
        self.entries[@intCast(new_entry_idx)] = HashEntry{
            .hash = hash,
            .group_id = group_id,
            .next = self.buckets[@intCast(bucket_idx)],
        };
        self.buckets[@intCast(bucket_idx)] = new_entry_idx;
        self.num_entries += 1;

        return group_id;
    }
};

fn nextPowerOf2(n: u32) u32 {
    if (n <= 1) return 1;
    var v = n - 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

// ============================================================================
// Robin Hood Hash Table for GroupBy
// ============================================================================
// Robin Hood hashing uses open addressing with linear probing.
// Key insight: when inserting, if the new element has traveled further than
// the element at the current position, swap them ("steal" the spot).
// This keeps probe sequences short and improves cache locality.

/// Entry for Robin Hood hash table with sum aggregation
const RHEntrySum = struct {
    hash: u64, // 0 means empty slot
    first_row: u32, // index into original data for key lookup
    sum: f64,

    const EMPTY: RHEntrySum = .{ .hash = 0, .first_row = 0, .sum = 0 };
};

/// Entry for Robin Hood hash table with multi-aggregation
const RHEntryMultiAgg = struct {
    hash: u64, // 0 means empty slot
    first_row: u32,
    sum: f64,
    min: f64,
    max: f64,
    count: u64,

    const EMPTY: RHEntryMultiAgg = .{ .hash = 0, .first_row = 0, .sum = 0, .min = 0, .max = 0, .count = 0 };
};

/// Calculate probe distance (how far from ideal position)
inline fn probeDistance(hash: u64, slot: usize, mask: usize) usize {
    const ideal = hash & mask;
    return (slot -% ideal) & mask;
}

// ============================================================================
// Swiss Table Style Hash Table for GroupBy
// ============================================================================
// Uses 1-byte control codes for SIMD probing (16 at a time).
// Control byte = high 7 bits of hash | 0x80 (occupied), or 0x00 (empty)
// This enables comparing 16 slots at once with a single SIMD operation.

/// Entry for Swiss Table with sum aggregation
/// Stores key directly to eliminate memory indirection during comparison
/// Hash removed - can be recomputed from key when needed (only during grow)
const SwissEntrySum = struct {
    key: i64, // Store key directly - faster comparison
    sum: f64,
    // Entry size: 16 bytes (fits nicely in cache lines)
};

/// Entry for Swiss Table with multi-aggregation
/// Stores key directly to eliminate memory indirection during comparison
/// Hash removed - can be recomputed from key when needed (only during grow)
const SwissEntryMultiAgg = struct {
    key: i64, // Store key directly - faster comparison
    sum: f64,
    min: f64,
    max: f64,
    count: u64,
    // Entry size: 40 bytes (was 48 with hash)
};

/// Control byte constants
const CTRL_EMPTY: u8 = 0x00;
const CTRL_DELETED: u8 = 0x7F; // Not used in this implementation

/// Extract control byte from hash (high 7 bits | 0x80 to mark as occupied)
inline fn h2(hash: u64) u8 {
    return @as(u8, @truncate(hash >> 57)) | 0x80;
}

/// SIMD vector for control byte comparison (16 bytes)
const CTRL_GROUP_SIZE = 16;
const CtrlVec = @Vector(CTRL_GROUP_SIZE, u8);

/// Swiss Table hash table for groupby sum aggregation
/// Uses control bytes for 16-wide SIMD probing
pub const SIMDHashTableSum = struct {
    ctrl: []u8, // Control bytes for SIMD matching
    entries: []SwissEntrySum,
    mask: usize,
    count: u32,
    allocator: Allocator,

    const LOAD_FACTOR_PERCENT: usize = 87; // Swiss tables can handle higher load

    pub fn init(allocator: Allocator, expected_groups: usize) !SIMDHashTableSum {
        // Size must be power of 2, minimum CTRL_GROUP_SIZE for proper SIMD operation
        const min_size = (expected_groups * 100) / LOAD_FACTOR_PERCENT;
        const table_size = nextPowerOf2(@intCast(@max(min_size, CTRL_GROUP_SIZE)));

        // Allocate control bytes with CTRL_GROUP_SIZE extra for wrap-around
        const ctrl = try allocator.alloc(u8, table_size + CTRL_GROUP_SIZE);
        @memset(ctrl, CTRL_EMPTY);

        const entries = try allocator.alloc(SwissEntrySum, table_size);

        return SIMDHashTableSum{
            .ctrl = ctrl,
            .entries = entries,
            .mask = table_size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SIMDHashTableSum) void {
        self.allocator.free(self.ctrl);
        self.allocator.free(self.entries);
    }

    /// Swiss Table style insert or update with 16-wide SIMD probing
    /// Key is stored directly in entry - no memory indirection for comparison
    pub fn insertOrUpdate(
        self: *SIMDHashTableSum,
        hash: u64,
        key: i64,
    ) !*SwissEntrySum {
        const ctrl_byte = h2(hash);
        const ctrl_vec: CtrlVec = @splat(ctrl_byte);
        const empty_vec: CtrlVec = @splat(CTRL_EMPTY);

        var group_idx = hash & self.mask;
        const table_len = self.entries.len;

        // Probe groups of 16 slots
        while (true) {
            // Load 16 control bytes at once
            const ctrl_group: CtrlVec = self.ctrl[group_idx..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte (potential hash match)
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            // Check each potential match
            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (group_idx + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                // Direct key comparison - no memory indirection!
                if (entry.key == key) {
                    return entry;
                }
                match_bits &= match_bits - 1; // Clear lowest set bit
            }

            // Check for empty slot
            const empty_mask = ctrl_group == empty_vec;
            const empty_bits = @as(u16, @bitCast(empty_mask));

            if (empty_bits != 0) {
                const bit_pos = @ctz(empty_bits);
                const slot = (group_idx + bit_pos) & self.mask;

                // Insert new entry with key stored directly
                self.ctrl[slot] = ctrl_byte;
                // Mirror to wrap-around zone if needed
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[table_len + slot] = ctrl_byte;
                }

                self.entries[slot] = .{
                    .key = key,
                    .sum = 0,
                };
                self.count += 1;

                // Check if we need to grow
                if (self.count * 100 > table_len * LOAD_FACTOR_PERCENT) {
                    try self.grow();
                    return self.find(key);
                }
                return &self.entries[slot];
            }

            // Move to next group (quadratic probing in groups)
            group_idx = (group_idx + CTRL_GROUP_SIZE) & self.mask;
        }
    }

    /// Find entry by key (recomputes hash from key)
    fn find(self: *SIMDHashTableSum, key: i64) *SwissEntrySum {
        const hash = rapidHash64(@bitCast(key));
        const ctrl_byte = h2(hash);
        const ctrl_vec: CtrlVec = @splat(ctrl_byte);

        var group_idx = hash & self.mask;

        while (true) {
            const ctrl_group: CtrlVec = self.ctrl[group_idx..][0..CTRL_GROUP_SIZE].*;
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (group_idx + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                // Direct key comparison
                if (entry.key == key) {
                    return entry;
                }
                match_bits &= match_bits - 1;
            }
            group_idx = (group_idx + CTRL_GROUP_SIZE) & self.mask;
        }
    }

    /// Grow table - recomputes hash from key for each entry
    fn grow(self: *SIMDHashTableSum) !void {
        const old_ctrl = self.ctrl;
        const old_entries = self.entries;
        const old_len = self.entries.len;
        const new_size = old_len * 2;

        self.ctrl = try self.allocator.alloc(u8, new_size + CTRL_GROUP_SIZE);
        @memset(self.ctrl, CTRL_EMPTY);
        self.entries = try self.allocator.alloc(SwissEntrySum, new_size);
        self.mask = new_size - 1;
        self.count = 0;

        // Re-insert all entries - recompute hash from key
        for (0..old_len) |i| {
            if (old_ctrl[i] != CTRL_EMPTY) {
                const entry = old_entries[i];
                const hash = rapidHash64(@bitCast(entry.key));
                const ctrl_byte = h2(hash);
                var slot = hash & self.mask;

                // Find empty slot with linear probe
                while (self.ctrl[slot] != CTRL_EMPTY) {
                    slot = (slot + 1) & self.mask;
                }

                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[new_size + slot] = ctrl_byte;
                }
                self.entries[slot] = entry;
                self.count += 1;
            }
        }

        self.allocator.free(old_ctrl);
        self.allocator.free(old_entries);
    }
};

/// Swiss Table hash table for groupby multi-aggregation
pub const SIMDHashTableMultiAgg = struct {
    ctrl: []u8,
    entries: []SwissEntryMultiAgg,
    mask: usize,
    count: u32,
    allocator: Allocator,

    const LOAD_FACTOR_PERCENT: usize = 87;

    pub fn init(allocator: Allocator, expected_groups: usize) !SIMDHashTableMultiAgg {
        const min_size = (expected_groups * 100) / LOAD_FACTOR_PERCENT;
        const table_size = nextPowerOf2(@intCast(@max(min_size, CTRL_GROUP_SIZE)));

        const ctrl = try allocator.alloc(u8, table_size + CTRL_GROUP_SIZE);
        @memset(ctrl, CTRL_EMPTY);

        const entries = try allocator.alloc(SwissEntryMultiAgg, table_size);

        return SIMDHashTableMultiAgg{
            .ctrl = ctrl,
            .entries = entries,
            .mask = table_size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SIMDHashTableMultiAgg) void {
        self.allocator.free(self.ctrl);
        self.allocator.free(self.entries);
    }

    /// Key is stored directly in entry - no memory indirection for comparison
    pub fn insertOrUpdate(
        self: *SIMDHashTableMultiAgg,
        hash: u64,
        key: i64,
    ) !*SwissEntryMultiAgg {
        const ctrl_byte = h2(hash);
        const ctrl_vec: CtrlVec = @splat(ctrl_byte);
        const empty_vec: CtrlVec = @splat(CTRL_EMPTY);

        var group_idx = hash & self.mask;
        const table_len = self.entries.len;

        while (true) {
            const ctrl_group: CtrlVec = self.ctrl[group_idx..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (group_idx + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                // Direct key comparison - no memory indirection!
                if (entry.key == key) {
                    return entry;
                }
                match_bits &= match_bits - 1;
            }

            // Check for empty slot
            const empty_mask = ctrl_group == empty_vec;
            const empty_bits = @as(u16, @bitCast(empty_mask));

            if (empty_bits != 0) {
                const bit_pos = @ctz(empty_bits);
                const slot = (group_idx + bit_pos) & self.mask;

                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[table_len + slot] = ctrl_byte;
                }

                self.entries[slot] = .{
                    .key = key,
                    .sum = 0,
                    .min = std.math.inf(f64),
                    .max = -std.math.inf(f64),
                    .count = 0,
                };
                self.count += 1;

                if (self.count * 100 > table_len * LOAD_FACTOR_PERCENT) {
                    try self.grow();
                    return self.find(key);
                }
                return &self.entries[slot];
            }

            group_idx = (group_idx + CTRL_GROUP_SIZE) & self.mask;
        }
    }

    /// Find entry by key (recomputes hash from key)
    fn find(self: *SIMDHashTableMultiAgg, key: i64) *SwissEntryMultiAgg {
        const hash = rapidHash64(@bitCast(key));
        const ctrl_byte = h2(hash);
        const ctrl_vec: CtrlVec = @splat(ctrl_byte);

        var group_idx = hash & self.mask;

        while (true) {
            const ctrl_group: CtrlVec = self.ctrl[group_idx..][0..CTRL_GROUP_SIZE].*;
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (group_idx + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                // Direct key comparison
                if (entry.key == key) {
                    return entry;
                }
                match_bits &= match_bits - 1;
            }
            group_idx = (group_idx + CTRL_GROUP_SIZE) & self.mask;
        }
    }

    /// Grow table - recomputes hash from key for each entry
    fn grow(self: *SIMDHashTableMultiAgg) !void {
        const old_ctrl = self.ctrl;
        const old_entries = self.entries;
        const old_len = self.entries.len;
        const new_size = old_len * 2;

        self.ctrl = try self.allocator.alloc(u8, new_size + CTRL_GROUP_SIZE);
        @memset(self.ctrl, CTRL_EMPTY);
        self.entries = try self.allocator.alloc(SwissEntryMultiAgg, new_size);
        self.mask = new_size - 1;
        self.count = 0;

        // Re-insert all entries - recompute hash from key
        for (0..old_len) |i| {
            if (old_ctrl[i] != CTRL_EMPTY) {
                const entry = old_entries[i];
                const hash = rapidHash64(@bitCast(entry.key));
                const ctrl_byte = h2(hash);
                var slot = hash & self.mask;

                while (self.ctrl[slot] != CTRL_EMPTY) {
                    slot = (slot + 1) & self.mask;
                }

                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[new_size + slot] = ctrl_byte;
                }
                self.entries[slot] = entry;
                self.count += 1;
            }
        }

        self.allocator.free(old_ctrl);
        self.allocator.free(old_entries);
    }
};

// ============================================================================
// Two-Phase GroupBy with Vectorized Aggregation
// ============================================================================
// Phase 1: Assign group IDs using Swiss Table (no aggregation in hash table)
// Phase 2: Aggregate into dense arrays using group IDs as indices
// This enables better cache locality and potentially SIMD aggregation.

/// Lightweight Swiss Table entry for group ID assignment only
const GroupIdEntry = struct {
    first_row: u32,
    hash: u64,
    group_id: u32,
};

/// Swiss Table for group ID assignment (no aggregation)
pub const GroupIdTable = struct {
    ctrl: []u8,
    entries: []GroupIdEntry,
    mask: usize,
    count: u32,
    allocator: Allocator,

    const LOAD_FACTOR_PERCENT: usize = 87;

    pub fn init(allocator: Allocator, expected_groups: usize) !GroupIdTable {
        const min_size = (expected_groups * 100) / LOAD_FACTOR_PERCENT;
        const table_size = nextPowerOf2(@intCast(@max(min_size, CTRL_GROUP_SIZE)));

        const ctrl = try allocator.alloc(u8, table_size + CTRL_GROUP_SIZE);
        @memset(ctrl, CTRL_EMPTY);

        const entries = try allocator.alloc(GroupIdEntry, table_size);

        return GroupIdTable{
            .ctrl = ctrl,
            .entries = entries,
            .mask = table_size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *GroupIdTable) void {
        self.allocator.free(self.ctrl);
        self.allocator.free(self.entries);
    }

    /// Returns group ID for this key (creates new group if needed)
    pub fn getOrCreateGroupId(
        self: *GroupIdTable,
        hash: u64,
        key: i64,
        key_data: []const i64,
        row_idx: u32,
    ) !u32 {
        const ctrl_byte = h2(hash);
        const ctrl_vec: CtrlVec = @splat(ctrl_byte);
        const empty_vec: CtrlVec = @splat(CTRL_EMPTY);

        var group_idx = hash & self.mask;
        const table_len = self.entries.len;

        while (true) {
            const ctrl_group: CtrlVec = self.ctrl[group_idx..][0..CTRL_GROUP_SIZE].*;

            // Check for matching control byte
            const match_mask = ctrl_group == ctrl_vec;
            var match_bits = @as(u16, @bitCast(match_mask));

            while (match_bits != 0) {
                const bit_pos = @ctz(match_bits);
                const slot = (group_idx + bit_pos) & self.mask;
                const entry = &self.entries[slot];

                if (entry.hash == hash and key_data[entry.first_row] == key) {
                    return entry.group_id;
                }
                match_bits &= match_bits - 1;
            }

            // Check for empty slot
            const empty_mask = ctrl_group == empty_vec;
            const empty_bits = @as(u16, @bitCast(empty_mask));

            if (empty_bits != 0) {
                const bit_pos = @ctz(empty_bits);
                const slot = (group_idx + bit_pos) & self.mask;

                const new_group_id = self.count;
                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[table_len + slot] = ctrl_byte;
                }

                self.entries[slot] = .{
                    .first_row = row_idx,
                    .hash = hash,
                    .group_id = new_group_id,
                };
                self.count += 1;

                // Check if we need to grow
                if (self.count * 100 > table_len * LOAD_FACTOR_PERCENT) {
                    try self.grow(key_data);
                }
                return new_group_id;
            }

            group_idx = (group_idx + CTRL_GROUP_SIZE) & self.mask;
        }
    }

    fn grow(self: *GroupIdTable, key_data: []const i64) !void {
        _ = key_data;
        const old_ctrl = self.ctrl;
        const old_entries = self.entries;
        const old_len = self.entries.len;
        const new_size = old_len * 2;

        self.ctrl = try self.allocator.alloc(u8, new_size + CTRL_GROUP_SIZE);
        @memset(self.ctrl, CTRL_EMPTY);
        self.entries = try self.allocator.alloc(GroupIdEntry, new_size);
        self.mask = new_size - 1;

        // Note: don't reset count - we're just rehashing
        for (0..old_len) |i| {
            if (old_ctrl[i] != CTRL_EMPTY) {
                const entry = old_entries[i];
                const ctrl_byte = h2(entry.hash);
                var slot = entry.hash & self.mask;

                while (self.ctrl[slot] != CTRL_EMPTY) {
                    slot = (slot + 1) & self.mask;
                }

                self.ctrl[slot] = ctrl_byte;
                if (slot < CTRL_GROUP_SIZE) {
                    self.ctrl[new_size + slot] = ctrl_byte;
                }
                self.entries[slot] = entry;
            }
        }

        self.allocator.free(old_ctrl);
        self.allocator.free(old_entries);
    }

    /// Get first row index for each group (for extracting keys)
    pub fn getFirstRows(self: *const GroupIdTable, out: []u32) void {
        const table_len = self.entries.len;
        for (self.ctrl[0..table_len], self.entries) |ctrl_byte, entry| {
            if (ctrl_byte != CTRL_EMPTY) {
                out[entry.group_id] = entry.first_row;
            }
        }
    }
};

/// SIMD-accelerated aggregation functions for dense arrays
/// These use prefetching and loop unrolling for better performance

/// Aggregate sum with prefetching (4x unrolled)
fn aggregateSumF64(values: []const f64, group_ids: []const u32, sums: []f64) void {
    @memset(sums, 0);

    const n = values.len;

    // 8x unrolled with interleaved prefetch
    var i: usize = 0;
    while (i + 8 <= n) : (i += 8) {
        // Prefetch 32 slots ahead
        @prefetch(sums.ptr + group_ids[i], .{ .locality = 3 });
        @prefetch(sums.ptr + group_ids[i + 4], .{ .locality = 3 });

        sums[group_ids[i]] += values[i];
        sums[group_ids[i + 1]] += values[i + 1];
        sums[group_ids[i + 2]] += values[i + 2];
        sums[group_ids[i + 3]] += values[i + 3];
        sums[group_ids[i + 4]] += values[i + 4];
        sums[group_ids[i + 5]] += values[i + 5];
        sums[group_ids[i + 6]] += values[i + 6];
        sums[group_ids[i + 7]] += values[i + 7];
    }

    // Handle remainder
    while (i < n) : (i += 1) {
        sums[group_ids[i]] += values[i];
    }
}

/// Aggregate min with prefetching
fn aggregateMinF64(values: []const f64, group_ids: []const u32, mins: []f64) void {
    @memset(mins, std.math.inf(f64));

    const n = values.len;
    const PREFETCH_DIST: usize = 16;

    var i: usize = 0;
    while (i + 4 <= n) : (i += 4) {
        if (i + PREFETCH_DIST + 4 <= n) {
            @prefetch(mins.ptr + group_ids[i + PREFETCH_DIST], .{ .locality = 1 });
            @prefetch(mins.ptr + group_ids[i + PREFETCH_DIST + 2], .{ .locality = 1 });
        }

        const g0 = group_ids[i];
        const g1 = group_ids[i + 1];
        const g2 = group_ids[i + 2];
        const g3 = group_ids[i + 3];

        mins[g0] = @min(mins[g0], values[i]);
        mins[g1] = @min(mins[g1], values[i + 1]);
        mins[g2] = @min(mins[g2], values[i + 2]);
        mins[g3] = @min(mins[g3], values[i + 3]);
    }

    while (i < n) : (i += 1) {
        const g = group_ids[i];
        mins[g] = @min(mins[g], values[i]);
    }
}

/// Aggregate max with prefetching
fn aggregateMaxF64(values: []const f64, group_ids: []const u32, maxs: []f64) void {
    @memset(maxs, -std.math.inf(f64));

    const n = values.len;
    const PREFETCH_DIST: usize = 16;

    var i: usize = 0;
    while (i + 4 <= n) : (i += 4) {
        if (i + PREFETCH_DIST + 4 <= n) {
            @prefetch(maxs.ptr + group_ids[i + PREFETCH_DIST], .{ .locality = 1 });
            @prefetch(maxs.ptr + group_ids[i + PREFETCH_DIST + 2], .{ .locality = 1 });
        }

        const g0 = group_ids[i];
        const g1 = group_ids[i + 1];
        const g2 = group_ids[i + 2];
        const g3 = group_ids[i + 3];

        maxs[g0] = @max(maxs[g0], values[i]);
        maxs[g1] = @max(maxs[g1], values[i + 1]);
        maxs[g2] = @max(maxs[g2], values[i + 2]);
        maxs[g3] = @max(maxs[g3], values[i + 3]);
    }

    while (i < n) : (i += 1) {
        const g = group_ids[i];
        maxs[g] = @max(maxs[g], values[i]);
    }
}

/// Aggregate count with prefetching
fn aggregateCount(group_ids: []const u32, counts: []u64) void {
    @memset(counts, 0);

    const n = group_ids.len;
    const PREFETCH_DIST: usize = 16;

    var i: usize = 0;
    while (i + 4 <= n) : (i += 4) {
        if (i + PREFETCH_DIST + 4 <= n) {
            @prefetch(counts.ptr + group_ids[i + PREFETCH_DIST], .{ .locality = 1 });
            @prefetch(counts.ptr + group_ids[i + PREFETCH_DIST + 2], .{ .locality = 1 });
        }

        counts[group_ids[i]] += 1;
        counts[group_ids[i + 1]] += 1;
        counts[group_ids[i + 2]] += 1;
        counts[group_ids[i + 3]] += 1;
    }

    while (i < n) : (i += 1) {
        counts[group_ids[i]] += 1;
    }
}

/// Combined aggregation for sum, min, max, count in a single pass
/// More cache-efficient than separate passes
fn aggregateAllF64(
    values: []const f64,
    group_ids: []const u32,
    sums: []f64,
    mins: []f64,
    maxs: []f64,
    counts: []u64,
) void {
    @memset(sums, 0);
    @memset(mins, std.math.inf(f64));
    @memset(maxs, -std.math.inf(f64));
    @memset(counts, 0);

    const n = values.len;

    // 8x unrolled
    var i: usize = 0;
    while (i + 8 <= n) : (i += 8) {
        inline for (0..8) |j| {
            const g = group_ids[i + j];
            const v = values[i + j];
            sums[g] += v;
            mins[g] = @min(mins[g], v);
            maxs[g] = @max(maxs[g], v);
            counts[g] += 1;
        }
    }

    // Handle remainder
    while (i < n) : (i += 1) {
        const g = group_ids[i];
        const v = values[i];
        sums[g] += v;
        mins[g] = @min(mins[g], v);
        maxs[g] = @max(maxs[g], v);
        counts[g] += 1;
    }
}

// ============================================================================
// Legacy Robin Hood Hash Table (kept for comparison)
// ============================================================================

/// Robin Hood hash table for groupby sum aggregation
/// Uses open addressing with linear probing and "stealing" for short probe sequences
pub const RobinHoodTableSum = struct {
    entries: []RHEntrySum,
    mask: usize,
    count: u32,
    allocator: Allocator,

    const LOAD_FACTOR_PERCENT: usize = 70;

    pub fn init(allocator: Allocator, expected_groups: usize) !RobinHoodTableSum {
        // Size for ~70% load factor, power of 2
        const min_size = (expected_groups * 100) / LOAD_FACTOR_PERCENT;
        const table_size = nextPowerOf2(@intCast(@max(min_size, 16)));

        const entries = try allocator.alloc(RHEntrySum, table_size);
        @memset(entries, RHEntrySum.EMPTY);

        return RobinHoodTableSum{
            .entries = entries,
            .mask = table_size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RobinHoodTableSum) void {
        self.allocator.free(self.entries);
    }

    /// Insert or update: returns pointer to entry for this key
    /// If new entry, first_row is set and sum initialized to 0
    pub fn insertOrUpdate(
        self: *RobinHoodTableSum,
        hash: u64,
        key: i64,
        key_data: []const i64,
        row_idx: u32,
    ) !*RHEntrySum {
        // Use hash | 1 to ensure hash is never 0 (0 = empty slot)
        const h = hash | 1;

        var slot = h & self.mask;
        var entry = RHEntrySum{
            .hash = h,
            .first_row = row_idx,
            .sum = 0,
        };
        var dist: usize = 0;

        while (true) {
            const existing = &self.entries[slot];

            // Empty slot - insert here
            if (existing.hash == 0) {
                existing.* = entry;
                self.count += 1;

                // Check if we need to grow
                if (self.count * 100 > self.entries.len * LOAD_FACTOR_PERCENT) {
                    try self.grow(key_data);
                    // After grow, find the entry again
                    return self.find(h, key, key_data);
                }

                return existing;
            }

            // Same key? Check actual key value
            if (existing.hash == h) {
                if (key_data[existing.first_row] == key) {
                    return existing;
                }
            }

            // Robin Hood: if we've traveled further, swap
            const existing_dist = probeDistance(existing.hash, slot, self.mask);
            if (dist > existing_dist) {
                // Swap entries
                const tmp = existing.*;
                existing.* = entry;
                entry = tmp;
                dist = existing_dist;
            }

            // Linear probe
            slot = (slot + 1) & self.mask;
            dist += 1;
        }
    }

    /// Find an existing entry (used after grow)
    fn find(self: *RobinHoodTableSum, hash: u64, key: i64, key_data: []const i64) *RHEntrySum {
        var slot = hash & self.mask;

        while (true) {
            const existing = &self.entries[slot];
            if (existing.hash == hash and key_data[existing.first_row] == key) {
                return existing;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    /// Grow the table when load factor exceeded
    fn grow(self: *RobinHoodTableSum, key_data: []const i64) !void {
        _ = key_data; // Not needed for re-insert
        const old_entries = self.entries;
        const new_size = self.entries.len * 2;

        self.entries = try self.allocator.alloc(RHEntrySum, new_size);
        @memset(self.entries, RHEntrySum.EMPTY);
        self.mask = new_size - 1;
        self.count = 0;

        // Re-insert all entries using direct insertion (no grow check needed)
        for (old_entries) |old_entry| {
            if (old_entry.hash != 0) {
                var slot = old_entry.hash & self.mask;
                var entry = old_entry;
                var dist: usize = 0;

                while (true) {
                    const existing = &self.entries[slot];
                    if (existing.hash == 0) {
                        existing.* = entry;
                        self.count += 1;
                        break;
                    }
                    // Robin Hood swap if needed
                    const existing_dist = probeDistance(existing.hash, slot, self.mask);
                    if (dist > existing_dist) {
                        const tmp = existing.*;
                        existing.* = entry;
                        entry = tmp;
                        dist = existing_dist;
                    }
                    slot = (slot + 1) & self.mask;
                    dist += 1;
                }
            }
        }

        self.allocator.free(old_entries);
    }
};

/// Robin Hood hash table for groupby multi-aggregation
pub const RobinHoodTableMultiAgg = struct {
    entries: []RHEntryMultiAgg,
    mask: usize,
    count: u32,
    allocator: Allocator,

    const LOAD_FACTOR_PERCENT: usize = 70;

    pub fn init(allocator: Allocator, expected_groups: usize) !RobinHoodTableMultiAgg {
        const min_size = (expected_groups * 100) / LOAD_FACTOR_PERCENT;
        const table_size = nextPowerOf2(@intCast(@max(min_size, 16)));

        const entries = try allocator.alloc(RHEntryMultiAgg, table_size);
        @memset(entries, RHEntryMultiAgg.EMPTY);

        return RobinHoodTableMultiAgg{
            .entries = entries,
            .mask = table_size - 1,
            .count = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RobinHoodTableMultiAgg) void {
        self.allocator.free(self.entries);
    }

    pub fn insertOrUpdate(
        self: *RobinHoodTableMultiAgg,
        hash: u64,
        key: i64,
        key_data: []const i64,
        row_idx: u32,
    ) !*RHEntryMultiAgg {
        const h = hash | 1;

        var slot = h & self.mask;
        var entry = RHEntryMultiAgg{
            .hash = h,
            .first_row = row_idx,
            .sum = 0,
            .min = std.math.inf(f64),
            .max = -std.math.inf(f64),
            .count = 0,
        };
        var dist: usize = 0;

        while (true) {
            const existing = &self.entries[slot];

            if (existing.hash == 0) {
                existing.* = entry;
                self.count += 1;

                if (self.count * 100 > self.entries.len * LOAD_FACTOR_PERCENT) {
                    try self.grow(key_data);
                    return self.find(h, key, key_data);
                }

                return existing;
            }

            if (existing.hash == h) {
                if (key_data[existing.first_row] == key) {
                    return existing;
                }
            }

            const existing_dist = probeDistance(existing.hash, slot, self.mask);
            if (dist > existing_dist) {
                const tmp = existing.*;
                existing.* = entry;
                entry = tmp;
                dist = existing_dist;
            }

            slot = (slot + 1) & self.mask;
            dist += 1;
        }
    }

    fn find(self: *RobinHoodTableMultiAgg, hash: u64, key: i64, key_data: []const i64) *RHEntryMultiAgg {
        var slot = hash & self.mask;

        while (true) {
            const existing = &self.entries[slot];
            if (existing.hash == hash and key_data[existing.first_row] == key) {
                return existing;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    fn grow(self: *RobinHoodTableMultiAgg, key_data: []const i64) !void {
        _ = key_data; // Not needed for re-insert
        const old_entries = self.entries;
        const new_size = self.entries.len * 2;

        self.entries = try self.allocator.alloc(RHEntryMultiAgg, new_size);
        @memset(self.entries, RHEntryMultiAgg.EMPTY);
        self.mask = new_size - 1;
        self.count = 0;

        // Re-insert all entries using direct insertion (no grow check needed)
        for (old_entries) |old_entry| {
            if (old_entry.hash != 0) {
                var slot = old_entry.hash & self.mask;
                var entry = old_entry;
                var dist: usize = 0;

                while (true) {
                    const existing = &self.entries[slot];
                    if (existing.hash == 0) {
                        existing.* = entry;
                        self.count += 1;
                        break;
                    }
                    const existing_dist = probeDistance(existing.hash, slot, self.mask);
                    if (dist > existing_dist) {
                        const tmp = existing.*;
                        existing.* = entry;
                        entry = tmp;
                        dist = existing_dist;
                    }
                    slot = (slot + 1) & self.mask;
                    dist += 1;
                }
            }
        }

        self.allocator.free(old_entries);
    }
};

// ============================================================================
// GroupBy Operations
// ============================================================================

/// Compute group IDs for each row based on pre-computed hashes
/// This is the core groupby operation - it builds a hash table and assigns
/// each row to a group.
pub fn computeGroupIds(
    allocator: Allocator,
    hashes: []const u64,
) !GroupByResult {
    const n = hashes.len;
    if (n == 0) {
        return GroupByResult{
            .group_ids = try allocator.alloc(u32, 0),
            .num_groups = 0,
            .allocator = allocator,
        };
    }

    // Estimate number of groups (assume ~10% unique keys)
    const estimated_groups = @max(n / 10, 1024);

    var hash_table = try GroupByHashTable.init(allocator, estimated_groups);
    defer hash_table.deinit();

    // Allocate output
    const group_ids = try allocator.alloc(u32, n);

    // Process each row - assign to a group
    for (hashes, 0..) |hash, i| {
        group_ids[i] = try hash_table.insertOrGet(hash);
    }

    return GroupByResult{
        .group_ids = group_ids,
        .num_groups = hash_table.num_entries,
        .allocator = allocator,
    };
}

/// Compute group IDs with firstRowIdx and groupCounts in a single pass
/// This eliminates the Go loop that builds these after groupby
pub fn computeGroupIdsExt(
    allocator: Allocator,
    hashes: []const u64,
) !GroupByResultExt {
    const n = hashes.len;
    if (n == 0) {
        return GroupByResultExt{
            .group_ids = try allocator.alloc(u32, 0),
            .num_groups = 0,
            .first_row_idx = try allocator.alloc(u32, 0),
            .group_counts = try allocator.alloc(u32, 0),
            .allocator = allocator,
        };
    }

    // Estimate number of groups
    const estimated_groups = @max(n / 10, 1024);

    var hash_table = try GroupByHashTable.init(allocator, estimated_groups);
    defer hash_table.deinit();

    // Track first row index for each group (sentinel is maxInt)
    var first_rows = try std.ArrayList(u32).initCapacity(allocator, estimated_groups);
    defer first_rows.deinit(allocator);
    var counts = try std.ArrayList(u32).initCapacity(allocator, estimated_groups);
    defer counts.deinit(allocator);

    // Allocate output group_ids
    const group_ids = try allocator.alloc(u32, n);

    // Process each row
    for (hashes, 0..) |hash, i| {
        const bucket_idx = hash & (hash_table.table_size - 1);
        var entry_idx = hash_table.buckets[@intCast(bucket_idx)];
        var found = false;

        // Search chain for existing hash
        while (entry_idx >= 0) {
            const entry = &hash_table.entries[@intCast(entry_idx)];
            if (entry.hash == hash) {
                // Found existing group
                group_ids[i] = entry.group_id;
                counts.items[entry.group_id] += 1;
                found = true;
                break;
            }
            entry_idx = entry.next;
        }

        if (!found) {
            // Create new group
            const group_id = hash_table.num_entries;

            // Grow entries array if needed
            if (hash_table.num_entries >= hash_table.entries.len) {
                const new_size = hash_table.entries.len * 2;
                hash_table.entries = try hash_table.allocator.realloc(hash_table.entries, new_size);
            }

            // Add new entry at front of chain
            const new_entry_idx: i32 = @intCast(hash_table.num_entries);
            hash_table.entries[@intCast(new_entry_idx)] = HashEntry{
                .hash = hash,
                .group_id = group_id,
                .next = hash_table.buckets[@intCast(bucket_idx)],
            };
            hash_table.buckets[@intCast(bucket_idx)] = new_entry_idx;
            hash_table.num_entries += 1;

            // Track first row and initialize count
            try first_rows.append(allocator, @intCast(i));
            try counts.append(allocator, 1);

            group_ids[i] = group_id;
        }
    }

    const num_groups = hash_table.num_entries;

    // Copy to owned slices
    const first_row_idx = try allocator.alloc(u32, num_groups);
    @memcpy(first_row_idx, first_rows.items[0..num_groups]);

    const group_counts = try allocator.alloc(u32, num_groups);
    @memcpy(group_counts, counts.items[0..num_groups]);

    return GroupByResultExt{
        .group_ids = group_ids,
        .num_groups = num_groups,
        .first_row_idx = first_row_idx,
        .group_counts = group_counts,
        .allocator = allocator,
    };
}

/// Compute group IDs with key verification (handles hash collisions)
/// Uses actual key values to verify matches, not just hashes
pub fn computeGroupIdsWithKeys(
    allocator: Allocator,
    hashes: []const u64,
    key_data: []const i64, // Single i64 key column
) !GroupByResult {
    const n = hashes.len;
    if (n == 0) {
        return GroupByResult{
            .group_ids = try allocator.alloc(u32, 0),
            .num_groups = 0,
            .allocator = allocator,
        };
    }

    // Use a more sophisticated hash table that stores first row index per group
    // for key comparison
    const estimated_groups = @max(n / 10, 1024);
    const table_size = nextPowerOf2(@intCast(estimated_groups * 2));

    // Bucket array: index into first_rows, -1 = empty
    const buckets = try allocator.alloc(i32, table_size);
    defer allocator.free(buckets);
    @memset(buckets, -1);

    // For each group: store hash, first row index, next pointer
    var group_hashes = try std.ArrayList(u64).initCapacity(allocator, estimated_groups);
    defer group_hashes.deinit(allocator);
    var group_first_rows = try std.ArrayList(u32).initCapacity(allocator, estimated_groups);
    defer group_first_rows.deinit(allocator);
    var group_nexts = try std.ArrayList(i32).initCapacity(allocator, estimated_groups);
    defer group_nexts.deinit(allocator);

    // Allocate output
    const group_ids = try allocator.alloc(u32, n);

    var num_groups: u32 = 0;

    // Process each row
    for (0..n) |i| {
        const hash = hashes[i];
        const key = key_data[i];
        const bucket_idx = hash & (table_size - 1);

        var found = false;
        var entry_idx = buckets[@intCast(bucket_idx)];

        // Search chain for matching key
        while (entry_idx >= 0) {
            const g_hash = group_hashes.items[@intCast(entry_idx)];
            if (g_hash == hash) {
                // Hash match - verify actual key
                const first_row = group_first_rows.items[@intCast(entry_idx)];
                if (key_data[first_row] == key) {
                    // Key match - assign to this group
                    group_ids[i] = @intCast(entry_idx);
                    found = true;
                    break;
                }
            }
            entry_idx = group_nexts.items[@intCast(entry_idx)];
        }

        if (!found) {
            // Create new group
            const group_id = num_groups;
            num_groups += 1;

            try group_hashes.append(allocator, hash);
            try group_first_rows.append(allocator, @intCast(i));
            try group_nexts.append(allocator, buckets[@intCast(bucket_idx)]);

            buckets[@intCast(bucket_idx)] = @intCast(group_id);
            group_ids[i] = group_id;
        }
    }

    return GroupByResult{
        .group_ids = group_ids,
        .num_groups = num_groups,
        .allocator = allocator,
    };
}

/// Parallel version using multiple threads
/// Partitions work across threads, each building local hash tables,
/// then merges results
pub fn computeGroupIdsParallel(
    allocator: Allocator,
    hashes: []const u64,
    num_threads: u32,
) !GroupByResult {
    // For simplicity, use sequential for now
    // Full parallel implementation would:
    // 1. Partition hashes into thread-local chunks
    // 2. Each thread builds local hash table
    // 3. Merge local tables into global
    // 4. Re-scan to assign final group IDs

    // TODO: Implement true parallel groupby
    _ = num_threads;
    return computeGroupIds(allocator, hashes);
}

// ============================================================================
// Aggregation with Group IDs
// ============================================================================

/// Sum values by group (in-place, more efficient than Go version)
pub fn sumByGroup(comptime T: type, data: []const T, group_ids: []const u32, out: []T) void {
    // Zero output
    @memset(out, 0);

    // Accumulate
    for (data, group_ids) |val, gid| {
        out[gid] += val;
    }
}

/// Sum f64 values by group using SIMD where possible
pub fn sumF64ByGroupSIMD(data: []const f64, group_ids: []const u32, out: []f64) void {
    // Zero output
    @memset(out, 0);

    // For now, use scalar accumulation
    // SIMD optimization would require sorted group_ids or histogram approach
    for (data, group_ids) |val, gid| {
        out[gid] += val;
    }
}

/// Count rows per group
pub fn countByGroup(group_ids: []const u32, out: []u64) void {
    @memset(out, 0);
    for (group_ids) |gid| {
        out[gid] += 1;
    }
}

/// Min by group
pub fn minByGroup(comptime T: type, data: []const T, group_ids: []const u32, out: []T) void {
    // Initialize with max value
    const max_val = if (@typeInfo(T) == .float) std.math.inf(T) else std.math.maxInt(T);
    @memset(out, max_val);

    for (data, group_ids) |val, gid| {
        if (val < out[gid]) {
            out[gid] = val;
        }
    }
}

/// Max by group
pub fn maxByGroup(comptime T: type, data: []const T, group_ids: []const u32, out: []T) void {
    // Initialize with min value
    const min_val = if (@typeInfo(T) == .float) -std.math.inf(T) else std.math.minInt(T);
    @memset(out, min_val);

    for (data, group_ids) |val, gid| {
        if (val > out[gid]) {
            out[gid] = val;
        }
    }
}

// ============================================================================
// End-to-End GroupBy Operations (Single CGO Call)
// ============================================================================

/// Result of end-to-end groupby sum operation
pub const GroupBySumResult = struct {
    /// Unique key values (one per group)
    keys: []i64,
    /// Sum values for each group
    sums: []f64,
    /// Number of groups
    num_groups: u32,
    /// Allocator for cleanup
    allocator: Allocator,

    pub fn deinit(self: *GroupBySumResult) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.sums);
    }
};

// ============================================================================
// Multi-threaded GroupBy with Radix Partitioning
// ============================================================================
// Strategy:
// 1. Hash all keys, partition by hash bits (64 partitions)
// 2. Scatter row indices to partition buffers
// 3. Process partitions in parallel - each partition fits in L2 cache
// 4. Concatenate results from all partitions

// Hash constants for inline hashing
const RAPID_SECRET0: u64 = 0x2d358dccaa6c78a5;
const RAPID_SECRET1: u64 = 0x8bb84b93962eacc9;
const RAPID_SECRET2: u64 = 0x4b33a62ed433d4a3;

inline fn rapidMix(a: u64, b: u64) u64 {
    const result = @as(u128, a) *% @as(u128, b);
    return @truncate(result ^ (result >> 64));
}

inline fn rapidHash64(val: u64) u64 {
    const a = val ^ RAPID_SECRET0;
    const b = val ^ RAPID_SECRET1;
    return rapidMix(a, b) ^ RAPID_SECRET2;
}

// Threading configuration (uses simd.zig's thread config for consistency)
const MAX_THREADS: usize = 32;
const NUM_PARTITIONS: usize = 64; // 6 bits of hash
const PARTITION_BITS: u6 = 6;
const PARTITION_MASK: u64 = NUM_PARTITIONS - 1;

/// Extract partition ID from hash (use middle bits for better distribution)
inline fn getPartition(hash: u64) usize {
    return @intCast((hash >> 20) & PARTITION_MASK);
}

/// Result from processing a single partition
const PartitionResult = struct {
    keys: []i64,
    sums: []f64,
    num_groups: u32,
};

/// Context for partition worker thread
const PartitionWorkerContext = struct {
    // Input data
    key_data: []const i64,
    value_data: []const f64,
    hashes: []const u64,
    // Partition assignments
    partition_indices: []const u32,
    partition_offsets: []const u32,
    // Which partitions this thread processes
    start_partition: usize,
    end_partition: usize,
    // Output (allocated by worker)
    results: []PartitionResult,
    allocator: Allocator,
    // Error flag
    had_error: bool,
};

/// Worker function for processing partitions - uses thread-local arena
fn partitionWorker(ctx: *PartitionWorkerContext) void {
    // Create a thread-local arena for this worker
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    for (ctx.start_partition..ctx.end_partition) |p| {
        const start = ctx.partition_offsets[p];
        const end = ctx.partition_offsets[p + 1];
        const count = end - start;

        if (count == 0) {
            ctx.results[p] = .{
                .keys = &[_]i64{},
                .sums = &[_]f64{},
                .num_groups = 0,
            };
            continue;
        }

        // Estimate groups for this partition
        const estimated_groups = @max(count / 10, 64);

        // Create local hash table using arena
        var table = SIMDHashTableSum.init(arena, estimated_groups) catch {
            ctx.had_error = true;
            return;
        };

        // Process all rows in this partition
        const indices = ctx.partition_indices[start..end];
        for (indices) |row_idx| {
            const hash = ctx.hashes[row_idx];
            const key = ctx.key_data[row_idx];
            const value = ctx.value_data[row_idx];

            const entry = table.insertOrUpdate(hash, key) catch {
                ctx.had_error = true;
                return;
            };
            entry.sum += value;
        }

        // Extract results - allocate from shared allocator for final output
        const num_groups = table.count;
        const keys = ctx.allocator.alloc(i64, num_groups) catch {
            ctx.had_error = true;
            return;
        };
        const sums = ctx.allocator.alloc(f64, num_groups) catch {
            ctx.allocator.free(keys);
            ctx.had_error = true;
            return;
        };

        var group_idx: u32 = 0;
        const table_len = table.entries.len;
        for (table.ctrl[0..table_len], table.entries) |ctrl_byte, entry| {
            if (ctrl_byte != CTRL_EMPTY) {
                keys[group_idx] = entry.key; // Key stored directly in entry
                sums[group_idx] = entry.sum;
                group_idx += 1;
            }
        }

        // Table cleanup handled by arena

        ctx.results[p] = .{
            .keys = keys,
            .sums = sums,
            .num_groups = num_groups,
        };
    }
}

/// Context for chunk-parallel groupby
const ChunkWorkerContext = struct {
    key_data: []const i64,
    value_data: []const f64,
    start_row: usize,
    end_row: usize,
    // Output: local hash table results
    keys: []i64,
    sums: []f64,
    num_groups: u32,
    allocator: Allocator,
    had_error: bool,
};

/// Chunk worker - processes a range of rows with local hash table
fn chunkWorkerSum(ctx: *ChunkWorkerContext) void {
    const start = ctx.start_row;
    const end = ctx.end_row;
    const count = end - start;

    if (count == 0) {
        ctx.keys = &[_]i64{};
        ctx.sums = &[_]f64{};
        ctx.num_groups = 0;
        return;
    }

    // Thread-local arena
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const estimated_groups = @max(count / 10, 1024);
    var table = SIMDHashTableSum.init(arena, estimated_groups) catch {
        ctx.had_error = true;
        return;
    };

    // Process rows in this chunk
    for (start..end) |i| {
        const key = ctx.key_data[i];
        const hash = rapidHash64(@bitCast(key));
        const entry = table.insertOrUpdate(hash, key) catch {
            ctx.had_error = true;
            return;
        };
        entry.sum += ctx.value_data[i];
    }

    // Extract results
    const num_groups = table.count;
    const keys = ctx.allocator.alloc(i64, num_groups) catch {
        ctx.had_error = true;
        return;
    };
    const sums = ctx.allocator.alloc(f64, num_groups) catch {
        ctx.allocator.free(keys);
        ctx.had_error = true;
        return;
    };

    var group_idx: u32 = 0;
    const table_len = table.entries.len;
    for (table.ctrl[0..table_len], table.entries) |ctrl_byte, entry| {
        if (ctrl_byte != CTRL_EMPTY) {
            keys[group_idx] = entry.key; // Key stored directly in entry
            sums[group_idx] = entry.sum;
            group_idx += 1;
        }
    }

    ctx.keys = keys;
    ctx.sums = sums;
    ctx.num_groups = num_groups;
}

pub fn groupbySumI64KeyF64Value(
    allocator: Allocator,
    key_data: []const i64,
    value_data: []const f64,
) !GroupBySumResult {
    const n = key_data.len;
    if (n == 0) {
        return GroupBySumResult{
            .keys = try allocator.alloc(i64, 0),
            .sums = try allocator.alloc(f64, 0),
            .num_groups = 0,
            .allocator = allocator,
        };
    }

    // Use single-threaded for all sizes - multi-threading overhead too high
    // The radix partition approach has too much overhead (hash, count, scatter)
    // The chunk-merge approach has too much merge overhead
    // Single-threaded with optimized Swiss table is actually competitive
    return groupbySumSingleThread(allocator, key_data, value_data);
}

/// Single-threaded groupby for small inputs
/// Uses direct key storage - no memory indirection for comparison
fn groupbySumSingleThread(
    allocator: Allocator,
    key_data: []const i64,
    value_data: []const f64,
) !GroupBySumResult {
    const n = key_data.len;
    const estimated_groups = @max(n / 10, 1024);
    var table = try SIMDHashTableSum.init(allocator, estimated_groups);
    defer table.deinit();

    for (0..n) |i| {
        const key = key_data[i];
        const hash = rapidHash64(@bitCast(key));
        const entry = try table.insertOrUpdate(hash, key);
        entry.sum += value_data[i];
    }

    const num_groups = table.count;
    const keys = try allocator.alloc(i64, num_groups);
    const sums = try allocator.alloc(f64, num_groups);
    const table_len = table.entries.len;

    var group_idx: u32 = 0;
    for (table.ctrl[0..table_len], table.entries) |ctrl_byte, entry| {
        if (ctrl_byte != CTRL_EMPTY) {
            keys[group_idx] = entry.key; // Key stored directly in entry
            sums[group_idx] = entry.sum;
            group_idx += 1;
        }
    }

    return GroupBySumResult{
        .keys = keys,
        .sums = sums,
        .num_groups = num_groups,
        .allocator = allocator,
    };
}

/// End-to-end groupby with multiple aggregations (sum, min, max, count)
pub const GroupByMultiAggResult = struct {
    /// Unique key values (one per group)
    keys: []i64,
    /// Sum values for each group
    sums: []f64,
    /// Min values for each group
    mins: []f64,
    /// Max values for each group
    maxs: []f64,
    /// Count per group
    counts: []u64,
    /// Number of groups
    num_groups: u32,
    /// Allocator for cleanup
    allocator: Allocator,

    pub fn deinit(self: *GroupByMultiAggResult) void {
        self.allocator.free(self.keys);
        self.allocator.free(self.sums);
        self.allocator.free(self.mins);
        self.allocator.free(self.maxs);
        self.allocator.free(self.counts);
    }
};

/// Result from processing a single partition (multi-agg)
const PartitionResultMultiAgg = struct {
    keys: []i64,
    sums: []f64,
    mins: []f64,
    maxs: []f64,
    counts: []u64,
    num_groups: u32,
};

/// Context for multi-agg partition worker thread
const PartitionWorkerContextMultiAgg = struct {
    key_data: []const i64,
    value_data: []const f64,
    hashes: []const u64,
    partition_indices: []const u32,
    partition_offsets: []const u32,
    start_partition: usize,
    end_partition: usize,
    results: []PartitionResultMultiAgg,
    allocator: Allocator,
    had_error: bool,
};

/// Worker function for multi-agg partition processing - uses thread-local arena
fn partitionWorkerMultiAgg(ctx: *PartitionWorkerContextMultiAgg) void {
    // Thread-local arena for temporary allocations
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    for (ctx.start_partition..ctx.end_partition) |p| {
        const start = ctx.partition_offsets[p];
        const end = ctx.partition_offsets[p + 1];
        const count = end - start;

        if (count == 0) {
            ctx.results[p] = .{
                .keys = &[_]i64{},
                .sums = &[_]f64{},
                .mins = &[_]f64{},
                .maxs = &[_]f64{},
                .counts = &[_]u64{},
                .num_groups = 0,
            };
            continue;
        }

        const estimated_groups = @max(count / 10, 64);
        var table = SIMDHashTableMultiAgg.init(arena, estimated_groups) catch {
            ctx.had_error = true;
            return;
        };

        const indices = ctx.partition_indices[start..end];
        for (indices) |row_idx| {
            const hash = ctx.hashes[row_idx];
            const key = ctx.key_data[row_idx];
            const value = ctx.value_data[row_idx];

            const entry = table.insertOrUpdate(hash, key) catch {
                ctx.had_error = true;
                return;
            };
            entry.sum += value;
            entry.min = @min(entry.min, value);
            entry.max = @max(entry.max, value);
            entry.count += 1;
        }

        // Extract results to shared allocator
        const num_groups = table.count;
        const keys = ctx.allocator.alloc(i64, num_groups) catch {
            ctx.had_error = true;
            return;
        };
        const sums = ctx.allocator.alloc(f64, num_groups) catch {
            ctx.allocator.free(keys);
            ctx.had_error = true;
            return;
        };
        const mins = ctx.allocator.alloc(f64, num_groups) catch {
            ctx.allocator.free(keys);
            ctx.allocator.free(sums);
            ctx.had_error = true;
            return;
        };
        const maxs = ctx.allocator.alloc(f64, num_groups) catch {
            ctx.allocator.free(keys);
            ctx.allocator.free(sums);
            ctx.allocator.free(mins);
            ctx.had_error = true;
            return;
        };
        const cnts = ctx.allocator.alloc(u64, num_groups) catch {
            ctx.allocator.free(keys);
            ctx.allocator.free(sums);
            ctx.allocator.free(mins);
            ctx.allocator.free(maxs);
            ctx.had_error = true;
            return;
        };

        var group_idx: u32 = 0;
        const table_len = table.entries.len;
        for (table.ctrl[0..table_len], table.entries) |ctrl_byte, entry| {
            if (ctrl_byte != CTRL_EMPTY) {
                keys[group_idx] = entry.key; // Key stored directly in entry
                sums[group_idx] = entry.sum;
                mins[group_idx] = entry.min;
                maxs[group_idx] = entry.max;
                cnts[group_idx] = entry.count;
                group_idx += 1;
            }
        }

        // Table cleanup handled by arena

        ctx.results[p] = .{
            .keys = keys,
            .sums = sums,
            .mins = mins,
            .maxs = maxs,
            .counts = cnts,
            .num_groups = num_groups,
        };
    }
}

/// End-to-end groupby with sum, min, max, count
pub fn groupbyMultiAggI64KeyF64Value(
    allocator: Allocator,
    key_data: []const i64,
    value_data: []const f64,
) !GroupByMultiAggResult {
    const n = key_data.len;
    if (n == 0) {
        return GroupByMultiAggResult{
            .keys = try allocator.alloc(i64, 0),
            .sums = try allocator.alloc(f64, 0),
            .mins = try allocator.alloc(f64, 0),
            .maxs = try allocator.alloc(f64, 0),
            .counts = try allocator.alloc(u64, 0),
            .num_groups = 0,
            .allocator = allocator,
        };
    }

    // Use optimized single-threaded implementation
    return groupbyMultiAggSingleThread(allocator, key_data, value_data);
}

/// Single-threaded multi-agg for small inputs
/// Uses direct key storage - no memory indirection for comparison
fn groupbyMultiAggSingleThread(
    allocator: Allocator,
    key_data: []const i64,
    value_data: []const f64,
) !GroupByMultiAggResult {
    const n = key_data.len;
    const estimated_groups = @max(n / 10, 1024);
    var table = try SIMDHashTableMultiAgg.init(allocator, estimated_groups);
    defer table.deinit();

    for (0..n) |i| {
        const key = key_data[i];
        const hash = rapidHash64(@bitCast(key));
        const entry = try table.insertOrUpdate(hash, key);
        const v = value_data[i];
        entry.sum += v;
        entry.min = @min(entry.min, v);
        entry.max = @max(entry.max, v);
        entry.count += 1;
    }

    const num_groups = table.count;
    const keys = try allocator.alloc(i64, num_groups);
    const sums = try allocator.alloc(f64, num_groups);
    const mins = try allocator.alloc(f64, num_groups);
    const maxs = try allocator.alloc(f64, num_groups);
    const counts = try allocator.alloc(u64, num_groups);
    const table_len = table.entries.len;

    var group_idx: u32 = 0;
    for (table.ctrl[0..table_len], table.entries) |ctrl_byte, entry| {
        if (ctrl_byte != CTRL_EMPTY) {
            keys[group_idx] = entry.key; // Key stored directly in entry
            sums[group_idx] = entry.sum;
            mins[group_idx] = entry.min;
            maxs[group_idx] = entry.max;
            counts[group_idx] = entry.count;
            group_idx += 1;
        }
    }

    return GroupByMultiAggResult{
        .keys = keys,
        .sums = sums,
        .mins = mins,
        .maxs = maxs,
        .counts = counts,
        .num_groups = num_groups,
        .allocator = allocator,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "groupby basic" {
    const allocator = std.testing.allocator;

    // Test data: some repeated hashes
    const hashes = [_]u64{ 100, 200, 100, 300, 200, 100 };

    var result = try computeGroupIds(allocator, &hashes);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 3), result.num_groups);

    // Rows with same hash should have same group ID
    try std.testing.expectEqual(result.group_ids[0], result.group_ids[2]);
    try std.testing.expectEqual(result.group_ids[0], result.group_ids[5]);
    try std.testing.expectEqual(result.group_ids[1], result.group_ids[4]);
}

test "groupby with keys" {
    const allocator = std.testing.allocator;

    const hashes = [_]u64{ 100, 200, 100, 300, 200, 100 };
    const keys = [_]i64{ 1, 2, 1, 3, 2, 1 };

    var result = try computeGroupIdsWithKeys(allocator, &hashes, &keys);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 3), result.num_groups);
}

test "sum by group" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const group_ids = [_]u32{ 0, 1, 0, 2, 1, 0 };
    var out: [3]f64 = undefined;

    sumByGroup(f64, &data, &group_ids, &out);

    try std.testing.expectApproxEqAbs(@as(f64, 10.0), out[0], 0.001); // 1+3+6
    try std.testing.expectApproxEqAbs(@as(f64, 7.0), out[1], 0.001);  // 2+5
    try std.testing.expectApproxEqAbs(@as(f64, 4.0), out[2], 0.001);  // 4
}
