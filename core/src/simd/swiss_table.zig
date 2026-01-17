// Swiss Table implementation for high-performance joins
// Based on Google's SwissTable / Rust's hashbrown design
//
// Key characteristics:
// - SIMD-accelerated control byte probing (16 at a time)
// - H2 hash (7 bits) stored in control byte for fast filtering
// - Triangular probing between groups
// - Optimized for i64 keys with vector of i32 indices as values

const std = @import("std");
const builtin = @import("builtin");

// Control byte values
const CTRL_EMPTY: u8 = 0xFF;
const CTRL_DELETED: u8 = 0x80;

// Group size for SIMD operations - 16 bytes = 128-bit SSE register
const GROUP_SIZE: usize = 16;

// SIMD types for group operations
const Group = @Vector(GROUP_SIZE, u8);
const BoolVec = @Vector(GROUP_SIZE, bool);
const Mask = u16; // Bitmask for 16 elements

/// Extract H2 hash (top 7 bits of hash, with high bit cleared)
inline fn h2(hash: u64) u8 {
    return @truncate((hash >> 57) & 0x7F);
}

/// SIMD: Load 16 control bytes as a vector
inline fn loadGroup(ctrl: [*]const u8) Group {
    return ctrl[0..GROUP_SIZE].*;
}

/// SIMD: Find all slots matching the h2 fingerprint (returns bitmask)
inline fn matchH2(group: Group, h2_val: u8) Mask {
    const cmp: BoolVec = group == @as(Group, @splat(h2_val));
    return @bitCast(@as(@Vector(GROUP_SIZE, u1), @intFromBool(cmp)));
}

/// SIMD: Find all empty slots (returns bitmask)
inline fn matchEmpty(group: Group) Mask {
    const cmp: BoolVec = group == @as(Group, @splat(CTRL_EMPTY));
    return @bitCast(@as(@Vector(GROUP_SIZE, u1), @intFromBool(cmp)));
}

/// SIMD: Find all empty or deleted slots (returns bitmask)
inline fn matchEmptyOrDeleted(group: Group) Mask {
    // Empty = 0xFF, Deleted = 0x80, both have high bit set
    const cmp: BoolVec = group >= @as(Group, @splat(CTRL_DELETED));
    return @bitCast(@as(@Vector(GROUP_SIZE, u1), @intFromBool(cmp)));
}

/// Small inline vector for storing row indices (optimized for 1-4 matches)
pub const IdxVec = struct {
    // Inline storage for small vectors (common case)
    inline_buf: [4]i32 = undefined,
    inline_len: u8 = 0,
    // Overflow to heap for large vectors
    overflow_ptr: ?[*]i32 = null,
    overflow_cap: u32 = 0,

    pub fn init() IdxVec {
        return .{};
    }

    pub fn push(self: *IdxVec, allocator: std.mem.Allocator, val: i32) !void {
        const total_len = self.len();

        if (total_len < 4) {
            // Store in inline buffer
            self.inline_buf[self.inline_len] = val;
            self.inline_len += 1;
        } else if (self.overflow_ptr == null) {
            // First overflow - allocate and copy inline
            const new_cap: u32 = 8;
            const ptr = try allocator.alloc(i32, new_cap);
            @memcpy(ptr[0..4], &self.inline_buf);
            ptr[4] = val;
            self.overflow_ptr = ptr.ptr;
            self.overflow_cap = new_cap;
            self.inline_len = 5; // Total count stored in inline_len when using overflow
        } else {
            // Append to overflow
            const current_len = self.inline_len;
            if (current_len >= self.overflow_cap) {
                // Grow
                const new_cap = self.overflow_cap * 2;
                const old_slice = self.overflow_ptr.?[0..self.overflow_cap];
                const new_ptr = try allocator.realloc(old_slice, new_cap);
                self.overflow_ptr = new_ptr.ptr;
                self.overflow_cap = new_cap;
            }
            self.overflow_ptr.?[current_len] = val;
            self.inline_len += 1;
        }
    }

    pub fn len(self: *const IdxVec) usize {
        return self.inline_len;
    }

    pub fn items(self: *const IdxVec) []const i32 {
        if (self.overflow_ptr) |ptr| {
            return ptr[0..self.inline_len];
        }
        return self.inline_buf[0..self.inline_len];
    }

    pub fn deinit(self: *IdxVec, allocator: std.mem.Allocator) void {
        if (self.overflow_ptr) |ptr| {
            allocator.free(ptr[0..self.overflow_cap]);
        }
    }
};

/// Swiss Table for join operations
/// Maps i64 keys to vectors of i32 row indices
pub const JoinSwissTable = struct {
    ctrl: []u8, // Control bytes
    keys: []i64, // Keys
    values: []IdxVec, // Values (vectors of indices)
    size: usize, // Number of entries
    capacity: usize, // Total slots
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initialize with estimated capacity
    pub fn init(allocator: std.mem.Allocator, estimated_entries: usize) !Self {
        // Swiss tables work best at ~87.5% load factor
        // Use power of 2 for efficient modulo
        var capacity = estimated_entries * 8 / 7; // ~87.5% load factor
        capacity = std.math.ceilPowerOfTwo(usize, capacity) catch capacity;
        capacity = @max(capacity, GROUP_SIZE); // Minimum one group

        const ctrl = try allocator.alloc(u8, capacity + GROUP_SIZE);
        @memset(ctrl, CTRL_EMPTY);

        const keys = try allocator.alloc(i64, capacity);
        const values = try allocator.alloc(IdxVec, capacity);

        // Initialize values to empty
        for (values) |*v| {
            v.* = IdxVec.init();
        }

        return Self{
            .ctrl = ctrl,
            .keys = keys,
            .values = values,
            .size = 0,
            .capacity = capacity,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free all IdxVec overflow allocations
        for (self.values) |*v| {
            v.deinit(self.allocator);
        }
        self.allocator.free(self.ctrl);
        self.allocator.free(self.keys);
        self.allocator.free(self.values);
    }

    /// Hash function for i64 keys - wyhash-inspired, faster than splitmix
    /// Single multiply + xor is sufficient for hash tables
    inline fn hash(key: i64) u64 {
        // wyhash-style: single multiply with good constant, then fold
        const k: u64 = @bitCast(key);
        // Use golden ratio prime for better mixing
        const h = k *% 0x9E3779B97F4A7C15;
        return h ^ (h >> 32);
    }

    /// Find slot for key using SIMD, returns (slot_index, found)
    fn findSlot(self: *const Self, key: i64, key_hash: u64) struct { usize, bool } {
        const h2_val = h2(key_hash);
        const cap_mask = self.capacity - 1;
        var pos = @as(usize, @truncate(key_hash)) & cap_mask;

        // Triangular probing through groups
        var probe_seq: usize = 0;
        while (probe_seq <= self.capacity / GROUP_SIZE) : (probe_seq += 1) {
            // Align to group boundary
            const group_start = pos & ~@as(usize, GROUP_SIZE - 1);

            // SIMD: Load 16 control bytes and find all h2 matches
            const group = loadGroup(self.ctrl.ptr + group_start);
            var match_mask = matchH2(group, h2_val);

            // Check all potential matches in this group
            while (match_mask != 0) {
                // Get first match using count trailing zeros
                const match_offset = @ctz(match_mask);
                const slot = (group_start + match_offset) & cap_mask;

                // Verify actual key match
                if (self.keys[slot] == key) {
                    return .{ slot, true };
                }

                // Clear this bit and continue
                match_mask &= match_mask - 1;
            }

            // SIMD: Check if there's an empty slot (means key definitely not found)
            if (matchEmpty(group) != 0) {
                // Find first empty slot for potential insertion
                const empty_mask = matchEmpty(group);
                const empty_offset = @ctz(empty_mask);
                return .{ (group_start + empty_offset) & cap_mask, false };
            }

            // Move to next group (triangular probing)
            pos = (pos + probe_seq + 1) & cap_mask;
        }

        // Should never reach here with proper load factor
        return .{ 0, false };
    }

    /// Find empty slot for insertion (doesn't check for existing key)
    fn findEmptySlot(self: *const Self, key_hash: u64) usize {
        const cap_mask = self.capacity - 1;
        var pos = @as(usize, @truncate(key_hash)) & cap_mask;

        var probe_seq: usize = 0;
        while (true) : (probe_seq += 1) {
            const group_start = pos & ~@as(usize, GROUP_SIZE - 1);
            const group = loadGroup(self.ctrl.ptr + group_start);
            const empty_deleted_mask = matchEmptyOrDeleted(group);

            if (empty_deleted_mask != 0) {
                const offset = @ctz(empty_deleted_mask);
                return (group_start + offset) & cap_mask;
            }

            pos = (pos + probe_seq + 1) & cap_mask;
        }
    }

    /// Insert or update: adds row_idx to the vector for key
    pub fn insertOrAppend(self: *Self, key: i64, row_idx: i32) !void {
        const key_hash = hash(key);
        const result = self.findSlot(key, key_hash);
        const slot = result[0];
        const found = result[1];

        if (found) {
            // Key exists - append to vector
            try self.values[slot].push(self.allocator, row_idx);
        } else {
            // New key - insert
            self.ctrl[slot] = h2(key_hash);
            self.keys[slot] = key;
            self.values[slot] = IdxVec.init();
            try self.values[slot].push(self.allocator, row_idx);
            self.size += 1;

            // Mirror control byte for wrap-around SIMD reads
            if (slot < GROUP_SIZE) {
                self.ctrl[self.capacity + slot] = self.ctrl[slot];
            }
        }
    }

    /// Get all indices for a key, or null if not found
    pub fn get(self: *const Self, key: i64) ?[]const i32 {
        const key_hash = hash(key);
        const result = self.findSlot(key, key_hash);

        if (result[1]) {
            return self.values[result[0]].items();
        }
        return null;
    }

    /// Get with aggressive prefetch - prefetches ctrl, keys, and values for next lookup
    pub fn getWithPrefetch(self: *const Self, key: i64, next_key: i64) ?[]const i32 {
        // Prefetch for next key - ctrl bytes, keys, and values
        const next_hash = hash(next_key);
        const cap_mask = self.capacity - 1;
        const next_pos = @as(usize, @truncate(next_hash)) & cap_mask;
        const next_group = next_pos & ~@as(usize, GROUP_SIZE - 1);

        // Prefetch control bytes (most important)
        @prefetch(self.ctrl.ptr + next_group, .{ .rw = .read, .locality = 3, .cache = .data });
        // Prefetch keys at likely slot
        @prefetch(@as([*]const u8, @ptrCast(self.keys.ptr + next_pos)), .{ .rw = .read, .locality = 2, .cache = .data });

        // Do current lookup
        return self.get(key);
    }

    /// Fast probe-only lookup - inlined and optimized for the common case
    /// Uses probe-specific optimizations (no need to find empty slots for insertion)
    pub inline fn probe(self: *const Self, key: i64) ?[]const i32 {
        const key_hash = hash(key);
        const h2_val = h2(key_hash);
        const cap_mask = self.capacity - 1;
        var pos = @as(usize, @truncate(key_hash)) & cap_mask;

        // First group - most lookups hit here
        const group_start = pos & ~@as(usize, GROUP_SIZE - 1);
        const group = loadGroup(self.ctrl.ptr + group_start);
        var match_mask = matchH2(group, h2_val);

        // Fast path: check matches in first group
        while (match_mask != 0) {
            const match_offset = @ctz(match_mask);
            const slot = (group_start + match_offset) & cap_mask;
            if (self.keys[slot] == key) {
                return self.values[slot].items();
            }
            match_mask &= match_mask - 1;
        }

        // If empty slot found, key doesn't exist
        if (matchEmpty(group) != 0) {
            return null;
        }

        // Rare: need to probe further groups
        var probe_seq: usize = 1;
        while (probe_seq <= self.capacity / GROUP_SIZE) : (probe_seq += 1) {
            pos = (pos + probe_seq) & cap_mask;
            const gs = pos & ~@as(usize, GROUP_SIZE - 1);
            const g = loadGroup(self.ctrl.ptr + gs);
            var mm = matchH2(g, h2_val);

            while (mm != 0) {
                const mo = @ctz(mm);
                const s = (gs + mo) & cap_mask;
                if (self.keys[s] == key) {
                    return self.values[s].items();
                }
                mm &= mm - 1;
            }

            if (matchEmpty(g) != 0) {
                return null;
            }
        }
        return null;
    }

    /// Batch lookup for 4 keys at once with software pipelining
    pub fn getBatch(
        self: *const Self,
        keys_in: [4]i64,
        results: *[4]?[]const i32,
    ) void {
        const cap_mask = self.capacity - 1;

        // Compute all 4 hashes upfront
        const hashes: [4]u64 = .{
            hash(keys_in[0]),
            hash(keys_in[1]),
            hash(keys_in[2]),
            hash(keys_in[3]),
        };

        // Prefetch all 4 group locations + keys
        inline for (0..4) |i| {
            const pos = @as(usize, @truncate(hashes[i])) & cap_mask;
            const group_start = pos & ~@as(usize, GROUP_SIZE - 1);
            @prefetch(self.ctrl.ptr + group_start, .{ .rw = .read, .locality = 3, .cache = .data });
            @prefetch(@as([*]const u8, @ptrCast(self.keys.ptr + pos)), .{ .rw = .read, .locality = 2, .cache = .data });
        }

        // Now do all 4 lookups using optimized probe
        inline for (0..4) |i| {
            results[i] = self.probeWithHash(keys_in[i], hashes[i]);
        }
    }

    /// Probe with pre-computed hash
    inline fn probeWithHash(self: *const Self, key: i64, key_hash: u64) ?[]const i32 {
        const h2_val = h2(key_hash);
        const cap_mask = self.capacity - 1;
        var pos = @as(usize, @truncate(key_hash)) & cap_mask;

        var probe_seq: usize = 0;
        while (probe_seq <= self.capacity / GROUP_SIZE) : (probe_seq += 1) {
            const group_start = pos & ~@as(usize, GROUP_SIZE - 1);
            const group = loadGroup(self.ctrl.ptr + group_start);
            var match_mask = matchH2(group, h2_val);

            while (match_mask != 0) {
                const match_offset = @ctz(match_mask);
                const slot = (group_start + match_offset) & cap_mask;
                if (self.keys[slot] == key) {
                    return self.values[slot].items();
                }
                match_mask &= match_mask - 1;
            }

            if (matchEmpty(group) != 0) {
                return null;
            }
            pos = (pos + probe_seq + 1) & cap_mask;
        }
        return null;
    }

    /// Iterator over all entries in the table
    pub fn iterator(self: *const Self) Iterator {
        return Iterator{
            .table = self,
            .group_idx = 0,
            .mask = 0,
        };
    }

    pub const Iterator = struct {
        table: *const JoinSwissTable,
        group_idx: usize,
        mask: Mask,

        pub fn next(self: *Iterator) ?struct { key: i64, values: []const i32 } {
            while (self.mask == 0) {
                if (self.group_idx >= self.table.capacity) return null;

                const group = loadGroup(self.table.ctrl.ptr + self.group_idx);
                // Match used slots (control byte < 0x80)
                const cmp: BoolVec = group < @as(Group, @splat(CTRL_DELETED));
                self.mask = @bitCast(@as(@Vector(GROUP_SIZE, u1), @intFromBool(cmp)));
                self.group_idx += GROUP_SIZE;
            }

            const offset = @ctz(self.mask);
            const slot = (self.group_idx - GROUP_SIZE) + offset;
            self.mask &= self.mask - 1;

            return .{
                .key = self.table.keys[slot],
                .values = self.table.values[slot].items(),
            };
        }
    };
};

// ============================================================================
// Tests
// ============================================================================

test "swiss_table - basic insert and get" {
    const allocator = std.testing.allocator;

    var table = try JoinSwissTable.init(allocator, 100);
    defer table.deinit();

    // Insert some values
    try table.insertOrAppend(42, 0);
    try table.insertOrAppend(42, 5);
    try table.insertOrAppend(100, 1);
    try table.insertOrAppend(200, 2);

    // Lookup
    const val42 = table.get(42);
    try std.testing.expect(val42 != null);
    try std.testing.expectEqual(@as(usize, 2), val42.?.len);
    try std.testing.expectEqual(@as(i32, 0), val42.?[0]);
    try std.testing.expectEqual(@as(i32, 5), val42.?[1]);

    const val100 = table.get(100);
    try std.testing.expect(val100 != null);
    try std.testing.expectEqual(@as(usize, 1), val100.?.len);

    const val999 = table.get(999);
    try std.testing.expect(val999 == null);
}

test "swiss_table - many duplicates" {
    const allocator = std.testing.allocator;

    var table = try JoinSwissTable.init(allocator, 10);
    defer table.deinit();

    // Insert 10 values for same key
    for (0..10) |i| {
        try table.insertOrAppend(42, @intCast(i));
    }

    const val = table.get(42);
    try std.testing.expect(val != null);
    try std.testing.expectEqual(@as(usize, 10), val.?.len);
}

test "swiss_table - large table" {
    const allocator = std.testing.allocator;

    var table = try JoinSwissTable.init(allocator, 10000);
    defer table.deinit();

    // Insert 10000 unique keys
    for (0..10000) |i| {
        try table.insertOrAppend(@intCast(i), @intCast(i));
    }

    // Verify
    for (0..10000) |i| {
        const val = table.get(@intCast(i));
        try std.testing.expect(val != null);
        try std.testing.expectEqual(@as(usize, 1), val.?.len);
        try std.testing.expectEqual(@as(i32, @intCast(i)), val.?[0]);
    }
}
