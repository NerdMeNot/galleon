//! Swiss Table - High Performance Hash Table
//!
//! A Zig implementation of Google's Swiss Table (abseil) / Rust's hashbrown.
//! Uses SIMD for parallel control byte scanning and triangular probing.
//!
//! Key features:
//! - SIMD-accelerated lookups (16 slots checked per instruction)
//! - 87.5% max load factor (memory efficient)
//! - Cache-friendly memory layout
//! - Triangular probing sequence
//!
//! References:
//! - https://abseil.io/about/design/swisstables
//! - https://faultlore.com/blah/hashbrown-tldr/
//! - CppCon 2017: Matt Kulukundis "Designing a Fast, Efficient, Cache-friendly Hash Table"

const std = @import("std");

// ============================================================================
// Partition Routing
// ============================================================================

/// Route a hash to a partition index.
/// Uses 128-bit multiplication to extract a uniform partition index from the hash.
/// This compiles to a single mul/mulhi instruction on x86-64/aarch64.
///
/// This is the same algorithm as Polars' hash_to_partition().
pub inline fn hashToPartition(h: u64, n_partitions: usize) usize {
    // h / 2^64 is almost uniform in [0, 1)
    // floor(h * n_partitions / 2^64) is almost uniform in [0, n_partitions)
    return @truncate((@as(u128, h) *% @as(u128, n_partitions)) >> 64);
}

/// Fast "dirty" hash for partition routing.
/// Only needs good distribution in top bits (used with hashToPartition).
/// Uses multiplication by a random odd number.
pub inline fn dirtyHash(x: u64) u64 {
    const RANDOM_ODD: u64 = 0x55fbfd6bfc5458e9;
    return x *% RANDOM_ODD;
}

/// Fast integer hash using fibonacci/golden ratio multiply.
/// Single multiply is fastest and sufficient for hash table buckets.
pub inline fn fastIntHash(x: u64) u64 {
    const GOLDEN: u64 = 0x9E3779B97F4A7C15; // 2^64 / phi
    return x *% GOLDEN;
}

// ============================================================================
// Constants
// ============================================================================

/// Number of control bytes per group.
/// On ARM/aarch64, we use 8-byte groups because NEON 128-bit ops have multi-cycle latency.
/// On x86-64 with SSE2, we use 16-byte groups.
/// This matches hashbrown's approach.
pub const GROUP_WIDTH: usize = if (isAarch64()) 8 else 16;

fn isAarch64() bool {
    return @import("builtin").cpu.arch == .aarch64;
}

/// Control byte values (matching hashbrown's encoding)
/// This encoding allows efficient SIMD checks:
/// - match_empty: compare to 0xFF
/// - match_empty_or_deleted: check high bit (signed negative)
pub const EMPTY: u8 = 0b1111_1111; // 0xFF - slot never used
pub const DELETED: u8 = 0b1000_0000; // 0x80 - tombstone
// FULL slots have high bit clear, contain 7-bit H2: 0b0hhh_hhhh

/// Maximum load factor: 7/8 = 87.5%
const MAX_LOAD_FACTOR_NUMERATOR: usize = 7;
const MAX_LOAD_FACTOR_DENOMINATOR: usize = 8;

// ============================================================================
// SIMD Types and Operations
// ============================================================================

/// SIMD vector for control byte operations (GROUP_WIDTH bytes)
const Group = @Vector(GROUP_WIDTH, u8);
const BoolGroup = @Vector(GROUP_WIDTH, bool);

/// Bitmask type: u8 for 8-byte groups (aarch64), u16 for 16-byte groups (x86)
const BitMask = if (GROUP_WIDTH == 8) u8 else u16;
const BitMaskBits = if (GROUP_WIDTH == 8) u3 else u4;

/// Extract H2 from hash (top 7 bits, shifted to low 7)
inline fn h2(hash: u64) u8 {
    // Use top 7 bits to maximize independence from H1
    return @truncate(hash >> 57);
}

/// Compute H1 (used for bucket index)
inline fn h1(hash: u64, bucket_mask: usize) usize {
    return @truncate(hash & bucket_mask);
}

/// Match control bytes against H2, return bitmask of matches
inline fn matchH2(ctrl: Group, h2_val: u8) BitMask {
    const needle: Group = @splat(h2_val);
    const matches: BoolGroup = ctrl == needle;
    return @bitCast(matches);
}

/// Match EMPTY control bytes
inline fn matchEmpty(ctrl: Group) BitMask {
    const empty: Group = @splat(EMPTY);
    const matches: BoolGroup = ctrl == empty;
    return @bitCast(matches);
}

/// Match EMPTY or DELETED control bytes (both have high bit set)
/// With hashbrown encoding: EMPTY=0xFF, DELETED=0x80
/// Both are negative when interpreted as signed bytes.
/// FULL slots (0x00-0x7F) are non-negative as signed.
inline fn matchEmptyOrDeleted(ctrl: Group) BitMask {
    // Check if high bit is set: (ctrl & 0x80) != 0
    // This catches both EMPTY (0xFF) and DELETED (0x80)
    const high_bit: Group = @splat(@as(u8, 0x80));
    const has_high_bit = (ctrl & high_bit) == high_bit;
    return @bitCast(has_high_bit);
}

/// Load a group of control bytes from memory
inline fn loadGroup(ptr: [*]const u8) Group {
    return ptr[0..GROUP_WIDTH].*;
}

// ============================================================================
// Bit manipulation helpers
// ============================================================================

/// Iterator over set bits in a bitmask
const BitMaskIterator = struct {
    mask: BitMask,

    inline fn next(self: *BitMaskIterator) ?BitMaskBits {
        if (self.mask == 0) return null;
        const bit: BitMaskBits = @truncate(@ctz(self.mask));
        self.mask &= self.mask - 1; // Clear lowest set bit
        return bit;
    }
};

// ============================================================================
// Probing Sequence
// ============================================================================

/// Triangular probing sequence
/// Visits positions: pos, pos+1, pos+3, pos+6, pos+10, ...
/// Guarantees visiting all groups exactly once in a power-of-2 table
const ProbeSeq = struct {
    pos: usize,
    stride: usize,
    bucket_mask: usize,

    inline fn init(hash: u64, bucket_mask: usize) ProbeSeq {
        return .{
            .pos = h1(hash, bucket_mask),
            .stride = 0,
            .bucket_mask = bucket_mask,
        };
    }

    /// Move to next group in probe sequence
    inline fn next(self: *ProbeSeq) void {
        self.stride += GROUP_WIDTH;
        self.pos = (self.pos + self.stride) & self.bucket_mask;
    }

    /// Get current position
    inline fn current(self: *const ProbeSeq) usize {
        return self.pos;
    }
};

// ============================================================================
// Swiss Table Implementation
// ============================================================================

/// High-performance hash map using Swiss Table design
pub fn Table(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        /// Entry stored in the table
        pub const Entry = struct {
            key: K,
            value: V,
        };

        // Memory layout:
        // [ctrl bytes: n + GROUP_WIDTH] [entries: n]
        // Extra GROUP_WIDTH ctrl bytes for unaligned SIMD loads at end

        ctrl: [*]u8, // Control bytes
        entries: [*]Entry, // Key-value pairs
        bucket_mask: usize, // n - 1 (for fast modulo)
        growth_left: usize, // Insertions before resize
        len: usize, // Number of stored items
        allocator: std.mem.Allocator,

        // ====================================================================
        // Initialization
        // ====================================================================

        /// Create an empty Swiss Table
        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .ctrl = @constCast(&[_]u8{EMPTY} ** GROUP_WIDTH),
                .entries = undefined,
                .bucket_mask = 0,
                .growth_left = 0,
                .len = 0,
                .allocator = allocator,
            };
        }

        /// Create with pre-allocated capacity
        pub fn initCapacity(allocator: std.mem.Allocator, initial_capacity: usize) !Self {
            var self = init(allocator);
            try self.reserve(initial_capacity);
            return self;
        }

        /// Free all memory
        pub fn deinit(self: *Self) void {
            // Only free if we actually allocated (bucket_mask > 0 means capacity >= 16)
            if (self.bucket_mask >= 15) { // At least 16 buckets allocated
                const num_buckets = self.bucket_mask + 1;
                const ctrl_size = num_buckets + GROUP_WIDTH;
                const total_size = ctrl_size + num_buckets * @sizeOf(Entry);

                // Free the single allocation starting at ctrl
                const slice = self.ctrl[0..total_size];
                self.allocator.free(slice);
            }
            self.* = init(self.allocator);
        }

        // ====================================================================
        // Core Operations
        // ====================================================================

        /// Get value for key, or null if not found
        pub fn get(self: *const Self, key: K) ?*V {
            if (self.len == 0) return null;

            const hash = hashKey(key);
            const h2_val = h2(hash);
            const pos = h1(hash, self.bucket_mask);

            // Prefetch the first likely entry location
            @prefetch(@as([*]const u8, @ptrCast(&self.entries[pos])), .{
                .rw = .read,
                .locality = 3,
                .cache = .data,
            });

            var probe = ProbeSeq{ .pos = pos, .stride = 0, .bucket_mask = self.bucket_mask };

            while (true) {
                const current_pos = probe.current();
                const ctrl = loadGroup(self.ctrl + current_pos);

                // Check for H2 matches
                var match_mask = matchH2(ctrl, h2_val);
                while (match_mask != 0) {
                    const bit: u4 = @truncate(@ctz(match_mask));
                    match_mask &= match_mask - 1; // Clear lowest bit

                    const idx = (current_pos + bit) & self.bucket_mask;
                    if (self.entries[idx].key == key) {
                        return &self.entries[idx].value;
                    }
                }

                // If we see EMPTY, key doesn't exist
                if (matchEmpty(ctrl) != 0) {
                    return null;
                }

                probe.next();

                // Prefetch next probe location
                @prefetch(@as([*]const u8, @ptrCast(&self.entries[probe.current()])), .{
                    .rw = .read,
                    .locality = 3,
                    .cache = .data,
                });
            }
        }

        /// Check if key exists
        pub fn contains(self: *const Self, key: K) bool {
            return self.get(key) != null;
        }

        /// Insert or update a key-value pair
        /// Returns the old value if key existed
        pub fn put(self: *Self, key: K, value: V) !?V {
            // Ensure capacity
            if (self.growth_left == 0) {
                try self.grow();
            }

            return self.insertNoGrow(key, value);
        }

        /// Internal insert that assumes there's space (used during rehash)
        fn insertNoGrow(self: *Self, key: K, value: V) ?V {
            const hash = hashKey(key);
            const h2_val = h2(hash);

            var probe = ProbeSeq.init(hash, self.bucket_mask);
            var insert_idx: ?usize = null;

            while (true) {
                const pos = probe.current();
                const ctrl = loadGroup(self.ctrl + pos);

                // Check for existing key
                var matches = BitMaskIterator{ .mask = matchH2(ctrl, h2_val) };
                while (matches.next()) |bit| {
                    // Wrap index to handle groups that cross table boundary
                    const idx = (pos + bit) & self.bucket_mask;
                    if (self.entries[idx].key == key) {
                        // Key exists, update value
                        const old = self.entries[idx].value;
                        self.entries[idx].value = value;
                        return old;
                    }
                }

                // Track first EMPTY/DELETED slot for insertion
                if (insert_idx == null) {
                    const empty_or_deleted = matchEmptyOrDeleted(ctrl);
                    if (empty_or_deleted != 0) {
                        const bit: u4 = @truncate(@ctz(empty_or_deleted));
                        insert_idx = (pos + bit) & self.bucket_mask;
                    }
                }

                // If we see EMPTY, key doesn't exist
                if (matchEmpty(ctrl) != 0) {
                    break;
                }

                probe.next();
            }

            // Insert new key
            const idx = insert_idx.?;
            const was_empty = self.ctrl[idx] == EMPTY;
            self.setCtrl(idx, h2_val);
            self.entries[idx] = .{ .key = key, .value = value };
            self.len += 1;

            // Only decrease growth_left for truly new slots
            if (was_empty) {
                self.growth_left -= 1;
            }

            return null;
        }

        /// Fast insert for known-new keys (skips existence check)
        /// Used for bulk inserts and rehashing when keys are guaranteed unique
        pub fn putNew(self: *Self, key: K, value: V) !void {
            if (self.growth_left == 0) {
                try self.grow();
            }
            self.insertNew(key, value);
        }

        /// Get existing value or insert default, return pointer.
        /// This is the key API for building join hash tables where multiple
        /// rows can have the same key.
        ///
        /// Returns a pointer to the value (existing or newly inserted).
        pub fn getOrInsertDefault(self: *Self, key: K, default: V) !*V {
            // Ensure capacity
            if (self.growth_left == 0) {
                try self.grow();
            }

            const hash = hashKey(key);
            const h2_val = h2(hash);

            var probe = ProbeSeq.init(hash, self.bucket_mask);
            var insert_idx: ?usize = null;

            while (true) {
                const pos = probe.current();
                const ctrl = loadGroup(self.ctrl + pos);

                // Check for existing key
                var matches = BitMaskIterator{ .mask = matchH2(ctrl, h2_val) };
                while (matches.next()) |bit| {
                    const idx = (pos + bit) & self.bucket_mask;
                    if (self.entries[idx].key == key) {
                        // Key exists, return pointer to value
                        return &self.entries[idx].value;
                    }
                }

                // Track first EMPTY/DELETED slot for insertion
                if (insert_idx == null) {
                    const empty_or_deleted = matchEmptyOrDeleted(ctrl);
                    if (empty_or_deleted != 0) {
                        const bit: u4 = @truncate(@ctz(empty_or_deleted));
                        insert_idx = (pos + bit) & self.bucket_mask;
                    }
                }

                // If we see EMPTY, key doesn't exist
                if (matchEmpty(ctrl) != 0) {
                    break;
                }

                probe.next();
            }

            // Insert new key with default value
            const idx = insert_idx.?;
            const was_empty = self.ctrl[idx] == EMPTY;
            self.setCtrl(idx, h2_val);
            self.entries[idx] = .{ .key = key, .value = default };
            self.len += 1;

            if (was_empty) {
                self.growth_left -= 1;
            }

            return &self.entries[idx].value;
        }

        /// Get pointer to value if key exists
        pub fn getPtr(self: *Self, key: K) ?*V {
            if (self.len == 0) return null;

            const hash = hashKey(key);
            const h2_val = h2(hash);
            const pos = h1(hash, self.bucket_mask);

            @prefetch(@as([*]const u8, @ptrCast(&self.entries[pos])), .{
                .rw = .read,
                .locality = 3,
                .cache = .data,
            });

            var probe = ProbeSeq{ .pos = pos, .stride = 0, .bucket_mask = self.bucket_mask };

            while (true) {
                const current_pos = probe.current();
                const ctrl = loadGroup(self.ctrl + current_pos);

                var match_mask = matchH2(ctrl, h2_val);
                while (match_mask != 0) {
                    const bit: u4 = @truncate(@ctz(match_mask));
                    match_mask &= match_mask - 1;

                    const idx = (current_pos + bit) & self.bucket_mask;
                    if (self.entries[idx].key == key) {
                        return &self.entries[idx].value;
                    }
                }

                if (matchEmpty(ctrl) != 0) {
                    return null;
                }

                probe.next();
            }
        }

        /// Fast internal insert that skips key existence checks
        fn insertNew(self: *Self, key: K, value: V) void {
            const hash = hashKey(key);
            const h2_val = h2(hash);

            var probe = ProbeSeq.init(hash, self.bucket_mask);

            while (true) {
                const pos = probe.current();
                const ctrl = loadGroup(self.ctrl + pos);

                // Find first EMPTY slot (not DELETED for new inserts)
                const empty_mask = matchEmpty(ctrl);
                if (empty_mask != 0) {
                    const bit: u4 = @truncate(@ctz(empty_mask));
                    const idx = (pos + bit) & self.bucket_mask;
                    self.setCtrl(idx, h2_val);
                    self.entries[idx] = .{ .key = key, .value = value };
                    self.len += 1;
                    self.growth_left -= 1;
                    return;
                }

                probe.next();
            }
        }

        /// Remove a key, returns the value if it existed
        pub fn remove(self: *Self, key: K) ?V {
            if (self.len == 0) return null;

            const hash = hashKey(key);
            const h2_val = h2(hash);

            var probe = ProbeSeq.init(hash, self.bucket_mask);

            while (true) {
                const pos = probe.current();
                const ctrl = loadGroup(self.ctrl + pos);

                // Check for H2 matches
                var matches = BitMaskIterator{ .mask = matchH2(ctrl, h2_val) };
                while (matches.next()) |bit| {
                    // Wrap index to handle groups that cross table boundary
                    const idx = (pos + bit) & self.bucket_mask;
                    if (self.entries[idx].key == key) {
                        // Found it - remove
                        const value = self.entries[idx].value;
                        self.setCtrl(idx, DELETED);
                        self.len -= 1;
                        return value;
                    }
                }

                // If we see EMPTY, key doesn't exist
                if (matchEmpty(ctrl) != 0) {
                    return null;
                }

                probe.next();
            }
        }

        /// Number of entries in the table
        pub fn count(self: *const Self) usize {
            return self.len;
        }

        /// Current capacity (number of buckets)
        pub fn capacity(self: *const Self) usize {
            return self.bucket_mask + 1;
        }

        /// Returns true if the table contains no elements
        pub fn isEmpty(self: *const Self) bool {
            return self.len == 0;
        }

        /// Clears the table, removing all key-value pairs.
        /// Keeps the allocated memory for reuse.
        pub fn clear(self: *Self) void {
            if (self.len == 0) return;

            const num_buckets = self.bucket_mask + 1;
            if (num_buckets >= 16) {
                // Reset all control bytes to EMPTY
                const ctrl_size = num_buckets + GROUP_WIDTH;
                @memset(self.ctrl[0..ctrl_size], EMPTY);
                self.growth_left = bucketCountForCapacity(num_buckets);
            }
            self.len = 0;
        }

        /// Clears the table and deallocates memory, returning to empty state.
        pub fn clearAndFree(self: *Self) void {
            self.deinit();
        }

        /// Retains only the elements specified by the predicate.
        /// Removes all entries for which `predicate(key, value)` returns false.
        pub fn retain(self: *Self, predicate: *const fn (K, *V) bool) void {
            if (self.len == 0) return;

            const num_buckets = self.bucket_mask + 1;
            for (0..num_buckets) |i| {
                if (self.ctrl[i] & 0x80 == 0) {
                    // FULL slot
                    if (!predicate(self.entries[i].key, &self.entries[i].value)) {
                        self.setCtrl(i, DELETED);
                        self.len -= 1;
                    }
                }
            }
        }

        /// Creates a copy of this table
        pub fn clone(self: *const Self) !Self {
            if (self.bucket_mask < 15) {
                // Empty table
                return init(self.allocator);
            }

            const num_buckets = self.bucket_mask + 1;
            const ctrl_size = num_buckets + GROUP_WIDTH;
            const total_size = ctrl_size + num_buckets * @sizeOf(Entry);

            const memory = try self.allocator.alloc(u8, total_size);

            var new_table = Self{
                .ctrl = memory.ptr,
                .entries = @ptrCast(@alignCast(memory.ptr + ctrl_size)),
                .bucket_mask = self.bucket_mask,
                .growth_left = self.growth_left,
                .len = self.len,
                .allocator = self.allocator,
            };

            // Copy control bytes and entries
            @memcpy(new_table.ctrl[0..ctrl_size], self.ctrl[0..ctrl_size]);
            @memcpy(
                @as([*]u8, @ptrCast(new_table.entries))[0 .. num_buckets * @sizeOf(Entry)],
                @as([*]const u8, @ptrCast(self.entries))[0 .. num_buckets * @sizeOf(Entry)],
            );

            return new_table;
        }

        /// Shrinks the capacity of the table as much as possible.
        /// It will drop down as much as possible while maintaining the internal rules
        /// and possibly leaving some space in accordance with the resize policy.
        pub fn shrinkToFit(self: *Self) !void {
            try self.shrinkTo(self.len);
        }

        /// Shrinks the capacity of the table with a lower limit.
        /// The capacity will remain at least as large as both the length and the supplied minimum.
        pub fn shrinkTo(self: *Self, min_capacity: usize) !void {
            const target = @max(self.len, min_capacity);
            if (target == 0) {
                self.deinit();
                return;
            }

            // Calculate minimum buckets needed
            var new_buckets: usize = 16;
            while (bucketCountForCapacity(new_buckets) < target) {
                new_buckets *= 2;
            }

            // Only shrink if we can reduce size
            if (new_buckets < self.bucket_mask + 1) {
                try self.resize(new_buckets);
            }
        }

        // ====================================================================
        // Entry API (hashbrown-style)
        // ====================================================================

        /// Entry API for in-place manipulation
        pub const EntryResult = union(enum) {
            occupied: OccupiedEntry,
            vacant: VacantEntry,
        };

        /// An occupied entry in the table
        pub const OccupiedEntry = struct {
            table: *Self,
            index: usize,

            /// Gets a reference to the value in the entry
            pub fn get(self: OccupiedEntry) *V {
                return &self.table.entries[self.index].value;
            }

            /// Gets a reference to the key in the entry
            pub fn key(self: OccupiedEntry) K {
                return self.table.entries[self.index].key;
            }

            /// Sets the value of the entry and returns the old value
            pub fn insert(self: OccupiedEntry, value: V) V {
                const old = self.table.entries[self.index].value;
                self.table.entries[self.index].value = value;
                return old;
            }

            /// Takes the value out of the entry, and returns it
            pub fn remove(self: OccupiedEntry) V {
                const value = self.table.entries[self.index].value;
                self.table.setCtrl(self.index, DELETED);
                self.table.len -= 1;
                return value;
            }
        };

        /// A vacant entry in the table
        pub const VacantEntry = struct {
            table: *Self,
            key: K,
            hash: u64,
            insert_idx: usize,
            was_empty: bool,

            /// Sets the value of the entry with the VacantEntry's key
            pub fn insert(self: VacantEntry, value: V) *V {
                const h2_val = h2(self.hash);
                self.table.setCtrl(self.insert_idx, h2_val);
                self.table.entries[self.insert_idx] = .{ .key = self.key, .value = value };
                self.table.len += 1;
                if (self.was_empty) {
                    self.table.growth_left -= 1;
                }
                return &self.table.entries[self.insert_idx].value;
            }

            /// Gets the key that would be used when inserting
            pub fn getKey(self: VacantEntry) K {
                return self.key;
            }
        };

        /// Gets the given key's corresponding entry in the table for in-place manipulation.
        /// Ensures capacity before returning.
        pub fn entry(self: *Self, key: K) !EntryResult {
            // Ensure capacity first
            if (self.growth_left == 0) {
                try self.grow();
            }

            const hash = hashKey(key);
            const h2_val = h2(hash);

            var probe = ProbeSeq.init(hash, self.bucket_mask);
            var insert_idx: ?usize = null;
            var insert_was_empty: bool = false;

            while (true) {
                const pos = probe.current();
                const ctrl = loadGroup(self.ctrl + pos);

                // Check for existing key
                var matches = BitMaskIterator{ .mask = matchH2(ctrl, h2_val) };
                while (matches.next()) |bit| {
                    const idx = (pos + bit) & self.bucket_mask;
                    if (self.entries[idx].key == key) {
                        return .{ .occupied = .{ .table = self, .index = idx } };
                    }
                }

                // Track first EMPTY/DELETED slot for insertion
                if (insert_idx == null) {
                    const empty_or_deleted = matchEmptyOrDeleted(ctrl);
                    if (empty_or_deleted != 0) {
                        const bit: u4 = @truncate(@ctz(empty_or_deleted));
                        insert_idx = (pos + bit) & self.bucket_mask;
                        insert_was_empty = self.ctrl[insert_idx.?] == EMPTY;
                    }
                }

                // If we see EMPTY, key doesn't exist
                if (matchEmpty(ctrl) != 0) {
                    break;
                }

                probe.next();
            }

            return .{ .vacant = .{
                .table = self,
                .key = key,
                .hash = hash,
                .insert_idx = insert_idx.?,
                .was_empty = insert_was_empty,
            } };
        }

        /// Inserts a key-value pair into the map if the key is not present,
        /// then returns a pointer to the value.
        pub fn getOrInsert(self: *Self, key: K, value: V) !*V {
            const e = try self.entry(key);
            switch (e) {
                .occupied => |o| return o.get(),
                .vacant => |v| return v.insert(value),
            }
        }

        /// Inserts a key-value pair into the map if the key is not present,
        /// using a function to compute the value.
        pub fn getOrInsertWith(self: *Self, key: K, default_fn: *const fn () V) !*V {
            const e = try self.entry(key);
            switch (e) {
                .occupied => |o| return o.get(),
                .vacant => |v| return v.insert(default_fn()),
            }
        }

        // ====================================================================
        // Iteration
        // ====================================================================

        /// Iterator over key-value pairs
        pub const Iterator = struct {
            table: *const Self,
            index: usize,

            pub fn next(self: *Iterator) ?*Entry {
                const num_buckets = self.table.bucket_mask + 1;
                while (self.index < num_buckets) {
                    const i = self.index;
                    self.index += 1;
                    // Check if slot is FULL (high bit clear)
                    if (self.table.ctrl[i] & 0x80 == 0) {
                        return &self.table.entries[i];
                    }
                }
                return null;
            }
        };

        /// Iterator over keys only
        pub const KeyIterator = struct {
            table: *const Self,
            index: usize,

            pub fn next(self: *KeyIterator) ?K {
                const num_buckets = self.table.bucket_mask + 1;
                while (self.index < num_buckets) {
                    const i = self.index;
                    self.index += 1;
                    if (self.table.ctrl[i] & 0x80 == 0) {
                        return self.table.entries[i].key;
                    }
                }
                return null;
            }
        };

        /// Iterator over values only (immutable)
        pub const ValueIterator = struct {
            table: *const Self,
            index: usize,

            pub fn next(self: *ValueIterator) ?*const V {
                const num_buckets = self.table.bucket_mask + 1;
                while (self.index < num_buckets) {
                    const i = self.index;
                    self.index += 1;
                    if (self.table.ctrl[i] & 0x80 == 0) {
                        return &self.table.entries[i].value;
                    }
                }
                return null;
            }
        };

        /// Iterator over values only (mutable)
        pub const ValueMutIterator = struct {
            table: *Self,
            index: usize,

            pub fn next(self: *ValueMutIterator) ?*V {
                const num_buckets = self.table.bucket_mask + 1;
                while (self.index < num_buckets) {
                    const i = self.index;
                    self.index += 1;
                    if (self.table.ctrl[i] & 0x80 == 0) {
                        return &self.table.entries[i].value;
                    }
                }
                return null;
            }
        };

        pub fn iterator(self: *const Self) Iterator {
            return .{ .table = self, .index = 0 };
        }

        /// Returns an iterator over keys
        pub fn keys(self: *const Self) KeyIterator {
            return .{ .table = self, .index = 0 };
        }

        /// Returns an iterator over values (immutable)
        pub fn values(self: *const Self) ValueIterator {
            return .{ .table = self, .index = 0 };
        }

        /// Returns an iterator over values (mutable)
        pub fn valuesMut(self: *Self) ValueMutIterator {
            return .{ .table = self, .index = 0 };
        }

        // ====================================================================
        // Internal Operations
        // ====================================================================

        /// Set control byte at index, handling mirroring for first GROUP_WIDTH positions
        inline fn setCtrl(self: *Self, index: usize, ctrl_byte: u8) void {
            self.ctrl[index] = ctrl_byte;
            // Mirror first GROUP_WIDTH bytes at the end
            if (index < GROUP_WIDTH) {
                const num_buckets = self.bucket_mask + 1;
                self.ctrl[num_buckets + index] = ctrl_byte;
            }
        }

        /// Reserve space for at least `additional` more entries
        pub fn reserve(self: *Self, additional: usize) !void {
            const required = self.len + additional;
            if (required <= bucketCountForCapacity(self.bucket_mask + 1)) {
                return;
            }

            // Calculate new size
            var new_buckets: usize = 16; // Minimum size
            while (bucketCountForCapacity(new_buckets) < required) {
                new_buckets *= 2;
            }

            try self.resize(new_buckets);
        }

        /// Grow the table (double size)
        fn grow(self: *Self) !void {
            const new_buckets = if (self.bucket_mask == 0) 16 else (self.bucket_mask + 1) * 2;
            try self.resize(new_buckets);
        }

        /// Resize to new number of buckets
        fn resize(self: *Self, new_buckets: usize) !void {
            std.debug.assert(std.math.isPowerOfTwo(new_buckets));
            std.debug.assert(new_buckets >= 16);

            const old_buckets = self.bucket_mask + 1;
            const old_ctrl = self.ctrl;
            const old_entries = self.entries;

            // Allocate new storage
            const ctrl_size = new_buckets + GROUP_WIDTH;
            const total_size = ctrl_size + new_buckets * @sizeOf(Entry);

            const memory = try self.allocator.alloc(u8, total_size);

            // Setup new table
            self.ctrl = memory.ptr;
            self.entries = @ptrCast(@alignCast(memory.ptr + ctrl_size));
            self.bucket_mask = new_buckets - 1;
            self.growth_left = bucketCountForCapacity(new_buckets);
            self.len = 0;

            // Initialize all control bytes to EMPTY
            @memset(self.ctrl[0..ctrl_size], EMPTY);

            // Rehash all existing entries (use insertNew since keys are guaranteed unique)
            if (old_buckets >= 16) {
                for (0..old_buckets) |i| {
                    if (old_ctrl[i] & 0x80 == 0) {
                        // FULL slot - rehash
                        const old_entry = old_entries[i];
                        self.insertNew(old_entry.key, old_entry.value);
                    }
                }

                // Free old storage - must match the allocation size
                const old_ctrl_size = old_buckets + GROUP_WIDTH;
                const old_total_size = old_ctrl_size + old_buckets * @sizeOf(Entry);
                self.allocator.free(old_ctrl[0..old_total_size]);
            }
        }

        /// Calculate usable capacity for a given bucket count
        fn bucketCountForCapacity(buckets: usize) usize {
            if (buckets == 0) return 0;
            // 87.5% load factor = 7/8
            return (buckets * MAX_LOAD_FACTOR_NUMERATOR) / MAX_LOAD_FACTOR_DENOMINATOR;
        }

        /// Hash a key - optimized for integer types
        fn hashKey(key: K) u64 {
            // Fast path for integer types (similar to foldhash/rapidhash)
            if (comptime @typeInfo(K) == .int or @typeInfo(K) == .comptime_int) {
                // Use module-level fastIntHash
                return fastIntHash(@as(u64, @bitCast(@as(i64, @intCast(key)))));
            } else if (comptime @sizeOf(K) <= 8 and std.meta.hasUniqueRepresentation(K)) {
                // Fast path for small types with unique representation
                var bits: u64 = 0;
                const key_bytes = std.mem.asBytes(&key);
                @memcpy(std.mem.asBytes(&bits)[0..@sizeOf(K)], key_bytes);
                return fastIntHash(bits);
            } else if (comptime std.meta.hasUniqueRepresentation(K)) {
                const bytes = std.mem.asBytes(&key);
                return std.hash.Wyhash.hash(0, bytes);
            } else {
                var hasher = std.hash.Wyhash.init(0);
                std.hash.autoHash(&hasher, key);
                return hasher.final();
            }
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

test "basic operations" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    // Insert
    try std.testing.expectEqual(null, try map.put(1, 100));
    try std.testing.expectEqual(null, try map.put(2, 200));
    try std.testing.expectEqual(null, try map.put(3, 300));

    // Get
    try std.testing.expectEqual(100, map.get(1).?.*);
    try std.testing.expectEqual(200, map.get(2).?.*);
    try std.testing.expectEqual(300, map.get(3).?.*);
    try std.testing.expectEqual(null, map.get(4));

    // Update
    try std.testing.expectEqual(100, try map.put(1, 1000));
    try std.testing.expectEqual(1000, map.get(1).?.*);

    // Remove
    try std.testing.expectEqual(200, map.remove(2));
    try std.testing.expectEqual(null, map.get(2));
    try std.testing.expectEqual(2, map.count());

    // Contains
    try std.testing.expect(map.contains(1));
    try std.testing.expect(!map.contains(2));
}

test "grow and rehash" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    // Insert many items to trigger growth
    for (0..1000) |i| {
        _ = try map.put(@intCast(i), @intCast(i * 10));
    }

    try std.testing.expectEqual(1000, map.count());

    // Verify all items
    for (0..1000) |i| {
        const key: i64 = @intCast(i);
        try std.testing.expectEqual(key * 10, map.get(key).?.*);
    }
}

test "iteration" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    _ = try map.put(1, 100);
    _ = try map.put(2, 200);
    _ = try map.put(3, 300);

    var sum: i64 = 0;
    var it = map.iterator();
    while (it.next()) |entry| {
        sum += entry.value;
    }

    try std.testing.expectEqual(600, sum);
}

test "putNew for bulk inserts" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    // Bulk insert using putNew (known unique keys)
    for (0..1000) |i| {
        try map.putNew(@intCast(i), @intCast(i * 100));
    }

    try std.testing.expectEqual(1000, map.count());

    // Verify all items
    for (0..1000) |i| {
        const key: i64 = @intCast(i);
        try std.testing.expectEqual(key * 100, map.get(key).?.*);
    }
}

test "SIMD operations" {
    // Test matchH2 - use GROUP_WIDTH elements
    // With hashbrown encoding: EMPTY=0xFF, DELETED=0x80, FULL=0x00-0x7F
    if (GROUP_WIDTH == 8) {
        // 8-element test (aarch64)
        const ctrl: Group = .{ 0x42, 0xFF, 0x42, 0x80, 0x00, 0x42, 0xFF, 0x80 };

        const matches = matchH2(ctrl, 0x42);
        // Positions 0, 2, 5 should match (0x42)
        try std.testing.expectEqual(@as(BitMask, 0b0010_0101), matches);

        const empties = matchEmpty(ctrl);
        // Positions 1, 6 should be EMPTY (0xFF)
        try std.testing.expectEqual(@as(BitMask, 0b0100_0010), empties);

        const empty_or_deleted = matchEmptyOrDeleted(ctrl);
        // Positions 1, 3, 6, 7 should be EMPTY or DELETED (high bit set)
        try std.testing.expectEqual(@as(BitMask, 0b1100_1010), empty_or_deleted);
    } else {
        // 16-element test (x86-64)
        const ctrl: Group = .{ 0x42, 0xFF, 0x42, 0x80, 0x00, 0x42, 0xFF, 0x80, 0x42, 0x01, 0x02, 0x03, 0x04, 0x05, 0x42, 0x07 };

        const matches = matchH2(ctrl, 0x42);
        // Positions 0, 2, 5, 8, 14 should match
        try std.testing.expectEqual(@as(BitMask, 0b0100_0001_0010_0101), matches);

        const empties = matchEmpty(ctrl);
        // Positions 1, 6 should be EMPTY (0xFF)
        try std.testing.expectEqual(@as(BitMask, 0b0000_0000_0100_0010), empties);

        const empty_or_deleted = matchEmptyOrDeleted(ctrl);
        // Positions 1, 3, 6, 7 should be EMPTY or DELETED (high bit set)
        try std.testing.expectEqual(@as(BitMask, 0b0000_0000_1100_1010), empty_or_deleted);
    }
}

test "getOrInsertDefault - new key" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    const ptr = try map.getOrInsertDefault(42, 0);
    try std.testing.expectEqual(@as(i64, 0), ptr.*);

    // Modify through pointer
    ptr.* = 100;

    // Should get same value back
    try std.testing.expectEqual(@as(i64, 100), map.get(42).?.*);
}

test "getOrInsertDefault - existing key" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    _ = try map.put(42, 999);

    // Should return existing value, not default
    const ptr = try map.getOrInsertDefault(42, 0);
    try std.testing.expectEqual(@as(i64, 999), ptr.*);
}

test "getOrInsertDefault - multiple duplicates" {
    // Simulate join use case: build table with multiple indices per key
    // Using ArrayList as value type to demonstrate accumulating values
    const IndexList = std.ArrayListUnmanaged(i32);

    var map = Table(i64, IndexList).init(std.testing.allocator);
    defer {
        var it = map.iterator();
        while (it.next()) |entry| {
            var e = entry;
            e.value.deinit(std.testing.allocator);
        }
        map.deinit();
    }

    // Simulate building: key 10 appears at rows 0, 5, 9
    const rows_for_10 = [_]i32{ 0, 5, 9 };
    for (rows_for_10) |row| {
        const list = try map.getOrInsertDefault(10, IndexList{});
        try list.append(std.testing.allocator, row);
    }

    // Key 20 appears at row 3
    const list_20 = try map.getOrInsertDefault(20, IndexList{});
    try list_20.append(std.testing.allocator, 3);

    // Verify
    const vec_10 = map.get(10).?;
    try std.testing.expectEqual(@as(usize, 3), vec_10.items.len);
    try std.testing.expectEqualSlices(i32, &rows_for_10, vec_10.items);

    const vec_20 = map.get(20).?;
    try std.testing.expectEqual(@as(usize, 1), vec_20.items.len);
    try std.testing.expectEqual(@as(i32, 3), vec_20.items[0]);
}

test "hashToPartition - distribution" {
    // Test that hashToPartition distributes evenly
    const n_partitions: usize = 8;
    var counts: [8]usize = .{ 0, 0, 0, 0, 0, 0, 0, 0 };

    // Hash many values and count partition distribution
    for (0..10000) |i| {
        const h = dirtyHash(@as(u64, i));
        const p = hashToPartition(h, n_partitions);
        counts[p] += 1;
    }

    // Each partition should have roughly 10000/8 = 1250 items
    // Allow 20% deviation
    for (counts) |c| {
        try std.testing.expect(c > 1000);
        try std.testing.expect(c < 1500);
    }
}

test "hashToPartition - edge cases" {
    // Single partition always returns 0
    try std.testing.expectEqual(@as(usize, 0), hashToPartition(0, 1));
    try std.testing.expectEqual(@as(usize, 0), hashToPartition(std.math.maxInt(u64), 1));

    // Two partitions
    const h1_result = hashToPartition(0, 2);
    const h2_result = hashToPartition(std.math.maxInt(u64), 2);
    try std.testing.expect(h1_result < 2);
    try std.testing.expect(h2_result < 2);
}

test "clear and isEmpty" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    try std.testing.expect(map.isEmpty());

    _ = try map.put(1, 100);
    _ = try map.put(2, 200);

    try std.testing.expect(!map.isEmpty());
    try std.testing.expectEqual(@as(usize, 2), map.count());

    map.clear();

    try std.testing.expect(map.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), map.count());
    // Capacity should be preserved
    try std.testing.expect(map.capacity() >= 16);
}

test "clone" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    _ = try map.put(1, 100);
    _ = try map.put(2, 200);
    _ = try map.put(3, 300);

    var cloned = try map.clone();
    defer cloned.deinit();

    // Cloned should have same contents
    try std.testing.expectEqual(@as(usize, 3), cloned.count());
    try std.testing.expectEqual(@as(i64, 100), cloned.get(1).?.*);
    try std.testing.expectEqual(@as(i64, 200), cloned.get(2).?.*);
    try std.testing.expectEqual(@as(i64, 300), cloned.get(3).?.*);

    // Modifying original shouldn't affect clone
    _ = try map.put(4, 400);
    try std.testing.expect(!cloned.contains(4));
}

test "Entry API - vacant" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    const e = try map.entry(42);
    switch (e) {
        .occupied => try std.testing.expect(false), // Should be vacant
        .vacant => |v| {
            _ = v.insert(100);
        },
    }

    try std.testing.expectEqual(@as(i64, 100), map.get(42).?.*);
}

test "Entry API - occupied" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    _ = try map.put(42, 100);

    const e = try map.entry(42);
    switch (e) {
        .occupied => |o| {
            try std.testing.expectEqual(@as(i64, 100), o.get().*);
            const old = o.insert(200);
            try std.testing.expectEqual(@as(i64, 100), old);
        },
        .vacant => try std.testing.expect(false), // Should be occupied
    }

    try std.testing.expectEqual(@as(i64, 200), map.get(42).?.*);
}

test "keys and values iterators" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    _ = try map.put(1, 100);
    _ = try map.put(2, 200);
    _ = try map.put(3, 300);

    // Test keys iterator
    var key_sum: i64 = 0;
    var keys_it = map.keys();
    while (keys_it.next()) |k| {
        key_sum += k;
    }
    try std.testing.expectEqual(@as(i64, 6), key_sum);

    // Test values iterator
    var val_sum: i64 = 0;
    var vals_it = map.values();
    while (vals_it.next()) |v| {
        val_sum += v.*;
    }
    try std.testing.expectEqual(@as(i64, 600), val_sum);

    // Test valuesMut iterator
    var vals_mut_it = map.valuesMut();
    while (vals_mut_it.next()) |v| {
        v.* *= 2;
    }

    // Verify values were modified
    try std.testing.expectEqual(@as(i64, 200), map.get(1).?.*);
    try std.testing.expectEqual(@as(i64, 400), map.get(2).?.*);
    try std.testing.expectEqual(@as(i64, 600), map.get(3).?.*);
}

test "shrinkToFit" {
    var map = Table(i64, i64).init(std.testing.allocator);
    defer map.deinit();

    // Add many items
    for (0..100) |i| {
        _ = try map.put(@intCast(i), @intCast(i * 10));
    }

    const cap_before = map.capacity();

    // Remove most items
    for (50..100) |i| {
        _ = map.remove(@intCast(i));
    }

    // Shrink
    try map.shrinkToFit();

    // Capacity should be smaller (or at least not larger with same count)
    try std.testing.expect(map.capacity() <= cap_before);
    try std.testing.expectEqual(@as(usize, 50), map.count());

    // Verify remaining items
    for (0..50) |i| {
        const key: i64 = @intCast(i);
        try std.testing.expectEqual(key * 10, map.get(key).?.*);
    }
}
