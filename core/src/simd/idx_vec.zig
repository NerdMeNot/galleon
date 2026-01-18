//! IdxVec - Small Vector for Join Indices
//!
//! A SmallVec optimization that stores up to 4 indices inline to avoid
//! heap allocations for the common case of unique or low-duplicate keys.

const std = @import("std");

/// Index type used throughout join operations
pub const IdxSize = i32;

/// Sentinel value for null/missing indices
pub const NULL_IDX: IdxSize = -1;

/// Use c_allocator for heap allocations (matches SwissTable)
const heap_allocator = std.heap.c_allocator;

/// SmallVec that stores up to 4 indices inline.
/// For keys with more than 4 duplicates, spills to heap.
pub const IdxVec4 = struct {
    const INLINE_CAP: usize = 4;

    // Inline storage for up to 4 indices
    inline_buf: [INLINE_CAP]IdxSize = .{ NULL_IDX, NULL_IDX, NULL_IDX, NULL_IDX },
    inline_len: u8 = 0,

    // Heap storage for overflow
    heap: ?[]IdxSize = null,

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Free heap memory if allocated. Uses c_allocator internally.
    pub fn deinit(self: *Self) void {
        if (self.heap) |h| {
            heap_allocator.free(h);
            self.heap = null;
        }
    }

    pub fn len(self: *const Self) usize {
        if (self.heap) |h| {
            return h.len;
        }
        return self.inline_len;
    }

    /// Push an index. Uses provided allocator for heap operations.
    pub fn push(self: *Self, allocator: std.mem.Allocator, idx: IdxSize) !void {
        if (self.heap) |h| {
            // Already on heap, grow it using c_allocator for consistency
            const new_heap = try heap_allocator.realloc(h, h.len + 1);
            new_heap[new_heap.len - 1] = idx;
            self.heap = new_heap;
        } else if (self.inline_len < INLINE_CAP) {
            // Still fits inline
            self.inline_buf[self.inline_len] = idx;
            self.inline_len += 1;
        } else {
            // Spill to heap using c_allocator
            const new_heap = try heap_allocator.alloc(IdxSize, INLINE_CAP + 1);
            @memcpy(new_heap[0..INLINE_CAP], &self.inline_buf);
            new_heap[INLINE_CAP] = idx;
            self.heap = new_heap;
        }
        _ = allocator; // Ignore - we use c_allocator internally
    }

    pub fn slice(self: *const Self) []const IdxSize {
        if (self.heap) |h| {
            return h;
        }
        return self.inline_buf[0..self.inline_len];
    }

    pub fn iterator(self: *const Self) Iterator {
        return .{ .vec = self, .pos = 0 };
    }

    pub const Iterator = struct {
        vec: *const Self,
        pos: usize,

        pub fn next(self: *Iterator) ?IdxSize {
            const s = self.vec.slice();
            if (self.pos >= s.len) return null;
            const val = s[self.pos];
            self.pos += 1;
            return val;
        }
    };
};

// ============================================================================
// Tests
// ============================================================================

test "IdxVec4 - inline storage" {
    var vec = IdxVec4.init();
    defer vec.deinit();

    try vec.push(std.testing.allocator, 10);
    try vec.push(std.testing.allocator, 20);
    try vec.push(std.testing.allocator, 30);

    try std.testing.expectEqual(@as(usize, 3), vec.len());
    try std.testing.expectEqualSlices(IdxSize, &[_]IdxSize{ 10, 20, 30 }, vec.slice());
}

test "IdxVec4 - heap spill" {
    var vec = IdxVec4.init();
    defer vec.deinit();

    // Fill inline (4 elements)
    try vec.push(std.testing.allocator, 1);
    try vec.push(std.testing.allocator, 2);
    try vec.push(std.testing.allocator, 3);
    try vec.push(std.testing.allocator, 4);

    try std.testing.expectEqual(@as(usize, 4), vec.len());
    try std.testing.expect(vec.heap == null); // Still inline

    // Spill to heap
    try vec.push(std.testing.allocator, 5);

    try std.testing.expectEqual(@as(usize, 5), vec.len());
    try std.testing.expect(vec.heap != null); // Now on heap
    try std.testing.expectEqualSlices(IdxSize, &[_]IdxSize{ 1, 2, 3, 4, 5 }, vec.slice());
}

test "IdxVec4 - iterator" {
    var vec = IdxVec4.init();
    defer vec.deinit();

    try vec.push(std.testing.allocator, 100);
    try vec.push(std.testing.allocator, 200);

    var it = vec.iterator();
    try std.testing.expectEqual(@as(?IdxSize, 100), it.next());
    try std.testing.expectEqual(@as(?IdxSize, 200), it.next());
    try std.testing.expectEqual(@as(?IdxSize, null), it.next());
}
