//! Chase-Lev Work-Stealing Deque
//!
//! A lock-free, single-producer multi-consumer deque optimized for work-stealing.
//! Based on "Dynamic Circular Work-Stealing Deque" by Chase and Lev (2005)
//! with improvements from "Correct and Efficient Work-Stealing for Weak Memory
//! Models" by Lê et al. (2013).
//!
//! Key properties:
//! - Owner thread: push (LIFO), pop (LIFO) - no synchronization needed for push
//! - Stealing threads: steal (FIFO) - lock-free with CAS
//! - Dynamically growable circular buffer

const std = @import("std");

/// Result of a steal operation.
pub fn StealResult(comptime T: type) type {
    return union(enum) {
        /// Successfully stolen an item
        success: T,
        /// Deque was empty
        empty,
        /// Lost race with pop or another steal; retry may succeed
        retry,
    };
}

/// A work-stealing deque.
/// Only the owner thread may call push() and pop().
/// Any thread may call steal().
pub fn Deque(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Circular buffer holding the items.
        /// Access via atomic to handle buffer growth.
        buffer: std.atomic.Value(*Buffer),

        /// Bottom index (modified only by owner, read by stealers).
        /// Points to the next slot where push will write.
        bottom: std.atomic.Value(isize),

        /// Top index (modified by owner on pop, and by stealers on steal).
        /// Points to the oldest item that can be stolen.
        top: std.atomic.Value(isize),

        /// Allocator for buffer allocation/reallocation.
        allocator: std.mem.Allocator,

        /// Old buffers that were replaced during growth (freed on deinit).
        /// Max 16 growths allowed (buffer grows from 32 to 2^36 elements).
        old_buffers: [16]?*Buffer,
        old_buffer_count: usize,

        /// Minimum capacity (power of 2).
        const MIN_CAPACITY: usize = 32;

        /// Circular buffer with power-of-2 capacity.
        const Buffer = struct {
            storage: []T,
            mask: usize, // capacity - 1

            fn init(allocator: std.mem.Allocator, cap: usize) !*Buffer {
                std.debug.assert(std.math.isPowerOfTwo(cap));

                const buf = try allocator.create(Buffer);
                errdefer allocator.destroy(buf);

                buf.storage = try allocator.alloc(T, cap);
                buf.mask = cap - 1;

                return buf;
            }

            fn deinit(self: *Buffer, allocator: std.mem.Allocator) void {
                allocator.free(self.storage);
                allocator.destroy(self);
            }

            fn capacity(self: *const Buffer) usize {
                return self.mask + 1;
            }

            fn get(self: *const Buffer, index: isize) T {
                const i = @as(usize, @intCast(@mod(index, @as(isize, @intCast(self.capacity())))));
                return self.storage[i];
            }

            fn put(self: *Buffer, index: isize, value: T) void {
                const i = @as(usize, @intCast(@mod(index, @as(isize, @intCast(self.capacity())))));
                self.storage[i] = value;
            }

            /// Grow buffer, copying items from old buffer.
            fn grow(self: *const Buffer, allocator: std.mem.Allocator, bottom: isize, top: isize) !*Buffer {
                const new_capacity = self.capacity() * 2;
                const new_buf = try Buffer.init(allocator, new_capacity);

                // Copy items from old to new buffer
                var i = top;
                while (i < bottom) : (i += 1) {
                    new_buf.put(i, self.get(i));
                }

                return new_buf;
            }
        };

        /// Initialize a new deque with default capacity.
        pub fn init(allocator: std.mem.Allocator) !Self {
            return initWithCapacity(allocator, MIN_CAPACITY);
        }

        /// Initialize a new deque with specified initial capacity.
        pub fn initWithCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
            const actual_capacity = std.math.ceilPowerOfTwo(usize, @max(capacity, MIN_CAPACITY)) catch MIN_CAPACITY;
            const buffer = try Buffer.init(allocator, actual_capacity);

            return Self{
                .buffer = std.atomic.Value(*Buffer).init(buffer),
                .bottom = std.atomic.Value(isize).init(0),
                .top = std.atomic.Value(isize).init(0),
                .allocator = allocator,
                .old_buffers = [_]?*Buffer{null} ** 16,
                .old_buffer_count = 0,
            };
        }

        /// Deinitialize the deque, freeing all memory.
        pub fn deinit(self: *Self) void {
            // Free old buffers that were replaced during growth
            for (self.old_buffers[0..self.old_buffer_count]) |maybe_buf| {
                if (maybe_buf) |old_buf| {
                    old_buf.deinit(self.allocator);
                }
            }

            // Free current buffer
            self.buffer.load(.acquire).deinit(self.allocator);
        }

        /// Check if the deque is empty.
        /// Note: This is a snapshot; the deque may change immediately after.
        pub fn isEmpty(self: *const Self) bool {
            const b = self.bottom.load(.acquire);
            const t = self.top.load(.acquire);
            return b <= t;
        }

        /// Get approximate length.
        /// Note: This is a snapshot; actual length may differ.
        pub fn len(self: *const Self) usize {
            const b = self.bottom.load(.acquire);
            const t = self.top.load(.acquire);
            if (b <= t) return 0;
            return @intCast(b - t);
        }

        /// Push an item onto the bottom of the deque.
        /// Only the owner thread may call this.
        pub fn push(self: *Self, value: T) !void {
            const b = self.bottom.load(.monotonic);
            const t = self.top.load(.acquire);
            var buf = self.buffer.load(.monotonic);

            // Check if we need to grow
            const size = b - t;
            if (size >= @as(isize, @intCast(buf.capacity() - 1))) {
                // Track old buffer for later cleanup
                const old_buf = buf;
                // Grow buffer
                buf = try buf.grow(self.allocator, b, t);
                self.buffer.store(buf, .release);
                // Save old buffer (will be freed on deinit)
                if (self.old_buffer_count < 16) {
                    self.old_buffers[self.old_buffer_count] = old_buf;
                    self.old_buffer_count += 1;
                }
            }

            // Store item
            buf.put(b, value);

            // Publish the new bottom with release ordering
            // This ensures the item store is visible before bottom increment is visible
            self.bottom.store(b + 1, .release);
        }

        /// Pop an item from the bottom of the deque.
        /// Only the owner thread may call this.
        /// Returns null if the deque is empty.
        pub fn pop(self: *Self) ?T {
            const b = self.bottom.load(.monotonic) - 1;
            const buf = self.buffer.load(.monotonic);

            // Publish the decremented bottom with seq_cst ordering
            // This acts as a full barrier ensuring visibility to stealers
            _ = self.bottom.swap(b, .seq_cst);

            const t = self.top.load(.acquire);

            if (t <= b) {
                // Non-empty
                const value = buf.get(b);

                if (t == b) {
                    // This is the last element, race with stealers
                    if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic) != null) {
                        // Lost race with a stealer
                        self.bottom.store(b + 1, .monotonic);
                        return null;
                    }
                    self.bottom.store(b + 1, .monotonic);
                }

                return value;
            } else {
                // Empty
                self.bottom.store(b + 1, .monotonic);
                return null;
            }
        }

        /// Steal an item from the top of the deque.
        /// Any thread may call this.
        pub fn steal(self: *Self) StealResult(T) {
            const t = self.top.load(.acquire);

            // Full barrier via seq_cst load (synchronizes with pop's seq_cst store)
            const b = self.bottom.load(.seq_cst);

            if (t >= b) {
                return .empty;
            }

            // Non-empty, try to steal
            const buf = self.buffer.load(.acquire);
            const value = buf.get(t);

            // Try to increment top
            if (self.top.cmpxchgStrong(t, t + 1, .seq_cst, .monotonic) != null) {
                // Lost race
                return .retry;
            }

            return .{ .success = value };
        }

        /// Steal half of the items from this deque into another deque.
        /// Returns the number of items stolen.
        pub fn stealBatch(self: *Self, target: *Self, max_steal: usize) usize {
            var stolen: usize = 0;
            const to_steal = @min(max_steal, self.len() / 2 + 1);

            while (stolen < to_steal) {
                switch (self.steal()) {
                    .success => |value| {
                        target.push(value) catch break;
                        stolen += 1;
                    },
                    .empty => break,
                    .retry => continue,
                }
            }

            return stolen;
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

test "Deque - basic push/pop (LIFO)" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    try deque.push(1);
    try deque.push(2);
    try deque.push(3);

    // Pop returns LIFO order
    try std.testing.expectEqual(@as(?u32, 3), deque.pop());
    try std.testing.expectEqual(@as(?u32, 2), deque.pop());
    try std.testing.expectEqual(@as(?u32, 1), deque.pop());
    try std.testing.expectEqual(@as(?u32, null), deque.pop());
}

test "Deque - steal returns FIFO" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    try deque.push(1);
    try deque.push(2);
    try deque.push(3);

    // Steal returns FIFO order (oldest first)
    try std.testing.expectEqual(StealResult(u32){ .success = 1 }, deque.steal());
    try std.testing.expectEqual(StealResult(u32){ .success = 2 }, deque.steal());
    try std.testing.expectEqual(StealResult(u32){ .success = 3 }, deque.steal());
    try std.testing.expectEqual(StealResult(u32).empty, deque.steal());
}

test "Deque - interleaved push/pop/steal" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    try deque.push(1);
    try deque.push(2);
    try std.testing.expectEqual(StealResult(u32){ .success = 1 }, deque.steal()); // Steal oldest

    try deque.push(3);
    try std.testing.expectEqual(@as(?u32, 3), deque.pop()); // Pop newest

    try std.testing.expectEqual(StealResult(u32){ .success = 2 }, deque.steal()); // Steal remaining
    try std.testing.expectEqual(StealResult(u32).empty, deque.steal());
}

test "Deque - grow buffer" {
    var deque = try Deque(u32).initWithCapacity(std.testing.allocator, 4);
    defer deque.deinit();

    // Push more than initial capacity
    for (0..100) |i| {
        try deque.push(@intCast(i));
    }

    // Verify all items are present (pop in reverse order)
    var i: u32 = 100;
    while (i > 0) {
        i -= 1;
        try std.testing.expectEqual(@as(?u32, i), deque.pop());
    }
}

test "Deque - isEmpty and len" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    try std.testing.expect(deque.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), deque.len());

    try deque.push(1);
    try std.testing.expect(!deque.isEmpty());
    try std.testing.expectEqual(@as(usize, 1), deque.len());

    try deque.push(2);
    try std.testing.expectEqual(@as(usize, 2), deque.len());

    _ = deque.pop();
    try std.testing.expectEqual(@as(usize, 1), deque.len());
}

test "Deque - concurrent steal (single stealer)" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    const NUM_ITEMS: u32 = 1000;

    // Push items
    for (0..NUM_ITEMS) |i| {
        try deque.push(@intCast(i));
    }

    var stolen_count = std.atomic.Value(u32).init(0);

    // Spawn stealer thread
    const stealer = std.Thread.spawn(.{}, struct {
        fn run(d: *Deque(u32), count: *std.atomic.Value(u32)) void {
            while (true) {
                switch (d.steal()) {
                    .success => {
                        _ = count.fetchAdd(1, .acq_rel);
                    },
                    .empty => break,
                    .retry => continue,
                }
            }
        }
    }.run, .{ &deque, &stolen_count }) catch unreachable;

    stealer.join();

    // All items should be stolen
    try std.testing.expectEqual(NUM_ITEMS, stolen_count.load(.acquire));
    try std.testing.expect(deque.isEmpty());
}

test "Deque - concurrent push/steal" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    const NUM_ITEMS: u32 = 10000;
    var stolen_sum = std.atomic.Value(u64).init(0);
    var done_pushing = std.atomic.Value(bool).init(false);

    // Spawn stealer thread
    const stealer = std.Thread.spawn(.{}, struct {
        fn run(d: *Deque(u32), sum: *std.atomic.Value(u64), done: *std.atomic.Value(bool)) void {
            while (!done.load(.acquire) or !d.isEmpty()) {
                switch (d.steal()) {
                    .success => |v| {
                        _ = sum.fetchAdd(v, .acq_rel);
                    },
                    .empty, .retry => {
                        std.Thread.yield() catch {};
                    },
                }
            }
        }
    }.run, .{ &deque, &stolen_sum, &done_pushing }) catch unreachable;

    // Push items (owner thread)
    var expected_sum: u64 = 0;
    for (0..NUM_ITEMS) |i| {
        try deque.push(@intCast(i));
        expected_sum += i;
    }
    done_pushing.store(true, .release);

    stealer.join();

    // Sum of stolen items should equal expected
    try std.testing.expectEqual(expected_sum, stolen_sum.load(.acquire));
}

test "Deque - multiple stealers" {
    var deque = try Deque(u32).init(std.testing.allocator);
    defer deque.deinit();

    const NUM_ITEMS: u32 = 10000;
    const NUM_STEALERS: usize = 4;

    // Push items
    for (0..NUM_ITEMS) |i| {
        try deque.push(@intCast(i));
    }

    var total_stolen = std.atomic.Value(u32).init(0);

    // Spawn multiple stealer threads
    var threads: [NUM_STEALERS]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(d: *Deque(u32), count: *std.atomic.Value(u32)) void {
                while (true) {
                    switch (d.steal()) {
                        .success => {
                            _ = count.fetchAdd(1, .acq_rel);
                        },
                        .empty => break,
                        .retry => continue,
                    }
                }
            }
        }.run, .{ &deque, &total_stolen }) catch unreachable;
    }

    // Wait for all stealers
    for (&threads) |*t| {
        t.join();
    }

    // All items should be stolen exactly once
    try std.testing.expectEqual(NUM_ITEMS, total_stolen.load(.acquire));
}
