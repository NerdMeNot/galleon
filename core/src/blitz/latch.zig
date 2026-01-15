//! Synchronization Primitives for Blitz
//!
//! This module provides lightweight synchronization primitives used for
//! coordinating parallel task execution in the work-stealing scheduler.

const std = @import("std");
const builtin = @import("builtin");
const Futex = std.Thread.Futex;

// ============================================================================
// SpinWait - Adaptive Spinning
// ============================================================================

/// Adaptive spinning utility that spins for a while before yielding.
/// Useful for short waits where blocking would be too expensive.
pub const SpinWait = struct {
    iteration: u32 = 0,

    const SPIN_LIMIT: u32 = 6;
    const YIELD_LIMIT: u32 = 10;

    pub fn reset(self: *SpinWait) void {
        self.iteration = 0;
    }

    /// Spin once, potentially yielding or sleeping based on iteration count.
    pub fn spin(self: *SpinWait) void {
        if (self.iteration < SPIN_LIMIT) {
            // Spin with pause instruction
            const shift: u5 = @intCast(self.iteration);
            var spin_count: u32 = @as(u32, 1) << shift;
            while (spin_count > 0) : (spin_count -= 1) {
                std.atomic.spinLoopHint();
            }
        } else if (self.iteration < YIELD_LIMIT) {
            // Yield to other threads
            std.Thread.yield() catch {};
        } else {
            // Sleep briefly
            std.Thread.sleep(1_000); // 1 microsecond
        }
        self.iteration +|= 1;
    }

    /// Returns true if the next spin would yield instead of spin.
    pub fn wouldYield(self: *const SpinWait) bool {
        return self.iteration >= SPIN_LIMIT;
    }
};

// ============================================================================
// OnceLatch - Single-Shot Completion Signal
// ============================================================================

/// A single-shot latch that signals completion of a task.
/// Can be waited on by multiple threads; once set, all waiters are released.
pub const OnceLatch = struct {
    const PENDING: u32 = 0;
    const DONE: u32 = 1;
    const WAITING: u32 = 2;

    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(PENDING),

    pub fn init() OnceLatch {
        return .{};
    }

    /// Check if the latch is set (non-blocking).
    pub fn isDone(self: *const OnceLatch) bool {
        return self.state.load(.acquire) == DONE;
    }

    /// Set the latch to done and wake all waiters.
    pub fn setDone(self: *OnceLatch) void {
        const prev = self.state.swap(DONE, .acq_rel);
        if (prev == WAITING) {
            // Wake all waiters
            Futex.wake(&self.state, std.math.maxInt(u32));
        }
    }

    /// Wait until the latch is set.
    pub fn wait(self: *OnceLatch) void {
        var spinner = SpinWait{};

        while (true) {
            const state = self.state.load(.acquire);
            if (state == DONE) return;

            if (!spinner.wouldYield()) {
                spinner.spin();
                continue;
            }

            // Try to mark as waiting
            if (self.state.cmpxchgWeak(PENDING, WAITING, .acq_rel, .acquire)) |_| {
                // CAS failed, retry
                spinner.spin();
                continue;
            }

            // Wait on futex
            Futex.wait(&self.state, WAITING);

            // Check if done
            if (self.state.load(.acquire) == DONE) return;
        }
    }

    /// Wait with timeout. Returns true if latch was set, false on timeout.
    pub fn waitTimeout(self: *OnceLatch, timeout_ns: u64) bool {
        const deadline = std.time.nanoTimestamp() + @as(i128, timeout_ns);
        var spinner = SpinWait{};

        while (true) {
            const state = self.state.load(.acquire);
            if (state == DONE) return true;

            const now = std.time.nanoTimestamp();
            if (now >= deadline) return false;

            if (!spinner.wouldYield()) {
                spinner.spin();
                continue;
            }

            // Try to mark as waiting
            _ = self.state.cmpxchgWeak(PENDING, WAITING, .acq_rel, .acquire);

            const remaining = @as(u64, @intCast(deadline - now));
            Futex.timedWait(&self.state, WAITING, remaining) catch |err| {
                if (err == error.Timeout) {
                    return self.state.load(.acquire) == DONE;
                }
            };
        }
    }
};

// ============================================================================
// CountLatch - Countdown Latch
// ============================================================================

/// A countdown latch that blocks until the count reaches zero.
/// Used for fork-join synchronization where we need to wait for N tasks.
pub const CountLatch = struct {
    // Pack count and waiter flag into single atomic
    // High bit = has waiters, lower 31 bits = count
    const WAITER_BIT: u32 = 1 << 31;
    const COUNT_MASK: u32 = ~WAITER_BIT;

    state: std.atomic.Value(u32),

    pub fn init(count: u32) CountLatch {
        std.debug.assert(count <= COUNT_MASK);
        return .{
            .state = std.atomic.Value(u32).init(count),
        };
    }

    /// Get current count (for debugging).
    pub fn getCount(self: *const CountLatch) u32 {
        return self.state.load(.acquire) & COUNT_MASK;
    }

    /// Check if count is zero.
    pub fn isDone(self: *const CountLatch) bool {
        return self.getCount() == 0;
    }

    /// Decrement the count. If it reaches zero, wake all waiters.
    pub fn countDown(self: *CountLatch) void {
        const prev = self.state.fetchSub(1, .acq_rel);
        const prev_count = prev & COUNT_MASK;
        const had_waiters = (prev & WAITER_BIT) != 0;

        std.debug.assert(prev_count > 0); // Underflow check

        if (prev_count == 1 and had_waiters) {
            // Count reached zero and there are waiters
            Futex.wake(&self.state, std.math.maxInt(u32));
        }
    }

    /// Increment the count. Used for dynamic task spawning.
    pub fn countUp(self: *CountLatch) void {
        const prev = self.state.fetchAdd(1, .acq_rel);
        std.debug.assert((prev & COUNT_MASK) < COUNT_MASK); // Overflow check
    }

    /// Wait until count reaches zero.
    pub fn wait(self: *CountLatch) void {
        var spinner = SpinWait{};

        while (true) {
            const state = self.state.load(.acquire);
            const count = state & COUNT_MASK;

            if (count == 0) return;

            if (!spinner.wouldYield()) {
                spinner.spin();
                continue;
            }

            // Set waiter bit
            if ((state & WAITER_BIT) == 0) {
                _ = self.state.cmpxchgWeak(state, state | WAITER_BIT, .acq_rel, .acquire);
            }

            // Wait on futex (using state value, not just count)
            const current = self.state.load(.acquire);
            if ((current & COUNT_MASK) == 0) return;

            Futex.wait(&self.state, current);
        }
    }

    /// Wait with timeout. Returns true if count reached zero, false on timeout.
    pub fn waitTimeout(self: *CountLatch, timeout_ns: u64) bool {
        const deadline = std.time.nanoTimestamp() + @as(i128, timeout_ns);
        var spinner = SpinWait{};

        while (true) {
            const state = self.state.load(.acquire);
            const count = state & COUNT_MASK;

            if (count == 0) return true;

            const now = std.time.nanoTimestamp();
            if (now >= deadline) return false;

            if (!spinner.wouldYield()) {
                spinner.spin();
                continue;
            }

            // Set waiter bit
            if ((state & WAITER_BIT) == 0) {
                _ = self.state.cmpxchgWeak(state, state | WAITER_BIT, .acq_rel, .acquire);
            }

            const current = self.state.load(.acquire);
            if ((current & COUNT_MASK) == 0) return true;

            const remaining = @as(u64, @intCast(deadline - now));
            Futex.timedWait(&self.state, current, remaining) catch |err| {
                if (err == error.Timeout) {
                    return (self.state.load(.acquire) & COUNT_MASK) == 0;
                }
            };
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "SpinWait - basic spinning" {
    var sw = SpinWait{};

    // First few iterations should not yield
    try std.testing.expect(!sw.wouldYield());

    // Spin a few times
    for (0..5) |_| {
        sw.spin();
    }

    // After spin limit, should yield
    for (0..5) |_| {
        sw.spin();
    }
    try std.testing.expect(sw.wouldYield());

    // Reset
    sw.reset();
    try std.testing.expect(!sw.wouldYield());
}

test "OnceLatch - basic usage" {
    var latch = OnceLatch.init();

    try std.testing.expect(!latch.isDone());

    latch.setDone();

    try std.testing.expect(latch.isDone());

    // Wait should return immediately
    latch.wait();
}

test "OnceLatch - concurrent wait and set" {
    var latch = OnceLatch.init();
    var done = std.atomic.Value(bool).init(false);

    // Spawn waiter thread
    const waiter = std.Thread.spawn(.{}, struct {
        fn run(l: *OnceLatch, d: *std.atomic.Value(bool)) void {
            l.wait();
            d.store(true, .release);
        }
    }.run, .{ &latch, &done }) catch unreachable;

    // Give waiter time to start waiting
    std.Thread.sleep(1_000_000); // 1ms

    // Set done
    latch.setDone();

    // Wait for waiter to complete
    waiter.join();

    try std.testing.expect(done.load(.acquire));
}

test "CountLatch - basic countdown" {
    var latch = CountLatch.init(3);

    try std.testing.expectEqual(@as(u32, 3), latch.getCount());
    try std.testing.expect(!latch.isDone());

    latch.countDown();
    try std.testing.expectEqual(@as(u32, 2), latch.getCount());

    latch.countDown();
    try std.testing.expectEqual(@as(u32, 1), latch.getCount());

    latch.countDown();
    try std.testing.expectEqual(@as(u32, 0), latch.getCount());
    try std.testing.expect(latch.isDone());
}

test "CountLatch - concurrent countdown" {
    var latch = CountLatch.init(4);
    var completed = std.atomic.Value(u32).init(0);

    // Spawn 4 threads that count down
    var threads: [4]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, struct {
            fn run(l: *CountLatch, c: *std.atomic.Value(u32)) void {
                std.Thread.sleep(100_000); // Small delay
                l.countDown();
                _ = c.fetchAdd(1, .acq_rel);
            }
        }.run, .{ &latch, &completed }) catch unreachable;
    }

    // Wait for all to complete
    latch.wait();

    // Join threads
    for (&threads) |*t| {
        t.join();
    }

    try std.testing.expectEqual(@as(u32, 4), completed.load(.acquire));
    try std.testing.expect(latch.isDone());
}

test "CountLatch - countUp" {
    var latch = CountLatch.init(1);

    latch.countUp();
    try std.testing.expectEqual(@as(u32, 2), latch.getCount());

    latch.countDown();
    latch.countDown();
    try std.testing.expect(latch.isDone());
}
