//! Synchronization Primitives for Blitz2
//!
//! Lightweight synchronization using futex for blocking.
//! Optimized for the heartbeat-based scheduler where most operations are local.

const std = @import("std");
const Futex = std.Thread.Futex;

// ============================================================================
// SpinWait - Adaptive Spinning
// ============================================================================

/// Adaptive spinning utility for short waits.
/// Spins with increasing backoff before yielding/sleeping.
pub const SpinWait = struct {
    iteration: u32 = 0,

    const SPIN_LIMIT: u32 = 10;
    const YIELD_LIMIT: u32 = 20;

    pub fn reset(self: *SpinWait) void {
        self.iteration = 0;
    }

    /// Spin once, potentially yielding based on iteration count.
    pub fn spin(self: *SpinWait) void {
        if (self.iteration < SPIN_LIMIT) {
            // Exponential backoff with pause instruction
            const shift: u5 = @intCast(@min(self.iteration, 31));
            var spin_count: u32 = @as(u32, 1) << shift;
            while (spin_count > 0) : (spin_count -= 1) {
                std.atomic.spinLoopHint();
            }
        } else if (self.iteration < YIELD_LIMIT) {
            std.Thread.yield() catch {};
        } else {
            std.Thread.sleep(1_000); // 1 microsecond
        }
        self.iteration +|= 1;
    }

    /// Returns true if the next spin would yield.
    pub fn wouldYield(self: *const SpinWait) bool {
        return self.iteration >= SPIN_LIMIT;
    }
};

// ============================================================================
// OnceLatch - Single-Shot Completion Signal
// ============================================================================

/// A single-shot latch that signals completion.
/// Once set, all waiters are released and future waits return immediately.
pub const OnceLatch = struct {
    const PENDING: u32 = 0;
    const DONE: u32 = 1;
    const WAITING: u32 = 2;

    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(PENDING),

    pub fn init() OnceLatch {
        return .{};
    }

    /// Check if done (non-blocking).
    pub inline fn isDone(self: *const OnceLatch) bool {
        return self.state.load(.acquire) == DONE;
    }

    /// Set to done and wake all waiters.
    pub fn setDone(self: *OnceLatch) void {
        const prev = self.state.swap(DONE, .acq_rel);
        if (prev == WAITING) {
            Futex.wake(&self.state, std.math.maxInt(u32));
        }
    }

    /// Wait until done.
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
                spinner.spin();
                continue;
            }

            Futex.wait(&self.state, WAITING);

            if (self.state.load(.acquire) == DONE) return;
        }
    }
};

// ============================================================================
// CountLatch - Countdown Latch
// ============================================================================

/// A countdown latch that blocks until count reaches zero.
/// Used for fork-join where we wait for N tasks.
pub const CountLatch = struct {
    const WAITER_BIT: u32 = 1 << 31;
    const COUNT_MASK: u32 = ~WAITER_BIT;

    state: std.atomic.Value(u32),

    pub fn init(count: u32) CountLatch {
        std.debug.assert(count <= COUNT_MASK);
        return .{
            .state = std.atomic.Value(u32).init(count),
        };
    }

    /// Get current count.
    pub fn getCount(self: *const CountLatch) u32 {
        return self.state.load(.acquire) & COUNT_MASK;
    }

    /// Check if count is zero.
    pub inline fn isDone(self: *const CountLatch) bool {
        return self.getCount() == 0;
    }

    /// Decrement count. Wakes waiters when reaching zero.
    pub fn countDown(self: *CountLatch) void {
        const prev = self.state.fetchSub(1, .acq_rel);
        const prev_count = prev & COUNT_MASK;
        const had_waiters = (prev & WAITER_BIT) != 0;

        std.debug.assert(prev_count > 0);

        if (prev_count == 1 and had_waiters) {
            Futex.wake(&self.state, std.math.maxInt(u32));
        }
    }

    /// Increment count.
    pub fn countUp(self: *CountLatch) void {
        const prev = self.state.fetchAdd(1, .acq_rel);
        std.debug.assert((prev & COUNT_MASK) < COUNT_MASK);
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

            const current = self.state.load(.acquire);
            if ((current & COUNT_MASK) == 0) return;

            Futex.wait(&self.state, current);
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "SpinWait - basic" {
    var sw = SpinWait{};
    try std.testing.expect(!sw.wouldYield());

    for (0..10) |_| {
        sw.spin();
    }
    try std.testing.expect(sw.wouldYield());

    sw.reset();
    try std.testing.expect(!sw.wouldYield());
}

test "OnceLatch - basic" {
    var latch = OnceLatch.init();
    try std.testing.expect(!latch.isDone());

    latch.setDone();
    try std.testing.expect(latch.isDone());

    // Wait returns immediately
    latch.wait();
}

test "CountLatch - countdown" {
    var latch = CountLatch.init(3);

    try std.testing.expectEqual(@as(u32, 3), latch.getCount());
    try std.testing.expect(!latch.isDone());

    latch.countDown();
    latch.countDown();
    latch.countDown();

    try std.testing.expect(latch.isDone());
}
