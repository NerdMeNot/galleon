//! Sleep Management for Blitz
//!
//! Manages idle worker thread parking and wakeup using futex-based
//! synchronization. Workers go to sleep when there's no work available
//! and are woken up when new work is injected.

const std = @import("std");
const Futex = std.Thread.Futex;
const latch = @import("latch.zig");
const SpinWait = latch.SpinWait;

/// Sleep state values
const AWAKE: u32 = 0;
const SLEEPING: u32 = 1;
const NOTIFIED: u32 = 2;

/// Manages the sleep state of a single worker.
pub const WorkerSleep = struct {
    state: std.atomic.Value(u32) = std.atomic.Value(u32).init(AWAKE),

    /// Attempt to sleep. Returns false if notified before sleeping.
    pub fn sleep(self: *WorkerSleep) void {
        // Try to transition from AWAKE to SLEEPING
        const expected: u32 = AWAKE;
        if (self.state.cmpxchgStrong(expected, SLEEPING, .acq_rel, .acquire)) |actual| {
            // Was notified before we could sleep
            if (actual == NOTIFIED) {
                // Consume the notification
                self.state.store(AWAKE, .release);
                return;
            }
        }

        // Wait on futex
        while (self.state.load(.acquire) == SLEEPING) {
            Futex.wait(&self.state, SLEEPING);
        }

        // Consume notification
        self.state.store(AWAKE, .release);
    }

    /// Attempt to sleep with timeout. Returns true if woken, false on timeout.
    pub fn sleepTimeout(self: *WorkerSleep, timeout_ns: u64) bool {
        // Try to transition from AWAKE to SLEEPING
        const expected: u32 = AWAKE;
        if (self.state.cmpxchgStrong(expected, SLEEPING, .acq_rel, .acquire)) |actual| {
            if (actual == NOTIFIED) {
                self.state.store(AWAKE, .release);
                return true;
            }
        }

        // Wait with timeout
        Futex.timedWait(&self.state, SLEEPING, timeout_ns) catch |err| {
            if (err == error.Timeout) {
                // Try to transition back to AWAKE
                const state = self.state.swap(AWAKE, .acq_rel);
                return state == NOTIFIED;
            }
        };

        self.state.store(AWAKE, .release);
        return true;
    }

    /// Wake a sleeping worker.
    pub fn wake(self: *WorkerSleep) void {
        const prev = self.state.swap(NOTIFIED, .acq_rel);
        if (prev == SLEEPING) {
            Futex.wake(&self.state, 1);
        }
    }

    /// Check if worker is sleeping.
    pub fn isSleeping(self: *const WorkerSleep) bool {
        return self.state.load(.acquire) == SLEEPING;
    }

    /// Reset to awake state.
    pub fn reset(self: *WorkerSleep) void {
        self.state.store(AWAKE, .release);
    }
};

/// Global sleep manager that coordinates worker sleeping/waking.
pub const SleepManager = struct {
    /// Number of workers currently sleeping
    sleeping_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Generation counter to prevent spurious wakeups
    generation: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Maximum number of workers
    max_workers: u32,

    pub fn init(max_workers: u32) SleepManager {
        return .{
            .max_workers = max_workers,
        };
    }

    /// Record that a worker is going to sleep.
    pub fn workerSleeping(self: *SleepManager) void {
        _ = self.sleeping_count.fetchAdd(1, .acq_rel);
    }

    /// Record that a worker has woken up.
    pub fn workerAwake(self: *SleepManager) void {
        _ = self.sleeping_count.fetchSub(1, .acq_rel);
    }

    /// Get the number of sleeping workers.
    pub fn getSleepingCount(self: *const SleepManager) u32 {
        return self.sleeping_count.load(.acquire);
    }

    /// Get the number of active (non-sleeping) workers.
    pub fn getActiveCount(self: *const SleepManager) u32 {
        const sleeping = self.sleeping_count.load(.acquire);
        return if (sleeping >= self.max_workers) 0 else self.max_workers - sleeping;
    }

    /// Check if all workers are sleeping (deadlock detection).
    pub fn allAsleep(self: *const SleepManager) bool {
        return self.sleeping_count.load(.acquire) >= self.max_workers;
    }

    /// Increment generation (called when new work is available).
    pub fn notifyNewWork(self: *SleepManager) void {
        _ = self.generation.fetchAdd(1, .acq_rel);
    }

    /// Get current generation.
    pub fn getGeneration(self: *const SleepManager) u64 {
        return self.generation.load(.acquire);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "WorkerSleep - basic wake" {
    var ws = WorkerSleep{};

    // Should be awake initially
    try std.testing.expect(!ws.isSleeping());

    // Wake without sleeping (should be a no-op)
    ws.wake();
}

test "WorkerSleep - sleep and wake" {
    var ws = WorkerSleep{};
    var woken = std.atomic.Value(bool).init(false);

    // Spawn sleeper thread
    const sleeper = std.Thread.spawn(.{}, struct {
        fn run(sleep_state: *WorkerSleep, flag: *std.atomic.Value(bool)) void {
            sleep_state.sleep();
            flag.store(true, .release);
        }
    }.run, .{ &ws, &woken }) catch unreachable;

    // Give sleeper time to sleep
    std.Thread.sleep(1_000_000); // 1ms

    // Wake the sleeper
    ws.wake();

    sleeper.join();
    try std.testing.expect(woken.load(.acquire));
}

test "WorkerSleep - wake before sleep" {
    var ws = WorkerSleep{};

    // Wake before sleeping
    ws.wake();

    // Sleep should return immediately (notification consumed)
    ws.sleep();

    try std.testing.expect(!ws.isSleeping());
}

test "SleepManager - counting" {
    var sm = SleepManager.init(4);

    try std.testing.expectEqual(@as(u32, 0), sm.getSleepingCount());
    try std.testing.expectEqual(@as(u32, 4), sm.getActiveCount());
    try std.testing.expect(!sm.allAsleep());

    sm.workerSleeping();
    try std.testing.expectEqual(@as(u32, 1), sm.getSleepingCount());
    try std.testing.expectEqual(@as(u32, 3), sm.getActiveCount());

    sm.workerSleeping();
    sm.workerSleeping();
    sm.workerSleeping();
    try std.testing.expect(sm.allAsleep());

    sm.workerAwake();
    try std.testing.expect(!sm.allAsleep());
    try std.testing.expectEqual(@as(u32, 3), sm.getSleepingCount());
}

test "SleepManager - generation" {
    var sm = SleepManager.init(4);

    try std.testing.expectEqual(@as(u64, 0), sm.getGeneration());

    sm.notifyNewWork();
    try std.testing.expectEqual(@as(u64, 1), sm.getGeneration());

    sm.notifyNewWork();
    sm.notifyNewWork();
    try std.testing.expectEqual(@as(u64, 3), sm.getGeneration());
}
