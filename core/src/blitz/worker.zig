//! Worker Thread for Blitz
//!
//! Each worker has its own work-stealing deque and runs a work loop that:
//! 1. Pops from local deque (LIFO - cache friendly)
//! 2. Steals from other workers (FIFO - fairness)
//! 3. Steals from global injector queue
//! 4. Sleeps when no work is available

const std = @import("std");
const Deque = @import("deque.zig").Deque;
const StealResult = @import("deque.zig").StealResult;
const Job = @import("job.zig").Job;
const sleep_mod = @import("sleep.zig");
const WorkerSleep = sleep_mod.WorkerSleep;
const SleepManager = sleep_mod.SleepManager;
const latch = @import("latch.zig");
const SpinWait = latch.SpinWait;

/// Worker state
pub const WorkerState = enum(u8) {
    /// Actively executing tasks
    running,
    /// Looking for work (spinning/stealing)
    searching,
    /// Parked, waiting for wakeup
    sleeping,
    /// Shutdown complete
    terminated,
};

/// Worker statistics (for debugging/monitoring)
pub const WorkerStats = struct {
    jobs_executed: u64 = 0,
    jobs_stolen: u64 = 0,
    steal_attempts: u64 = 0,
    sleep_count: u64 = 0,
};

/// Per-worker state
pub const Worker = struct {
    /// Worker ID (0 to num_workers - 1)
    id: u32,

    /// Local work-stealing deque
    deque: Deque(*Job),

    /// Current state
    state: std.atomic.Value(WorkerState),

    /// Sleep management
    sleep_state: WorkerSleep,

    /// Thread handle
    thread: ?std.Thread,

    /// Random state for victim selection (XorShift)
    rng_state: u64,

    /// Statistics
    stats: WorkerStats,

    /// Allocator
    allocator: std.mem.Allocator,

    // Registry reference is passed to run() to avoid circular dependency

    /// Initialize a worker (allocates on heap)
    pub fn init(id: u32, allocator: std.mem.Allocator) !*Worker {
        const self = try allocator.create(Worker);
        errdefer allocator.destroy(self);

        self.* = Worker{
            .id = id,
            .deque = try Deque(*Job).init(allocator),
            .state = std.atomic.Value(WorkerState).init(.running),
            .sleep_state = WorkerSleep{},
            .thread = null,
            .rng_state = @as(u64, id) *% 0x9E3779B97F4A7C15 +% 1, // Golden ratio hash
            .stats = WorkerStats{},
            .allocator = allocator,
        };
        return self;
    }

    /// Deinitialize a worker
    pub fn deinit(self: *Worker) void {
        self.deque.deinit();
    }

    /// Push a job onto the local deque
    pub fn push(self: *Worker, job: *Job) !void {
        try self.deque.push(job);
    }

    /// Pop a job from the local deque
    pub fn pop(self: *Worker) ?*Job {
        return self.deque.pop();
    }

    /// Get next random victim index for stealing
    pub fn randomVictim(self: *Worker, num_workers: u32) u32 {
        // XorShift64
        var x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        return @intCast(x % num_workers);
    }

    /// Set worker state
    pub fn setState(self: *Worker, new_state: WorkerState) void {
        self.state.store(new_state, .release);
    }

    /// Get worker state
    pub fn getState(self: *const Worker) WorkerState {
        return self.state.load(.acquire);
    }

    /// Sleep until woken
    pub fn sleep(self: *Worker, sleep_mgr: *SleepManager) void {
        self.stats.sleep_count += 1;
        sleep_mgr.workerSleeping();
        self.setState(.sleeping);
        self.sleep_state.sleep();
        sleep_mgr.workerAwake();
    }

    /// Wake this worker
    pub fn wake(self: *Worker) void {
        self.sleep_state.wake();
    }

    /// Check if sleeping
    pub fn isSleeping(self: *const Worker) bool {
        return self.state.load(.acquire) == .sleeping;
    }
};

/// Work loop context (passed to worker thread)
pub const WorkLoopContext = struct {
    worker: *Worker,
    workers: []const *Worker,
    num_workers: u32,
    injector: *anyopaque, // Injector queue (opaque to avoid circular dep)
    injector_steal_fn: *const fn (*anyopaque) ?*Job,
    sleep_mgr: *SleepManager,
    shutdown: *std.atomic.Value(bool),
};

/// Main work loop for a worker thread
pub fn workLoop(ctx: WorkLoopContext) void {
    const worker = ctx.worker;
    var spinner = SpinWait{};
    var search_count: u32 = 0;
    const MAX_SEARCH_ROUNDS: u32 = 32;

    while (!ctx.shutdown.load(.acquire)) {
        // 1. Try local deque (LIFO)
        if (worker.pop()) |job| {
            worker.setState(.running);
            job.execute();
            worker.stats.jobs_executed += 1;
            spinner.reset();
            search_count = 0;
            continue;
        }

        // 2. Try stealing from other workers
        worker.setState(.searching);
        if (tryStealFromWorkers(worker, ctx.workers, ctx.num_workers)) |job| {
            worker.setState(.running);
            job.execute();
            worker.stats.jobs_executed += 1;
            worker.stats.jobs_stolen += 1;
            spinner.reset();
            search_count = 0;
            continue;
        }

        // 3. Try global injector queue
        if (ctx.injector_steal_fn(ctx.injector)) |job| {
            worker.setState(.running);
            job.execute();
            worker.stats.jobs_executed += 1;
            spinner.reset();
            search_count = 0;
            continue;
        }

        // 4. No work found
        search_count += 1;

        if (search_count < MAX_SEARCH_ROUNDS) {
            // Spin a bit before sleeping
            spinner.spin();
        } else {
            // Go to sleep
            worker.sleep(ctx.sleep_mgr);
            spinner.reset();
            search_count = 0;
        }
    }

    worker.setState(.terminated);
}

/// Try to steal from other workers
fn tryStealFromWorkers(self: *Worker, workers: []const *Worker, num_workers: u32) ?*Job {
    if (num_workers <= 1) return null;

    // Start from random victim
    const start = self.randomVictim(num_workers);

    var i: u32 = 0;
    while (i < num_workers) : (i += 1) {
        const victim_id = (start + i) % num_workers;

        // Don't steal from ourselves
        if (victim_id == self.id) continue;

        self.stats.steal_attempts += 1;

        const victim = workers[victim_id];
        switch (victim.deque.steal()) {
            .success => |job| return job,
            .empty => continue,
            .retry => {
                // Retry this victim once
                switch (victim.deque.steal()) {
                    .success => |job| return job,
                    else => continue,
                }
            },
        }
    }

    return null;
}

// ============================================================================
// Tests
// ============================================================================

test "Worker - basic init/deinit" {
    const worker = try Worker.init(0, std.testing.allocator);
    defer {
        worker.deinit();
        std.testing.allocator.destroy(worker);
    }

    try std.testing.expectEqual(@as(u32, 0), worker.id);
    try std.testing.expectEqual(WorkerState.running, worker.getState());
}

test "Worker - push/pop" {
    const worker = try Worker.init(0, std.testing.allocator);
    defer {
        worker.deinit();
        std.testing.allocator.destroy(worker);
    }

    const TestCtx = struct {
        value: u32,
    };

    var ctx = TestCtx{ .value = 42 };
    var job = Job.from(TestCtx, &ctx, struct {
        fn exec(c: *TestCtx) void {
            c.value *= 2;
        }
    }.exec);

    try worker.push(&job);

    const popped = worker.pop();
    try std.testing.expect(popped != null);
    try std.testing.expect(popped.? == &job);
}

test "Worker - random victim" {
    const worker = try Worker.init(0, std.testing.allocator);
    defer {
        worker.deinit();
        std.testing.allocator.destroy(worker);
    }

    // Generate some random victims and ensure they're in range
    var seen = [_]bool{false} ** 8;
    for (0..100) |_| {
        const victim = worker.randomVictim(8);
        try std.testing.expect(victim < 8);
        seen[victim] = true;
    }

    // Should have seen most victims
    var count: usize = 0;
    for (seen) |s| {
        if (s) count += 1;
    }
    try std.testing.expect(count >= 6); // Probabilistic, should see most
}

test "Worker - state transitions" {
    const worker = try Worker.init(0, std.testing.allocator);
    defer {
        worker.deinit();
        std.testing.allocator.destroy(worker);
    }

    try std.testing.expectEqual(WorkerState.running, worker.getState());

    worker.setState(.searching);
    try std.testing.expectEqual(WorkerState.searching, worker.getState());

    worker.setState(.sleeping);
    try std.testing.expectEqual(WorkerState.sleeping, worker.getState());

    worker.setState(.terminated);
    try std.testing.expectEqual(WorkerState.terminated, worker.getState());
}
