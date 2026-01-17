//! ThreadPool with Hybrid Heartbeat + Active Stealing for Blitz
//!
//! Combines the best of Spice's heartbeat model and Rayon's active stealing:
//!
//! **From Spice (heartbeat scheduling):**
//! - Workers push/pop jobs locally (no synchronization)
//! - Heartbeat thread periodically requests work sharing
//! - Hot path is local linked-list ops + atomic bool load
//!
//! **From Rayon (active stealing):**
//! - Idle workers actively scan for work before sleeping
//! - Immediate work visibility when idle workers are waiting
//! - No delay waiting for heartbeat when workers are available
//!
//! Result: Low overhead on hot path + responsive work distribution.

const std = @import("std");
const Job = @import("job.zig").Job;
const JobExecuteState = @import("job.zig").JobExecuteState;
const Worker = @import("worker.zig").Worker;
const Task = @import("worker.zig").Task;

/// Configuration for the thread pool.
pub const ThreadPoolConfig = struct {
    /// Number of background workers. null = auto-detect (cores - 1).
    background_worker_count: ?usize = null,

    /// How often each worker is heartbeat (in nanoseconds).
    /// Default: 10 microseconds (faster work distribution for general-purpose use).
    /// For embedded/specialized use, can increase to 100μs.
    heartbeat_interval: usize = 10 * std.time.ns_per_us,
};

/// Thread pool with heartbeat-based work stealing.
pub const ThreadPool = struct {
    allocator: std.mem.Allocator,

    /// Mutex for coordinating workers. Only held during heartbeat, never during execution.
    mutex: std.Thread.Mutex = .{},

    /// List of all registered workers (both background and on-demand).
    workers: std.ArrayListUnmanaged(*Worker) = .{},

    /// Background worker threads.
    background_threads: std.ArrayListUnmanaged(std.Thread) = .{},

    /// The heartbeat thread.
    heartbeat_thread: ?std.Thread = null,

    /// Condition variable: signaled when new jobs are ready.
    job_ready: std.Thread.Condition = .{},

    /// Semaphore: workers signal when they're ready.
    workers_ready: std.Thread.Semaphore = .{},

    /// Set to true during shutdown.
    is_stopping: bool = false,

    /// Monotonic timer for job priority (older = higher priority).
    time: usize = 0,

    /// Heartbeat interval (nanoseconds).
    heartbeat_interval: usize = 10 * std.time.ns_per_us,

    /// Number of idle workers waiting for work (for active stealing).
    /// When > 0, workers should share work immediately instead of waiting for heartbeat.
    idle_workers: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    /// Create an empty thread pool (for testing).
    pub fn initEmpty(allocator: std.mem.Allocator) ThreadPool {
        return ThreadPool{
            .allocator = allocator,
        };
    }

    /// Initialize the thread pool.
    pub fn init(allocator: std.mem.Allocator) ThreadPool {
        return ThreadPool{
            .allocator = allocator,
        };
    }

    /// Start the thread pool with the given configuration.
    pub fn start(self: *ThreadPool, config: ThreadPoolConfig) void {
        const actual_count = config.background_worker_count orelse blk: {
            const cpus = std.Thread.getCpuCount() catch 1;
            break :blk if (cpus > 1) cpus - 1 else 1;
        };

        self.heartbeat_interval = config.heartbeat_interval;
        self.background_threads.ensureUnusedCapacity(self.allocator, actual_count) catch @panic("OOM");
        self.workers.ensureUnusedCapacity(self.allocator, actual_count + 4) catch @panic("OOM");

        // Spawn background workers
        for (0..actual_count) |_| {
            const thread = std.Thread.spawn(.{}, backgroundWorker, .{self}) catch @panic("spawn error");
            self.background_threads.append(self.allocator, thread) catch @panic("OOM");
        }

        // Spawn heartbeat thread
        self.heartbeat_thread = std.Thread.spawn(.{}, heartbeatWorker, .{self}) catch @panic("spawn error");

        // Wait for all workers to be ready
        for (0..actual_count) |_| {
            self.workers_ready.wait();
        }
    }

    /// Shut down the thread pool.
    pub fn deinit(self: *ThreadPool) void {
        // Signal stop
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.is_stopping = true;
            self.job_ready.broadcast();
        }

        // Wait for background workers
        for (self.background_threads.items) |thread| {
            thread.join();
        }

        // Wait for heartbeat thread
        if (self.heartbeat_thread) |thread| {
            thread.join();
        }

        // Clean up
        self.background_threads.deinit(self.allocator);
        self.workers.deinit(self.allocator);
        self.* = undefined;
    }

    /// Execute a function on the thread pool.
    /// Creates a temporary worker for the calling thread.
    pub fn call(self: *ThreadPool, comptime T: type, func: anytype, arg: anytype) T {
        // Create a one-off worker for this thread
        var worker = Worker{ .pool = self };
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.workers.append(self.allocator, &worker) catch @panic("OOM");
        }

        defer {
            self.mutex.lock();
            defer self.mutex.unlock();
            // Remove ourselves from the workers list
            for (self.workers.items, 0..) |w, idx| {
                if (w == &worker) {
                    _ = self.workers.swapRemove(idx);
                    break;
                }
            }
        }

        var t = worker.begin();
        return t.call(T, func, arg);
    }

    /// Heartbeat handler - called by workers when heartbeat flag is set.
    /// This is the cold path for sharing work.
    pub fn heartbeat(self: *ThreadPool, worker: *Worker) void {
        @branchHint(.cold);

        self.mutex.lock();
        defer self.mutex.unlock();

        if (worker.shared_job == null) {
            // Try to share the oldest job
            if (worker.job_head.shift()) |job| {
                // Allocate execute state
                const exec_state = self.allocator.create(JobExecuteState) catch @panic("OOM");
                exec_state.* = .{ .result = undefined };
                job.setExecuteState(exec_state);

                worker.shared_job = job;
                worker.job_time = self.time;
                self.time += 1;

                // Signal waiting workers
                self.job_ready.signal();
            }
        }

        worker.heartbeat.store(false, .monotonic);
        worker.stats.heartbeats += 1;
    }

    /// Wait for a job to complete.
    /// Returns false if the job was not actually started (still ours).
    pub fn waitForJob(self: *ThreadPool, worker: *Worker, job: *Job) bool {
        const exec_state = job.getExecuteState();

        {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (worker.shared_job == job) {
                // Job wasn't picked up yet - reclaim it
                worker.shared_job = null;
                self.allocator.destroy(exec_state);
                return false;
            }

            // Help by executing other jobs while waiting
            while (!exec_state.done.isSet()) {
                if (self.popReadyJob()) |other_job| {
                    self.mutex.unlock();
                    defer self.mutex.lock();
                    worker.executeJob(other_job);
                } else {
                    break;
                }
            }
        }

        // Wait outside the lock
        exec_state.done.wait();
        return true;
    }

    /// Destroy an execute state, returning it to the pool.
    pub fn destroyExecuteState(self: *ThreadPool, exec_state: *JobExecuteState) void {
        self.allocator.destroy(exec_state);
    }

    /// Find and pop a ready job (oldest first).
    fn popReadyJob(self: *ThreadPool) ?*Job {
        var best_worker: ?*Worker = null;

        for (self.workers.items) |worker| {
            if (worker.shared_job) |_| {
                if (best_worker) |best| {
                    // Pick older job
                    if (worker.job_time < best.job_time) {
                        best_worker = worker;
                    }
                } else {
                    best_worker = worker;
                }
            }
        }

        if (best_worker) |worker| {
            defer worker.shared_job = null;
            return worker.shared_job;
        }

        return null;
    }

    /// Get the number of workers.
    pub fn numWorkers(self: *ThreadPool) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.workers.items.len;
    }
};

/// Background worker thread function.
/// Tracks idle state for immediate sharing optimization.
fn backgroundWorker(pool: *ThreadPool) void {
    var worker = Worker{ .pool = pool };
    var first = true;

    pool.mutex.lock();
    defer pool.mutex.unlock();

    pool.workers.append(pool.allocator, &worker) catch @panic("OOM");

    while (true) {
        if (pool.is_stopping) break;

        if (pool.popReadyJob()) |job| {
            // Release lock while executing
            pool.mutex.unlock();
            defer pool.mutex.lock();
            worker.executeJob(job);
            continue;
        }

        if (first) {
            pool.workers_ready.post();
            first = false;
        }

        // Track idle state for immediate sharing optimization (Rayon-style)
        _ = pool.idle_workers.fetchAdd(1, .release);
        defer _ = pool.idle_workers.fetchSub(1, .release);

        pool.job_ready.wait(&pool.mutex);
    }
}

/// Heartbeat thread function.
/// Sets heartbeat flag on one worker at a time, cycling through all workers.
fn heartbeatWorker(pool: *ThreadPool) void {
    var i: usize = 0;

    while (true) {
        var to_sleep: u64 = pool.heartbeat_interval;

        {
            pool.mutex.lock();
            defer pool.mutex.unlock();

            if (pool.is_stopping) break;

            const workers = pool.workers.items;
            if (workers.len > 0) {
                i = i % workers.len;
                workers[i].heartbeat.store(true, .monotonic);
                i += 1;
                // Spread heartbeat interval across all workers
                to_sleep = pool.heartbeat_interval / workers.len;
            }
        }

        std.Thread.sleep(to_sleep);
    }
}

// ============================================================================
// Tests
// ============================================================================

test "ThreadPool - initEmpty" {
    var pool = ThreadPool.initEmpty(std.testing.allocator);
    defer pool.deinit();

    try std.testing.expect(!pool.is_stopping);
}

test "ThreadPool - init and deinit" {
    var pool = ThreadPool.init(std.testing.allocator);
    pool.start(.{ .background_worker_count = 2 });
    defer pool.deinit();

    try std.testing.expect(pool.background_threads.items.len == 2);
}

test "ThreadPool - call" {
    var pool = ThreadPool.init(std.testing.allocator);
    pool.start(.{ .background_worker_count = 2 });
    defer pool.deinit();

    const result = pool.call(i32, struct {
        fn compute(task: *Task, x: i32) i32 {
            _ = task;
            return x * 2;
        }
    }.compute, 21);

    try std.testing.expectEqual(@as(i32, 42), result);
}
