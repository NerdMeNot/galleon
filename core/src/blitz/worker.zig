//! Worker and Task for Blitz2
//!
//! Each worker has a local job queue (branch-free linked list) and runs tasks.
//! The Task is the handle passed to user functions for spawning sub-tasks.
//!
//! Key insight: All local queue operations are non-atomic. Only the heartbeat
//! flag and shared_job require synchronization.

const std = @import("std");
const Job = @import("job.zig").Job;
const JobExecuteState = @import("job.zig").JobExecuteState;

// Forward declaration for ThreadPool
pub const ThreadPool = @import("pool.zig").ThreadPool;

/// Worker statistics for debugging/monitoring.
pub const WorkerStats = struct {
    jobs_executed: u64 = 0,
    jobs_stolen: u64 = 0,
    heartbeats: u64 = 0,
};

/// Per-worker state.
///
/// Each worker has:
/// - A local job queue (sentinel head + tail pointer in Task)
/// - A shared_job that other workers can steal
/// - A heartbeat flag set by the heartbeat thread
pub const Worker = struct {
    /// Reference to the thread pool.
    pool: *ThreadPool,

    /// Sentinel head of the local job queue.
    /// Jobs are pushed at the tail, popped from tail (LIFO).
    /// The heartbeat shifts jobs from head (FIFO) to share with others.
    job_head: Job = Job.head(),

    /// A job (in executing state) that other workers can pick up.
    /// Set by heartbeat when we have work to share.
    shared_job: ?*Job = null,

    /// The time when the job was shared (for prioritizing oldest).
    job_time: usize = 0,

    /// Heartbeat flag. Set to true by heartbeat thread to request sharing.
    /// Checked on every t.call() - this is the hot path.
    heartbeat: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),

    /// Statistics.
    stats: WorkerStats = .{},

    /// Start executing work on this worker.
    /// Returns a Task that can be used for spawning sub-tasks.
    pub fn begin(self: *Worker) Task {
        std.debug.assert(self.job_head.isTail());

        return Task{
            .worker = self,
            .job_tail = &self.job_head,
        };
    }

    /// Execute a job that was stolen from another worker.
    pub fn executeJob(self: *Worker, job: *Job) void {
        var t = self.begin();
        job.handler.?(&t, job);
        self.stats.jobs_executed += 1;
    }
};

/// Task is the handle passed to user functions.
///
/// Size: 16 bytes (2 pointers) - small enough to pass by value.
///
/// The task allows:
/// - tick(): Check if heartbeat wants us to share work
/// - call(): Execute a function (with implicit tick)
/// - fork()/join(): Parallel execution via Future
pub const Task = struct {
    /// The worker we're running on.
    worker: *Worker,

    /// Pointer to the tail of the local job queue.
    /// Updated by push/pop operations.
    job_tail: *Job,

    /// Check heartbeat and potentially share work.
    /// This is the hot path - just an atomic load (~3ns).
    /// Only if the flag is set do we take the cold path.
    pub inline fn tick(self: *Task) void {
        if (self.worker.heartbeat.load(.monotonic)) {
            self.worker.pool.heartbeat(self.worker);
        }
    }

    /// Execute a function, returning its result.
    /// Implicitly calls tick() to check for heartbeat.
    pub inline fn call(self: *Task, comptime T: type, func: anytype, arg: anytype) T {
        return callWithContext(
            self.worker,
            self.job_tail,
            T,
            func,
            arg,
        );
    }
};

/// Internal call implementation.
///
/// This function signature is critical for performance. We take worker and job_tail
/// as parameters (rather than a Task struct) because LLVM is better at keeping
/// parameters in registers than struct fields.
fn callWithContext(
    worker: *Worker,
    job_tail: *Job,
    comptime T: type,
    func: anytype,
    arg: anytype,
) T {
    var t = Task{
        .worker = worker,
        .job_tail = job_tail,
    };
    t.tick();
    return @call(.always_inline, func, .{
        &t,
        arg,
    });
}

// ============================================================================
// Tests
// ============================================================================

test "Worker - begin creates valid task" {
    // We need a minimal pool for this test
    const pool = @import("pool.zig");
    var tp = pool.ThreadPool.initEmpty(std.testing.allocator);
    defer tp.deinit();

    var worker = Worker{ .pool = &tp };
    const task = worker.begin();

    try std.testing.expect(task.worker == &worker);
    try std.testing.expect(task.job_tail == &worker.job_head);
    try std.testing.expect(worker.job_head.isTail());
}

test "Task - tick with no heartbeat" {
    const pool = @import("pool.zig");
    var tp = pool.ThreadPool.initEmpty(std.testing.allocator);
    defer tp.deinit();

    var worker = Worker{ .pool = &tp };
    worker.heartbeat.store(false, .monotonic);

    var task = worker.begin();

    // tick() should be essentially a no-op when heartbeat is false
    task.tick();
    // Just verify it doesn't crash
}
