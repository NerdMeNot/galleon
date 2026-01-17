//! Future for Blitz2
//!
//! Future(Input, Output) enables fork-join parallelism with return values.
//! The future is stack-allocated (~32 bytes) and uses comptime specialization
//! to eliminate vtable overhead.
//!
//! Usage:
//! ```zig
//! var future = Future(i32, i64).init();
//! future.fork(&task, compute, input_value);
//! // ... do other work ...
//! const result = future.join(&task) orelse compute(&task, input_value);
//! ```

const std = @import("std");
const Job = @import("job.zig").Job;
const JobExecuteState = @import("job.zig").JobExecuteState;
const Task = @import("worker.zig").Task;

/// A future represents a computation that may run on another thread.
///
/// Size: ~32 bytes (Job + Input) - stack-allocated in every frame.
pub fn Future(comptime Input: type, comptime Output: type) type {
    return struct {
        const Self = @This();

        /// The underlying job.
        job: Job,

        /// Input value to pass to the function.
        input: Input,

        /// Create a new pending future.
        pub inline fn init() Self {
            return Self{
                .job = Job.pending(),
                .input = undefined,
            };
        }

        /// Schedule work to potentially run on another thread.
        ///
        /// After calling fork(), you MUST call join() or tryJoin().
        /// The function signature must be: fn(*Task, Input) Output
        ///
        /// Hybrid optimization: If idle workers are waiting, immediately trigger
        /// work sharing (Rayon-style immediate visibility).
        pub inline fn fork(
            self: *Self,
            task: *Task,
            comptime func: fn (*Task, Input) Output,
            input: Input,
        ) void {
            const handler = struct {
                fn handler(t: *Task, job: *Job) void {
                    const fut: *Self = @fieldParentPtr("job", job);
                    const exec_state = job.getExecuteState();
                    const value = t.call(Output, func, fut.input);
                    exec_state.resultPtr(Output).* = value;
                    exec_state.done.set();
                }
            }.handler;

            self.input = input;
            self.job.push(&task.job_tail, handler);

            // Rayon-style immediate sharing: if idle workers are waiting,
            // share work now instead of waiting for heartbeat.
            // This adds ~3ns (atomic load) but enables much faster work distribution.
            if (task.worker.pool.idle_workers.load(.monotonic) > 0) {
                task.worker.pool.heartbeat(task.worker);
            }
        }

        /// Wait for the result of fork().
        ///
        /// Returns the computed value if the job was stolen and executed,
        /// or null if the job was not stolen (caller should execute locally).
        ///
        /// This is only safe to call if fork() was actually called.
        pub inline fn join(self: *Self, task: *Task) ?Output {
            std.debug.assert(self.job.state() != .pending);
            return self.tryJoin(task);
        }

        /// Wait for the result, safe to call even if fork() wasn't called.
        ///
        /// Returns:
        /// - null if pending (fork never called) or queued (not stolen, pop it)
        /// - Output value if executing (stolen and completed)
        pub inline fn tryJoin(self: *Self, task: *Task) ?Output {
            switch (self.job.state()) {
                .pending => return null,
                .queued => {
                    // Job wasn't stolen - pop it and execute locally
                    self.job.pop(&task.job_tail);
                    return null;
                },
                .executing => return self.joinExecuting(task),
            }
        }

        /// Cold path: wait for a stolen job to complete.
        fn joinExecuting(self: *Self, task: *Task) ?Output {
            @branchHint(.cold);

            const w = task.worker;
            const pool = w.pool;
            const exec_state = self.job.getExecuteState();

            if (pool.waitForJob(w, &self.job)) {
                const result = exec_state.resultPtr(Output).*;
                pool.destroyExecuteState(exec_state);
                return result;
            }

            return null;
        }
    };
}

/// Void future for when you don't need a return value.
pub const VoidFuture = Future(void, void);

// ============================================================================
// Tests
// ============================================================================

test "Future - basic state transitions" {
    var future = Future(i32, i64).init();
    try std.testing.expectEqual(Job.State.pending, future.job.state());
}

test "Future - input storage" {
    var future = Future(struct { x: i32, y: i32 }, i64).init();
    future.input = .{ .x = 10, .y = 20 };
    try std.testing.expectEqual(@as(i32, 10), future.input.x);
    try std.testing.expectEqual(@as(i32, 20), future.input.y);
}

test "Future - size is reasonable" {
    // Future should be small enough to stack-allocate
    const FutureType = Future(i32, i64);
    try std.testing.expect(@sizeOf(FutureType) <= 48);
}
