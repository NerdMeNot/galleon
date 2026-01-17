//! Branch-Free Job Queue for Blitz2
//!
//! Implements a doubly-linked list of jobs with three states:
//! - pending: Job not yet queued (handler = null)
//! - queued: Job in the local queue (prev_or_null != null)
//! - executing: Job being executed by another worker (prev_or_null = null)
//!
//! The key insight from Spice is that this queue is purely local - no atomics needed.
//! Push/pop are O(1) operations on the tail. Shift (for stealing) works from the head.
//! A sentinel head node eliminates null checks in the hot path.

const std = @import("std");
const Task = @import("worker.zig").Task;

/// Maximum size for return values stored in JobExecuteState.
/// 4 words (32 bytes) fits most return types including small structs.
const max_result_words = 4;

/// Job represents a unit of work that can potentially be executed on a different thread.
///
/// Jobs form a doubly-linked list with a sentinel head for branch-free operations.
/// Size: 24 bytes (3 pointers) - compact enough to be stack-allocated in every frame.
pub const Job = struct {
    /// Function to execute. null = pending state.
    handler: ?*const fn (*Task, *Job) void,

    /// In queued state: pointer to previous job
    /// In executing state: null (distinguishes from queued)
    /// In pending state: undefined
    prev_or_null: ?*anyopaque,

    /// In queued state: pointer to next job (null if tail)
    /// In executing state: pointer to JobExecuteState
    /// In pending state: undefined
    next_or_state: ?*anyopaque,

    /// Job state enumeration
    pub const State = enum {
        pending,
        queued,
        executing,
    };

    /// Create a sentinel head node for starting a job queue.
    /// The head is special: it's never executed, just marks the start.
    pub fn head() Job {
        return Job{
            .handler = undefined, // Not used for head
            .prev_or_null = null, // No previous (we are the head)
            .next_or_state = null, // No next yet (we are also the tail)
        };
    }

    /// Create a pending job (not yet queued).
    pub fn pending() Job {
        return Job{
            .handler = null,
            .prev_or_null = undefined,
            .next_or_state = undefined,
        };
    }

    /// Get the current state of this job.
    pub fn state(self: Job) State {
        if (self.handler == null) return .pending;
        if (self.prev_or_null != null) return .queued;
        return .executing;
    }

    /// Check if this job is the tail of the queue (next is null).
    pub fn isTail(self: Job) bool {
        return self.next_or_state == null;
    }

    /// Get the execute state for an executing job.
    /// Caller must ensure job is in executing state.
    pub fn getExecuteState(self: *Job) *JobExecuteState {
        std.debug.assert(self.state() == .executing);
        return @ptrCast(@alignCast(self.next_or_state));
    }

    /// Set the execute state for an executing job.
    pub fn setExecuteState(self: *Job, execute_state: *JobExecuteState) void {
        std.debug.assert(self.state() == .executing);
        self.next_or_state = execute_state;
    }

    /// Push this job onto the queue (append at tail).
    /// This is the hot path - no branches, no atomics.
    ///
    /// Before: head -> ... -> tail (where tail.next = null)
    /// After:  head -> ... -> tail -> self (where self.next = null)
    pub fn push(self: *Job, tail: **Job, handler: *const fn (*Task, *Job) void) void {
        std.debug.assert(self.state() == .pending);

        self.handler = handler;
        tail.*.next_or_state = self; // tail.next = self
        self.prev_or_null = tail.*; // self.prev = tail
        self.next_or_state = null; // self.next = null (new tail)
        tail.* = self; // Update tail pointer

        std.debug.assert(self.state() == .queued);
    }

    /// Pop this job from the queue (remove from tail).
    /// This is called when we want to execute a job locally instead of letting it be stolen.
    ///
    /// Before: ... -> prev -> self (where self = tail)
    /// After:  ... -> prev (where prev = new tail)
    pub fn pop(self: *Job, tail: **Job) void {
        std.debug.assert(self.state() == .queued);
        std.debug.assert(tail.* == self);

        const prev: *Job = @ptrCast(@alignCast(self.prev_or_null));
        prev.next_or_state = null; // prev.next = null (prev is new tail)
        tail.* = prev; // Update tail pointer

        self.* = undefined; // Clear for safety
    }

    /// Shift the oldest job from the queue (remove from head).
    /// This is used by the heartbeat to find a job to share.
    ///
    /// Before: head -> job -> next -> ... -> tail
    /// After:  head -> next -> ... -> tail (job is returned in executing state)
    ///
    /// Returns null if:
    /// - Queue is empty (head.next = null)
    /// - Only one job (can't remove because we can't update tail)
    pub fn shift(self: *Job) ?*Job {
        const job: *Job = @as(?*Job, @ptrCast(@alignCast(self.next_or_state))) orelse return null;

        std.debug.assert(job.state() == .queued);

        // Get the job after the one we're removing
        const next: ?*Job = @ptrCast(@alignCast(job.next_or_state));

        // Can't remove if job is the tail (would need to update tail pointer)
        if (next == null) return null;

        // Unlink job from the list
        next.?.prev_or_null = self; // next.prev = head
        self.next_or_state = next; // head.next = next

        // Transition job to executing state
        job.prev_or_null = null; // Mark as executing (not queued)
        job.next_or_state = undefined; // Will be set to JobExecuteState

        std.debug.assert(job.state() == .executing);
        return job;
    }
};

/// State for a job that's being executed by another worker.
/// This is allocated from a pool and holds the return value.
pub const JobExecuteState = struct {
    /// Signaled when execution completes.
    done: std.Thread.ResetEvent = .{},

    /// Storage for the return value.
    result: ResultType,

    const ResultType = [max_result_words]u64;

    /// Get a typed pointer to the result storage.
    pub fn resultPtr(self: *JobExecuteState, comptime T: type) *T {
        if (@sizeOf(T) > @sizeOf(ResultType)) {
            @compileError("return type is too large to be passed between threads (max 32 bytes)");
        }
        const bytes = std.mem.sliceAsBytes(&self.result);
        return std.mem.bytesAsValue(T, bytes);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "Job - state transitions" {
    var job = Job.pending();
    try std.testing.expectEqual(Job.State.pending, job.state());

    // Simulate push
    var head = Job.head();
    var tail: *Job = &head;
    const handler = struct {
        fn h(_: *Task, _: *Job) void {}
    }.h;

    job.push(&tail, handler);
    try std.testing.expectEqual(Job.State.queued, job.state());
    try std.testing.expect(tail == &job);
}

test "Job - push and pop" {
    var head = Job.head();
    var tail: *Job = &head;
    const handler = struct {
        fn h(_: *Task, _: *Job) void {}
    }.h;

    // Push 3 jobs
    var jobs: [3]Job = .{ Job.pending(), Job.pending(), Job.pending() };
    for (&jobs) |*j| {
        j.push(&tail, handler);
    }

    try std.testing.expect(tail == &jobs[2]);

    // Pop them in reverse order (LIFO)
    jobs[2].pop(&tail);
    try std.testing.expect(tail == &jobs[1]);

    jobs[1].pop(&tail);
    try std.testing.expect(tail == &jobs[0]);

    jobs[0].pop(&tail);
    try std.testing.expect(tail == &head);
}

test "Job - shift (steal from head)" {
    var head = Job.head();
    var tail: *Job = &head;
    const handler = struct {
        fn h(_: *Task, _: *Job) void {}
    }.h;

    // With empty queue, shift returns null
    try std.testing.expect(head.shift() == null);

    // Push 2 jobs
    var job1 = Job.pending();
    var job2 = Job.pending();
    job1.push(&tail, handler);
    job2.push(&tail, handler);

    // Shift returns job1 (oldest)
    const shifted = head.shift();
    try std.testing.expect(shifted == &job1);
    try std.testing.expectEqual(Job.State.executing, job1.state());

    // Can't shift job2 because it's the tail
    try std.testing.expect(head.shift() == null);
}

test "Job - sentinel head is branch-free" {
    var head = Job.head();
    var tail: *Job = &head;

    // Head is always valid, tail is always valid
    // This means we never have null checks in push/pop
    try std.testing.expect(head.isTail());
    try std.testing.expect(tail.isTail());

    const handler = struct {
        fn h(_: *Task, _: *Job) void {}
    }.h;

    var job = Job.pending();
    job.push(&tail, handler);

    // After push, head still valid, tail points to job
    try std.testing.expect(!head.isTail());
    try std.testing.expect(job.isTail());
}
