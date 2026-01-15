//! Job Abstraction for Blitz
//!
//! Since Zig doesn't have closures, we use function pointers with explicit
//! context structs. This module provides abstractions for defining and
//! executing parallel tasks.

const std = @import("std");
const latch = @import("latch.zig");
const OnceLatch = latch.OnceLatch;
const CountLatch = latch.CountLatch;

// ============================================================================
// Core Job Types
// ============================================================================

/// A job represents a unit of work that can be executed.
/// Jobs are placed in work-stealing deques and executed by worker threads.
pub const Job = struct {
    /// Function pointer to execute
    execute_fn: *const fn (*anyopaque) void,

    /// Opaque context pointer
    context: *anyopaque,

    /// Execute the job
    pub fn execute(self: *const Job) void {
        self.execute_fn(self.context);
    }

    /// Create a job from a typed context and function
    pub fn from(
        comptime Context: type,
        context: *Context,
        comptime execute_fn: fn (*Context) void,
    ) Job {
        return Job{
            .execute_fn = @ptrCast(&execute_fn),
            .context = @ptrCast(context),
        };
    }
};

/// A job with an associated completion latch.
/// Used when the caller needs to wait for the job to complete.
pub const LatchedJob = struct {
    job: Job,
    latch: *OnceLatch,

    /// Execute the job and signal completion
    pub fn execute(self: *const LatchedJob) void {
        self.job.execute();
        self.latch.setDone();
    }

    pub fn asJob(self: *LatchedJob) Job {
        return Job{
            .execute_fn = @ptrCast(&executeWrapper),
            .context = @ptrCast(self),
        };
    }

    fn executeWrapper(self: *LatchedJob) void {
        self.execute();
    }
};

// ============================================================================
// Stack-Allocated Jobs (for join())
// ============================================================================

/// A job that stores its context inline (no heap allocation).
/// Used for join() where the job lives on the stack.
pub fn StackJob(comptime Context: type, comptime execute_fn: fn (*Context) void) type {
    return struct {
        const Self = @This();

        context: Context,
        latch: OnceLatch,
        job: Job,

        pub fn init(ctx: Context) Self {
            return Self{
                .context = ctx,
                .latch = OnceLatch.init(),
                // Job context is set to a sentinel; executeWrapper uses @fieldParentPtr
                .job = Job{
                    .execute_fn = @ptrCast(&executeWrapper),
                    .context = undefined, // Will use @fieldParentPtr instead
                },
            };
        }

        fn executeWrapper(ctx: *anyopaque) void {
            // The Job stores a pointer to its own location, use @fieldParentPtr
            // to get the containing StackJob from the job field
            const job_ptr: *Job = @alignCast(@ptrCast(ctx));
            const self: *Self = @fieldParentPtr("job", job_ptr);
            execute_fn(&self.context);
        }

        /// Get a pointer to the job
        pub fn getJob(self: *Self) *Job {
            // Set context to point to the job itself (for @fieldParentPtr)
            self.job.context = @ptrCast(&self.job);
            return &self.job;
        }

        /// Wait for the job to complete
        pub fn wait(self: *Self) void {
            self.latch.wait();
        }

        /// Check if done without blocking
        pub fn isDone(self: *const Self) bool {
            return self.latch.isDone();
        }

        /// Signal completion (called when job finishes)
        pub fn complete(self: *Self) void {
            self.latch.setDone();
        }
    };
}

// ============================================================================
// Heap-Allocated Jobs (for spawn())
// ============================================================================

/// A heap-allocated job with inline context.
/// Used for fire-and-forget spawned tasks.
pub fn HeapJob(comptime Context: type, comptime execute_fn: fn (*Context) void) type {
    return struct {
        const Self = @This();

        context: Context,
        job: Job,
        allocator: std.mem.Allocator,

        pub fn create(allocator: std.mem.Allocator, ctx: Context) !*Self {
            const self = try allocator.create(Self);
            self.* = Self{
                .context = ctx,
                .job = undefined,
                .allocator = allocator,
            };
            self.job = Job{
                .execute_fn = @ptrCast(&executeAndDestroy),
                .context = @ptrCast(self),
            };
            return self;
        }

        fn executeAndDestroy(self: *Self) void {
            execute_fn(&self.context);
            self.allocator.destroy(self);
        }

        pub fn getJob(self: *Self) *Job {
            return &self.job;
        }
    };
}

// ============================================================================
// Join Job (for fork-join parallelism)
// ============================================================================

/// Context for the right side of a join operation.
/// The left side is executed directly, the right side may be stolen.
pub fn JoinJob(
    comptime ContextA: type,
    comptime ContextB: type,
    comptime fn_a: fn (*ContextA) void,
    comptime fn_b: fn (*ContextB) void,
) type {
    return struct {
        const Self = @This();

        ctx_a: *ContextA,
        ctx_b: *ContextB,
        latch: CountLatch,
        job_b: Job,

        pub fn init(ctx_a: *ContextA, ctx_b: *ContextB) Self {
            var self = Self{
                .ctx_a = ctx_a,
                .ctx_b = ctx_b,
                .latch = CountLatch.init(2), // Wait for both A and B
                .job_b = undefined,
            };
            self.job_b = Job{
                .execute_fn = @ptrCast(&executeBAndSignal),
                .context = @ptrCast(&self),
            };
            return self;
        }

        fn executeBAndSignal(self: *Self) void {
            fn_b(self.ctx_b);
            self.latch.countDown();
        }

        /// Execute A directly (owner thread)
        pub fn executeA(self: *Self) void {
            fn_a(self.ctx_a);
            self.latch.countDown();
        }

        /// Execute B directly (if not stolen)
        pub fn executeB(self: *Self) void {
            fn_b(self.ctx_b);
            self.latch.countDown();
        }

        /// Get the job for B (to be pushed to deque)
        pub fn getJobB(self: *Self) *Job {
            return &self.job_b;
        }

        /// Wait for both tasks to complete
        pub fn wait(self: *Self) void {
            self.latch.wait();
        }

        /// Check if both tasks are done
        pub fn isDone(self: *const Self) bool {
            return self.latch.isDone();
        }
    };
}

// ============================================================================
// Parallel For Job
// ============================================================================

/// Context for a parallel for iteration.
pub fn ForEachContext(comptime T: type, comptime body_fn: fn (T, usize, usize) void) type {
    return struct {
        const Self = @This();

        user_context: T,
        start: usize,
        end: usize,
        grain_size: usize,
        latch: *CountLatch,

        pub fn init(
            user_ctx: T,
            start: usize,
            end: usize,
            grain_size: usize,
            count_latch: *CountLatch,
        ) Self {
            return Self{
                .user_context = user_ctx,
                .start = start,
                .end = end,
                .grain_size = grain_size,
                .latch = count_latch,
            };
        }

        pub fn execute(self: *Self) void {
            const len = self.end - self.start;

            if (len <= self.grain_size) {
                // Base case: execute sequentially
                body_fn(self.user_context, self.start, self.end);
                self.latch.countDown();
            } else {
                // Recursive case: split and potentially parallelize
                // This would be handled by the scheduler
                body_fn(self.user_context, self.start, self.end);
                self.latch.countDown();
            }
        }
    };
}

// ============================================================================
// Parallel Reduce Job
// ============================================================================

/// Context for a parallel reduce operation.
pub fn ReduceContext(
    comptime T: type,
    comptime Context: type,
    comptime map_fn: fn (Context, usize) T,
    comptime combine_fn: fn (T, T) T,
) type {
    return struct {
        const Self = @This();

        user_context: Context,
        start: usize,
        end: usize,
        identity: T,
        grain_size: usize,
        result: T,
        latch: *CountLatch,

        pub fn init(
            user_ctx: Context,
            start: usize,
            end: usize,
            identity: T,
            grain_size: usize,
            count_latch: *CountLatch,
        ) Self {
            return Self{
                .user_context = user_ctx,
                .start = start,
                .end = end,
                .identity = identity,
                .grain_size = grain_size,
                .result = identity,
                .latch = count_latch,
            };
        }

        pub fn execute(self: *Self) void {
            // Sequential execution within this chunk
            var result = self.identity;
            for (self.start..self.end) |i| {
                result = combine_fn(result, map_fn(self.user_context, i));
            }
            self.result = result;
            self.latch.countDown();
        }

        pub fn getResult(self: *const Self) T {
            return self.result;
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

test "Job - basic execution" {
    const Context = struct {
        value: u32,
        executed: bool,
    };

    var ctx = Context{ .value = 42, .executed = false };

    const job = Job.from(Context, &ctx, struct {
        fn exec(c: *Context) void {
            c.executed = true;
        }
    }.exec);

    try std.testing.expect(!ctx.executed);
    job.execute();
    try std.testing.expect(ctx.executed);
}

test "StackJob - inline context" {
    const Context = struct {
        value: u32,
        result: u32,
    };

    var stack_job = StackJob(Context, struct {
        fn exec(c: *Context) void {
            c.result = c.value * 2;
        }
    }.exec).init(Context{ .value = 21, .result = 0 });

    // Must call getJob() to set up the context pointer before executing
    const job = stack_job.getJob();
    job.execute();
    stack_job.complete();

    try std.testing.expectEqual(@as(u32, 42), stack_job.context.result);
    try std.testing.expect(stack_job.isDone());
}

test "HeapJob - heap allocated" {
    const Context = struct {
        counter: *std.atomic.Value(u32),
    };

    var counter = std.atomic.Value(u32).init(0);

    const heap_job = try HeapJob(Context, struct {
        fn exec(c: *Context) void {
            _ = c.counter.fetchAdd(1, .acq_rel);
        }
    }.exec).create(std.testing.allocator, Context{ .counter = &counter });

    heap_job.job.execute();

    try std.testing.expectEqual(@as(u32, 1), counter.load(.acquire));
    // HeapJob self-destructs, no need to free
}

test "JoinJob - both tasks complete" {
    const CtxA = struct { done: bool };
    const CtxB = struct { done: bool };

    var ctx_a = CtxA{ .done = false };
    var ctx_b = CtxB{ .done = false };

    var join_job = JoinJob(
        CtxA,
        CtxB,
        struct {
            fn exec(c: *CtxA) void {
                c.done = true;
            }
        }.exec,
        struct {
            fn exec(c: *CtxB) void {
                c.done = true;
            }
        }.exec,
    ).init(&ctx_a, &ctx_b);

    // Execute both directly (simulating no stealing)
    join_job.executeA();
    join_job.executeB();

    try std.testing.expect(ctx_a.done);
    try std.testing.expect(ctx_b.done);
    try std.testing.expect(join_job.isDone());
}
