//! Thread Pool Registry for Blitz
//!
//! The Registry manages the pool of worker threads and provides the global
//! injector queue for external job submission. It follows a lazy initialization
//! pattern similar to Galleon's dispatch.zig.

const std = @import("std");
const Worker = @import("worker.zig").Worker;
const WorkerState = @import("worker.zig").WorkerState;
const WorkLoopContext = @import("worker.zig").WorkLoopContext;
const workLoop = @import("worker.zig").workLoop;
const Job = @import("job.zig").Job;
const SleepManager = @import("sleep.zig").SleepManager;
const latch = @import("latch.zig");
const CountLatch = latch.CountLatch;

// Import thread configuration from simd/core.zig
const core = @import("../simd/core.zig");
const MAX_THREADS = core.MAX_THREADS;
const getMaxThreads = core.getMaxThreads;

// ============================================================================
// Injector Queue (MPMC for external submissions)
// ============================================================================

/// Node in the injector queue
const InjectorNode = struct {
    job: *Job,
    next: ?*InjectorNode,
};

/// Multi-producer multi-consumer queue for external job injection.
/// Uses a lock for producers (external threads) and lock-free stealing for workers.
pub const InjectorQueue = struct {
    /// Head of the queue (steal from here)
    head: std.atomic.Value(?*InjectorNode),

    /// Tail of the queue (push here)
    tail: std.atomic.Value(?*InjectorNode),

    /// Lock for producers
    lock: std.Thread.Mutex,

    /// Allocator for nodes
    allocator: std.mem.Allocator,

    /// Approximate count (for heuristics)
    count: std.atomic.Value(u32),

    pub fn init(allocator: std.mem.Allocator) InjectorQueue {
        return .{
            .head = std.atomic.Value(?*InjectorNode).init(null),
            .tail = std.atomic.Value(?*InjectorNode).init(null),
            .lock = .{},
            .allocator = allocator,
            .count = std.atomic.Value(u32).init(0),
        };
    }

    pub fn deinit(self: *InjectorQueue) void {
        // Drain remaining nodes
        while (self.steal()) |job| {
            _ = job; // Jobs are not owned by injector
        }
    }

    /// Push a job (called by external threads or workers injecting)
    pub fn push(self: *InjectorQueue, job: *Job) !void {
        const node = try self.allocator.create(InjectorNode);
        node.* = .{ .job = job, .next = null };

        self.lock.lock();
        defer self.lock.unlock();

        const old_tail = self.tail.load(.acquire);
        if (old_tail) |t| {
            t.next = node;
        } else {
            self.head.store(node, .release);
        }
        self.tail.store(node, .release);
        _ = self.count.fetchAdd(1, .acq_rel);
    }

    /// Steal a job (called by workers)
    pub fn steal(self: *InjectorQueue) ?*Job {
        // Try lock-free first
        var head = self.head.load(.acquire);
        if (head == null) return null;

        // Need lock for correct dequeue
        self.lock.lock();
        defer self.lock.unlock();

        head = self.head.load(.acquire);
        if (head) |h| {
            const job = h.job;
            const next = h.next;

            self.head.store(next, .release);
            if (next == null) {
                self.tail.store(null, .release);
            }

            _ = self.count.fetchSub(1, .acq_rel);
            self.allocator.destroy(h);
            return job;
        }

        return null;
    }

    /// Get approximate count
    pub fn getCount(self: *const InjectorQueue) u32 {
        return self.count.load(.acquire);
    }

    /// Check if empty
    pub fn isEmpty(self: *const InjectorQueue) bool {
        return self.head.load(.acquire) == null;
    }
};

// ============================================================================
// Registry
// ============================================================================

/// Thread pool registry
pub const Registry = struct {
    /// Worker array
    workers: [MAX_THREADS]?*Worker,

    /// Number of workers
    num_workers: u32,

    /// Global injector queue
    injector: InjectorQueue,

    /// Sleep manager
    sleep_mgr: SleepManager,

    /// Shutdown flag
    shutdown: std.atomic.Value(bool),

    /// Startup latch (wait for all workers to start)
    startup_latch: CountLatch,

    /// Allocator
    allocator: std.mem.Allocator,

    /// Has been started
    started: bool,

    /// Initialize the registry
    pub fn init(allocator: std.mem.Allocator) !*Registry {
        const num_workers = @as(u32, @intCast(getMaxThreads()));

        const self = try allocator.create(Registry);
        self.* = Registry{
            .workers = [_]?*Worker{null} ** MAX_THREADS,
            .num_workers = num_workers,
            .injector = InjectorQueue.init(allocator),
            .sleep_mgr = SleepManager.init(num_workers),
            .shutdown = std.atomic.Value(bool).init(false),
            .startup_latch = CountLatch.init(num_workers),
            .allocator = allocator,
            .started = false,
        };

        // Create workers
        for (0..num_workers) |i| {
            self.workers[i] = try Worker.init(@intCast(i), allocator);
        }

        return self;
    }

    /// Deinitialize the registry
    pub fn deinit(self: *Registry) void {
        // Ensure shutdown
        if (self.started and !self.shutdown.load(.acquire)) {
            self.shutdownAndWait();
        }

        // Destroy workers
        for (0..self.num_workers) |i| {
            if (self.workers[i]) |w| {
                w.deinit();
                self.allocator.destroy(w);
                self.workers[i] = null;
            }
        }

        self.injector.deinit();
        self.allocator.destroy(self);
    }

    /// Start worker threads
    pub fn start(self: *Registry) !void {
        if (self.started) return;

        // Build worker pointer array for work loop
        var worker_ptrs: [MAX_THREADS]*Worker = undefined;
        for (0..self.num_workers) |i| {
            worker_ptrs[i] = self.workers[i].?;
        }

        // Spawn worker threads
        for (0..self.num_workers) |i| {
            const worker = self.workers[i].?;

            worker.thread = try std.Thread.spawn(.{}, struct {
                fn run(
                    w: *Worker,
                    ptrs: [MAX_THREADS]*Worker,
                    num: u32,
                    inj: *InjectorQueue,
                    sleep: *SleepManager,
                    shutdown_flag: *std.atomic.Value(bool),
                    startup: *CountLatch,
                ) void {
                    // Set thread-local worker pointer
                    tls_current_worker = w;

                    // Signal ready
                    startup.countDown();

                    // Run work loop
                    const ctx = WorkLoopContext{
                        .worker = w,
                        .workers = ptrs[0..num],
                        .num_workers = num,
                        .injector = @ptrCast(inj),
                        .injector_steal_fn = @ptrCast(&InjectorQueue.steal),
                        .sleep_mgr = sleep,
                        .shutdown = shutdown_flag,
                    };
                    workLoop(ctx);
                }
            }.run, .{
                worker,
                worker_ptrs,
                self.num_workers,
                &self.injector,
                &self.sleep_mgr,
                &self.shutdown,
                &self.startup_latch,
            });
        }

        // Wait for all workers to start
        self.startup_latch.wait();
        self.started = true;
    }

    /// Signal shutdown and wait for workers to terminate
    pub fn shutdownAndWait(self: *Registry) void {
        // Signal shutdown
        self.shutdown.store(true, .release);

        // Wake all sleeping workers
        for (0..self.num_workers) |i| {
            if (self.workers[i]) |w| {
                w.wake();
            }
        }

        // Join worker threads
        for (0..self.num_workers) |i| {
            if (self.workers[i]) |w| {
                if (w.thread) |thread| {
                    thread.join();
                    w.thread = null;
                }
            }
        }
    }

    /// Inject a job into the global queue
    pub fn inject(self: *Registry, job: *Job) !void {
        try self.injector.push(job);
        self.sleep_mgr.notifyNewWork();
        self.wakeOne();
    }

    /// Wake one sleeping worker
    pub fn wakeOne(self: *Registry) void {
        for (0..self.num_workers) |i| {
            if (self.workers[i]) |w| {
                if (w.isSleeping()) {
                    w.wake();
                    return;
                }
            }
        }
    }

    /// Wake N sleeping workers
    pub fn wakeMany(self: *Registry, count: u32) void {
        var woken: u32 = 0;
        for (0..self.num_workers) |i| {
            if (woken >= count) break;
            if (self.workers[i]) |w| {
                if (w.isSleeping()) {
                    w.wake();
                    woken += 1;
                }
            }
        }
    }

    /// Wake all sleeping workers
    pub fn wakeAll(self: *Registry) void {
        for (0..self.num_workers) |i| {
            if (self.workers[i]) |w| {
                w.wake();
            }
        }
    }

    /// Get worker by ID
    pub fn getWorker(self: *Registry, id: u32) ?*Worker {
        if (id >= self.num_workers) return null;
        return self.workers[id];
    }

    /// Get number of workers
    pub fn getNumWorkers(self: *const Registry) u32 {
        return self.num_workers;
    }

    /// Check if shutdown requested
    pub fn isShutdown(self: *const Registry) bool {
        return self.shutdown.load(.acquire);
    }
};

// ============================================================================
// Thread-Local Worker Pointer
// ============================================================================

/// Thread-local pointer to current worker (set when thread starts)
pub threadlocal var tls_current_worker: ?*Worker = null;

/// Get the current worker (if called from a worker thread)
pub fn getCurrentWorker() ?*Worker {
    return tls_current_worker;
}

/// Check if currently on a worker thread
pub fn isWorkerThread() bool {
    return tls_current_worker != null;
}

// ============================================================================
// Global Registry (Lazy Initialization)
// ============================================================================

var global_registry: ?*Registry = null;
var registry_initialized: bool = false;
var registry_lock: std.Thread.Mutex = .{};

/// Get or create the global registry
pub fn getGlobalRegistry() !*Registry {
    // Fast path: already initialized
    if (@atomicLoad(bool, &registry_initialized, .acquire)) {
        return global_registry.?;
    }

    // Slow path: initialize with locking
    registry_lock.lock();
    defer registry_lock.unlock();

    if (!registry_initialized) {
        global_registry = try Registry.init(std.heap.c_allocator);
        try global_registry.?.start();
        @atomicStore(bool, &registry_initialized, true, .release);
    }

    return global_registry.?;
}

/// Check if global registry is initialized
pub fn isGlobalRegistryInitialized() bool {
    return @atomicLoad(bool, &registry_initialized, .acquire);
}

/// Shutdown the global registry
pub fn shutdownGlobalRegistry() void {
    registry_lock.lock();
    defer registry_lock.unlock();

    if (registry_initialized) {
        if (global_registry) |reg| {
            reg.deinit();
            global_registry = null;
        }
        @atomicStore(bool, &registry_initialized, false, .release);
    }
}

// ============================================================================
// Tests
// ============================================================================

test "InjectorQueue - basic push/steal" {
    var iq = InjectorQueue.init(std.testing.allocator);
    defer iq.deinit();

    const Ctx = struct { value: u32 };
    var ctx = Ctx{ .value = 42 };
    var job = Job.from(Ctx, &ctx, struct {
        fn exec(c: *Ctx) void {
            c.value *= 2;
        }
    }.exec);

    try iq.push(&job);
    try std.testing.expect(!iq.isEmpty());
    try std.testing.expectEqual(@as(u32, 1), iq.getCount());

    const stolen = iq.steal();
    try std.testing.expect(stolen != null);
    try std.testing.expect(stolen.? == &job);
    try std.testing.expect(iq.isEmpty());
}

test "InjectorQueue - FIFO order" {
    var iq = InjectorQueue.init(std.testing.allocator);
    defer iq.deinit();

    const Ctx = struct { value: u32 };
    var ctx1 = Ctx{ .value = 1 };
    var ctx2 = Ctx{ .value = 2 };
    var ctx3 = Ctx{ .value = 3 };

    var job1 = Job.from(Ctx, &ctx1, struct {
        fn exec(_: *Ctx) void {}
    }.exec);
    var job2 = Job.from(Ctx, &ctx2, struct {
        fn exec(_: *Ctx) void {}
    }.exec);
    var job3 = Job.from(Ctx, &ctx3, struct {
        fn exec(_: *Ctx) void {}
    }.exec);

    try iq.push(&job1);
    try iq.push(&job2);
    try iq.push(&job3);

    // Should steal in FIFO order
    try std.testing.expect(iq.steal().? == &job1);
    try std.testing.expect(iq.steal().? == &job2);
    try std.testing.expect(iq.steal().? == &job3);
    try std.testing.expect(iq.steal() == null);
}

test "Registry - init/deinit" {
    const reg = try Registry.init(std.testing.allocator);
    defer reg.deinit();

    try std.testing.expect(reg.num_workers > 0);
    try std.testing.expect(reg.num_workers <= MAX_THREADS);
    try std.testing.expect(!reg.isShutdown());
}

test "Registry - start and shutdown" {
    const reg = try Registry.init(std.testing.allocator);
    defer reg.deinit();

    try reg.start();
    try std.testing.expect(reg.started);

    // Give workers time to start their loops
    std.Thread.sleep(10_000_000); // 10ms

    // Workers should be running or sleeping
    for (0..reg.num_workers) |i| {
        const worker = reg.workers[i].?;
        const state = worker.getState();
        try std.testing.expect(state != .terminated);
    }
}

test "Registry - inject and execute" {
    const reg = try Registry.init(std.testing.allocator);
    defer reg.deinit();

    try reg.start();

    // Create a job that increments a counter
    var counter = std.atomic.Value(u32).init(0);
    var done = latch.OnceLatch.init();

    const Ctx = struct {
        counter: *std.atomic.Value(u32),
        done: *latch.OnceLatch,
    };
    var ctx = Ctx{ .counter = &counter, .done = &done };

    var job = Job.from(Ctx, &ctx, struct {
        fn exec(c: *Ctx) void {
            _ = c.counter.fetchAdd(1, .acq_rel);
            c.done.setDone();
        }
    }.exec);

    // Inject the job
    try reg.inject(&job);

    // Wait for completion
    done.wait();

    try std.testing.expectEqual(@as(u32, 1), counter.load(.acquire));
}
