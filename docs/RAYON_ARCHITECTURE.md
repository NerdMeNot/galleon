# Rayon Architecture Analysis

## Purpose

This document analyzes Rayon's architecture to inform the design of a work-stealing parallel execution framework for Zig/Galleon. The goal is to create a standalone library ("Ziyon") that can be extracted later.

## Table of Contents

1. [Core Philosophy](#core-philosophy)
2. [Key Primitives](#key-primitives)
3. [Work-Stealing Architecture](#work-stealing-architecture)
4. [Parallel Iterator Design](#parallel-iterator-design)
5. [Chase-Lev Deque](#chase-lev-deque)
6. [Proposed Zig Implementation](#proposed-zig-implementation)

---

## Core Philosophy

Rayon's fundamental insight is **"potential parallelism"** rather than guaranteed parallelism:

> "The decision of whether or not to use parallel threads is made dynamically, based on whether idle cores are available"

This differs from explicit thread spawning (like Go's goroutines or crossbeam's scoped threads):
- Calling `join(a, b)` does NOT guarantee parallel execution
- The runtime decides based on available resources
- If no idle cores exist, `join` executes sequentially
- This eliminates the need for users to reason about optimal parallelism

**Key insight**: Knowing when parallelism is profitable requires global context (idle cores, other parallel operations, cache state). The runtime has this context; the user doesn't.

---

## Key Primitives

### 1. `join(a, b)` - The Foundation

The fundamental building block. Everything else is built on top of this.

```
join(closure_a, closure_b) -> (result_a, result_b)
```

**Semantics**:
1. Push `b` onto current thread's work queue
2. Execute `a` immediately
3. When `a` completes:
   - If `b` was stolen by another thread, wait for it (or steal other work while waiting)
   - If `b` is still in queue, execute it ourselves
4. Return both results

**Why this works**:
- No explicit thread management
- Automatically adapts to system load
- Recursive divide-and-conquer naturally emerges
- Work-stealing provides automatic load balancing

### 2. `scope(|s| { ... })` - Dynamic Fork-Join

Enables spawning an arbitrary number of tasks:

```rust
scope(|s| {
    for item in items {
        s.spawn(|_| process(item));
    }
}); // Blocks until all spawned tasks complete
```

**Key properties**:
- Structured concurrency: scope doesn't return until all tasks complete
- Enables borrowing local variables (unlike `spawn` which requires `'static`)
- Implemented using latches and counters

### 3. `spawn(closure)` - Fire and Forget

For `'static` closures that don't need to be awaited:

```rust
spawn(|| expensive_computation());
// Returns immediately, computation happens in background
```

### 4. Parallel Iterators

High-level abstraction built on `join`:

```rust
data.par_iter()
    .map(|x| x * 2)
    .filter(|x| x > 10)
    .sum()
```

Automatically splits data using divide-and-conquer, then merges results.

---

## Work-Stealing Architecture

### Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         Registry                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  Global Injector Queue (for external task submission)        ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Worker 0 │  │ Worker 1 │  │ Worker 2 │  │ Worker N │        │
│  │          │  │          │  │          │  │          │        │
│  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │        │
│  │ │Deque │ │  │ │Deque │ │  │ │Deque │ │  │ │Deque │ │        │
│  │ │      │ │  │ │      │ │  │ │      │ │  │ │      │ │        │
│  │ │ LIFO │ │  │ │ LIFO │ │  │ │ LIFO │ │  │ │ LIFO │ │        │
│  │ │ pop  │ │  │ │ pop  │ │  │ │ pop  │ │  │ │ pop  │ │        │
│  │ │      │ │  │ │      │ │  │ │      │ │  │ │      │ │        │
│  │ │ FIFO │◄┼──┼─┼steal │◄┼──┼─┼steal │◄┼──┼─┼steal │ │        │
│  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

### Registry (Thread Pool Manager)

```zig
const Registry = struct {
    thread_infos: []ThreadInfo,      // Metadata per worker
    injected_jobs: Injector(Job),    // Global queue for external submissions
    terminate_count: AtomicUsize,    // Reference count for graceful shutdown
    sleep: SleepState,               // Manages idle thread sleeping/waking
};
```

### ThreadInfo (Per-Worker Metadata)

```zig
const ThreadInfo = struct {
    stealer: Stealer(Job),    // Other threads steal from here (FIFO end)
    primed: LockLatch,        // Signals thread is ready
    stopped: LockLatch,       // Signals thread has stopped
    terminate: OnceLatch,     // Shutdown signal
};
```

### WorkerThread (Per-Thread Execution State)

```zig
const WorkerThread = struct {
    worker: Worker(Job),      // Local deque (LIFO push/pop)
    index: usize,             // Position in pool
    rng: XorShift64Star,      // For random victim selection
    registry: *Registry,      // Back-reference
};
```

### Work-Stealing Loop

```
while not terminated:
    // 1. Try local work first (LIFO - cache friendly)
    if job = worker.pop():
        execute(job)
        continue

    // 2. Try global injector queue
    if job = registry.injected_jobs.steal():
        execute(job)
        continue

    // 3. Try stealing from random victim (FIFO - fairness)
    victim = rng.next() % num_workers
    for i in 0..num_workers:
        idx = (victim + i) % num_workers
        if idx != my_index:
            if job = thread_infos[idx].stealer.steal():
                execute(job)
                break

    // 4. No work found, go to sleep
    sleep.wait_for_work()
```

### Why LIFO Local / FIFO Steal?

**LIFO for local operations**:
- Most recently pushed task is likely still in L1/L2 cache
- Maintains temporal locality
- Recursive algorithms benefit (child tasks executed before siblings)

**FIFO for stealing**:
- Older tasks represent larger subtrees (more work)
- Stealing large chunks reduces steal frequency
- Fairness: work progresses across the tree breadth

---

## Parallel Iterator Design

### The Producer/Consumer Model

Rayon's parallel iterators use a **two-mode** design:

#### Pull Mode (Producer)
```zig
const Producer = struct {
    fn split_at(self, index: usize) -> struct { Self, Self };
    fn into_iter(self) -> Iterator;
    fn len(self) -> usize;
};
```

For indexed data (slices, ranges) where we know exact positions.

#### Push Mode (Consumer)
```zig
const Consumer = struct {
    fn split_at(self, index: usize) -> struct { Self, Self, Reducer };
    fn into_folder(self) -> Folder;
    fn full(self) -> bool;  // Short-circuit (e.g., find())
};
```

For operations that consume results (sum, collect, for_each).

### The Bridge Function

Connects producers to consumers recursively:

```
fn bridge(producer: Producer, consumer: Consumer):
    if producer.len() <= threshold or consumer.full():
        // Sequential execution
        folder = consumer.into_folder()
        for item in producer.into_iter():
            folder.consume(item)
        return folder.complete()

    // Parallel split
    mid = producer.len() / 2
    (left_p, right_p) = producer.split_at(mid)
    (left_c, right_c, reducer) = consumer.split_at(mid)

    (left_result, right_result) = join(
        || bridge(left_p, left_c),
        || bridge(right_p, right_c)
    )

    return reducer.reduce(left_result, right_result)
```

### Granularity Control

```zig
// Minimum items per sequential task (default: 1)
fn with_min_len(self, min: usize) -> Self;

// Maximum items per sequential task (default: MAX)
fn with_max_len(self, max: usize) -> Self;
```

Rayon auto-tunes, but users can override for specific workloads.

---

## Chase-Lev Deque

The core data structure enabling efficient work-stealing.

### Structure

```zig
const Deque = struct {
    buffer: Atomic(*CircularArray),  // Growable circular buffer
    top: AtomicUsize,                // Steal end (incremented by stealers)
    bottom: AtomicUsize,             // Local end (modified by owner)
};

const CircularArray = struct {
    log_size: u6,                    // log2(capacity)
    items: [*]Job,                   // Actual storage

    fn capacity(self) -> usize { 1 << self.log_size; }
    fn get(self, i: usize) -> Job { items[i & (capacity() - 1)]; }
    fn put(self, i: usize, job: Job) { items[i & (capacity() - 1)] = job; }
};
```

### Operations

#### Push (Owner only, bottom end)
```zig
fn push(self, job: Job) void {
    b = self.bottom.load(.relaxed);
    t = self.top.load(.acquire);

    array = self.buffer.load(.relaxed);
    if (b - t >= array.capacity() - 1) {
        // Grow buffer
        array = grow(array, t, b);
        self.buffer.store(array, .release);
    }

    array.put(b, job);
    atomic_fence(.release);
    self.bottom.store(b + 1, .relaxed);
}
```

#### Pop (Owner only, bottom end)
```zig
fn pop(self) ?Job {
    b = self.bottom.load(.relaxed) - 1;
    self.bottom.store(b, .relaxed);
    atomic_fence(.seq_cst);
    t = self.top.load(.relaxed);

    if (t <= b) {
        // Non-empty
        job = self.buffer.load(.relaxed).get(b);
        if (t == b) {
            // Last element, race with stealers
            if (!self.top.cmpxchg(t, t + 1, .seq_cst, .relaxed)) {
                // Lost race
                self.bottom.store(b + 1, .relaxed);
                return null;
            }
            self.bottom.store(b + 1, .relaxed);
        }
        return job;
    } else {
        // Empty
        self.bottom.store(b + 1, .relaxed);
        return null;
    }
}
```

#### Steal (Other threads, top end)
```zig
fn steal(self) StealResult {
    t = self.top.load(.acquire);
    atomic_fence(.seq_cst);
    b = self.bottom.load(.acquire);

    if (t >= b) {
        return .empty;
    }

    job = self.buffer.load(.consume).get(t);
    if (!self.top.cmpxchg(t, t + 1, .seq_cst, .relaxed)) {
        return .retry;  // Lost race with another stealer or pop
    }

    return .{ .success = job };
}
```

### Memory Ordering Rationale

- **Push**: Release fence ensures job is visible before bottom increment
- **Pop**: SeqCst fence prevents reordering with steal's SeqCst
- **Steal**: Acquire on top, SeqCst fence, acquire on bottom, CAS with SeqCst

The SeqCst fences establish a total order between pop and steal operations on the last element.

---

## Proposed Zig Implementation

### Module Structure

```
ziyon/
├── src/
│   ├── root.zig           # Public API
│   ├── deque.zig          # Chase-Lev work-stealing deque
│   ├── registry.zig       # Thread pool management
│   ├── worker.zig         # Per-thread state and loop
│   ├── job.zig            # Job abstraction
│   ├── latch.zig          # Synchronization primitives
│   ├── sleep.zig          # Idle thread management
│   └── parallel_iter.zig  # Parallel iterator implementation
└── build.zig
```

### Core API

```zig
const ziyon = @import("ziyon");

// Initialize global pool (optional, auto-inits on first use)
pub fn init(config: Config) void;
pub fn deinit() void;

// Core primitives
pub fn join(comptime A: type, comptime B: type, a: A, b: B) struct { A.ReturnType, B.ReturnType };
pub fn scope(comptime F: type, f: F) F.ReturnType;
pub fn spawn(comptime F: type, f: F) void;

// Parallel slices
pub fn parallelFor(comptime T: type, slice: []T, comptime F: type, f: F) void;
pub fn parallelMap(comptime T: type, comptime U: type, input: []const T, output: []U, comptime F: type, f: F) void;
pub fn parallelReduce(comptime T: type, slice: []const T, identity: T, comptime F: type, f: F) T;
```

### Usage Example

```zig
const ziyon = @import("ziyon");

// Parallel sum using divide-and-conquer
fn parallelSum(data: []const f64) f64 {
    if (data.len <= 1024) {
        // Sequential threshold
        var sum: f64 = 0;
        for (data) |v| sum += v;
        return sum;
    }

    const mid = data.len / 2;
    const left, const right = ziyon.join(
        struct { fn call(d: []const f64) f64 { return parallelSum(d); } }.call,
        data[0..mid],
        struct { fn call(d: []const f64) f64 { return parallelSum(d); } }.call,
        data[mid..],
    );

    return left + right;
}
```

### Integration with Galleon

```zig
// In galleon SIMD operations:
pub fn sumParallel(data: []const f64) f64 {
    const THRESHOLD = 65536;  // ~512KB, fits in L2 cache

    if (data.len <= THRESHOLD) {
        return sumSimd(data);  // Single-threaded SIMD
    }

    // Parallel divide-and-conquer with SIMD leaves
    return ziyon.parallelReduce(
        f64,
        data,
        0.0,
        struct {
            fn combine(a: f64, b: f64) f64 { return a + b; }
            fn process(chunk: []const f64) f64 { return sumSimd(chunk); }
        },
    );
}
```

---

## Key Design Decisions for Ziyon

### 1. Avoid Closures

Zig doesn't have closures like Rust. Use:
- Function pointers with context
- Comptime-known function types
- Explicit capture structs

### 2. Memory Management

- Use arena allocator for job metadata
- Circular array growth uses general allocator
- Consider thread-local arenas for reduced contention

### 3. Platform-Specific Optimizations

- Linux: futex for sleeping/waking
- macOS: os_unfair_lock, keyed events
- Windows: SRW locks, keyed events
- Fallback: pthread condition variables

### 4. Cache-Line Padding

```zig
const CacheLinePadded = struct {
    value: AtomicUsize,
    _padding: [64 - @sizeOf(AtomicUsize)]u8 = undefined,
};
```

Prevents false sharing between worker thread counters.

### 5. Adaptive Thresholds

- Auto-tune sequential threshold based on:
  - Operation cost (sum is cheap, complex transforms are expensive)
  - Cache size
  - Number of workers

---

## References

1. [Rayon GitHub Repository](https://github.com/rayon-rs/rayon)
2. [Rayon: data parallelism in Rust (Niko Matsakis)](https://smallcultfollowing.com/babysteps/blog/2015/12/18/rayon-data-parallelism-in-rust/)
3. [Dynamic Circular Work-Stealing Deque (Chase & Lev, 2005)](https://www.dre.vanderbilt.edu/~schmidt/PDF/work-stealing-dequeue.pdf)
4. [Correct and Efficient Work-Stealing for Weak Memory Models (Lê et al, 2013)](https://fzn.fr/readings/ppopp13.pdf)
5. [Crossbeam Deque Documentation](https://docs.rs/crossbeam-deque/latest/crossbeam_deque/)
6. [Resource efficient Thread Pools with Zig (kprotty)](https://zig.news/kprotty/resource-efficient-thread-pools-with-zig-3291)
7. [Implementing Rayon's Parallel Iterators Tutorial](https://geo-ant.github.io/blog/2022/implementing-parallel-iterators-rayon/)
8. [Rayon Plumbing README](https://github.com/rayon-rs/rayon/blob/main/src/iter/plumbing/README.md)
