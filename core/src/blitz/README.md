# Blitz: High-Performance Parallel Runtime

Blitz is a work-stealing parallel runtime for Zig, designed for low-overhead fork-join parallelism. It combines **heartbeat scheduling** (from Spice) with **active stealing** (from Rayon) to achieve ~20ns join overhead while maintaining excellent parallel scaling.

## Key Features

- **~20ns join overhead** - 25-50x faster than traditional Chase-Lev deques
- **Hybrid scheduling** - Heartbeat-based sharing + immediate work visibility
- **Zero-allocation hot path** - Local queue operations require no synchronization
- **Comptime specialization** - No vtable overhead for futures
- **SIMD-accelerated primitives** - Parallel sum, min, max with vectorization

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ThreadPool                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │  Worker 0   │  │  Worker 1   │  │  Worker N   │             │
│  │  ┌───────┐  │  │  ┌───────┐  │  │  ┌───────┐  │             │
│  │  │ Local │  │  │  │ Local │  │  │  │ Local │  │             │
│  │  │ Queue │  │  │  │ Queue │  │  │  │ Queue │  │             │
│  │  └───────┘  │  │  └───────┘  │  │  └───────┘  │             │
│  │  heartbeat  │  │  heartbeat  │  │  heartbeat  │             │
│  │  shared_job │  │  shared_job │  │  shared_job │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│                                                                  │
│  ┌──────────────────┐  ┌────────────────────────────┐          │
│  │ Heartbeat Thread │  │ idle_workers (atomic u32)  │          │
│  │ Sets flags every │  │ Enables immediate sharing  │          │
│  │ 10μs per worker  │  │ when workers are waiting   │          │
│  └──────────────────┘  └────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

### How It Works

1. **Local Queue Operations (Hot Path)**
   - Each worker has a local job queue (doubly-linked list)
   - Push/pop are pure local operations - no atomics, no locks
   - Cost: ~5ns per operation

2. **Heartbeat Scheduling**
   - A dedicated thread sets `heartbeat` flags on workers every ~10μs
   - When a worker sees its flag set, it advertises its oldest job
   - Other workers can steal advertised jobs under a mutex
   - Cost: ~3ns atomic load per `tick()` call

3. **Active Stealing (Rayon-style)**
   - When workers go idle, they increment `idle_workers` counter
   - `fork()` checks this counter - if workers are waiting, share immediately
   - Provides instant work visibility without waiting for heartbeat
   - Cost: ~3ns atomic load in `fork()`

### Why This Design?

| Approach | Push/Pop | Work Sharing | Idle Latency |
|----------|----------|--------------|--------------|
| Chase-Lev (Rayon) | ~50ns (CAS) | Immediate | Low |
| Heartbeat-only (Spice) | ~5ns (local) | 10-100μs | High |
| **Blitz (Hybrid)** | ~5ns (local) | Immediate when idle | Low |

## Files

| File | Purpose |
|------|---------|
| `mod.zig` | Module exports |
| `api.zig` | High-level API (join, parallelFor, parallelReduce) |
| `pool.zig` | ThreadPool with heartbeat thread |
| `job.zig` | Branch-free Job struct with state machine |
| `worker.zig` | Worker and Task types |
| `future.zig` | Future(Input, Output) for fork-join with return values |
| `latch.zig` | Synchronization primitives (OnceLatch, CountLatch) |
| `sync.zig` | SyncPtr for lock-free parallel writes |
| `threshold.zig` | Parallelization threshold heuristics |
| `iter.zig` | Parallel iterators (Rayon-style) |
| `scope.zig` | Scope-based parallelism (join2, join3, joinN) |
| `algorithms.zig` | Parallel algorithms (sort, scan, find, partition) |
| `bench.zig` | Comprehensive benchmarks |
| `build.zig` | Standalone build configuration |
| `rust_bench/` | Rust Rayon benchmarks for comparison |

## API Reference

### Initialization

```zig
const blitz = @import("blitz");

// Initialize with defaults (auto-detect thread count)
try blitz.init();
defer blitz.deinit();

// Or with custom configuration
try blitz.initWithConfig(.{
    .background_worker_count = 8,
    .heartbeat_interval = 10 * std.time.ns_per_us,
});

// Check status
const workers = blitz.numWorkers();
const initialized = blitz.isInitialized();
```

### Fork-Join (join)

Execute two tasks potentially in parallel:

```zig
const results = blitz.join(
    u64,  // Return type A
    u64,  // Return type B
    computeA,  // fn(ArgA) -> u64
    computeB,  // fn(ArgB) -> u64
    arg_a,
    arg_b,
);
// results[0] = computeA(arg_a)
// results[1] = computeB(arg_b)
```

For void functions:
```zig
blitz.joinVoid(doWorkA, doWorkB, arg_a, arg_b);
```

### Parallel For (parallelFor)

Execute a function over range [0, n):

```zig
const Context = struct { data: []f64 };
const ctx = Context{ .data = my_data };

blitz.parallelFor(my_data.len, Context, ctx, struct {
    fn body(c: Context, start: usize, end: usize) void {
        for (c.data[start..end]) |*v| {
            v.* = processValue(v.*);
        }
    }
}.body);
```

With custom grain size:
```zig
blitz.parallelForWithGrain(n, Context, ctx, body_fn, grain_size);
```

### Parallel Reduce (parallelReduce)

Map-reduce with associative combine:

```zig
const sum = blitz.parallelReduce(
    f64,           // Result type
    data.len,      // Count
    0.0,           // Identity
    []const f64,   // Context type
    data,          // Context
    struct {
        fn map(d: []const f64, i: usize) f64 {
            return d[i];
        }
    }.map,
    struct {
        fn combine(a: f64, b: f64) f64 {
            return a + b;
        }
    }.combine,
);
```

### Convenience Functions

```zig
// Aggregations
const sum = blitz.parallelSum(f64, data);
const min = blitz.parallelMin(f64, data);
const max = blitz.parallelMax(f64, data);
const mean = blitz.parallelMean(f64, data);
const product = blitz.parallelProduct(f64, data);

// Integer variants
const sum_i = blitz.parallelSumInt(i64, int_data);
const min_i = blitz.parallelMinInt(i64, int_data);
const max_i = blitz.parallelMaxInt(i64, int_data);
const mean_i = blitz.parallelMeanInt(i64, int_data);

// Element-wise operations
blitz.parallelAdd(f64, a, b, out);
blitz.parallelFill(f64, data, 0.0);
blitz.parallelFillIndexed(f64, void, data, struct {
    fn gen(_: void, i: usize) f64 { return @floatFromInt(i); }
}.gen, {});

// Transformations
blitz.parallelMap(f64, f64, void, input, output, struct {
    fn transform(_: void, x: f64) f64 { return x * 2; }
}.transform, {});

blitz.parallelMapInPlace(f64, void, data, struct {
    fn transform(_: void, x: f64) f64 { return @sqrt(x); }
}.transform, {});

// Predicates
const has_negative = blitz.parallelAny(f64, void, data, struct {
    fn pred(_: void, x: f64) bool { return x < 0; }
}.pred, {});

const all_positive = blitz.parallelAll(f64, void, data, struct {
    fn pred(_: void, x: f64) bool { return x > 0; }
}.pred, {});

const count_zeros = blitz.parallelCount(f64, void, data, struct {
    fn pred(_: void, x: f64) bool { return x == 0; }
}.pred, {});
```

### Threshold System

Blitz includes heuristics to avoid parallelization overhead for small data:

```zig
const OpType = blitz.OpType;

// Check if parallelization is beneficial
if (blitz.shouldParallelize(OpType.sum, data.len)) {
    // Use parallel version
} else {
    // Use sequential version
}

// Memory-bound operations have higher thresholds
const is_mem_bound = blitz.isMemoryBound(OpType.add);
```

## Performance

Measured on Apple M1 Pro (10 cores):

| Benchmark | Result |
|-----------|--------|
| Join overhead (empty) | 22 ns |
| Join overhead (minimal) | 23 ns |
| parallelFor (1K items) | 0.06 μs |
| Compute 1M sin+cos+sqrt | 2.60x speedup |
| Compute 10M sin+cos | 4.63x speedup |
| Fibonacci(30) recursive | 0.44x (overhead-bound) |

### When to Use

**Good for Blitz:**
- Compute-bound work (math, transformations)
- Large datasets (>10K elements)
- Divide-and-conquer algorithms
- Independent parallel tasks

**Not ideal for Blitz:**
- Memory-bound operations (simple copy, add)
- Very small datasets (<1K elements)
- Fine-grained parallelism with tiny tasks
- I/O-bound work

## Low-Level API

For advanced use cases, you can use the low-level primitives directly:

### Future

```zig
const Future = blitz.Future;

var future = Future(i32, i64).init();
future.fork(&task, compute, input);

// ... do other work ...

const result = future.join(&task) orelse compute(&task, input);
```

### Worker and Task

```zig
const pool = blitz.getPool();

_ = pool.call(ResultType, struct {
    fn compute(task: *Task, arg: ArgType) ResultType {
        // task.tick() - check heartbeat
        // task.call(T, fn, arg) - recursive call with tick
        // Future.fork/join - parallel subtasks
    }
}.compute, arg);
```

## Parallel Iterators (Rayon-style)

Blitz provides composable parallel iterators similar to Rayon:

```zig
const blitz = @import("blitz");

// Parallel sum
const sum = blitz.iter(i64, data).sum();

// Parallel min/max
const min = blitz.iter(i64, data).min();
const max = blitz.iter(i64, data).max();

// Map and collect
var result = try blitz.iter(i64, data)
    .map(square)
    .collect(allocator);
defer allocator.free(result);

// In-place mutation
blitz.iterMut(i64, data).mapInPlace(double);
blitz.iterMut(i64, data).fill(42);

// Range iteration
blitz.range(0, 1000).forEach(processIndex);
const total = blitz.range(0, 100).sum(i64, valueAtIndex);
```

## Scope-Based Parallelism

Execute multiple tasks in parallel:

```zig
// Execute 2 tasks in parallel
const results = blitz.join2(A, B, fnA, fnB);

// Execute 3 tasks in parallel
const results = blitz.join3(A, B, C, fnA, fnB, fnC);

// Execute N tasks in parallel
const funcs = [_]fn() i64{ fn1, fn2, fn3, fn4 };
const results = blitz.joinN(i64, 4, &funcs);

// Parallel for over index range
blitz.parallelForRange(0, 1000, processIndex);
blitz.parallelForRangeWithContext(MyCtx, ctx, 0, 1000, processWithContext);
```

## Parallel Algorithms

```zig
// Parallel merge sort
try blitz.parallelSort(i64, data, allocator);

// With custom comparator
try blitz.parallelSortBy(i64, data, allocator, myLessThan);

// Parallel prefix sum (scan)
blitz.parallelScan(i64, input, output);           // inclusive
blitz.parallelScanExclusive(i64, input, output);  // exclusive

// Parallel find
if (blitz.parallelFind(i64, data, isTarget)) |index| {
    // Found at index
}

// Parallel partition
const pivot = blitz.parallelPartition(i64, data, lessThanFive);
```

## Lock-Free Parallel Writes

For parallel materialization patterns (like Polars):

```zig
// SyncPtr for parallel writes to disjoint regions
var buffer: [1000]u64 = undefined;
const ptr = blitz.SyncPtr(u64).init(&buffer);

// Each thread writes to its region
blitz.parallelFor(num_chunks, Context, ctx, struct {
    fn body(c: Context, chunk_start: usize, chunk_end: usize) void {
        for (c.values[chunk_start..chunk_end], 0..) |val, i| {
            ptr.writeAt(c.offsets[chunk_start] + i, val);
        }
    }
}.body);

// Parallel flatten nested slices
blitz.parallelFlatten(u64, slices, output);

// Parallel scatter
blitz.parallelScatter(u64, values, indices, output);
```

## Comparison with Other Libraries

| Feature | Blitz | Rayon (Rust) | Spice (Zig) |
|---------|-------|--------------|-------------|
| Join overhead | ~20ns | ~50ns | <5ns |
| Work stealing | Hybrid | Chase-Lev | Heartbeat |
| Idle latency | Low | Low | High |
| API ergonomics | Good | Excellent | Minimal |
| Parallel iterators | Yes | Yes | No |
| Scope/spawn | Yes | Yes | Yes |
| Parallel sort | Yes | Yes | No |
| Parallel scan | Yes | Yes | No |

## Implementation Notes

### Branch-Free Job Queue

Jobs use a state machine encoded in pointer fields:
- `handler == null` → pending (not yet pushed)
- `prev != null` → queued (in local queue)
- `prev == null` → executing (stolen by another worker)

This eliminates branches in the hot path.

### Thread-Local Task Context

To avoid pool.call() overhead on recursive joins, Blitz maintains a thread-local `current_task` pointer. When already inside a pool context, subsequent join() calls use the fast path directly.

### Heartbeat Interval Tuning

The default 10μs heartbeat interval balances:
- Lower interval → faster work distribution, more overhead
- Higher interval → less overhead, slower distribution when no idle workers

For specialized workloads, adjust via `ThreadPoolConfig.heartbeat_interval`.

## Building and Benchmarking

### Zig Blitz

```bash
# Build the library
cd core && zig build -Doptimize=ReleaseFast

# Run tests
cd core/src/blitz && zig test mod.zig

# Run benchmarks
cd core/src/blitz && zig build-exe bench.zig -O ReleaseFast && ./bench
```

### Rust Rayon (for comparison)

```bash
# Build and run Rayon benchmarks
cd core/src/blitz/rust_bench
cargo build --release
./target/release/blitz_bench
```

### Comparing Results

Both benchmark suites measure the same operations:
- Join overhead
- Parallel sum
- Parallel map
- Parallel reduce (find max)
- Parallel for (write indices)
- Parallel sort
- Parallel iterators

This allows direct comparison of Blitz vs Rayon performance.

## License

Part of the Galleon project. See repository root for license information.
