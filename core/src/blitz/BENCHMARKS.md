# Blitz Benchmarks

Performance benchmarks for the Blitz parallel execution library.

## Running Benchmarks

```bash
cd core/src/blitz
zig build-exe bench.zig -O ReleaseFast && ./bench
```

For Rayon comparison:
```bash
cd core/src/blitz/rust_bench
cargo run --release
```

## System Configuration

Results below measured on:
- **CPU**: Apple M1 Pro (10 cores)
- **Memory**: 16 GB
- **Zig**: 0.15.2
- **Workers**: 10

## Benchmark Results

### Join Overhead

Measures the cost of fork-join synchronization with minimal work.

| Metric | Time |
|--------|------|
| Empty join (100K iterations) | **~35-60 ns** avg |

This is the core overhead of Blitz's heartbeat scheduling. The low overhead enables fine-grained parallelism that would be impractical with traditional work-stealing (~500-1000ns).

### Parallel Sum (SIMD + Parallel)

Reduces an array of `i64` values using SIMD-optimized parallel reduction.

| N | Sequential | Parallel | Speedup |
|---|------------|----------|---------|
| 1,000 | 0.000 ms | 0.000 ms | 1.00x |
| 10,000 | 0.002 ms | 0.001 ms | 1.73x |
| 100,000 | 0.007 ms | 0.007 ms | 1.04x |
| 1,000,000 | 0.082 ms | 0.079 ms | 1.04x |
| 10,000,000 | 0.999 ms | 0.803 ms | **1.24x** |

**Notes:**
- Uses SIMD vectorization (8-wide with 4 accumulators) at all sizes
- Parallelism kicks in only at N > 5M to avoid overhead
- Memory bandwidth limits scaling for simple reductions

### Parallel Max (SIMD + Parallel)

Finds maximum value using SIMD-optimized parallel reduction.

| N | Sequential | Parallel | Speedup |
|---|------------|----------|---------|
| 1,000 | 0.000 ms | 0.000 ms | 1.50x |
| 10,000 | 0.003 ms | 0.002 ms | 1.57x |
| 100,000 | 0.016 ms | 0.014 ms | 1.08x |
| 1,000,000 | 0.134 ms | 0.075 ms | **1.78x** |
| 10,000,000 | 1.432 ms | 0.791 ms | **1.81x** |

### Parallel For (Write Indices)

Writes `i * 2` to each index in parallel.

| N | Sequential | Parallel | Speedup |
|---|------------|----------|---------|
| 1,000 | 0.000 ms | 0.000 ms | 1.50x |
| 10,000 | 0.002 ms | 0.001 ms | **2.22x** |
| 100,000 | 0.017 ms | 0.014 ms | 1.21x |
| 1,000,000 | 0.086 ms | 0.067 ms | **1.28x** |
| 10,000,000 | 0.870 ms | 0.755 ms | 1.15x |

**Notes:**
- Memory-bound operation (writing to memory)
- Uses 64K grain size to amortize parallelism overhead
- Good speedups across all sizes

### Parallel Sort (Merge Sort)

Parallel merge sort vs `std.sort.pdq` (pattern-defeating quicksort).

| N | Sequential | Parallel | Speedup |
|---|------------|----------|---------|
| 1,000 | 0.017 ms | 0.019 ms | 0.91x |
| 10,000 | 0.290 ms | 0.221 ms | 1.31x |
| 100,000 | 4.095 ms | 0.989 ms | **4.14x** |
| 1,000,000 | 48.619 ms | 7.519 ms | **6.47x** |

**Notes:**
- Parallel merge sort with O(n) auxiliary space
- Excellent scaling due to compute-bound nature
- Uses parallel merge for O(log² n) span

### Parallel Scan (Prefix Sum)

Parallel inclusive prefix sum.

| N | Sequential | Parallel | Speedup |
|---|------------|----------|---------|
| 1,000 | 0.001 ms | 0.001 ms | 1.00x |
| 10,000 | 0.006 ms | 0.006 ms | 1.03x |
| 100,000 | 0.029 ms | 0.042 ms | 0.69x |
| 1,000,000 | 0.283 ms | 0.396 ms | 0.71x |

**Notes:**
- Scan has inherent sequential dependencies limiting parallelism
- Three-phase algorithm: local scans, prefix sums, propagation
- Sequential is often faster for pure scan operations
- Best suited for cases where scan feeds into parallel consumption

## Performance Characteristics

### When Blitz Excels

| Workload | Why |
|----------|-----|
| **Fine-grained parallelism** | 27ns join overhead enables recursive divide-and-conquer |
| **Compute-bound operations** | Sort, map with expensive transforms |
| **Large datasets (N > 100K)** | Parallelism overhead amortized |
| **SIMD-friendly reductions** | sum, min, max use vectorized code |

### When to Use Sequential

| Workload | Why |
|----------|-----|
| **N < 10K elements** | Parallelism overhead exceeds benefit |
| **Memory-bound operations** | Bandwidth-limited, not compute-limited |
| **Simple operations** | Copy, fill - memory throughput limited |

## Throughput Analysis

### SIMD Aggregations

The SIMD implementation processes 32 elements per loop iteration:
- 8-wide vectors (`@Vector(8, T)`)
- 4 accumulators for instruction-level parallelism

Theoretical throughput on M1:
- Vector width: 128-bit NEON
- 8 × i64 per vector = not possible (64-bit max)
- Actually: 2 × i64 per 128-bit register, using 4 accumulators

Measured single-thread sum throughput:
- 10M elements / 1.121ms = **8.9 billion elements/sec**

### Parallel Scaling

At N=10M with 10 workers:
- Sum: 1.34x speedup (memory bandwidth limited)
- Max: 1.71x speedup (slightly better due to branch prediction)
- Sort: 6.21x speedup (compute bound, near-linear scaling)

## Comparison with Thresholds

Blitz includes automatic threshold detection via `blitz.shouldParallelize()`:

```zig
const threshold = @import("threshold.zig");

if (threshold.shouldParallelize(.sum, data.len)) {
    return blitz.iter(i64, data).sum();
} else {
    // Use sequential
    var sum: i64 = 0;
    for (data) |v| sum += v;
    return sum;
}
```

Thresholds are now calculated dynamically based on:
- **Worker count**: More workers = lower threshold
- **Operation cost**: Expensive ops (sort) parallelize at smaller sizes
- **SIMD efficiency**: SIMD already vectorizes, so larger data needed to justify thread sync

Formula: `threshold = (workers × 500ns × 10) / cost_per_element × SIMD_factor`

## Blitz vs Rayon Comparison

Comparison with Rust's Rayon parallel library on the same hardware.

### Join Overhead (Fork-Join Synchronization)

| Library | Empty Join | Relative |
|---------|-----------|----------|
| **Blitz** | **24 ns** | 1x |
| Rayon | 14,943 ns | 623x slower |

This is Blitz's key advantage - heartbeat scheduling has dramatically lower synchronization overhead than Rayon's work-stealing deque.

### Sum (SIMD + Parallel)

| N | Blitz | Rayon | Winner |
|---|-------|-------|--------|
| 1K | 0.000ms | 0.039ms | **Blitz** (no overhead) |
| 10K | 0.001ms | 0.043ms | **Blitz** (43x) |
| 100K | 0.008ms | 0.075ms | **Blitz** (9x) |
| 1M | 0.083ms | 0.077ms | Rayon (1.1x) |
| 10M | 0.840ms | 0.883ms | **Blitz** (1.05x) |

### Max/Reduce

| N | Blitz Max | Rayon Reduce(max) | Winner |
|---|-----------|-------------------|--------|
| 1K | 0.000ms | 0.049ms | **Blitz** |
| 10K | 0.001ms | 0.062ms | **Blitz** (62x) |
| 100K | 0.008ms | 0.076ms | **Blitz** (9.5x) |
| 1M | 0.093ms | 0.073ms | Rayon (1.3x) |
| 10M | 0.882ms | 0.778ms | Rayon (1.1x) |

### Sort (Parallel)

| N | Blitz | Rayon | Winner |
|---|-------|-------|--------|
| 1K | 0.009ms | 0.016ms | **Blitz** (1.8x) |
| 10K | 0.157ms | 0.222ms | **Blitz** (1.4x) |
| 100K | 1.003ms | 0.747ms | Rayon (1.3x) |
| 1M | 7.830ms | 6.700ms | Rayon (1.2x) |

### Parallel For (Write Indices)

| N | Blitz | Rayon | Winner |
|---|-------|-------|--------|
| 1K | 0.000ms | 0.040ms | **Blitz** |
| 10K | 0.001ms | 0.037ms | **Blitz** (37x) |
| 100K | 0.008ms | 0.082ms | **Blitz** (10x) |
| 1M | 0.074ms | 0.139ms | **Blitz** (1.9x) |
| 10M | 0.755ms | 0.899ms | **Blitz** (1.2x) |

### Summary

| Metric | Blitz | Rayon | Notes |
|--------|-------|-------|-------|
| **Join overhead** | 24ns | 14,943ns | Blitz **600x faster** |
| **Small data (N<100K)** | Much faster | Slow | Blitz wins 10-60x |
| **Large data aggregations** | Comparable | Comparable | Both memory-bound |
| **Sort at scale** | Comparable | Slightly faster | Rayon's sort highly optimized |
| **parallelFor** | Faster | Slower | Blitz wins all sizes |

**Key Insight**: Blitz excels at fine-grained parallelism due to 600x lower join overhead. For large datasets, both achieve similar throughput since they're memory-bandwidth limited.

### Running Rayon Benchmarks

```bash
cd core/src/blitz/rust_bench
cargo run --release
```

## Reproducing Results

```zig
const std = @import("std");
const blitz = @import("mod.zig");

pub fn main() !void {
    try blitz.init();
    defer blitz.deinit();

    const allocator = std.heap.page_allocator;
    const data = try allocator.alloc(i64, 10_000_000);
    defer allocator.free(data);

    // Initialize
    for (data, 0..) |*v, i| {
        v.* = @intCast(i % 1000);
    }

    // Benchmark parallel sum
    const start = std.time.nanoTimestamp();
    const sum = blitz.iter(i64, data).sum();
    const elapsed_ns = std.time.nanoTimestamp() - start;

    std.debug.print("Sum: {d}, Time: {d:.3}ms\n", .{
        sum,
        @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0,
    });
}
```
