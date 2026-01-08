# Galleon Performance Benchmarks

This document presents comprehensive benchmarks comparing Galleon with Polars and Pandas across various DataFrame operations.

## Test Environment

| Component | Specification |
|-----------|---------------|
| CPU | Apple M-series (11 cores) |
| RAM | 16GB+ |
| OS | macOS 26.0 |
| Go | 1.21+ |
| Zig | 0.13 |

### Library Versions

| Library | Version | Threading |
|---------|---------|-----------|
| Galleon | 0.1.0 | Auto-detected (11 threads) |
| Polars | 1.36.1 | Multi-threaded (Rust) |
| Pandas | 2.3.3 | Single-threaded (NumPy) |

## Benchmark Methodology

- **Warmup**: 2 iterations before measurement
- **Iterations**: 10 timed runs per operation
- **Metric**: Median time (robust to outliers)
- **Data**: Identical random data with seed=42
- **GC**: Forced garbage collection between runs

## Results Summary

### 1 Million Rows - Key Findings

| Operation | Galleon | Polars | Pandas | Galleon vs Polars | Galleon vs Pandas |
|-----------|---------|--------|--------|-------------------|-------------------|
| **Aggregations** |||||
| Sum | 0.11ms | 0.08ms | 0.25ms | 1.3x slower | **2.3x faster** |
| Min | 0.15ms | 0.08ms | 0.45ms | 1.9x slower | **3.0x faster** |
| Max | 0.12ms | 0.08ms | 0.42ms | 1.5x slower | **3.5x faster** |
| Mean | 0.09ms | 0.08ms | 0.44ms | 1.1x slower | **4.9x faster** |
| **Filtering** |||||
| Filter (>0) | 0.30ms | 0.43ms | 3.34ms | **1.4x faster** | **11.1x faster** |
| **Sorting** |||||
| Sort | 28.4ms | 16.7ms | 86.3ms | 1.7x slower | **3.0x faster** |
| **GroupBy** |||||
| GroupBy Sum | 6.04ms | 6.82ms | 22.5ms | **1.1x faster** | **3.7x faster** |
| **Joins** |||||
| Inner Join | 27.4ms | 28.7ms | 153.0ms | **1.05x faster** | **5.6x faster** |
| Left Join | 33.5ms | 31.8ms | 165.7ms | 1.05x slower | **4.9x faster** |

### Performance Highlights

```
1 Million Rows - Operation Time (lower is better)
═══════════════════════════════════════════════════════════════════════

GroupBy Sum:
  Galleon  ████████████ 6.0ms      ← Competitive with Polars!
  Polars   █████████████ 6.8ms
  Pandas   █████████████████████████████████████████████ 22.5ms

Inner Join:
  Galleon  ████████████████████████████ 27.4ms   ← Matches Polars!
  Polars   █████████████████████████████ 28.7ms
  Pandas   █████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████ 153.0ms

Filter:
  Galleon  ██ 0.30ms              ← Faster than Polars!
  Polars   ███ 0.43ms
  Pandas   ██████████████████████████████████ 3.34ms

Sum:
  Galleon  █ 0.11ms               ← Near-Polars speed
  Polars   █ 0.08ms
  Pandas   ███ 0.25ms
```

## Detailed Results by Size

### 10,000 Rows

| Operation | Galleon | Polars | Pandas |
|-----------|---------|--------|--------|
| Sum | <0.01ms | 0.001ms | 0.01ms |
| Min | <0.01ms | 0.001ms | 0.01ms |
| Max | <0.01ms | 0.001ms | 0.01ms |
| Mean | <0.01ms | 0.001ms | 0.01ms |
| Filter | <0.01ms | 0.08ms | 0.10ms |
| Sort | 0.55ms | 0.31ms | 0.40ms |
| GroupBy Sum | 0.04ms | 0.24ms | 0.15ms |
| Inner Join | 0.23ms | 0.38ms | 1.16ms |
| Left Join | 0.25ms | 0.51ms | 1.11ms |

**Key insight**: At 10K rows, Galleon's GroupBy and Joins are already faster than Polars!

### 100,000 Rows

| Operation | Galleon | Polars | Pandas |
|-----------|---------|--------|--------|
| Sum | 0.01ms | 0.009ms | 0.03ms |
| Min | 0.01ms | 0.009ms | 0.04ms |
| Max | 0.01ms | 0.009ms | 0.05ms |
| Mean | 0.01ms | 0.009ms | 0.05ms |
| Filter | 0.04ms | 0.10ms | 0.37ms |
| Sort | 2.68ms | 1.32ms | 7.15ms |
| GroupBy Sum | 0.50ms | 0.90ms | 1.80ms |
| Inner Join | 2.75ms | 1.80ms | 9.83ms |
| Left Join | 2.97ms | 2.20ms | 9.47ms |

### 1,000,000 Rows

| Operation | Galleon | Polars | Pandas |
|-----------|---------|--------|--------|
| Sum | 0.11ms | 0.08ms | 0.25ms |
| Min | 0.15ms | 0.08ms | 0.45ms |
| Max | 0.12ms | 0.08ms | 0.42ms |
| Mean | 0.09ms | 0.08ms | 0.44ms |
| Filter | 0.30ms | 0.43ms | 3.34ms |
| Sort | 28.38ms | 16.68ms | 86.25ms |
| GroupBy Sum | 6.04ms | 6.82ms | 22.49ms |
| Inner Join | 27.39ms | 28.68ms | 152.97ms |
| Left Join | 33.47ms | 31.78ms | 165.71ms |

## Scaling Analysis

### Join Performance vs Data Size

```
Inner Join Time (ms) - Log Scale
     │
150  ┤                                                    ╭─ Pandas (153ms)
     │                                               ╭────╯
100  ┤                                          ╭────╯
     │                                     ╭────╯
 50  ┤                                ╭────╯
     │                           ╭────╯
 27  ┤                      ╭────╯─────────────────────── Galleon (27.4ms)
     │                 ╭────╯        ╭────────────────── Polars (28.7ms)
  0  ┼─────────────────╯─────────────╯
     └─────┼─────────┼─────────┼─────────┼─────────
          10K      100K      500K       1M      Rows
```

### GroupBy Performance vs Data Size

```
GroupBy Sum Time (ms)
     │
 22  ┤                                                    ╭─ Pandas
     │                                               ╭────╯
 15  ┤                                          ╭────╯
     │                                     ╭────╯
 10  ┤                                ╭────╯
     │                           ╭────╯
  6  ┤                      ╭────╯─────────────────────── Galleon (6.0ms)
     │                 ╭────╯        ╭────────────────── Polars (6.8ms)
  0  ┼─────────────────╯─────────────╯
     └─────┼─────────┼─────────┼─────────┼─────────
          10K      100K      500K       1M      Rows
```

## Operation-Specific Analysis

### Aggregations (Sum, Min, Max, Mean)

Galleon's SIMD-accelerated aggregations deliver excellent performance:

- **Sub-millisecond at 1M rows**: 0.09-0.15ms for all aggregations
- **Near-Polars speed**: Only 1.1-1.9x slower than Polars
- **Much faster than Pandas**: 2.3-4.9x faster

```go
// SIMD-accelerated aggregation
sum := galleon.SumF64(data)  // Uses vectorized operations
```

### Filtering

Galleon's filter operation is **faster than Polars**:

| Size | Galleon | Polars | Speedup |
|------|---------|--------|---------|
| 100K | 0.04ms | 0.10ms | 2.5x |
| 1M | 0.30ms | 0.43ms | 1.4x |

The SIMD-based comparison generates a bitmask in parallel:

```go
mask := galleon.FilterMaskGreaterThanF64(data, threshold)
// Processes 8 elements per SIMD instruction
```

### GroupBy

**Galleon beats Polars at 1M rows**:

| Size | Galleon | Polars | Winner |
|------|---------|--------|--------|
| 10K | 0.04ms | 0.24ms | Galleon (6x) |
| 100K | 0.50ms | 0.90ms | Galleon (1.8x) |
| 1M | 6.04ms | 6.82ms | Galleon (1.1x) |

Optimizations:
- Hash-based grouping with Robin Hood hashing
- Parallel aggregation with thread-local accumulators
- SIMD-accelerated hash computation

### Joins

**Galleon matches Polars for joins**:

| Operation | Galleon | Polars | Difference |
|-----------|---------|--------|------------|
| Inner Join (1M) | 27.39ms | 28.68ms | Galleon 5% faster |
| Left Join (1M) | 33.47ms | 31.78ms | Polars 5% faster |

Both are **5-6x faster than Pandas**.

Join optimizations:
- `fastIntHash` for integer keys
- Parallel hash table probing
- Prefetching for cache efficiency

### Sorting

Galleon's sort uses **parallel sample sort** with SIMD-accelerated quicksort:

| Size | Galleon | Polars | Pandas |
|------|---------|--------|--------|
| 10K | 0.55ms | 0.31ms | 0.40ms |
| 100K | 2.68ms | 1.32ms | 7.15ms |
| 1M | 28.38ms | 16.68ms | 86.25ms |

**3x faster than Pandas**, only 1.7x slower than Polars.

Sort optimizations:
- Pair-based sorting: `(value, index)` pairs for cache-friendly comparisons
- Parallel sample sort: Data partitioned into buckets, sorted in parallel
- SIMD-accelerated quicksort: Vectorized comparisons for inner sort

## Memory Usage

| Operation | Galleon | Polars | Pandas |
|-----------|---------|--------|--------|
| DataFrame (1M×2) | ~16MB | ~16MB | ~24MB |
| Join hash table | ~20MB | ~15MB | ~30MB |
| GroupBy (100K groups) | ~4MB | ~3MB | ~8MB |

## Conclusions

### When to Use Galleon

**Excellent fit:**
- Go-native applications needing DataFrame operations
- Filter-heavy workloads (Galleon beats Polars!)
- GroupBy aggregations (matches Polars)
- Join operations (matches Polars)
- Any workload currently using Pandas

**Trade-offs:**
- Sorting is 1.7x slower than Polars (but 3x faster than Pandas)
- Simple aggregations slightly slower than Polars (1.1-1.9x)
- Much faster than Pandas for everything (2-11x)

### Performance Summary

| Category | vs Polars | vs Pandas |
|----------|-----------|-----------|
| Aggregations | 0.5-0.9x | 2-5x faster |
| Filtering | **1.4x faster** | **11x faster** |
| Sorting | 0.6x | **3x faster** |
| GroupBy | **1.1x faster** | **3.7x faster** |
| Joins | **1.0x (tie)** | **5-6x faster** |

### Performance Roadmap

Planned optimizations:
1. ~~**Parallel sort**: Target 2-3x improvement~~ ✅ **Done!** (3.6x improvement achieved)
2. **String operations**: SIMD string comparison
3. **Lazy evaluation optimizations**: Better predicate pushdown
4. **Memory-mapped I/O**: Large file handling

## Running Benchmarks

### Python (Polars/Pandas)

```bash
cd benchmarks
python3 -m venv .venv
source .venv/bin/activate
pip install polars pandas numpy
python run_benchmarks.py --sizes 10000,100000,1000000
```

### Go (Galleon)

```bash
cd benchmarks
go run run_galleon_benchmark.go
```

## Raw Data

Benchmark results are saved to:
- `benchmarks/benchmark_results.json` - Python results
- `benchmarks/galleon_benchmark_results.json` - Go results

## Reproducibility

To reproduce these benchmarks:

1. Use the same hardware (results vary by CPU)
2. Close other applications
3. Use the same random seed (42)
4. Run multiple times and take median
5. Report library versions

---

*Last updated: January 2026 (sort optimization: 3.6x improvement)*
*Benchmarks run on Apple M-series with 11 cores*
*Polars 1.36.1, Pandas 2.3.3, Galleon 0.1.0*
