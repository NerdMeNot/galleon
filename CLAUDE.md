# Claude Instructions for Galleon

This file provides context and guidelines for AI assistants (Claude, Cursor, etc.) working with the Galleon codebase.

## Project Overview

Galleon is a high-performance DataFrame library for Go with a Zig SIMD backend. It uses CGO to bridge Go's high-level API with Zig's low-level vectorized operations.

## Directory Structure

```
galleon/
├── core/                    # Zig SIMD backend (DO NOT modify without understanding CGO implications)
│   ├── src/
│   │   ├── main.zig         # CGO exports (C ABI functions)
│   │   ├── simd.zig         # SIMD module entry point
│   │   ├── simd/            # SIMD operations (modular)
│   │   │   ├── mod.zig          # Module exports
│   │   │   ├── core.zig         # Core types and constants
│   │   │   ├── aggregations.zig # Sum, min, max, mean
│   │   │   ├── arithmetic.zig   # Add, sub, mul, div
│   │   │   ├── comparisons.zig  # Gt, lt, eq comparisons
│   │   │   ├── filters.zig      # Filter operations
│   │   │   ├── hashing.zig      # Hash functions
│   │   │   ├── joins.zig        # Join algorithms
│   │   │   ├── sorting.zig      # Sort operations
│   │   │   ├── gather.zig       # Gather/scatter
│   │   │   └── groupby_agg.zig  # GroupBy SIMD aggregations
│   │   ├── groupby.zig      # GroupBy hash tables
│   │   └── column.zig       # Generic column storage types
│   ├── include/
│   │   └── galleon.h        # C header file for CGO (must match main.zig exports)
│   └── build.zig            # Zig build configuration
├── go/                      # Go package
│   ├── galleon.go           # CGO bindings and low-level SIMD wrappers
│   ├── series.go            # Series type (single column)
│   ├── dataframe.go         # DataFrame type (collection of Series)
│   ├── dtype.go             # Type system (DType enum)
│   ├── groupby.go           # GroupBy implementation
│   ├── join.go              # Join implementations
│   ├── expr.go              # Expression system for lazy evaluation
│   ├── lazyframe.go         # LazyFrame API
│   ├── lazy_executor.go     # Plan execution
│   ├── lazy_optimizer.go    # Query optimization
│   ├── parallel.go          # Morsel-based parallel execution
│   ├── pool.go              # Memory pooling
│   ├── io_*.go              # I/O operations (CSV, JSON, Parquet)
│   └── benchmarks/          # Performance tests
├── docs/                    # Documentation
│   ├── 00-philosophy.md
│   ├── 01-getting-started/
│   ├── 02-guides/
│   ├── 03-api/
│   └── 04-reference/
├── WHITEPAPER.md            # Detailed technical documentation
└── README.md                # User-facing documentation
```

## Key Concepts

### CGO Interface
- Zig functions are exported with C ABI in `main.zig`
- C header `galleon.h` declares these functions
- Go calls them via CGO in `galleon.go`
- **Critical**: Header must match Zig exports exactly

### Memory Management
- Zig allocates using `c_allocator` (libc malloc)
- Go holds unsafe pointers to Zig memory
- `runtime.SetFinalizer` ensures cleanup
- Zero-copy: Go slices view Zig memory directly

### Threading
- `MAX_THREADS = 32` (compile-time constant for arrays)
- `configured_max_threads` (runtime variable, 0 = auto-detect)
- Use `getMaxThreads()` to get effective thread count
- Go side: `SetMaxThreads(n)`, `GetMaxThreads()`

### SIMD Patterns
- `VECTOR_WIDTH = 8` elements per vector
- `UNROLL_FACTOR = 4` vectors per loop iteration
- Multi-accumulator pattern for ILP (instruction-level parallelism)
- `fastIntHash` for joins, `rapidHash64` for general hashing

## Common Tasks

### Adding a New SIMD Operation

1. Implement in the appropriate `core/src/simd/*.zig` module:
   ```zig
   // e.g., in aggregations.zig for aggregation operations
   pub fn newOperation(data: []const f64) f64 {
       // SIMD implementation
   }
   ```

2. Export from `core/src/simd/mod.zig` if needed:
   ```zig
   pub const newOperation = aggregations.newOperation;
   ```

3. Export with C ABI in `core/src/main.zig`:
   ```zig
   export fn galleon_new_operation(data: [*]const f64, len: usize) f64 {
       return simd.newOperation(data[0..len]);
   }
   ```

4. Declare in `core/include/galleon.h`:
   ```c
   double galleon_new_operation(const double* data, size_t len);
   ```

5. Build Zig: `cd core && zig build`

6. Wrap in `go/galleon.go`:
   ```go
   func NewOperation(data []float64) float64 {
       return float64(C.galleon_new_operation(
           (*C.double)(unsafe.Pointer(&data[0])),
           C.size_t(len(data)),
       ))
   }
   ```

### Adding a New DataFrame Operation

1. Add method to `go/dataframe.go`:
   ```go
   func (df *DataFrame) NewOp() *DataFrame {
       // Implementation
   }
   ```

2. Add tests to `go/dataframe_test.go`

### Modifying Thread Configuration

- Zig side: `core/src/simd.zig` (getMaxThreads, setMaxThreads)
- CGO export: `core/src/main.zig`
- Header: `core/include/galleon.h`
- Go wrapper: `go/galleon.go`

## Build Commands

```bash
# Build Zig library (required after any Zig changes)
cd core && zig build -Doptimize=ReleaseFast

# Build Go package
cd go && go build ./...

# Run tests
cd go && go test ./...

# Run specific test
cd go && go test -v -run TestJoin

# Run benchmarks
cd go && go test -bench=. ./benchmarks/
```

## Important Files to Understand

1. **core/src/main.zig** - All CGO exports (C ABI entry points)
2. **core/src/simd/mod.zig** - SIMD module exports
3. **core/src/simd/joins.zig** - Join algorithms (inner, left, right)
4. **core/src/simd/aggregations.zig** - SIMD aggregations (sum, min, max, mean)
5. **core/include/galleon.h** - C interface (must match main.zig)
6. **go/galleon.go** - CGO wrappers and low-level operations
7. **go/join.go** - Join orchestration logic
8. **go/groupby.go** - GroupBy orchestration logic

## Performance Considerations

- Avoid CGO calls in tight loops (overhead ~100ns per call)
- Prefer end-to-end operations (single CGO call)
- Use parallel operations for data > 10K rows
- Profile with `go test -bench -cpuprofile`

## Benchmarking Guidelines

**CRITICAL: The ONLY proper way to run comparative benchmarks is using the containerized benchmark.**

```bash
# Build the benchmark container (from repo root)
podman build -f go/benchmarks/Dockerfile -t galleon-bench .

# Run comparative benchmarks (Galleon vs Polars vs Pandas)
podman run --rm galleon-bench
```

**Why container-only?**
- Polars and Galleon run in the SAME environment with identical resources
- Local `go test -bench=...` only measures Galleon, not Polars
- Container ensures reproducible, fair comparisons

**Local benchmarks are ONLY for:**
- Quick iteration during development
- Galleon-to-Galleon comparisons (before/after a change)
- NOT for comparing against Polars numbers

**Current performance gaps (container, 1M elements):**
- Sort F64: Galleon ~20ms vs Polars ~5ms (3.8x slower) - uses parallel quicksort
- Argsort F64: Galleon ~86ms vs Polars ~18ms (4.8x slower)
- Left Join: Galleon ~21ms vs Polars ~5ms (4.2x slower)
- Inner Join: Galleon ~4ms vs Polars ~2.7ms (1.5x slower)
- Median: Galleon ~4.4ms vs Polars ~2.5ms (1.8x slower)

## Testing Changes

Always verify after changes:
1. `cd core && zig build` - Zig compiles
2. `cd go && go build ./...` - Go compiles
3. `cd go && go test ./...` - Tests pass
4. Run relevant benchmarks to check performance

## Common Pitfalls

1. **Header mismatch**: If Zig exports don't match header, CGO fails silently
2. **Memory leaks**: Zig allocations need explicit free or Go finalizer
3. **Pointer validity**: Go pointers must stay valid during CGO calls
4. **Thread safety**: Zig global state uses atomic operations

## Reference

- [Philosophy](docs/00-philosophy.md) - Why Galleon exists and design decisions
- [WHITEPAPER.md](WHITEPAPER.md) - Full technical documentation
- [Full Documentation](docs/README.md) - Installation, guides, and API reference
- [Zig Documentation](https://ziglang.org/documentation/)
- [CGO Documentation](https://pkg.go.dev/cmd/cgo)
