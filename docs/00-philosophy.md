# Philosophy

## Why Galleon Exists

Galleon was born from a simple observation: **Go deserves production-grade data science tooling.**

The data science ecosystem has long been dominated by Python, and for good reason—libraries like pandas, NumPy, and Polars have made data manipulation accessible and powerful. But Python's dynamic typing, GIL limitations, and deployment complexity create real challenges in production environments where reliability, performance, and operational simplicity matter.

Go, with its static typing, excellent concurrency model, and single-binary deployments, is increasingly the language of choice for backend services, infrastructure, and data pipelines. Yet when Go developers need to manipulate tabular data, they're often forced to shell out to Python, maintain polyglot systems, or settle for limited native options.

Galleon aims to change that.

## Design Philosophy

### The Right Tool for Each Layer

Galleon is built on a deliberate architectural choice: **Zig for compute, Go for interface**.

This isn't about using trendy technologies—it's about playing to each language's strengths:

**Zig handles the data layer** because:
- Memory lives entirely off the Go heap, avoiding GC pressure
- SIMD intrinsics enable vectorized operations across CPU architectures
- Manual memory management provides predictable, consistent performance
- Zero-cost abstractions mean no runtime overhead

**Go handles the API layer** because:
- Static typing catches errors at compile time
- The type system provides excellent IDE support and documentation
- Error handling is explicit and composable
- The ecosystem provides excellent tooling for testing, profiling, and deployment

This separation means users get the ergonomics of Go with the raw performance of hand-tuned native code.

### Why Not Pure Go?

Go is a wonderful language, but its garbage collector—while excellent for most workloads—is optimized for low-latency applications with short-lived allocations. DataFrames are the opposite: they hold large amounts of data in memory for extended periods, often for the lifetime of a request or even longer.

When the Go GC encounters a heap filled with millions of float64 values that won't be collected anytime soon, it still must scan and track them. This creates unnecessary overhead and unpredictable latency spikes. By keeping data in Zig-managed memory, we sidestep this entirely—the GC simply doesn't see our data.

Additionally, Go lacks access to SIMD intrinsics. While the compiler performs some auto-vectorization, it's no substitute for explicit SIMD operations. Summing a million floats with AVX-512 isn't just faster—it's a different category of performance.

### Why Not Pure Zig (or Rust, or C++)?

Raw performance without usability is a hollow victory.

Zig (and Rust, and C++) can certainly build fast DataFrame libraries. But they impose a significant learning curve and cognitive overhead. Manual memory management, while powerful, is also a source of bugs. The goal isn't to build the fastest possible library—it's to build the fastest *practical* library that real teams can adopt.

Go strikes a remarkable balance: it's safe enough to move quickly, typed enough to catch mistakes early, and simple enough that teams can maintain code they didn't write. By exposing Galleon through Go, we make high-performance data science accessible to the broader Go ecosystem without requiring everyone to become systems programmers.

### The CGO Bridge

The elephant in the room: CGO has costs. Each call across the Go-Zig boundary incurs overhead (~100-200ns). This is why Galleon is designed around **bulk operations**—we don't cross the boundary for each element, we cross it once per column or per operation.

This architectural constraint actually leads to better design. It encourages operations that work on entire arrays, which is exactly how SIMD works best. The overhead of one CGO call is trivial when amortized across millions of elements.

## Testing Philosophy

CGO introduces a category of bugs that Go's type system cannot catch. A mismatched function signature, an incorrect pointer cast, or a memory management error won't cause a compile error—it will cause a runtime crash, memory corruption, or silent data errors.

This is why Galleon maintains extensive test coverage:

- **Unit tests** verify individual operations behave correctly
- **Correctness tests** validate that SIMD results match scalar implementations
- **CGO validation tests** ensure header files match actual exports
- **Edge case tests** cover empty arrays, single elements, and boundary conditions
- **Type variant tests** verify all supported data types (Float64, Float32, Int64, Int32, Bool, String)

We test not because we're paranoid, but because the compiler can't save us here. Every untested code path is a potential production incident.

## Performance Philosophy

Galleon follows a principle borrowed from database engineering: **optimize for the common case, handle edge cases correctly**.

The common case in data science is:
- Columns with thousands to millions of rows
- Numeric operations (aggregations, filters, arithmetic)
- Sequential processing with occasional parallelism

We optimize aggressively for this workload:
- SIMD vectorization for numeric operations
- Parallel execution for large datasets
- Memory pooling to reduce allocation overhead
- Lazy evaluation to eliminate intermediate results

But we don't sacrifice correctness for speed. Operations on empty arrays, single elements, or unusual types might not be as optimized, but they produce correct results.

## The Broader Vision

Galleon is a first step, not a final destination.

The Go ecosystem needs more than DataFrames. It needs:
- Time series analysis
- Statistical modeling
- Machine learning inference
- Data visualization

Each of these can follow the same pattern: Zig (or Rust, or C) for compute-intensive operations, Go for the user-facing API. Galleon demonstrates that this architecture works—that you can have both performance and usability.

We believe Go can be a first-class language for data science. Not by replacing Python, but by providing a better option for teams that already use Go, that value type safety, that need predictable performance, and that want simpler deployments.

Galleon is our contribution to that future.

## Guiding Principles

1. **Correctness over cleverness** — A slow correct answer beats a fast wrong one
2. **Usability over flexibility** — Optimize for the common case, not every possible case
3. **Explicit over implicit** — Errors should be visible, not hidden
4. **Tested over trusted** — If it's not tested, it's broken
5. **Simple over complex** — The best code is code you don't have to write

---

*Galleon: Production-grade DataFrames for Go*
