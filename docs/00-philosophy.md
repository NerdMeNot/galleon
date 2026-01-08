# Philosophy

## Why Galleon Exists

Galleon was born from a simple observation: **Go deserves production-grade data science tooling.**

The data science ecosystem has long been dominated by Python, and for good reason—libraries like pandas, NumPy, and Polars have made data manipulation accessible and powerful. But Python's dynamic typing, GIL limitations, and deployment complexity create real challenges in production environments where reliability, performance, and operational simplicity matter.

Go, with its static typing, excellent concurrency model, and single-binary deployments, is increasingly the language of choice for backend services, infrastructure, and data pipelines. Yet when Go developers need to manipulate tabular data, they're often forced to shell out to Python, maintain polyglot systems, or settle for limited native options.

Galleon aims to change that.

## Why Not Python

Let's be direct: **Python is not a production-grade language**.

This is a controversial statement in 2024, but it's one born from experience deploying and maintaining systems at scale. Python excels at prototyping, exploration, and scripting. It has an unmatched ecosystem for data science and machine learning. But these strengths don't translate to production reliability.

### The Type Safety Problem

Python's dynamic typing is often sold as a feature—"duck typing," "flexibility," "rapid iteration." In practice, it means:

- **Bugs hide until runtime.** A typo in a variable name, a wrong argument order, a None where you expected a list—these errors don't surface until that code path executes. In production. At 3 AM.

- **Refactoring is terrifying.** Renaming a function? Changing a parameter? Good luck knowing what breaks. Your IDE can guess, your linters can try, but without types, nothing is certain.

- **Documentation lies.** Docstrings say one thing, code does another. Type hints help, but they're optional, unenforced, and frequently out of date.

- **Testing becomes mandatory for things the compiler should catch.** You write tests to verify that functions receive the right types—tests that wouldn't exist in a statically typed language because the compiler already guarantees it.

Go catches an entire category of bugs at compile time. Not "maybe catches with the right linter configuration"—*catches, always, before the code can run*.

### The GIL Problem

Python's Global Interpreter Lock is an embarrassment that the language has carried for over 30 years. In an era of 64-core machines, Python cannot execute two lines of Python code simultaneously within the same process.

Yes, you can use multiprocessing. Yes, you can use async/await. Yes, you can call into C extensions that release the GIL. But these are workarounds for a fundamental limitation, each with their own complexity:

- **Multiprocessing** means serialization overhead, memory duplication, and IPC complexity
- **Async/await** only helps with I/O-bound work, not CPU-bound computation
- **C extensions** mean you're not really writing Python anymore

Go was designed for concurrency. Goroutines and channels are first-class citizens. Spinning up thousands of concurrent operations is trivial and efficient. There's no GIL, no workarounds, no asterisks.

### The Performance Problem

Python is slow. Not "a bit slower"—**50-100x slower** than compiled languages for CPU-bound work. The standard response is "just call into NumPy/pandas," but this only helps when your workload maps cleanly onto their operations. The moment you need custom logic, a loop, or anything that can't be vectorized, you're back to Python speed.

More importantly, Python's performance is *unpredictable*. Memory allocation can trigger garbage collection at any time. String concatenation has pathological cases. The same code can be fast or slow depending on object sizes, reference counts, and the phase of the moon.

Galleon, running on Zig's deterministic memory management and Go's efficient runtime, provides consistent, predictable performance. When you benchmark it, that's the performance you get in production.

### The Deployment Problem

Deploying Python is an exercise in dependency hell:

- Virtual environments that mysteriously break
- Package versions that conflict
- System Python vs. user Python vs. pyenv Python
- Native extensions that need specific compilers
- Docker images that balloon to gigabytes

Go produces a single static binary. Copy it to the server. Run it. There's no runtime to install, no dependencies to manage, no virtualenv to activate. It just works.

### The Hidden Infrastructure Tax

Python's limitations have spawned an entire ecosystem of workarounds:

- **Cython** to make Python fast (by not writing Python)
- **mypy/pyright** to add type checking (that the language should have)
- **Celery/RQ** to work around the GIL for background tasks
- **Dask/Ray** to scale beyond a single process
- **Poetry/pip-tools/conda** to manage the dependency nightmare

Each tool adds complexity. Each requires expertise to configure correctly. Each is another thing that can break.

Go needs none of this. The language, the toolchain, and the standard library provide what you need out of the box.

### Python's Place

None of this means Python is useless. For:

- **Jupyter notebooks and exploration** — Python is excellent
- **ML model training** — The ecosystem is unmatched
- **Quick scripts and automation** — It's productive and readable
- **Prototyping** — Moving fast matters

But for production services that process data, serve requests, and need to be reliable at scale? Go is the better choice. And now, with Galleon, Go can handle the data science workloads too.

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
