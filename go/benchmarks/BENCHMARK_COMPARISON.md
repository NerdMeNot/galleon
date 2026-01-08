# Galleon vs Polars Performance Comparison

Benchmarks run on Apple M3 Pro.

## Summary Table

| Operation | Size | Polars (Rust) | Galleon (Go+Zig) | Polars Speedup |
|-----------|------|---------------|------------------|----------------|
| **GroupBy Sum** | 10K | 0.28 ms | 1.50 ms | 5.4x faster |
| | 100K | 0.88 ms | 17.01 ms | 19.3x faster |
| | 1M | 5.99 ms | 250.18 ms | 41.8x faster |
| **GroupBy Mean** | 10K | 0.26 ms | 1.45 ms | 5.6x faster |
| | 100K | 0.86 ms | 16.87 ms | 19.6x faster |
| | 1M | 5.97 ms | 253.07 ms | 42.4x faster |
| **GroupBy Min** | 10K | 0.25 ms | 1.53 ms | 6.1x faster |
| | 100K | 0.88 ms | 17.79 ms | 20.2x faster |
| | 1M | 6.45 ms | 530.92 ms | 82.3x faster |
| **GroupBy Max** | 10K | 0.26 ms | 1.50 ms | 5.8x faster |
| | 100K | 0.82 ms | 17.37 ms | 21.2x faster |
| | 1M | 6.43 ms | 297.01 ms | 46.2x faster |
| **GroupBy Count** | 10K | 0.13 ms | 1.43 ms | 11.0x faster |
| | 100K | 0.69 ms | 16.74 ms | 24.3x faster |
| | 1M | 6.57 ms | 335.69 ms | 51.1x faster |
| **GroupBy Multi-Agg** | 10K | 0.30 ms | 1.63 ms | 5.4x faster |
| | 100K | 1.00 ms | 18.57 ms | 18.6x faster |
| | 1M | 7.61 ms | 247.83 ms | 32.6x faster |
| **Inner Join** | 10K | 0.42 ms | 11.10 ms | 26.4x faster |
| | 100K | 2.06 ms | 117.48 ms | 57.0x faster |
| | 1M | 26.80 ms | 1502.45 ms | 56.1x faster |
| **Left Join** | 10K | 0.57 ms | 11.17 ms | 19.6x faster |
| | 100K | 2.14 ms | 119.46 ms | 55.8x faster |
| | 1M | 29.75 ms | 1516.55 ms | 51.0x faster |

## Analysis

### Why Polars is Faster

1. **Multi-threading**: Polars uses Rayon for parallel execution across all CPU cores. Galleon is currently single-threaded.

2. **Optimized Hash Tables**: Polars uses highly optimized hash tables with SIMD probing. Galleon uses Go's `map[uint64][]int`.

3. **Memory Layout**: Polars uses Apache Arrow columnar format with careful cache-line alignment. Galleon uses Go slices.

4. **String Hashing**: Galleon's groupby uses `fmt.Sprintf("%v", val)` for hashing which is slow. Polars has specialized hash functions.

5. **Lazy Evaluation**: Polars' query optimizer can fuse operations. Galleon executes eagerly.

### Where Galleon's Zig SIMD Helps

The Zig SIMD backend provides significant speedups for:
- **Column aggregations**: Min/Max are 2.6-3.9x faster than pure Go
- **Gather operations**: Used in join result materialization

### Opportunities for Improvement

1. **Parallel execution**: Add goroutine-based parallelism for GroupBy and Join
2. **Better hash tables**: Implement SIMD-accelerated hash tables in Zig
3. **Avoid string formatting**: Use type-specific hash functions
4. **LazyFrame**: Add query optimization and operation fusion
5. **Arrow integration**: Use Apache Arrow memory format

## Conclusion

Polars is **20-80x faster** than Galleon for most operations, primarily due to multi-threading and optimized data structures. However, Galleon provides:

- **Go-native API**: Natural for Go projects
- **Simple integration**: No Python/Rust dependency
- **Growing performance**: Zig SIMD backend provides foundation for optimization

For production workloads requiring maximum performance, Polars remains the better choice. For Go projects that need DataFrame operations without external dependencies, Galleon offers a simpler integration path with room for future optimization.
