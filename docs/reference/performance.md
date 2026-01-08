# Performance Reference

This guide covers optimization strategies for getting the best performance from Galleon.

## Performance Characteristics

### Operation Complexity

| Operation | Time Complexity | Space Complexity |
|-----------|-----------------|------------------|
| Sum, Mean, Min, Max | O(n) | O(1) |
| Filter | O(n) | O(k) where k = matching rows |
| Sort | O(n log n) | O(n) |
| GroupBy | O(n) | O(g) where g = groups |
| Join (hash) | O(n + m) | O(min(n, m)) |
| Select columns | O(1) | O(1) (view) |

### SIMD Acceleration

Operations accelerated by SIMD (Single Instruction, Multiple Data):

| Operation | SIMD Speedup |
|-----------|--------------|
| Sum (Float64) | 4-8x |
| Min/Max (Float64) | 4-8x |
| Filter mask creation | 4-8x |
| Scalar arithmetic | 4-8x |
| Element-wise operations | 4-8x |

## Thread Configuration

### Optimal Thread Count

```go
// Auto-detect is usually optimal
galleon.SetMaxThreads(0)

// For CPU-bound workloads on dedicated machine
config := galleon.GetThreadConfig()
// config.MaxThreads == CPU cores

// For shared environments
galleon.SetMaxThreads(runtime.NumCPU() / 2)
```

### Thread Scaling

Performance scales with threads for:
- Large aggregations (> 100K rows)
- Filter operations
- GroupBy with many groups
- Parallel file scans

Overhead dominates for:
- Small datasets (< 10K rows)
- Single-group aggregations
- Column selections

## Memory Optimization

### Column Selection

```go
// Good: Select only needed columns
df = df.Select("id", "value", "category")

// Better: Use lazy select for large files
result, _ := galleon.ScanCSV("large.csv").
    Select(Col("id"), Col("value")).
    Collect()
```

### Filter Early

```go
// Good: Filter reduces data size first
result := df.
    Filter(Col("active").Eq(Lit(true))).
    GroupBy("category").
    Agg(Col("value").Sum())

// Less efficient: Process all data then filter
result := df.
    GroupBy("category").
    Agg(Col("value").Sum()).
    Filter(Col("sum").Gt(Lit(0)))
```

### Avoid Unnecessary Copies

```go
// View (no copy)
column := df.Column("value")  // Returns reference

// Copy (allocates new memory)
cloned := df.Clone()
```

## Data Type Optimization

### Use Smaller Types When Possible

```go
// Int32 vs Int64 (saves 50% memory)
small := galleon.NewSeriesInt32("counts", data32)
large := galleon.NewSeriesInt64("counts", data64)

// Float32 vs Float64 (saves 50% memory)
small := galleon.NewSeriesFloat32("values", data32)
large := galleon.NewSeriesFloat64("values", data64)
```

### Memory Usage by Type

| Type | Bytes per Element |
|------|-------------------|
| Bool | 1 bit + validity |
| Int32 | 4 + validity |
| Int64 | 8 + validity |
| Float32 | 4 + validity |
| Float64 | 8 + validity |
| String | Variable + offsets |

## I/O Performance

### File Format Comparison

| Format | Read Speed | Write Speed | Compression | Column Selection |
|--------|------------|-------------|-------------|------------------|
| Parquet | Fast | Moderate | Built-in | Yes (column pruning) |
| CSV | Moderate | Fast | None | No |
| JSON | Slow | Slow | None | No |

### Large File Processing

```go
// Use lazy scanning for large files
result, _ := galleon.ScanParquet("huge.parquet").
    Filter(Col("date").Gte(Lit("2024-01-01"))).
    Select(Col("id"), Col("amount")).
    Collect()

// Benefits:
// - Predicate pushdown: Only read matching row groups
// - Projection pushdown: Only read needed columns
// - Streaming: Process in chunks
```

### Parallel I/O

```go
// Parquet files with multiple row groups
// are read in parallel automatically
galleon.SetMaxThreads(8)
df, _ := galleon.ReadParquet("partitioned.parquet")
```

## GroupBy Optimization

### Minimize Group Count

```go
// Fewer groups = faster aggregation
// 100 groups: ~10ms
// 10,000 groups: ~100ms
// 1,000,000 groups: ~1000ms

// Optimize by binning continuous values
df = df.WithColumn("value_bin",
    Col("value").Div(Lit(10)).Cast(Int64).Mul(Lit(10)),
)
result := df.GroupBy("value_bin").Agg(Col("amount").Sum())
```

### Integer Keys

```go
// Fast: Integer group keys
result := df.GroupBy("category_id").Sum("value")

// Slower: String group keys (require hashing)
result := df.GroupBy("category_name").Sum("value")
```

## Join Optimization

### Smaller Table on Right

```go
// Join builds hash table from right DataFrame
// Put smaller table on right for best performance

// Good: small right table
result, _ := largeDf.Join(smallDf, On("key"))

// Less efficient: large right table
result, _ := smallDf.Join(largeDf, On("key"))
```

### Pre-filter Before Join

```go
// Good: Filter before join
filteredLeft := left.Filter(Col("active").Eq(Lit(true)))
result, _ := filteredLeft.Join(right, On("id"))

// Less efficient: Join then filter
result, _ := left.Join(right, On("id")).
    Filter(Col("active").Eq(Lit(true)))
```

### Use Integer Keys

```go
// Fast: Integer join keys
result, _ := orders.Join(customers, On("customer_id"))

// Slower: String join keys
result, _ := orders.Join(customers, On("customer_name"))
```

## Benchmarking Tips

### Warmup

```go
// Run operation once to warm up (JIT, caches)
_ = df.Column("value").Sum()

// Then benchmark
start := time.Now()
for i := 0; i < iterations; i++ {
    _ = df.Column("value").Sum()
}
elapsed := time.Since(start) / time.Duration(iterations)
```

### Disable GC During Benchmarks

```go
import "runtime/debug"

debug.SetGCPercent(-1)  // Disable GC
defer debug.SetGCPercent(100)  // Re-enable

// Run benchmark
```

### Use Go's Built-in Benchmarking

```go
func BenchmarkSum(b *testing.B) {
    series := galleon.NewSeriesFloat64("values", data)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _ = series.Sum()
    }
}
```

## Performance Comparison Guide

### When to Use Galleon

Galleon excels at:
- Numeric aggregations (SIMD-accelerated)
- Large-scale filtering
- Multi-threaded operations
- Memory-mapped file processing
- CGO overhead is acceptable

### When to Consider Alternatives

Consider native Go solutions for:
- Very small datasets (< 1000 rows)
- Simple operations where CGO overhead dominates
- When you need pure Go deployment

## Common Performance Pitfalls

### 1. Repeated Column Access

```go
// Bad: Repeated column lookup
for i := 0; i < n; i++ {
    col := df.Column("value")  // Lookup each iteration
    // ...
}

// Good: Cache column reference
col := df.Column("value")
for i := 0; i < n; i++ {
    // Use col
}
```

### 2. Unnecessary Type Conversions

```go
// Bad: Convert types repeatedly
for _, row := range rows {
    val := series.AsFloat64().Float64()[row]  // Converts each time
}

// Good: Convert once
floats := series.AsFloat64().Float64()
for _, row := range rows {
    val := floats[row]
}
```

### 3. Over-parallelization

```go
// Bad: Too many threads for small data
galleon.SetMaxThreads(32)
small := galleon.NewSeriesFloat64("x", make([]float64, 100))
_ = small.Sum()  // Parallel overhead exceeds computation

// Good: Match threads to workload
galleon.SetMaxThreads(0)  // Let auto-detection decide
```

## Performance Monitoring

### Memory Usage

```go
import "runtime"

var m runtime.MemStats
runtime.ReadMemStats(&m)
fmt.Printf("Alloc: %d MB\n", m.Alloc/1024/1024)
fmt.Printf("HeapAlloc: %d MB\n", m.HeapAlloc/1024/1024)
```

### Operation Timing

```go
func timeOp(name string, fn func()) {
    start := time.Now()
    fn()
    fmt.Printf("%s: %v\n", name, time.Since(start))
}

timeOp("Sum", func() { _ = series.Sum() })
timeOp("Filter", func() { _ = df.Filter(expr) })
timeOp("GroupBy", func() { _ = df.GroupBy("cat").Sum("val") })
```
