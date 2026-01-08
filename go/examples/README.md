# Galleon Examples

This directory contains example programs demonstrating various features of Galleon.

## Running Examples

Each example is a standalone Go program. From the `go` directory, run:

```bash
go run ./examples/01_basic_usage/main.go
```

Or run all examples:

```bash
for ex in examples/*/main.go; do echo "=== $ex ===" && go run $ex; done
```

## Examples Overview

### 01_basic_usage

Fundamental operations:
- Creating Series (Int64, Float64, String)
- Creating DataFrames
- Basic aggregations (Sum, Min, Max, Mean)
- Accessing data

### 02_filtering_sorting

Data manipulation:
- Filtering with boolean masks
- Filtering by indices
- Sorting DataFrames
- Selecting and dropping columns
- Head/Tail operations

### 03_groupby

GroupBy operations:
- Single-key groupby
- Multiple aggregations
- Multi-key groupby
- Chaining with filters

### 04_joins

Join operations:
- Inner joins
- Left joins
- Joins with different column names
- Large-scale joins
- Join + aggregation pipelines

### 05_lazy_evaluation

Lazy evaluation:
- LazyFrame API
- Deferred execution
- Complex query building
- Expression construction

### 06_io_operations

I/O operations:
- CSV read/write
- JSON read/write
- Parquet read/write
- Type specification
- Lazy scanning

## Quick Reference

### Creating DataFrames

```go
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesFloat64("value", []float64{1.5, 2.5, 3.5}),
    galleon.NewSeriesString("name", []string{"a", "b", "c"}),
)
```

### Filtering

```go
// Mask-based filtering
values := df.ColumnByName("value").Float64()
mask := galleon.FilterMaskGreaterThanF64(values, 2.0)
byteMask := make([]byte, len(mask))
for i, m := range mask {
    if m { byteMask[i] = 1 }
}
filtered, _ := df.FilterByMask(byteMask)

// Index-based filtering
indices := galleon.FilterGreaterThanF64(values, 2.0)
filtered, _ := df.FilterByIndices(indices)
```

### GroupBy

```go
// Single aggregation
result, _ := df.GroupBy("category").Sum("value")

// Multiple aggregations
result, _ := df.GroupBy("category").Agg(
    galleon.AggSum("value").Alias("total"),
    galleon.AggMean("value").Alias("average"),
)
```

### Joins

```go
// Same column name
result, _ := left.Join(right, galleon.On("id"))

// Different column names
result, _ := left.Join(right,
    galleon.LeftOn("order_id").RightOn("id"),
)
```

### Lazy Evaluation

```go
result, _ := df.Lazy().
    Filter(galleon.Col("value").Gt(galleon.Lit(100.0))).
    GroupBy("category").
    Agg(galleon.Col("value").Sum().Alias("total")).
    Collect()
```

### I/O

```go
// CSV
df, _ := galleon.ReadCSV("data.csv")
df.WriteCSV("output.csv")

// JSON
df, _ := galleon.ReadJSON("data.json")
df.WriteJSON("output.json")

// Parquet
df, _ := galleon.ReadParquet("data.parquet")
df.WriteParquet("output.parquet")

// Lazy scan
result, _ := galleon.ScanCSV("large.csv").
    Filter(galleon.Col("x").Gt(galleon.Lit(0.0))).
    Collect()
```

## Thread Configuration

```go
// Auto-detect (default)
galleon.SetMaxThreads(0)

// Set explicitly
galleon.SetMaxThreads(8)

// Check configuration
config := galleon.GetThreadConfig()
fmt.Printf("Threads: %d, Auto: %v\n", config.MaxThreads, config.AutoDetected)
```
