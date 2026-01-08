# Galleon Examples

This directory contains example programs demonstrating various features of Galleon.

## Running Examples

Each example is a standalone Go program. To run an example:

```bash
cd examples/01_basic_usage
go run main.go
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
- Expression-based filtering
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
- JSONL format
- Type specification
- Handling missing values
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
// Expression-based
filtered := df.Filter(galleon.Col("value").Gt(galleon.Lit(2.0)))

// Mask-based
mask := galleon.FilterMaskGreaterThanF64(values, 2.0)
```

### GroupBy

```go
result := df.GroupBy("category").Agg(
    galleon.Col("value").Sum().Alias("total"),
    galleon.Col("value").Mean().Alias("average"),
)
```

### Joins

```go
// Same column name
result, _ := left.Join(right, galleon.On("id"))

// Different column names
result, _ := left.Join(right,
    galleon.LeftOn("order_id"),
    galleon.RightOn("id"),
)
```

### Lazy Evaluation

```go
result, _ := df.Lazy().
    Filter(galleon.Col("value").Gt(galleon.Lit(100))).
    GroupBy(galleon.Col("category")).
    Agg(galleon.Col("value").Sum()).
    Collect()
```

### I/O

```go
// CSV
df, _ := galleon.ReadCSV("data.csv", galleon.DefaultCSVReadOptions())
df.WriteCSV("output.csv")

// JSON
df, _ := galleon.ReadJSON("data.json")
df.WriteJSON("output.json")

// Lazy scan
result, _ := galleon.ScanCSV("large.csv").
    Filter(galleon.Col("x").Gt(galleon.Lit(0))).
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
