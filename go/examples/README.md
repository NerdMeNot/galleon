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

### 07_window_functions

Window functions and time series:
- Lag/Lead (shift operations)
- Diff/PctChange (price changes)
- CumSum/CumMin/CumMax (cumulative metrics)
- RollingMean/RollingSum (moving averages)
- Technical analysis (drawdown, volatility)
- Week-over-week comparisons

### 08_reshape

Pivot and melt operations:
- Basic pivot (long to wide format)
- Pivot with different aggregations (sum, mean, count)
- Basic melt (wide to long format)
- Melt with auto value detection
- Round-trip transformations
- Real-world examples (sales reports, surveys, time series)

### 09_string_operations

String manipulation:
- Case transformations (Upper, Lower)
- Text cleaning (Trim)
- Pattern matching (Contains)
- Prefix/suffix matching (StartsWith, EndsWith)
- String replacement (Replace)
- Email validation pipeline
- Text categorization
- URL processing

### 10_advanced_features

UDFs and caching:
- Simple user-defined functions
- Complex business logic transformations
- Mathematical transformations (log, normalization)
- Basic caching for performance
- Caching expensive joins
- Chaining multiple UDFs
- Error handling in UDFs
- Performance comparison with/without caching

### 11_statistics

Advanced statistical analysis:
- Complete statistical summary (count, mean, median, std, min, max)
- Quantile analysis (percentiles, quartiles)
- Distribution shape (skewness, kurtosis)
- Correlation analysis
- Group statistics
- Outlier detection (IQR method)
- Performance benchmarking
- Quality control analysis

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

### Window Functions

```go
// Shift operations
result, _ := df.Lazy().
    WithColumn("prev_close", galleon.Col("close").Lag(1, 0.0)).
    WithColumn("next_close", galleon.Col("close").Lead(1, 0.0)).
    Collect()

// Price changes
result, _ := df.Lazy().
    WithColumn("price_change", galleon.Col("close").Diff()).
    WithColumn("pct_change", galleon.Col("close").PctChange()).
    Collect()

// Cumulative metrics
result, _ := df.Lazy().
    WithColumn("cumulative_volume", galleon.Col("volume").CumSum()).
    WithColumn("running_max", galleon.Col("close").CumMax()).
    Collect()

// Rolling aggregations
result, _ := df.Lazy().
    WithColumn("ma20", galleon.Col("close").RollingMean(20)).
    WithColumn("rolling_vol", galleon.Col("volume").RollingSum(7)).
    Collect()
```

### Reshape Operations

```go
// Pivot (long to wide)
wide, _ := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "date",
        Column: "product",
        Values: "sales",
        AggFn:  galleon.AggTypeSum,
    }).
    Collect()

// Melt (wide to long)
long, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"date"},
        ValueVars: []string{"Laptop", "Phone", "Tablet"},
        VarName:   "product",
        ValueName: "sales",
    }).
    Collect()
```

### String Operations

```go
// Text transformations
result, _ := df.Lazy().
    WithColumn("name_lower", galleon.Col("name").Str().Lower()).
    WithColumn("name_upper", galleon.Col("name").Str().Upper()).
    WithColumn("name_clean", galleon.Col("name").Str().Trim()).
    Collect()

// Pattern matching
errors, _ := df.Lazy().
    Filter(galleon.Col("message").Str().Contains("ERROR")).
    Collect()

pdfFiles, _ := df.Lazy().
    Filter(galleon.Col("filename").Str().EndsWith(".pdf")).
    Collect()

// String replacement
updated, _ := df.Lazy().
    WithColumn("new_code", galleon.Col("old_code").Str().Replace("OLD", "NEW")).
    Collect()
```

### User-Defined Functions

```go
// Apply custom transformation
result, _ := df.Lazy().
    Apply("price", func(s *galleon.Series) (*galleon.Series, error) {
        data := s.Float64()
        result := make([]float64, len(data))
        for i, v := range data {
            result[i] = v * 1.2  // 20% markup
        }
        return galleon.NewSeriesFloat64("retail_price", result), nil
    }).
    Collect()
```

### Caching

```go
// Cache expensive computation
cached := df.Lazy().
    Filter(galleon.Col("value").Gt(galleon.Lit(1000))).
    GroupBy("category").
    Agg(galleon.Col("value").Sum().Alias("total")).
    Cache()  // Materialize once

// Reuse cached result multiple times
result1, _ := cached.Filter(galleon.Col("total").Gt(galleon.Lit(5000))).Collect()
result2, _ := cached.Sort("total", false).Head(10).Collect()
```

### Advanced Statistics

```go
// Complete statistical summary
stats, _ := df.Lazy().
    Select(
        galleon.Col("value").Count().Alias("count"),
        galleon.Col("value").Mean().Alias("mean"),
        galleon.Col("value").Median().Alias("median"),
        galleon.Col("value").Std().Alias("std_dev"),
        galleon.Col("value").Quantile(0.25).Alias("q25"),
        galleon.Col("value").Quantile(0.75).Alias("q75"),
        galleon.Col("value").Skewness().Alias("skewness"),
        galleon.Col("value").Kurtosis().Alias("kurtosis"),
    ).
    Collect()

// Group statistics
stats, _ := df.Lazy().
    GroupBy("region").
    Agg(
        galleon.Col("sales").Mean().Alias("avg_sales"),
        galleon.Col("sales").Median().Alias("median_sales"),
        galleon.Col("sales").Std().Alias("std_dev"),
    ).
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
