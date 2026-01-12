# Lazy Evaluation Guide

Lazy evaluation defers computation until results are needed, enabling query optimization and efficient memory usage.

## Overview

### Eager vs Lazy Execution

**Eager execution** (default):
```go
// Each operation executes immediately
filtered := df.Filter(Col("value").Gt(Lit(100)))  // Executes now
sorted := filtered.Sort("value", true)             // Executes now
result := sorted.Head(10)                          // Executes now
```

**Lazy execution**:
```go
// Operations are planned but not executed
lazy := df.Lazy().
    Filter(Col("value").Gt(Lit(100))).  // Planned
    Sort("value", true).            // Planned
    Limit(10)                            // Planned

// Execute the entire plan at once
result, err := lazy.Collect()  // Executes all operations
```

## Creating LazyFrames

### From DataFrame

```go
lazy := df.Lazy()
```

### From File Scan

```go
// CSV scan
lazy := galleon.ScanCSV("data.csv")
lazy := galleon.ScanCSVWithOptions("data.csv", opts)

// Parquet scan
lazy := galleon.ScanParquet("data.parquet")
```

## LazyFrame Operations

### Filter

```go
lazy := df.Lazy().
    Filter(galleon.Col("age").Gt(galleon.Lit(18)))
```

### Select

```go
lazy := df.Lazy().
    Select(
        galleon.Col("id"),
        galleon.Col("name"),
        galleon.Col("value"),
    )
```

### WithColumn

```go
lazy := df.Lazy().
    WithColumn("total", galleon.Col("price").Mul(galleon.Col("quantity")))
```

### Sort

```go
lazy := df.Lazy().
    Sort("value", false)  // Descending
```

### Limit

```go
lazy := df.Lazy().
    Limit(100)
```

### GroupBy and Agg

```go
lazy := df.Lazy().
    GroupBy("category")).
    Agg(
        galleon.Col("value").Sum().Alias("total"),
        galleon.Col("value").Mean().Alias("average"),
    )
```

### Join

```go
lazy := df.Lazy().
    Join(
        other.Lazy(),
        galleon.On("id"),
    )
```

### Pivot

```go
lazy := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "date",
        Column: "metric",
        Values: "value",
        AggFn:  galleon.AggTypeSum,
    })
```

### Melt

```go
lazy := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"id"},
        ValueVars: []string{"col1", "col2"},
        VarName:   "variable",
        ValueName: "value",
    })
```

### Cache

Materialize intermediate results for reuse:

```go
// Cache expensive computation
cached := df.Lazy().
    Filter(galleon.Col("value").Gt(galleon.Lit(100))).
    GroupBy("category")).
    Agg(galleon.Col("value").Sum().Alias("total")).
    Cache()

// Reuse cached result multiple times
result1, _ := cached.Filter(galleon.Col("total").Gt(galleon.Lit(1000))).Collect()
result2, _ := cached.Sort("total", false).Head(10).Collect()
```

### Apply (UDF)

Apply custom functions to columns:

```go
// Apply user-defined function
lazy := df.Lazy().
    Apply("price", func(s *galleon.Series) (*galleon.Series, error) {
        data := s.Float64()
        result := make([]float64, len(data))
        for i, v := range data {
            result[i] = v * 1.1  // 10% markup
        }
        return galleon.NewSeriesFloat64(s.Name(), result), nil
    })
```

## Collecting Results

### Collect

Executes the plan and returns a DataFrame:

```go
result, err := lazy.Collect()
if err != nil {
    // Handle error
}
```

### Fetch

Executes and returns up to n rows:

```go
result, err := lazy.Fetch(100)
```

## Query Optimization

### Predicate Pushdown

Filters are pushed down to minimize data processed:

```go
// Filter pushed to file read
result, _ := galleon.ScanCSV("large.csv").
    Filter(Col("status").Eq(Lit("active"))).
    Collect()

// Optimization: Only matching rows are fully materialized
```

### Projection Pushdown

Only needed columns are read:

```go
// Only reads 'id' and 'name' columns from file
result, _ := galleon.ScanCSV("data.csv").
    Select(Col("id"), Col("name")).
    Collect()
```

### Common Subexpression Elimination

Duplicate computations are identified and shared:

```go
lazy := df.Lazy().
    WithColumn("total", Col("a").Add(Col("b"))).
    Filter(Col("a").Add(Col("b")).Gt(Lit(100)))
// a + b computed once, reused
```

### Limit Pushdown

Limits propagate through operations where possible:

```go
// Limit pushed through sort for early termination
result, _ := df.Lazy().
    Sort("value", false).
    Limit(10).
    Collect()
// Uses partial sort algorithm for top-k
```

## Execution Planning

### Viewing the Plan

```go
lazy := df.Lazy().
    Filter(Col("value").Gt(Lit(100))).
    GroupBy("category")).
    Agg(Col("value").Sum().Alias("total"))

// Print the execution plan
fmt.Println(lazy.Explain())
```

Output:
```
Aggregate [category]
  Agg: SUM(value) AS total
  └── Filter [value > 100]
        └── DataFrame [rows=1000, cols=3]
```

### Plan Optimization

The optimizer applies transformations:
1. Predicate pushdown
2. Projection pruning
3. Common subexpression elimination
4. Join reordering (for multi-join queries)

## Lazy I/O Benefits

### Memory Efficiency

```go
// Process 100GB file with 1GB memory
result, _ := galleon.ScanCSV("huge_file.csv").
    Filter(Col("date").Gte(Lit("2024-01-01"))).
    GroupBy("category")).
    Agg(Col("amount").Sum().Alias("total")).
    Collect()

// File processed in chunks, only aggregates kept in memory
```

### Parallel Execution

```go
// Scans are parallelized across threads
galleon.SetMaxThreads(8)

result, _ := galleon.ScanParquet("partitioned/").
    Filter(Col("region").Eq(Lit("US"))).
    Collect()
```

## Common Patterns

### ETL Pipeline

```go
func etlPipeline(inputPath, outputPath string) error {
    result, err := galleon.ScanCSV(inputPath).
        // Clean data
        Filter(Col("value").IsNotNull()).
        // Transform
        WithColumn("normalized", Col("value").Div(Col("max_value"))).
        // Aggregate
        GroupBy("category")).
        Agg(
            Col("normalized").Mean().Alias("avg_normalized"),
            Col("id").Count().Alias("count"),
        ).
        // Sort results
        Sort("avg_normalized", false).
        Collect()

    if err != nil {
        return err
    }

    return result.WriteCSV(outputPath)
}
```

### Incremental Processing

```go
// Process daily files
dates := []string{"2024-01-01", "2024-01-02", "2024-01-03"}
var results []*galleon.DataFrame

for _, date := range dates {
    path := fmt.Sprintf("data/%s.csv", date)
    result, _ := galleon.ScanCSV(path).
        Filter(Col("status").Eq(Lit("complete"))).
        GroupBy("product")).
        Agg(Col("sales").Sum().Alias("daily_sales")).
        WithColumn("date", Lit(date)).
        Collect()
    results = append(results, result)
}

// Combine results
combined := galleon.Concat(results...)
```

### Complex Analytics

```go
// Multi-stage analysis
result, _ := galleon.ScanParquet("sales.parquet").
    // Join with dimension tables
    Join(
        galleon.ScanParquet("products.parquet"),
        galleon.On("product_id"),
    ).
    Join(
        galleon.ScanParquet("regions.parquet"),
        galleon.On("region_id"),
    ).
    // Filter recent data
    Filter(Col("date").Gte(Lit("2024-01-01"))).
    // Aggregate
    GroupBy("region_name", "category").
    Agg(
        Col("revenue").Sum().Alias("total_revenue"),
        Col("quantity").Sum().Alias("total_units"),
        Col("order_id").Count().Alias("order_count"),
    ).
    // Calculate derived metrics
    WithColumn("avg_order_value",
        Col("total_revenue").Div(Col("order_count")),
    ).
    // Sort and limit
    Sort("total_revenue", false).
    Limit(50).
    Collect()
```

## Error Handling

```go
result, err := lazy.Collect()
if err != nil {
    switch {
    case strings.Contains(err.Error(), "file not found"):
        // Source file doesn't exist
    case strings.Contains(err.Error(), "column not found"):
        // Referenced column doesn't exist
    case strings.Contains(err.Error(), "type mismatch"):
        // Operation type incompatibility
    default:
        // Other execution error
    }
}
```

## Best Practices

### 1. Filter Early

```go
// Good: Filter before expensive operations
lazy := df.Lazy().
    Filter(Col("active").Eq(Lit(true))).
    GroupBy("category")).
    Agg(Col("value").Sum())

// Less efficient: Filter after
lazy := df.Lazy().
    GroupBy("category")).
    Agg(Col("value").Sum()).
    Filter(Col("sum").Gt(Lit(0)))
```

### 2. Select Only Needed Columns

```go
// Good: Select early
lazy := df.Lazy().
    Select(Col("id"), Col("value")).
    GroupBy("id")).
    Agg(Col("value").Sum())

// Less efficient: Carry all columns
lazy := df.Lazy().
    GroupBy("id")).
    Agg(Col("value").Sum())
```

### 3. Use Scan for Large Files

```go
// Good: Lazy scan
result, _ := galleon.ScanCSV("huge.csv").
    Filter(Col("x").Gt(Lit(0))).
    Collect()

// Less efficient: Eager read
df, _ := galleon.ReadCSV("huge.csv", opts)  // Loads entire file
result := df.Filter(Col("x").Gt(Lit(0)))
```

## Advanced Features

### Caching Intermediate Results

When you need to reuse an expensive computation multiple times, use `Cache()` to materialize the result:

```go
// Expensive aggregation that we'll reuse
expensive := df.Lazy().
    Filter(galleon.Col("date").Gte(galleon.Lit("2024-01-01"))).
    GroupBy("product", "region").
    Agg(
        galleon.Col("sales").Sum().Alias("total_sales"),
        galleon.Col("quantity").Sum().Alias("total_units"),
    ).
    Cache()  // Materialize and cache this result

// Use cached result for multiple analyses
topProducts, _ := expensive.
    Sort("total_sales", false).
    Head(10).
    Collect()

regionalStats, _ := expensive.
    GroupBy("region")).
    Agg(
        galleon.Col("total_sales").Sum().Alias("region_sales"),
    ).
    Collect()

// Cache is automatically reused - expensive query runs only once
```

#### When to Use Cache

**Use Cache when:**
- You reuse the same intermediate result multiple times
- The computation is expensive (large aggregations, complex joins)
- The result fits comfortably in memory

**Skip Cache when:**
- You use the result only once
- The result is very large (might cause OOM)
- The computation is cheap (simple filters)

#### Clearing Cache

```go
// Clear all cached results
galleon.ClearCache()
```

### User-Defined Functions (UDF)

Apply custom transformations using Go functions:

```go
// Simple UDF: 10% markup
result, _ := df.Lazy().
    Apply("price", func(s *galleon.Series) (*galleon.Series, error) {
        data := s.Float64()
        result := make([]float64, len(data))
        for i, v := range data {
            result[i] = v * 1.1
        }
        return galleon.NewSeriesFloat64(s.Name(), result), nil
    }).
    Collect()
```

#### Complex UDF Example

```go
// Custom scoring function
result, _ := df.Lazy().
    Apply("features", func(s *galleon.Series) (*galleon.Series, error) {
        data := s.Float64()
        scores := make([]float64, len(data))

        for i, v := range data {
            // Custom business logic
            if v > 100 {
                scores[i] = math.Log(v) * 10
            } else {
                scores[i] = v * 0.5
            }
        }

        return galleon.NewSeriesFloat64("score", scores), nil
    }).
    Collect()
```

#### UDF Best Practices

1. **Keep UDFs simple**: Complex logic should be broken into multiple steps
2. **Handle errors**: Always return proper errors for invalid data
3. **Consider performance**: UDFs are slower than built-in operations
4. **Type safety**: Validate input types in your UDF

```go
// Good UDF structure
func customTransform(s *galleon.Series) (*galleon.Series, error) {
    // Validate input type
    if s.DType() != galleon.Float64 {
        return nil, fmt.Errorf("expected Float64, got %v", s.DType())
    }

    // Get data
    data := s.Float64()
    result := make([]float64, len(data))

    // Apply transformation
    for i, v := range data {
        if math.IsNaN(v) {
            result[i] = 0.0  // Handle nulls
            continue
        }
        result[i] = yourLogic(v)
    }

    return galleon.NewSeriesFloat64(s.Name(), result), nil
}

// Use it
df.Lazy().Apply("column", customTransform).Collect()
```

## LazyFrame API Reference

### Creation

```go
func (df *DataFrame) Lazy() *LazyFrame
func ScanCSV(path string) *LazyFrame
func ScanCSVWithOptions(path string, opts CSVReadOptions) *LazyFrame
func ScanParquet(path string) *LazyFrame
```

### Transformations

```go
func (lf *LazyFrame) Filter(expr Expr) *LazyFrame
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame
func (lf *LazyFrame) WithColumn(name string, expr Expr) *LazyFrame
func (lf *LazyFrame) Sort(expr Expr, descending bool) *LazyFrame
func (lf *LazyFrame) Limit(n int) *LazyFrame
func (lf *LazyFrame) GroupBy(exprs ...Expr) *LazyGroupBy
func (lf *LazyFrame) Join(other *LazyFrame, opts ...JoinOption) *LazyFrame
func (lf *LazyFrame) Pivot(opts PivotOptions) *LazyFrame
func (lf *LazyFrame) Melt(opts MeltOptions) *LazyFrame
func (lf *LazyFrame) Cache() *LazyFrame
func (lf *LazyFrame) Apply(column string, fn func(*Series) (*Series, error)) *LazyFrame
```

### Execution

```go
func (lf *LazyFrame) Collect() (*DataFrame, error)
func (lf *LazyFrame) Fetch(n int) (*DataFrame, error)
func (lf *LazyFrame) Explain() string
```

### Utility

```go
func ClearCache()
```
