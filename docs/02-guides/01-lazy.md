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
    Sort(Col("value"), true).            // Planned
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
    Sort(galleon.Col("value"), false)  // Descending
```

### Limit

```go
lazy := df.Lazy().
    Limit(100)
```

### GroupBy and Agg

```go
lazy := df.Lazy().
    GroupBy(galleon.Col("category")).
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
    Sort(Col("value"), false).
    Limit(10).
    Collect()
// Uses partial sort algorithm for top-k
```

## Execution Planning

### Viewing the Plan

```go
lazy := df.Lazy().
    Filter(Col("value").Gt(Lit(100))).
    GroupBy(Col("category")).
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
    GroupBy(Col("category")).
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
        GroupBy(Col("category")).
        Agg(
            Col("normalized").Mean().Alias("avg_normalized"),
            Col("id").Count().Alias("count"),
        ).
        // Sort results
        Sort(Col("avg_normalized"), false).
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
        GroupBy(Col("product")).
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
    GroupBy(Col("region_name"), Col("category")).
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
    Sort(Col("total_revenue"), false).
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
    GroupBy(Col("category")).
    Agg(Col("value").Sum())

// Less efficient: Filter after
lazy := df.Lazy().
    GroupBy(Col("category")).
    Agg(Col("value").Sum()).
    Filter(Col("sum").Gt(Lit(0)))
```

### 2. Select Only Needed Columns

```go
// Good: Select early
lazy := df.Lazy().
    Select(Col("id"), Col("value")).
    GroupBy(Col("id")).
    Agg(Col("value").Sum())

// Less efficient: Carry all columns
lazy := df.Lazy().
    GroupBy(Col("id")).
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
```

### Execution

```go
func (lf *LazyFrame) Collect() (*DataFrame, error)
func (lf *LazyFrame) Fetch(n int) (*DataFrame, error)
func (lf *LazyFrame) Explain() string
```
