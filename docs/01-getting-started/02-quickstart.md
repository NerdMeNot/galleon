# Quick Start Guide

Get up and running with Galleon in 5 minutes.

## Prerequisites

- **Go**: 1.21 or later
- **C Compiler**: Required for CGO
  - **macOS**: Xcode Command Line Tools (`xcode-select --install`)
  - **Linux**: GCC (usually pre-installed)
  - **Windows**: MinGW-w64 or MSVC

## Installation

```bash
go get github.com/NerdMeNot/galleon/go
```

That's it! Prebuilt libraries are included for all major platforms.

## Your First DataFrame

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Create a DataFrame from Series
    df, err := galleon.NewDataFrame(
        galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
        galleon.NewSeriesString("name", []string{
            "Alice", "Bob", "Charlie", "Diana", "Eve",
        }),
        galleon.NewSeriesFloat64("score", []float64{
            85.5, 92.0, 78.3, 96.1, 88.7,
        }),
    )
    if err != nil {
        panic(err)
    }

    // Print DataFrame info
    fmt.Printf("Rows: %d, Columns: %d\n", df.Height(), df.Width())
    fmt.Printf("Columns: %v\n", df.Columns())
}
```

Output:
```
Rows: 5, Columns: 3
Columns: [id name score]
```

## Basic Operations

### Aggregations

```go
// Get a column and compute statistics
scores := df.ColumnByName("score")
fmt.Printf("Sum:  %.2f\n", scores.Sum())
fmt.Printf("Mean: %.2f\n", scores.Mean())
fmt.Printf("Min:  %.2f\n", scores.Min())
fmt.Printf("Max:  %.2f\n", scores.Max())
```

### Filtering

```go
// Filter rows where score > 85
scores := df.ColumnByName("score")
mask := make([]bool, df.Height())
for i, v := range scores.Float64() {
    mask[i] = v > 85.0
}
highScores := df.Filter(mask)
fmt.Printf("High scorers: %d\n", highScores.Height())
```

### Sorting

```go
// Sort by score descending
sorted, err := df.SortBy("score", false)
if err != nil {
    panic(err)
}
```

### Adding Columns

```go
// Add a computed column
scores := df.ColumnByName("score").Float64()
grades := make([]float64, len(scores))
for i, s := range scores {
    grades[i] = s / 10.0
}
df, err = df.WithColumn(galleon.NewSeriesFloat64("grade", grades))
if err != nil {
    panic(err)
}
```

## GroupBy and Aggregation

```go
// Sample data
sales, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("region", []string{
        "East", "West", "East", "West", "East",
    }),
    galleon.NewSeriesFloat64("amount", []float64{
        100, 150, 200, 175, 125,
    }),
)

// Group by region - single aggregation
result, _ := sales.GroupBy("region").Sum("amount")

// Group by region - multiple aggregations
result, _ = sales.GroupBy("region").Agg(
    galleon.AggSum("amount").Alias("total"),
    galleon.AggMean("amount").Alias("average"),
)
```

## Reading and Writing Files

### CSV

```go
// Read CSV
df, err := galleon.ReadCSV("data.csv", galleon.DefaultCSVReadOptions())

// Write CSV
err = df.WriteCSV("output.csv")
```

### JSON

```go
// Read JSON
df, err := galleon.ReadJSON("data.json")

// Write JSON
err = df.WriteJSON("output.json")
```

### Parquet

```go
// Read Parquet
df, err := galleon.ReadParquet("data.parquet")
```

## Joins

```go
orders, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("customer_id", []int64{1, 2, 1, 3}),
    galleon.NewSeriesFloat64("amount", []float64{99.99, 49.99, 29.99, 79.99}),
)

customers, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("customer_id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
)

// Inner join on same column name
result, _ := orders.Join(customers, galleon.On("customer_id"))

// Join on different column names
result, _ = orders.Join(customers,
    galleon.LeftOn("customer_id").RightOn("customer_id"),
)
```

## Lazy Evaluation

For large datasets, use lazy evaluation:

```go
// Convert DataFrame to LazyFrame
lazy := df.Lazy()

// Build a query - nothing executes yet
query := lazy.
    Filter(galleon.Col("price").Gt(galleon.Lit(50.0))).
    GroupBy("category").
    Agg(galleon.Col("price").Sum().Alias("total")).
    Sort("total", false).
    Head(10)

// Execute the query
result, err := query.Collect()
```

Benefits:
- Deferred execution
- Query optimization
- Memory efficiency

## Thread Configuration

```go
// Check current thread count
fmt.Printf("Threads: %d\n", galleon.GetMaxThreads())

// Set thread count (0 = auto-detect)
galleon.SetMaxThreads(8)
```

## Complete Example

```go
package main

import (
    "fmt"
    "strings"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Create sample data
    df, _ := galleon.NewDataFrame(
        galleon.NewSeriesString("product", []string{
            "Widget", "Gadget", "Widget", "Gadget", "Gizmo",
        }),
        galleon.NewSeriesString("region", []string{
            "East", "East", "West", "West", "East",
        }),
        galleon.NewSeriesFloat64("sales", []float64{
            1000, 1500, 1200, 1800, 900,
        }),
    )

    // Using Lazy API for chained operations
    result, _ := df.Lazy().
        Filter(galleon.Col("sales").Gt(galleon.Lit(500.0))).
        GroupBy("product").
        Agg(
            galleon.Col("sales").Sum().Alias("total_sales"),
        ).
        Sort("total_sales", false).
        Collect()

    // Print results
    fmt.Println("Sales Summary:")
    fmt.Printf("%-10s %12s\n", "Product", "Total Sales")
    fmt.Println(strings.Repeat("-", 25))

    products := result.ColumnByName("product").Strings()
    totals := result.ColumnByName("total_sales").Float64()

    for i := 0; i < result.Height(); i++ {
        fmt.Printf("%-10s %12.2f\n", products[i], totals[i])
    }
}
```

Output:
```
Sales Summary:
Product     Total Sales
-------------------------
Gadget         3300.00
Widget         2200.00
Gizmo           900.00
```

## Next Steps

- [DataFrame API](../03-api/01-dataframe.md) - Full DataFrame reference
- [GroupBy Guide](../02-guides/02-groupby.md) - Advanced aggregations
- [Join Guide](../02-guides/03-joins.md) - Combining DataFrames
- [Lazy Evaluation](../02-guides/01-lazy.md) - Query optimization
- [Performance Tips](../04-reference/02-performance.md) - Optimization guide
