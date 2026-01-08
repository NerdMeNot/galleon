# Quick Start Guide

Get up and running with Galleon in 5 minutes.

## Prerequisites

- Go 1.21 or later
- Zig 0.13 or later (for building the core library)

## Installation

```bash
# Clone the repository
git clone https://github.com/NerdMeNot/galleon.git
cd galleon

# Build the Zig core library
cd core
zig build -Doptimize=ReleaseFast
cd ..

# Run Go tests to verify
cd go
go test ./...
```

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
    fmt.Printf("Columns: %v\n", df.ColumnNames())
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
scores := df.Column("score")
fmt.Printf("Sum:  %.2f\n", scores.Sum())
fmt.Printf("Mean: %.2f\n", scores.Mean())
fmt.Printf("Min:  %.2f\n", scores.Min())
fmt.Printf("Max:  %.2f\n", scores.Max())
```

### Filtering

```go
// Filter rows where score > 85
highScores := df.Filter(
    galleon.Col("score").Gt(galleon.Lit(85.0)),
)
fmt.Printf("High scorers: %d\n", highScores.Height())
```

### Sorting

```go
// Sort by score descending
sorted := df.Sort("score", false)
```

### Adding Columns

```go
// Add a computed column
df = df.WithColumn("grade",
    galleon.Col("score").Div(galleon.Lit(10.0)),
)
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

// Group by region and sum
result := sales.GroupBy("region").Agg(
    galleon.Col("amount").Sum().Alias("total"),
    galleon.Col("amount").Mean().Alias("average"),
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
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
)

// Join on customer_id = id
result, _ := orders.Join(customers,
    galleon.LeftOn("customer_id"),
    galleon.RightOn("id"),
)
```

## Lazy Evaluation

For large datasets, use lazy evaluation:

```go
result, err := galleon.ScanCSV("large_file.csv").
    Filter(galleon.Col("value").Gt(galleon.Lit(100))).
    GroupBy(galleon.Col("category")).
    Agg(galleon.Col("value").Sum().Alias("total")).
    Sort(galleon.Col("total"), false).
    Limit(10).
    Collect()
```

Benefits:
- Predicate pushdown
- Projection pushdown
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

    // Pipeline: Filter, Group, Aggregate, Sort
    result := df.
        Filter(galleon.Col("sales").Gt(galleon.Lit(500.0))).
        GroupBy("product").
        Agg(
            galleon.Col("sales").Sum().Alias("total_sales"),
            galleon.Col("sales").Count().Alias("num_sales"),
        ).
        Sort("total_sales", false)

    // Print results
    fmt.Println("Sales Summary:")
    fmt.Printf("%-10s %12s %10s\n", "Product", "Total Sales", "Count")
    fmt.Println(strings.Repeat("-", 35))

    products := result.Column("product").Strings()
    totals := result.Column("total_sales").Float64()
    counts := result.Column("num_sales").Int64()

    for i := 0; i < result.Height(); i++ {
        fmt.Printf("%-10s %12.2f %10d\n",
            products[i], totals[i], counts[i])
    }
}
```

Output:
```
Sales Summary:
Product     Total Sales      Count
-----------------------------------
Gadget         3300.00          2
Widget         2200.00          2
Gizmo           900.00          1
```

## Next Steps

- [DataFrame API](api-dataframe.md) - Full DataFrame reference
- [GroupBy Guide](guide-groupby.md) - Advanced aggregations
- [Join Guide](guide-joins.md) - Combining DataFrames
- [Lazy Evaluation](guide-lazy.md) - Query optimization
- [Performance Tips](reference-performance.md) - Optimization guide
