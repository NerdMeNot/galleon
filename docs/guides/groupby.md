# GroupBy Operations Guide

GroupBy operations are fundamental for data aggregation and analysis. Galleon provides high-performance groupby with parallel execution.

## Basic GroupBy

### Single Column GroupBy

```go
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("category", []string{"A", "B", "A", "B", "A"}),
    galleon.NewSeriesFloat64("value", []float64{10, 20, 30, 40, 50}),
)

// Group by category and sum values
result := df.GroupBy("category").Sum("value")
```

### Multi-Column GroupBy

```go
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("region", []string{"East", "East", "West", "West"}),
    galleon.NewSeriesString("product", []string{"A", "B", "A", "B"}),
    galleon.NewSeriesFloat64("sales", []float64{100, 200, 150, 250}),
)

// Group by region and product
result := df.GroupBy("region", "product").Sum("sales")
```

## Aggregation Functions

### Built-in Aggregations

```go
grouped := df.GroupBy("category")

// Sum
result := grouped.Sum("value")

// Mean (average)
result := grouped.Mean("value")

// Min
result := grouped.Min("value")

// Max
result := grouped.Max("value")

// Count
result := grouped.Count()

// Standard deviation
result := grouped.Std("value")

// Variance
result := grouped.Var("value")

// First value in each group
result := grouped.First("value")

// Last value in each group
result := grouped.Last("value")
```

### Multiple Aggregations with Agg

The `Agg` method allows multiple aggregations in a single pass:

```go
result := df.GroupBy("category").Agg(
    galleon.Col("value").Sum().Alias("total"),
    galleon.Col("value").Mean().Alias("average"),
    galleon.Col("value").Min().Alias("minimum"),
    galleon.Col("value").Max().Alias("maximum"),
    galleon.Col("value").Count().Alias("count"),
)
```

### Aggregating Multiple Columns

```go
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("category", []string{"A", "B", "A", "B"}),
    galleon.NewSeriesFloat64("sales", []float64{100, 200, 150, 250}),
    galleon.NewSeriesFloat64("quantity", []float64{10, 20, 15, 25}),
)

result := df.GroupBy("category").Agg(
    galleon.Col("sales").Sum().Alias("total_sales"),
    galleon.Col("quantity").Sum().Alias("total_quantity"),
    galleon.Col("sales").Mean().Alias("avg_sale"),
)
```

## Expression-Based Aggregations

### Computed Aggregations

```go
// Calculate total revenue (price * quantity) per category
result := df.GroupBy("category").Agg(
    galleon.Col("price").Mul(galleon.Col("quantity")).Sum().Alias("revenue"),
)
```

### Conditional Aggregations

```go
// Sum values where status is active
// First filter, then group
active := df.Filter(galleon.Col("status").Eq(galleon.Lit("active")))
result := active.GroupBy("category").Sum("value")
```

## GroupBy with Sorting

### Sort Results by Aggregated Values

```go
result := df.GroupBy("category").
    Agg(galleon.Col("sales").Sum().Alias("total")).
    Sort("total", false)  // Descending order
```

### Top N Groups

```go
// Top 10 categories by total sales
result := df.GroupBy("category").
    Agg(galleon.Col("sales").Sum().Alias("total")).
    Sort("total", false).
    Head(10)
```

## GroupBy in Lazy Evaluation

For large datasets, use lazy evaluation for optimized execution:

```go
result, err := df.Lazy().
    Filter(galleon.Col("date").Gte(galleon.Lit("2024-01-01"))).
    GroupBy(galleon.Col("category")).
    Agg(
        galleon.Col("sales").Sum().Alias("total_sales"),
        galleon.Col("quantity").Mean().Alias("avg_quantity"),
    ).
    Sort(galleon.Col("total_sales"), false).
    Limit(100).
    Collect()
```

Benefits of lazy groupby:
- Predicate pushdown: Filters applied before grouping
- Projection pushdown: Only needed columns are processed
- Query optimization: Execution plan is optimized

## Performance Considerations

### Hash-Based Grouping

Galleon uses hash-based grouping with:
- Robin Hood hashing for fast lookups
- Cache-aligned hash tables
- SIMD-accelerated hash computation (where applicable)

### Parallel Execution

GroupBy operations are parallelized:
1. Data is partitioned across threads
2. Each thread builds local aggregates
3. Local aggregates are merged into final result

```
Thread 1: [A:10, B:20] ─┐
Thread 2: [A:30, B:40] ─┼─> Merge: [A:40, B:60]
Thread 3: [A:50, B:50] ─┘
```

### Memory Efficiency

For many-group scenarios:
- Pre-size hint can improve performance
- Results are streamed when possible
- Memory is released as groups complete

## Complete Example

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Sales data
    df, _ := galleon.NewDataFrame(
        galleon.NewSeriesString("region", []string{
            "East", "East", "West", "West", "East", "West",
        }),
        galleon.NewSeriesString("product", []string{
            "Widget", "Gadget", "Widget", "Gadget", "Widget", "Widget",
        }),
        galleon.NewSeriesFloat64("sales", []float64{
            1000, 1500, 1200, 1800, 900, 1100,
        }),
        galleon.NewSeriesInt64("quantity", []int64{
            10, 15, 12, 18, 9, 11,
        }),
    )

    // Analysis 1: Sales by region
    fmt.Println("Sales by Region:")
    byRegion := df.GroupBy("region").Agg(
        galleon.Col("sales").Sum().Alias("total_sales"),
        galleon.Col("quantity").Sum().Alias("total_units"),
    )
    fmt.Println(byRegion)

    // Analysis 2: Product performance
    fmt.Println("\nProduct Performance:")
    byProduct := df.GroupBy("product").Agg(
        galleon.Col("sales").Sum().Alias("total_sales"),
        galleon.Col("sales").Mean().Alias("avg_sale"),
        galleon.Col("sales").Count().Alias("transactions"),
    ).Sort("total_sales", false)
    fmt.Println(byProduct)

    // Analysis 3: Region + Product breakdown
    fmt.Println("\nRegion-Product Breakdown:")
    detailed := df.GroupBy("region", "product").Agg(
        galleon.Col("sales").Sum().Alias("total_sales"),
        galleon.Col("quantity").Mean().Alias("avg_qty"),
    ).Sort("total_sales", false)
    fmt.Println(detailed)
}
```

## GroupBy API Reference

### GroupBy Method

```go
func (df *DataFrame) GroupBy(columns ...string) *GroupBy
```

### GroupBy Result Type

```go
type GroupBy struct {
    // Internal state
}

// Single aggregation methods
func (g *GroupBy) Sum(column string) *DataFrame
func (g *GroupBy) Mean(column string) *DataFrame
func (g *GroupBy) Min(column string) *DataFrame
func (g *GroupBy) Max(column string) *DataFrame
func (g *GroupBy) Count() *DataFrame
func (g *GroupBy) Std(column string) *DataFrame
func (g *GroupBy) Var(column string) *DataFrame
func (g *GroupBy) First(column string) *DataFrame
func (g *GroupBy) Last(column string) *DataFrame

// Multiple aggregations
func (g *GroupBy) Agg(exprs ...Expr) *DataFrame
```
