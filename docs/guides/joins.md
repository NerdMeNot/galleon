# Join Operations Guide

Join operations combine data from multiple DataFrames based on matching key columns. Galleon provides high-performance joins with hash-based algorithms.

## Join Types

### Inner Join

Returns only rows where keys match in both DataFrames:

```go
left, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie", "Diana"}),
)

right, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{2, 3, 5}),
    galleon.NewSeriesFloat64("score", []float64{85.0, 92.0, 78.0}),
)

// Inner join: only ids 2 and 3 match
result, _ := left.Join(right, galleon.On("id"))
// Result has 2 rows: Bob (85.0), Charlie (92.0)
```

### Left Join

Returns all rows from left DataFrame, with matches from right:

```go
result, _ := left.LeftJoin(right, galleon.On("id"))
// Result has 4 rows:
// - Alice (null score)
// - Bob (85.0)
// - Charlie (92.0)
// - Diana (null score)
```

### Right Join

Returns all rows from right DataFrame, with matches from left:

```go
result, _ := left.RightJoin(right, galleon.On("id"))
// Result has 3 rows:
// - Bob (85.0)
// - Charlie (92.0)
// - null name (78.0, id=5)
```

## Join Options

### On (Same Column Names)

When both DataFrames have the same key column name:

```go
result, _ := left.Join(right, galleon.On("id"))

// Multiple key columns
result, _ := left.Join(right, galleon.On("region", "date"))
```

### LeftOn / RightOn (Different Column Names)

When key columns have different names:

```go
left, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("user_id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
)

right, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("customer_id", []int64{1, 2}),
    galleon.NewSeriesFloat64("amount", []float64{100.0, 200.0}),
)

result, _ := left.Join(right,
    galleon.LeftOn("user_id"),
    galleon.RightOn("customer_id"),
)
```

### Suffix (Handling Duplicate Column Names)

When non-key columns have the same name:

```go
left, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2}),
    galleon.NewSeriesFloat64("value", []float64{10.0, 20.0}),
)

right, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2}),
    galleon.NewSeriesFloat64("value", []float64{100.0, 200.0}),
)

result, _ := left.Join(right,
    galleon.On("id"),
    galleon.Suffix("_right"),
)
// Columns: id, value, value_right
```

## Multi-Key Joins

Join on multiple columns for composite keys:

```go
sales, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("region", []string{"East", "East", "West"}),
    galleon.NewSeriesString("product", []string{"A", "B", "A"}),
    galleon.NewSeriesFloat64("amount", []float64{100, 200, 150}),
)

targets, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("region", []string{"East", "West"}),
    galleon.NewSeriesString("product", []string{"A", "A"}),
    galleon.NewSeriesFloat64("target", []float64{120, 160}),
)

// Join on both region AND product
result, _ := sales.Join(targets, galleon.On("region", "product"))
```

## Join Patterns

### Star Schema Joins

Common in data warehousing:

```go
// Fact table
sales, _ := galleon.ReadCSV("sales.csv", opts)

// Dimension tables
products, _ := galleon.ReadCSV("products.csv", opts)
customers, _ := galleon.ReadCSV("customers.csv", opts)
dates, _ := galleon.ReadCSV("dates.csv", opts)

// Join fact with dimensions
result, _ := sales.
    Join(products, galleon.On("product_id")).
    Join(customers, galleon.On("customer_id")).
    Join(dates, galleon.On("date_id"))
```

### Self Join

Join a DataFrame with itself:

```go
employees, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
    galleon.NewSeriesInt64("manager_id", []int64{0, 1, 1}),
)

// Get employee with manager name
result, _ := employees.Join(
    employees.Select("id", "name").Rename(map[string]string{
        "id":   "manager_id",
        "name": "manager_name",
    }),
    galleon.On("manager_id"),
)
```

### Lookup Join

Add columns from a reference table:

```go
orders, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("order_id", []int64{1, 2, 3}),
    galleon.NewSeriesString("status_code", []string{"P", "S", "C"}),
)

statusLookup, _ := galleon.NewDataFrame(
    galleon.NewSeriesString("code", []string{"P", "S", "C", "X"}),
    galleon.NewSeriesString("description", []string{
        "Pending", "Shipped", "Cancelled", "Unknown",
    }),
)

result, _ := orders.LeftJoin(statusLookup,
    galleon.LeftOn("status_code"),
    galleon.RightOn("code"),
)
```

## Lazy Join Operations

For large datasets, use lazy joins for optimized execution:

```go
result, err := galleon.ScanCSV("large_orders.csv").
    Join(
        galleon.ScanCSV("products.csv"),
        galleon.On("product_id"),
    ).
    Filter(galleon.Col("amount").Gt(galleon.Lit(1000))).
    Select(
        galleon.Col("order_id"),
        galleon.Col("product_name"),
        galleon.Col("amount"),
    ).
    Collect()
```

Benefits:
- Predicate pushdown to source scans
- Projection pushdown: only needed columns read
- Memory-efficient streaming execution

## Performance Considerations

### Hash Join Algorithm

Galleon uses hash-based joins:
1. **Build phase**: Create hash table from smaller DataFrame
2. **Probe phase**: Match rows from larger DataFrame against hash table

```
Build: right DataFrame → Hash Table
Probe: left DataFrame → Hash Table → Matches
```

### Join Order Optimization

For multiple joins, order matters:

```go
// Better: Join smallest tables first
result, _ := small.
    Join(medium, galleon.On("key")).
    Join(large, galleon.On("key"))

// Worse: Join large tables first
result, _ := large.
    Join(medium, galleon.On("key")).
    Join(small, galleon.On("key"))
```

### Key Column Types

Best performance with integer keys:

```go
// Fast: Integer keys
result, _ := left.Join(right, galleon.On("id"))  // Int64 key

// Slower: String keys (require hashing)
result, _ := left.Join(right, galleon.On("name"))  // String key
```

### Memory Usage

For very large joins:
- Build hash table from smaller DataFrame
- Stream larger DataFrame through probe phase
- Consider filtering before joining

## Error Handling

```go
result, err := left.Join(right, galleon.On("id"))
if err != nil {
    switch {
    case strings.Contains(err.Error(), "column not found"):
        // Key column doesn't exist
    case strings.Contains(err.Error(), "type mismatch"):
        // Key columns have different types
    default:
        // Other error
    }
}
```

## Complete Example

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Orders table
    orders, _ := galleon.NewDataFrame(
        galleon.NewSeriesInt64("order_id", []int64{1, 2, 3, 4, 5}),
        galleon.NewSeriesInt64("customer_id", []int64{101, 102, 101, 103, 102}),
        galleon.NewSeriesInt64("product_id", []int64{1, 2, 1, 3, 2}),
        galleon.NewSeriesFloat64("amount", []float64{99.99, 149.99, 99.99, 199.99, 149.99}),
    )

    // Customers table
    customers, _ := galleon.NewDataFrame(
        galleon.NewSeriesInt64("id", []int64{101, 102, 103}),
        galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
        galleon.NewSeriesString("region", []string{"East", "West", "East"}),
    )

    // Products table
    products, _ := galleon.NewDataFrame(
        galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
        galleon.NewSeriesString("product_name", []string{"Widget", "Gadget", "Gizmo"}),
        galleon.NewSeriesString("category", []string{"Tools", "Electronics", "Electronics"}),
    )

    // Join orders with customers
    withCustomers, _ := orders.Join(customers,
        galleon.LeftOn("customer_id"),
        galleon.RightOn("id"),
    )

    // Join with products
    full, _ := withCustomers.Join(products,
        galleon.LeftOn("product_id"),
        galleon.RightOn("id"),
    )

    // Analyze: Sales by region and category
    result := full.GroupBy("region", "category").Agg(
        galleon.Col("amount").Sum().Alias("total_sales"),
        galleon.Col("order_id").Count().Alias("order_count"),
    ).Sort("total_sales", false)

    fmt.Println("Sales Analysis:")
    fmt.Println(result)
}
```

## Join API Reference

### Join Methods

```go
// Inner join
func (df *DataFrame) Join(other *DataFrame, opts ...JoinOption) (*DataFrame, error)

// Left join (keep all left rows)
func (df *DataFrame) LeftJoin(other *DataFrame, opts ...JoinOption) (*DataFrame, error)

// Right join (keep all right rows)
func (df *DataFrame) RightJoin(other *DataFrame, opts ...JoinOption) (*DataFrame, error)
```

### Join Options

```go
// Same column name in both DataFrames
func On(columns ...string) JoinOption

// Left DataFrame key columns
func LeftOn(columns ...string) JoinOption

// Right DataFrame key columns
func RightOn(columns ...string) JoinOption

// Suffix for duplicate column names
func Suffix(suffix string) JoinOption
```
