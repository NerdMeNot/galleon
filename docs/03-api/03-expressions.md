# Expressions API Reference

Expressions provide a way to build queries declaratively. They are used with `Filter`, `WithColumn`, `GroupBy.Agg`, and lazy evaluation.

## Column References

### Col

Creates a column reference expression.

```go
func Col(name string) Expr
```

**Example:**
```go
expr := galleon.Col("value")
```

## Literal Values

### Lit

Creates a literal value expression.

```go
func Lit(value interface{}) Expr
```

**Supported types:**
- `int`, `int32`, `int64`
- `float32`, `float64`
- `string`
- `bool`

**Example:**
```go
galleon.Lit(100)        // int
galleon.Lit(3.14)       // float64
galleon.Lit("hello")    // string
galleon.Lit(true)       // bool
```

## Arithmetic Operations

All arithmetic operations return a new `Expr`.

### Add

Addition operation.

```go
func (e Expr) Add(other Expr) Expr
```

**Example:**
```go
// Column + Literal
Col("price").Add(Lit(10))

// Column + Column
Col("a").Add(Col("b"))
```

### Sub

Subtraction operation.

```go
func (e Expr) Sub(other Expr) Expr
```

### Mul

Multiplication operation.

```go
func (e Expr) Mul(other Expr) Expr
```

**Example:**
```go
// Calculate total: price * quantity
Col("price").Mul(Col("quantity"))
```

### Div

Division operation.

```go
func (e Expr) Div(other Expr) Expr
```

### Mod

Modulo operation.

```go
func (e Expr) Mod(other Expr) Expr
```

## Comparison Operations

All comparison operations return a boolean expression.

### Eq

Equality comparison.

```go
func (e Expr) Eq(other Expr) Expr
```

**Example:**
```go
Col("status").Eq(Lit("active"))
```

### Neq

Not-equal comparison.

```go
func (e Expr) Neq(other Expr) Expr
```

### Gt

Greater than comparison.

```go
func (e Expr) Gt(other Expr) Expr
```

**Example:**
```go
Col("age").Gt(Lit(18))
```

### Gte

Greater than or equal comparison.

```go
func (e Expr) Gte(other Expr) Expr
```

### Lt

Less than comparison.

```go
func (e Expr) Lt(other Expr) Expr
```

### Lte

Less than or equal comparison.

```go
func (e Expr) Lte(other Expr) Expr
```

## Logical Operations

### And

Logical AND operation.

```go
func (e Expr) And(other Expr) Expr
```

**Example:**
```go
Col("age").Gt(Lit(18)).And(Col("age").Lt(Lit(65)))
```

### Or

Logical OR operation.

```go
func (e Expr) Or(other Expr) Expr
```

### Not

Logical NOT operation.

```go
func (e Expr) Not() Expr
```

## Aggregation Functions

These functions create aggregate expressions for use with `GroupBy.Agg`.

### Sum

Sum aggregation.

```go
func (e Expr) Sum() Expr
```

**Example:**
```go
Col("sales").Sum()
```

### Min

Minimum aggregation.

```go
func (e Expr) Min() Expr
```

### Max

Maximum aggregation.

```go
func (e Expr) Max() Expr
```

### Mean

Average aggregation.

```go
func (e Expr) Mean() Expr
```

### Count

Count aggregation.

```go
func (e Expr) Count() Expr
```

### First

First value in group.

```go
func (e Expr) First() Expr
```

### Last

Last value in group.

```go
func (e Expr) Last() Expr
```

### Std

Standard deviation aggregation.

```go
func (e Expr) Std() Expr
```

### Var

Variance aggregation.

```go
func (e Expr) Var() Expr
```

### Skewness

Skewness (3rd standardized moment) aggregation.

```go
func (e Expr) Skewness() Expr
```

**Example:**
```go
Col("values").Skewness()
```

**Note:** `Skew()` is deprecated, use `Skewness()` instead.

### Kurtosis

Kurtosis (4th standardized moment) aggregation.

```go
func (e Expr) Kurtosis() Expr
```

**Example:**
```go
Col("values").Kurtosis()
```

**Note:** `Kurt()` is deprecated, use `Kurtosis()` instead.

### Median

Median value aggregation.

```go
func (e Expr) Median() Expr
```

### Quantile

Quantile aggregation at specified probability.

```go
func (e Expr) Quantile(q float64) Expr
```

**Example:**
```go
// 95th percentile
Col("response_time").Quantile(0.95)
```

## Naming and Aliasing

### Alias

Assigns a name to the expression result.

```go
func (e Expr) Alias(name string) Expr
```

**Example:**
```go
Col("price").Mul(Col("quantity")).Alias("total")
```

## Type Casting

### Cast

Converts to a different type.

```go
func (e Expr) Cast(dtype DType) Expr
```

**Example:**
```go
Col("id").Cast(galleon.Float64)
```

## String Operations

String operations are accessed via the `Str()` namespace on column expressions.

### Str

Returns a string namespace for string operations.

```go
func (e Expr) Str() *StringNamespace
```

### Upper

Converts string to uppercase.

```go
func (ns *StringNamespace) Upper() Expr
```

**Example:**
```go
Col("name").Str().Upper()
```

### Lower

Converts string to lowercase.

```go
func (ns *StringNamespace) Lower() Expr
```

**Example:**
```go
Col("email").Str().Lower()
```

### Len

Returns the length of the string.

```go
func (ns *StringNamespace) Len() Expr
```

**Example:**
```go
Col("description").Str().Len()
```

### Contains

Checks if string contains substring.

```go
func (ns *StringNamespace) Contains(pattern string) Expr
```

**Example:**
```go
Col("text").Str().Contains("error")
```

### StartsWith

Checks if string starts with prefix.

```go
func (ns *StringNamespace) StartsWith(prefix string) Expr
```

**Example:**
```go
Col("url").Str().StartsWith("https://")
```

### EndsWith

Checks if string ends with suffix.

```go
func (ns *StringNamespace) EndsWith(suffix string) Expr
```

**Example:**
```go
Col("filename").Str().EndsWith(".csv")
```

### Replace

Replaces occurrences of a pattern with a replacement string.

```go
func (ns *StringNamespace) Replace(old, new string) Expr
```

**Example:**
```go
Col("text").Str().Replace("foo", "bar")
```

### Trim

Removes leading and trailing whitespace.

```go
func (ns *StringNamespace) Trim() Expr
```

**Example:**
```go
Col("input").Str().Trim()
```

## Window Functions

Window functions perform calculations across rows related to the current row.

### Lag

Accesses a value from a previous row.

```go
func (e Expr) Lag(offset int, defaultValue interface{}) Expr
```

**Example:**
```go
// Previous day's closing price
Col("close_price").Lag(1, 0.0)
```

### Lead

Accesses a value from a following row.

```go
func (e Expr) Lead(offset int, defaultValue interface{}) Expr
```

**Example:**
```go
// Next day's opening price
Col("open_price").Lead(1, 0.0)
```

### Diff

Calculates the difference between current and previous row (1 period).

```go
func (e Expr) Diff() Expr
```

**Example:**
```go
// Daily price change
Col("price").Diff()
```

### DiffN

Calculates the difference with n periods offset.

```go
func (e Expr) DiffN(n int) Expr
```

**Example:**
```go
// Week-over-week change
Col("value").DiffN(7)
```

### PctChange

Calculates the percentage change from previous row.

```go
func (e Expr) PctChange() Expr
```

**Example:**
```go
// Daily return percentage
Col("price").PctChange()
```

### CumSum

Cumulative sum aggregation.

```go
func (e Expr) CumSum() Expr
```

**Example:**
```go
// Running total
Col("sales").CumSum()
```

### CumMin

Cumulative minimum aggregation.

```go
func (e Expr) CumMin() Expr
```

**Example:**
```go
// Lowest price seen so far
Col("price").CumMin()
```

### CumMax

Cumulative maximum aggregation.

```go
func (e Expr) CumMax() Expr
```

**Example:**
```go
// Highest price seen so far
Col("price").CumMax()
```

### RollingSum

Rolling window sum aggregation.

```go
func (e Expr) RollingSum(windowSize int, minPeriods int) Expr
```

**Example:**
```go
// 7-day rolling sum
Col("daily_sales").RollingSum(7, 1)
```

### RollingMean

Rolling window mean aggregation.

```go
func (e Expr) RollingMean(windowSize int, minPeriods int) Expr
```

**Example:**
```go
// 30-day moving average
Col("price").RollingMean(30, 20)
```

## Null Handling

### IsNull

Checks for null values.

```go
func (e Expr) IsNull() Expr
```

### IsNotNull

Checks for non-null values.

```go
func (e Expr) IsNotNull() Expr
```

### FillNull

Replaces null values.

```go
func (e Expr) FillNull(value interface{}) Expr
```

## Usage Examples

### Filtering

```go
// Simple comparison
df.Filter(Col("value").Gt(Lit(100)))

// Multiple conditions
df.Filter(
    Col("age").Gte(Lit(18)).And(
        Col("status").Eq(Lit("active")),
    ),
)

// Complex expression
df.Filter(
    Col("price").Mul(Col("quantity")).Gt(Lit(1000)),
)
```

### Adding Columns

```go
// Computed column
df.WithColumn("total", Col("price").Mul(Col("quantity")))

// Conditional column (using expression)
df.WithColumn("category",
    Col("value").Gt(Lit(100)).Cast(galleon.Int64),
)
```

### GroupBy Aggregations

```go
df.GroupBy("category").Agg(
    Col("value").Sum().Alias("total"),
    Col("value").Mean().Alias("average"),
    Col("value").Min().Alias("minimum"),
    Col("value").Max().Alias("maximum"),
    Col("id").Count().Alias("count"),
)
```

### Lazy Evaluation

```go
result, _ := df.Lazy().
    Filter(Col("date").Gte(Lit("2024-01-01"))).
    WithColumn("revenue", Col("price").Mul(Col("qty"))).
    GroupBy("product")).
    Agg(
        Col("revenue").Sum().Alias("total_revenue"),
        Col("qty").Sum().Alias("total_units"),
    ).
    Sort("total_revenue", false).
    Limit(10).
    Collect()
```

### Building Expressions Dynamically

```go
// Build expressions programmatically
columns := []string{"col1", "col2", "col3"}
aggs := make([]galleon.Expr, len(columns))
for i, col := range columns {
    aggs[i] = galleon.Col(col).Sum().Alias(col + "_sum")
}

result := df.GroupBy("key").Agg(aggs...)
```

## Expression String Representation

Every expression has a `String()` method for debugging:

```go
expr := Col("price").Mul(Col("quantity")).Alias("total")
fmt.Println(expr.String())
// Output: (price * quantity) AS total
```

## Advanced Examples

### Time Series Analysis

```go
result, _ := df.Lazy().
    // Calculate daily returns
    WithColumn("return", Col("close").PctChange()).
    // Calculate 20-day moving average
    WithColumn("ma20", Col("close").RollingMean(20, 15)).
    // Calculate 50-day moving average
    WithColumn("ma50", Col("close").RollingMean(50, 40)).
    // Generate trading signal
    WithColumn("signal",
        Col("ma20").Gt(Col("ma50")).Cast(Int64),
    ).
    Collect()
```

### Text Processing

```go
result, _ := df.Lazy().
    // Normalize email addresses
    WithColumn("email_normalized", Col("email").Str().Lower().Str().Trim()).
    // Extract domain
    WithColumn("has_error", Col("log_message").Str().Contains("ERROR")).
    // Filter valid URLs
    Filter(Col("url").Str().StartsWith("https://")).
    // Clean filenames
    WithColumn("filename_clean",
        Col("filename").Str().Replace(" ", "_").Str().Lower(),
    ).
    Collect()
```

### Statistical Analysis

```go
stats, _ := df.Lazy().
    GroupBy("category")).
    Agg(
        Col("value").Count().Alias("n"),
        Col("value").Mean().Alias("mean"),
        Col("value").Median().Alias("median"),
        Col("value").Std().Alias("std_dev"),
        Col("value").Skewness().Alias("skewness"),
        Col("value").Kurtosis().Alias("kurtosis"),
        Col("value").Quantile(0.25).Alias("q25"),
        Col("value").Quantile(0.75).Alias("q75"),
        Col("value").Min().Alias("min"),
        Col("value").Max().Alias("max"),
    ).
    Collect()
```

### Financial Calculations

```go
result, _ := df.Lazy().
    // Sort by date
    Sort("date", true).
    // Calculate returns
    WithColumn("return", Col("close").PctChange()).
    // Calculate volatility (20-day rolling std)
    WithColumn("volatility", Col("return").RollingStd(20, 15)).
    // Calculate cumulative returns
    WithColumn("cum_return",
        Col("return").Add(Lit(1.0)).CumProd().Sub(Lit(1.0)),
    ).
    // Calculate running maximum price (for drawdown)
    WithColumn("running_max", Col("close").CumMax()).
    // Calculate drawdown
    WithColumn("drawdown",
        Col("close").Sub(Col("running_max")).Div(Col("running_max")),
    ).
    Collect()
```

### Data Quality Checks

```go
quality, _ := df.Lazy().
    // Null statistics per column
    Select(
        Lit(df.Height()).Alias("total_rows"),
        Col("id").IsNull().Sum().Alias("id_nulls"),
        Col("value").IsNull().Sum().Alias("value_nulls"),
        Col("category").IsNull().Sum().Alias("category_nulls"),
    ).
    // Add percentages
    WithColumn("id_null_pct",
        Col("id_nulls").Cast(Float64).Div(Col("total_rows")).Mul(Lit(100.0)),
    ).
    Collect()
```

### Cohort Analysis

```go
cohorts, _ := df.Lazy().
    // Calculate user tenure
    Sort("user_id", Col("date"), true).
    WithColumn("first_purchase",
        Col("date").Over().PartitionBy("user_id").First(),
    ).
    WithColumn("days_since_first",
        Col("date").Sub(Col("first_purchase")),
    ).
    // Group by cohort and tenure
    GroupBy("first_purchase", "days_since_first").
    Agg(
        Col("user_id").NUnique().Alias("active_users"),
        Col("amount").Sum().Alias("total_revenue"),
        Col("amount").Mean().Alias("avg_order_value"),
    ).
    Collect()
```
