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

### Contains

Checks if string contains substring.

```go
func (e Expr) Contains(pattern string) Expr
```

### StartsWith

Checks if string starts with prefix.

```go
func (e Expr) StartsWith(prefix string) Expr
```

### EndsWith

Checks if string ends with suffix.

```go
func (e Expr) EndsWith(suffix string) Expr
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
    GroupBy(Col("product")).
    Agg(
        Col("revenue").Sum().Alias("total_revenue"),
        Col("qty").Sum().Alias("total_units"),
    ).
    Sort(Col("total_revenue"), false).
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
