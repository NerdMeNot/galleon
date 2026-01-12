# Reshape Operations Guide

Reshape operations transform data between wide and long formats. Galleon provides `Pivot` and `Melt` operations for data reshaping.

## Overview

### Wide vs Long Format

**Long format** (tidy data):
```
| date     | metric   | value |
|----------|----------|-------|
| 2024-01  | sales    | 100   |
| 2024-01  | cost     | 60    |
| 2024-02  | sales    | 150   |
| 2024-02  | cost     | 80    |
```

**Wide format**:
```
| date     | sales | cost |
|----------|-------|------|
| 2024-01  | 100   | 60   |
| 2024-02  | 150   | 80   |
```

## Pivot - Long to Wide

Pivot transforms long-format data into wide format by creating new columns from unique values.

### Basic Pivot

```go
result, _ := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "date",      // Row identifier
        Column: "metric",    // Column to pivot (becomes new column names)
        Values: "value",     // Values to populate new columns
    }).
    Collect()
```

**Input (long format):**
```
| date     | metric   | value |
|----------|----------|-------|
| 2024-01  | sales    | 100   |
| 2024-01  | cost     | 60    |
| 2024-02  | sales    | 150   |
| 2024-02  | cost     | 80    |
```

**Output (wide format):**
```
| date     | sales | cost |
|----------|-------|------|
| 2024-01  | 100   | 60   |
| 2024-02  | 150   | 80   |
```

### Pivot with Aggregation

When multiple values exist for the same index-column combination, specify an aggregation function:

```go
result, _ := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "date",
        Column: "metric",
        Values: "value",
        AggFn:  galleon.AggTypeSum,  // Sum duplicates
    }).
    Collect()
```

**Available aggregation functions:**
- `AggTypeSum` - Sum all values
- `AggTypeMean` - Calculate average
- `AggTypeMin` - Take minimum value
- `AggTypeMax` - Take maximum value
- `AggTypeFirst` - Take first value (default)
- `AggTypeLast` - Take last value
- `AggTypeCount` - Count occurrences

### Multi-Level Pivots

```go
// Pivot with multiple columns in index
result, _ := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "region",
        Column: "product",
        Values: "sales",
        AggFn:  galleon.AggTypeSum,
    }).
    Collect()
```

## Melt - Wide to Long

Melt transforms wide-format data into long format by unpivoting columns.

### Basic Melt

```go
result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"date"},     // Columns to keep
        ValueVars: []string{"sales", "cost"},  // Columns to unpivot
        VarName:   "metric",             // Name for variable column
        ValueName: "value",              // Name for value column
    }).
    Collect()
```

**Input (wide format):**
```
| date     | sales | cost |
|----------|-------|------|
| 2024-01  | 100   | 60   |
| 2024-02  | 150   | 80   |
```

**Output (long format):**
```
| date     | metric | value |
|----------|--------|-------|
| 2024-01  | sales  | 100   |
| 2024-01  | cost   | 60    |
| 2024-02  | sales  | 150   |
| 2024-02  | cost   | 80    |
```

### Auto-Detect Value Variables

When `ValueVars` is empty, all non-ID columns are melted:

```go
result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars: []string{"id", "date"},
        // ValueVars omitted - melts all other columns
    }).
    Collect()
```

### Default Column Names

If `VarName` or `ValueName` are not specified, defaults are used:

```go
result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars: []string{"date"},
        // VarName defaults to "variable"
        // ValueName defaults to "value"
    }).
    Collect()
```

## Common Use Cases

### Time Series Data

Convert wide time series to long format for easier analysis:

```go
// Wide format: columns for each month
// date | jan | feb | mar | ...
// Convert to long format for time series analysis

result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"product_id"},
        VarName:   "month",
        ValueName: "sales",
    }).
    // Sort by time
    Sort(galleon.Col("month"), true).
    Collect()
```

### Crosstab to Normalized Form

```go
// Crosstab: regions as rows, products as columns
// Convert to normalized form for database storage

result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"region"},
        VarName:   "product",
        ValueName: "sales",
    }).
    // Remove zero sales
    Filter(galleon.Col("sales").Gt(galleon.Lit(0))).
    Collect()
```

### Survey Data Processing

```go
// Wide survey responses (one column per question)
// Convert to long format for analysis

result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"respondent_id", "date"},
        VarName:   "question",
        ValueName: "response",
    }).
    // Calculate response distributions
    GroupBy(galleon.Col("question")).
    Agg(
        galleon.Col("response").Mean().Alias("avg_score"),
        galleon.Col("response").Std().Alias("std_dev"),
        galleon.Col("respondent_id").Count().Alias("n_responses"),
    ).
    Collect()
```

### Report Generation

Pivot aggregated data for reporting:

```go
// Create monthly sales report by product category

result, _ := df.Lazy().
    // Aggregate to monthly level
    GroupBy(galleon.Col("month"), galleon.Col("category")).
    Agg(
        galleon.Col("sales").Sum().Alias("total_sales"),
    ).
    // Pivot to wide format for report
    Pivot(galleon.PivotOptions{
        Index:  "category",
        Column: "month",
        Values: "total_sales",
        AggFn:  galleon.AggTypeSum,
    }).
    Collect()
```

## Combining Pivot and Melt

Round-trip operations for data transformations:

```go
// Start with long format
df, _ := galleon.ReadCSV("sales.csv")

// Pivot to wide for calculations
wide, _ := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "product",
        Column: "quarter",
        Values: "revenue",
    }).
    // Calculate year-over-year growth
    WithColumn("growth",
        galleon.Col("Q4_2024").Sub(galleon.Col("Q4_2023")).
            Div(galleon.Col("Q4_2023")),
    ).
    Collect()

// Melt back to long format
long, _ := wide.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"product", "growth"},
        VarName:   "quarter",
        ValueName: "revenue",
    }).
    Collect()
```

## Performance Tips

### 1. Pivot Performance

```go
// For large datasets, filter before pivoting
result, _ := df.Lazy().
    Filter(galleon.Col("date").Gte(galleon.Lit("2024-01-01"))).
    Pivot(galleon.PivotOptions{
        Index:  "customer_id",
        Column: "product",
        Values: "quantity",
        AggFn:  galleon.AggTypeSum,
    }).
    Collect()
```

### 2. Melt Performance

```go
// Specify ValueVars explicitly for better performance
result, _ := df.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"id"},
        ValueVars: []string{"col1", "col2", "col3"},  // Explicit is faster
    }).
    Collect()
```

### 3. Memory Considerations

```go
// For very wide pivots, consider filtering columns first
result, _ := df.Lazy().
    // Keep only top 10 products
    Filter(
        galleon.Col("product").IsIn(topProducts),
    ).
    Pivot(galleon.PivotOptions{
        Index:  "date",
        Column: "product",
        Values: "sales",
    }).
    Collect()
```

## Comparison with Other Tools

### Pandas

```python
# Pandas pivot
df.pivot(index='date', columns='metric', values='value')

# Galleon equivalent
df.Lazy().Pivot(PivotOptions{
    Index: "date", Column: "metric", Values: "value",
}).Collect()
```

```python
# Pandas melt
df.melt(id_vars=['date'], value_vars=['sales', 'cost'])

# Galleon equivalent
df.Lazy().Melt(MeltOptions{
    IDVars: []string{"date"},
    ValueVars: []string{"sales", "cost"},
}).Collect()
```

### Polars

```python
# Polars pivot
df.pivot(values="value", index="date", columns="metric")

# Galleon equivalent
df.Lazy().Pivot(PivotOptions{
    Index: "date", Column: "metric", Values: "value",
}).Collect()
```

## API Reference

### Pivot

```go
type PivotOptions struct {
    Index  string   // Column to use as row identifier
    Column string   // Column whose values become new column names
    Values string   // Column whose values populate the new columns
    AggFn  AggType  // Aggregation function for duplicate values
}

func (lf *LazyFrame) Pivot(opts PivotOptions) *LazyFrame
```

### Melt

```go
type MeltOptions struct {
    IDVars    []string // Columns to keep as identifier variables
    ValueVars []string // Columns to unpivot (empty = all non-ID columns)
    VarName   string   // Name for the variable column (default: "variable")
    ValueName string   // Name for the value column (default: "value")
}

func (lf *LazyFrame) Melt(opts MeltOptions) *LazyFrame
```
