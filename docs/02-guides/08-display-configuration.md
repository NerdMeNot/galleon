# Display Configuration Guide

Galleon provides a flexible display system for DataFrames and Series with configurable formatting options, multiple table styles, and automatic truncation for large datasets.

## Overview

When you print a DataFrame or Series, Galleon automatically:
- Shows a shape header: `shape: (rows, cols)`
- Displays column names and data types
- Truncates large DataFrames (shows head + tail)
- Truncates wide DataFrames (shows first + last columns)
- Formats values with configurable precision

## Default Output

```go
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
    galleon.NewSeriesFloat64("score", []float64{95.5, 87.25, 92.0}),
)
fmt.Println(df)
```

Output:
```
shape: (3, 3)
╭──────────┬──────────┬──────────╮
│ id       │ name     │ score    │
│ Int64    │ String   │ Float64  │
├──────────┼──────────┼──────────┤
│        1 │    Alice │  95.5000 │
│        2 │      Bob │  87.2500 │
│        3 │  Charlie │  92.0000 │
╰──────────┴──────────┴──────────╯
```

## Large DataFrame Handling

For DataFrames with many rows, Galleon shows head and tail rows with an ellipsis indicator:

```go
// DataFrame with 1000 rows
fmt.Println(largeDF)
```

Output:
```
shape: (1000, 3)
╭──────────┬──────────┬──────────╮
│ id       │ name     │ value    │
│ Int64    │ String   │ Float64  │
├──────────┼──────────┼──────────┤
│        1 │   Item_1 │   0.0000 │
│        2 │   Item_2 │   1.5000 │
│        3 │   Item_3 │   3.0000 │
│        4 │   Item_4 │   4.5000 │
│        5 │   Item_5 │   6.0000 │
│        … │        … │        … │
│      996 │ Item_996 │1492.5000 │
│      997 │ Item_997 │1494.0000 │
│      998 │ Item_998 │1495.5000 │
│      999 │ Item_999 │1497.0000 │
│     1000 │Item_1000 │1498.5000 │
╰──────────┴──────────┴──────────╯
```

## Series Display

Series use the same display configuration as DataFrames:

```go
s := galleon.NewSeriesFloat64("prices", []float64{10.5, 20.3, 15.7, 8.2, 25.1})
fmt.Println(s)
```

Output:
```
Series: 'prices' (Float64)
length: 5
╭─────┬──────────╮
│   0 │  10.5000 │
│   1 │  20.3000 │
│   2 │  15.7000 │
│   3 │   8.2000 │
│   4 │  25.1000 │
╰─────┴──────────╯
```

### Large Series

Large Series show head and tail with ellipsis:

```go
// Series with 100 elements
fmt.Println(largeSeries)
```

Output:
```
Series: 'values' (Float64)
length: 100
╭─────┬──────────╮
│   0 │   0.0000 │
│   1 │   2.5000 │
│   2 │   5.0000 │
│   3 │   7.5000 │
│   4 │  10.0000 │
│   … │        … │
│  95 │ 237.5000 │
│  96 │ 240.0000 │
│  97 │ 242.5000 │
│  98 │ 245.0000 │
│  99 │ 247.5000 │
╰─────┴──────────╯
```

### Custom Series Display

```go
cfg := galleon.DefaultDisplayConfig()
cfg.FloatPrecision = 2
cfg.TableStyle = "ascii"

fmt.Println(s.StringWithConfig(cfg))
```

Output:
```
Series: 'prices' (Float64)
length: 5
+-----+----------+
|   0 |    10.50 |
|   1 |    20.30 |
|   2 |    15.70 |
|   3 |     8.20 |
|   4 |    25.10 |
+-----+----------+
```

## Configuration Options

### DisplayConfig Structure

```go
type DisplayConfig struct {
    MaxRows        int    // Max rows to display (default: 10)
    MaxCols        int    // Max columns to display (default: 10)
    MaxColWidth    int    // Max width per column (default: 25)
    MinColWidth    int    // Min width per column (default: 8)
    FloatPrecision int    // Decimal places for floats (default: 4)
    ShowDTypes     bool   // Show data types row (default: true)
    ShowShape      bool   // Show shape header (default: true)
    TableStyle     string // Border style (default: "rounded")
}
```

### Global Configuration

Set options globally to affect all DataFrame printing:

```go
// Set maximum rows to display
galleon.SetMaxDisplayRows(20)  // Show 10 head + 10 tail

// Set maximum columns to display
galleon.SetMaxDisplayCols(6)   // Show 3 first + 3 last columns

// Set float precision
galleon.SetFloatPrecision(2)   // 2 decimal places

// Set table style
galleon.SetTableStyle("ascii")  // Use ASCII characters
```

### Get Current Configuration

```go
cfg := galleon.GetDisplayConfig()
fmt.Printf("Max rows: %d\n", cfg.MaxRows)
fmt.Printf("Float precision: %d\n", cfg.FloatPrecision)
```

### Custom Configuration for Single Print

Use `StringWithConfig` for one-off custom formatting:

```go
cfg := galleon.DefaultDisplayConfig()
cfg.FloatPrecision = 2
cfg.ShowDTypes = false
cfg.TableStyle = "minimal"

fmt.Println(df.StringWithConfig(cfg))
```

## Table Styles

Galleon supports four table styles:

### Rounded (Default)

```go
galleon.SetTableStyle("rounded")
```

```
╭──────────┬──────────╮
│ id       │ value    │
│ Int64    │ Float64  │
├──────────┼──────────┤
│        1 │  10.5000 │
│        2 │  20.3000 │
╰──────────┴──────────╯
```

### Sharp

```go
galleon.SetTableStyle("sharp")
```

```
┌──────────┬──────────┐
│ id       │ value    │
│ Int64    │ Float64  │
├──────────┼──────────┤
│        1 │  10.5000 │
│        2 │  20.3000 │
└──────────┴──────────┘
```

### ASCII

For terminals that don't support Unicode:

```go
galleon.SetTableStyle("ascii")
```

```
+----------+----------+
| id       | value    |
| Int64    | Float64  |
+----------+----------+
|        1 |  10.5000 |
|        2 |  20.3000 |
+----------+----------+
```

### Minimal

Clean look without borders:

```go
galleon.SetTableStyle("minimal")
```

```
 ────────── ──────────
  id         value
  Int64      Float64
 ────────── ──────────
         1    10.5000
         2    20.3000
 ────────── ──────────
```

## Common Patterns

### Compact Output for Logs

```go
cfg := galleon.DefaultDisplayConfig()
cfg.MaxRows = 4
cfg.FloatPrecision = 2
cfg.ShowDTypes = false
cfg.TableStyle = "minimal"

log.Println(df.StringWithConfig(cfg))
```

### Full Precision for Debugging

```go
cfg := galleon.DefaultDisplayConfig()
cfg.FloatPrecision = 10
cfg.MaxColWidth = 40

fmt.Println(df.StringWithConfig(cfg))
```

### Wide DataFrames

```go
// Show all columns regardless of terminal width
galleon.SetMaxDisplayCols(100)
fmt.Println(wideDF)

// Or limit to fit terminal
galleon.SetMaxDisplayCols(6)  // Shows first 3 and last 3
fmt.Println(wideDF)
```

### Temporary Configuration

```go
// Save current config
original := galleon.GetDisplayConfig()

// Modify for specific output
galleon.SetFloatPrecision(2)
galleon.SetTableStyle("ascii")
fmt.Println(df)

// Restore original
galleon.SetDisplayConfig(original)
```

## Value Formatting

### Numeric Values

- **Integers**: Right-aligned, no decimal places
- **Floats**: Right-aligned, configurable precision (default 4)
- **NaN/Null**: Displayed as `null`

### String Values

- Right-aligned within column
- Truncated with `...` if longer than `MaxColWidth`

### Boolean Values

- Displayed as `true` or `false`

## API Reference

### Global Functions

```go
// Get/Set entire configuration
func GetDisplayConfig() DisplayConfig
func SetDisplayConfig(cfg DisplayConfig)

// Individual setters
func SetMaxDisplayRows(n int)
func SetMaxDisplayCols(n int)
func SetFloatPrecision(n int)
func SetTableStyle(style string)

// Get default configuration
func DefaultDisplayConfig() DisplayConfig
```

### DataFrame Methods

```go
// Use global configuration
func (df *DataFrame) String() string

// Use custom configuration
func (df *DataFrame) StringWithConfig(cfg DisplayConfig) string
```

## Best Practices

1. **Set defaults at startup**: Configure display options once at application start
   ```go
   func init() {
       galleon.SetFloatPrecision(2)
       galleon.SetTableStyle("rounded")
   }
   ```

2. **Use ASCII for logs**: When writing to log files, use ASCII style for compatibility
   ```go
   galleon.SetTableStyle("ascii")
   ```

3. **Limit rows for large data**: Prevent overwhelming output
   ```go
   galleon.SetMaxDisplayRows(20)  // 10 head + 10 tail
   ```

4. **Use StringWithConfig for reports**: Custom formatting without affecting global state
   ```go
   report := df.StringWithConfig(reportConfig)
   ```
