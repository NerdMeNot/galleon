# Galleon

A high-performance DataFrame library for Go, powered by Zig SIMD operations.

[![Go Reference](https://pkg.go.dev/badge/github.com/NerdMeNot/galleon/go.svg)](https://pkg.go.dev/github.com/NerdMeNot/galleon/go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Note**: This project is under active development and is not yet recommended for production use. APIs may change, and some features are still being optimized. Contributions and feedback are welcome!

## Overview

Galleon combines Go's developer ergonomics with Zig's low-level SIMD capabilities to deliver high-performance DataFrame operations. It provides a Polars-inspired API for data manipulation, aggregation, joins, and I/O operations.

### Key Features

- **SIMD-Accelerated Operations**: 2-15x speedups for aggregations, filtering, and comparisons
- **Parallel Execution**: Auto-detecting thread configuration with work-stealing distribution
- **Zero-Copy Data Access**: Go slices directly view Zig-allocated memory
- **Lazy Evaluation**: Query optimization with predicate and projection pushdown
- **Multiple I/O Formats**: CSV, JSON, and Parquet support

## Installation

### Prerequisites

- Go 1.21 or later
- Zig 0.11 or later

### Building

```bash
# Clone the repository
git clone https://github.com/NerdMeNot/galleon.git
cd galleon

# Build the Zig SIMD library
cd core
zig build -Doptimize=ReleaseFast
cd ..

# Install the Go package
go get github.com/NerdMeNot/galleon/go
```

## Quick Start

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Option 1: Create from Series
    df, _ := galleon.NewDataFrame(
        galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
        galleon.NewSeriesFloat64("value", []float64{10.5, 20.3, 15.7, 8.2, 25.1}),
        galleon.NewSeriesString("category", []string{"A", "B", "A", "B", "A"}),
    )

    // Option 2: Create from structs
    type Record struct {
        ID       int64   `galleon:"id"`
        Value    float64 `galleon:"value"`
        Category string  `galleon:"category"`
    }
    records := []Record{ /* ... */ }
    df, _ := galleon.FromStructs(records)

    // Option 3: Create from maps
    mapData := []map[string]interface{}{
        {"id": 1, "value": 10.5, "category": "A"},
        {"id": 2, "value": 20.3, "category": "B"},
    }
    df, _ := galleon.FromRecords(mapData)

    // Filter and aggregate
    result := df.
        Filter(galleon.Col("value").Gt(galleon.Lit(10.0))).
        GroupBy("category").
        Agg(galleon.Col("value").Sum().Alias("total"))

    fmt.Println(result)
}
```

## Feature Showcase

### Data Loading

```go
// From structs with tags
type User struct {
    UserID   int64   `galleon:"user_id"`
    FullName string  `galleon:"name"`
    Email    string  `galleon:"email"`
    Internal string  `galleon:"-"`  // Skip this field
}

users := []User{
    {UserID: 1, FullName: "Alice Smith", Email: "alice@example.com"},
    {UserID: 2, FullName: "Bob Jones", Email: "bob@example.com"},
}
df, _ := galleon.FromStructs(users)

// From maps with auto type detection
records := []map[string]interface{}{
    {"id": 1, "name": "Alice", "score": 95.5, "active": true},
    {"id": 2, "name": "Bob", "score": 87.3, "active": false},
}
df, _ := galleon.FromRecords(records)
// Types auto-detected: id=Int64, name=String, score=Float64, active=Bool
```

### Window Functions & Time Series

```go
// Calculate moving averages and price changes
result, _ := df.Lazy().
    Sort(galleon.Col("date"), true).
    // Price changes
    WithColumn("price_change", galleon.Col("close").Diff()).
    WithColumn("pct_return", galleon.Col("close").PctChange()).
    // Moving averages
    WithColumn("ma20", galleon.Col("close").RollingMean(20, 15)).
    WithColumn("ma50", galleon.Col("close").RollingMean(50, 40)).
    // Cumulative metrics
    WithColumn("running_max", galleon.Col("close").CumMax()).
    WithColumn("running_total", galleon.Col("volume").CumSum()).
    Collect()
```

### String Operations

```go
// Clean and process text data
result, _ := df.Lazy().
    // Normalize text
    WithColumn("email_clean", galleon.Col("email").Str().Lower().Str().Trim()).
    // Filter by pattern
    Filter(galleon.Col("filename").Str().EndsWith(".csv")).
    // Search text
    WithColumn("has_error", galleon.Col("log").Str().Contains("ERROR")).
    // Transform
    WithColumn("uppercase_name", galleon.Col("name").Str().Upper()).
    Collect()
```

### Pivot & Melt (Reshape)

```go
// Long to wide format
wide, _ := df.Lazy().
    Pivot(galleon.PivotOptions{
        Index:  "date",
        Column: "metric",
        Values: "value",
        AggFn:  galleon.AggTypeSum,
    }).
    Collect()

// Wide to long format
long, _ := wide.Lazy().
    Melt(galleon.MeltOptions{
        IDVars:    []string{"date"},
        ValueVars: []string{"sales", "cost"},
        VarName:   "metric",
        ValueName: "amount",
    }).
    Collect()
```

### Advanced Aggregations

```go
// Statistical analysis by group
stats, _ := df.Lazy().
    GroupBy(galleon.Col("category")).
    Agg(
        galleon.Col("value").Count().Alias("n"),
        galleon.Col("value").Mean().Alias("mean"),
        galleon.Col("value").Median().Alias("median"),
        galleon.Col("value").Std().Alias("std_dev"),
        galleon.Col("value").Skewness().Alias("skewness"),
        galleon.Col("value").Kurtosis().Alias("kurtosis"),
        galleon.Col("value").Quantile(0.95).Alias("p95"),
    ).
    Collect()
```

### User-Defined Functions

```go
// Apply custom transformations
result, _ := df.Lazy().
    Apply("price", func(s *galleon.Series) (*galleon.Series, error) {
        data := s.Float64()
        result := make([]float64, len(data))
        for i, v := range data {
            // Custom business logic
            result[i] = math.Log(v) * 100
        }
        return galleon.NewSeriesFloat64("log_price", result), nil
    }).
    Collect()
```

### Caching for Performance

```go
// Cache expensive intermediate results
cached := df.Lazy().
    Filter(galleon.Col("date").Gte(galleon.Lit("2024-01-01"))).
    GroupBy(galleon.Col("product")).
    Agg(galleon.Col("sales").Sum().Alias("total_sales")).
    Cache()  // Materialize once

// Reuse cached result multiple times
topProducts, _ := cached.Sort(galleon.Col("total_sales"), false).Head(10).Collect()
avgSales, _ := cached.Select(galleon.Col("total_sales").Mean()).Collect()
```

## Performance

Galleon achieves significant speedups over pure Go implementations:

| Operation | Speedup vs Go |
|-----------|---------------|
| Min/Max | 2-3x |
| Filtering | 10-15x |
| Inner Join (1M rows) | ~60ms |
| Left Join (1M rows) | ~60ms |

### Thread Configuration

```go
// Auto-detect CPU cores (default)
galleon.SetMaxThreads(0)

// Or set explicitly
galleon.SetMaxThreads(8)

// Check configuration
config := galleon.GetThreadConfig()
fmt.Printf("Using %d threads (auto=%v)\n", config.MaxThreads, config.AutoDetected)
```

## Documentation

- [Philosophy](docs/00-philosophy.md) - Why Galleon exists and design decisions
- [Technical Whitepaper](WHITEPAPER.md) - Detailed architecture and implementation
- [Full Documentation](docs/README.md) - Installation, guides, and API reference
- [API Reference](https://pkg.go.dev/github.com/NerdMeNot/galleon/go) - Go package documentation
- [Contributing](CONTRIBUTING.md) - How to contribute

## Project Structure

```
galleon/
├── core/                    # Zig SIMD backend
│   ├── src/
│   │   ├── main.zig         # CGO exports
│   │   ├── simd.zig         # SIMD module entry
│   │   ├── simd/            # SIMD operations (modular)
│   │   │   ├── aggregations.zig
│   │   │   ├── arithmetic.zig
│   │   │   ├── comparisons.zig
│   │   │   ├── filters.zig
│   │   │   ├── joins.zig
│   │   │   └── ...
│   │   ├── groupby.zig      # GroupBy hash tables
│   │   └── column.zig       # Column storage
│   ├── include/             # C headers for CGO
│   └── build.zig
├── go/                      # Go package
│   ├── galleon.go           # CGO bindings
│   ├── series.go            # Series type
│   ├── dataframe.go         # DataFrame type
│   ├── lazyframe.go         # Lazy evaluation
│   ├── join.go              # Join operations
│   ├── groupby.go           # GroupBy operations
│   ├── io_*.go              # I/O (CSV, JSON, Parquet)
│   └── benchmarks/          # Performance tests
├── docs/                    # Documentation
│   ├── 00-philosophy.md
│   ├── 01-getting-started/
│   ├── 02-guides/
│   ├── 03-api/
│   └── 04-reference/
├── WHITEPAPER.md
└── justfile                 # Build commands
```

## Supported Operations

### Aggregations
- **Basic**: `Sum`, `Min`, `Max`, `Mean`, `Count`, `Std`, `Var`
- **Statistical**: `Median`, `Quantile`, `Skewness`, `Kurtosis`, `Correlation`
- **Multi-column**: `SumHorizontal`, `MinHorizontal`, `MaxHorizontal`

### Element-wise Operations
- **Arithmetic**: `Add`, `Sub`, `Mul`, `Div` (scalar and vector)
- **Comparisons**: `Gt`, `Lt`, `Eq`, `Neq`, `Gte`, `Lte`
- **Null handling**: `IsNull`, `IsNotNull`, `FillNull`, `Coalesce`

### String Operations
- **Transformations**: `Upper`, `Lower`, `Trim`, `Replace`
- **Predicates**: `Contains`, `StartsWith`, `EndsWith`
- **Metrics**: `Len`

### Window Functions
- **Shift**: `Lag`, `Lead`, `Diff`, `DiffN`, `PctChange`
- **Cumulative**: `CumSum`, `CumMin`, `CumMax`
- **Rolling**: `RollingSum`, `RollingMean`, `RollingMin`, `RollingMax`, `RollingStd`
- **Ranking**: `RowNumber`, `Rank`, `DenseRank`

### DataFrame Operations
- **Selection**: `Select`, `Drop`, `Filter`, `Sort`, `Head`, `Tail`, `Slice`
- **Transformation**: `WithColumn`, `Rename`, `Cast`, `Distinct`
- **Aggregation**: `GroupBy`, `Agg`
- **Joining**: `Join`, `LeftJoin`, `RightJoin`, `InnerJoin`
- **Reshaping**: `Pivot`, `Melt`
- **UDF**: `Apply` (user-defined functions)

### I/O Formats
- **CSV**: `ReadCSV`, `WriteCSV`, `ScanCSV`
- **JSON**: `ReadJSON`, `WriteJSON`
- **Parquet**: `ReadParquet`, `WriteParquet`, `ScanParquet`

### Lazy Evaluation
- **Scanning**: `ScanCSV`, `ScanParquet`, `ScanJSON`
- **Optimization**: Predicate pushdown, projection pruning
- **Caching**: `Cache()` for intermediate result materialization
- **Execution**: Deferred with `Collect()` or `Fetch(n)`

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by [Polars](https://pola.rs/) DataFrame library
- SIMD techniques from database research literature
- Go community for CGO best practices
