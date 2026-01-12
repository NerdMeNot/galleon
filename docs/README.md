# Galleon Documentation

Welcome to the Galleon documentation. This directory contains comprehensive documentation for the Galleon DataFrame library.

## Documentation Structure

```
docs/
├── 00-philosophy.md         # Why Galleon exists
├── 01-getting-started/      # Installation and quick start
│   ├── 01-installation.md
│   └── 02-quickstart.md
├── 02-guides/               # In-depth guides
│   ├── 01-lazy.md
│   ├── 02-groupby.md
│   ├── 03-joins.md
│   ├── 04-reshape.md        # Pivot and Melt operations
│   ├── 05-window-functions.md  # Window functions and time series
│   ├── 06-data-loading.md   # Loading from structs and files
│   ├── 07-nested-types.md   # Struct, List, Array series
│   └── 08-display-configuration.md  # Display formatting options
├── 03-api/                  # API reference
│   ├── 01-dataframe.md
│   ├── 02-series.md
│   ├── 03-expressions.md
│   ├── 04-io.md
│   └── 05-configuration.md
└── 04-reference/            # Reference material
    ├── 01-types.md
    └── 02-performance.md
```

## Philosophy

- [Why Galleon Exists](00-philosophy.md) - Design philosophy and architectural decisions

## Getting Started

- [Installation Guide](01-getting-started/01-installation.md) - How to install and build Galleon
- [Quick Start](01-getting-started/02-quickstart.md) - Get up and running in 5 minutes

## API Reference

- [DataFrame API](03-api/01-dataframe.md) - DataFrame operations and methods
- [Series API](03-api/02-series.md) - Series operations and methods
- [Expressions API](03-api/03-expressions.md) - Expression building for queries
- [I/O API](03-api/04-io.md) - Reading and writing data files
- [Configuration API](03-api/05-configuration.md) - Thread and performance configuration

## Guides

- [Lazy Evaluation](02-guides/01-lazy.md) - LazyFrame and query optimization
- [GroupBy Operations](02-guides/02-groupby.md) - Comprehensive groupby guide
- [Join Operations](02-guides/03-joins.md) - Join types and best practices
- [Reshape Operations](02-guides/04-reshape.md) - Pivot and Melt for data reshaping
- [Window Functions](02-guides/05-window-functions.md) - Time series and rolling calculations
- [Data Loading](02-guides/06-data-loading.md) - Loading from structs, maps, and files
- [Nested Data Types](02-guides/07-nested-types.md) - Struct, List, and Array series
- [Display Configuration](02-guides/08-display-configuration.md) - DataFrame printing and formatting

## Reference

- [Type System](04-reference/01-types.md) - Data types and type handling
- [Performance Tips](04-reference/02-performance.md) - Optimization guidelines

## Quick Links

| Topic | Description |
|-------|-------------|
| [DataFrame](03-api/01-dataframe.md) | Core DataFrame type and operations |
| [Series](03-api/02-series.md) | Column data type with SIMD operations |
| [Expressions](03-api/03-expressions.md) | Query building with aggregations, window functions, strings |
| [Lazy Evaluation](02-guides/01-lazy.md) | Query optimization, caching, UDFs |
| [Window Functions](02-guides/05-window-functions.md) | Time series, lag/lead, rolling aggregations |
| [Reshape](02-guides/04-reshape.md) | Pivot and Melt operations |
| [Data Loading](02-guides/06-data-loading.md) | FromStructs, FromRecords, file I/O |
| [Nested Types](02-guides/07-nested-types.md) | Struct, List, Array series |
| [Display Config](02-guides/08-display-configuration.md) | Table styles, truncation, formatting |
| [I/O](03-api/04-io.md) | CSV, JSON, Parquet |
| [Performance](04-reference/02-performance.md) | Optimization tips |

## Feature Comparison

Galleon provides API parity with Polars and Pandas for common DataFrame operations:

| Feature | Galleon | Polars | Pandas | Notes |
|---------|---------|--------|--------|-------|
| **Basic Operations** |
| Filter | ✅ | ✅ | ✅ | SIMD-accelerated |
| Select | ✅ | ✅ | ✅ | Projection pushdown |
| GroupBy | ✅ | ✅ | ✅ | Swiss table hash |
| Join | ✅ | ✅ | ✅ | Inner, Left, Right |
| Sort | ✅ | ✅ | ✅ | SIMD radix sort |
| **Aggregations** |
| Sum/Min/Max/Mean | ✅ | ✅ | ✅ | SIMD vectorized |
| Median/Quantile | ✅ | ✅ | ✅ | |
| Std/Var | ✅ | ✅ | ✅ | |
| Skewness/Kurtosis | ✅ | ✅ | ✅ | 3rd/4th moments |
| Correlation | ✅ | ✅ | ✅ | Pearson correlation |
| **Window Functions** |
| Lag/Lead | ✅ | ✅ | ✅ | |
| Diff/PctChange | ✅ | ✅ | ✅ | |
| CumSum/CumMin/CumMax | ✅ | ✅ | ✅ | |
| Rolling (Sum/Mean/Std) | ✅ | ✅ | ✅ | |
| Rank/RowNumber | ✅ | ✅ | ✅ | |
| **String Operations** |
| Upper/Lower/Trim | ✅ | ✅ | ✅ | |
| Contains/StartsWith | ✅ | ✅ | ✅ | |
| Replace | ✅ | ✅ | ✅ | |
| **Reshape** |
| Pivot | ✅ | ✅ | ✅ | With aggregation |
| Melt | ✅ | ✅ | ✅ | Wide to long |
| **Advanced** |
| Lazy Evaluation | ✅ | ✅ | ❌ | Query optimization |
| Cache | ✅ | ✅ | ❌ | Materialize intermediate |
| UDF | ✅ | ✅ | ✅ | User-defined functions |
| **I/O** |
| CSV | ✅ | ✅ | ✅ | Read/Write/Scan |
| JSON | ✅ | ✅ | ✅ | |
| Parquet | ✅ | ✅ | ✅ | |
| **Performance** |
| SIMD | ✅ | ✅ | ❌ | AVX2/AVX-512 |
| Parallel | ✅ | ✅ | Partial | Multi-threaded |
| Zero-copy | ✅ | ✅ | ❌ | Go views Zig memory |

## Quick Reference

### Common Patterns

```go
// Load data from structs
type User struct {
    ID    int64  `galleon:"user_id"`
    Name  string `galleon:"name"`
    Email string `galleon:"email"`
}
users := []User{ /* ... */ }
df, _ := galleon.FromStructs(users)

// Load data from maps
records := []map[string]interface{}{
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.3},
}
df, _ := galleon.FromRecords(records)

// Time series analysis
df.Lazy().
    Sort("date", true).
    WithColumn("ma20", Col("close").RollingMean(20, 15)).
    WithColumn("return", Col("close").PctChange()).
    Collect()

// Text processing
df.Lazy().
    WithColumn("clean", Col("text").Str().Lower().Str().Trim()).
    Filter(Col("text").Str().Contains("keyword")).
    Collect()

// Pivot table
df.Lazy().
    Pivot(PivotOptions{
        Index: "row", Column: "col", Values: "val", AggFn: AggTypeSum,
    }).
    Collect()

// Cached aggregation
cached := df.Lazy().GroupBy("key").Agg(Col("val").Sum()).Cache()
result1, _ := cached.Filter(Col("sum").Gt(Lit(100))).Collect()
result2, _ := cached.Sort("sum", false).Collect()

// Custom UDF
df.Lazy().
    Apply("price", func(s *Series) (*Series, error) {
        // Custom transformation
        return transformed, nil
    }).
    Collect()
```
