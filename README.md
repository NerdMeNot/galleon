# Galleon

A high-performance DataFrame library for Go, powered by Zig SIMD operations.

[![Go Reference](https://pkg.go.dev/badge/github.com/NerdMeNot/galleon/go.svg)](https://pkg.go.dev/github.com/NerdMeNot/galleon/go)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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
    // Create a DataFrame
    df, _ := galleon.NewDataFrame(
        galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
        galleon.NewSeriesFloat64("value", []float64{10.5, 20.3, 15.7, 8.2, 25.1}),
        galleon.NewSeriesString("category", []string{"A", "B", "A", "B", "A"}),
    )

    // Filter and aggregate
    result := df.
        Filter(galleon.Col("value").Gt(galleon.Lit(10.0))).
        GroupBy("category").
        Agg(galleon.Col("value").Sum().Alias("total"))

    fmt.Println(result)
}
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
- `Sum`, `Min`, `Max`, `Mean`, `Count`, `Std`, `Var`

### Element-wise
- `Add`, `Sub`, `Mul`, `Div` (scalar and vector)
- `Gt`, `Lt`, `Eq`, `Gte`, `Lte` (comparisons)

### DataFrame
- `Select`, `Drop`, `Filter`, `Sort`
- `GroupBy`, `Agg`
- `Join`, `LeftJoin`, `RightJoin`
- `WithColumn`, `Rename`
- `Head`, `Tail`, `Slice`

### I/O
- `ReadCSV`, `WriteCSV`
- `ReadJSON`, `WriteJSON`
- `ReadParquet`

### Lazy Evaluation
- `ScanCSV`, `ScanParquet`
- Query optimization
- Deferred execution with `Collect()`

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by [Polars](https://pola.rs/) DataFrame library
- SIMD techniques from database research literature
- Go community for CGO best practices
