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
│   └── 03-joins.md
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

## Reference

- [Type System](04-reference/01-types.md) - Data types and type handling
- [Performance Tips](04-reference/02-performance.md) - Optimization guidelines

## Quick Links

| Topic | Description |
|-------|-------------|
| [DataFrame](03-api/01-dataframe.md) | Core DataFrame type and operations |
| [Series](03-api/02-series.md) | Column data type |
| [Expressions](03-api/03-expressions.md) | Query building |
| [I/O](03-api/04-io.md) | CSV, JSON, Parquet |
| [Performance](04-reference/02-performance.md) | Optimization tips |
