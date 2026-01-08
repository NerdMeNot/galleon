# Changelog

All notable changes to Galleon will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of Galleon DataFrame library
- Core DataFrame and Series types with SIMD-accelerated operations
- Zig backend with vectorized aggregations (Sum, Min, Max, Mean)
- Parallel inner join with fastIntHash and interleaved 4-key probing
- Parallel left join implementation
- GroupBy operations with hash-based grouping
- End-to-end join and groupby operations (single CGO call)
- Lazy evaluation with LazyFrame API
- Query optimization (projection pushdown, filter pushdown)
- Expression system for building queries
- I/O support for CSV, JSON, and Parquet formats
- Configurable thread count with auto-detection
- Zero-copy data access between Go and Zig
- Memory pooling for frequently allocated objects
- Comprehensive test suite and benchmarks

### Performance Characteristics
- Inner Join (1M x 500K rows): ~60ms with 11 threads
- Left Join (1M x 500K rows): ~60ms with 11 threads
- Min/Max aggregations: 2-3x faster than native Go
- Filtering operations: 10-15x faster than native Go

### Architecture
- Two-layer design: Go API + Zig SIMD backend
- CGO interface for cross-language calls
- Cache-aligned (64-byte) column storage
- Null bitmap support for nullable values
- Work-stealing morsel-based parallelism

## [0.1.0] - 2026-01-08

### Added
- Project structure reorganization
  - Go code moved to `go/` directory
  - Zig code in `core/` directory
  - Root-level documentation files
- Technical whitepaper documenting architecture
- README with quick start guide
- Contributing guidelines
- MIT License

### Changed
- Module path updated to `github.com/NerdMeNot/galleon/go`
- CGO paths updated for new directory structure

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.1.0 | 2026-01-08 | Initial release with core functionality |

## Migration Guide

### From Pre-0.1.0 to 0.1.0

If you were using Galleon before the directory reorganization:

1. Update your import path:
   ```go
   // Old
   import "github.com/NerdMeNot/galleon"

   // New
   import "github.com/NerdMeNot/galleon/go"
   ```

2. Run `go mod tidy` to update dependencies

3. The API remains unchanged - only the import path changed
