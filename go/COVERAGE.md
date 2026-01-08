# Test Coverage Report

## Summary

| Component | Current Coverage | Target | Status |
|-----------|-----------------|--------|--------|
| **Go** | 76.9% | >90% | 🟡 In progress |
| **Zig** | ~80% (142 tests, ~130/155 exports covered) | >90% | 🟡 In progress |

## Recent Improvements

### Zig Coverage (improved from 57% to ~80%)
- Added 59 new tests (83 -> 142 total tests)
- Added f32/i32 type variants for aggregations
- Added comprehensive filter/mask SIMD tests
- Added join edge cases (no matches, all match, negative keys, etc.)
- Added arithmetic tests for all type variants
- Added SwissJoinTable tests

### Go Coverage (improved from 54.8% to 76.9%)
- Extended series_test.go with all type variants (Float32, Int32, Bool, String)
- Added comprehensive groupby_test.go for edge cases and all data types
- Added join_test.go for Float32/Int32/Bool keys and error paths
- Added io_test.go for CSV/JSON options and error handling
- Added expr_test.go for Cast, Alias, Clone, and AllCols expressions
- Added dataframe_test.go for Schema and all-types filtering
- Added series.go Add/Mul/Head/Tail tests for Float32, Int32 types
- Added series.go String() method tests for large arrays
- Added expr.go BinaryOp.String() and AggType.String() full coverage
- Added galleon.go column arithmetic tests (AddF64, SubF64, etc.)
- Added lazy_executor_test.go tests for toFloat64Slice, castSeries, evaluateVectorOp
- Added lazy_executor_test.go tests for evaluateComparison (all types + error cases)
- Added lazy_executor_test.go tests for evaluateExpr (Alias, Cast, AllCols error)
- Added lazy_executor_test.go tests for executeJoin (RightJoin, OuterJoin, UnsupportedType)
- Added io_test.go tests for appendNullValue (all 6 types)
- Fixed duplicate CGO LDFLAGS causing linker warnings

## Go Coverage by File

### Well-Covered Files (>80%)
| File | Coverage | Notes |
|------|----------|-------|
| `dtype.go` | 100% | All DType methods and Schema operations |
| `lazyframe.go` | 99.4% | LazyFrame operations |
| `parallel.go` | 98.3% | ParallelConfig, MorselIterator, parallel ops |
| `pool.go` | 93.6% | Memory pooling |
| `dataframe.go` | 93.0% | Schema, filtering, all types covered |
| `groupby.go` | 89.4% | Multi-key, all aggregations |
| `expr.go` | 88.7% | All expression types, String methods |
| `io_csv.go` | 86.9% | Options, error paths |
| `lazy_optimizer.go` | 84.9% | Optimization rules |
| `series.go` | 81.6% | Type-specific methods, Add/Mul/Head/Tail |

### Good Coverage (60-80%)
| File | Coverage | Notes |
|------|----------|-------|
| `lazy_executor.go` | ~80% | Expression evaluation, join execution, comparisons |
| `join.go` | 76.8% | All join types, type variants |
| `io_parquet.go` | ~75% | Schema, compression, appendNullValue |
| `io_json.go` | 75.8% | Format options |
| `galleon.go` | 69.6% | Core CGO wrappers (finalizers at 0%) |

### Needs More Work
| File | Coverage | Remaining |
|------|----------|-----------|
| `readParquetParallel` | 0% | Requires multi-row-group files |
| Some `join.go` internals | 0% | Parallel join variants |

## Remaining 0% Coverage Functions

### Hard to Test (Finalizers, Internal Methods)
- `free()` methods - Runtime finalizers (memory management)
- `exprType()` - Internal unexported methods on expression types

### Note
Most 0% coverage functions are either:
1. **Finalizers** - Called by Go runtime, not directly testable
2. **Internal methods** - Used for type dispatch, tested indirectly


### I/O files
- Many error handling paths
- Less common options

## Zig Test Coverage

### Tested Modules (142 tests total)
| Module | Tests | Notes |
|--------|-------|-------|
| `simd/comparisons.zig` | 13 | All comparison operators, SIMD paths |
| `simd/arithmetic.zig` | 27 | f64, f32, i64, i32 variants, SIMD paths |
| `simd/sorting.zig` | 8 | Sorting algorithms |
| `simd/aggregations.zig` | 24 | sum, min, max, mean, variance, stdDev |
| `simd/filters.zig` | 22 | Filter masks, SIMD paths, edge cases |
| `simd/hashing.zig` | 10 | Hash functions, determinism tests |
| `simd/gather.zig` | 7 | Gather operations, invalid indices |
| `simd/groupby_agg.zig` | 8 | GroupBy aggregations |
| `simd/joins.zig` | 23 | Inner/left join, SwissTable, edge cases |
| `groupby.zig` | 3 | GroupBy hash table |
| `main.zig` | 3 | Column operations |
| `simd/core.zig` | 2 | Thread config |

### Remaining Untested Areas (~25 exports)
- Some parallel join variants
- Extended parallel aggregation functions
- Less common edge cases

## Priority Improvements

### Phase 1: Go Core (Target: 75%) - DONE
1. ~~**series.go** - Add tests for all type variants~~ - Completed
2. ~~**groupby.go** - Test multi-key groupby, edge cases~~ - Completed
3. ~~**join.go** - Test all join types, null handling~~ - Completed

### Phase 2: Go I/O (Target: 70%) - DONE
1. ~~**io_csv.go** - Delimiter options, encoding, error paths~~ - Completed
2. ~~**io_json.go** - Array vs object format~~ - Completed
3. **io_parquet.go** - Compression, schema (partial)

### Phase 3: Zig Core (Target: 85%) - DONE
1. ~~Hash functions - Determinism, collision tests~~ - Completed
2. ~~Gather functions - Index bounds, null handling~~ - Completed
3. ~~Join edge cases - All join types, empty arrays~~ - Completed
4. ~~Arithmetic/Aggregation - All type variants~~ - Completed

### Phase 4: Final Push (Target: 90%)
1. Parallel join variants (Zig)
2. Go lazy_optimizer.go - Optimization rules
3. Go io_parquet.go - Schema evolution
4. Edge case tests - Empty data, NaN propagation

## Running Coverage

```bash
# Go coverage report
cd go && go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
go tool cover -html=coverage.out -o coverage.html

# Zig tests
cd core && zig build test
```

## Test File Structure

```
go/
├── galleon_test.go        # CGO wrapper tests
├── series_test.go         # Series method tests
├── dataframe_test.go      # DataFrame tests
├── expr_test.go           # Expression system tests
├── lazyframe_test.go      # LazyFrame tests
├── lazy_executor_test.go  # Plan execution tests
├── parallel_test.go       # Parallel execution tests
├── dtype_test.go          # Type system tests
├── groupby_test.go        # GroupBy tests
├── join_test.go           # Join tests
├── io_test.go             # CSV/JSON I/O tests
├── correctness_test.go    # Data correctness validation
└── cgo_validation_test.go # CGO header checks

core/src/
├── main.zig               # Export tests (3 tests)
├── groupby.zig            # GroupBy tests (3 tests)
├── simd/
│   ├── mod.zig            # Module test runner
│   ├── core.zig           # Thread config tests (2 tests)
│   ├── comparisons.zig    # Comparison tests (13 tests)
│   ├── arithmetic.zig     # Arithmetic tests (27 tests)
│   ├── sorting.zig        # Sorting tests (8 tests)
│   ├── aggregations.zig   # Aggregation tests (24 tests)
│   ├── filters.zig        # Filter tests (22 tests)
│   ├── hashing.zig        # Hash function tests (10 tests)
│   ├── gather.zig         # Gather tests (7 tests)
│   ├── groupby_agg.zig    # GroupBy aggregation tests (8 tests)
│   └── joins.zig          # Join tests (23 tests)
```

---
*Last updated: 2026-01-08*
*Coverage measured with: go test -cover, zig build test*
