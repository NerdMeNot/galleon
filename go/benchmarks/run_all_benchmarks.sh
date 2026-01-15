#!/bin/bash
# Run all benchmarks: Galleon (Go), Polars (Python), Pandas (Python)
# Uses identical data with seed 42 for fair comparison

set -e

echo "================================================================================"
echo "GALLEON vs POLARS vs PANDAS - COMPREHENSIVE BENCHMARK"
echo "================================================================================"
echo ""
echo "Environment:"
echo "  - Go: $(go version)"
echo "  - Zig: $(zig version)"
echo "  - Python: $(python3 --version)"
echo "  - Polars: $(python3 -c 'import polars; print(polars.__version__)')"
echo "  - Pandas: $(python3 -c 'import pandas; print(pandas.__version__)')"
echo ""
echo "All tests use seed=42 for identical random data"
echo ""

cd /galleon/go

# ============================================================================
# Run Go benchmarks and capture results
# ============================================================================
echo "================================================================================"
echo "RUNNING GO BENCHMARKS (Galleon)"
echo "================================================================================"

# Statistics benchmarks
echo ""
echo "--- Statistics Benchmarks ---"
go test -tags dev -bench='BenchmarkStats_' -benchtime=500ms -timeout=5m 2>/dev/null | grep -E '^Benchmark|^ok'

# Window benchmarks
echo ""
echo "--- Window Function Benchmarks ---"
go test -tags dev -bench='BenchmarkWindow_' -benchtime=500ms -timeout=5m 2>/dev/null | grep -E '^Benchmark|^ok'

# Fold/Horizontal benchmarks
echo ""
echo "--- Fold/Horizontal Benchmarks ---"
go test -tags dev -bench='BenchmarkFold_' -benchtime=500ms -timeout=5m 2>/dev/null | grep -E '^Benchmark|^ok'

# Categorical benchmarks
echo ""
echo "--- Categorical Benchmarks ---"
go test -tags dev -bench='BenchmarkCategorical_' -benchtime=500ms -timeout=5m 2>/dev/null | grep -E '^Benchmark|^ok'

# Core operations for reference
echo ""
echo "--- Core Operations (Reference) ---"
go test -tags dev -bench='BenchmarkZigSum_1M|BenchmarkZigMin_1M|BenchmarkSort_1M' -benchtime=500ms -timeout=5m 2>/dev/null | grep -E '^Benchmark|^ok'

# Sort and Join benchmarks
echo ""
echo "--- Sort and Join Benchmarks ---"
go test -tags dev -bench='BenchmarkSortJoin_' -benchtime=500ms -timeout=10m 2>/dev/null | grep -E '^Benchmark|^ok'

# Resource consumption benchmarks
echo ""
echo "--- Resource Consumption Benchmarks (Memory & Throughput) ---"
go test -tags dev -bench='BenchmarkResource_' -benchmem -benchtime=500ms -timeout=5m 2>/dev/null | grep -E '^Benchmark|^ok'

# ============================================================================
# Run Python benchmarks
# ============================================================================
echo ""
echo "================================================================================"
echo "RUNNING PYTHON BENCHMARKS (Polars & Pandas)"
echo "================================================================================"

python3 /galleon/go/benchmarks/compare_all_features.py

echo ""
echo "================================================================================"
echo "RUNNING PYTHON RESOURCE BENCHMARKS (Memory & Throughput)"
echo "================================================================================"

python3 /galleon/go/benchmarks/compare_resources.py

echo ""
echo "================================================================================"
echo "BENCHMARK COMPLETE"
echo "================================================================================"
