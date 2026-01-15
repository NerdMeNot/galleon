#!/usr/bin/env python3
"""
Comprehensive feature benchmark: Galleon vs Polars vs Pandas
Tests identical data across all libraries for fair comparison.

Run with: python3 compare_all_features.py
"""

import time
import numpy as np
import sys

# Check for optional libraries
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    print("Warning: Polars not installed (pip install polars)")

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Warning: Pandas not installed (pip install pandas)")

# Seed for reproducibility - SAME seed used in Go benchmarks
SEED = 42

def benchmark(func, iterations=5, warmup=1):
    """Run benchmark with warmup iterations."""
    for _ in range(warmup):
        func()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return np.mean(times) * 1000  # Return mean in ms

def format_ms(ms):
    if ms is None:
        return "N/A"
    if ms < 0.001:
        return f"{ms*1000000:.0f} ns"
    elif ms < 1:
        return f"{ms*1000:.1f} µs"
    elif ms < 1000:
        return f"{ms:.2f} ms"
    else:
        return f"{ms/1000:.2f} s"

def print_header(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)

def print_row(operation, polars_ms, pandas_ms):
    p_str = format_ms(polars_ms)
    pd_str = format_ms(pandas_ms)

    # Calculate Polars vs Pandas comparison
    if polars_ms and pandas_ms:
        if polars_ms < pandas_ms:
            comparison = f"Polars {pandas_ms/polars_ms:.1f}x faster"
        else:
            comparison = f"Pandas {polars_ms/pandas_ms:.1f}x faster"
    else:
        comparison = ""

    print(f"{operation:<30} {p_str:>12} {pd_str:>12}  {comparison}")

# ============================================================================
# Generate identical test data
# ============================================================================

def generate_test_data(n, seed=SEED):
    """Generate test data with fixed seed for reproducibility."""
    np.random.seed(seed)
    return {
        'float64': np.random.randn(n) * 100,
        'float64_2': np.random.randn(n) * 100,
        'int64': np.random.randint(0, 1000000, n),
        'categories': np.random.choice(['cat_a', 'cat_b', 'cat_c', 'cat_d', 'cat_e'], n),
        'group_keys': np.random.randint(0, max(1, n // 100), n),
    }

# ============================================================================
# Statistics Benchmarks
# ============================================================================

def benchmark_statistics(n):
    print_header(f"STATISTICS BENCHMARKS - {n:,} elements")
    print(f"{'Operation':<30} {'Polars':>12} {'Pandas':>12}  {'Comparison'}")
    print("-" * 70)

    data = generate_test_data(n)

    if HAS_POLARS:
        pl_series = pl.Series(data['float64'])
        pl_series2 = pl.Series(data['float64_2'])

    if HAS_PANDAS:
        pd_series = pd.Series(data['float64'])
        pd_series2 = pd.Series(data['float64_2'])

    # Median
    polars_ms = benchmark(lambda: pl_series.median()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.median()) if HAS_PANDAS else None
    print_row("Median", polars_ms, pandas_ms)

    # Quantile (95th percentile)
    polars_ms = benchmark(lambda: pl_series.quantile(0.95)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.quantile(0.95)) if HAS_PANDAS else None
    print_row("Quantile (0.95)", polars_ms, pandas_ms)

    # Variance
    polars_ms = benchmark(lambda: pl_series.var()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.var()) if HAS_PANDAS else None
    print_row("Variance", polars_ms, pandas_ms)

    # StdDev
    polars_ms = benchmark(lambda: pl_series.std()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.std()) if HAS_PANDAS else None
    print_row("StdDev", polars_ms, pandas_ms)

    # Skewness
    polars_ms = benchmark(lambda: pl_series.skew()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.skew()) if HAS_PANDAS else None
    print_row("Skewness", polars_ms, pandas_ms)

    # Kurtosis
    polars_ms = benchmark(lambda: pl_series.kurtosis()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.kurtosis()) if HAS_PANDAS else None
    print_row("Kurtosis", polars_ms, pandas_ms)

# ============================================================================
# Window Function Benchmarks
# ============================================================================

def benchmark_window(n):
    print_header(f"WINDOW FUNCTION BENCHMARKS - {n:,} elements, window=100")
    print(f"{'Operation':<30} {'Polars':>12} {'Pandas':>12}  {'Comparison'}")
    print("-" * 70)

    data = generate_test_data(n)
    window_size = 100

    if HAS_POLARS:
        pl_series = pl.Series(data['float64'])

    if HAS_PANDAS:
        pd_series = pd.Series(data['float64'])

    # Rolling Sum
    polars_ms = benchmark(lambda: pl_series.rolling_sum(window_size)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.rolling(window_size).sum()) if HAS_PANDAS else None
    print_row("Rolling Sum", polars_ms, pandas_ms)

    # Rolling Mean
    polars_ms = benchmark(lambda: pl_series.rolling_mean(window_size)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.rolling(window_size).mean()) if HAS_PANDAS else None
    print_row("Rolling Mean", polars_ms, pandas_ms)

    # Rolling Min
    polars_ms = benchmark(lambda: pl_series.rolling_min(window_size)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.rolling(window_size).min()) if HAS_PANDAS else None
    print_row("Rolling Min", polars_ms, pandas_ms)

    # Rolling Max
    polars_ms = benchmark(lambda: pl_series.rolling_max(window_size)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.rolling(window_size).max()) if HAS_PANDAS else None
    print_row("Rolling Max", polars_ms, pandas_ms)

    # Cumulative Sum
    polars_ms = benchmark(lambda: pl_series.cum_sum()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.cumsum()) if HAS_PANDAS else None
    print_row("Cumulative Sum", polars_ms, pandas_ms)

    # Diff
    polars_ms = benchmark(lambda: pl_series.diff()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.diff()) if HAS_PANDAS else None
    print_row("Diff", polars_ms, pandas_ms)

    # Shift/Lag
    polars_ms = benchmark(lambda: pl_series.shift(5)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.shift(5)) if HAS_PANDAS else None
    print_row("Shift (lag 5)", polars_ms, pandas_ms)

    # Rank
    polars_ms = benchmark(lambda: pl_series.rank()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.rank()) if HAS_PANDAS else None
    print_row("Rank", polars_ms, pandas_ms)

# ============================================================================
# Horizontal/Fold Benchmarks
# ============================================================================

def benchmark_fold(n):
    print_header(f"HORIZONTAL/FOLD BENCHMARKS - {n:,} rows x 3 columns")
    print(f"{'Operation':<30} {'Polars':>12} {'Pandas':>12}  {'Comparison'}")
    print("-" * 70)

    np.random.seed(SEED)
    col_a = np.random.randn(n) * 100
    col_b = np.random.randn(n) * 100
    col_c = np.random.randn(n) * 100

    if HAS_POLARS:
        pl_df = pl.DataFrame({'a': col_a, 'b': col_b, 'c': col_c})

    if HAS_PANDAS:
        pd_df = pd.DataFrame({'a': col_a, 'b': col_b, 'c': col_c})

    # Sum across columns (horizontal)
    polars_ms = benchmark(lambda: pl_df.select(pl.sum_horizontal('a', 'b', 'c'))) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_df[['a', 'b', 'c']].sum(axis=1)) if HAS_PANDAS else None
    print_row("Sum Horizontal (3 cols)", polars_ms, pandas_ms)

    # Min across columns
    polars_ms = benchmark(lambda: pl_df.select(pl.min_horizontal('a', 'b', 'c'))) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_df[['a', 'b', 'c']].min(axis=1)) if HAS_PANDAS else None
    print_row("Min Horizontal (3 cols)", polars_ms, pandas_ms)

    # Max across columns
    polars_ms = benchmark(lambda: pl_df.select(pl.max_horizontal('a', 'b', 'c'))) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_df[['a', 'b', 'c']].max(axis=1)) if HAS_PANDAS else None
    print_row("Max Horizontal (3 cols)", polars_ms, pandas_ms)

# ============================================================================
# Categorical Benchmarks
# ============================================================================

def benchmark_categorical(n):
    print_header(f"CATEGORICAL BENCHMARKS - {n:,} rows")
    print(f"{'Operation':<30} {'Polars':>12} {'Pandas':>12}  {'Comparison'}")
    print("-" * 70)

    data = generate_test_data(n)

    if HAS_POLARS:
        pl_df = pl.DataFrame({
            'category': pl.Series(data['categories']).cast(pl.Categorical),
            'value': data['float64']
        })

    if HAS_PANDAS:
        pd_df = pd.DataFrame({
            'category': pd.Categorical(data['categories']),
            'value': data['float64']
        })

    # GroupBy with categorical key
    polars_ms = benchmark(lambda: pl_df.group_by('category').agg(pl.col('value').sum())) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_df.groupby('category', observed=True)['value'].sum()) if HAS_PANDAS else None
    print_row("GroupBy (categorical)", polars_ms, pandas_ms)

# ============================================================================
# Core Operations (for reference)
# ============================================================================

def benchmark_core(n):
    print_header(f"CORE OPERATIONS - {n:,} elements")
    print(f"{'Operation':<30} {'Polars':>12} {'Pandas':>12}  {'Comparison'}")
    print("-" * 70)

    data = generate_test_data(n)

    if HAS_POLARS:
        pl_series = pl.Series(data['float64'])

    if HAS_PANDAS:
        pd_series = pd.Series(data['float64'])

    # Sum
    polars_ms = benchmark(lambda: pl_series.sum()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.sum()) if HAS_PANDAS else None
    print_row("Sum", polars_ms, pandas_ms)

    # Min
    polars_ms = benchmark(lambda: pl_series.min()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.min()) if HAS_PANDAS else None
    print_row("Min", polars_ms, pandas_ms)

    # Max
    polars_ms = benchmark(lambda: pl_series.max()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.max()) if HAS_PANDAS else None
    print_row("Max", polars_ms, pandas_ms)

    # Mean
    polars_ms = benchmark(lambda: pl_series.mean()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.mean()) if HAS_PANDAS else None
    print_row("Mean", polars_ms, pandas_ms)

    # Sort
    polars_ms = benchmark(lambda: pl_series.sort()) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series.sort_values()) if HAS_PANDAS else None
    print_row("Sort", polars_ms, pandas_ms)

    # Filter (> threshold)
    threshold = 0.0
    polars_ms = benchmark(lambda: pl_series.filter(pl_series > threshold)) if HAS_POLARS else None
    pandas_ms = benchmark(lambda: pd_series[pd_series > threshold]) if HAS_PANDAS else None
    print_row("Filter (> 0)", polars_ms, pandas_ms)

# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("=" * 70)
    print("PYTHON BENCHMARK: Polars vs Pandas")
    print("=" * 70)
    print()
    print(f"Using seed {SEED} for reproducible data (same as Go benchmarks)")
    print()

    # Test size - 1M elements (matches Go benchmarks)
    n = 1_000_000

    benchmark_core(n)
    benchmark_statistics(n)
    benchmark_window(n)
    benchmark_fold(n)
    benchmark_categorical(n)

    print()
    print("=" * 70)
    print("PYTHON BENCHMARK COMPLETE")
    print("=" * 70)

if __name__ == '__main__':
    main()
