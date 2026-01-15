#!/usr/bin/env python3
"""
Resource consumption benchmarks: Polars vs Pandas
Measures memory usage, allocations, and throughput.

Run with: python3 compare_resources.py
"""

import time
import tracemalloc
import gc
import numpy as np
import sys

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    print("Warning: Polars not installed")

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Warning: Pandas not installed")

SEED = 42

def measure_memory_and_time(func, iterations=3):
    """Measure peak memory usage and execution time."""
    gc.collect()

    # Warmup
    func()
    gc.collect()

    # Measure time
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = np.mean(times) * 1000  # ms

    # Measure memory
    gc.collect()
    tracemalloc.start()
    func()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return avg_time, peak / 1024 / 1024  # time in ms, memory in MB

def format_result(time_ms, memory_mb):
    if time_ms < 1:
        time_str = f"{time_ms*1000:.1f} µs"
    elif time_ms < 1000:
        time_str = f"{time_ms:.2f} ms"
    else:
        time_str = f"{time_ms/1000:.2f} s"

    if memory_mb < 1:
        mem_str = f"{memory_mb*1024:.1f} KB"
    else:
        mem_str = f"{memory_mb:.1f} MB"

    return time_str, mem_str

def print_header(title):
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    print(f"{'Operation':<30} {'Polars Time':>12} {'Polars Mem':>12} {'Pandas Time':>12} {'Pandas Mem':>12}")
    print("-" * 80)

def print_row(operation, polars_result, pandas_result):
    if polars_result:
        p_time, p_mem = format_result(*polars_result)
    else:
        p_time, p_mem = "N/A", "N/A"

    if pandas_result:
        pd_time, pd_mem = format_result(*pandas_result)
    else:
        pd_time, pd_mem = "N/A", "N/A"

    print(f"{operation:<30} {p_time:>12} {p_mem:>12} {pd_time:>12} {pd_mem:>12}")

# ============================================================================
# Resource Benchmarks
# ============================================================================

def benchmark_aggregation_resources(n):
    print_header(f"AGGREGATION RESOURCES - {n:,} elements")

    np.random.seed(SEED)
    data = np.random.randn(n) * 100

    if HAS_POLARS:
        pl_series = pl.Series(data)
    if HAS_PANDAS:
        pd_series = pd.Series(data)

    # Sum
    polars_result = measure_memory_and_time(lambda: pl_series.sum()) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.sum()) if HAS_PANDAS else None
    print_row("Sum", polars_result, pandas_result)

    # Variance
    polars_result = measure_memory_and_time(lambda: pl_series.var()) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.var()) if HAS_PANDAS else None
    print_row("Variance", polars_result, pandas_result)

    # Median
    polars_result = measure_memory_and_time(lambda: pl_series.median()) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.median()) if HAS_PANDAS else None
    print_row("Median", polars_result, pandas_result)

    # Quantile
    polars_result = measure_memory_and_time(lambda: pl_series.quantile(0.95)) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.quantile(0.95)) if HAS_PANDAS else None
    print_row("Quantile (0.95)", polars_result, pandas_result)

def benchmark_window_resources(n):
    print_header(f"WINDOW FUNCTION RESOURCES - {n:,} elements, window=100")

    np.random.seed(SEED)
    data = np.random.randn(n) * 100
    window = 100

    if HAS_POLARS:
        pl_series = pl.Series(data)
    if HAS_PANDAS:
        pd_series = pd.Series(data)

    # Rolling Sum
    polars_result = measure_memory_and_time(lambda: pl_series.rolling_sum(window)) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.rolling(window).sum()) if HAS_PANDAS else None
    print_row("Rolling Sum", polars_result, pandas_result)

    # Rolling Mean
    polars_result = measure_memory_and_time(lambda: pl_series.rolling_mean(window)) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.rolling(window).mean()) if HAS_PANDAS else None
    print_row("Rolling Mean", polars_result, pandas_result)

    # Rolling Min
    polars_result = measure_memory_and_time(lambda: pl_series.rolling_min(window)) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.rolling(window).min()) if HAS_PANDAS else None
    print_row("Rolling Min", polars_result, pandas_result)

    # Cumulative Sum
    polars_result = measure_memory_and_time(lambda: pl_series.cum_sum()) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.cumsum()) if HAS_PANDAS else None
    print_row("Cumulative Sum", polars_result, pandas_result)

    # Diff
    polars_result = measure_memory_and_time(lambda: pl_series.diff()) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_series.diff()) if HAS_PANDAS else None
    print_row("Diff", polars_result, pandas_result)

def benchmark_horizontal_resources(n):
    print_header(f"HORIZONTAL/FOLD RESOURCES - {n:,} rows x 3 columns")

    np.random.seed(SEED)
    col_a = np.random.randn(n) * 100
    col_b = np.random.randn(n) * 100
    col_c = np.random.randn(n) * 100

    if HAS_POLARS:
        pl_df = pl.DataFrame({'a': col_a, 'b': col_b, 'c': col_c})
    if HAS_PANDAS:
        pd_df = pd.DataFrame({'a': col_a, 'b': col_b, 'c': col_c})

    # Sum Horizontal
    polars_result = measure_memory_and_time(lambda: pl_df.select(pl.sum_horizontal('a', 'b', 'c'))) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_df[['a', 'b', 'c']].sum(axis=1)) if HAS_PANDAS else None
    print_row("Sum Horizontal (3 cols)", polars_result, pandas_result)

    # Min Horizontal
    polars_result = measure_memory_and_time(lambda: pl_df.select(pl.min_horizontal('a', 'b', 'c'))) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(lambda: pd_df[['a', 'b', 'c']].min(axis=1)) if HAS_PANDAS else None
    print_row("Min Horizontal (3 cols)", polars_result, pandas_result)

def benchmark_categorical_resources(n):
    print_header(f"CATEGORICAL RESOURCES - {n:,} rows")

    np.random.seed(SEED)
    categories = np.random.choice(['cat_a', 'cat_b', 'cat_c', 'cat_d', 'cat_e'], n)
    values = np.random.randn(n) * 100

    # Creation
    def create_polars():
        return pl.DataFrame({
            'category': pl.Series(categories).cast(pl.Categorical),
            'value': values
        })

    def create_pandas():
        return pd.DataFrame({
            'category': pd.Categorical(categories),
            'value': values
        })

    polars_result = measure_memory_and_time(create_polars) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(create_pandas) if HAS_PANDAS else None
    print_row("Create Categorical DF", polars_result, pandas_result)

    # GroupBy
    if HAS_POLARS:
        pl_df = create_polars()
    if HAS_PANDAS:
        pd_df = create_pandas()

    polars_result = measure_memory_and_time(
        lambda: pl_df.group_by('category').agg(pl.col('value').sum())
    ) if HAS_POLARS else None
    pandas_result = measure_memory_and_time(
        lambda: pd_df.groupby('category', observed=True)['value'].sum()
    ) if HAS_PANDAS else None
    print_row("GroupBy (categorical)", polars_result, pandas_result)

def benchmark_memory_efficiency():
    print_header("MEMORY EFFICIENCY - Categorical vs String (1M rows)")

    np.random.seed(SEED)
    n = 1_000_000
    categories = np.random.choice(['category_a', 'category_b', 'category_c', 'category_d', 'category_e'], n)

    # Polars Categorical
    if HAS_POLARS:
        gc.collect()
        tracemalloc.start()
        pl_cat = pl.Series(categories).cast(pl.Categorical)
        _, peak_cat = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        gc.collect()
        tracemalloc.start()
        pl_str = pl.Series(categories)
        _, peak_str = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"Polars Categorical:    {peak_cat/1024/1024:.1f} MB")
        print(f"Polars String:         {peak_str/1024/1024:.1f} MB")
        print(f"Memory savings:        {(1 - peak_cat/peak_str)*100:.1f}%")

    print()

    # Pandas Categorical
    if HAS_PANDAS:
        gc.collect()
        tracemalloc.start()
        pd_cat = pd.Categorical(categories)
        _, peak_cat = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        gc.collect()
        tracemalloc.start()
        pd_str = pd.Series(categories)
        _, peak_str = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"Pandas Categorical:    {peak_cat/1024/1024:.1f} MB")
        print(f"Pandas String:         {peak_str/1024/1024:.1f} MB")
        print(f"Memory savings:        {(1 - peak_cat/peak_str)*100:.1f}%")

def benchmark_throughput():
    print_header("THROUGHPUT (GB/s) - 1M float64 elements")

    np.random.seed(SEED)
    n = 1_000_000
    data_size_gb = n * 8 / 1e9  # 8 bytes per float64
    data = np.random.randn(n) * 100

    if HAS_POLARS:
        pl_series = pl.Series(data)
    if HAS_PANDAS:
        pd_series = pd.Series(data)

    operations = [
        ("Sum", lambda: pl_series.sum(), lambda: pd_series.sum()),
        ("Mean", lambda: pl_series.mean(), lambda: pd_series.mean()),
        ("Min", lambda: pl_series.min(), lambda: pd_series.min()),
        ("Max", lambda: pl_series.max(), lambda: pd_series.max()),
    ]

    print(f"{'Operation':<30} {'Polars GB/s':>15} {'Pandas GB/s':>15}")
    print("-" * 60)

    for name, polars_fn, pandas_fn in operations:
        # Polars throughput
        if HAS_POLARS:
            times = []
            for _ in range(10):
                start = time.perf_counter()
                polars_fn()
                times.append(time.perf_counter() - start)
            polars_gbps = data_size_gb / np.mean(times)
        else:
            polars_gbps = 0

        # Pandas throughput
        if HAS_PANDAS:
            times = []
            for _ in range(10):
                start = time.perf_counter()
                pandas_fn()
                times.append(time.perf_counter() - start)
            pandas_gbps = data_size_gb / np.mean(times)
        else:
            pandas_gbps = 0

        print(f"{name:<30} {polars_gbps:>14.1f}  {pandas_gbps:>14.1f}")

# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("=" * 80)
    print("RESOURCE CONSUMPTION BENCHMARK: Polars vs Pandas")
    print("=" * 80)
    print()
    print(f"Using seed {SEED} for reproducible data")
    print()

    n = 1_000_000

    benchmark_aggregation_resources(n)
    benchmark_window_resources(n)
    benchmark_horizontal_resources(n)
    benchmark_categorical_resources(n)
    benchmark_memory_efficiency()
    benchmark_throughput()

    print()
    print("=" * 80)
    print("RESOURCE BENCHMARK COMPLETE")
    print("=" * 80)

if __name__ == '__main__':
    main()
