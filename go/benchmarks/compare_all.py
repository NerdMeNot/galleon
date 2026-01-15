#!/usr/bin/env python3
"""
Comprehensive benchmark: Galleon vs Polars vs Pandas
Run this after running the Go benchmarks to compare.
"""

import time
import subprocess
import sys

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

import numpy as np

def benchmark(func, iterations=10, warmup=2):
    """Run benchmark with warmup iterations."""
    for _ in range(warmup):
        func()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        'mean_ms': np.mean(times) * 1000,
        'min_ms': np.min(times) * 1000,
        'std_ms': np.std(times) * 1000,
        'throughput_gb_s': None,  # Will be calculated per operation
    }

def format_result(result, size, bytes_per_elem=8):
    """Format benchmark result with throughput."""
    throughput = (size * bytes_per_elem) / (result['mean_ms'] / 1000) / 1e9
    return f"{result['mean_ms']:8.3f} ms | {throughput:6.1f} GB/s"

def run_polars_benchmarks(sizes):
    """Run Polars benchmarks."""
    if not HAS_POLARS:
        return {}

    results = {}

    for n in sizes:
        np.random.seed(42)
        data = np.random.randn(n)
        df = pl.DataFrame({'value': data})
        series = df['value']

        # Sum
        r = benchmark(lambda: series.sum())
        results[f'sum_{n}'] = r

        # Min
        r = benchmark(lambda: series.min())
        results[f'min_{n}'] = r

        # Max
        r = benchmark(lambda: series.max())
        results[f'max_{n}'] = r

        # Mean
        r = benchmark(lambda: series.mean())
        results[f'mean_{n}'] = r

        # Vector operations (need two series)
        data2 = np.random.randn(n)
        s1 = pl.Series(data)
        s2 = pl.Series(data2)

        # Add
        r = benchmark(lambda: s1 + s2)
        results[f'add_{n}'] = r

        # Multiply
        r = benchmark(lambda: s1 * s2)
        results[f'mul_{n}'] = r

        # Divide
        r = benchmark(lambda: s1 / s2)
        results[f'div_{n}'] = r

    return results

def run_pandas_benchmarks(sizes):
    """Run Pandas benchmarks."""
    if not HAS_PANDAS:
        return {}

    results = {}

    for n in sizes:
        np.random.seed(42)
        data = np.random.randn(n)
        series = pd.Series(data)

        # Sum
        r = benchmark(lambda: series.sum())
        results[f'sum_{n}'] = r

        # Min
        r = benchmark(lambda: series.min())
        results[f'min_{n}'] = r

        # Max
        r = benchmark(lambda: series.max())
        results[f'max_{n}'] = r

        # Mean
        r = benchmark(lambda: series.mean())
        results[f'mean_{n}'] = r

        # Vector operations
        data2 = np.random.randn(n)
        s1 = pd.Series(data)
        s2 = pd.Series(data2)

        # Add
        r = benchmark(lambda: s1 + s2)
        results[f'add_{n}'] = r

        # Multiply
        r = benchmark(lambda: s1 * s2)
        results[f'mul_{n}'] = r

        # Divide
        r = benchmark(lambda: s1 / s2)
        results[f'div_{n}'] = r

    return results

def run_numpy_benchmarks(sizes):
    """Run NumPy benchmarks (baseline)."""
    results = {}

    for n in sizes:
        np.random.seed(42)
        data = np.random.randn(n)

        # Sum
        r = benchmark(lambda: np.sum(data))
        results[f'sum_{n}'] = r

        # Min
        r = benchmark(lambda: np.min(data))
        results[f'min_{n}'] = r

        # Max
        r = benchmark(lambda: np.max(data))
        results[f'max_{n}'] = r

        # Mean
        r = benchmark(lambda: np.mean(data))
        results[f'mean_{n}'] = r

        # Vector operations
        data2 = np.random.randn(n)

        # Add
        r = benchmark(lambda: data + data2)
        results[f'add_{n}'] = r

        # Multiply
        r = benchmark(lambda: data * data2)
        results[f'mul_{n}'] = r

        # Divide
        r = benchmark(lambda: data / data2)
        results[f'div_{n}'] = r

    return results

def main():
    print("=" * 80)
    print("COMPREHENSIVE BENCHMARK: Polars vs Pandas vs NumPy")
    print("=" * 80)
    print()
    print("Note: Run Go benchmarks separately with:")
    print("  cd /path/to/galleon/go")
    print("  go test -tags dev -bench 'BenchmarkSimd' -benchmem")
    print()

    sizes = [100_000, 1_000_000, 10_000_000]

    print("Running benchmarks...")
    print()

    numpy_results = run_numpy_benchmarks(sizes)
    polars_results = run_polars_benchmarks(sizes) if HAS_POLARS else {}
    pandas_results = run_pandas_benchmarks(sizes) if HAS_PANDAS else {}

    # Print results in a table
    operations = ['sum', 'min', 'max', 'mean', 'add', 'mul', 'div']

    for n in sizes:
        print()
        print("=" * 80)
        print(f"Size: {n:,} elements (float64)")
        print("=" * 80)
        print()
        print(f"{'Operation':<12} {'NumPy':>24} {'Polars':>24} {'Pandas':>24}")
        print("-" * 80)

        for op in operations:
            key = f'{op}_{n}'

            numpy_str = format_result(numpy_results[key], n) if key in numpy_results else "N/A"
            polars_str = format_result(polars_results[key], n) if key in polars_results else "N/A"
            pandas_str = format_result(pandas_results[key], n) if key in pandas_results else "N/A"

            print(f"{op:<12} {numpy_str:>24} {polars_str:>24} {pandas_str:>24}")

    print()
    print("=" * 80)
    print("NOTES:")
    print("- NumPy uses optimized C/Fortran with SIMD (via BLAS/LAPACK)")
    print("- Polars uses Rust with explicit SIMD and multi-threading")
    print("- Pandas uses NumPy under the hood")
    print("- Galleon uses Go + Zig with SIMD (run Go benchmarks separately)")
    print("- Throughput = (elements * 8 bytes) / time")
    print("=" * 80)

if __name__ == '__main__':
    main()
