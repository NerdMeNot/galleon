#!/usr/bin/env python3
"""
Galleon vs Polars vs Pandas - Unified Benchmark Comparison

This script runs comprehensive benchmarks across all three libraries
and outputs a clean comparison table.

Usage: python3 benchmark_comparison.py [--sizes 100000,1000000]
"""

import subprocess
import json
import time
import sys
import os
import argparse
from typing import Dict, List, Optional, Tuple

import numpy as np

# Try importing Polars and Pandas
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

# ============================================================================
# Configuration
# ============================================================================

SEED = 42
DEFAULT_SIZES = [100_000, 1_000_000]
ITERATIONS = 5
WARMUP = 2
WINDOW_SIZE = 100

# ============================================================================
# Utility Functions
# ============================================================================

def benchmark(func, iterations=ITERATIONS, warmup=WARMUP):
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

def format_time(ms: Optional[float]) -> str:
    """Format time with appropriate units."""
    if ms is None:
        return "N/A"
    if ms < 0.001:
        return f"{ms*1000:.1f} us"
    elif ms < 1:
        return f"{ms*1000:.0f} us"
    elif ms < 1000:
        return f"{ms:.2f} ms"
    else:
        return f"{ms/1000:.2f} s"

def format_speedup(galleon_ms: Optional[float], other_ms: Optional[float]) -> str:
    """Calculate and format speedup."""
    if galleon_ms is None or other_ms is None:
        return ""
    if other_ms == 0:
        return ""
    ratio = galleon_ms / other_ms
    if ratio > 1:
        return f"{ratio:.1f}x slower"
    else:
        return f"{1/ratio:.1f}x faster"

def generate_data(n: int, seed: int = SEED) -> Dict:
    """Generate test data matching Go benchmarks exactly."""
    np.random.seed(seed)
    return {
        'float64': np.random.randn(n) * 100,
        'float64_2': np.random.randn(n) * 100,
        'int64': np.random.randint(0, 1000000, n, dtype=np.int64),
        'group_keys': np.random.randint(0, max(10, n // 100), n, dtype=np.int64),
    }

# ============================================================================
# Polars Benchmarks
# ============================================================================

def benchmark_polars(sizes: List[int]) -> Dict[Tuple[str, int], float]:
    """Run all Polars benchmarks."""
    if not HAS_POLARS:
        return {}

    results = {}

    for size in sizes:
        data = generate_data(size)
        s = pl.Series(data['float64'])
        s2 = pl.Series(data['float64_2'])

        # Core Aggregations
        results[('Sum', size)] = benchmark(lambda: s.sum())
        results[('Mean', size)] = benchmark(lambda: s.mean())
        results[('Min', size)] = benchmark(lambda: s.min())
        results[('Max', size)] = benchmark(lambda: s.max())

        # Statistics
        results[('Median', size)] = benchmark(lambda: s.median())
        results[('Quantile (0.95)', size)] = benchmark(lambda: s.quantile(0.95))
        results[('Variance', size)] = benchmark(lambda: s.var())
        results[('StdDev', size)] = benchmark(lambda: s.std())

        # Sorting
        results[('Sort F64', size)] = benchmark(lambda: s.sort())
        results[('Argsort F64', size)] = benchmark(lambda: s.arg_sort())

        # Arithmetic
        results[('Add', size)] = benchmark(lambda: s + s2)
        results[('Mul', size)] = benchmark(lambda: s * s2)
        results[('Div', size)] = benchmark(lambda: s / s2)
        results[('Add Scalar', size)] = benchmark(lambda: s + 42.0)
        results[('Mul Scalar', size)] = benchmark(lambda: s * 2.5)

        # Comparisons
        results[('CmpGt', size)] = benchmark(lambda: s > 0)
        results[('FilterGt (indices)', size)] = benchmark(lambda: (s > 0).arg_true())

        # Window Functions
        results[('Rolling Sum', size)] = benchmark(lambda: s.rolling_sum(window_size=WINDOW_SIZE))
        results[('Rolling Mean', size)] = benchmark(lambda: s.rolling_mean(window_size=WINDOW_SIZE))
        results[('Rolling Min', size)] = benchmark(lambda: s.rolling_min(window_size=WINDOW_SIZE))
        results[('Rolling Max', size)] = benchmark(lambda: s.rolling_max(window_size=WINDOW_SIZE))
        results[('Diff', size)] = benchmark(lambda: s.diff())
        results[('Rank', size)] = benchmark(lambda: s.rank())

        # Horizontal/Fold (using DataFrame)
        df_fold = pl.DataFrame({'a': data['float64'], 'b': data['float64_2']})
        results[('Sum Horizontal', size)] = benchmark(lambda: df_fold.select(pl.sum_horizontal(['a', 'b'])))
        results[('Min Horizontal', size)] = benchmark(lambda: df_fold.select(pl.min_horizontal(['a', 'b'])))
        results[('Max Horizontal', size)] = benchmark(lambda: df_fold.select(pl.max_horizontal(['a', 'b'])))

        # GroupBy
        df_groupby = pl.DataFrame({
            'key': data['group_keys'],
            'value': data['float64']
        })
        results[('GroupBy Sum', size)] = benchmark(lambda: df_groupby.group_by('key').agg(pl.col('value').sum()))
        results[('GroupBy Mean', size)] = benchmark(lambda: df_groupby.group_by('key').agg(pl.col('value').mean()))
        results[('GroupBy Count', size)] = benchmark(lambda: df_groupby.group_by('key').agg(pl.col('value').count()))

        # Joins
        right_size = size // 10
        right_data = generate_data(right_size, seed=SEED+1)
        left_df = pl.DataFrame({
            'key': data['int64'],
            'left_val': data['float64']
        })
        right_df = pl.DataFrame({
            'key': right_data['int64'],
            'right_val': right_data['float64']
        })
        results[('Inner Join', size)] = benchmark(lambda: left_df.join(right_df, on='key', how='inner'))
        results[('Left Join', size)] = benchmark(lambda: left_df.join(right_df, on='key', how='left'))

    return results

# ============================================================================
# Pandas Benchmarks
# ============================================================================

def benchmark_pandas(sizes: List[int]) -> Dict[Tuple[str, int], float]:
    """Run all Pandas benchmarks."""
    if not HAS_PANDAS:
        return {}

    results = {}

    for size in sizes:
        data = generate_data(size)
        s = pd.Series(data['float64'])
        s2 = pd.Series(data['float64_2'])

        # Core Aggregations
        results[('Sum', size)] = benchmark(lambda: s.sum())
        results[('Mean', size)] = benchmark(lambda: s.mean())
        results[('Min', size)] = benchmark(lambda: s.min())
        results[('Max', size)] = benchmark(lambda: s.max())

        # Statistics
        results[('Median', size)] = benchmark(lambda: s.median())
        results[('Quantile (0.95)', size)] = benchmark(lambda: s.quantile(0.95))
        results[('Variance', size)] = benchmark(lambda: s.var())
        results[('StdDev', size)] = benchmark(lambda: s.std())

        # Sorting
        results[('Sort F64', size)] = benchmark(lambda: s.sort_values())
        results[('Argsort F64', size)] = benchmark(lambda: s.argsort())

        # Arithmetic
        results[('Add', size)] = benchmark(lambda: s + s2)
        results[('Mul', size)] = benchmark(lambda: s * s2)
        results[('Div', size)] = benchmark(lambda: s / s2)
        results[('Add Scalar', size)] = benchmark(lambda: s + 42.0)
        results[('Mul Scalar', size)] = benchmark(lambda: s * 2.5)

        # Comparisons
        results[('CmpGt', size)] = benchmark(lambda: s > 0)
        results[('FilterGt (indices)', size)] = benchmark(lambda: np.where(s > 0)[0])

        # Window Functions
        results[('Rolling Sum', size)] = benchmark(lambda: s.rolling(WINDOW_SIZE).sum())
        results[('Rolling Mean', size)] = benchmark(lambda: s.rolling(WINDOW_SIZE).mean())
        results[('Rolling Min', size)] = benchmark(lambda: s.rolling(WINDOW_SIZE).min())
        results[('Rolling Max', size)] = benchmark(lambda: s.rolling(WINDOW_SIZE).max())
        results[('Diff', size)] = benchmark(lambda: s.diff())
        results[('Rank', size)] = benchmark(lambda: s.rank())

        # Horizontal/Fold (using DataFrame)
        df_fold = pd.DataFrame({'a': data['float64'], 'b': data['float64_2']})
        results[('Sum Horizontal', size)] = benchmark(lambda: df_fold[['a', 'b']].sum(axis=1))
        results[('Min Horizontal', size)] = benchmark(lambda: df_fold[['a', 'b']].min(axis=1))
        results[('Max Horizontal', size)] = benchmark(lambda: df_fold[['a', 'b']].max(axis=1))

        # GroupBy
        df_groupby = pd.DataFrame({
            'key': data['group_keys'],
            'value': data['float64']
        })
        results[('GroupBy Sum', size)] = benchmark(lambda: df_groupby.groupby('key')['value'].sum())
        results[('GroupBy Mean', size)] = benchmark(lambda: df_groupby.groupby('key')['value'].mean())
        results[('GroupBy Count', size)] = benchmark(lambda: df_groupby.groupby('key')['value'].count())

        # Joins
        right_size = size // 10
        right_data = generate_data(right_size, seed=SEED+1)
        left_df = pd.DataFrame({
            'key': data['int64'],
            'left_val': data['float64']
        })
        right_df = pd.DataFrame({
            'key': right_data['int64'],
            'right_val': right_data['float64']
        })
        results[('Inner Join', size)] = benchmark(lambda: left_df.merge(right_df, on='key', how='inner'))
        results[('Left Join', size)] = benchmark(lambda: left_df.merge(right_df, on='key', how='left'))

    return results

# ============================================================================
# Galleon Benchmarks (via Go subprocess)
# ============================================================================

def run_galleon_benchmarks(sizes: List[int]) -> Dict[Tuple[str, int], float]:
    """Run Galleon benchmarks via Go test and parse results."""
    results = {}

    # Run Go benchmarks and capture JSON output
    try:
        env = os.environ.copy()
        env['BENCHMARK_JSON'] = '1'
        env['BENCHMARK_SIZES'] = ','.join(str(s) for s in sizes)
        proc = subprocess.run(
            ['go', 'test', '-tags', 'dev', '-v', '-run', 'TestOutputBenchmarkJSON', './benchmarks/'],
            cwd='/galleon/go',
            capture_output=True,
            text=True,
            env=env,
            timeout=300
        )

        # Parse JSON from output
        output = proc.stdout
        # Find JSON array in output
        start_idx = output.find('[')
        end_idx = output.rfind(']') + 1

        if start_idx >= 0 and end_idx > start_idx:
            json_str = output[start_idx:end_idx]
            data = json.loads(json_str)

            for item in data:
                key = (item['operation'], item['size'])
                results[key] = item['time_ms']
    except Exception as e:
        print(f"Warning: Could not run Galleon benchmarks: {e}", file=sys.stderr)

        # Fallback: parse standard Go benchmark output
        try:
            proc = subprocess.run(
                ['go', 'test', '-tags', 'dev', '-bench', 'BenchmarkAll_', '-benchtime', '500ms', './benchmarks/'],
                cwd='/galleon/go',
                capture_output=True,
                text=True,
                timeout=300
            )

            # Parse standard benchmark output
            for line in proc.stdout.split('\n'):
                if line.startswith('BenchmarkAll_'):
                    parts = line.split()
                    if len(parts) >= 3:
                        # Parse benchmark name and time
                        name = parts[0].replace('BenchmarkAll_', '').replace('_', ' ')
                        # Extract size from name like "Sum_F64/1000000"
                        if '/' in name:
                            name_parts = name.split('/')
                            base_name = name_parts[0].replace('F64', '').replace('I64', '').strip()
                            try:
                                size = int(name_parts[1])
                                # Find ns/op
                                for i, p in enumerate(parts):
                                    if 'ns/op' in p:
                                        ns_per_op = float(parts[i-1])
                                        results[(base_name, size)] = ns_per_op / 1_000_000
                                        break
                            except:
                                pass
        except Exception as e2:
            print(f"Warning: Fallback benchmark parsing failed: {e2}", file=sys.stderr)

    return results

# ============================================================================
# Output Formatting
# ============================================================================

def print_comparison_table(
    sizes: List[int],
    galleon: Dict,
    polars: Dict,
    pandas: Dict,
    operations: List[str],
    category: str
):
    """Print a formatted comparison table for a category."""

    print()
    print("=" * 100)
    print(f"{category.upper()}")
    print("=" * 100)
    print()

    # Header
    header = f"{'Operation':<25}"
    for size in sizes:
        size_str = f"{size//1000}K" if size < 1_000_000 else f"{size//1_000_000}M"
        header += f" | {'Galleon':>10} {'Polars':>10} {'Pandas':>10}"
    print(header)
    print("-" * 100)

    for op in operations:
        row = f"{op:<25}"
        for size in sizes:
            key = (op, size)
            g = galleon.get(key)
            p = polars.get(key)
            pd_val = pandas.get(key)

            g_str = format_time(g) if g else "N/A"
            p_str = format_time(p) if p else "N/A"
            pd_str = format_time(pd_val) if pd_val else "N/A"

            row += f" | {g_str:>10} {p_str:>10} {pd_str:>10}"
        print(row)

    print()

    # Speedup summary
    print("Speedup vs Polars (lower = Galleon faster):")
    for op in operations:
        speedups = []
        for size in sizes:
            key = (op, size)
            g = galleon.get(key)
            p = polars.get(key)
            if g and p and p > 0:
                speedups.append(f"{g/p:.1f}x")
            else:
                speedups.append("N/A")
        print(f"  {op:<25}: {', '.join(speedups)}")
    print()

def main():
    parser = argparse.ArgumentParser(description='Galleon vs Polars vs Pandas Benchmark')
    parser.add_argument('--sizes', default='100000,1000000', help='Comma-separated sizes')
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(',')]

    print()
    print("=" * 100)
    print("GALLEON vs POLARS vs PANDAS - COMPREHENSIVE BENCHMARK")
    print("=" * 100)
    print()
    print("Environment:")
    print(f"  - Sizes: {sizes}")
    print(f"  - Seed: {SEED}")
    print(f"  - Iterations: {ITERATIONS}")
    if HAS_POLARS:
        print(f"  - Polars: {pl.__version__}")
    if HAS_PANDAS:
        print(f"  - Pandas: {pd.__version__}")
    print()

    # Run benchmarks
    print("Running Galleon benchmarks...")
    galleon_results = run_galleon_benchmarks(sizes)

    print("Running Polars benchmarks...")
    polars_results = benchmark_polars(sizes)

    print("Running Pandas benchmarks...")
    pandas_results = benchmark_pandas(sizes)

    # Define operation categories
    categories = {
        'Core Aggregations': ['Sum', 'Mean', 'Min', 'Max'],
        'Statistics': ['Median', 'Quantile (0.95)', 'Variance', 'StdDev'],
        'Sorting': ['Sort F64', 'Argsort F64'],
        'Arithmetic': ['Add', 'Mul', 'Div', 'Add Scalar', 'Mul Scalar'],
        'Comparisons': ['CmpGt', 'FilterGt (indices)'],
        'Window Functions': ['Rolling Sum', 'Rolling Mean', 'Rolling Min', 'Rolling Max', 'Diff', 'Rank'],
        'Horizontal/Fold': ['Sum Horizontal', 'Min Horizontal', 'Max Horizontal'],
        'GroupBy': ['GroupBy Sum', 'GroupBy Mean', 'GroupBy Count'],
        'Joins': ['Inner Join', 'Left Join'],
    }

    # Print results
    for category, operations in categories.items():
        print_comparison_table(sizes, galleon_results, polars_results, pandas_results, operations, category)

    # Summary table (use largest size for summary)
    summary_size = max(sizes)
    print()
    print("=" * 100)
    print(f"SUMMARY: GALLEON vs POLARS SPEEDUP ({summary_size:,} elements)")
    print("=" * 100)
    print()
    print(f"{'Operation':<30} {'Galleon':>12} {'Polars':>12} {'Ratio':>12} {'Winner':>10}")
    print("-" * 100)

    all_ops = []
    for ops in categories.values():
        all_ops.extend(ops)

    size = summary_size
    galleon_wins = 0
    polars_wins = 0

    for op in all_ops:
        key = (op, size)
        g = galleon_results.get(key)
        p = polars_results.get(key)

        if g and p:
            ratio = g / p
            winner = "Galleon" if ratio < 1 else "Polars"
            if ratio < 1:
                galleon_wins += 1
            else:
                polars_wins += 1
            print(f"{op:<30} {format_time(g):>12} {format_time(p):>12} {ratio:>11.1f}x {winner:>10}")
        else:
            print(f"{op:<30} {'N/A':>12} {'N/A':>12} {'N/A':>12} {'N/A':>10}")

    print("-" * 100)
    print(f"Galleon wins: {galleon_wins}, Polars wins: {polars_wins}")
    print()

if __name__ == '__main__':
    main()
