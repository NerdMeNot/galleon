#!/usr/bin/env python3
"""
Comprehensive Benchmark Suite: Galleon vs Polars vs Pandas

This script runs benchmarks for DataFrame operations across three libraries:
- Galleon (Go + Zig SIMD)
- Polars (Rust)
- Pandas (Python/NumPy)

Results are saved to JSON and can be rendered as Markdown tables.

Usage:
    python run_benchmarks.py [--sizes 10000,100000,1000000] [--output results.json]
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np

# Try to import optional dependencies
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    print("Warning: Polars not installed, skipping Polars benchmarks")

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("Warning: Pandas not installed, skipping Pandas benchmarks")


def run_benchmark(func, warmup=2, iterations=10):
    """Run benchmark with warmup and return statistics in milliseconds"""
    # Warmup
    for _ in range(warmup):
        func()

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func()
        elapsed = (time.perf_counter() - start) * 1000  # Convert to ms
        times.append(elapsed)
        del result  # Ensure result is freed

    times.sort()
    return {
        'median': times[len(times) // 2],
        'min': times[0],
        'max': times[-1],
        'mean': np.mean(times),
        'std': np.std(times),
        'all_times': times,
    }


def generate_data(n, seed=42):
    """Generate identical test data for all libraries"""
    np.random.seed(seed)

    left_n = n
    right_n = n // 2
    num_keys = n // 10

    return {
        # Join data
        'left_ids': np.random.randint(0, num_keys, size=left_n, dtype=np.int64),
        'left_vals': np.random.randn(left_n).astype(np.float64),
        'right_ids': np.random.randint(0, num_keys, size=right_n, dtype=np.int64),
        'right_vals': np.random.randn(right_n).astype(np.float64),
        # GroupBy data
        'group_keys': np.random.randint(0, num_keys, size=n, dtype=np.int64),
        'values': np.random.randn(n).astype(np.float64),
        # Metadata
        'left_n': left_n,
        'right_n': right_n,
        'num_keys': num_keys,
    }


def benchmark_polars(data, iterations=10):
    """Benchmark Polars operations"""
    if not HAS_POLARS:
        return None

    # Create DataFrames
    left_df = pl.DataFrame({
        'id': data['left_ids'],
        'left_val': data['left_vals']
    })
    right_df = pl.DataFrame({
        'id': data['right_ids'],
        'right_val': data['right_vals']
    })
    groupby_df = pl.DataFrame({
        'key': data['group_keys'],
        'value': data['values']
    })

    results = {'library': 'polars', 'version': pl.__version__}

    # Aggregations
    results['sum'] = run_benchmark(
        lambda: groupby_df['value'].sum(), iterations=iterations)
    results['min'] = run_benchmark(
        lambda: groupby_df['value'].min(), iterations=iterations)
    results['max'] = run_benchmark(
        lambda: groupby_df['value'].max(), iterations=iterations)
    results['mean'] = run_benchmark(
        lambda: groupby_df['value'].mean(), iterations=iterations)

    # Filter
    threshold = 0.0
    results['filter'] = run_benchmark(
        lambda: groupby_df.filter(pl.col('value') > threshold), iterations=iterations)

    # Sort
    results['sort'] = run_benchmark(
        lambda: groupby_df.sort('value'), iterations=iterations)

    # GroupBy
    results['groupby_sum'] = run_benchmark(
        lambda: groupby_df.group_by('key').agg(pl.col('value').sum()),
        iterations=iterations)
    results['groupby_mean'] = run_benchmark(
        lambda: groupby_df.group_by('key').agg(pl.col('value').mean()),
        iterations=iterations)
    results['groupby_multi'] = run_benchmark(
        lambda: groupby_df.group_by('key').agg([
            pl.col('value').sum().alias('sum'),
            pl.col('value').mean().alias('mean'),
            pl.col('value').min().alias('min'),
            pl.col('value').max().alias('max'),
            pl.col('value').count().alias('count'),
        ]), iterations=iterations)

    # Joins
    results['inner_join'] = run_benchmark(
        lambda: left_df.join(right_df, on='id', how='inner'),
        warmup=2, iterations=iterations)
    results['left_join'] = run_benchmark(
        lambda: left_df.join(right_df, on='id', how='left'),
        warmup=2, iterations=iterations)

    return results


def benchmark_pandas(data, iterations=10):
    """Benchmark Pandas operations"""
    if not HAS_PANDAS:
        return None

    # Create DataFrames
    left_df = pd.DataFrame({
        'id': data['left_ids'],
        'left_val': data['left_vals']
    })
    right_df = pd.DataFrame({
        'id': data['right_ids'],
        'right_val': data['right_vals']
    })
    groupby_df = pd.DataFrame({
        'key': data['group_keys'],
        'value': data['values']
    })

    results = {'library': 'pandas', 'version': pd.__version__}

    # Aggregations
    results['sum'] = run_benchmark(
        lambda: groupby_df['value'].sum(), iterations=iterations)
    results['min'] = run_benchmark(
        lambda: groupby_df['value'].min(), iterations=iterations)
    results['max'] = run_benchmark(
        lambda: groupby_df['value'].max(), iterations=iterations)
    results['mean'] = run_benchmark(
        lambda: groupby_df['value'].mean(), iterations=iterations)

    # Filter
    threshold = 0.0
    results['filter'] = run_benchmark(
        lambda: groupby_df[groupby_df['value'] > threshold], iterations=iterations)

    # Sort
    results['sort'] = run_benchmark(
        lambda: groupby_df.sort_values('value'), iterations=iterations)

    # GroupBy
    results['groupby_sum'] = run_benchmark(
        lambda: groupby_df.groupby('key')['value'].sum(),
        iterations=iterations)
    results['groupby_mean'] = run_benchmark(
        lambda: groupby_df.groupby('key')['value'].mean(),
        iterations=iterations)
    results['groupby_multi'] = run_benchmark(
        lambda: groupby_df.groupby('key')['value'].agg(['sum', 'mean', 'min', 'max', 'count']),
        iterations=iterations)

    # Joins
    results['inner_join'] = run_benchmark(
        lambda: pd.merge(left_df, right_df, on='id', how='inner'),
        warmup=2, iterations=iterations)
    results['left_join'] = run_benchmark(
        lambda: pd.merge(left_df, right_df, on='id', how='left'),
        warmup=2, iterations=iterations)

    return results


def run_galleon_benchmark(data, go_dir):
    """Run Galleon benchmarks via Go test"""
    # Save data for Go to read
    np.save('/tmp/galleon_bench_left_ids.npy', data['left_ids'])
    np.save('/tmp/galleon_bench_left_vals.npy', data['left_vals'])
    np.save('/tmp/galleon_bench_right_ids.npy', data['right_ids'])
    np.save('/tmp/galleon_bench_right_vals.npy', data['right_vals'])
    np.save('/tmp/galleon_bench_group_keys.npy', data['group_keys'])
    np.save('/tmp/galleon_bench_values.npy', data['values'])

    with open('/tmp/galleon_bench_info.txt', 'w') as f:
        f.write(f"{data['left_n']}\n")
        f.write(f"{data['right_n']}\n")
        f.write(f"{data['num_keys']}\n")

    # Run Go benchmark (output captured from Go benchmark tool)
    # This is a placeholder - actual Go benchmarks run separately
    return None


def format_time(ms):
    """Format time in appropriate units"""
    if ms < 1:
        return f"{ms*1000:.1f}µs"
    elif ms < 1000:
        return f"{ms:.2f}ms"
    else:
        return f"{ms/1000:.2f}s"


def print_comparison_table(results, sizes):
    """Print formatted comparison table"""
    operations = [
        ('sum', 'Sum'),
        ('min', 'Min'),
        ('max', 'Max'),
        ('mean', 'Mean'),
        ('filter', 'Filter (>0)'),
        ('sort', 'Sort'),
        ('groupby_sum', 'GroupBy Sum'),
        ('groupby_mean', 'GroupBy Mean'),
        ('groupby_multi', 'GroupBy Multi'),
        ('inner_join', 'Inner Join'),
        ('left_join', 'Left Join'),
    ]

    for size in sizes:
        if size not in results:
            continue

        size_results = results[size]
        polars_res = size_results.get('polars', {})
        pandas_res = size_results.get('pandas', {})

        print(f"\n{'='*80}")
        print(f"Size: {size:,} rows")
        print(f"{'='*80}")
        print(f"{'Operation':<20} {'Polars':>12} {'Pandas':>12} {'Speedup':>12}")
        print(f"{'-'*80}")

        for op_key, op_name in operations:
            polars_time = polars_res.get(op_key, {}).get('median', float('inf'))
            pandas_time = pandas_res.get(op_key, {}).get('median', float('inf'))

            if polars_time < float('inf') and pandas_time < float('inf'):
                speedup = pandas_time / polars_time
                speedup_str = f"{speedup:.1f}x"
            else:
                speedup_str = "N/A"

            polars_str = format_time(polars_time) if polars_time < float('inf') else "N/A"
            pandas_str = format_time(pandas_time) if pandas_time < float('inf') else "N/A"

            print(f"{op_name:<20} {polars_str:>12} {pandas_str:>12} {speedup_str:>12}")


def generate_markdown_table(results, sizes):
    """Generate Markdown table for documentation"""
    lines = []

    operations = [
        ('sum', 'Sum'),
        ('min', 'Min'),
        ('max', 'Max'),
        ('mean', 'Mean'),
        ('filter', 'Filter'),
        ('sort', 'Sort'),
        ('groupby_sum', 'GroupBy Sum'),
        ('groupby_mean', 'GroupBy Mean'),
        ('inner_join', 'Inner Join'),
        ('left_join', 'Left Join'),
    ]

    for size in sizes:
        if size not in results:
            continue

        size_results = results[size]
        polars_res = size_results.get('polars', {})
        pandas_res = size_results.get('pandas', {})

        lines.append(f"\n### {size:,} Rows\n")
        lines.append("| Operation | Polars | Pandas | Polars Speedup |")
        lines.append("|-----------|--------|--------|----------------|")

        for op_key, op_name in operations:
            polars_time = polars_res.get(op_key, {}).get('median', float('inf'))
            pandas_time = pandas_res.get(op_key, {}).get('median', float('inf'))

            if polars_time < float('inf') and pandas_time < float('inf'):
                speedup = pandas_time / polars_time
                speedup_str = f"{speedup:.1f}x"
            else:
                speedup_str = "N/A"

            polars_str = format_time(polars_time) if polars_time < float('inf') else "N/A"
            pandas_str = format_time(pandas_time) if pandas_time < float('inf') else "N/A"

            lines.append(f"| {op_name} | {polars_str} | {pandas_str} | {speedup_str} |")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Run DataFrame benchmarks')
    parser.add_argument('--sizes', type=str, default='10000,100000,1000000',
                       help='Comma-separated list of data sizes')
    parser.add_argument('--iterations', type=int, default=10,
                       help='Number of iterations per benchmark')
    parser.add_argument('--output', type=str, default='benchmark_results.json',
                       help='Output JSON file')
    parser.add_argument('--markdown', type=str, default='benchmark_results.md',
                       help='Output Markdown file')
    args = parser.parse_args()

    sizes = [int(s.strip()) for s in args.sizes.split(',')]

    print("="*80)
    print("DATAFRAME BENCHMARK SUITE")
    print("="*80)
    print(f"Date: {datetime.now().isoformat()}")
    print(f"Sizes: {sizes}")
    print(f"Iterations: {args.iterations}")
    if HAS_POLARS:
        print(f"Polars version: {pl.__version__}")
    if HAS_PANDAS:
        print(f"Pandas version: {pd.__version__}")
    print("="*80)

    all_results = {
        'metadata': {
            'date': datetime.now().isoformat(),
            'sizes': sizes,
            'iterations': args.iterations,
            'polars_version': pl.__version__ if HAS_POLARS else None,
            'pandas_version': pd.__version__ if HAS_PANDAS else None,
        },
        'results': {}
    }

    for size in sizes:
        print(f"\n{'='*80}")
        print(f"Benchmarking size: {size:,} rows")
        print("="*80)

        print("Generating data...")
        data = generate_data(size)
        print(f"  Left table:  {data['left_n']:,} rows")
        print(f"  Right table: {data['right_n']:,} rows")
        print(f"  Unique keys: {data['num_keys']:,}")

        size_results = {}

        if HAS_POLARS:
            print("\nRunning Polars benchmarks...")
            polars_results = benchmark_polars(data, iterations=args.iterations)
            size_results['polars'] = polars_results
            print(f"  Inner Join: {polars_results['inner_join']['median']:.2f}ms")
            print(f"  Left Join:  {polars_results['left_join']['median']:.2f}ms")
            print(f"  GroupBy Sum: {polars_results['groupby_sum']['median']:.2f}ms")

        if HAS_PANDAS:
            print("\nRunning Pandas benchmarks...")
            pandas_results = benchmark_pandas(data, iterations=args.iterations)
            size_results['pandas'] = pandas_results
            print(f"  Inner Join: {pandas_results['inner_join']['median']:.2f}ms")
            print(f"  Left Join:  {pandas_results['left_join']['median']:.2f}ms")
            print(f"  GroupBy Sum: {pandas_results['groupby_sum']['median']:.2f}ms")

        all_results['results'][size] = size_results

    # Print comparison tables
    print_comparison_table(all_results['results'], sizes)

    # Save JSON results
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        # Convert numpy types for JSON serialization
        def convert(obj):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, (np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.float64, np.float32)):
                return float(obj)
            return obj

        json.dump(all_results, f, indent=2, default=convert)
    print(f"\nResults saved to {output_path}")

    # Generate Markdown
    markdown = generate_markdown_table(all_results['results'], sizes)
    md_path = Path(args.markdown)
    with open(md_path, 'w') as f:
        f.write("# Benchmark Results\n\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        f.write(markdown)
    print(f"Markdown saved to {md_path}")


if __name__ == '__main__':
    main()
