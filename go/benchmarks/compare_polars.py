#!/usr/bin/env python3
"""
Benchmark comparison: Galleon (Go+Zig) vs Polars
"""

import time
import polars as pl
import numpy as np

def benchmark(func, iterations=10, warmup=2):
    """Run benchmark with warmup iterations."""
    # Warmup
    for _ in range(warmup):
        func()

    # Actual benchmark
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        'mean': np.mean(times) * 1000,  # Convert to ms
        'min': np.min(times) * 1000,
        'max': np.max(times) * 1000,
        'std': np.std(times) * 1000,
    }

def main():
    print("=" * 70)
    print("POLARS BENCHMARK - For comparison with Galleon (Go+Zig)")
    print("=" * 70)
    print()

    # Test different sizes
    sizes = [10_000, 100_000, 1_000_000]

    for n in sizes:
        print(f"\n{'='*70}")
        print(f"Size: {n:,} rows")
        print("=" * 70)

        # Create test data
        np.random.seed(42)
        num_groups = n // 10  # 10 rows per group on average

        df = pl.DataFrame({
            'group_key': np.random.randint(0, num_groups, n),
            'value_f64': np.random.randn(n),
            'value_i64': np.random.randint(0, 1000000, n),
        })

        # GroupBy Sum
        result = benchmark(lambda: df.group_by('group_key').agg(pl.col('value_f64').sum()))
        print(f"  GroupBy Sum (f64):     {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # GroupBy Mean
        result = benchmark(lambda: df.group_by('group_key').agg(pl.col('value_f64').mean()))
        print(f"  GroupBy Mean (f64):    {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # GroupBy Min
        result = benchmark(lambda: df.group_by('group_key').agg(pl.col('value_f64').min()))
        print(f"  GroupBy Min (f64):     {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # GroupBy Max
        result = benchmark(lambda: df.group_by('group_key').agg(pl.col('value_f64').max()))
        print(f"  GroupBy Max (f64):     {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # GroupBy Count
        result = benchmark(lambda: df.group_by('group_key').agg(pl.col('value_f64').count()))
        print(f"  GroupBy Count:         {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # GroupBy Multiple Aggregations
        result = benchmark(lambda: df.group_by('group_key').agg([
            pl.col('value_f64').sum().alias('sum'),
            pl.col('value_f64').mean().alias('mean'),
            pl.col('value_f64').min().alias('min'),
            pl.col('value_f64').max().alias('max'),
            pl.col('value_f64').count().alias('count'),
        ]))
        print(f"  GroupBy Multi-Agg:     {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # Create join test data
        left_n = n
        right_n = n // 2
        num_keys = n // 10

        left_df = pl.DataFrame({
            'id': np.random.randint(0, num_keys, left_n),
            'left_val': np.random.randn(left_n),
        })

        right_df = pl.DataFrame({
            'id': np.random.randint(0, num_keys, right_n),
            'right_val': np.random.randn(right_n),
        })

        # Inner Join
        result = benchmark(lambda: left_df.join(right_df, on='id', how='inner'), iterations=5)
        print(f"  Inner Join:            {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

        # Left Join
        result = benchmark(lambda: left_df.join(right_df, on='id', how='left'), iterations=5)
        print(f"  Left Join:             {result['mean']:8.3f} ms  (std: {result['std']:.3f})")

    print("\n" + "=" * 70)
    print("NOTES:")
    print("- Polars uses Rust with SIMD and multi-threading")
    print("- Galleon uses Go with Zig SIMD backend (currently single-threaded)")
    print("- Times are in milliseconds (lower is better)")
    print("=" * 70)

if __name__ == '__main__':
    main()
