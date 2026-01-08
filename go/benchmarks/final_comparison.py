#!/usr/bin/env python3
"""
Final comparison benchmark: Galleon vs Polars vs Pandas
Tests with identical data across multiple sizes
"""

import time
import numpy as np
import polars as pl
import pandas as pd
import sys

def run_benchmark(name, func, warmup=2, iterations=5):
    """Run benchmark with warmup and return median time in ms"""
    # Warmup
    for _ in range(warmup):
        func()
    
    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
    
    times.sort()
    median = times[len(times) // 2]
    return median, min(times), max(times)

def generate_data(n, seed=42):
    """Generate identical test data for all libraries"""
    np.random.seed(seed)
    
    left_n = n
    right_n = n // 2
    num_keys = n // 10
    
    # Join data
    left_ids = np.random.randint(0, num_keys, size=left_n, dtype=np.int64)
    left_vals = np.random.randn(left_n)
    right_ids = np.random.randint(0, num_keys, size=right_n, dtype=np.int64)
    right_vals = np.random.randn(right_n)
    
    # GroupBy data
    group_keys = np.random.randint(0, num_keys, size=n, dtype=np.int64)
    values = np.random.randn(n)
    
    return {
        'left_ids': left_ids,
        'left_vals': left_vals,
        'right_ids': right_ids,
        'right_vals': right_vals,
        'group_keys': group_keys,
        'values': values,
        'left_n': left_n,
        'right_n': right_n,
        'num_keys': num_keys,
    }

def benchmark_polars(data):
    """Benchmark Polars operations"""
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
    
    results = {}
    
    # Inner Join
    median, min_t, max_t = run_benchmark("Inner Join", 
        lambda: left_df.join(right_df, on='id', how='inner'))
    results['inner_join'] = (median, min_t, max_t)
    
    # Left Join
    median, min_t, max_t = run_benchmark("Left Join",
        lambda: left_df.join(right_df, on='id', how='left'))
    results['left_join'] = (median, min_t, max_t)
    
    # GroupBy Sum
    median, min_t, max_t = run_benchmark("GroupBy Sum",
        lambda: groupby_df.group_by('key').agg(pl.col('value').sum()))
    results['groupby_sum'] = (median, min_t, max_t)
    
    # GroupBy Mean
    median, min_t, max_t = run_benchmark("GroupBy Mean",
        lambda: groupby_df.group_by('key').agg(pl.col('value').mean()))
    results['groupby_mean'] = (median, min_t, max_t)
    
    # GroupBy Multi-Agg
    median, min_t, max_t = run_benchmark("GroupBy Multi",
        lambda: groupby_df.group_by('key').agg([
            pl.col('value').sum().alias('sum'),
            pl.col('value').mean().alias('mean'),
            pl.col('value').min().alias('min'),
            pl.col('value').max().alias('max'),
            pl.col('value').count().alias('count'),
        ]))
    results['groupby_multi'] = (median, min_t, max_t)
    
    return results

def benchmark_pandas(data):
    """Benchmark Pandas operations"""
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
    
    results = {}
    
    # Inner Join
    median, min_t, max_t = run_benchmark("Inner Join",
        lambda: pd.merge(left_df, right_df, on='id', how='inner'))
    results['inner_join'] = (median, min_t, max_t)
    
    # Left Join
    median, min_t, max_t = run_benchmark("Left Join",
        lambda: pd.merge(left_df, right_df, on='id', how='left'))
    results['left_join'] = (median, min_t, max_t)
    
    # GroupBy Sum
    median, min_t, max_t = run_benchmark("GroupBy Sum",
        lambda: groupby_df.groupby('key')['value'].sum())
    results['groupby_sum'] = (median, min_t, max_t)
    
    # GroupBy Mean
    median, min_t, max_t = run_benchmark("GroupBy Mean",
        lambda: groupby_df.groupby('key')['value'].mean())
    results['groupby_mean'] = (median, min_t, max_t)
    
    # GroupBy Multi-Agg
    median, min_t, max_t = run_benchmark("GroupBy Multi",
        lambda: groupby_df.groupby('key')['value'].agg(['sum', 'mean', 'min', 'max', 'count']))
    results['groupby_multi'] = (median, min_t, max_t)
    
    return results

def print_results(size, polars_results, pandas_results):
    """Print formatted results"""
    print(f"\n{'='*70}")
    print(f"Size: {size:,} rows (left={size:,}, right={size//2:,}, keys={size//10:,})")
    print(f"{'='*70}")
    print(f"{'Operation':<20} {'Polars':>12} {'Pandas':>12} {'Polars speedup':>15}")
    print(f"{'-'*70}")
    
    for op in ['inner_join', 'left_join', 'groupby_sum', 'groupby_mean', 'groupby_multi']:
        polars_time = polars_results[op][0]
        pandas_time = pandas_results[op][0]
        speedup = pandas_time / polars_time if polars_time > 0 else 0
        
        op_name = op.replace('_', ' ').title()
        print(f"{op_name:<20} {polars_time:>10.2f}ms {pandas_time:>10.2f}ms {speedup:>13.1f}x")

def main():
    sizes = [10_000, 100_000, 1_000_000]
    
    print("="*70)
    print("POLARS vs PANDAS BENCHMARK")
    print("="*70)
    print(f"Polars version: {pl.__version__}")
    print(f"Pandas version: {pd.__version__}")
    
    all_results = {}
    
    for size in sizes:
        print(f"\nGenerating data for size {size:,}...")
        data = generate_data(size)
        
        print("Running Polars benchmarks...")
        polars_results = benchmark_polars(data)
        
        print("Running Pandas benchmarks...")
        pandas_results = benchmark_pandas(data)
        
        all_results[size] = {
            'polars': polars_results,
            'pandas': pandas_results
        }
        
        print_results(size, polars_results, pandas_results)
    
    # Print summary for 1M rows
    print(f"\n{'='*70}")
    print("SUMMARY - Polars times at 1M rows (for Galleon comparison)")
    print(f"{'='*70}")
    polars_1m = all_results[1_000_000]['polars']
    for op, (median, min_t, max_t) in polars_1m.items():
        op_name = op.replace('_', ' ').title()
        print(f"  {op_name:<20}: {median:>8.2f}ms (min: {min_t:.2f}, max: {max_t:.2f})")

if __name__ == '__main__':
    main()
