#!/usr/bin/env python3
"""
Resource usage comparison: Polars vs Galleon estimates
"""

import time
import resource
import numpy as np
import polars as pl
import os

def get_memory_mb():
    """Get current process memory usage in MB"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024

def measure_polars_join(n, warmup=False):
    """Measure Polars join timing"""
    np.random.seed(42)
    
    left_n = n
    right_n = n // 2
    num_keys = n // 10
    
    # Generate data
    left_ids = np.random.randint(0, num_keys, size=left_n, dtype=np.int64)
    left_vals = np.random.randn(left_n)
    right_ids = np.random.randint(0, num_keys, size=right_n, dtype=np.int64)
    right_vals = np.random.randn(right_n)
    
    left_df = pl.DataFrame({'id': left_ids, 'left_val': left_vals})
    right_df = pl.DataFrame({'id': right_ids, 'right_val': right_vals})
    
    mem_before = get_memory_mb()
    
    start = time.perf_counter()
    result = left_df.join(right_df, on='id', how='inner')
    elapsed = (time.perf_counter() - start) * 1000
    
    mem_after = get_memory_mb()
    
    return {
        'time_ms': elapsed,
        'memory_delta_mb': mem_after - mem_before,
        'result_rows': len(result),
    }

def main():
    print("="*70)
    print("RESOURCE USAGE COMPARISON")
    print("="*70)
    print(f"\nPolars version: {pl.__version__}")
    print(f"CPU cores: {os.cpu_count()}")
    print(f"Polars threads: {pl.thread_pool_size()}")
    
    n = 1_000_000
    left_n = n
    right_n = n // 2
    num_keys = n // 10
    
    print(f"\n{'='*70}")
    print(f"Test: {n:,} rows (left={left_n:,}, right={right_n:,}, keys={num_keys:,})")
    print(f"{'='*70}")
    
    # Warmup
    for _ in range(2):
        measure_polars_join(n)
    
    # Measure Polars
    times = []
    for _ in range(5):
        r = measure_polars_join(n)
        times.append(r['time_ms'])
    
    polars_time = sorted(times)[2]  # median
    polars_min = min(times)
    polars_max = max(times)
    result_rows = r['result_rows']
    
    print(f"\n--- POLARS ---")
    print(f"  Time:          {polars_time:.2f} ms (min: {polars_min:.2f}, max: {polars_max:.2f})")
    print(f"  Threads:       {pl.thread_pool_size()} (uses all cores)")
    print(f"  Result rows:   {result_rows:,}")
    
    # Estimate Polars memory (based on their documentation)
    # Polars pre-allocates based on cardinality estimation
    polars_mem_estimate = (
        left_n * 8 +           # left hashes
        right_n * 8 +          # right hashes  
        right_n * 4 * 4 +      # hash table (~4x)
        right_n * 4 +          # next pointers
        result_rows * 4 * 2    # result indices
    ) / 1024 / 1024
    
    print(f"  Memory (est):  ~{polars_mem_estimate:.1f} MB")
    
    print(f"\n--- GALLEON (from benchmarks) ---")
    galleon_time = 33  # median from our benchmarks
    print(f"  Time:          ~{galleon_time} ms")
    print(f"  Threads:       8 (configurable)")
    print(f"  Result rows:   ~{result_rows:,}")
    
    # Galleon memory estimate (our implementation)
    # We don't pre-allocate left hashes (computed on-the-fly)
    galleon_mem_estimate = (
        # left_n * 8 +         # NO left hashes - computed on-the-fly!
        right_n * 8 +          # right hashes  
        right_n * 4 * 4 +      # hash table (~4x adaptive)
        right_n * 4 +          # next pointers
        result_rows * 4 * 2    # result indices
    ) / 1024 / 1024
    
    print(f"  Memory (est):  ~{galleon_mem_estimate:.1f} MB")
    
    print(f"\n--- COMPARISON ---")
    print(f"  Speed:         Polars is {galleon_time/polars_time:.2f}x faster")
    print(f"  Memory:        Galleon saves ~{(polars_mem_estimate - galleon_mem_estimate):.1f} MB ({(1 - galleon_mem_estimate/polars_mem_estimate)*100:.0f}% less)")
    print(f"  Threads:       Similar (8 vs 11)")
    
    print(f"\n--- MEMORY BREAKDOWN (1M x 500K join) ---")
    print(f"                      Polars      Galleon     Savings")
    print(f"  Left hashes:        {left_n*8/1024/1024:.1f} MB      0 MB        {left_n*8/1024/1024:.1f} MB (on-the-fly)")
    print(f"  Right hashes:       {right_n*8/1024/1024:.1f} MB      {right_n*8/1024/1024:.1f} MB      0 MB")
    print(f"  Hash table:         {right_n*4*4/1024/1024:.1f} MB      {right_n*4*4/1024/1024:.1f} MB      0 MB (adaptive)")
    print(f"  Next pointers:      {right_n*4/1024/1024:.1f} MB      {right_n*4/1024/1024:.1f} MB      0 MB")
    print(f"  Result arrays:      {result_rows*4*2/1024/1024:.1f} MB      {result_rows*4*2/1024/1024:.1f} MB      0 MB")
    print(f"  ---------------------------------------------------------")
    print(f"  TOTAL:              {polars_mem_estimate:.1f} MB      {galleon_mem_estimate:.1f} MB      {polars_mem_estimate - galleon_mem_estimate:.1f} MB")

if __name__ == '__main__':
    main()
