//! Rust Rayon Benchmarks
//!
//! Comprehensive benchmarks comparing Rayon performance against sequential execution.
//! This benchmark suite mirrors the Zig blitz benchmarks for direct comparison.
//!
//! Run with: cargo run --release

use rayon::prelude::*;
use std::hint::black_box;
use std::time::Instant;

// ============================================================================
// Configuration
// ============================================================================

const ITERATIONS: usize = 10;
const WARMUP_ITERATIONS: usize = 3;

const SIZES: [usize; 5] = [1_000, 10_000, 100_000, 1_000_000, 10_000_000];

// ============================================================================
// Random number generator (xorshift64 - matches Zig implementation)
// ============================================================================

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    println!();
    println!("{}", "=".repeat(80));
    println!("                    Rust Rayon Benchmark Suite");
    println!("{}", "=".repeat(80));
    println!(
        "Iterations: {} (+ {} warmup)   Platform: {}",
        ITERATIONS,
        WARMUP_ITERATIONS,
        std::env::consts::ARCH
    );
    println!(
        "Rayon threads: {}",
        rayon::current_num_threads()
    );
    println!();

    bench_join_overhead();
    println!();

    for &n in &SIZES {
        println!("{}", "=".repeat(80));
        println!("N = {:>12}", n);
        println!("{}", "=".repeat(80));
        println!();

        bench_parallel_sum(n);
        bench_parallel_map(n);
        bench_parallel_reduce(n);
        bench_parallel_for(n);
        println!();

        // Sort benchmarks (skip 10M due to memory)
        if n <= 1_000_000 {
            bench_parallel_sort(n);
            println!();
        }

        // Iterator benchmarks
        bench_parallel_iter_sum(n);
        bench_parallel_iter_map_collect(n);
        bench_parallel_iter_filter(n);
        println!();
    }

    println!("{}", "=".repeat(80));
    println!("Benchmark complete.");
    println!("{}", "=".repeat(80));
    println!();
}

// ============================================================================
// Join Overhead
// ============================================================================

fn bench_join_overhead() {
    println!("=== Join Overhead ===");

    let iterations: usize = 100_000;

    // Warmup
    for _ in 0..1000 {
        let (a, b) = rayon::join(|| 1i64, || 2i64);
        black_box(a + b);
    }

    // Benchmark
    let start = Instant::now();
    let mut count: i64 = 0;
    for _ in 0..iterations {
        let (a, b) = rayon::join(|| 1i64, || 2i64);
        count += a + b;
    }
    let elapsed_ns = start.elapsed().as_nanos() as f64;
    let avg_ns = elapsed_ns / iterations as f64;

    println!("Empty join: {:.1}ns avg (total={})", avg_ns, count);
}

// ============================================================================
// Parallel Sum
// ============================================================================

fn bench_parallel_sum(n: usize) {
    // Initialize data
    let data: Vec<i64> = (0..n).map(|i| (i % 1000) as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let _: i64 = data.iter().sum();
        let _: i64 = data.par_iter().sum();
    }

    // Sequential sum
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let sum: i64 = data.iter().sum();
        black_box(sum);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel sum
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let sum: i64 = data.par_iter().sum();
        black_box(sum);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  Sum:           seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}

// ============================================================================
// Parallel Map
// ============================================================================

fn bench_parallel_map(n: usize) {
    // Skip 10M for memory
    if n > 1_000_000 {
        return;
    }

    let input: Vec<i64> = (0..n).map(|i| i as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let _: Vec<i64> = input.iter().map(|&x| x * 2 + 1).collect();
        let _: Vec<i64> = input.par_iter().map(|&x| x * 2 + 1).collect();
    }

    // Sequential map
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: Vec<i64> = input.iter().map(|&x| x * 2 + 1).collect();
        black_box(result);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel map
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: Vec<i64> = input.par_iter().map(|&x| x * 2 + 1).collect();
        black_box(result);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  Map(x*2+1):    seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}

// ============================================================================
// Parallel Reduce (find max)
// ============================================================================

fn bench_parallel_reduce(n: usize) {
    // Initialize with pseudo-random values
    let mut rng = Rng::new(12345);
    let data: Vec<i64> = (0..n).map(|_| (rng.next() % 1_000_000) as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let _: i64 = *data.iter().max().unwrap();
        let _: i64 = data.par_iter().cloned().reduce(|| i64::MIN, |a, b| a.max(b));
    }

    // Sequential max
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let max: i64 = *data.iter().max().unwrap();
        black_box(max);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel reduce max
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let max: i64 = data.par_iter().cloned().reduce(|| i64::MIN, |a, b| a.max(b));
        black_box(max);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  Reduce(max):   seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}

// ============================================================================
// Parallel For (write indices)
// ============================================================================

fn bench_parallel_for(n: usize) {
    let mut data: Vec<u64> = vec![0; n];

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        for (i, v) in data.iter_mut().enumerate() {
            *v = (i * 2) as u64;
        }
        data.par_iter_mut().enumerate().for_each(|(i, v)| {
            *v = (i * 2) as u64;
        });
    }

    // Sequential
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        for (i, v) in data.iter_mut().enumerate() {
            *v = (i * 2) as u64;
        }
        black_box(&data);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        data.par_iter_mut().enumerate().for_each(|(i, v)| {
            *v = (i * 2) as u64;
        });
        black_box(&data);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  For(indices):  seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}

// ============================================================================
// Parallel Sort
// ============================================================================

fn bench_parallel_sort(n: usize) {
    let mut rng = Rng::new(54321);
    let original: Vec<i64> = (0..n).map(|_| rng.next() as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let mut data = original.clone();
        data.sort();
        let mut data = original.clone();
        data.par_sort();
    }

    // Sequential sort
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut data = original.clone();
        data.sort();
        black_box(&data);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel sort
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut data = original.clone();
        data.par_sort();
        black_box(&data);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    // Verify correctness
    let mut test_data = original.clone();
    test_data.par_sort();
    let is_sorted = test_data.windows(2).all(|w| w[0] <= w[1]);

    println!(
        "  Sort:          seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup (sorted={})",
        seq_ms, par_ms, speedup, is_sorted
    );
}

// ============================================================================
// Parallel Iterator - Sum
// ============================================================================

fn bench_parallel_iter_sum(n: usize) {
    let data: Vec<i64> = (0..n).map(|i| (i % 1000) as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let _: i64 = data.iter().sum();
        let _: i64 = data.par_iter().sum();
    }

    // Sequential
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let sum: i64 = data.iter().sum();
        black_box(sum);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let sum: i64 = data.par_iter().sum();
        black_box(sum);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  iter().sum():  seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}

// ============================================================================
// Parallel Iterator - Map and Collect
// ============================================================================

fn bench_parallel_iter_map_collect(n: usize) {
    if n > 1_000_000 {
        return;
    }

    let data: Vec<i64> = (0..n).map(|i| i as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let _: Vec<i64> = data.iter().map(|&x| x * x).collect();
        let _: Vec<i64> = data.par_iter().map(|&x| x * x).collect();
    }

    // Sequential
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: Vec<i64> = data.iter().map(|&x| x * x).collect();
        black_box(result);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: Vec<i64> = data.par_iter().map(|&x| x * x).collect();
        black_box(result);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  map().collect: seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}

// ============================================================================
// Parallel Iterator - Filter
// ============================================================================

fn bench_parallel_iter_filter(n: usize) {
    if n > 1_000_000 {
        return;
    }

    let data: Vec<i64> = (0..n).map(|i| i as i64).collect();

    // Warmup
    for _ in 0..WARMUP_ITERATIONS {
        let _: Vec<i64> = data.iter().filter(|&&x| x % 2 == 0).cloned().collect();
        let _: Vec<i64> = data.par_iter().filter(|&&x| x % 2 == 0).cloned().collect();
    }

    // Sequential
    let seq_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: Vec<i64> = data.iter().filter(|&&x| x % 2 == 0).cloned().collect();
        black_box(result);
    }
    let seq_ms = seq_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    // Parallel
    let par_start = Instant::now();
    for _ in 0..ITERATIONS {
        let result: Vec<i64> = data.par_iter().filter(|&&x| x % 2 == 0).cloned().collect();
        black_box(result);
    }
    let par_ms = par_start.elapsed().as_secs_f64() * 1000.0 / ITERATIONS as f64;

    let speedup = seq_ms / par_ms;

    println!(
        "  filter(even):  seq {:>8.3}ms   par {:>8.3}ms   {:.2}x speedup",
        seq_ms, par_ms, speedup
    );
}
