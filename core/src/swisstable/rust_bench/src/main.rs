//! Rust hashbrown Benchmarks
//!
//! Comprehensive benchmarks comparing hashbrown (Swiss Table) performance against:
//! - std::collections::HashMap
//!
//! This benchmark suite mirrors the Zig swisstable benchmarks for direct comparison.
//!
//! Run with: cargo run --release

use hashbrown::{HashMap, HashSet};
use std::collections::HashMap as StdHashMap;
use std::collections::HashSet as StdHashSet;
use std::hint::black_box;
use std::time::Instant;

// ============================================================================
// Configuration
// ============================================================================

const ITERATIONS: usize = 5;
const WARMUP_ITERATIONS: usize = 2;

const SIZES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

// ============================================================================
// Statistics helpers
// ============================================================================

struct Stats {
    min_ns: u128,
    max_ns: u128,
    total_ns: u128,
    count: usize,
}

impl Stats {
    fn new() -> Self {
        Self {
            min_ns: u128::MAX,
            max_ns: 0,
            total_ns: 0,
            count: 0,
        }
    }

    fn add(&mut self, ns: u128) {
        self.min_ns = self.min_ns.min(ns);
        self.max_ns = self.max_ns.max(ns);
        self.total_ns += ns;
        self.count += 1;
    }

    fn avg_ns(&self) -> u128 {
        if self.count > 0 {
            self.total_ns / self.count as u128
        } else {
            0
        }
    }

    fn ns_per_op(&self, ops: usize) -> f64 {
        self.avg_ns() as f64 / ops as f64
    }
}

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

    fn next_bounded(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    println!();
    println!("{}", "=".repeat(80));
    println!("                    Rust hashbrown Benchmark Suite");
    println!("{}", "=".repeat(80));
    println!(
        "Iterations: {} (+ {} warmup)   Platform: {}",
        ITERATIONS,
        WARMUP_ITERATIONS,
        std::env::consts::ARCH
    );
    println!();

    for &n in &SIZES {
        println!("{}", "=".repeat(80));
        println!("N = {:>12}", n);
        println!("{}", "=".repeat(80));
        println!();

        // Core operations
        bench_insert_sequential(n);
        bench_insert_random(n);
        bench_insert_prealloc(n);
        bench_insert_unique(n);
        println!();

        bench_lookup_hit(n);
        bench_lookup_miss(n);
        bench_lookup_random(n);
        println!();

        bench_remove(n);
        bench_remove_and_reinsert(n);
        println!();

        // Iteration
        bench_iteration(n);
        bench_keys_iteration(n);
        println!();

        // Entry API
        bench_entry_api(n);
        println!();

        // Set operations (for smaller sizes only)
        if n <= 100_000 {
            bench_set_insert(n);
            bench_set_contains(n);
            bench_set_union(n);
            bench_set_intersection(n);
            println!();
        }

        // Memory usage
        bench_memory_usage(n);
        println!();
    }

    println!("{}", "=".repeat(80));
    println!("Benchmark complete.");
    println!("{}", "=".repeat(80));
    println!();
}

// ============================================================================
// Insert Benchmarks
// ============================================================================

fn bench_insert_sequential(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut map: HashMap<i64, i64> = HashMap::new();
            let start = Instant::now();
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::new();
            let start = Instant::now();
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Insert (seq)", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_insert_random(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Pre-generate random keys
    let mut rng = Rng::new(12345);
    let keys: Vec<i64> = (0..n).map(|_| rng.next() as i64).collect();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut map: HashMap<i64, i64> = HashMap::new();
            let start = Instant::now();
            for &k in &keys {
                map.insert(k, k);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::new();
            let start = Instant::now();
            for &k in &keys {
                map.insert(k, k);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Insert (rnd)", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_insert_prealloc(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown with capacity
        {
            let mut map: HashMap<i64, i64> = HashMap::with_capacity(n);
            let start = Instant::now();
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap with capacity
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
            let start = Instant::now();
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Insert (pre)", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_insert_unique(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown with insert_unique_unchecked (fastest path)
        {
            let mut map: HashMap<i64, i64> = HashMap::with_capacity(n);
            let start = Instant::now();
            for i in 0..n {
                unsafe {
                    map.insert_unique_unchecked(i as i64, i as i64);
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap (no equivalent)
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
            let start = Instant::now();
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("InsertUnique", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

// ============================================================================
// Lookup Benchmarks
// ============================================================================

fn bench_lookup_hit(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup maps
    let mut hb_map: HashMap<i64, i64> = HashMap::with_capacity(n);
    let mut std_map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
    for i in 0..n {
        hb_map.insert(i as i64, i as i64);
        std_map.insert(i as i64, i as i64);
    }

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut checksum: i64 = 0;
            let start = Instant::now();
            for i in 0..n {
                if let Some(&v) = hb_map.get(&(i as i64)) {
                    checksum = checksum.wrapping_add(v);
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(checksum);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut checksum: i64 = 0;
            let start = Instant::now();
            for i in 0..n {
                if let Some(&v) = std_map.get(&(i as i64)) {
                    checksum = checksum.wrapping_add(v);
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(checksum);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Lookup (hit)", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_lookup_miss(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup maps with keys 0..n
    let mut hb_map: HashMap<i64, i64> = HashMap::with_capacity(n);
    let mut std_map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
    for i in 0..n {
        hb_map.insert(i as i64, i as i64);
        std_map.insert(i as i64, i as i64);
    }

    // Lookup keys n..2n (all misses)
    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut miss_count: usize = 0;
            let start = Instant::now();
            for i in n..(n * 2) {
                if hb_map.get(&(i as i64)).is_none() {
                    miss_count += 1;
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(miss_count);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut miss_count: usize = 0;
            let start = Instant::now();
            for i in n..(n * 2) {
                if std_map.get(&(i as i64)).is_none() {
                    miss_count += 1;
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(miss_count);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Lookup (miss)", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_lookup_random(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup maps
    let mut hb_map: HashMap<i64, i64> = HashMap::with_capacity(n);
    let mut std_map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
    for i in 0..n {
        hb_map.insert(i as i64, i as i64);
        std_map.insert(i as i64, i as i64);
    }

    // Pre-generate random lookup keys (50% hit, 50% miss)
    let mut rng = Rng::new(54321);
    let lookup_keys: Vec<i64> = (0..n)
        .map(|_| rng.next_bounded((n * 2) as u64) as i64)
        .collect();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut checksum: i64 = 0;
            let start = Instant::now();
            for &k in &lookup_keys {
                if let Some(&v) = hb_map.get(&k) {
                    checksum = checksum.wrapping_add(v);
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(checksum);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut checksum: i64 = 0;
            let start = Instant::now();
            for &k in &lookup_keys {
                if let Some(&v) = std_map.get(&k) {
                    checksum = checksum.wrapping_add(v);
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(checksum);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Lookup (rnd)", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

// ============================================================================
// Remove Benchmarks
// ============================================================================

fn bench_remove(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut map: HashMap<i64, i64> = HashMap::with_capacity(n);
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let start = Instant::now();
            for i in 0..n {
                map.remove(&(i as i64));
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let start = Instant::now();
            for i in 0..n {
                map.remove(&(i as i64));
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Remove", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_remove_and_reinsert(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown: remove half, reinsert
        {
            let mut map: HashMap<i64, i64> = HashMap::with_capacity(n);
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let start = Instant::now();
            // Remove even keys
            for i in (0..n).step_by(2) {
                map.remove(&(i as i64));
            }
            // Reinsert them
            for i in (0..n).step_by(2) {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
            for i in 0..n {
                map.insert(i as i64, i as i64);
            }
            let start = Instant::now();
            for i in (0..n).step_by(2) {
                map.remove(&(i as i64));
            }
            for i in (0..n).step_by(2) {
                map.insert(i as i64, i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Remove+Reins", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

// ============================================================================
// Iteration Benchmarks
// ============================================================================

fn bench_iteration(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup maps
    let mut hb_map: HashMap<i64, i64> = HashMap::with_capacity(n);
    let mut std_map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
    for i in 0..n {
        hb_map.insert(i as i64, i as i64);
        std_map.insert(i as i64, i as i64);
    }

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut sum: i64 = 0;
            let start = Instant::now();
            for (&k, &v) in &hb_map {
                sum = sum.wrapping_add(k).wrapping_add(v);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(sum);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap
        {
            let mut sum: i64 = 0;
            let start = Instant::now();
            for (&k, &v) in &std_map {
                sum = sum.wrapping_add(k).wrapping_add(v);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(sum);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Iterate", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_keys_iteration(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup maps
    let mut hb_map: HashMap<i64, i64> = HashMap::with_capacity(n);
    let mut std_map: StdHashMap<i64, i64> = StdHashMap::with_capacity(n);
    for i in 0..n {
        hb_map.insert(i as i64, i as i64);
        std_map.insert(i as i64, i as i64);
    }

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown keys()
        {
            let mut sum: i64 = 0;
            let start = Instant::now();
            for &k in hb_map.keys() {
                sum = sum.wrapping_add(k);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(sum);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap keys()
        {
            let mut sum: i64 = 0;
            let start = Instant::now();
            for &k in std_map.keys() {
                sum = sum.wrapping_add(k);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(sum);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Keys iter", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

// ============================================================================
// Entry API Benchmarks
// ============================================================================

fn bench_entry_api(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown entry API
        {
            let mut map: HashMap<i64, i64> = HashMap::new();
            let start = Instant::now();
            for i in 0..n {
                let key = (i % (n / 2)) as i64; // 50% duplicates
                *map.entry(key).or_insert(0) += 1;
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashMap entry API
        {
            let mut map: StdHashMap<i64, i64> = StdHashMap::new();
            let start = Instant::now();
            for i in 0..n {
                let key = (i % (n / 2)) as i64;
                *map.entry(key).or_insert(0) += 1;
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&map);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Entry API", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

// ============================================================================
// Set Benchmarks
// ============================================================================

fn bench_set_insert(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown HashSet
        {
            let mut set: HashSet<i64> = HashSet::new();
            let start = Instant::now();
            for i in 0..n {
                set.insert(i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&set);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashSet
        {
            let mut set: StdHashSet<i64> = StdHashSet::new();
            let start = Instant::now();
            for i in 0..n {
                set.insert(i as i64);
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(&set);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Set insert", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_set_contains(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup sets
    let mut hb_set: HashSet<i64> = HashSet::with_capacity(n);
    let mut std_set: StdHashSet<i64> = StdHashSet::with_capacity(n);
    for i in 0..n {
        hb_set.insert(i as i64);
        std_set.insert(i as i64);
    }

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let mut count: usize = 0;
            let start = Instant::now();
            for i in 0..(n * 2) {
                if hb_set.contains(&(i as i64)) {
                    count += 1;
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(count);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashSet
        {
            let mut count: usize = 0;
            let start = Instant::now();
            for i in 0..(n * 2) {
                if std_set.contains(&(i as i64)) {
                    count += 1;
                }
            }
            let elapsed = start.elapsed().as_nanos();
            black_box(count);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Set contains", hb_stats.ns_per_op(n * 2), std_stats.ns_per_op(n * 2));
}

fn bench_set_union(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup sets with 50% overlap
    let mut hb_set_a: HashSet<i64> = HashSet::with_capacity(n);
    let mut hb_set_b: HashSet<i64> = HashSet::with_capacity(n);
    let mut std_set_a: StdHashSet<i64> = StdHashSet::with_capacity(n);
    let mut std_set_b: StdHashSet<i64> = StdHashSet::with_capacity(n);

    for i in 0..n {
        hb_set_a.insert(i as i64);
        hb_set_b.insert((i + n / 2) as i64);
        std_set_a.insert(i as i64);
        std_set_b.insert((i + n / 2) as i64);
    }

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let start = Instant::now();
            let result: HashSet<i64> = hb_set_a.union(&hb_set_b).copied().collect();
            let elapsed = start.elapsed().as_nanos();
            black_box(&result);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashSet
        {
            let start = Instant::now();
            let result: StdHashSet<i64> = std_set_a.union(&std_set_b).copied().collect();
            let elapsed = start.elapsed().as_nanos();
            black_box(&result);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Set union", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

fn bench_set_intersection(n: usize) {
    let mut hb_stats = Stats::new();
    let mut std_stats = Stats::new();

    // Setup sets with 50% overlap
    let mut hb_set_a: HashSet<i64> = HashSet::with_capacity(n);
    let mut hb_set_b: HashSet<i64> = HashSet::with_capacity(n);
    let mut std_set_a: StdHashSet<i64> = StdHashSet::with_capacity(n);
    let mut std_set_b: StdHashSet<i64> = StdHashSet::with_capacity(n);

    for i in 0..n {
        hb_set_a.insert(i as i64);
        hb_set_b.insert((i + n / 2) as i64);
        std_set_a.insert(i as i64);
        std_set_b.insert((i + n / 2) as i64);
    }

    for iter in 0..(WARMUP_ITERATIONS + ITERATIONS) {
        // hashbrown
        {
            let start = Instant::now();
            let result: HashSet<i64> = hb_set_a.intersection(&hb_set_b).copied().collect();
            let elapsed = start.elapsed().as_nanos();
            black_box(&result);
            if iter >= WARMUP_ITERATIONS {
                hb_stats.add(elapsed);
            }
        }

        // std HashSet
        {
            let start = Instant::now();
            let result: StdHashSet<i64> = std_set_a.intersection(&std_set_b).copied().collect();
            let elapsed = start.elapsed().as_nanos();
            black_box(&result);
            if iter >= WARMUP_ITERATIONS {
                std_stats.add(elapsed);
            }
        }
    }

    print_result("Set intersect", hb_stats.ns_per_op(n), std_stats.ns_per_op(n));
}

// ============================================================================
// Memory Usage
// ============================================================================

fn bench_memory_usage(n: usize) {
    // Note: Rust doesn't expose internal capacity details as easily
    // This is an approximation based on known load factors

    // hashbrown: 87.5% load factor, 1 byte control per slot
    let hb_capacity = (n as f64 / 0.875).ceil() as usize;
    let hb_entry_size = std::mem::size_of::<(i64, i64)>();
    let hb_ctrl_size = hb_capacity + 16; // GROUP_WIDTH padding
    let hb_mem = hb_ctrl_size + hb_capacity * hb_entry_size;

    // std HashMap: ~90% load factor (varies), different layout
    let std_capacity = (n as f64 / 0.9).ceil() as usize;
    let std_entry_size = std::mem::size_of::<(i64, i64)>() + 8; // key+value+hash
    let std_mem = std_capacity * std_entry_size;

    let hb_bytes_per_entry = hb_mem as f64 / n as f64;
    let std_bytes_per_entry = std_mem as f64 / n as f64;

    print!(
        "  Memory:      hb    {:>6.1} B/entry   std {:>6.1} B/entry   ",
        hb_bytes_per_entry, std_bytes_per_entry
    );
    if hb_bytes_per_entry < std_bytes_per_entry {
        println!(
            "{:.0}% smaller",
            (1.0 - hb_bytes_per_entry / std_bytes_per_entry) * 100.0
        );
    } else {
        println!(
            "{:.0}% larger",
            (hb_bytes_per_entry / std_bytes_per_entry - 1.0) * 100.0
        );
    }
}

// ============================================================================
// Output helpers
// ============================================================================

fn print_result(name: &str, hb_ns: f64, std_ns: f64) {
    let speedup = std_ns / hb_ns;
    let indicator = if speedup >= 1.0 { "+" } else { "-" };
    println!(
        "  {:<14} hb    {:>8.1}ns/op   std {:>8.1}ns/op   {}{:.2}x",
        name, hb_ns, std_ns, indicator, speedup
    );
}
