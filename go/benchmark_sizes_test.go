package galleon

import (
	"fmt"
	"testing"
	"time"
)

// TestBenchmarkAcrossSizes runs comprehensive benchmarks across different data sizes
// comparing Go+Zig SIMD against native Go
func TestBenchmarkAcrossSizes(t *testing.T) {
	sizes := []int{
		1_000,
		10_000,
		100_000,
		1_000_000,
		10_000_000,
	}

	iterations := map[int]int{
		1_000:      1000,
		10_000:     500,
		100_000:    200,
		1_000_000:  100,
		10_000_000: 20,
	}

	fmt.Println("\n╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║              GALLEON PERFORMANCE BENCHMARK - Go+Zig vs Native Go             ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")

	for _, size := range sizes {
		data := makeTestDataForBench(size)
		iter := iterations[size]

		// Warm up
		_ = SumF64(data)
		_ = MinF64(data)
		_ = MaxF64(data)

		fmt.Printf("\n┌─────────────────────────────────────────────────────────────────────────────┐\n")
		fmt.Printf("│ Size: %d elements (%d iterations)                                      \n", size, iter)
		fmt.Printf("├─────────────────────────────────────────────────────────────────────────────┤\n")
		fmt.Printf("│ Operation     │ Zig+CGO       │ Native Go     │ Speedup │ GB/s (Zig)      │\n")
		fmt.Printf("├───────────────┼───────────────┼───────────────┼─────────┼─────────────────┤\n")

		// Sum benchmark
		zigTime := benchmarkOp(func() { _ = SumF64(data) }, iter)
		goTime := benchmarkOp(func() {
			var sum float64
			for _, v := range data {
				sum += v
			}
			_ = sum
		}, iter)
		gbps := float64(size*8) / float64(zigTime.Nanoseconds())
		fmt.Printf("│ Sum           │ %13s │ %13s │ %6.1fx │ %6.2f GB/s     │\n",
			formatDuration(zigTime), formatDuration(goTime), float64(goTime)/float64(zigTime), gbps)

		// Min benchmark
		zigTime = benchmarkOp(func() { _ = MinF64(data) }, iter)
		goTime = benchmarkOp(func() {
			min := data[0]
			for _, v := range data[1:] {
				if v < min {
					min = v
				}
			}
			_ = min
		}, iter)
		gbps = float64(size*8) / float64(zigTime.Nanoseconds())
		fmt.Printf("│ Min           │ %13s │ %13s │ %6.1fx │ %6.2f GB/s     │\n",
			formatDuration(zigTime), formatDuration(goTime), float64(goTime)/float64(zigTime), gbps)

		// Max benchmark
		zigTime = benchmarkOp(func() { _ = MaxF64(data) }, iter)
		goTime = benchmarkOp(func() {
			max := data[0]
			for _, v := range data[1:] {
				if v > max {
					max = v
				}
			}
			_ = max
		}, iter)
		gbps = float64(size*8) / float64(zigTime.Nanoseconds())
		fmt.Printf("│ Max           │ %13s │ %13s │ %6.1fx │ %6.2f GB/s     │\n",
			formatDuration(zigTime), formatDuration(goTime), float64(goTime)/float64(zigTime), gbps)

		// Mean benchmark
		zigTime = benchmarkOp(func() { _ = MeanF64(data) }, iter)
		goTime = benchmarkOp(func() {
			var sum float64
			for _, v := range data {
				sum += v
			}
			_ = sum / float64(len(data))
		}, iter)
		gbps = float64(size*8) / float64(zigTime.Nanoseconds())
		fmt.Printf("│ Mean          │ %13s │ %13s │ %6.1fx │ %6.2f GB/s     │\n",
			formatDuration(zigTime), formatDuration(goTime), float64(goTime)/float64(zigTime), gbps)

		// Filter benchmark (only for sizes up to 1M to avoid too long)
		if size <= 1_000_000 {
			// Boolean mask version with allocation
			zigTime = benchmarkOp(func() { _ = FilterMaskGreaterThanF64(data, 500.0) }, iter)
			goTime = benchmarkOp(func() {
				mask := make([]bool, len(data))
				for j, v := range data {
					mask[j] = v > 500.0
				}
				_ = mask
			}, iter)
			gbps = float64(size*8) / float64(zigTime.Nanoseconds())
			fmt.Printf("│ FilterAlloc   │ %13s │ %13s │ %6.1fx │ %6.2f GB/s     │\n",
				formatDuration(zigTime), formatDuration(goTime), float64(goTime)/float64(zigTime), gbps)

			// Boolean mask version with pooling (no allocation after warmup)
			// Warm up the pool
			for i := 0; i < 10; i++ {
				m := FilterMaskGreaterThanF64Pooled(data, 500.0)
				m.Release()
			}
			zigTime = benchmarkOp(func() {
				m := FilterMaskGreaterThanF64Pooled(data, 500.0)
				m.Release()
			}, iter)
			gbps = float64(size*8) / float64(zigTime.Nanoseconds())
			fmt.Printf("│ FilterPooled  │ %13s │      (pooled) │    n/a  │ %6.2f GB/s     │\n",
				formatDuration(zigTime), gbps)

			// Pre-allocated version (zero allocation) - bool
			preallocMask := make([]bool, len(data))
			zigTime = benchmarkOp(func() {
				_ = FilterMaskGreaterThanF64Into(data, 500.0, preallocMask)
			}, iter)
			gbps = float64(size*8) / float64(zigTime.Nanoseconds())
			fmt.Printf("│ FilterBool    │ %13s │  (zero alloc) │    n/a  │ %6.2f GB/s     │\n",
				formatDuration(zigTime), gbps)

			// Pre-allocated version (zero allocation) - u8 (most optimized)
			preallocU8 := make([]byte, len(data))
			zigTime = benchmarkOp(func() {
				_ = FilterMaskU8GreaterThanF64Into(data, 500.0, preallocU8)
			}, iter)
			gbps = float64(size*8) / float64(zigTime.Nanoseconds())
			fmt.Printf("│ FilterU8      │ %13s │  (zero alloc) │    n/a  │ %6.2f GB/s     │\n",
				formatDuration(zigTime), gbps)
		}

		// AddScalar benchmark (in-place operation)
		dataCopy := make([]float64, len(data))
		copy(dataCopy, data)
		zigTime = benchmarkOp(func() { AddScalarF64(dataCopy, 1.0) }, iter)
		copy(dataCopy, data)
		goTime = benchmarkOp(func() {
			for i := range dataCopy {
				dataCopy[i] += 1.0
			}
		}, iter)
		gbps = float64(size*8*2) / float64(zigTime.Nanoseconds()) // read + write
		fmt.Printf("│ AddScalar     │ %13s │ %13s │ %6.1fx │ %6.2f GB/s     │\n",
			formatDuration(zigTime), formatDuration(goTime), float64(goTime)/float64(zigTime), gbps)

		fmt.Printf("└───────────────┴───────────────┴───────────────┴─────────┴─────────────────┘\n")
	}
}

func makeTestDataForBench(n int) []float64 {
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(i%1000) + 0.5
	}
	return data
}

func benchmarkOp(op func(), iterations int) time.Duration {
	start := time.Now()
	for i := 0; i < iterations; i++ {
		op()
	}
	return time.Since(start) / time.Duration(iterations)
}

func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000)
	} else if d < time.Second {
		return fmt.Sprintf("%.3fms", float64(d.Nanoseconds())/1_000_000)
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}
