package galleon

import (
	"fmt"
	"testing"
	"time"
)

// Comprehensive benchmark comparing Zig SIMD (via CGO) vs Native Go
// Run with: go test -bench=Comparison -benchtime=5s -count=3

const benchSize = 1_000_000

func makeTestData(n int) []float64 {
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(i%1000) + 0.5
	}
	return data
}

// ============================================================================
// Sum Benchmarks
// ============================================================================

func BenchmarkComparison_Sum_ZigSIMD(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SumF64(data)
	}
}

func BenchmarkComparison_Sum_GoNative(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum float64
		for _, v := range data {
			sum += v
		}
		_ = sum
	}
}

// ============================================================================
// Min Benchmarks
// ============================================================================

func BenchmarkComparison_Min_ZigSIMD(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MinF64(data)
	}
}

func BenchmarkComparison_Min_GoNative(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		min := data[0]
		for _, v := range data[1:] {
			if v < min {
				min = v
			}
		}
		_ = min
	}
}

// ============================================================================
// Max Benchmarks
// ============================================================================

func BenchmarkComparison_Max_ZigSIMD(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MaxF64(data)
	}
}

func BenchmarkComparison_Max_GoNative(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		max := data[0]
		for _, v := range data[1:] {
			if v > max {
				max = v
			}
		}
		_ = max
	}
}

// ============================================================================
// Mean Benchmarks
// ============================================================================

func BenchmarkComparison_Mean_ZigSIMD(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MeanF64(data)
	}
}

func BenchmarkComparison_Mean_GoNative(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum float64
		for _, v := range data {
			sum += v
		}
		_ = sum / float64(len(data))
	}
}

// ============================================================================
// Filter Benchmarks
// ============================================================================

func BenchmarkComparison_Filter_ZigSIMD(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FilterGreaterThanF64(data, 500.0)
	}
}

func BenchmarkComparison_Filter_GoNative(b *testing.B) {
	data := makeTestData(benchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		indices := make([]uint32, 0, len(data)/2)
		for j, v := range data {
			if v > 500.0 {
				indices = append(indices, uint32(j))
			}
		}
		_ = indices
	}
}

// ============================================================================
// CGO Overhead Test - measure raw call overhead
// ============================================================================

func BenchmarkCGO_Overhead_TinySlice(b *testing.B) {
	// Tiny slice to measure pure CGO call overhead
	data := []float64{1.0, 2.0, 3.0, 4.0}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SumF64(data)
	}
}

func BenchmarkCGO_Overhead_GoTinySlice(b *testing.B) {
	data := []float64{1.0, 2.0, 3.0, 4.0}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum float64
		for _, v := range data {
			sum += v
		}
		_ = sum
	}
}

// ============================================================================
// Print comparison report
// ============================================================================

func TestPrintComparisonReport(t *testing.T) {
	sizes := []int{1_000, 10_000, 100_000, 1_000_000, 10_000_000}
	iterations := 100

	fmt.Println("\n=== Galleon Go+Zig vs Native Go Performance ===")
	fmt.Println("(Also showing Polars reference times where available)")
	fmt.Println()

	for _, size := range sizes {
		data := makeTestData(size)

		// Warm up
		_ = SumF64(data)

		// Sum benchmark
		start := time.Now()
		for i := 0; i < iterations; i++ {
			_ = SumF64(data)
		}
		zigSumTime := time.Since(start) / time.Duration(iterations)

		start = time.Now()
		for i := 0; i < iterations; i++ {
			var sum float64
			for _, v := range data {
				sum += v
			}
			_ = sum
		}
		goSumTime := time.Since(start) / time.Duration(iterations)

		// Min benchmark
		start = time.Now()
		for i := 0; i < iterations; i++ {
			_ = MinF64(data)
		}
		zigMinTime := time.Since(start) / time.Duration(iterations)

		start = time.Now()
		for i := 0; i < iterations; i++ {
			min := data[0]
			for _, v := range data[1:] {
				if v < min {
					min = v
				}
			}
			_ = min
		}
		goMinTime := time.Since(start) / time.Duration(iterations)

		fmt.Printf("Size: %d elements\n", size)
		fmt.Printf("  Sum:  Zig+CGO=%v  Go=%v  (%.1fx)\n",
			zigSumTime, goSumTime, float64(goSumTime)/float64(zigSumTime))
		fmt.Printf("  Min:  Zig+CGO=%v  Go=%v  (%.1fx)\n",
			zigMinTime, goMinTime, float64(goMinTime)/float64(zigMinTime))
		fmt.Println()
	}

	// Reference: Polars times for 1M elements
	fmt.Println("=== Reference: Polars (1M elements) ===")
	fmt.Println("  Sum:  ~0.09ms")
	fmt.Println("  Sort: ~20ms")
	fmt.Println()
	fmt.Println("=== Reference: Pandas (1M elements) ===")
	fmt.Println("  Sum:  ~0.24ms")
	fmt.Println("  Sort: ~97ms")
}
