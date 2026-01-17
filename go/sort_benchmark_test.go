package galleon

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Benchmark configurations
var benchSizes = []int{1_000, 10_000, 100_000, 1_000_000}

// ============================================================================
// Random Data Benchmarks
// ============================================================================

func BenchmarkSortRandomF64(b *testing.B) {
	for _, size := range benchSizes {
		data := makeRandomF64(size)

		b.Run(fmt.Sprintf("Argsort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ArgsortF64(data, true)
			}
		})

		b.Run(fmt.Sprintf("Sort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SortF64(data, true)
			}
		})
	}
}

func BenchmarkSortRandomI64(b *testing.B) {
	for _, size := range benchSizes {
		data := makeRandomI64(size)

		b.Run(fmt.Sprintf("Argsort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ArgsortI64(data, true)
			}
		})

		b.Run(fmt.Sprintf("Sort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SortI64(data, true)
			}
		})
	}
}

// ============================================================================
// Already Sorted Data (Verge Sort Optimization)
// ============================================================================

func BenchmarkSortAlreadySortedF64(b *testing.B) {
	for _, size := range benchSizes {
		data := makeSortedF64(size)

		b.Run(fmt.Sprintf("Argsort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ArgsortF64(data, true)
			}
		})

		b.Run(fmt.Sprintf("Sort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SortF64(data, true)
			}
		})
	}
}

// ============================================================================
// Reverse Sorted Data
// ============================================================================

func BenchmarkSortReverseSortedF64(b *testing.B) {
	for _, size := range benchSizes {
		data := makeReverseSortedF64(size)

		b.Run(fmt.Sprintf("Argsort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ArgsortF64(data, true)
			}
		})

		b.Run(fmt.Sprintf("Sort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SortF64(data, true)
			}
		})
	}
}

// ============================================================================
// Nearly Sorted Data (few elements out of place)
// ============================================================================

func BenchmarkSortNearlySortedF64(b *testing.B) {
	for _, size := range benchSizes {
		data := makeNearlySortedF64(size, 0.01) // 1% elements swapped

		b.Run(fmt.Sprintf("Argsort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ArgsortF64(data, true)
			}
		})

		b.Run(fmt.Sprintf("Sort_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SortF64(data, true)
			}
		})
	}
}

// ============================================================================
// Comparative Summary Test
// ============================================================================

func TestSortPerformanceComparison(t *testing.T) {
	sizes := []int{10_000, 100_000, 1_000_000}
	patterns := []struct {
		name string
		gen  func(int) []float64
	}{
		{"Random", makeRandomF64},
		{"Sorted", makeSortedF64},
		{"Reverse", makeReverseSortedF64},
		{"Nearly Sorted (1%)", func(n int) []float64 { return makeNearlySortedF64(n, 0.01) }},
	}

	fmt.Println("\n" + "=" + string(make([]byte, 78)) + "=")
	fmt.Println("SORT PERFORMANCE COMPARISON: Argsort vs Sort (Direct)")
	fmt.Println("=" + string(make([]byte, 78)) + "=")
	fmt.Printf("\n%-20s %10s %15s %15s %10s\n", "Pattern", "Size", "Argsort (ms)", "Sort (ms)", "Speedup")
	fmt.Println(string(make([]byte, 75)))

	for _, pattern := range patterns {
		for _, size := range sizes {
			data := pattern.gen(size)

			// Warm up
			ArgsortF64(data, true)
			SortF64(data, true)

			// Benchmark argsort
			iterations := getIterations(size)
			start := time.Now()
			for i := 0; i < iterations; i++ {
				ArgsortF64(data, true)
			}
			argsortDuration := time.Since(start) / time.Duration(iterations)

			// Benchmark sort (direct)
			start = time.Now()
			for i := 0; i < iterations; i++ {
				SortF64(data, true)
			}
			sortDuration := time.Since(start) / time.Duration(iterations)

			speedup := float64(argsortDuration) / float64(sortDuration)
			fmt.Printf("%-20s %10d %15.3f %15.3f %9.2fx\n",
				pattern.name,
				size,
				float64(argsortDuration.Microseconds())/1000.0,
				float64(sortDuration.Microseconds())/1000.0,
				speedup,
			)
		}
	}
	fmt.Println()
}

func TestSortCorrectness(t *testing.T) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("RandomF64_%d", size), func(t *testing.T) {
			data := makeRandomF64(size)
			indices := ArgsortF64(data, true)

			// Verify sorted order
			for i := 0; i < len(indices)-1; i++ {
				if data[indices[i]] > data[indices[i+1]] {
					t.Errorf("Not sorted at position %d: %f > %f", i, data[indices[i]], data[indices[i+1]])
				}
			}
		})

		t.Run(fmt.Sprintf("RandomI64_%d", size), func(t *testing.T) {
			data := makeRandomI64(size)
			indices := ArgsortI64(data, true)

			// Verify sorted order
			for i := 0; i < len(indices)-1; i++ {
				if data[indices[i]] > data[indices[i+1]] {
					t.Errorf("Not sorted at position %d: %d > %d", i, data[indices[i]], data[indices[i+1]])
				}
			}
		})

		t.Run(fmt.Sprintf("SortF64_%d", size), func(t *testing.T) {
			data := makeRandomF64(size)
			sorted := SortF64(data, true)

			// Verify sorted order
			for i := 0; i < len(sorted)-1; i++ {
				if sorted[i] > sorted[i+1] {
					t.Errorf("Not sorted at position %d: %f > %f", i, sorted[i], sorted[i+1])
				}
			}
		})

		t.Run(fmt.Sprintf("SortI64_%d", size), func(t *testing.T) {
			data := makeRandomI64(size)
			sorted := SortI64(data, true)

			// Verify sorted order
			for i := 0; i < len(sorted)-1; i++ {
				if sorted[i] > sorted[i+1] {
					t.Errorf("Not sorted at position %d: %d > %d", i, sorted[i], sorted[i+1])
				}
			}
		})

		t.Run(fmt.Sprintf("SortedF64_%d", size), func(t *testing.T) {
			data := makeSortedF64(size)
			indices := ArgsortF64(data, true)

			// Verify sorted order
			for i := 0; i < len(indices)-1; i++ {
				if data[indices[i]] > data[indices[i+1]] {
					t.Errorf("Not sorted at position %d: %f > %f", i, data[indices[i]], data[indices[i+1]])
				}
			}
		})
	}
}

// ============================================================================
// Helper Functions
// ============================================================================

func makeRandomF64(n int) []float64 {
	rng := rand.New(rand.NewSource(42))
	data := make([]float64, n)
	for i := range data {
		data[i] = rng.Float64()*1000 - 500
	}
	return data
}

func makeRandomI64(n int) []int64 {
	rng := rand.New(rand.NewSource(42))
	data := make([]int64, n)
	for i := range data {
		data[i] = rng.Int63n(1000000) - 500000
	}
	return data
}

func makeSortedF64(n int) []float64 {
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(i)
	}
	return data
}

func makeReverseSortedF64(n int) []float64 {
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(n - i)
	}
	return data
}

func makeNearlySortedF64(n int, swapFraction float64) []float64 {
	data := makeSortedF64(n)
	rng := rand.New(rand.NewSource(42))

	// Swap a fraction of elements
	swaps := int(float64(n) * swapFraction)
	for i := 0; i < swaps; i++ {
		a := rng.Intn(n)
		b := rng.Intn(n)
		data[a], data[b] = data[b], data[a]
	}
	return data
}

func getIterations(size int) int {
	switch {
	case size >= 1_000_000:
		return 5
	case size >= 100_000:
		return 20
	case size >= 10_000:
		return 100
	default:
		return 500
	}
}
