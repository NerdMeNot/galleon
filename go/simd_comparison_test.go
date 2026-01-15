package galleon

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkGalleonVsOthers runs benchmarks with sizes matching Python comparison
func BenchmarkGalleonVsOthers(b *testing.B) {
	config := GetSimdConfig()
	b.Logf("Galleon SIMD: %s (%d-bit vectors)", config.LevelName, config.VectorBytes*8)

	sizes := []int{100_000, 1_000_000, 10_000_000}

	for _, n := range sizes {
		// Generate test data
		data := make([]float64, n)
		data2 := make([]float64, n)
		rand.Seed(42)
		for i := range data {
			data[i] = rand.NormFloat64()
			data2[i] = rand.NormFloat64()
		}

		b.Run(fmt.Sprintf("Sum/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = SumF64(data)
			}
		})

		b.Run(fmt.Sprintf("Min/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = MinF64(data)
			}
		})

		b.Run(fmt.Sprintf("Max/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = MaxF64(data)
			}
		})

		b.Run(fmt.Sprintf("Mean/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = MeanF64(data)
			}
		})

		out := make([]float64, n)

		b.Run(fmt.Sprintf("Add/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				AddF64(data, data2, out)
			}
		})

		b.Run(fmt.Sprintf("Mul/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MulF64(data, data2, out)
			}
		})

		b.Run(fmt.Sprintf("Div/n=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				DivF64(data, data2, out)
			}
		})
	}
}

// TestPrintComparison prints a formatted comparison table
func TestPrintComparison(t *testing.T) {
	config := GetSimdConfig()

	t.Log("=" + fmt.Sprintf("%78s", "") + "=")
	t.Log("GALLEON BENCHMARK RESULTS")
	t.Logf("SIMD Level: %s (%d-bit vectors)", config.LevelName, config.VectorBytes*8)
	t.Log("=" + fmt.Sprintf("%78s", "") + "=")
	t.Log("")
	t.Log("Run with: go test -tags dev -bench 'BenchmarkGalleonVsOthers' -benchmem")
	t.Log("")
	t.Log("Compare throughput (GB/s) with Python results from compare_all.py")
}
