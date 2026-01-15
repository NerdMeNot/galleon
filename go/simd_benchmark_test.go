package galleon

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkSimdLevelComparison compares performance across different SIMD levels
func BenchmarkSimdLevelComparison(b *testing.B) {
	// Print current system info
	config := GetSimdConfig()
	b.Logf("Native SIMD level: %s (%d-bit vectors)", config.LevelName, config.VectorBytes*8)

	sizes := []int{1000, 10000, 100000, 1000000}
	levels := []SimdLevel{SimdScalar, SimdSSE4, SimdAVX2, SimdAVX512}

	for _, size := range sizes {
		// Generate test data
		data := make([]float64, size)
		data2 := make([]float64, size)
		for i := range data {
			data[i] = rand.Float64() * 1000
			data2[i] = rand.Float64() * 1000
		}

		for _, level := range levels {
			levelName := level.String()

			// Sum benchmark
			b.Run(fmt.Sprintf("Sum/%s/n=%d", levelName, size), func(b *testing.B) {
				SetSimdLevel(level)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = SumF64(data)
				}
			})

			// Min benchmark
			b.Run(fmt.Sprintf("Min/%s/n=%d", levelName, size), func(b *testing.B) {
				SetSimdLevel(level)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = MinF64(data)
				}
			})

			// Max benchmark
			b.Run(fmt.Sprintf("Max/%s/n=%d", levelName, size), func(b *testing.B) {
				SetSimdLevel(level)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = MaxF64(data)
				}
			})

			// Vector Add benchmark
			b.Run(fmt.Sprintf("VecAdd/%s/n=%d", levelName, size), func(b *testing.B) {
				SetSimdLevel(level)
				out := make([]float64, size)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					AddF64(data, data2, out)
				}
			})

			// Vector Mul benchmark
			b.Run(fmt.Sprintf("VecMul/%s/n=%d", levelName, size), func(b *testing.B) {
				SetSimdLevel(level)
				out := make([]float64, size)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					MulF64(data, data2, out)
				}
			})
		}
	}

	// Restore native level
	SetSimdLevel(config.Level)
}

// BenchmarkSimdSumComparison focuses on sum operation comparison
func BenchmarkSimdSumComparison(b *testing.B) {
	config := GetSimdConfig()
	b.Logf("Native SIMD level: %s", config.LevelName)

	size := 1000000
	data := make([]float64, size)
	for i := range data {
		data[i] = rand.Float64() * 1000
	}

	levels := []SimdLevel{SimdScalar, SimdSSE4, SimdAVX2, SimdAVX512}

	for _, level := range levels {
		b.Run(level.String(), func(b *testing.B) {
			SetSimdLevel(level)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = SumF64(data)
			}
			b.SetBytes(int64(size * 8)) // 8 bytes per float64
		})
	}

	SetSimdLevel(config.Level)
}

// BenchmarkSimdMinMaxComparison focuses on min/max comparison
func BenchmarkSimdMinMaxComparison(b *testing.B) {
	config := GetSimdConfig()

	size := 1000000
	data := make([]float64, size)
	for i := range data {
		data[i] = rand.Float64() * 1000
	}

	levels := []SimdLevel{SimdScalar, SimdSSE4, SimdAVX2, SimdAVX512}

	for _, level := range levels {
		b.Run(fmt.Sprintf("Min/%s", level.String()), func(b *testing.B) {
			SetSimdLevel(level)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = MinF64(data)
			}
			b.SetBytes(int64(size * 8))
		})

		b.Run(fmt.Sprintf("Max/%s", level.String()), func(b *testing.B) {
			SetSimdLevel(level)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = MaxF64(data)
			}
			b.SetBytes(int64(size * 8))
		})
	}

	SetSimdLevel(config.Level)
}

// BenchmarkSimdArithmeticComparison focuses on vector arithmetic
func BenchmarkSimdArithmeticComparison(b *testing.B) {
	config := GetSimdConfig()

	size := 1000000
	a := make([]float64, size)
	bb := make([]float64, size)
	out := make([]float64, size)
	for i := range a {
		a[i] = rand.Float64() * 1000
		bb[i] = rand.Float64() * 1000
	}

	levels := []SimdLevel{SimdScalar, SimdSSE4, SimdAVX2, SimdAVX512}

	for _, level := range levels {
		b.Run(fmt.Sprintf("Add/%s", level.String()), func(b *testing.B) {
			SetSimdLevel(level)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				AddF64(a, bb, out)
			}
			b.SetBytes(int64(size * 8 * 3)) // 3 arrays
		})

		b.Run(fmt.Sprintf("Mul/%s", level.String()), func(b *testing.B) {
			SetSimdLevel(level)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				MulF64(a, bb, out)
			}
			b.SetBytes(int64(size * 8 * 3))
		})

		b.Run(fmt.Sprintf("Div/%s", level.String()), func(b *testing.B) {
			SetSimdLevel(level)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				DivF64(a, bb, out)
			}
			b.SetBytes(int64(size * 8 * 3))
		})
	}

	SetSimdLevel(config.Level)
}

// TestSimdLevelInfo prints SIMD configuration info
func TestSimdLevelInfo(t *testing.T) {
	config := GetSimdConfig()
	t.Logf("=== SIMD Configuration ===")
	t.Logf("Level: %s (enum value: %d)", config.LevelName, config.Level)
	t.Logf("Vector width: %d bytes (%d bits)", config.VectorBytes, config.VectorBytes*8)
	t.Logf("")
	t.Logf("=== Vector Element Counts ===")
	t.Logf("f64 elements per vector: %d", config.VectorBytes/8)
	t.Logf("f32 elements per vector: %d", config.VectorBytes/4)
	t.Logf("i64 elements per vector: %d", config.VectorBytes/8)
	t.Logf("i32 elements per vector: %d", config.VectorBytes/4)
}
