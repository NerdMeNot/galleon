package galleon

import (
	"math"
	"math/rand"
	"testing"
)

// ============================================================================
// Fold/Horizontal Operation Benchmarks
// Run with: go test -tags dev -bench=BenchmarkFold -benchmem
// ============================================================================

func makeFoldBenchData(n int) []float64 {
	data := make([]float64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.Float64() * 1000
	}
	return data
}

func makeFoldBenchDataWithNaN(n int) []float64 {
	data := make([]float64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		if r.Float64() < 0.1 { // 10% NaN
			data[i] = math.NaN()
		} else {
			data[i] = r.Float64() * 1000
		}
	}
	return data
}

// ============================================================================
// Sum Horizontal Benchmarks (2 columns)
// ============================================================================

func BenchmarkFold_SumHorizontal2_10K(b *testing.B) {
	a := makeFoldBenchData(10_000)
	c := makeFoldBenchData(10_000)
	out := make([]float64, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_SumHorizontal2_100K(b *testing.B) {
	a := makeFoldBenchData(100_000)
	c := makeFoldBenchData(100_000)
	out := make([]float64, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_SumHorizontal2_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal2F64(a, c, out)
	}
}

// ============================================================================
// Sum Horizontal Benchmarks (3 columns)
// ============================================================================

func BenchmarkFold_SumHorizontal3_10K(b *testing.B) {
	a := makeFoldBenchData(10_000)
	c := makeFoldBenchData(10_000)
	d := makeFoldBenchData(10_000)
	out := make([]float64, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal3F64(a, c, d, out)
	}
}

func BenchmarkFold_SumHorizontal3_100K(b *testing.B) {
	a := makeFoldBenchData(100_000)
	c := makeFoldBenchData(100_000)
	d := makeFoldBenchData(100_000)
	out := make([]float64, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal3F64(a, c, d, out)
	}
}

func BenchmarkFold_SumHorizontal3_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	d := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal3F64(a, c, d, out)
	}
}

// ============================================================================
// Min Horizontal Benchmarks
// ============================================================================

func BenchmarkFold_MinHorizontal2_10K(b *testing.B) {
	a := makeFoldBenchData(10_000)
	c := makeFoldBenchData(10_000)
	out := make([]float64, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_MinHorizontal2_100K(b *testing.B) {
	a := makeFoldBenchData(100_000)
	c := makeFoldBenchData(100_000)
	out := make([]float64, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_MinHorizontal2_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_MinHorizontal3_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	d := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinHorizontal3F64(a, c, d, out)
	}
}

// ============================================================================
// Max Horizontal Benchmarks
// ============================================================================

func BenchmarkFold_MaxHorizontal2_10K(b *testing.B) {
	a := makeFoldBenchData(10_000)
	c := makeFoldBenchData(10_000)
	out := make([]float64, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_MaxHorizontal2_100K(b *testing.B) {
	a := makeFoldBenchData(100_000)
	c := makeFoldBenchData(100_000)
	out := make([]float64, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_MaxHorizontal2_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_MaxHorizontal3_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	d := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxHorizontal3F64(a, c, d, out)
	}
}

// ============================================================================
// Product Horizontal Benchmarks
// ============================================================================

func BenchmarkFold_ProductHorizontal2_10K(b *testing.B) {
	a := makeFoldBenchData(10_000)
	c := makeFoldBenchData(10_000)
	out := make([]float64, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProductHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_ProductHorizontal2_100K(b *testing.B) {
	a := makeFoldBenchData(100_000)
	c := makeFoldBenchData(100_000)
	out := make([]float64, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProductHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_ProductHorizontal2_1M(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProductHorizontal2F64(a, c, out)
	}
}

// ============================================================================
// Any/All Horizontal Benchmarks
// ============================================================================

func BenchmarkFold_AnyHorizontal2_10K(b *testing.B) {
	a := make([]uint8, 10_000)
	c := make([]uint8, 10_000)
	out := make([]uint8, 10_000)
	for i := range a {
		a[i] = uint8(i % 2)
		c[i] = uint8((i + 1) % 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AnyHorizontal2(a, c, out)
	}
}

func BenchmarkFold_AnyHorizontal2_100K(b *testing.B) {
	a := make([]uint8, 100_000)
	c := make([]uint8, 100_000)
	out := make([]uint8, 100_000)
	for i := range a {
		a[i] = uint8(i % 2)
		c[i] = uint8((i + 1) % 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AnyHorizontal2(a, c, out)
	}
}

func BenchmarkFold_AnyHorizontal2_1M(b *testing.B) {
	a := make([]uint8, 1_000_000)
	c := make([]uint8, 1_000_000)
	out := make([]uint8, 1_000_000)
	for i := range a {
		a[i] = uint8(i % 2)
		c[i] = uint8((i + 1) % 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AnyHorizontal2(a, c, out)
	}
}

func BenchmarkFold_AllHorizontal2_1M(b *testing.B) {
	a := make([]uint8, 1_000_000)
	c := make([]uint8, 1_000_000)
	out := make([]uint8, 1_000_000)
	for i := range a {
		a[i] = uint8(i % 2)
		c[i] = uint8((i + 1) % 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AllHorizontal2(a, c, out)
	}
}

// ============================================================================
// Count Non-Null Horizontal Benchmarks
// ============================================================================

func BenchmarkFold_CountNonNull2_10K(b *testing.B) {
	a := makeFoldBenchDataWithNaN(10_000)
	c := makeFoldBenchDataWithNaN(10_000)
	out := make([]uint32, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountNonNullHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_CountNonNull2_100K(b *testing.B) {
	a := makeFoldBenchDataWithNaN(100_000)
	c := makeFoldBenchDataWithNaN(100_000)
	out := make([]uint32, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountNonNullHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_CountNonNull2_1M(b *testing.B) {
	a := makeFoldBenchDataWithNaN(1_000_000)
	c := makeFoldBenchDataWithNaN(1_000_000)
	out := make([]uint32, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountNonNullHorizontal2F64(a, c, out)
	}
}

func BenchmarkFold_CountNonNull3_1M(b *testing.B) {
	a := makeFoldBenchDataWithNaN(1_000_000)
	c := makeFoldBenchDataWithNaN(1_000_000)
	d := makeFoldBenchDataWithNaN(1_000_000)
	out := make([]uint32, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CountNonNullHorizontal3F64(a, c, d, out)
	}
}
