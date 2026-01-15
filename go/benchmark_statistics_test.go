package galleon

import (
	"math/rand"
	"testing"
)

// ============================================================================
// Statistics Benchmarks - Compare Galleon with baseline Go implementations
// Run with: go test -tags dev -bench=BenchmarkStats -benchmem
// ============================================================================

func makeStatsBenchData(n int) []float64 {
	data := make([]float64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.NormFloat64() * 100 // Normal distribution
	}
	return data
}

// ============================================================================
// Median Benchmarks
// ============================================================================

func BenchmarkStats_Median_10K(b *testing.B) {
	data := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MedianF64(data)
	}
}

func BenchmarkStats_Median_100K(b *testing.B) {
	data := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MedianF64(data)
	}
}

func BenchmarkStats_Median_1M(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MedianF64(data)
	}
}

// ============================================================================
// Quantile Benchmarks
// ============================================================================

func BenchmarkStats_Quantile_10K(b *testing.B) {
	data := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		QuantileF64(data, 0.95)
	}
}

func BenchmarkStats_Quantile_100K(b *testing.B) {
	data := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		QuantileF64(data, 0.95)
	}
}

func BenchmarkStats_Quantile_1M(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		QuantileF64(data, 0.95)
	}
}

// ============================================================================
// Variance Benchmarks
// ============================================================================

func BenchmarkStats_Variance_10K(b *testing.B) {
	data := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarianceF64(data)
	}
}

func BenchmarkStats_Variance_100K(b *testing.B) {
	data := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarianceF64(data)
	}
}

func BenchmarkStats_Variance_1M(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarianceF64(data)
	}
}

// ============================================================================
// StdDev Benchmarks
// ============================================================================

func BenchmarkStats_StdDev_10K(b *testing.B) {
	data := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StdDevF64(data)
	}
}

func BenchmarkStats_StdDev_100K(b *testing.B) {
	data := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StdDevF64(data)
	}
}

func BenchmarkStats_StdDev_1M(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StdDevF64(data)
	}
}

// ============================================================================
// Correlation Benchmarks
// ============================================================================

func BenchmarkStats_Correlation_10K(b *testing.B) {
	x := makeStatsBenchData(10_000)
	y := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CorrelationF64(x, y)
	}
}

func BenchmarkStats_Correlation_100K(b *testing.B) {
	x := makeStatsBenchData(100_000)
	y := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CorrelationF64(x, y)
	}
}

func BenchmarkStats_Correlation_1M(b *testing.B) {
	x := makeStatsBenchData(1_000_000)
	y := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CorrelationF64(x, y)
	}
}

// ============================================================================
// Skewness Benchmarks
// ============================================================================

func BenchmarkStats_Skewness_10K(b *testing.B) {
	data := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SkewnessF64(data)
	}
}

func BenchmarkStats_Skewness_100K(b *testing.B) {
	data := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SkewnessF64(data)
	}
}

func BenchmarkStats_Skewness_1M(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SkewnessF64(data)
	}
}

// ============================================================================
// Kurtosis Benchmarks
// ============================================================================

func BenchmarkStats_Kurtosis_10K(b *testing.B) {
	data := makeStatsBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		KurtosisF64(data)
	}
}

func BenchmarkStats_Kurtosis_100K(b *testing.B) {
	data := makeStatsBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		KurtosisF64(data)
	}
}

func BenchmarkStats_Kurtosis_1M(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		KurtosisF64(data)
	}
}
