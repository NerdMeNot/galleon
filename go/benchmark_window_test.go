package galleon

import (
	"math/rand"
	"testing"
)

// ============================================================================
// Window Function Benchmarks
// Run with: go test -tags dev -bench=BenchmarkWindow -benchmem
// ============================================================================

func makeWindowBenchData(n int) []float64 {
	data := make([]float64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.Float64() * 1000
	}
	return data
}

// ============================================================================
// Rolling Sum Benchmarks
// ============================================================================

func BenchmarkWindow_RollingSum_10K_W10(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingSumF64(data, 10, 1, out)
	}
}

func BenchmarkWindow_RollingSum_100K_W100(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingSumF64(data, 100, 1, out)
	}
}

func BenchmarkWindow_RollingSum_1M_W100(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingSumF64(data, 100, 1, out)
	}
}

// ============================================================================
// Rolling Mean Benchmarks
// ============================================================================

func BenchmarkWindow_RollingMean_10K_W10(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMeanF64(data, 10, 1, out)
	}
}

func BenchmarkWindow_RollingMean_100K_W100(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMeanF64(data, 100, 1, out)
	}
}

func BenchmarkWindow_RollingMean_1M_W100(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMeanF64(data, 100, 1, out)
	}
}

// ============================================================================
// Rolling Min Benchmarks
// ============================================================================

func BenchmarkWindow_RollingMin_10K_W10(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMinF64(data, 10, 1, out)
	}
}

func BenchmarkWindow_RollingMin_100K_W100(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMinF64(data, 100, 1, out)
	}
}

func BenchmarkWindow_RollingMin_1M_W100(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMinF64(data, 100, 1, out)
	}
}

// ============================================================================
// Rolling Max Benchmarks
// ============================================================================

func BenchmarkWindow_RollingMax_10K_W10(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMaxF64(data, 10, 1, out)
	}
}

func BenchmarkWindow_RollingMax_100K_W100(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMaxF64(data, 100, 1, out)
	}
}

func BenchmarkWindow_RollingMax_1M_W100(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMaxF64(data, 100, 1, out)
	}
}

// ============================================================================
// Cumulative Sum Benchmarks
// ============================================================================

func BenchmarkWindow_CumSum_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumSumF64(data, out)
	}
}

func BenchmarkWindow_CumSum_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumSumF64(data, out)
	}
}

func BenchmarkWindow_CumSum_1M(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumSumF64(data, out)
	}
}

// ============================================================================
// Cumulative Min Benchmarks
// ============================================================================

func BenchmarkWindow_CumMin_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumMinF64(data, out)
	}
}

func BenchmarkWindow_CumMin_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumMinF64(data, out)
	}
}

func BenchmarkWindow_CumMin_1M(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumMinF64(data, out)
	}
}

// ============================================================================
// Cumulative Max Benchmarks
// ============================================================================

func BenchmarkWindow_CumMax_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumMaxF64(data, out)
	}
}

func BenchmarkWindow_CumMax_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumMaxF64(data, out)
	}
}

func BenchmarkWindow_CumMax_1M(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CumMaxF64(data, out)
	}
}

// ============================================================================
// Lag Benchmarks
// ============================================================================

func BenchmarkWindow_Lag_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LagF64(data, 5, 0.0, out)
	}
}

func BenchmarkWindow_Lag_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LagF64(data, 5, 0.0, out)
	}
}

func BenchmarkWindow_Lag_1M(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LagF64(data, 5, 0.0, out)
	}
}

// ============================================================================
// Lead Benchmarks
// ============================================================================

func BenchmarkWindow_Lead_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LeadF64(data, 5, 0.0, out)
	}
}

func BenchmarkWindow_Lead_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LeadF64(data, 5, 0.0, out)
	}
}

func BenchmarkWindow_Lead_1M(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		LeadF64(data, 5, 0.0, out)
	}
}

// ============================================================================
// Diff Benchmarks
// ============================================================================

func BenchmarkWindow_Diff_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DiffF64(data, 0.0, out)
	}
}

func BenchmarkWindow_Diff_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DiffF64(data, 0.0, out)
	}
}

func BenchmarkWindow_Diff_1M(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DiffF64(data, 0.0, out)
	}
}

// ============================================================================
// Rank Benchmarks
// ============================================================================

func BenchmarkWindow_Rank_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]uint32, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RankF64(data, out)
	}
}

func BenchmarkWindow_Rank_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]uint32, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RankF64(data, out)
	}
}

func BenchmarkWindow_DenseRank_10K(b *testing.B) {
	data := makeWindowBenchData(10_000)
	out := make([]uint32, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DenseRankF64(data, out)
	}
}

func BenchmarkWindow_DenseRank_100K(b *testing.B) {
	data := makeWindowBenchData(100_000)
	out := make([]uint32, len(data))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DenseRankF64(data, out)
	}
}

// ============================================================================
// Row Number Benchmarks
// ============================================================================

func BenchmarkWindow_RowNumber_10K(b *testing.B) {
	out := make([]uint32, 10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RowNumber(out)
	}
}

func BenchmarkWindow_RowNumber_100K(b *testing.B) {
	out := make([]uint32, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RowNumber(out)
	}
}

func BenchmarkWindow_RowNumber_1M(b *testing.B) {
	out := make([]uint32, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RowNumber(out)
	}
}
