package galleon

import (
	"runtime"
	"testing"
)

// ============================================================================
// Resource Consumption Benchmarks
// Measures memory allocations, bytes allocated, and throughput
// Run with: go test -tags dev -bench=BenchmarkResource -benchmem
// ============================================================================

// Helper to get current memory stats
func getMemStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// ============================================================================
// Memory Efficiency Benchmarks - Bytes per element
// ============================================================================

func BenchmarkResource_Sum_1M_Memory(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8)) // 8 bytes per float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}

func BenchmarkResource_Variance_1M_Memory(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VarianceF64(data)
	}
}

func BenchmarkResource_Median_1M_Memory(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MedianF64(data)
	}
}

func BenchmarkResource_Quantile_1M_Memory(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		QuantileF64(data, 0.95)
	}
}

func BenchmarkResource_Argsort_1M_Memory(b *testing.B) {
	data := makeStatsBenchData(1_000_000)
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortF64(data, true)
	}
}

// ============================================================================
// Rolling Window Memory Benchmarks
// ============================================================================

func BenchmarkResource_RollingSum_1M_Memory(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingSumF64(data, 100, 1, out)
	}
}

func BenchmarkResource_RollingMin_1M_Memory(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMinF64(data, 100, 1, out)
	}
}

func BenchmarkResource_RollingMax_1M_Memory(b *testing.B) {
	data := makeWindowBenchData(1_000_000)
	out := make([]float64, len(data))
	b.ReportAllocs()
	b.SetBytes(int64(len(data) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RollingMaxF64(data, 100, 1, out)
	}
}

// ============================================================================
// Horizontal/Fold Memory Benchmarks
// ============================================================================

func BenchmarkResource_SumHorizontal3_1M_Memory(b *testing.B) {
	a := makeFoldBenchData(1_000_000)
	c := makeFoldBenchData(1_000_000)
	d := makeFoldBenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ReportAllocs()
	b.SetBytes(int64(3 * 1_000_000 * 8)) // 3 input columns
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumHorizontal3F64(a, c, d, out)
	}
}

// ============================================================================
// Categorical Memory Benchmarks
// ============================================================================

func BenchmarkResource_Categorical_Create_1M_Memory(b *testing.B) {
	data := makeCategoricalBenchStrings(1_000_000, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesCategorical("cat", data)
	}
}

func BenchmarkResource_Categorical_GroupBy_1M_Memory(b *testing.B) {
	catData := makeCategoricalBenchStrings(1_000_000, 100)
	values := make([]float64, 1_000_000)
	for i := range values {
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesCategorical("group", catData),
		NewSeriesFloat64("value", values),
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df.Lazy().
			GroupBy("group").
			Agg(Col("value").Sum().Alias("sum")).
			Collect()
	}
}

// ============================================================================
// Throughput Benchmarks - GB/s calculations
// ============================================================================

func BenchmarkResource_Throughput_Sum(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000, 10_000_000}
	for _, n := range sizes {
		b.Run(formatSize(n), func(b *testing.B) {
			data := makeStatsBenchData(n)
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				SumF64(data)
			}
		})
	}
}

func BenchmarkResource_Throughput_Filter(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000, 10_000_000}
	for _, n := range sizes {
		b.Run(formatSize(n), func(b *testing.B) {
			data := makeStatsBenchData(n)
			mask := make([]bool, n)
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FilterMaskGreaterThanF64Into(data, 0.0, mask)
			}
		})
	}
}

func BenchmarkResource_Throughput_RollingSum(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}
	for _, n := range sizes {
		b.Run(formatSize(n), func(b *testing.B) {
			data := makeWindowBenchData(n)
			out := make([]float64, n)
			b.SetBytes(int64(n * 8))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				RollingSumF64(data, 100, 1, out)
			}
		})
	}
}

// ============================================================================
// Memory Scaling Benchmarks - How memory grows with size
// ============================================================================

func BenchmarkResource_MemoryScale_Categorical(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}
	numCategories := 100

	for _, n := range sizes {
		b.Run(formatSize(n), func(b *testing.B) {
			data := makeCategoricalBenchStrings(n, numCategories)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NewSeriesCategorical("cat", data)
			}
		})
	}
}

func BenchmarkResource_MemoryScale_String(b *testing.B) {
	sizes := []int{10_000, 100_000, 1_000_000}
	numCategories := 100

	for _, n := range sizes {
		b.Run(formatSize(n), func(b *testing.B) {
			data := makeCategoricalBenchStrings(n, numCategories)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = NewSeriesString("str", data)
			}
		})
	}
}

// Helper to format size for benchmark names
func formatSize(n int) string {
	switch {
	case n >= 1_000_000:
		return formatSizeStr(n/1_000_000) + "M"
	case n >= 1_000:
		return formatSizeStr(n/1_000) + "K"
	default:
		return formatSizeStr(n)
	}
}

func formatSizeStr(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		result = string(rune('0'+n%10)) + result
		n /= 10
	}
	return result
}
