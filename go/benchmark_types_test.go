package galleon

import (
	"math/rand"
	"testing"
)

// ============================================================================
// Multi-Type Benchmarks - Compare F64, F32, I64, I32 performance
// Run with: go test -tags dev -bench=BenchmarkTypes -benchmem
// ============================================================================

func makeF64BenchData(n int) []float64 {
	data := make([]float64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.Float64() * 1000
	}
	return data
}

func makeF32BenchData(n int) []float32 {
	data := make([]float32, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = float32(r.Float64() * 1000)
	}
	return data
}

func makeI64BenchData(n int) []int64 {
	data := make([]int64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.Int63n(1000000)
	}
	return data
}

func makeI32BenchData(n int) []int32 {
	data := make([]int32, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = int32(r.Int31n(1000000))
	}
	return data
}

// ============================================================================
// Sum Benchmarks - All Types
// ============================================================================

func BenchmarkTypes_Sum_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
	b.SetBytes(int64(len(data) * 8))
}

func BenchmarkTypes_Sum_F32_1M(b *testing.B) {
	data := makeF32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF32(data)
	}
	b.SetBytes(int64(len(data) * 4))
}

func BenchmarkTypes_Sum_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumI64(data)
	}
	b.SetBytes(int64(len(data) * 8))
}

func BenchmarkTypes_Sum_I32_1M(b *testing.B) {
	data := makeI32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumI32(data)
	}
	b.SetBytes(int64(len(data) * 4))
}

// ============================================================================
// Min Benchmarks - All Types
// ============================================================================

func BenchmarkTypes_Min_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinF64(data)
	}
	b.SetBytes(int64(len(data) * 8))
}

func BenchmarkTypes_Min_F32_1M(b *testing.B) {
	data := makeF32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinF32(data)
	}
	b.SetBytes(int64(len(data) * 4))
}

func BenchmarkTypes_Min_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinI64(data)
	}
	b.SetBytes(int64(len(data) * 8))
}

func BenchmarkTypes_Min_I32_1M(b *testing.B) {
	data := makeI32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinI32(data)
	}
	b.SetBytes(int64(len(data) * 4))
}

// ============================================================================
// Max Benchmarks - All Types
// ============================================================================

func BenchmarkTypes_Max_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxF64(data)
	}
	b.SetBytes(int64(len(data) * 8))
}

func BenchmarkTypes_Max_F32_1M(b *testing.B) {
	data := makeF32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxF32(data)
	}
	b.SetBytes(int64(len(data) * 4))
}

func BenchmarkTypes_Max_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxI64(data)
	}
	b.SetBytes(int64(len(data) * 8))
}

func BenchmarkTypes_Max_I32_1M(b *testing.B) {
	data := makeI32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxI32(data)
	}
	b.SetBytes(int64(len(data) * 4))
}

// ============================================================================
// Argsort Benchmarks - All Types
// ============================================================================

func BenchmarkTypes_Argsort_F64_100K(b *testing.B) {
	data := makeF64BenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortF64(data, true)
	}
}

func BenchmarkTypes_Argsort_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortF64(data, true)
	}
}

func BenchmarkTypes_Argsort_F32_1M(b *testing.B) {
	data := makeF32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortF32(data, true)
	}
}

func BenchmarkTypes_Argsort_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortI64(data, true)
	}
}

func BenchmarkTypes_Argsort_I32_1M(b *testing.B) {
	data := makeI32BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortI32(data, true)
	}
}

// ============================================================================
// Series Benchmarks - Generic API Performance
// ============================================================================

func BenchmarkSeries_Sum_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	s := NewSeriesF64("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Sum()
	}
}

func BenchmarkSeries_Sum_F32_1M(b *testing.B) {
	data := makeF32BenchData(1_000_000)
	s := NewSeriesF32("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Sum()
	}
}

func BenchmarkSeries_Sum_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	s := NewSeriesI64("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Sum()
	}
}

func BenchmarkSeries_Sum_I32_1M(b *testing.B) {
	data := makeI32BenchData(1_000_000)
	s := NewSeriesI32("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Sum()
	}
}

// ============================================================================
// Series Sort Benchmarks
// ============================================================================

func BenchmarkSeries_Sort_F64_100K(b *testing.B) {
	data := makeF64BenchData(100_000)
	s := NewSeriesF64("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := s.Sort(true)
		sorted.Release()
	}
}

func BenchmarkSeries_Sort_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	s := NewSeriesF64("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := s.Sort(true)
		sorted.Release()
	}
}

func BenchmarkSeries_Sort_F32_1M(b *testing.B) {
	data := makeF32BenchData(1_000_000)
	s := NewSeriesF32("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := s.Sort(true)
		sorted.Release()
	}
}

func BenchmarkSeries_Sort_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	s := NewSeriesI64("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := s.Sort(true)
		sorted.Release()
	}
}

func BenchmarkSeries_Sort_I32_1M(b *testing.B) {
	data := makeI32BenchData(1_000_000)
	s := NewSeriesI32("test", data)
	defer s.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := s.Sort(true)
		sorted.Release()
	}
}

// ============================================================================
// GroupBy Benchmarks
// ============================================================================

func BenchmarkGroupBy_ComputeIDs_10K(b *testing.B) {
	hashes := make([]uint64, 10_000)
	for i := range hashes {
		hashes[i] = uint64(i % 100)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeGroupIDs(hashes)
	}
}

func BenchmarkGroupBy_ComputeIDs_100K(b *testing.B) {
	hashes := make([]uint64, 100_000)
	for i := range hashes {
		hashes[i] = uint64(i % 1000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeGroupIDs(hashes)
	}
}

func BenchmarkGroupBy_ComputeIDs_1M(b *testing.B) {
	hashes := make([]uint64, 1_000_000)
	for i := range hashes {
		hashes[i] = uint64(i % 10000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeGroupIDs(hashes)
	}
}

func BenchmarkGroupBy_SumF64_1M(b *testing.B) {
	groupIDs := make([]uint32, 1_000_000)
	values := makeF64BenchData(1_000_000)
	for i := range groupIDs {
		groupIDs[i] = uint32(i % 1000)
	}
	out := make([]float64, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AggregateSumF64ByGroup(values, groupIDs, out)
	}
}

func BenchmarkGroupBy_SumI64_1M(b *testing.B) {
	groupIDs := make([]uint32, 1_000_000)
	values := makeI64BenchData(1_000_000)
	for i := range groupIDs {
		groupIDs[i] = uint32(i % 1000)
	}
	out := make([]int64, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AggregateSumI64ByGroup(values, groupIDs, out)
	}
}

// ============================================================================
// DataFrame GroupBy End-to-End Benchmarks
// ============================================================================

func BenchmarkDataFrame_GroupBy_Sum_10K(b *testing.B) {
	keys := make([]int64, 10_000)
	values := make([]float64, 10_000)
	for i := range keys {
		keys[i] = int64(i % 100)
		values[i] = float64(i)
	}
	keySeries := NewSeriesI64("key", keys)
	defer keySeries.Release()
	valueSeries := NewSeriesF64("value", values)
	defer valueSeries.Release()
	df := FromColumns(keySeries, valueSeries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = df.GroupBy("key").Sum("value")
	}
}

func BenchmarkDataFrame_GroupBy_Sum_100K(b *testing.B) {
	keys := make([]int64, 100_000)
	values := make([]float64, 100_000)
	for i := range keys {
		keys[i] = int64(i % 1000)
		values[i] = float64(i)
	}
	keySeries := NewSeriesI64("key", keys)
	defer keySeries.Release()
	valueSeries := NewSeriesF64("value", values)
	defer valueSeries.Release()
	df := FromColumns(keySeries, valueSeries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = df.GroupBy("key").Sum("value")
	}
}

func BenchmarkDataFrame_GroupBy_Sum_1M(b *testing.B) {
	keys := make([]int64, 1_000_000)
	values := make([]float64, 1_000_000)
	for i := range keys {
		keys[i] = int64(i % 10000)
		values[i] = float64(i)
	}
	keySeries := NewSeriesI64("key", keys)
	defer keySeries.Release()
	valueSeries := NewSeriesF64("value", values)
	defer valueSeries.Release()
	df := FromColumns(keySeries, valueSeries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = df.GroupBy("key").Sum("value")
	}
}

// ============================================================================
// Comparison Benchmarks (element-wise)
// ============================================================================

func BenchmarkTypes_CmpGt_F64_1M(b *testing.B) {
	a := makeF64BenchData(1_000_000)
	c := makeF64BenchData(1_000_000)
	out := make([]byte, len(a))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CmpGtF64(a, c, out)
	}
}

// ============================================================================
// Vector Arithmetic Benchmarks
// ============================================================================

func BenchmarkTypes_Add_F64_1M(b *testing.B) {
	a := makeF64BenchData(1_000_000)
	c := makeF64BenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AddF64(a, c, out)
	}
	b.SetBytes(int64(len(a) * 8 * 3))
}

func BenchmarkTypes_Add_I64_1M(b *testing.B) {
	a := makeI64BenchData(1_000_000)
	c := makeI64BenchData(1_000_000)
	out := make([]int64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AddI64(a, c, out)
	}
	b.SetBytes(int64(len(a) * 8 * 3))
}

func BenchmarkTypes_Mul_F64_1M(b *testing.B) {
	a := makeF64BenchData(1_000_000)
	c := makeF64BenchData(1_000_000)
	out := make([]float64, 1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MulF64(a, c, out)
	}
	b.SetBytes(int64(len(a) * 8 * 3))
}

// ============================================================================
// Filter Benchmarks
// ============================================================================

func BenchmarkTypes_Filter_F64_1M(b *testing.B) {
	data := makeF64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FilterGreaterThanF64(data, 500.0)
	}
}

func BenchmarkTypes_Filter_I64_1M(b *testing.B) {
	data := makeI64BenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FilterGreaterThanI64(data, 500000)
	}
}

// ============================================================================
// Size Comparison Benchmarks - Same operation, different sizes
// ============================================================================

func BenchmarkTypes_Sum_F64_10K(b *testing.B) {
	data := makeF64BenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}

func BenchmarkTypes_Sum_F64_100K(b *testing.B) {
	data := makeF64BenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}

func BenchmarkTypes_Sum_F64_10M(b *testing.B) {
	data := makeF64BenchData(10_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}
