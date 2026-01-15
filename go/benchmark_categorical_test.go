package galleon

import (
	"fmt"
	"math/rand"
	"testing"
)

// ============================================================================
// Categorical Benchmarks - Compare Categorical vs String operations
// Run with: go test -tags dev -bench=BenchmarkCategorical -benchmem
// ============================================================================

func makeCategoricalBenchStrings(n int, numCategories int) []string {
	categories := make([]string, numCategories)
	for i := 0; i < numCategories; i++ {
		categories[i] = fmt.Sprintf("category_%d", i)
	}

	data := make([]string, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = categories[r.Intn(numCategories)]
	}
	return data
}

// ============================================================================
// Creation Benchmarks - Categorical vs String
// ============================================================================

func BenchmarkCategorical_Create_10K_10Cat(b *testing.B) {
	data := makeCategoricalBenchStrings(10_000, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesCategorical("cat", data)
	}
}

func BenchmarkCategorical_Create_100K_100Cat(b *testing.B) {
	data := makeCategoricalBenchStrings(100_000, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesCategorical("cat", data)
	}
}

func BenchmarkCategorical_Create_1M_1000Cat(b *testing.B) {
	data := makeCategoricalBenchStrings(1_000_000, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesCategorical("cat", data)
	}
}

func BenchmarkString_Create_10K(b *testing.B) {
	data := makeCategoricalBenchStrings(10_000, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesString("str", data)
	}
}

func BenchmarkString_Create_100K(b *testing.B) {
	data := makeCategoricalBenchStrings(100_000, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesString("str", data)
	}
}

func BenchmarkString_Create_1M(b *testing.B) {
	data := makeCategoricalBenchStrings(1_000_000, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewSeriesString("str", data)
	}
}

// ============================================================================
// GroupBy Benchmarks - Categorical vs String keys
// ============================================================================

func BenchmarkCategorical_GroupBy_10K_10Cat(b *testing.B) {
	catData := makeCategoricalBenchStrings(10_000, 10)
	values := make([]float64, 10_000)
	for i := range values {
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesCategorical("group", catData),
		NewSeriesFloat64("value", values),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df.Lazy().
			GroupBy("group").
			Agg(Col("value").Sum().Alias("sum")).
			Collect()
	}
}

func BenchmarkCategorical_GroupBy_100K_100Cat(b *testing.B) {
	catData := makeCategoricalBenchStrings(100_000, 100)
	values := make([]float64, 100_000)
	for i := range values {
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesCategorical("group", catData),
		NewSeriesFloat64("value", values),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df.Lazy().
			GroupBy("group").
			Agg(Col("value").Sum().Alias("sum")).
			Collect()
	}
}

func BenchmarkString_GroupBy_10K_10Cat(b *testing.B) {
	strData := makeCategoricalBenchStrings(10_000, 10)
	values := make([]float64, 10_000)
	for i := range values {
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesString("group", strData),
		NewSeriesFloat64("value", values),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df.Lazy().
			GroupBy("group").
			Agg(Col("value").Sum().Alias("sum")).
			Collect()
	}
}

func BenchmarkString_GroupBy_100K_100Cat(b *testing.B) {
	strData := makeCategoricalBenchStrings(100_000, 100)
	values := make([]float64, 100_000)
	for i := range values {
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesString("group", strData),
		NewSeriesFloat64("value", values),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df.Lazy().
			GroupBy("group").
			Agg(Col("value").Sum().Alias("sum")).
			Collect()
	}
}

// ============================================================================
// Join Benchmarks - Categorical vs String keys
// ============================================================================

func BenchmarkCategorical_Join_10K_10Cat(b *testing.B) {
	catData1 := makeCategoricalBenchStrings(10_000, 10)
	catData2 := makeCategoricalBenchStrings(10_000, 10)
	values1 := make([]float64, 10_000)
	values2 := make([]float64, 10_000)
	for i := range values1 {
		values1[i] = float64(i)
		values2[i] = float64(i * 2)
	}

	df1, _ := NewDataFrame(
		NewSeriesCategorical("key", catData1),
		NewSeriesFloat64("value1", values1),
	)
	df2, _ := NewDataFrame(
		NewSeriesCategorical("key", catData2),
		NewSeriesFloat64("value2", values2),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df1.Lazy().
			Join(df2.Lazy(), On("key")).
			Collect()
	}
}

func BenchmarkString_Join_10K_10Cat(b *testing.B) {
	strData1 := makeCategoricalBenchStrings(10_000, 10)
	strData2 := makeCategoricalBenchStrings(10_000, 10)
	values1 := make([]float64, 10_000)
	values2 := make([]float64, 10_000)
	for i := range values1 {
		values1[i] = float64(i)
		values2[i] = float64(i * 2)
	}

	df1, _ := NewDataFrame(
		NewSeriesString("key", strData1),
		NewSeriesFloat64("value1", values1),
	)
	df2, _ := NewDataFrame(
		NewSeriesString("key", strData2),
		NewSeriesFloat64("value2", values2),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = df1.Lazy().
			Join(df2.Lazy(), On("key")).
			Collect()
	}
}

// ============================================================================
// Memory Benchmarks - Categorical vs String
// ============================================================================

func BenchmarkCategorical_Memory_1M_10Cat(b *testing.B) {
	// Categorical with low cardinality should use much less memory
	data := makeCategoricalBenchStrings(1_000_000, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSeriesCategorical("cat", data)
		_ = s.Len()
	}
}

func BenchmarkString_Memory_1M_10Cat(b *testing.B) {
	// String stores all values separately
	data := makeCategoricalBenchStrings(1_000_000, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := NewSeriesString("str", data)
		_ = s.Len()
	}
}

// ============================================================================
// Conversion Benchmarks
// ============================================================================

func BenchmarkCategorical_AsCategorical_100K(b *testing.B) {
	data := makeCategoricalBenchStrings(100_000, 100)
	s := NewSeriesString("str", data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.AsCategorical()
	}
}

func BenchmarkCategorical_AsString_100K(b *testing.B) {
	data := makeCategoricalBenchStrings(100_000, 100)
	s := NewSeriesCategorical("cat", data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.AsString()
	}
}
