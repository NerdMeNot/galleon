package galleon

import (
	"math/rand"
	"testing"
)

// ============================================================================
// Sort and Join Benchmarks - For comparison with Polars/Pandas
// Run with: go test -tags dev -bench=BenchmarkSortJoin -benchmem
// ============================================================================

// ============================================================================
// Sort Benchmarks
// ============================================================================

func makeSortBenchDataF64(n int) []float64 {
	data := make([]float64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.NormFloat64() * 100
	}
	return data
}

func makeSortBenchDataI64(n int) []int64 {
	data := make([]int64, n)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = r.Int63n(1_000_000)
	}
	return data
}

func BenchmarkSortJoin_Sort_F64_10K(b *testing.B) {
	data := makeSortBenchDataF64(10_000)
	series := NewSeriesFloat64("value", data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		series.Sort()
	}
}

func BenchmarkSortJoin_Sort_F64_100K(b *testing.B) {
	data := makeSortBenchDataF64(100_000)
	series := NewSeriesFloat64("value", data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		series.Sort()
	}
}

func BenchmarkSortJoin_Sort_F64_1M(b *testing.B) {
	data := makeSortBenchDataF64(1_000_000)
	series := NewSeriesFloat64("value", data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		series.Sort()
	}
}

func BenchmarkSortJoin_Sort_I64_1M(b *testing.B) {
	data := makeSortBenchDataI64(1_000_000)
	series := NewSeriesInt64("value", data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		series.Sort()
	}
}

func BenchmarkSortJoin_SortDF_1M(b *testing.B) {
	data := makeSortBenchDataF64(1_000_000)
	ids := makeSortBenchDataI64(1_000_000)
	df, _ := NewDataFrame(
		NewSeriesInt64("id", ids),
		NewSeriesFloat64("value", data),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.SortBy("value", true)
	}
}

// ============================================================================
// Argsort Benchmarks (lower level)
// ============================================================================

func BenchmarkSortJoin_Argsort_F64_1M(b *testing.B) {
	data := makeSortBenchDataF64(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortF64(data, true) // ascending=true
	}
}

func BenchmarkSortJoin_Argsort_I64_1M(b *testing.B) {
	data := makeSortBenchDataI64(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ArgsortI64(data, true) // ascending=true
	}
}

// ============================================================================
// Join Benchmarks
// ============================================================================

// makeJoinBenchData creates left and right DataFrames for join benchmarks
// Uses same pattern as Polars comparison: left=n, right=n/2, keys=n/10
func makeJoinBenchData(n int) (*DataFrame, *DataFrame) {
	r := rand.New(rand.NewSource(42))
	numKeys := n / 10

	// Left DataFrame: n rows
	leftIds := make([]int64, n)
	leftVals := make([]float64, n)
	for i := 0; i < n; i++ {
		leftIds[i] = int64(r.Intn(numKeys))
		leftVals[i] = r.NormFloat64()
	}

	// Right DataFrame: n/2 rows
	rightN := n / 2
	rightIds := make([]int64, rightN)
	rightVals := make([]float64, rightN)
	for i := 0; i < rightN; i++ {
		rightIds[i] = int64(r.Intn(numKeys))
		rightVals[i] = r.NormFloat64()
	}

	left, _ := NewDataFrame(
		NewSeriesInt64("id", leftIds),
		NewSeriesFloat64("left_val", leftVals),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", rightIds),
		NewSeriesFloat64("right_val", rightVals),
	)

	return left, right
}

func BenchmarkSortJoin_InnerJoin_10K(b *testing.B) {
	left, right := makeJoinBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.Join(right, On("id"))
	}
}

func BenchmarkSortJoin_InnerJoin_100K(b *testing.B) {
	left, right := makeJoinBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.Join(right, On("id"))
	}
}

func BenchmarkSortJoin_InnerJoin_1M(b *testing.B) {
	left, right := makeJoinBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.Join(right, On("id"))
	}
}

func BenchmarkSortJoin_LeftJoin_10K(b *testing.B) {
	left, right := makeJoinBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.LeftJoin(right, On("id"))
	}
}

func BenchmarkSortJoin_LeftJoin_100K(b *testing.B) {
	left, right := makeJoinBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.LeftJoin(right, On("id"))
	}
}

func BenchmarkSortJoin_LeftJoin_1M(b *testing.B) {
	left, right := makeJoinBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.LeftJoin(right, On("id"))
	}
}

// ============================================================================
// GroupBy Benchmarks (for reference alongside joins)
// ============================================================================

func makeGroupByBenchData(n int) *DataFrame {
	r := rand.New(rand.NewSource(42))
	numKeys := n / 10 // 100K groups for 1M rows

	keys := make([]int64, n)
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		keys[i] = int64(r.Intn(numKeys))
		values[i] = r.NormFloat64()
	}

	df, _ := NewDataFrame(
		NewSeriesInt64("key", keys),
		NewSeriesFloat64("value", values),
	)
	return df
}

func BenchmarkSortJoin_GroupBySum_10K(b *testing.B) {
	df := makeGroupByBenchData(10_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.GroupBy("key").Sum("value")
	}
}

func BenchmarkSortJoin_GroupBySum_100K(b *testing.B) {
	df := makeGroupByBenchData(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.GroupBy("key").Sum("value")
	}
}

func BenchmarkSortJoin_GroupBySum_1M(b *testing.B) {
	df := makeGroupByBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.GroupBy("key").Sum("value")
	}
}

func BenchmarkSortJoin_GroupByMulti_1M(b *testing.B) {
	df := makeGroupByBenchData(1_000_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.GroupBy("key").Agg(
			AggSum("value").Alias("sum"),
			AggMean("value").Alias("mean"),
			AggMin("value").Alias("min"),
			AggMax("value").Alias("max"),
			AggCount().Alias("count"),
		)
	}
}
