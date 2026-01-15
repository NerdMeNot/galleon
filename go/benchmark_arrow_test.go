package galleon

import (
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// Arrow Interchange Benchmarks
// Run with: go test -tags dev -bench=BenchmarkArrow -benchmem
// ============================================================================

func makeArrowBenchDF(nRows, nCols int) *DataFrame {
	r := rand.New(rand.NewSource(42))
	series := make([]*Series, nCols)
	for c := 0; c < nCols; c++ {
		data := make([]float64, nRows)
		for i := range data {
			data[i] = r.NormFloat64() * 100
		}
		series[c] = NewSeriesFloat64("col_"+string(rune('a'+c)), data)
	}
	df, _ := NewDataFrame(series...)
	return df
}

func makeArrowBenchDFMixed(nRows int) *DataFrame {
	r := rand.New(rand.NewSource(42))

	// Float64 column
	floats := make([]float64, nRows)
	for i := range floats {
		floats[i] = r.NormFloat64() * 100
	}

	// Int64 column
	ints := make([]int64, nRows)
	for i := range ints {
		ints[i] = int64(r.Intn(1000000))
	}

	// String column
	categories := []string{"cat_a", "cat_b", "cat_c", "cat_d", "cat_e"}
	strings := make([]string, nRows)
	for i := range strings {
		strings[i] = categories[r.Intn(len(categories))]
	}

	// Bool column
	bools := make([]bool, nRows)
	for i := range bools {
		bools[i] = r.Float64() > 0.5
	}

	df, _ := NewDataFrame(
		NewSeriesFloat64("floats", floats),
		NewSeriesInt64("ints", ints),
		NewSeriesString("strings", strings),
		NewSeriesBool("bools", bools),
	)
	return df
}

// ============================================================================
// Export Benchmarks - DataFrame to Arrow
// ============================================================================

func BenchmarkArrow_Export_10K_4Col(b *testing.B) {
	df := makeArrowBenchDF(10_000, 4)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

func BenchmarkArrow_Export_100K_4Col(b *testing.B) {
	df := makeArrowBenchDF(100_000, 4)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

func BenchmarkArrow_Export_1M_4Col(b *testing.B) {
	df := makeArrowBenchDF(1_000_000, 4)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

func BenchmarkArrow_Export_100K_10Col(b *testing.B) {
	df := makeArrowBenchDF(100_000, 10)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

// ============================================================================
// Import Benchmarks - Arrow to DataFrame
// ============================================================================

func BenchmarkArrow_Import_10K_4Col(b *testing.B) {
	df := makeArrowBenchDF(10_000, 4)
	mem := memory.DefaultAllocator
	record, _ := df.ToArrow(mem)
	defer record.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDataFrameFromArrow(record)
	}
}

func BenchmarkArrow_Import_100K_4Col(b *testing.B) {
	df := makeArrowBenchDF(100_000, 4)
	mem := memory.DefaultAllocator
	record, _ := df.ToArrow(mem)
	defer record.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDataFrameFromArrow(record)
	}
}

func BenchmarkArrow_Import_1M_4Col(b *testing.B) {
	df := makeArrowBenchDF(1_000_000, 4)
	mem := memory.DefaultAllocator
	record, _ := df.ToArrow(mem)
	defer record.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDataFrameFromArrow(record)
	}
}

func BenchmarkArrow_Import_100K_10Col(b *testing.B) {
	df := makeArrowBenchDF(100_000, 10)
	mem := memory.DefaultAllocator
	record, _ := df.ToArrow(mem)
	defer record.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDataFrameFromArrow(record)
	}
}

// ============================================================================
// Roundtrip Benchmarks
// ============================================================================

func BenchmarkArrow_Roundtrip_10K_4Col(b *testing.B) {
	df := makeArrowBenchDF(10_000, 4)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		_, _ = NewDataFrameFromArrow(record)
		record.Release()
	}
}

func BenchmarkArrow_Roundtrip_100K_4Col(b *testing.B) {
	df := makeArrowBenchDF(100_000, 4)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		_, _ = NewDataFrameFromArrow(record)
		record.Release()
	}
}

func BenchmarkArrow_Roundtrip_1M_4Col(b *testing.B) {
	df := makeArrowBenchDF(1_000_000, 4)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		_, _ = NewDataFrameFromArrow(record)
		record.Release()
	}
}

// ============================================================================
// Mixed Type Benchmarks
// ============================================================================

func BenchmarkArrow_Export_Mixed_10K(b *testing.B) {
	df := makeArrowBenchDFMixed(10_000)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

func BenchmarkArrow_Export_Mixed_100K(b *testing.B) {
	df := makeArrowBenchDFMixed(100_000)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

func BenchmarkArrow_Import_Mixed_100K(b *testing.B) {
	df := makeArrowBenchDFMixed(100_000)
	mem := memory.DefaultAllocator
	record, _ := df.ToArrow(mem)
	defer record.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewDataFrameFromArrow(record)
	}
}

func BenchmarkArrow_Roundtrip_Mixed_100K(b *testing.B) {
	df := makeArrowBenchDFMixed(100_000)
	mem := memory.DefaultAllocator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		_, _ = NewDataFrameFromArrow(record)
		record.Release()
	}
}

// ============================================================================
// Categorical Arrow Benchmarks
// ============================================================================

func BenchmarkArrow_Export_Categorical_100K(b *testing.B) {
	categories := []string{"cat_a", "cat_b", "cat_c", "cat_d", "cat_e"}
	data := make([]string, 100_000)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = categories[r.Intn(len(categories))]
	}

	df, _ := NewDataFrame(
		NewSeriesCategorical("category", data),
	)
	mem := memory.DefaultAllocator

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		record.Release()
	}
}

func BenchmarkArrow_Roundtrip_Categorical_100K(b *testing.B) {
	categories := []string{"cat_a", "cat_b", "cat_c", "cat_d", "cat_e"}
	data := make([]string, 100_000)
	r := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = categories[r.Intn(len(categories))]
	}

	df, _ := NewDataFrame(
		NewSeriesCategorical("category", data),
	)
	mem := memory.DefaultAllocator

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, _ := df.ToArrow(mem)
		_, _ = NewDataFrameFromArrow(record)
		record.Release()
	}
}
