package benchmarks

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	_ "unsafe" // Required for go:linkname

	galleon "github.com/NerdMeNot/galleon/go"
)

// BenchResult holds a single benchmark result
type BenchResult struct {
	Operation string  `json:"operation"`
	Category  string  `json:"category"`
	Size      int     `json:"size"`
	TimeMs    float64 `json:"time_ms"`
}

// SEED matches Python benchmarks for identical data
const SEED = 42

// Test sizes - can be overridden by BENCHMARK_SIZES env var (comma-separated)
var sizes = []int{100_000, 1_000_000}

func init() {
	if envSizes := os.Getenv("BENCHMARK_SIZES"); envSizes != "" {
		parts := strings.Split(envSizes, ",")
		newSizes := make([]int, 0, len(parts))
		for _, p := range parts {
			if size, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
				newSizes = append(newSizes, size)
			}
		}
		if len(newSizes) > 0 {
			sizes = newSizes
		}
	}
}

// ============================================================================
// Data Generation (matches Python exactly)
// ============================================================================

func makeRandomF64(n int, seed int64) []float64 {
	rng := rand.New(rand.NewSource(seed))
	data := make([]float64, n)
	for i := range data {
		data[i] = rng.NormFloat64() * 100
	}
	return data
}

func makeRandomI64(n int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	data := make([]int64, n)
	for i := range data {
		data[i] = rng.Int63n(1000000)
	}
	return data
}

func makeGroupKeys(n int, numGroups int, seed int64) []int64 {
	rng := rand.New(rand.NewSource(seed))
	data := make([]int64, n)
	for i := range data {
		data[i] = rng.Int63n(int64(numGroups))
	}
	return data
}

// ============================================================================
// Core Aggregation Benchmarks
// ============================================================================

func BenchmarkAll_Sum_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.SumF64(data)
			}
		})
	}
}

func BenchmarkAll_Mean_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MeanF64(data)
			}
		})
	}
}

func BenchmarkAll_Min_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MinF64(data)
			}
		})
	}
}

func BenchmarkAll_Max_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MaxF64(data)
			}
		})
	}
}

// ============================================================================
// Statistics Benchmarks
// ============================================================================

func BenchmarkAll_Median_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MedianF64(data)
			}
		})
	}
}

func BenchmarkAll_Quantile95_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.QuantileF64(data, 0.95)
			}
		})
	}
}

func BenchmarkAll_Variance_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.VarianceF64(data)
			}
		})
	}
}

func BenchmarkAll_StdDev_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.StdDevF64(data)
			}
		})
	}
}

// ============================================================================
// Sort Benchmarks
// ============================================================================

func BenchmarkAll_Sort_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.SortF64(data, true)
			}
		})
	}
}

func BenchmarkAll_Argsort_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.ArgsortF64(data, true)
			}
		})
	}
}

func BenchmarkAll_Sort_I64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomI64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.SortI64(data, true)
			}
		})
	}
}

// ============================================================================
// Arithmetic Benchmarks
// ============================================================================

func BenchmarkAll_Add_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := makeRandomF64(size, SEED+1)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.AddF64(a, c, out)
			}
		})
	}
}

func BenchmarkAll_Mul_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := makeRandomF64(size, SEED+1)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MulF64(a, c, out)
			}
		})
	}
}

func BenchmarkAll_Div_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := makeRandomF64(size, SEED+1)
		// Ensure no zeros in divisor
		for i := range c {
			if c[i] == 0 {
				c[i] = 1.0
			}
		}
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.DivF64(a, c, out)
			}
		})
	}
}

func BenchmarkAll_AddScalar_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			// Make a copy since AddScalarF64 modifies in-place
			data := make([]float64, len(a))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, a)
				galleon.AddScalarF64(data, 42.0)
			}
		})
	}
}

func BenchmarkAll_MulScalar_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			data := make([]float64, len(a))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(data, a)
				galleon.MulScalarF64(data, 2.5)
			}
		})
	}
}

// ============================================================================
// Comparison Benchmarks
// ============================================================================

func BenchmarkAll_CmpGt_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := make([]float64, size) // zeros for comparison
		out := make([]byte, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.CmpGtF64(a, c, out)
			}
		})
	}
}

func BenchmarkAll_FilterGt_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.FilterGreaterThanF64(a, 0.0)
			}
		})
	}
}

// ============================================================================
// Window Function Benchmarks
// ============================================================================

func BenchmarkAll_RollingSum_F64(b *testing.B) {
	window := 100
	minPeriods := 1
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.RollingSumF64(data, window, minPeriods, out)
			}
		})
	}
}

func BenchmarkAll_RollingMean_F64(b *testing.B) {
	window := 100
	minPeriods := 1
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.RollingMeanF64(data, window, minPeriods, out)
			}
		})
	}
}

func BenchmarkAll_RollingMin_F64(b *testing.B) {
	window := 100
	minPeriods := 1
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.RollingMinF64(data, window, minPeriods, out)
			}
		})
	}
}

func BenchmarkAll_RollingMax_F64(b *testing.B) {
	window := 100
	minPeriods := 1
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.RollingMaxF64(data, window, minPeriods, out)
			}
		})
	}
}

func BenchmarkAll_Diff_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.DiffF64(data, 0.0, out)
			}
		})
	}
}

func BenchmarkAll_Rank_F64(b *testing.B) {
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]uint32, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.RankF64(data, out)
			}
		})
	}
}

// ============================================================================
// Horizontal/Fold Benchmarks
// ============================================================================

func BenchmarkAll_SumHorizontal2_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := makeRandomF64(size, SEED+1)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.SumHorizontal2F64(a, c, out)
			}
		})
	}
}

func BenchmarkAll_MinHorizontal2_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := makeRandomF64(size, SEED+1)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MinHorizontal2F64(a, c, out)
			}
		})
	}
}

func BenchmarkAll_MaxHorizontal2_F64(b *testing.B) {
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := makeRandomF64(size, SEED+1)
		out := make([]float64, size)
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				galleon.MaxHorizontal2F64(a, c, out)
			}
		})
	}
}

// ============================================================================
// GroupBy Benchmarks
// ============================================================================

func BenchmarkAll_GroupBySum(b *testing.B) {
	for _, size := range sizes {
		numGroups := size / 100
		if numGroups < 10 {
			numGroups = 10
		}
		keys := makeGroupKeys(size, numGroups, SEED)
		values := makeRandomF64(size, SEED)

		keySeries := galleon.NewSeriesI64("key", keys)
		valSeries := galleon.NewSeriesF64("value", values)
		df := galleon.FromColumns(keySeries, valSeries)

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = df.GroupBy("key").Sum("value")
			}
		})
	}
}

func BenchmarkAll_GroupByMean(b *testing.B) {
	for _, size := range sizes {
		numGroups := size / 100
		if numGroups < 10 {
			numGroups = 10
		}
		keys := makeGroupKeys(size, numGroups, SEED)
		values := makeRandomF64(size, SEED)

		keySeries := galleon.NewSeriesI64("key", keys)
		valSeries := galleon.NewSeriesF64("value", values)
		df := galleon.FromColumns(keySeries, valSeries)

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = df.GroupBy("key").Mean("value")
			}
		})
	}
}

func BenchmarkAll_GroupByCount(b *testing.B) {
	for _, size := range sizes {
		numGroups := size / 100
		if numGroups < 10 {
			numGroups = 10
		}
		keys := makeGroupKeys(size, numGroups, SEED)
		values := makeRandomF64(size, SEED)

		keySeries := galleon.NewSeriesI64("key", keys)
		valSeries := galleon.NewSeriesF64("value", values)
		df := galleon.FromColumns(keySeries, valSeries)

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = df.GroupBy("key").Count()
			}
		})
	}
}

// ============================================================================
// Join Benchmarks
// ============================================================================

func BenchmarkAll_InnerJoin(b *testing.B) {
	for _, size := range sizes {
		// Left table: size rows
		leftKeys := makeRandomI64(size, SEED)
		leftValues := makeRandomF64(size, SEED)

		// Right table: size/10 rows (typical dimension table)
		rightSize := size / 10
		rightKeys := makeRandomI64(rightSize, SEED+1)
		rightValues := makeRandomF64(rightSize, SEED+1)

		leftKeyS := galleon.NewSeriesI64("key", leftKeys)
		leftValS := galleon.NewSeriesF64("left_val", leftValues)
		leftDf := galleon.FromColumns(leftKeyS, leftValS)

		rightKeyS := galleon.NewSeriesI64("key", rightKeys)
		rightValS := galleon.NewSeriesF64("right_val", rightValues)
		rightDf := galleon.FromColumns(rightKeyS, rightValS)

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = galleon.InnerJoin(leftDf, rightDf, "key", "key")
			}
		})
	}
}

func BenchmarkAll_LeftJoin(b *testing.B) {
	for _, size := range sizes {
		leftKeys := makeRandomI64(size, SEED)
		leftValues := makeRandomF64(size, SEED)

		rightSize := size / 10
		rightKeys := makeRandomI64(rightSize, SEED+1)
		rightValues := makeRandomF64(rightSize, SEED+1)

		leftKeyS := galleon.NewSeriesI64("key", leftKeys)
		leftValS := galleon.NewSeriesF64("left_val", leftValues)
		leftDf := galleon.FromColumns(leftKeyS, leftValS)

		rightKeyS := galleon.NewSeriesI64("key", rightKeys)
		rightValS := galleon.NewSeriesF64("right_val", rightValues)
		rightDf := galleon.FromColumns(rightKeyS, rightValS)

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = galleon.LeftJoin(leftDf, rightDf, "key", "key")
			}
		})
	}
}

// ============================================================================
// JSON Output Test (for Python integration)
// ============================================================================

func TestOutputBenchmarkJSON(t *testing.T) {
	// This test outputs JSON that Python can parse
	if os.Getenv("BENCHMARK_JSON") != "1" {
		t.Skip("Set BENCHMARK_JSON=1 to run")
	}

	results := runAllBenchmarks()
	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(jsonBytes))
}

func runAllBenchmarks() []BenchResult {
	var results []BenchResult

	// Core aggregations
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)

		results = append(results, benchmarkOp("Sum", "Aggregation", size, func() {
			galleon.SumF64(data)
		}))
		results = append(results, benchmarkOp("Mean", "Aggregation", size, func() {
			galleon.MeanF64(data)
		}))
		results = append(results, benchmarkOp("Min", "Aggregation", size, func() {
			galleon.MinF64(data)
		}))
		results = append(results, benchmarkOp("Max", "Aggregation", size, func() {
			galleon.MaxF64(data)
		}))
	}

	// Statistics
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)

		results = append(results, benchmarkOp("Median", "Statistics", size, func() {
			galleon.MedianF64(data)
		}))
		results = append(results, benchmarkOp("Quantile (0.95)", "Statistics", size, func() {
			galleon.QuantileF64(data, 0.95)
		}))
		results = append(results, benchmarkOp("Variance", "Statistics", size, func() {
			galleon.VarianceF64(data)
		}))
		results = append(results, benchmarkOp("StdDev", "Statistics", size, func() {
			galleon.StdDevF64(data)
		}))
	}

	// Sorting
	for _, size := range sizes {
		dataF64 := makeRandomF64(size, SEED)
		dataI64 := makeRandomI64(size, SEED)

		results = append(results, benchmarkOp("Sort F64", "Sorting", size, func() {
			galleon.SortF64(dataF64, true)
		}))
		results = append(results, benchmarkOp("Argsort F64", "Sorting", size, func() {
			galleon.ArgsortF64(dataF64, true)
		}))
		results = append(results, benchmarkOp("Sort I64", "Sorting", size, func() {
			galleon.SortI64(dataI64, true)
		}))
	}

	// Arithmetic
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		b := makeRandomF64(size, SEED+1)
		out := make([]float64, size)

		results = append(results, benchmarkOp("Add", "Arithmetic", size, func() {
			galleon.AddF64(a, b, out)
		}))
		results = append(results, benchmarkOp("Mul", "Arithmetic", size, func() {
			galleon.MulF64(a, b, out)
		}))
		results = append(results, benchmarkOp("Div", "Arithmetic", size, func() {
			galleon.DivF64(a, b, out)
		}))

		// Scalar ops need copy since they're in-place
		dataCopy := make([]float64, size)
		results = append(results, benchmarkOp("Add Scalar", "Arithmetic", size, func() {
			copy(dataCopy, a)
			galleon.AddScalarF64(dataCopy, 42.0)
		}))
		results = append(results, benchmarkOp("Mul Scalar", "Arithmetic", size, func() {
			copy(dataCopy, a)
			galleon.MulScalarF64(dataCopy, 2.5)
		}))
	}

	// Comparisons
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		c := make([]float64, size)
		out := make([]byte, size)

		results = append(results, benchmarkOp("CmpGt", "Comparison", size, func() {
			galleon.CmpGtF64(a, c, out)
		}))
		results = append(results, benchmarkOp("FilterGt (indices)", "Comparison", size, func() {
			galleon.FilterGreaterThanF64(a, 0.0)
		}))
	}

	// Window functions
	window := 100
	minPeriods := 1
	for _, size := range sizes {
		data := makeRandomF64(size, SEED)
		out := make([]float64, size)

		results = append(results, benchmarkOp("Rolling Sum", "Window", size, func() {
			galleon.RollingSumF64(data, window, minPeriods, out)
		}))
		results = append(results, benchmarkOp("Rolling Mean", "Window", size, func() {
			galleon.RollingMeanF64(data, window, minPeriods, out)
		}))
		results = append(results, benchmarkOp("Rolling Min", "Window", size, func() {
			galleon.RollingMinF64(data, window, minPeriods, out)
		}))
		results = append(results, benchmarkOp("Rolling Max", "Window", size, func() {
			galleon.RollingMaxF64(data, window, minPeriods, out)
		}))
		results = append(results, benchmarkOp("Diff", "Window", size, func() {
			galleon.DiffF64(data, 0.0, out)
		}))

		outRank := make([]uint32, size)
		results = append(results, benchmarkOp("Rank", "Window", size, func() {
			galleon.RankF64(data, outRank)
		}))
	}

	// Horizontal/Fold
	for _, size := range sizes {
		a := makeRandomF64(size, SEED)
		b := makeRandomF64(size, SEED+1)
		out := make([]float64, size)

		results = append(results, benchmarkOp("Sum Horizontal", "Fold", size, func() {
			galleon.SumHorizontal2F64(a, b, out)
		}))
		results = append(results, benchmarkOp("Min Horizontal", "Fold", size, func() {
			galleon.MinHorizontal2F64(a, b, out)
		}))
		results = append(results, benchmarkOp("Max Horizontal", "Fold", size, func() {
			galleon.MaxHorizontal2F64(a, b, out)
		}))
	}

	// GroupBy
	for _, size := range sizes {
		numGroups := size / 100
		if numGroups < 10 {
			numGroups = 10
		}
		keys := makeGroupKeys(size, numGroups, SEED)
		values := makeRandomF64(size, SEED)

		keySeries := galleon.NewSeriesI64("key", keys)
		valSeries := galleon.NewSeriesF64("value", values)
		df := galleon.FromColumns(keySeries, valSeries)

		results = append(results, benchmarkOp("GroupBy Sum", "GroupBy", size, func() {
			_ = df.GroupBy("key").Sum("value")
		}))
		results = append(results, benchmarkOp("GroupBy Mean", "GroupBy", size, func() {
			_ = df.GroupBy("key").Mean("value")
		}))
		results = append(results, benchmarkOp("GroupBy Count", "GroupBy", size, func() {
			_ = df.GroupBy("key").Count()
		}))
	}

	// Joins
	for _, size := range sizes {
		leftKeys := makeRandomI64(size, SEED)
		leftValues := makeRandomF64(size, SEED)
		rightSize := size / 10
		rightKeys := makeRandomI64(rightSize, SEED+1)
		rightValues := makeRandomF64(rightSize, SEED+1)

		leftKeyS := galleon.NewSeriesI64("key", leftKeys)
		leftValS := galleon.NewSeriesF64("left_val", leftValues)
		leftDf := galleon.FromColumns(leftKeyS, leftValS)

		rightKeyS := galleon.NewSeriesI64("key", rightKeys)
		rightValS := galleon.NewSeriesF64("right_val", rightValues)
		rightDf := galleon.FromColumns(rightKeyS, rightValS)

		results = append(results, benchmarkOp("Inner Join", "Join", size, func() {
			_ = galleon.InnerJoin(leftDf, rightDf, "key", "key")
		}))
		results = append(results, benchmarkOp("Left Join", "Join", size, func() {
			_ = galleon.LeftJoin(leftDf, rightDf, "key", "key")
		}))
	}

	return results
}

func benchmarkOp(name, category string, size int, fn func()) BenchResult {
	// Warmup
	for i := 0; i < 3; i++ {
		fn()
	}

	// Benchmark
	iterations := 10
	if size >= 1_000_000 {
		iterations = 5
	}

	var totalNs int64
	for i := 0; i < iterations; i++ {
		start := nanotime()
		fn()
		totalNs += nanotime() - start
	}

	avgMs := float64(totalNs) / float64(iterations) / 1_000_000.0

	return BenchResult{
		Operation: name,
		Category:  category,
		Size:      size,
		TimeMs:    avgMs,
	}
}

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64
