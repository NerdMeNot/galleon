package benchmarks

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/NerdMeNot/galleon/go"
)

func runBenchmark(name string, iterations int, warmup int, fn func()) {
	// Warmup
	for i := 0; i < warmup; i++ {
		fn()
	}

	// Actual benchmark
	var total time.Duration
	var minTime, maxTime time.Duration
	minTime = time.Hour

	for i := 0; i < iterations; i++ {
		start := time.Now()
		fn()
		elapsed := time.Since(start)
		total += elapsed
		if elapsed < minTime {
			minTime = elapsed
		}
		if elapsed > maxTime {
			maxTime = elapsed
		}
	}

	avg := total / time.Duration(iterations)
	fmt.Printf("  %-24s %8.3f ms\n", name+":", float64(avg.Microseconds())/1000)
}

func TestCompareWithPolars(t *testing.T) {
	sizes := []int{10_000, 100_000, 1_000_000}

	fmt.Println("======================================================================")
	fmt.Println("GALLEON (Go+Zig) BENCHMARK - For comparison with Polars")
	fmt.Println("======================================================================")

	for _, n := range sizes {
		fmt.Printf("\n======================================================================\n")
		fmt.Printf("Size: %d rows\n", n)
		fmt.Println("======================================================================")

		// Create test data
		rand.Seed(42)
		numGroups := n / 10

		groupKeys := make([]int64, n)
		valuesF64 := make([]float64, n)
		valuesI64 := make([]int64, n)

		for i := 0; i < n; i++ {
			groupKeys[i] = int64(rand.Intn(numGroups))
			valuesF64[i] = rand.NormFloat64()
			valuesI64[i] = int64(rand.Intn(1000000))
		}

		df, _ := galleon.NewDataFrame(
			galleon.NewSeriesInt64("group_key", groupKeys),
			galleon.NewSeriesFloat64("value_f64", valuesF64),
			galleon.NewSeriesInt64("value_i64", valuesI64),
		)

		iterations := 10
		warmup := 2

		// GroupBy Sum
		runBenchmark("GroupBy Sum (f64)", iterations, warmup, func() {
			df.GroupBy("group_key").Sum("value_f64")
		})

		// GroupBy Mean
		runBenchmark("GroupBy Mean (f64)", iterations, warmup, func() {
			df.GroupBy("group_key").Mean("value_f64")
		})

		// GroupBy Min
		runBenchmark("GroupBy Min (f64)", iterations, warmup, func() {
			df.GroupBy("group_key").Min("value_f64")
		})

		// GroupBy Max
		runBenchmark("GroupBy Max (f64)", iterations, warmup, func() {
			df.GroupBy("group_key").Max("value_f64")
		})

		// GroupBy Count
		runBenchmark("GroupBy Count", iterations, warmup, func() {
			df.GroupBy("group_key").Count()
		})

		// GroupBy Multiple Aggregations
		runBenchmark("GroupBy Multi-Agg", iterations, warmup, func() {
			df.GroupBy("group_key").Agg(
				galleon.AggSum("value_f64").Alias("sum"),
				galleon.AggMean("value_f64").Alias("mean"),
				galleon.AggMin("value_f64").Alias("min"),
				galleon.AggMax("value_f64").Alias("max"),
				galleon.AggCount().Alias("count"),
			)
		})

		// Create join test data
		leftN := n
		rightN := n / 2
		numKeys := n / 10

		leftIds := make([]int64, leftN)
		leftVals := make([]float64, leftN)
		for i := 0; i < leftN; i++ {
			leftIds[i] = int64(rand.Intn(numKeys))
			leftVals[i] = rand.NormFloat64()
		}

		rightIds := make([]int64, rightN)
		rightVals := make([]float64, rightN)
		for i := 0; i < rightN; i++ {
			rightIds[i] = int64(rand.Intn(numKeys))
			rightVals[i] = rand.NormFloat64()
		}

		leftDf, _ := galleon.NewDataFrame(
			galleon.NewSeriesInt64("id", leftIds),
			galleon.NewSeriesFloat64("left_val", leftVals),
		)

		rightDf, _ := galleon.NewDataFrame(
			galleon.NewSeriesInt64("id", rightIds),
			galleon.NewSeriesFloat64("right_val", rightVals),
		)

		// Inner Join
		runBenchmark("Inner Join", 5, 2, func() {
			leftDf.Join(rightDf, galleon.On("id"))
		})

		// Left Join
		runBenchmark("Left Join", 5, 2, func() {
			leftDf.LeftJoin(rightDf, galleon.On("id"))
		})
	}

	fmt.Println("\n======================================================================")
	fmt.Println("NOTES:")
	fmt.Println("- Galleon uses Go + Zig SIMD backend (parallel aggregations)")
	fmt.Println("- Times are in milliseconds (lower is better)")
	fmt.Println("======================================================================")
}

func TestIOBenchmarks(t *testing.T) {
	sizes := []int{10_000, 100_000, 1_000_000}

	fmt.Println("======================================================================")
	fmt.Println("GALLEON I/O BENCHMARK")
	fmt.Println("======================================================================")

	for _, n := range sizes {
		fmt.Printf("\n======================================================================\n")
		fmt.Printf("Size: %d rows\n", n)
		fmt.Println("======================================================================")

		// Create test data
		rand.Seed(42)

		ids := make([]int64, n)
		values := make([]float64, n)
		names := make([]string, n)

		for i := 0; i < n; i++ {
			ids[i] = int64(i)
			values[i] = rand.NormFloat64()
			names[i] = fmt.Sprintf("item_%d", rand.Intn(1000))
		}

		df, _ := galleon.NewDataFrame(
			galleon.NewSeriesInt64("id", ids),
			galleon.NewSeriesFloat64("value", values),
			galleon.NewSeriesString("name", names),
		)

		iterations := 5
		warmup := 1

		// CSV Write
		var csvBuf bytes.Buffer
		runBenchmark("CSV Write", iterations, warmup, func() {
			csvBuf.Reset()
			df.WriteCSVToWriter(&csvBuf)
		})

		// CSV Read
		csvData := csvBuf.Bytes()
		runBenchmark("CSV Read", iterations, warmup, func() {
			galleon.ReadCSVFromReader(bytes.NewReader(csvData))
		})

		// Create numeric-only DataFrame for Parquet (strings have issues in parquet-go)
		dfNumeric, _ := galleon.NewDataFrame(
			galleon.NewSeriesInt64("id", ids),
			galleon.NewSeriesFloat64("value", values),
		)

		// Parquet Write (to temp file)
		tmpFile := fmt.Sprintf("/tmp/galleon_bench_%d.parquet", n)
		runBenchmark("Parquet Write", iterations, warmup, func() {
			dfNumeric.WriteParquet(tmpFile)
		})

		// Parquet Read
		runBenchmark("Parquet Read", iterations, warmup, func() {
			galleon.ReadParquet(tmpFile)
		})

		// Cleanup
		os.Remove(tmpFile)

		// Also benchmark parallel vs sequential
		fmt.Println("\n--- Parallel vs Sequential Comparison ---")

		// Sequential
		seqCfg := &galleon.ParallelConfig{
			Enabled: false,
		}
		galleon.SetParallelConfig(seqCfg)

		runBenchmark("CSV Write (seq)", iterations, warmup, func() {
			csvBuf.Reset()
			df.WriteCSVToWriter(&csvBuf)
		})

		runBenchmark("CSV Read (seq)", iterations, warmup, func() {
			galleon.ReadCSVFromReader(bytes.NewReader(csvData))
		})

		// Parallel
		parCfg := galleon.DefaultParallelConfig()
		galleon.SetParallelConfig(parCfg)

		runBenchmark("CSV Write (par)", iterations, warmup, func() {
			csvBuf.Reset()
			df.WriteCSVToWriter(&csvBuf)
		})

		runBenchmark("CSV Read (par)", iterations, warmup, func() {
			galleon.ReadCSVFromReader(bytes.NewReader(csvData))
		})
	}

	// Reset to default config
	galleon.SetParallelConfig(galleon.DefaultParallelConfig())

	fmt.Println("\n======================================================================")
	fmt.Println("I/O BENCHMARK COMPLETE")
	fmt.Println("======================================================================")
}
