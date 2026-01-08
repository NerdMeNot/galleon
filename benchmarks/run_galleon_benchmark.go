package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"time"

	galleon "github.com/NerdMeNot/galleon/go"
)

// BenchmarkResult holds timing results for a single benchmark
type BenchmarkResult struct {
	Median   float64   `json:"median"`
	Min      float64   `json:"min"`
	Max      float64   `json:"max"`
	Mean     float64   `json:"mean"`
	AllTimes []float64 `json:"all_times"`
}

// AllResults holds all benchmark results
type AllResults struct {
	Library      string                       `json:"library"`
	Version      string                       `json:"version"`
	Threads      int                          `json:"threads"`
	AutoDetected bool                         `json:"auto_detected"`
	Results      map[string]BenchmarkResult   `json:"results"`
	Sizes        map[int]map[string]BenchmarkResult `json:"by_size"`
}

func runBenchmark(name string, warmup, iterations int, fn func()) BenchmarkResult {
	// Warmup
	for i := 0; i < warmup; i++ {
		fn()
	}

	// Timed runs
	times := make([]float64, iterations)
	for i := 0; i < iterations; i++ {
		runtime.GC()
		start := time.Now()
		fn()
		times[i] = float64(time.Since(start).Microseconds()) / 1000.0 // ms
	}

	sort.Float64s(times)

	// Calculate mean
	var sum float64
	for _, t := range times {
		sum += t
	}
	mean := sum / float64(len(times))

	return BenchmarkResult{
		Median:   times[len(times)/2],
		Min:      times[0],
		Max:      times[len(times)-1],
		Mean:     mean,
		AllTimes: times,
	}
}

func generateData(n int, seed int64) (leftIds, rightIds, groupKeys []int64, leftVals, rightVals, values []float64) {
	r := rand.New(rand.NewSource(seed))

	leftN := n
	rightN := n / 2
	numKeys := n / 10

	leftIds = make([]int64, leftN)
	leftVals = make([]float64, leftN)
	for i := 0; i < leftN; i++ {
		leftIds[i] = int64(r.Intn(numKeys))
		leftVals[i] = r.NormFloat64()
	}

	rightIds = make([]int64, rightN)
	rightVals = make([]float64, rightN)
	for i := 0; i < rightN; i++ {
		rightIds[i] = int64(r.Intn(numKeys))
		rightVals[i] = r.NormFloat64()
	}

	groupKeys = make([]int64, n)
	values = make([]float64, n)
	for i := 0; i < n; i++ {
		groupKeys[i] = int64(r.Intn(numKeys))
		values[i] = r.NormFloat64()
	}

	return
}

func benchmarkSize(n int, iterations int) map[string]BenchmarkResult {
	results := make(map[string]BenchmarkResult)

	fmt.Printf("\n  Generating data for %d rows...\n", n)
	leftIds, rightIds, groupKeys, leftVals, rightVals, values := generateData(n, 42)

	// Create DataFrames
	leftDf, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", leftIds),
		galleon.NewSeriesFloat64("left_val", leftVals),
	)
	rightDf, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", rightIds),
		galleon.NewSeriesFloat64("right_val", rightVals),
	)
	groupDf, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("key", groupKeys),
		galleon.NewSeriesFloat64("value", values),
	)

	valuesF64 := values // Already float64

	// Aggregations
	fmt.Println("  Running aggregation benchmarks...")
	results["sum"] = runBenchmark("Sum", 2, iterations, func() {
		galleon.SumF64(valuesF64)
	})
	results["min"] = runBenchmark("Min", 2, iterations, func() {
		galleon.MinF64(valuesF64)
	})
	results["max"] = runBenchmark("Max", 2, iterations, func() {
		galleon.MaxF64(valuesF64)
	})
	results["mean"] = runBenchmark("Mean", 2, iterations, func() {
		galleon.MeanF64(valuesF64)
	})

	// Filter
	fmt.Println("  Running filter benchmark...")
	results["filter"] = runBenchmark("Filter", 2, iterations, func() {
		galleon.FilterMaskGreaterThanF64(valuesF64, 0.0)
	})

	// Sort
	fmt.Println("  Running sort benchmark...")
	results["sort"] = runBenchmark("Sort", 2, iterations, func() {
		galleon.ArgsortF64(valuesF64, true)
	})

	// GroupBy
	fmt.Println("  Running groupby benchmarks...")
	results["groupby_sum"] = runBenchmark("GroupBy Sum", 2, iterations, func() {
		groupDf.GroupBy("key").Sum("value")
	})

	// Joins
	fmt.Println("  Running join benchmarks...")
	results["inner_join"] = runBenchmark("Inner Join", 2, iterations, func() {
		leftDf.Join(rightDf, galleon.On("id"))
	})
	results["left_join"] = runBenchmark("Left Join", 2, iterations, func() {
		leftDf.LeftJoin(rightDf, galleon.On("id"))
	})

	return results
}

func main() {
	sizes := []int{10_000, 100_000, 1_000_000}
	iterations := 10

	config := galleon.GetThreadConfig()

	fmt.Println("=" + string(make([]byte, 79)))
	fmt.Println("GALLEON BENCHMARK SUITE")
	fmt.Println("=" + string(make([]byte, 79)))
	fmt.Printf("Threads: %d (auto-detected: %v)\n", config.MaxThreads, config.AutoDetected)
	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("Iterations: %d\n", iterations)
	fmt.Println("=" + string(make([]byte, 79)))

	allResults := AllResults{
		Library:      "galleon",
		Version:      "0.1.0",
		Threads:      config.MaxThreads,
		AutoDetected: config.AutoDetected,
		Sizes:        make(map[int]map[string]BenchmarkResult),
	}

	for _, size := range sizes {
		fmt.Printf("\n--- Benchmarking %d rows ---\n", size)
		results := benchmarkSize(size, iterations)
		allResults.Sizes[size] = results

		// Print results
		fmt.Printf("\n  Results for %d rows:\n", size)
		fmt.Printf("  %-20s %12s %12s %12s\n", "Operation", "Median", "Min", "Max")
		fmt.Printf("  %s\n", string(make([]byte, 60)))

		ops := []string{"sum", "min", "max", "mean", "filter", "sort", "groupby_sum", "inner_join", "left_join"}
		for _, op := range ops {
			if r, ok := results[op]; ok {
				fmt.Printf("  %-20s %10.2fms %10.2fms %10.2fms\n",
					op, r.Median, r.Min, r.Max)
			}
		}
	}

	// Save results to JSON
	output, err := json.MarshalIndent(allResults, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling results: %v\n", err)
		return
	}

	err = os.WriteFile("galleon_benchmark_results.json", output, 0644)
	if err != nil {
		fmt.Printf("Error writing results: %v\n", err)
		return
	}

	fmt.Println("\nResults saved to galleon_benchmark_results.json")
}
