//go:build ignore

package main

import (
	"fmt"
	"time"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	n := 1_000_000
	iterations := 5

	fmt.Println("=== Chunked vs Regular DataFrame Benchmark ===")
	fmt.Printf("Size: %d elements, Iterations: %d\n\n", n, iterations)

	// Create test data
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(n - i) // Reverse sorted for sort test
	}

	// Create DataFrames
	regularDF, _ := galleon.NewDataFrame(galleon.NewSeriesFloat64("values", data))
	chunkedDF, _ := galleon.NewDataFrameChunked(galleon.NewSeriesFloat64("values", data))

	// Benchmark Sum
	fmt.Println("--- Sum ---")
	regularSum := benchmark(iterations, func() {
		_ = regularDF.ColumnByName("values").Sum()
	})
	chunkedSum := benchmark(iterations, func() {
		_ = chunkedDF.ColumnByName("values").Sum()
	})
	fmt.Printf("Regular: %v\n", regularSum)
	fmt.Printf("Chunked: %v\n", chunkedSum)
	fmt.Printf("Speedup: %.2fx\n\n", float64(regularSum)/float64(chunkedSum))

	// Benchmark Min
	fmt.Println("--- Min ---")
	regularMin := benchmark(iterations, func() {
		_ = regularDF.ColumnByName("values").Min()
	})
	chunkedMin := benchmark(iterations, func() {
		_ = chunkedDF.ColumnByName("values").Min()
	})
	fmt.Printf("Regular: %v\n", regularMin)
	fmt.Printf("Chunked: %v\n", chunkedMin)
	fmt.Printf("Speedup: %.2fx\n\n", float64(regularMin)/float64(chunkedMin))

	// Benchmark Max
	fmt.Println("--- Max ---")
	regularMax := benchmark(iterations, func() {
		_ = regularDF.ColumnByName("values").Max()
	})
	chunkedMax := benchmark(iterations, func() {
		_ = chunkedDF.ColumnByName("values").Max()
	})
	fmt.Printf("Regular: %v\n", regularMax)
	fmt.Printf("Chunked: %v\n", chunkedMax)
	fmt.Printf("Speedup: %.2fx\n\n", float64(regularMax)/float64(chunkedMax))

	// Benchmark Mean
	fmt.Println("--- Mean ---")
	regularMean := benchmark(iterations, func() {
		_ = regularDF.ColumnByName("values").Mean()
	})
	chunkedMean := benchmark(iterations, func() {
		_ = chunkedDF.ColumnByName("values").Mean()
	})
	fmt.Printf("Regular: %v\n", regularMean)
	fmt.Printf("Chunked: %v\n", chunkedMean)
	fmt.Printf("Speedup: %.2fx\n\n", float64(regularMean)/float64(chunkedMean))

	// Benchmark Sort - Regular only first
	fmt.Println("--- Sort (Regular) ---")
	regularSort := benchmark(1, func() {
		_, _ = regularDF.SortBy("values", true)
	})
	fmt.Printf("Regular: %v\n", regularSort)

	fmt.Println("--- Sort (Chunked) ---")
	chunkedSort := benchmark(1, func() {
		_, _ = chunkedDF.SortBy("values", true)
	})
	fmt.Printf("Chunked: %v\n", chunkedSort)
	fmt.Printf("Speedup: %.2fx\n", float64(regularSort)/float64(chunkedSort))
}

func benchmark(iterations int, fn func()) time.Duration {
	// Warmup
	fn()

	start := time.Now()
	for i := 0; i < iterations; i++ {
		fn()
	}
	return time.Since(start) / time.Duration(iterations)
}
