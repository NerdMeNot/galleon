package galleon

import (
	"math"
	"testing"
)

func TestBlitzInit(t *testing.T) {
	// Test initialization
	if !BlitzInit() {
		t.Fatal("BlitzInit() returned false")
	}

	// Should be idempotent
	if !BlitzInit() {
		t.Fatal("Second BlitzInit() returned false")
	}

	// Should report as initialized
	if !BlitzIsInitialized() {
		t.Fatal("BlitzIsInitialized() returned false after init")
	}

	// Should have at least one worker
	workers := BlitzNumWorkers()
	if workers < 1 {
		t.Fatalf("BlitzNumWorkers() = %d, want >= 1", workers)
	}
	t.Logf("Blitz initialized with %d workers", workers)
}

func TestAutoParallelSum(t *testing.T) {
	// Test that SumF64 works correctly for both small and large data
	// (internally uses Blitz for large data)

	tests := []struct {
		name string
		size int
		want float64
	}{
		{"small_10", 10, 45},            // 0+1+...+9 = 45
		{"medium_1000", 1000, 499500},   // Sum of 0..999
		{"large_100000", 100000, 4999950000}, // Sum of 0..99999 (above parallel threshold)
		{"large_1000000", 1000000, 499999500000}, // Sum of 0..999999
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]float64, tt.size)
			for i := range data {
				data[i] = float64(i)
			}
			got := SumF64(data)
			if math.Abs(got-tt.want) > 1.0 { // Allow small floating point error
				t.Errorf("SumF64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAutoParallelMinMax(t *testing.T) {
	// Test that MinF64/MaxF64 work correctly for large data
	data := make([]float64, 200000) // Above threshold
	for i := range data {
		data[i] = float64(i) - 100000 // Range: -100000 to 99999
	}

	min := MinF64(data)
	max := MaxF64(data)

	if min != -100000 {
		t.Errorf("MinF64() = %v, want -100000", min)
	}
	if max != 99999 {
		t.Errorf("MaxF64() = %v, want 99999", max)
	}
}

func TestAutoParallelAdd(t *testing.T) {
	// Test that AddF64 works correctly for large data
	size := 200000 // Above threshold
	a := make([]float64, size)
	b := make([]float64, size)
	out := make([]float64, size)

	for i := range a {
		a[i] = float64(i)
		b[i] = float64(i * 2)
	}

	AddF64(a, b, out)

	// Check a few values
	for _, i := range []int{0, 1000, 100000, 199999} {
		want := float64(i) + float64(i*2)
		if out[i] != want {
			t.Errorf("out[%d] = %v, want %v", i, out[i], want)
		}
	}
}

// Benchmarks to show auto-parallelization speedup

func BenchmarkSumF64_Small(b *testing.B) {
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}

func BenchmarkSumF64_Large(b *testing.B) {
	data := make([]float64, 1000000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}

func BenchmarkSumF64_VeryLarge(b *testing.B) {
	data := make([]float64, 10000000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumF64(data)
	}
}

func BenchmarkMinF64_Large(b *testing.B) {
	data := make([]float64, 1000000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MinF64(data)
	}
}

func BenchmarkMaxF64_Large(b *testing.B) {
	data := make([]float64, 1000000)
	for i := range data {
		data[i] = float64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MaxF64(data)
	}
}

func BenchmarkAddF64_Large(b *testing.B) {
	size := 1000000
	a := make([]float64, size)
	c := make([]float64, size)
	out := make([]float64, size)
	for i := range a {
		a[i] = float64(i)
		c[i] = float64(i * 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AddF64(a, c, out)
	}
}
