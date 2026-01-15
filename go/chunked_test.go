package galleon

import (
	"math"
	"testing"
)

func TestChunkedColumnF64_Basic(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewChunkedColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create chunked column")
	}

	// Test length
	if col.Len() != 5 {
		t.Errorf("Expected length 5, got %d", col.Len())
	}

	// Test number of chunks (should be 1 for small data)
	if col.NumChunks() != 1 {
		t.Errorf("Expected 1 chunk, got %d", col.NumChunks())
	}

	// Test get
	if col.Get(0) != 1.0 {
		t.Errorf("Expected first element 1.0, got %f", col.Get(0))
	}
	if col.Get(4) != 5.0 {
		t.Errorf("Expected last element 5.0, got %f", col.Get(4))
	}

	// Test ToSlice
	slice := col.ToSlice()
	if len(slice) != 5 {
		t.Errorf("Expected slice length 5, got %d", len(slice))
	}
	for i, v := range data {
		if slice[i] != v {
			t.Errorf("Slice mismatch at %d: expected %f, got %f", i, v, slice[i])
		}
	}
}

func TestChunkedColumnF64_Aggregations(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewChunkedColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create chunked column")
	}

	// Test sum
	sum := col.Sum()
	if math.Abs(sum-15.0) > 0.001 {
		t.Errorf("Expected sum 15.0, got %f", sum)
	}

	// Test min
	min := col.Min()
	if math.Abs(min-1.0) > 0.001 {
		t.Errorf("Expected min 1.0, got %f", min)
	}

	// Test max
	max := col.Max()
	if math.Abs(max-5.0) > 0.001 {
		t.Errorf("Expected max 5.0, got %f", max)
	}

	// Test mean
	mean := col.Mean()
	if math.Abs(mean-3.0) > 0.001 {
		t.Errorf("Expected mean 3.0, got %f", mean)
	}
}

func TestChunkedColumnF64_FilterGt(t *testing.T) {
	data := []float64{1.0, 5.0, 2.0, 8.0, 3.0, 9.0, 4.0}
	col := NewChunkedColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create chunked column")
	}

	filtered := col.FilterGt(4.0)
	if filtered == nil {
		t.Fatal("FilterGt returned nil")
	}

	if filtered.Len() != 3 {
		t.Errorf("Expected filtered length 3, got %d", filtered.Len())
	}

	// Values > 4.0: 5.0, 8.0, 9.0
	slice := filtered.ToSlice()
	expected := []float64{5.0, 8.0, 9.0}
	for i, v := range expected {
		if slice[i] != v {
			t.Errorf("Filtered mismatch at %d: expected %f, got %f", i, v, slice[i])
		}
	}
}

func TestChunkedColumnF64_Sort(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	col := NewChunkedColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create chunked column")
	}

	sorted := col.Sort()
	if sorted == nil {
		t.Fatal("Sort returned nil")
	}

	if sorted.Len() != 5 {
		t.Errorf("Expected sorted length 5, got %d", sorted.Len())
	}

	slice := sorted.ToSlice()
	expected := []float64{1.0, 2.0, 5.0, 8.0, 9.0}
	for i, v := range expected {
		if slice[i] != v {
			t.Errorf("Sorted mismatch at %d: expected %f, got %f", i, v, slice[i])
		}
	}
}

func TestChunkedColumnF64_Argsort(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	col := NewChunkedColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create chunked column")
	}

	result := col.Argsort()
	if result == nil {
		t.Fatal("Argsort returned nil")
	}

	if result.Len() != 5 {
		t.Errorf("Expected argsort length 5, got %d", result.Len())
	}

	indices := result.Indices()
	// Sorted order: 1.0 (idx 3), 2.0 (idx 1), 5.0 (idx 0), 8.0 (idx 2), 9.0 (idx 4)
	expected := []uint32{3, 1, 0, 2, 4}
	for i, v := range expected {
		if indices[i] != v {
			t.Errorf("Argsort mismatch at %d: expected %d, got %d", i, v, indices[i])
		}
	}
}

func TestChunkedColumnF64_Empty(t *testing.T) {
	col := NewChunkedColumnF64(nil)
	if col == nil {
		t.Fatal("Failed to create empty chunked column")
	}

	if col.Len() != 0 {
		t.Errorf("Expected empty column length 0, got %d", col.Len())
	}

	if col.Sum() != 0 {
		t.Errorf("Expected empty sum 0, got %f", col.Sum())
	}
}

// Benchmarks

func BenchmarkChunkedColumnF64_Sum_1M(b *testing.B) {
	n := 1_000_000
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(i)
	}
	col := NewChunkedColumnF64(data)
	if col == nil {
		b.Fatal("Failed to create chunked column")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = col.Sum()
	}
}

func BenchmarkChunkedColumnF64_Sort_1M(b *testing.B) {
	n := 1_000_000
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(n - i) // Reverse sorted
	}
	col := NewChunkedColumnF64(data)
	if col == nil {
		b.Fatal("Failed to create chunked column")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sorted := col.Sort()
		if sorted == nil {
			b.Fatal("Sort returned nil")
		}
	}
}

func BenchmarkChunkedColumnF64_FilterGt_1M(b *testing.B) {
	n := 1_000_000
	data := make([]float64, n)
	for i := range data {
		data[i] = float64(i)
	}
	col := NewChunkedColumnF64(data)
	if col == nil {
		b.Fatal("Failed to create chunked column")
	}

	threshold := float64(n / 2) // Filter 50%

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filtered := col.FilterGt(threshold)
		if filtered == nil {
			b.Fatal("Filter returned nil")
		}
	}
}
