package galleon

import (
	"math"
	"testing"
)

func TestColumnF64Creation(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create column")
	}

	if col.Len() != 5 {
		t.Errorf("Expected length 5, got %d", col.Len())
	}

	if col.Get(0) != 1.0 {
		t.Errorf("Expected 1.0 at index 0, got %f", col.Get(0))
	}

	if col.Get(4) != 5.0 {
		t.Errorf("Expected 5.0 at index 4, got %f", col.Get(4))
	}
}

func TestColumnF64Data(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create column")
	}

	view := col.Data()
	if len(view) != 5 {
		t.Errorf("Expected view length 5, got %d", len(view))
	}

	for i, v := range data {
		if view[i] != v {
			t.Errorf("Expected %f at index %d, got %f", v, i, view[i])
		}
	}
}

func TestColumnF64Aggregations(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewColumnF64(data)
	if col == nil {
		t.Fatal("Failed to create column")
	}

	if col.Sum() != 15.0 {
		t.Errorf("Expected sum 15.0, got %f", col.Sum())
	}

	if col.Min() != 1.0 {
		t.Errorf("Expected min 1.0, got %f", col.Min())
	}

	if col.Max() != 5.0 {
		t.Errorf("Expected max 5.0, got %f", col.Max())
	}

	if col.Mean() != 3.0 {
		t.Errorf("Expected mean 3.0, got %f", col.Mean())
	}
}

func TestSumF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	result := SumF64(data)
	if result != 55.0 {
		t.Errorf("Expected 55.0, got %f", result)
	}
}

func TestMinMaxF64(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0, 3.0}

	if MinF64(data) != 1.0 {
		t.Errorf("Expected min 1.0, got %f", MinF64(data))
	}

	if MaxF64(data) != 9.0 {
		t.Errorf("Expected max 9.0, got %f", MaxF64(data))
	}
}

func TestMeanF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	result := MeanF64(data)
	if result != 3.0 {
		t.Errorf("Expected 3.0, got %f", result)
	}
}

func TestAddScalarF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	AddScalarF64(data, 10.0)

	expected := []float64{11.0, 12.0, 13.0, 14.0, 15.0}
	for i, v := range expected {
		if math.Abs(data[i]-v) > 0.0001 {
			t.Errorf("Expected %f at index %d, got %f", v, i, data[i])
		}
	}
}

func TestMulScalarF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	MulScalarF64(data, 2.0)

	expected := []float64{2.0, 4.0, 6.0, 8.0, 10.0}
	for i, v := range expected {
		if math.Abs(data[i]-v) > 0.0001 {
			t.Errorf("Expected %f at index %d, got %f", v, i, data[i])
		}
	}
}

func TestFilterGreaterThanF64(t *testing.T) {
	data := []float64{1.0, 5.0, 2.0, 8.0, 3.0, 9.0}
	indices := FilterGreaterThanF64(data, 4.0)

	if len(indices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(indices))
	}

	expected := []uint32{1, 3, 5} // indices of 5.0, 8.0, 9.0
	for i, v := range expected {
		if indices[i] != v {
			t.Errorf("Expected index %d at position %d, got %d", v, i, indices[i])
		}
	}
}

func TestArgsortF64(t *testing.T) {
	data := []float64{3.0, 1.0, 4.0, 1.0, 5.0}
	indices := ArgsortF64(data, true)

	if len(indices) != 5 {
		t.Errorf("Expected 5 indices, got %d", len(indices))
	}

	// First two should be indices 1 and 3 (both have value 1.0)
	if indices[0] != 1 && indices[0] != 3 {
		t.Errorf("Expected first index to be 1 or 3, got %d", indices[0])
	}

	// Index 2 should be next (value 3.0)
	if indices[2] != 0 {
		t.Errorf("Expected index 0 at position 2, got %d", indices[2])
	}
}

func TestEmptySlice(t *testing.T) {
	var empty []float64

	if SumF64(empty) != 0 {
		t.Error("Expected 0 for empty slice sum")
	}

	if MinF64(empty) != 0 {
		t.Error("Expected 0 for empty slice min")
	}

	if MaxF64(empty) != 0 {
		t.Error("Expected 0 for empty slice max")
	}

	if MeanF64(empty) != 0 {
		t.Error("Expected 0 for empty slice mean")
	}

	// Should not panic
	AddScalarF64(empty, 10.0)
	MulScalarF64(empty, 2.0)

	if FilterGreaterThanF64(empty, 0) != nil {
		t.Error("Expected nil for empty slice filter")
	}

	if ArgsortF64(empty, true) != nil {
		t.Error("Expected nil for empty slice argsort")
	}
}

// Benchmark tests
func BenchmarkSumF64_SIMD(b *testing.B) {
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i%1000) + 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SumF64(data)
	}
}

func BenchmarkSumF64_Native(b *testing.B) {
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i%1000) + 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sum float64
		for _, v := range data {
			sum += v
		}
		_ = sum
	}
}

func BenchmarkMinF64_SIMD(b *testing.B) {
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i%1000) + 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MinF64(data)
	}
}

func BenchmarkMinF64_Native(b *testing.B) {
	data := make([]float64, 1_000_000)
	for i := range data {
		data[i] = float64(i%1000) + 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		min := data[0]
		for _, v := range data[1:] {
			if v < min {
				min = v
			}
		}
		_ = min
	}
}
