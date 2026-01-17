package galleon

import (
	"math"
	"testing"
)

func TestArrowSeriesF64Sum(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	sum := s.Sum()
	expected := 15.0
	if math.Abs(sum-expected) > 0.0001 {
		t.Errorf("Sum() = %v, want %v", sum, expected)
	}
}

func TestArrowSeriesF64Min(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	min := s.Min()
	expected := 1.0
	if math.Abs(min-expected) > 0.0001 {
		t.Errorf("Min() = %v, want %v", min, expected)
	}
}

func TestArrowSeriesF64Max(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	max := s.Max()
	expected := 9.0
	if math.Abs(max-expected) > 0.0001 {
		t.Errorf("Max() = %v, want %v", max, expected)
	}
}

func TestArrowSeriesF64Mean(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mean := s.Mean()
	expected := 3.0
	if math.Abs(mean-expected) > 0.0001 {
		t.Errorf("Mean() = %v, want %v", mean, expected)
	}
}

func TestArrowSeriesI64Sum(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	s := NewSeriesI64("test", data)
	defer s.Release()

	sum := s.SumI64()
	expected := int64(15)
	if sum != expected {
		t.Errorf("SumI64() = %v, want %v", sum, expected)
	}
}

func TestArrowSeriesI64Min(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9}
	s := NewSeriesI64("test", data)
	defer s.Release()

	min := s.MinI64()
	expected := int64(1)
	if min != expected {
		t.Errorf("MinI64() = %v, want %v", min, expected)
	}
}

func TestArrowSeriesI64Max(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9}
	s := NewSeriesI64("test", data)
	defer s.Release()

	max := s.MaxI64()
	expected := int64(9)
	if max != expected {
		t.Errorf("MaxI64() = %v, want %v", max, expected)
	}
}

func TestArrowSeriesWithNulls(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	valid := []bool{true, true, false, true, true} // index 2 is null
	s := NewSeriesF64WithNulls("test", data, valid)
	defer s.Release()

	// Sum should skip the null value (3.0)
	// Expected: 1 + 2 + 4 + 5 = 12
	sum := s.Sum()
	expected := 12.0
	if math.Abs(sum-expected) > 0.0001 {
		t.Errorf("Sum() with nulls = %v, want %v", sum, expected)
	}

	// Verify null count
	if s.NullCount() != 1 {
		t.Errorf("NullCount() = %v, want 1", s.NullCount())
	}

	// Verify has nulls
	if !s.HasNulls() {
		t.Error("HasNulls() = false, want true")
	}
}

func TestArrowSeriesI64WithNulls(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	valid := []bool{true, false, true, false, true} // index 1,3 are null
	s := NewSeriesI64WithNulls("test", data, valid)
	defer s.Release()

	// Sum should skip null values
	// Expected: 1 + 3 + 5 = 9
	sum := s.SumI64()
	expected := int64(9)
	if sum != expected {
		t.Errorf("SumI64() with nulls = %v, want %v", sum, expected)
	}

	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %v, want 2", s.NullCount())
	}
}

func TestArrowSeriesLen(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	if s.Len() != 3 {
		t.Errorf("Len() = %v, want 3", s.Len())
	}
}

func TestArrowSeriesName(t *testing.T) {
	s := NewSeriesF64("my_series", []float64{1.0, 2.0})
	defer s.Release()

	if s.Name() != "my_series" {
		t.Errorf("Name() = %v, want 'my_series'", s.Name())
	}
}

func TestArrowSeriesDType(t *testing.T) {
	s64 := NewSeriesF64("f64", []float64{1.0})
	defer s64.Release()
	if s64.DType() != Float64 {
		t.Errorf("DType() = %v, want Float64", s64.DType())
	}

	i64 := NewSeriesI64("i64", []int64{1})
	defer i64.Release()
	if i64.DType() != Int64 {
		t.Errorf("DType() = %v, want Int64", i64.DType())
	}
}

func TestArrowSeriesEmpty(t *testing.T) {
	s := NewSeriesF64("empty", []float64{})
	defer s.Release()

	if s.Len() != 0 {
		t.Errorf("Len() = %v, want 0", s.Len())
	}

	if s.Sum() != 0 {
		t.Errorf("Sum() = %v, want 0", s.Sum())
	}
}

func TestArrowSeriesLargeDataset(t *testing.T) {
	// Test with larger dataset to verify SIMD performance
	n := 100000
	data := make([]float64, n)
	var expectedSum float64
	for i := 0; i < n; i++ {
		data[i] = float64(i)
		expectedSum += float64(i)
	}

	s := NewSeriesF64("large", data)
	defer s.Release()

	sum := s.Sum()
	if math.Abs(sum-expectedSum) > 0.1 {
		t.Errorf("Sum() on large dataset = %v, want %v", sum, expectedSum)
	}

	min := s.Min()
	if min != 0.0 {
		t.Errorf("Min() on large dataset = %v, want 0.0", min)
	}

	max := s.Max()
	expectedMax := float64(n - 1)
	if max != expectedMax {
		t.Errorf("Max() on large dataset = %v, want %v", max, expectedMax)
	}
}

// Sort tests
func TestArrowSeriesSortF64Ascending(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	// Verify min is first
	if sorted.Min() != 1.0 {
		t.Errorf("Sorted min = %v, want 1.0", sorted.Min())
	}

	// Verify max is last
	if sorted.Max() != 9.0 {
		t.Errorf("Sorted max = %v, want 9.0", sorted.Max())
	}

	// Verify sum unchanged
	if math.Abs(sorted.Sum()-s.Sum()) > 0.0001 {
		t.Errorf("Sorted sum = %v, want %v", sorted.Sum(), s.Sum())
	}
}

func TestArrowSeriesSortF64Descending(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	sorted := s.SortDesc()
	defer sorted.Release()

	// Verify max is first (descending)
	if sorted.Max() != 9.0 {
		t.Errorf("Sorted max = %v, want 9.0", sorted.Max())
	}

	// Verify sum unchanged
	if math.Abs(sorted.Sum()-s.Sum()) > 0.0001 {
		t.Errorf("Sorted sum = %v, want %v", sorted.Sum(), s.Sum())
	}
}

func TestArrowSeriesSortI64(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9}
	s := NewSeriesI64("test", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	if sorted.MinI64() != 1 {
		t.Errorf("Sorted min = %v, want 1", sorted.MinI64())
	}

	if sorted.MaxI64() != 9 {
		t.Errorf("Sorted max = %v, want 9", sorted.MaxI64())
	}

	if sorted.SumI64() != s.SumI64() {
		t.Errorf("Sorted sum = %v, want %v", sorted.SumI64(), s.SumI64())
	}
}

func TestArrowSeriesArgsortF64(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	indices := s.Argsort(true) // ascending
	if len(indices) != 5 {
		t.Fatalf("Argsort length = %v, want 5", len(indices))
	}

	// For data [5, 2, 8, 1, 9], sorted ascending indices should be [3, 1, 0, 2, 4]
	// which corresponds to values [1, 2, 5, 8, 9]
	expected := []uint32{3, 1, 0, 2, 4}
	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("Argsort[%d] = %v, want %v", i, idx, expected[i])
		}
	}
}

func TestArrowSeriesArgsortI64(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9}
	s := NewSeriesI64("test", data)
	defer s.Release()

	indices := s.Argsort(true)
	expected := []uint32{3, 1, 0, 2, 4}

	if len(indices) != len(expected) {
		t.Fatalf("Argsort length = %v, want %v", len(indices), len(expected))
	}

	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("Argsort[%d] = %v, want %v", i, idx, expected[i])
		}
	}
}

func TestArrowSeriesSortLargeDataset(t *testing.T) {
	n := 100000
	data := make([]float64, n)
	for i := 0; i < n; i++ {
		data[i] = float64(n - i - 1) // Reverse order
	}

	s := NewSeriesF64("large", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	// First element should be 0
	if sorted.Min() != 0.0 {
		t.Errorf("Sorted min = %v, want 0.0", sorted.Min())
	}

	// Last element should be n-1
	expectedMax := float64(n - 1)
	if sorted.Max() != expectedMax {
		t.Errorf("Sorted max = %v, want %v", sorted.Max(), expectedMax)
	}
}

// Benchmark tests
func BenchmarkArrowSeriesSumF64_1K(b *testing.B) {
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i)
	}
	s := NewSeriesF64("bench", data)
	defer s.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Sum()
	}
}

func BenchmarkArrowSeriesSumF64_100K(b *testing.B) {
	data := make([]float64, 100000)
	for i := range data {
		data[i] = float64(i)
	}
	s := NewSeriesF64("bench", data)
	defer s.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Sum()
	}
}

func BenchmarkArrowSeriesSumF64_1M(b *testing.B) {
	data := make([]float64, 1000000)
	for i := range data {
		data[i] = float64(i)
	}
	s := NewSeriesF64("bench", data)
	defer s.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Sum()
	}
}

// Compare ArrowSeries vs direct slice operations
func BenchmarkDirectSliceSum_1M(b *testing.B) {
	data := make([]float64, 1000000)
	for i := range data {
		data[i] = float64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SumF64(data)
	}
}

// --- Data Access Tests ---

func TestArrowSeriesAtF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	// Valid indices
	for i, expected := range data {
		val, ok := s.AtF64(i)
		if !ok {
			t.Errorf("AtF64(%d) returned not ok, want ok", i)
		}
		if val != expected {
			t.Errorf("AtF64(%d) = %v, want %v", i, val, expected)
		}
	}

	// Out of bounds
	_, ok := s.AtF64(-1)
	if ok {
		t.Error("AtF64(-1) should return not ok")
	}

	_, ok = s.AtF64(5)
	if ok {
		t.Error("AtF64(5) should return not ok for len=5")
	}
}

func TestArrowSeriesAtI64(t *testing.T) {
	data := []int64{10, 20, 30, 40, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	for i, expected := range data {
		val, ok := s.AtI64(i)
		if !ok {
			t.Errorf("AtI64(%d) returned not ok, want ok", i)
		}
		if val != expected {
			t.Errorf("AtI64(%d) = %v, want %v", i, val, expected)
		}
	}
}

func TestArrowSeriesAtWithNulls(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	valid := []bool{true, false, true, false, true} // indices 1,3 are null
	s := NewSeriesF64WithNulls("test", data, valid)
	defer s.Release()

	// Valid values
	val, ok := s.AtF64(0)
	if !ok || val != 1.0 {
		t.Errorf("AtF64(0) = (%v, %v), want (1.0, true)", val, ok)
	}

	val, ok = s.AtF64(2)
	if !ok || val != 3.0 {
		t.Errorf("AtF64(2) = (%v, %v), want (3.0, true)", val, ok)
	}

	// Null values
	_, ok = s.AtF64(1)
	if ok {
		t.Error("AtF64(1) should return not ok for null value")
	}

	_, ok = s.AtF64(3)
	if ok {
		t.Error("AtF64(3) should return not ok for null value")
	}
}

func TestArrowSeriesIsValid(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	valid := []bool{true, false, true}
	s := NewSeriesF64WithNulls("test", data, valid)
	defer s.Release()

	if !s.IsValid(0) {
		t.Error("IsValid(0) = false, want true")
	}
	if s.IsValid(1) {
		t.Error("IsValid(1) = true, want false")
	}
	if !s.IsValid(2) {
		t.Error("IsValid(2) = false, want true")
	}

	// Out of bounds
	if s.IsValid(-1) {
		t.Error("IsValid(-1) should return false")
	}
	if s.IsValid(3) {
		t.Error("IsValid(3) should return false")
	}
}

func TestArrowSeriesSliceF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	// Slice [1, 4)
	sliced := s.Slice(1, 4)
	defer sliced.Release()

	if sliced.Len() != 3 {
		t.Errorf("Slice(1,4).Len() = %d, want 3", sliced.Len())
	}

	expected := []float64{2.0, 3.0, 4.0}
	for i, want := range expected {
		got, ok := sliced.AtF64(i)
		if !ok || got != want {
			t.Errorf("Sliced[%d] = (%v, %v), want (%v, true)", i, got, ok, want)
		}
	}
}

func TestArrowSeriesSliceI64(t *testing.T) {
	data := []int64{10, 20, 30, 40, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	sliced := s.Slice(2, 5)
	defer sliced.Release()

	if sliced.Len() != 3 {
		t.Errorf("Slice(2,5).Len() = %d, want 3", sliced.Len())
	}

	expected := []int64{30, 40, 50}
	for i, want := range expected {
		got, ok := sliced.AtI64(i)
		if !ok || got != want {
			t.Errorf("Sliced[%d] = (%v, %v), want (%v, true)", i, got, ok, want)
		}
	}
}

func TestArrowSeriesSliceBounds(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	// Empty slice
	empty := s.Slice(2, 2)
	if empty.Len() != 0 {
		t.Errorf("Slice(2,2).Len() = %d, want 0", empty.Len())
	}

	// Negative start clamped to 0
	fromStart := s.Slice(-5, 2)
	defer fromStart.Release()
	if fromStart.Len() != 2 {
		t.Errorf("Slice(-5,2).Len() = %d, want 2", fromStart.Len())
	}

	// End past length clamped
	toEnd := s.Slice(1, 100)
	defer toEnd.Release()
	if toEnd.Len() != 2 {
		t.Errorf("Slice(1,100).Len() = %d, want 2", toEnd.Len())
	}

	// Start >= End returns empty
	reversed := s.Slice(3, 1)
	if reversed.Len() != 0 {
		t.Errorf("Slice(3,1).Len() = %d, want 0", reversed.Len())
	}
}

func TestArrowSeriesToFloat64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	result := s.ToFloat64()
	if len(result) != len(data) {
		t.Fatalf("ToFloat64() len = %d, want %d", len(result), len(data))
	}

	for i, want := range data {
		if result[i] != want {
			t.Errorf("ToFloat64()[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesToInt64(t *testing.T) {
	data := []int64{10, 20, 30, 40, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	result := s.ToInt64()
	if len(result) != len(data) {
		t.Fatalf("ToInt64() len = %d, want %d", len(result), len(data))
	}

	for i, want := range data {
		if result[i] != want {
			t.Errorf("ToInt64()[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesValues(t *testing.T) {
	// Float64
	f64Data := []float64{1.0, 2.0, 3.0}
	f64s := NewSeriesF64("f64", f64Data)
	defer f64s.Release()

	f64Result, ok := f64s.Values().([]float64)
	if !ok {
		t.Fatal("Values() for Float64 should return []float64")
	}
	if len(f64Result) != 3 {
		t.Errorf("Values() len = %d, want 3", len(f64Result))
	}

	// Int64
	i64Data := []int64{10, 20, 30}
	i64s := NewSeriesI64("i64", i64Data)
	defer i64s.Release()

	i64Result, ok := i64s.Values().([]int64)
	if !ok {
		t.Fatal("Values() for Int64 should return []int64")
	}
	if len(i64Result) != 3 {
		t.Errorf("Values() len = %d, want 3", len(i64Result))
	}
}

func TestArrowSeriesHead(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	head := s.Head(3)
	defer head.Release()

	if head.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", head.Len())
	}

	expected := []float64{1.0, 2.0, 3.0}
	result := head.ToFloat64()
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("Head(3)[%d] = %v, want %v", i, result[i], want)
		}
	}

	// Head larger than length
	headLarge := s.Head(100)
	defer headLarge.Release()
	if headLarge.Len() != 5 {
		t.Errorf("Head(100).Len() = %d, want 5", headLarge.Len())
	}
}

func TestArrowSeriesTail(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	tail := s.Tail(2)
	defer tail.Release()

	if tail.Len() != 2 {
		t.Errorf("Tail(2).Len() = %d, want 2", tail.Len())
	}

	expected := []float64{4.0, 5.0}
	result := tail.ToFloat64()
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("Tail(2)[%d] = %v, want %v", i, result[i], want)
		}
	}

	// Tail larger than length
	tailLarge := s.Tail(100)
	defer tailLarge.Release()
	if tailLarge.Len() != 5 {
		t.Errorf("Tail(100).Len() = %d, want 5", tailLarge.Len())
	}
}

func TestArrowSeriesEmptySlice(t *testing.T) {
	s := NewSeriesF64("empty", []float64{})
	defer s.Release()

	slice := s.Slice(0, 0)
	if slice.Len() != 0 {
		t.Errorf("Empty slice Len() = %d, want 0", slice.Len())
	}

	result := s.ToFloat64()
	if len(result) != 0 {
		t.Errorf("Empty ToFloat64() len = %d, want 0", len(result))
	}
}

// --- Filter Operation Tests ---

func TestArrowSeriesGtF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := s.GtF64(3.0)
	expected := []bool{false, false, false, true, true}

	if len(mask) != len(expected) {
		t.Fatalf("GtF64(3.0) mask len = %d, want %d", len(mask), len(expected))
	}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtF64(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesGeF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := s.GeF64(3.0)
	expected := []bool{false, false, true, true, true}

	if len(mask) != len(expected) {
		t.Fatalf("GeF64(3.0) mask len = %d, want %d", len(mask), len(expected))
	}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GeF64(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesLtF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := s.LtF64(3.0)
	expected := []bool{true, true, false, false, false}

	if len(mask) != len(expected) {
		t.Fatalf("LtF64(3.0) mask len = %d, want %d", len(mask), len(expected))
	}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("LtF64(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesLeF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := s.LeF64(3.0)
	expected := []bool{true, true, true, false, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("LeF64(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesEqF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 3.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := s.EqF64(3.0)
	expected := []bool{false, false, true, true, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("EqF64(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesNeF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 3.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := s.NeF64(3.0)
	expected := []bool{true, true, false, false, true}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("NeF64(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesGtI64(t *testing.T) {
	data := []int64{10, 20, 30, 40, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	mask := s.GtI64(30)
	expected := []bool{false, false, false, true, true}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtI64(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesLtI64(t *testing.T) {
	data := []int64{10, 20, 30, 40, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	mask := s.LtI64(30)
	expected := []bool{true, true, false, false, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("LtI64(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesEqI64(t *testing.T) {
	data := []int64{10, 20, 30, 30, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	mask := s.EqI64(30)
	expected := []bool{false, false, true, true, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("EqI64(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesFilterF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	// Filter to values > 2
	mask := s.GtF64(2.0)
	filtered := s.Filter(mask)
	defer filtered.Release()

	if filtered.Len() != 3 {
		t.Errorf("Filter(>2).Len() = %d, want 3", filtered.Len())
	}

	expected := []float64{3.0, 4.0, 5.0}
	result := filtered.ToFloat64()
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("Filtered[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesFilterI64(t *testing.T) {
	data := []int64{10, 20, 30, 40, 50}
	s := NewSeriesI64("test", data)
	defer s.Release()

	// Filter to values >= 30
	mask := s.GeI64(30)
	filtered := s.Filter(mask)
	defer filtered.Release()

	if filtered.Len() != 3 {
		t.Errorf("Filter(>=30).Len() = %d, want 3", filtered.Len())
	}

	expected := []int64{30, 40, 50}
	result := filtered.ToInt64()
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("Filtered[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesFilterAllTrue(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := []bool{true, true, true}
	filtered := s.Filter(mask)
	defer filtered.Release()

	if filtered.Len() != 3 {
		t.Errorf("Filter(all true).Len() = %d, want 3", filtered.Len())
	}

	if filtered.Sum() != s.Sum() {
		t.Errorf("Filter(all true).Sum() = %v, want %v", filtered.Sum(), s.Sum())
	}
}

func TestArrowSeriesFilterAllFalse(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	mask := []bool{false, false, false}
	filtered := s.Filter(mask)

	if filtered.Len() != 0 {
		t.Errorf("Filter(all false).Len() = %d, want 0", filtered.Len())
	}
}

func TestArrowSeriesFilterWithNulls(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	valid := []bool{true, false, true, false, true} // indices 1,3 are null
	s := NewSeriesF64WithNulls("test", data, valid)
	defer s.Release()

	// GtF64 should return false for null values
	mask := s.GtF64(2.0)
	// Expected: [false, false(null), true, false(null), true]
	expected := []bool{false, false, true, false, true}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtF64(2.0) with nulls[%d] = %v, want %v", i, mask[i], want)
		}
	}

	// Filter should preserve null handling
	filtered := s.Filter(mask)
	defer filtered.Release()

	// Only indices 2,4 pass the filter (values 3.0, 5.0)
	if filtered.Len() != 2 {
		t.Errorf("Filter with nulls Len() = %d, want 2", filtered.Len())
	}
}

func TestArrowSeriesFilterChained(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	// Chain filters: > 3 AND < 8
	mask1 := s.GtF64(3.0)
	filtered1 := s.Filter(mask1)
	defer filtered1.Release()

	mask2 := filtered1.LtF64(8.0)
	filtered2 := filtered1.Filter(mask2)
	defer filtered2.Release()

	if filtered2.Len() != 4 {
		t.Errorf("Chained filter Len() = %d, want 4", filtered2.Len())
	}

	expected := []float64{4.0, 5.0, 6.0, 7.0}
	result := filtered2.ToFloat64()
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("Chained filter[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesWhere(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF64("test", data)
	defer s.Release()

	// Where is an alias for Filter
	mask := s.GtF64(3.0)
	filtered := s.Where(mask)
	defer filtered.Release()

	if filtered.Len() != 2 {
		t.Errorf("Where(>3).Len() = %d, want 2", filtered.Len())
	}
}

func TestCountMask(t *testing.T) {
	mask := []bool{true, false, true, true, false}
	count := CountMask(mask)
	if count != 3 {
		t.Errorf("CountMask() = %d, want 3", count)
	}

	emptyMask := []bool{}
	if CountMask(emptyMask) != 0 {
		t.Error("CountMask(empty) should return 0")
	}

	allFalse := []bool{false, false, false}
	if CountMask(allFalse) != 0 {
		t.Error("CountMask(all false) should return 0")
	}

	allTrue := []bool{true, true, true}
	if CountMask(allTrue) != 3 {
		t.Error("CountMask(all true) should return 3")
	}
}

func TestArrowSeriesFilterEmpty(t *testing.T) {
	s := NewSeriesF64("empty", []float64{})
	defer s.Release()

	mask := []bool{}
	filtered := s.Filter(mask)

	if filtered.Len() != 0 {
		t.Errorf("Filter on empty series Len() = %d, want 0", filtered.Len())
	}
}

func TestArrowSeriesFilterLargeDataset(t *testing.T) {
	n := 100000
	data := make([]float64, n)
	for i := 0; i < n; i++ {
		data[i] = float64(i)
	}

	s := NewSeriesF64("large", data)
	defer s.Release()

	// Filter to values >= 50000
	mask := s.GeF64(50000)
	filtered := s.Filter(mask)
	defer filtered.Release()

	expectedLen := 50000
	if filtered.Len() != expectedLen {
		t.Errorf("Large filter Len() = %d, want %d", filtered.Len(), expectedLen)
	}

	// Verify first and last values
	first, ok := filtered.AtF64(0)
	if !ok || first != 50000.0 {
		t.Errorf("Large filter first = %v, want 50000.0", first)
	}

	last, ok := filtered.AtF64(filtered.Len() - 1)
	if !ok || last != float64(n-1) {
		t.Errorf("Large filter last = %v, want %v", last, float64(n-1))
	}
}

// ============================================================================
// Arithmetic Tests
// ============================================================================

func TestArrowSeriesAddF64(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{10.0, 20.0, 30.0, 40.0, 50.0})
	defer s2.Release()

	result := s1.Add(s2)
	if result == nil {
		t.Fatal("Add returned nil")
	}
	defer result.Release()

	expected := []float64{11.0, 22.0, 33.0, 44.0, 55.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Add[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesSubF64(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{10.0, 20.0, 30.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{1.0, 5.0, 10.0})
	defer s2.Release()

	result := s1.Sub(s2)
	if result == nil {
		t.Fatal("Sub returned nil")
	}
	defer result.Release()

	expected := []float64{9.0, 15.0, 20.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Sub[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesMulF64(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{2.0, 3.0, 4.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{5.0, 6.0, 7.0})
	defer s2.Release()

	result := s1.Mul(s2)
	if result == nil {
		t.Fatal("Mul returned nil")
	}
	defer result.Release()

	expected := []float64{10.0, 18.0, 28.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Mul[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesDivF64(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{10.0, 20.0, 30.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{2.0, 4.0, 5.0})
	defer s2.Release()

	result := s1.Div(s2)
	if result == nil {
		t.Fatal("Div returned nil")
	}
	defer result.Release()

	expected := []float64{5.0, 5.0, 6.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Div[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesAddI64(t *testing.T) {
	s1 := NewSeriesI64("a", []int64{1, 2, 3, 4, 5})
	defer s1.Release()
	s2 := NewSeriesI64("b", []int64{10, 20, 30, 40, 50})
	defer s2.Release()

	result := s1.Add(s2)
	if result == nil {
		t.Fatal("Add I64 returned nil")
	}
	defer result.Release()

	expected := []int64{11, 22, 33, 44, 55}
	values := result.ToInt64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Add I64[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesSubI64(t *testing.T) {
	s1 := NewSeriesI64("a", []int64{100, 200, 300})
	defer s1.Release()
	s2 := NewSeriesI64("b", []int64{10, 25, 50})
	defer s2.Release()

	result := s1.Sub(s2)
	if result == nil {
		t.Fatal("Sub I64 returned nil")
	}
	defer result.Release()

	expected := []int64{90, 175, 250}
	values := result.ToInt64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Sub I64[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesMulI64(t *testing.T) {
	s1 := NewSeriesI64("a", []int64{2, 3, 4})
	defer s1.Release()
	s2 := NewSeriesI64("b", []int64{5, 6, 7})
	defer s2.Release()

	result := s1.Mul(s2)
	if result == nil {
		t.Fatal("Mul I64 returned nil")
	}
	defer result.Release()

	expected := []int64{10, 18, 28}
	values := result.ToInt64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Mul I64[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesAddScalarF64(t *testing.T) {
	s := NewSeriesF64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	defer s.Release()

	result := s.AddScalar(10.0)
	if result == nil {
		t.Fatal("AddScalar returned nil")
	}
	defer result.Release()

	expected := []float64{11.0, 12.0, 13.0, 14.0, 15.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("AddScalar[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesSubScalarF64(t *testing.T) {
	s := NewSeriesF64("a", []float64{10.0, 20.0, 30.0})
	defer s.Release()

	result := s.SubScalar(5.0)
	if result == nil {
		t.Fatal("SubScalar returned nil")
	}
	defer result.Release()

	expected := []float64{5.0, 15.0, 25.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("SubScalar[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesMulScalarF64(t *testing.T) {
	s := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s.Release()

	result := s.MulScalar(3.0)
	if result == nil {
		t.Fatal("MulScalar returned nil")
	}
	defer result.Release()

	expected := []float64{3.0, 6.0, 9.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("MulScalar[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesDivScalarF64(t *testing.T) {
	s := NewSeriesF64("a", []float64{10.0, 20.0, 30.0})
	defer s.Release()

	result := s.DivScalar(2.0)
	if result == nil {
		t.Fatal("DivScalar returned nil")
	}
	defer result.Release()

	expected := []float64{5.0, 10.0, 15.0}
	values := result.ToFloat64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("DivScalar[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesAddScalarI64(t *testing.T) {
	s := NewSeriesI64("a", []int64{1, 2, 3, 4, 5})
	defer s.Release()

	result := s.AddScalarI64(100)
	if result == nil {
		t.Fatal("AddScalarI64 returned nil")
	}
	defer result.Release()

	expected := []int64{101, 102, 103, 104, 105}
	values := result.ToInt64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("AddScalarI64[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesMulScalarI64(t *testing.T) {
	s := NewSeriesI64("a", []int64{2, 3, 4})
	defer s.Release()

	result := s.MulScalarI64(5)
	if result == nil {
		t.Fatal("MulScalarI64 returned nil")
	}
	defer result.Release()

	expected := []int64{10, 15, 20}
	values := result.ToInt64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("MulScalarI64[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowSeriesArithmeticWithNulls(t *testing.T) {
	// Test that nulls propagate correctly
	s1 := NewSeriesF64WithNulls("a", []float64{1.0, 2.0, 3.0, 4.0}, []bool{true, false, true, true})
	defer s1.Release()
	s2 := NewSeriesF64WithNulls("b", []float64{10.0, 20.0, 30.0, 40.0}, []bool{true, true, false, true})
	defer s2.Release()

	result := s1.Add(s2)
	if result == nil {
		t.Fatal("Add with nulls returned nil")
	}
	defer result.Release()

	// Expected: [11.0, null, null, 44.0]
	// Index 0: both valid -> valid
	// Index 1: s1 null -> null
	// Index 2: s2 null -> null
	// Index 3: both valid -> valid

	if result.IsValid(0) != true {
		t.Error("Add null propagation: index 0 should be valid")
	}
	if result.IsValid(1) != false {
		t.Error("Add null propagation: index 1 should be null (s1 was null)")
	}
	if result.IsValid(2) != false {
		t.Error("Add null propagation: index 2 should be null (s2 was null)")
	}
	if result.IsValid(3) != true {
		t.Error("Add null propagation: index 3 should be valid")
	}

	// Check values at valid positions
	v0, ok := result.AtF64(0)
	if !ok || v0 != 11.0 {
		t.Errorf("Add with nulls[0] = %v, want 11.0", v0)
	}

	v3, ok := result.AtF64(3)
	if !ok || v3 != 44.0 {
		t.Errorf("Add with nulls[3] = %v, want 44.0", v3)
	}
}

func TestArrowSeriesScalarWithNulls(t *testing.T) {
	s := NewSeriesF64WithNulls("a", []float64{1.0, 2.0, 3.0}, []bool{true, false, true})
	defer s.Release()

	result := s.AddScalar(10.0)
	if result == nil {
		t.Fatal("AddScalar with nulls returned nil")
	}
	defer result.Release()

	// Expected: [11.0, null, 13.0]
	if result.IsValid(0) != true {
		t.Error("Scalar with null: index 0 should be valid")
	}
	if result.IsValid(1) != false {
		t.Error("Scalar with null: index 1 should be null")
	}
	if result.IsValid(2) != true {
		t.Error("Scalar with null: index 2 should be valid")
	}

	v0, ok := result.AtF64(0)
	if !ok || v0 != 11.0 {
		t.Errorf("AddScalar with nulls[0] = %v, want 11.0", v0)
	}

	v2, ok := result.AtF64(2)
	if !ok || v2 != 13.0 {
		t.Errorf("AddScalar with nulls[2] = %v, want 13.0", v2)
	}
}

func TestArrowSeriesArithmeticLengthMismatch(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{1.0, 2.0})
	defer s2.Release()

	result := s1.Add(s2)
	if result != nil {
		t.Error("Add with mismatched lengths should return nil")
		result.Release()
	}
}

func TestArrowSeriesArithmeticTypeMismatch(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()
	s2 := NewSeriesI64("b", []int64{1, 2, 3})
	defer s2.Release()

	result := s1.Add(s2)
	if result != nil {
		t.Error("Add with mismatched types should return nil")
		result.Release()
	}
}

func TestArrowSeriesDivisionByZero(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{1.0, 0.0, 2.0})
	defer s2.Release()

	result := s1.Div(s2)
	if result == nil {
		t.Fatal("Div with zero should not return nil (IEEE 754)")
	}
	defer result.Release()

	values := result.ToFloat64()
	// Division by zero should produce +Inf
	if values[0] != 1.0 {
		t.Errorf("Div[0] = %v, want 1.0", values[0])
	}
	if !math.IsInf(values[1], 1) {
		t.Errorf("Div[1] = %v, want +Inf", values[1])
	}
	if values[2] != 1.5 {
		t.Errorf("Div[2] = %v, want 1.5", values[2])
	}
}

// ============================================================================
// Float32 (F32) Tests
// ============================================================================

func TestArrowSeriesF32Sum(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	sum := s.Sum()
	expected := 15.0
	if math.Abs(sum-expected) > 0.0001 {
		t.Errorf("Sum() = %v, want %v", sum, expected)
	}
}

func TestArrowSeriesF32Min(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	min := s.Min()
	expected := 1.0
	if min != expected {
		t.Errorf("Min() = %v, want %v", min, expected)
	}
}

func TestArrowSeriesF32Max(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	max := s.Max()
	expected := 9.0
	if max != expected {
		t.Errorf("Max() = %v, want %v", max, expected)
	}
}

func TestArrowSeriesF32Mean(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	mean := s.Mean()
	expected := 3.0
	if math.Abs(mean-expected) > 0.0001 {
		t.Errorf("Mean() = %v, want %v", mean, expected)
	}
}

func TestArrowSeriesF32WithNulls(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	valid := []bool{true, true, false, true, true} // index 2 is null
	s := NewSeriesF32WithNulls("test", data, valid)
	defer s.Release()

	// Sum should skip the null value (3.0)
	// Expected: 1 + 2 + 4 + 5 = 12
	sum := s.Sum()
	expected := 12.0
	if math.Abs(sum-expected) > 0.0001 {
		t.Errorf("Sum() with nulls = %v, want %v", sum, expected)
	}

	if s.NullCount() != 1 {
		t.Errorf("NullCount() = %v, want 1", s.NullCount())
	}
}

func TestArrowSeriesF32DType(t *testing.T) {
	s := NewSeriesF32("f32", []float32{1.0})
	defer s.Release()
	if s.DType() != Float32 {
		t.Errorf("DType() = %v, want Float32", s.DType())
	}
}

func TestArrowSeriesAtF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	for i, expected := range data {
		val, ok := s.AtF32(i)
		if !ok {
			t.Errorf("AtF32(%d) returned not ok, want ok", i)
		}
		if val != expected {
			t.Errorf("AtF32(%d) = %v, want %v", i, val, expected)
		}
	}

	// Out of bounds
	_, ok := s.AtF32(-1)
	if ok {
		t.Error("AtF32(-1) should return not ok")
	}

	_, ok = s.AtF32(5)
	if ok {
		t.Error("AtF32(5) should return not ok for len=5")
	}
}

func TestArrowSeriesToFloat32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	result := s.ToFloat32()
	if len(result) != len(data) {
		t.Fatalf("ToFloat32() len = %d, want %d", len(result), len(data))
	}

	for i, want := range data {
		if result[i] != want {
			t.Errorf("ToFloat32()[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesF32Values(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0}
	s := NewSeriesF32("f32", data)
	defer s.Release()

	result, ok := s.Values().([]float32)
	if !ok {
		t.Fatal("Values() for Float32 should return []float32")
	}
	if len(result) != 3 {
		t.Errorf("Values() len = %d, want 3", len(result))
	}
}

func TestArrowSeriesSortF32(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	if sorted.Min() != 1.0 {
		t.Errorf("Sorted min = %v, want 1.0", sorted.Min())
	}

	if sorted.Max() != 9.0 {
		t.Errorf("Sorted max = %v, want 9.0", sorted.Max())
	}
}

func TestArrowSeriesArgsortF32(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	indices := s.Argsort(true) // ascending
	if len(indices) != 5 {
		t.Fatalf("Argsort length = %v, want 5", len(indices))
	}

	// For data [5, 2, 8, 1, 9], sorted ascending indices should be [3, 1, 0, 2, 4]
	expected := []uint32{3, 1, 0, 2, 4}
	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("Argsort F32[%d] = %v, want %v", i, idx, expected[i])
		}
	}
}

func TestArrowSeriesSliceF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	sliced := s.Slice(1, 4)
	defer sliced.Release()

	if sliced.Len() != 3 {
		t.Errorf("Slice(1,4).Len() = %d, want 3", sliced.Len())
	}

	expected := []float32{2.0, 3.0, 4.0}
	for i, want := range expected {
		got, ok := sliced.AtF32(i)
		if !ok || got != want {
			t.Errorf("Sliced F32[%d] = (%v, %v), want (%v, true)", i, got, ok, want)
		}
	}
}

func TestArrowSeriesGtF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	mask := s.GtF32(3.0)
	expected := []bool{false, false, false, true, true}

	if len(mask) != len(expected) {
		t.Fatalf("GtF32(3.0) mask len = %d, want %d", len(mask), len(expected))
	}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtF32(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesEqF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 3.0, 5.0}
	s := NewSeriesF32("test", data)
	defer s.Release()

	mask := s.EqF32(3.0)
	expected := []bool{false, false, true, true, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("EqF32(3.0)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

// ============================================================================
// Int32 (I32) Tests
// ============================================================================

func TestArrowSeriesI32Sum(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	s := NewSeriesI32("test", data)
	defer s.Release()

	sum := s.Sum()
	expected := 15.0
	if sum != expected {
		t.Errorf("Sum() = %v, want %v", sum, expected)
	}
}

func TestArrowSeriesI32Min(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	s := NewSeriesI32("test", data)
	defer s.Release()

	min := s.Min()
	expected := 1.0
	if min != expected {
		t.Errorf("Min() = %v, want %v", min, expected)
	}
}

func TestArrowSeriesI32Max(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	s := NewSeriesI32("test", data)
	defer s.Release()

	max := s.Max()
	expected := 9.0
	if max != expected {
		t.Errorf("Max() = %v, want %v", max, expected)
	}
}

func TestArrowSeriesI32WithNulls(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	valid := []bool{true, false, true, false, true} // index 1,3 are null
	s := NewSeriesI32WithNulls("test", data, valid)
	defer s.Release()

	// Sum should skip null values: 1 + 3 + 5 = 9
	sum := s.Sum()
	expected := 9.0
	if sum != expected {
		t.Errorf("Sum() with nulls = %v, want %v", sum, expected)
	}

	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %v, want 2", s.NullCount())
	}
}

func TestArrowSeriesI32DType(t *testing.T) {
	s := NewSeriesI32("i32", []int32{1})
	defer s.Release()
	if s.DType() != Int32 {
		t.Errorf("DType() = %v, want Int32", s.DType())
	}
}

func TestArrowSeriesAtI32(t *testing.T) {
	data := []int32{10, 20, 30, 40, 50}
	s := NewSeriesI32("test", data)
	defer s.Release()

	for i, expected := range data {
		val, ok := s.AtI32(i)
		if !ok {
			t.Errorf("AtI32(%d) returned not ok, want ok", i)
		}
		if val != expected {
			t.Errorf("AtI32(%d) = %v, want %v", i, val, expected)
		}
	}
}

func TestArrowSeriesToInt32(t *testing.T) {
	data := []int32{10, 20, 30, 40, 50}
	s := NewSeriesI32("test", data)
	defer s.Release()

	result := s.ToInt32()
	if len(result) != len(data) {
		t.Fatalf("ToInt32() len = %d, want %d", len(result), len(data))
	}

	for i, want := range data {
		if result[i] != want {
			t.Errorf("ToInt32()[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesI32Values(t *testing.T) {
	data := []int32{10, 20, 30}
	s := NewSeriesI32("i32", data)
	defer s.Release()

	result, ok := s.Values().([]int32)
	if !ok {
		t.Fatal("Values() for Int32 should return []int32")
	}
	if len(result) != 3 {
		t.Errorf("Values() len = %d, want 3", len(result))
	}
}

func TestArrowSeriesSortI32(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	s := NewSeriesI32("test", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	if sorted.Min() != 1 {
		t.Errorf("Sorted min = %v, want 1", sorted.Min())
	}

	if sorted.Max() != 9 {
		t.Errorf("Sorted max = %v, want 9", sorted.Max())
	}
}

func TestArrowSeriesArgsortI32(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	s := NewSeriesI32("test", data)
	defer s.Release()

	indices := s.Argsort(true) // ascending
	expected := []uint32{3, 1, 0, 2, 4}

	if len(indices) != len(expected) {
		t.Fatalf("Argsort I32 length = %v, want %v", len(indices), len(expected))
	}

	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("Argsort I32[%d] = %v, want %v", i, idx, expected[i])
		}
	}
}

func TestArrowSeriesSliceI32(t *testing.T) {
	data := []int32{10, 20, 30, 40, 50}
	s := NewSeriesI32("test", data)
	defer s.Release()

	sliced := s.Slice(2, 5)
	defer sliced.Release()

	if sliced.Len() != 3 {
		t.Errorf("Slice(2,5).Len() = %d, want 3", sliced.Len())
	}

	expected := []int32{30, 40, 50}
	for i, want := range expected {
		got, ok := sliced.AtI32(i)
		if !ok || got != want {
			t.Errorf("Sliced I32[%d] = (%v, %v), want (%v, true)", i, got, ok, want)
		}
	}
}

func TestArrowSeriesGtI32(t *testing.T) {
	data := []int32{10, 20, 30, 40, 50}
	s := NewSeriesI32("test", data)
	defer s.Release()

	mask := s.GtI32(30)
	expected := []bool{false, false, false, true, true}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtI32(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesEqI32(t *testing.T) {
	data := []int32{10, 20, 30, 30, 50}
	s := NewSeriesI32("test", data)
	defer s.Release()

	mask := s.EqI32(30)
	expected := []bool{false, false, true, true, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("EqI32(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

// ============================================================================
// UInt64 (U64) Tests
// ============================================================================

func TestArrowSeriesU64Sum(t *testing.T) {
	data := []uint64{1, 2, 3, 4, 5}
	s := NewSeriesU64("test", data)
	defer s.Release()

	sum := s.Sum()
	expected := 15.0
	if sum != expected {
		t.Errorf("Sum() = %v, want %v", sum, expected)
	}
}

func TestArrowSeriesU64Min(t *testing.T) {
	data := []uint64{5, 2, 8, 1, 9}
	s := NewSeriesU64("test", data)
	defer s.Release()

	min := s.Min()
	expected := 1.0
	if min != expected {
		t.Errorf("Min() = %v, want %v", min, expected)
	}
}

func TestArrowSeriesU64Max(t *testing.T) {
	data := []uint64{5, 2, 8, 1, 9}
	s := NewSeriesU64("test", data)
	defer s.Release()

	max := s.Max()
	expected := 9.0
	if max != expected {
		t.Errorf("Max() = %v, want %v", max, expected)
	}
}

func TestArrowSeriesU64WithNulls(t *testing.T) {
	data := []uint64{1, 2, 3, 4, 5}
	valid := []bool{true, false, true, false, true} // index 1,3 are null
	s := NewSeriesU64WithNulls("test", data, valid)
	defer s.Release()

	// Sum should skip null values: 1 + 3 + 5 = 9
	sum := s.Sum()
	expected := 9.0
	if sum != expected {
		t.Errorf("Sum() with nulls = %v, want %v", sum, expected)
	}

	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %v, want 2", s.NullCount())
	}
}

func TestArrowSeriesU64DType(t *testing.T) {
	s := NewSeriesU64("u64", []uint64{1})
	defer s.Release()
	if s.DType() != UInt64 {
		t.Errorf("DType() = %v, want UInt64", s.DType())
	}
}

func TestArrowSeriesAtU64(t *testing.T) {
	data := []uint64{10, 20, 30, 40, 50}
	s := NewSeriesU64("test", data)
	defer s.Release()

	for i, expected := range data {
		val, ok := s.AtU64(i)
		if !ok {
			t.Errorf("AtU64(%d) returned not ok, want ok", i)
		}
		if val != expected {
			t.Errorf("AtU64(%d) = %v, want %v", i, val, expected)
		}
	}
}

func TestArrowSeriesToUInt64(t *testing.T) {
	data := []uint64{10, 20, 30, 40, 50}
	s := NewSeriesU64("test", data)
	defer s.Release()

	result := s.ToUInt64()
	if len(result) != len(data) {
		t.Fatalf("ToUInt64() len = %d, want %d", len(result), len(data))
	}

	for i, want := range data {
		if result[i] != want {
			t.Errorf("ToUInt64()[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesU64Values(t *testing.T) {
	data := []uint64{10, 20, 30}
	s := NewSeriesU64("u64", data)
	defer s.Release()

	result, ok := s.Values().([]uint64)
	if !ok {
		t.Fatal("Values() for UInt64 should return []uint64")
	}
	if len(result) != 3 {
		t.Errorf("Values() len = %d, want 3", len(result))
	}
}

func TestArrowSeriesSortU64(t *testing.T) {
	data := []uint64{5, 2, 8, 1, 9}
	s := NewSeriesU64("test", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	if sorted.Min() != 1 {
		t.Errorf("Sorted min = %v, want 1", sorted.Min())
	}

	if sorted.Max() != 9 {
		t.Errorf("Sorted max = %v, want 9", sorted.Max())
	}
}

func TestArrowSeriesArgsortU64(t *testing.T) {
	data := []uint64{5, 2, 8, 1, 9}
	s := NewSeriesU64("test", data)
	defer s.Release()

	indices := s.Argsort(true) // ascending
	expected := []uint32{3, 1, 0, 2, 4}

	if len(indices) != len(expected) {
		t.Fatalf("Argsort U64 length = %v, want %v", len(indices), len(expected))
	}

	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("Argsort U64[%d] = %v, want %v", i, idx, expected[i])
		}
	}
}

func TestArrowSeriesSliceU64(t *testing.T) {
	data := []uint64{10, 20, 30, 40, 50}
	s := NewSeriesU64("test", data)
	defer s.Release()

	sliced := s.Slice(2, 5)
	defer sliced.Release()

	if sliced.Len() != 3 {
		t.Errorf("Slice(2,5).Len() = %d, want 3", sliced.Len())
	}

	expected := []uint64{30, 40, 50}
	for i, want := range expected {
		got, ok := sliced.AtU64(i)
		if !ok || got != want {
			t.Errorf("Sliced U64[%d] = (%v, %v), want (%v, true)", i, got, ok, want)
		}
	}
}

func TestArrowSeriesGtU64(t *testing.T) {
	data := []uint64{10, 20, 30, 40, 50}
	s := NewSeriesU64("test", data)
	defer s.Release()

	mask := s.GtU64(30)
	expected := []bool{false, false, false, true, true}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtU64(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesEqU64(t *testing.T) {
	data := []uint64{10, 20, 30, 30, 50}
	s := NewSeriesU64("test", data)
	defer s.Release()

	mask := s.EqU64(30)
	expected := []bool{false, false, true, true, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("EqU64(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

// ============================================================================
// UInt32 (U32) Tests
// ============================================================================

func TestArrowSeriesU32Sum(t *testing.T) {
	data := []uint32{1, 2, 3, 4, 5}
	s := NewSeriesU32("test", data)
	defer s.Release()

	sum := s.Sum()
	expected := 15.0
	if sum != expected {
		t.Errorf("Sum() = %v, want %v", sum, expected)
	}
}

func TestArrowSeriesU32Min(t *testing.T) {
	data := []uint32{5, 2, 8, 1, 9}
	s := NewSeriesU32("test", data)
	defer s.Release()

	min := s.Min()
	expected := 1.0
	if min != expected {
		t.Errorf("Min() = %v, want %v", min, expected)
	}
}

func TestArrowSeriesU32Max(t *testing.T) {
	data := []uint32{5, 2, 8, 1, 9}
	s := NewSeriesU32("test", data)
	defer s.Release()

	max := s.Max()
	expected := 9.0
	if max != expected {
		t.Errorf("Max() = %v, want %v", max, expected)
	}
}

func TestArrowSeriesU32WithNulls(t *testing.T) {
	data := []uint32{1, 2, 3, 4, 5}
	valid := []bool{true, false, true, false, true} // index 1,3 are null
	s := NewSeriesU32WithNulls("test", data, valid)
	defer s.Release()

	// Sum should skip null values: 1 + 3 + 5 = 9
	sum := s.Sum()
	expected := 9.0
	if sum != expected {
		t.Errorf("Sum() with nulls = %v, want %v", sum, expected)
	}

	if s.NullCount() != 2 {
		t.Errorf("NullCount() = %v, want 2", s.NullCount())
	}
}

func TestArrowSeriesU32DType(t *testing.T) {
	s := NewSeriesU32("u32", []uint32{1})
	defer s.Release()
	if s.DType() != UInt32 {
		t.Errorf("DType() = %v, want UInt32", s.DType())
	}
}

func TestArrowSeriesAtU32(t *testing.T) {
	data := []uint32{10, 20, 30, 40, 50}
	s := NewSeriesU32("test", data)
	defer s.Release()

	for i, expected := range data {
		val, ok := s.AtU32(i)
		if !ok {
			t.Errorf("AtU32(%d) returned not ok, want ok", i)
		}
		if val != expected {
			t.Errorf("AtU32(%d) = %v, want %v", i, val, expected)
		}
	}
}

func TestArrowSeriesToUInt32(t *testing.T) {
	data := []uint32{10, 20, 30, 40, 50}
	s := NewSeriesU32("test", data)
	defer s.Release()

	result := s.ToUInt32()
	if len(result) != len(data) {
		t.Fatalf("ToUInt32() len = %d, want %d", len(result), len(data))
	}

	for i, want := range data {
		if result[i] != want {
			t.Errorf("ToUInt32()[%d] = %v, want %v", i, result[i], want)
		}
	}
}

func TestArrowSeriesU32Values(t *testing.T) {
	data := []uint32{10, 20, 30}
	s := NewSeriesU32("u32", data)
	defer s.Release()

	result, ok := s.Values().([]uint32)
	if !ok {
		t.Fatal("Values() for UInt32 should return []uint32")
	}
	if len(result) != 3 {
		t.Errorf("Values() len = %d, want 3", len(result))
	}
}

func TestArrowSeriesSortU32(t *testing.T) {
	data := []uint32{5, 2, 8, 1, 9}
	s := NewSeriesU32("test", data)
	defer s.Release()

	sorted := s.SortAsc()
	defer sorted.Release()

	if sorted.Min() != 1 {
		t.Errorf("Sorted min = %v, want 1", sorted.Min())
	}

	if sorted.Max() != 9 {
		t.Errorf("Sorted max = %v, want 9", sorted.Max())
	}
}

func TestArrowSeriesArgsortU32(t *testing.T) {
	data := []uint32{5, 2, 8, 1, 9}
	s := NewSeriesU32("test", data)
	defer s.Release()

	indices := s.Argsort(true) // ascending
	expected := []uint32{3, 1, 0, 2, 4}

	if len(indices) != len(expected) {
		t.Fatalf("Argsort U32 length = %v, want %v", len(indices), len(expected))
	}

	for i, idx := range indices {
		if idx != expected[i] {
			t.Errorf("Argsort U32[%d] = %v, want %v", i, idx, expected[i])
		}
	}
}

func TestArrowSeriesSliceU32(t *testing.T) {
	data := []uint32{10, 20, 30, 40, 50}
	s := NewSeriesU32("test", data)
	defer s.Release()

	sliced := s.Slice(2, 5)
	defer sliced.Release()

	if sliced.Len() != 3 {
		t.Errorf("Slice(2,5).Len() = %d, want 3", sliced.Len())
	}

	expected := []uint32{30, 40, 50}
	for i, want := range expected {
		got, ok := sliced.AtU32(i)
		if !ok || got != want {
			t.Errorf("Sliced U32[%d] = (%v, %v), want (%v, true)", i, got, ok, want)
		}
	}
}

func TestArrowSeriesGtU32(t *testing.T) {
	data := []uint32{10, 20, 30, 40, 50}
	s := NewSeriesU32("test", data)
	defer s.Release()

	mask := s.GtU32(30)
	expected := []bool{false, false, false, true, true}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("GtU32(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

func TestArrowSeriesEqU32(t *testing.T) {
	data := []uint32{10, 20, 30, 30, 50}
	s := NewSeriesU32("test", data)
	defer s.Release()

	mask := s.EqU32(30)
	expected := []bool{false, false, true, true, false}

	for i, want := range expected {
		if mask[i] != want {
			t.Errorf("EqU32(30)[%d] = %v, want %v", i, mask[i], want)
		}
	}
}

// ============================================================================
// Type Dispatch Tests (Sum, Min, Max, Mean with generic interface)
// ============================================================================

func TestArrowSeriesSumTypeDispatch(t *testing.T) {
	// F32 through generic Sum()
	f32s := NewSeriesF32("f32", []float32{1.0, 2.0, 3.0})
	defer f32s.Release()
	if f32s.Sum() != 6.0 {
		t.Errorf("F32 Sum() = %v, want 6.0", f32s.Sum())
	}

	// I32 through generic Sum()
	i32s := NewSeriesI32("i32", []int32{1, 2, 3})
	defer i32s.Release()
	if i32s.Sum() != 6.0 {
		t.Errorf("I32 Sum() = %v, want 6.0", i32s.Sum())
	}

	// U64 through generic Sum()
	u64s := NewSeriesU64("u64", []uint64{1, 2, 3})
	defer u64s.Release()
	if u64s.Sum() != 6.0 {
		t.Errorf("U64 Sum() = %v, want 6.0", u64s.Sum())
	}

	// U32 through generic Sum()
	u32s := NewSeriesU32("u32", []uint32{1, 2, 3})
	defer u32s.Release()
	if u32s.Sum() != 6.0 {
		t.Errorf("U32 Sum() = %v, want 6.0", u32s.Sum())
	}
}

func TestArrowSeriesMinMaxTypeDispatch(t *testing.T) {
	// F32
	f32s := NewSeriesF32("f32", []float32{3.0, 1.0, 2.0})
	defer f32s.Release()
	if f32s.Min() != 1.0 {
		t.Errorf("F32 Min() = %v, want 1.0", f32s.Min())
	}
	if f32s.Max() != 3.0 {
		t.Errorf("F32 Max() = %v, want 3.0", f32s.Max())
	}

	// I32
	i32s := NewSeriesI32("i32", []int32{3, 1, 2})
	defer i32s.Release()
	if i32s.Min() != 1.0 {
		t.Errorf("I32 Min() = %v, want 1.0", i32s.Min())
	}
	if i32s.Max() != 3.0 {
		t.Errorf("I32 Max() = %v, want 3.0", i32s.Max())
	}

	// U64
	u64s := NewSeriesU64("u64", []uint64{3, 1, 2})
	defer u64s.Release()
	if u64s.Min() != 1.0 {
		t.Errorf("U64 Min() = %v, want 1.0", u64s.Min())
	}
	if u64s.Max() != 3.0 {
		t.Errorf("U64 Max() = %v, want 3.0", u64s.Max())
	}

	// U32
	u32s := NewSeriesU32("u32", []uint32{3, 1, 2})
	defer u32s.Release()
	if u32s.Min() != 1.0 {
		t.Errorf("U32 Min() = %v, want 1.0", u32s.Min())
	}
	if u32s.Max() != 3.0 {
		t.Errorf("U32 Max() = %v, want 3.0", u32s.Max())
	}
}

func TestArrowSeriesMeanTypeDispatch(t *testing.T) {
	// F32
	f32s := NewSeriesF32("f32", []float32{1.0, 2.0, 3.0})
	defer f32s.Release()
	if math.Abs(f32s.Mean()-2.0) > 0.0001 {
		t.Errorf("F32 Mean() = %v, want 2.0", f32s.Mean())
	}

	// I32 (mean computed as float)
	i32s := NewSeriesI32("i32", []int32{1, 2, 3})
	defer i32s.Release()
	if math.Abs(i32s.Mean()-2.0) > 0.0001 {
		t.Errorf("I32 Mean() = %v, want 2.0", i32s.Mean())
	}

	// U64 (mean computed as float)
	u64s := NewSeriesU64("u64", []uint64{1, 2, 3})
	defer u64s.Release()
	if math.Abs(u64s.Mean()-2.0) > 0.0001 {
		t.Errorf("U64 Mean() = %v, want 2.0", u64s.Mean())
	}

	// U32 (mean computed as float)
	u32s := NewSeriesU32("u32", []uint32{1, 2, 3})
	defer u32s.Release()
	if math.Abs(u32s.Mean()-2.0) > 0.0001 {
		t.Errorf("U32 Mean() = %v, want 2.0", u32s.Mean())
	}
}
