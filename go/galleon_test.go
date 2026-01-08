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

func TestEmptySlice_I64(t *testing.T) {
	var empty []int64

	if SumI64(empty) != 0 {
		t.Error("Expected 0 for empty I64 slice sum")
	}
	if MinI64(empty) != 0 {
		t.Error("Expected 0 for empty I64 slice min")
	}
	if MaxI64(empty) != 0 {
		t.Error("Expected 0 for empty I64 slice max")
	}
	AddScalarI64(empty, 10)
	MulScalarI64(empty, 2)
	if FilterGreaterThanI64(empty, 0) != nil {
		t.Error("Expected nil for empty I64 slice filter")
	}
	if ArgsortI64(empty, true) != nil {
		t.Error("Expected nil for empty I64 slice argsort")
	}
}

func TestEmptySlice_I32(t *testing.T) {
	var empty []int32

	if SumI32(empty) != 0 {
		t.Error("Expected 0 for empty I32 slice sum")
	}
	if MinI32(empty) != 0 {
		t.Error("Expected 0 for empty I32 slice min")
	}
	if MaxI32(empty) != 0 {
		t.Error("Expected 0 for empty I32 slice max")
	}
	AddScalarI32(empty, 10)
	MulScalarI32(empty, 2)
	if FilterGreaterThanI32(empty, 0) != nil {
		t.Error("Expected nil for empty I32 slice filter")
	}
	if ArgsortI32(empty, true) != nil {
		t.Error("Expected nil for empty I32 slice argsort")
	}
}

func TestEmptySlice_F32(t *testing.T) {
	var empty []float32

	if SumF32(empty) != 0 {
		t.Error("Expected 0 for empty F32 slice sum")
	}
	if MinF32(empty) != 0 {
		t.Error("Expected 0 for empty F32 slice min")
	}
	if MaxF32(empty) != 0 {
		t.Error("Expected 0 for empty F32 slice max")
	}
	if MeanF32(empty) != 0 {
		t.Error("Expected 0 for empty F32 slice mean")
	}
	AddScalarF32(empty, 10.0)
	MulScalarF32(empty, 2.0)
	if FilterGreaterThanF32(empty, 0) != nil {
		t.Error("Expected nil for empty F32 slice filter")
	}
	if ArgsortF32(empty, true) != nil {
		t.Error("Expected nil for empty F32 slice argsort")
	}
}

func TestEmptySlice_Bool(t *testing.T) {
	var empty []bool

	if CountTrue(empty) != 0 {
		t.Error("Expected 0 for empty bool slice CountTrue")
	}
	if CountFalse(empty) != 0 {
		t.Error("Expected 0 for empty bool slice CountFalse")
	}
}

func TestVectorOps_EmptyAndShort(t *testing.T) {
	// Test empty slice cases for vector operations
	var emptyF64 []float64
	var emptyI64 []int64
	outF64 := make([]float64, 3)
	outI64 := make([]int64, 3)
	outByte := make([]byte, 3)

	// Empty 'a' array - should return early
	AddF64(emptyF64, []float64{1, 2, 3}, outF64)
	SubF64(emptyF64, []float64{1, 2, 3}, outF64)
	MulF64(emptyF64, []float64{1, 2, 3}, outF64)
	DivF64(emptyF64, []float64{1, 2, 3}, outF64)
	AddI64(emptyI64, []int64{1, 2, 3}, outI64)
	SubI64(emptyI64, []int64{1, 2, 3}, outI64)
	MulI64(emptyI64, []int64{1, 2, 3}, outI64)

	// Comparison ops with empty
	CmpGtF64(emptyF64, []float64{1, 2, 3}, outByte)
	CmpGeF64(emptyF64, []float64{1, 2, 3}, outByte)
	CmpLtF64(emptyF64, []float64{1, 2, 3}, outByte)
	CmpLeF64(emptyF64, []float64{1, 2, 3}, outByte)
	CmpEqF64(emptyF64, []float64{1, 2, 3}, outByte)
	CmpNeF64(emptyF64, []float64{1, 2, 3}, outByte)

	// Test short 'b' array - should return early
	a := []float64{1, 2, 3}
	shortB := []float64{1}
	AddF64(a, shortB, outF64)
	SubF64(a, shortB, outF64)

	// Test short 'out' array - should return early
	shortOut := make([]float64, 1)
	AddF64(a, a, shortOut)
	SubF64(a, a, shortOut)

	// Empty mask tests
	emptyMask := []byte{}
	if CountMaskTrue(emptyMask) != 0 {
		t.Error("Expected 0 for empty mask")
	}
	emptyOutU32 := []uint32{}
	if IndicesFromMask(emptyMask, emptyOutU32) != 0 {
		t.Error("Expected 0 for IndicesFromMask with empty mask")
	}
}

func TestGroupByAgg_Empty(t *testing.T) {
	// Test empty slice cases for groupby aggregations
	var emptyF64 []float64
	var emptyI64 []int64
	var emptyU32 []uint32

	AggregateSumF64ByGroup(emptyF64, emptyU32, emptyF64)
	AggregateSumI64ByGroup(emptyI64, emptyU32, emptyI64)
	AggregateMinF64ByGroup(emptyF64, emptyU32, emptyF64)
	AggregateMinI64ByGroup(emptyI64, emptyU32, emptyI64)
	AggregateMaxF64ByGroup(emptyF64, emptyU32, emptyF64)
	AggregateMaxI64ByGroup(emptyI64, emptyU32, emptyI64)
	CountByGroup(emptyU32, []uint64{})

	// Hash functions with empty
	HashI64Column(emptyI64, []uint64{})
	HashI32Column([]int32{}, []uint64{})
	HashF64Column(emptyF64, []uint64{})
	HashF32Column([]float32{}, []uint64{})
	CombineHashes([]uint64{}, []uint64{}, []uint64{})

	// Gather with empty
	GatherF64(emptyF64, []int32{}, emptyF64)
	GatherI64(emptyI64, []int32{}, emptyI64)
	GatherI32([]int32{}, []int32{}, []int32{})
	GatherF32([]float32{}, []int32{}, []float32{})
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

// ============================================================================
// Thread Configuration Tests
// ============================================================================

func TestThreadConfig(t *testing.T) {
	config := GetThreadConfig()
	if config.MaxThreads <= 0 {
		t.Errorf("MaxThreads should be positive, got %d", config.MaxThreads)
	}
}

func TestSetGetMaxThreads(t *testing.T) {
	original := GetMaxThreads()
	defer SetMaxThreads(original) // Restore

	SetMaxThreads(4)
	result := GetMaxThreads()
	if result != 4 {
		t.Errorf("GetMaxThreads() = %d, want 4", result)
	}

	// Test auto-detect
	SetMaxThreads(0)
	if IsThreadsAutoDetected() != true {
		t.Error("IsThreadsAutoDetected should be true after SetMaxThreads(0)")
	}
}

// ============================================================================
// ColumnI64 Tests
// ============================================================================

func TestColumnI64Creation(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	col := NewColumnI64(data)
	if col == nil {
		t.Fatal("Failed to create I64 column")
	}

	if col.Len() != 5 {
		t.Errorf("Expected length 5, got %d", col.Len())
	}

	if col.Get(0) != 1 {
		t.Errorf("Expected 1 at index 0, got %d", col.Get(0))
	}

	if col.Get(4) != 5 {
		t.Errorf("Expected 5 at index 4, got %d", col.Get(4))
	}
}

func TestColumnI64Data(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	col := NewColumnI64(data)

	view := col.Data()
	if len(view) != 5 {
		t.Errorf("Expected view length 5, got %d", len(view))
	}

	for i, v := range data {
		if view[i] != v {
			t.Errorf("Expected %d at index %d, got %d", v, i, view[i])
		}
	}
}

func TestSumI64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	result := SumI64(data)
	if result != 55 {
		t.Errorf("Expected 55, got %d", result)
	}
}

func TestMinMaxI64(t *testing.T) {
	data := []int64{5, 2, 8, 1, 9, 3}

	if MinI64(data) != 1 {
		t.Errorf("Expected min 1, got %d", MinI64(data))
	}

	if MaxI64(data) != 9 {
		t.Errorf("Expected max 9, got %d", MaxI64(data))
	}
}

func TestAddScalarI64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	AddScalarI64(data, 10)

	expected := []int64{11, 12, 13, 14, 15}
	for i, v := range expected {
		if data[i] != v {
			t.Errorf("Expected %d at index %d, got %d", v, i, data[i])
		}
	}
}

func TestMulScalarI64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	MulScalarI64(data, 2)

	expected := []int64{2, 4, 6, 8, 10}
	for i, v := range expected {
		if data[i] != v {
			t.Errorf("Expected %d at index %d, got %d", v, i, data[i])
		}
	}
}

func TestFilterGreaterThanI64(t *testing.T) {
	data := []int64{1, 5, 2, 8, 3, 9}
	indices := FilterGreaterThanI64(data, 4)

	if len(indices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(indices))
	}
}

func TestArgsortI64(t *testing.T) {
	data := []int64{3, 1, 4, 1, 5}
	indices := ArgsortI64(data, true)

	if len(indices) != 5 {
		t.Errorf("Expected 5 indices, got %d", len(indices))
	}
}

// ============================================================================
// ColumnI32 Tests
// ============================================================================

func TestColumnI32Creation(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	col := NewColumnI32(data)
	if col == nil {
		t.Fatal("Failed to create I32 column")
	}

	if col.Len() != 5 {
		t.Errorf("Expected length 5, got %d", col.Len())
	}
}

func TestSumI32(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	result := SumI32(data)
	if result != 15 {
		t.Errorf("Expected 15, got %d", result)
	}
}

func TestMinMaxI32(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	if MinI32(data) != 1 {
		t.Errorf("Expected min 1, got %d", MinI32(data))
	}
	if MaxI32(data) != 9 {
		t.Errorf("Expected max 9, got %d", MaxI32(data))
	}
}

func TestFilterGreaterThanI32(t *testing.T) {
	data := []int32{1, 5, 2, 8, 3, 9}
	indices := FilterGreaterThanI32(data, 4)

	if len(indices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(indices))
	}
}

func TestArgsortI32(t *testing.T) {
	data := []int32{3, 1, 4, 1, 5}
	indices := ArgsortI32(data, true)

	if len(indices) != 5 {
		t.Errorf("Expected 5 indices, got %d", len(indices))
	}
}

// ============================================================================
// ColumnF32 Tests
// ============================================================================

func TestColumnF32Creation(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewColumnF32(data)
	if col == nil {
		t.Fatal("Failed to create F32 column")
	}

	if col.Len() != 5 {
		t.Errorf("Expected length 5, got %d", col.Len())
	}
}

func TestSumF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	result := SumF32(data)
	if result != 15.0 {
		t.Errorf("Expected 15.0, got %f", result)
	}
}

func TestMinMaxF32(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	if MinF32(data) != 1.0 {
		t.Errorf("Expected min 1.0, got %f", MinF32(data))
	}
	if MaxF32(data) != 9.0 {
		t.Errorf("Expected max 9.0, got %f", MaxF32(data))
	}
}

func TestMeanF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	result := MeanF32(data)
	if result != 3.0 {
		t.Errorf("Expected 3.0, got %f", result)
	}
}

func TestFilterGreaterThanF32(t *testing.T) {
	data := []float32{1.0, 5.0, 2.0, 8.0, 3.0, 9.0}
	indices := FilterGreaterThanF32(data, 4.0)

	if len(indices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(indices))
	}
}

func TestArgsortF32(t *testing.T) {
	data := []float32{3.0, 1.0, 4.0, 1.0, 5.0}
	indices := ArgsortF32(data, true)

	if len(indices) != 5 {
		t.Errorf("Expected 5 indices, got %d", len(indices))
	}
}

// ============================================================================
// ColumnBool Tests
// ============================================================================

func TestColumnBoolCreation(t *testing.T) {
	data := []bool{true, false, true, true, false}
	col := NewColumnBool(data)
	if col == nil {
		t.Fatal("Failed to create Bool column")
	}

	if col.Len() != 5 {
		t.Errorf("Expected length 5, got %d", col.Len())
	}
}

func TestCountTrue(t *testing.T) {
	data := []bool{true, false, true, true, false}
	count := CountTrue(data)
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}
}

func TestCountFalse(t *testing.T) {
	data := []bool{true, false, true, true, false}
	count := CountFalse(data)
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}
}

// ============================================================================
// Vector Operation Tests
// ============================================================================

func TestAddF64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{4.0, 5.0, 6.0}
	out := make([]float64, 3)

	AddF64(a, b, out)

	expected := []float64{5.0, 7.0, 9.0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("AddF64 out[%d] = %f, want %f", i, out[i], v)
		}
	}
}

func TestSubF64(t *testing.T) {
	a := []float64{10.0, 20.0, 30.0}
	b := []float64{1.0, 2.0, 3.0}
	out := make([]float64, 3)

	SubF64(a, b, out)

	expected := []float64{9.0, 18.0, 27.0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("SubF64 out[%d] = %f, want %f", i, out[i], v)
		}
	}
}

func TestMulF64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{2.0, 3.0, 4.0}
	out := make([]float64, 3)

	MulF64(a, b, out)

	expected := []float64{2.0, 6.0, 12.0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("MulF64 out[%d] = %f, want %f", i, out[i], v)
		}
	}
}

func TestDivF64(t *testing.T) {
	a := []float64{10.0, 20.0, 30.0}
	b := []float64{2.0, 4.0, 5.0}
	out := make([]float64, 3)

	DivF64(a, b, out)

	expected := []float64{5.0, 5.0, 6.0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("DivF64 out[%d] = %f, want %f", i, out[i], v)
		}
	}
}

func TestAddI64(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{4, 5, 6}
	out := make([]int64, 3)

	AddI64(a, b, out)

	expected := []int64{5, 7, 9}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("AddI64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestSubI64(t *testing.T) {
	a := []int64{10, 20, 30}
	b := []int64{1, 2, 3}
	out := make([]int64, 3)

	SubI64(a, b, out)

	expected := []int64{9, 18, 27}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("SubI64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestMulI64(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{2, 3, 4}
	out := make([]int64, 3)

	MulI64(a, b, out)

	expected := []int64{2, 6, 12}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("MulI64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

// ============================================================================
// Comparison Operation Tests
// ============================================================================

func TestCmpGtF64(t *testing.T) {
	a := []float64{5.0, 3.0, 7.0, 2.0}
	b := []float64{4.0, 4.0, 4.0, 4.0}
	out := make([]byte, 4)

	CmpGtF64(a, b, out)

	expected := []byte{1, 0, 1, 0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("CmpGtF64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestCmpGeF64(t *testing.T) {
	a := []float64{5.0, 4.0, 3.0}
	b := []float64{4.0, 4.0, 4.0}
	out := make([]byte, 3)

	CmpGeF64(a, b, out)

	expected := []byte{1, 1, 0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("CmpGeF64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestCmpLtF64(t *testing.T) {
	a := []float64{3.0, 5.0, 2.0}
	b := []float64{4.0, 4.0, 4.0}
	out := make([]byte, 3)

	CmpLtF64(a, b, out)

	expected := []byte{1, 0, 1}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("CmpLtF64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestCmpLeF64(t *testing.T) {
	a := []float64{3.0, 4.0, 5.0}
	b := []float64{4.0, 4.0, 4.0}
	out := make([]byte, 3)

	CmpLeF64(a, b, out)

	expected := []byte{1, 1, 0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("CmpLeF64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestCmpEqF64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{1.0, 5.0, 3.0}
	out := make([]byte, 3)

	CmpEqF64(a, b, out)

	expected := []byte{1, 0, 1}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("CmpEqF64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

func TestCmpNeF64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{1.0, 5.0, 3.0}
	out := make([]byte, 3)

	CmpNeF64(a, b, out)

	expected := []byte{0, 1, 0}
	for i, v := range expected {
		if out[i] != v {
			t.Errorf("CmpNeF64 out[%d] = %d, want %d", i, out[i], v)
		}
	}
}

// ============================================================================
// Mask Operation Tests
// ============================================================================

func TestCountMaskTrue(t *testing.T) {
	mask := []byte{1, 0, 1, 1, 0, 1}
	count := CountMaskTrue(mask)
	if count != 4 {
		t.Errorf("CountMaskTrue = %d, want 4", count)
	}
}

func TestIndicesFromMask(t *testing.T) {
	mask := []byte{1, 0, 1, 0, 1}
	outIndices := make([]uint32, 3)
	count := IndicesFromMask(mask, outIndices)

	if count != 3 {
		t.Errorf("IndicesFromMask count = %d, want 3", count)
	}

	expected := []uint32{0, 2, 4}
	for i, v := range expected {
		if outIndices[i] != v {
			t.Errorf("IndicesFromMask out[%d] = %d, want %d", i, outIndices[i], v)
		}
	}
}

// ============================================================================
// Hash Function Tests
// ============================================================================

func TestHashI64Column(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	hashes := make([]uint64, 5)

	HashI64Column(data, hashes)

	// Just verify we got hashes (they should be non-zero for most values)
	nonZero := 0
	for _, h := range hashes {
		if h != 0 {
			nonZero++
		}
	}
	if nonZero < 3 {
		t.Error("Expected most hashes to be non-zero")
	}

	// Verify same input produces same hash
	hashes2 := make([]uint64, 5)
	HashI64Column(data, hashes2)
	for i := range hashes {
		if hashes[i] != hashes2[i] {
			t.Errorf("Hash mismatch at %d: %d vs %d", i, hashes[i], hashes2[i])
		}
	}
}

func TestHashI32Column(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	hashes := make([]uint64, 5)
	HashI32Column(data, hashes)

	// Just verify function runs
	_ = hashes
}

func TestHashF64Column(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	hashes := make([]uint64, 5)
	HashF64Column(data, hashes)

	// Just verify function runs
	_ = hashes
}

func TestHashF32Column(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	hashes := make([]uint64, 5)
	HashF32Column(data, hashes)

	// Just verify function runs
	_ = hashes
}

func TestCombineHashes(t *testing.T) {
	hash1 := []uint64{100, 200, 300}
	hash2 := []uint64{10, 20, 30}
	out := make([]uint64, 3)

	CombineHashes(hash1, hash2, out)

	// Verify combined hashes differ from inputs
	for i := range out {
		if out[i] == hash1[i] || out[i] == hash2[i] {
			t.Errorf("Combined hash[%d] should differ from inputs", i)
		}
	}
}

// ============================================================================
// Gather Function Tests
// ============================================================================

func TestGatherF64(t *testing.T) {
	src := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	indices := []int32{4, 2, 0}
	dst := make([]float64, 3)

	GatherF64(src, indices, dst)

	expected := []float64{50.0, 30.0, 10.0}
	for i, v := range expected {
		if dst[i] != v {
			t.Errorf("GatherF64 dst[%d] = %f, want %f", i, dst[i], v)
		}
	}
}

func TestGatherI64(t *testing.T) {
	src := []int64{10, 20, 30, 40, 50}
	indices := []int32{4, 2, 0}
	dst := make([]int64, 3)

	GatherI64(src, indices, dst)

	expected := []int64{50, 30, 10}
	for i, v := range expected {
		if dst[i] != v {
			t.Errorf("GatherI64 dst[%d] = %d, want %d", i, dst[i], v)
		}
	}
}

func TestGatherI32(t *testing.T) {
	src := []int32{10, 20, 30, 40, 50}
	indices := []int32{4, 2, 0}
	dst := make([]int32, 3)

	GatherI32(src, indices, dst)

	expected := []int32{50, 30, 10}
	for i, v := range expected {
		if dst[i] != v {
			t.Errorf("GatherI32 dst[%d] = %d, want %d", i, dst[i], v)
		}
	}
}

func TestGatherF32(t *testing.T) {
	src := []float32{10.0, 20.0, 30.0, 40.0, 50.0}
	indices := []int32{4, 2, 0}
	dst := make([]float32, 3)

	GatherF32(src, indices, dst)

	expected := []float32{50.0, 30.0, 10.0}
	for i, v := range expected {
		if dst[i] != v {
			t.Errorf("GatherF32 dst[%d] = %f, want %f", i, dst[i], v)
		}
	}
}

// ============================================================================
// GroupBy Aggregation Tests
// ============================================================================

func TestAggregateSumF64ByGroup(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	groupIDs := []uint32{0, 0, 1, 1, 1}
	outSums := make([]float64, 2)

	AggregateSumF64ByGroup(data, groupIDs, outSums)

	// Group 0: 1.0 + 2.0 = 3.0
	// Group 1: 3.0 + 4.0 + 5.0 = 12.0
	if outSums[0] != 3.0 {
		t.Errorf("Group 0 sum = %f, want 3.0", outSums[0])
	}
	if outSums[1] != 12.0 {
		t.Errorf("Group 1 sum = %f, want 12.0", outSums[1])
	}
}

func TestAggregateSumI64ByGroup(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	groupIDs := []uint32{0, 0, 1, 1, 1}
	outSums := make([]int64, 2)

	AggregateSumI64ByGroup(data, groupIDs, outSums)

	if outSums[0] != 3 {
		t.Errorf("Group 0 sum = %d, want 3", outSums[0])
	}
	if outSums[1] != 12 {
		t.Errorf("Group 1 sum = %d, want 12", outSums[1])
	}
}

func TestCountByGroup(t *testing.T) {
	groupIDs := []uint32{0, 0, 1, 1, 1, 2}
	outCounts := make([]uint64, 3)

	CountByGroup(groupIDs, outCounts)

	if outCounts[0] != 2 {
		t.Errorf("Group 0 count = %d, want 2", outCounts[0])
	}
	if outCounts[1] != 3 {
		t.Errorf("Group 1 count = %d, want 3", outCounts[1])
	}
	if outCounts[2] != 1 {
		t.Errorf("Group 2 count = %d, want 1", outCounts[2])
	}
}

// ============================================================================
// ComputeGroupIDs Tests
// ============================================================================

func TestComputeGroupIDs(t *testing.T) {
	hashes := []uint64{100, 200, 100, 300, 200}
	groupIDs, numGroups := ComputeGroupIDs(hashes)

	if len(groupIDs) != 5 {
		t.Errorf("Expected 5 group IDs, got %d", len(groupIDs))
	}
	if numGroups != 3 {
		t.Errorf("Expected 3 unique groups, got %d", numGroups)
	}

	// Verify same hashes map to same group
	if groupIDs[0] != groupIDs[2] {
		t.Error("Same hash should map to same group")
	}
	if groupIDs[1] != groupIDs[4] {
		t.Error("Same hash should map to same group")
	}
}

// ============================================================================
// Filter Pool Tests
// ============================================================================

func TestFilterMaskGreaterThanF64Pooled(t *testing.T) {
	data := []float64{1.0, 5.0, 2.0, 8.0, 3.0}
	mask := FilterMaskGreaterThanF64Pooled(data, 3.0)
	defer mask.Release()

	if mask == nil {
		t.Fatal("Expected non-nil mask")
	}

	count := 0
	for _, v := range mask.Data {
		if v {
			count++
		}
	}
	if count != 2 {
		t.Errorf("Expected 2 true values, got %d", count)
	}
}

func TestFilterGreaterThanF64Pooled(t *testing.T) {
	data := []float64{1.0, 5.0, 2.0, 8.0, 3.0}
	result := FilterGreaterThanF64Pooled(data, 3.0)
	defer result.Release()

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	if len(result.Data) != 2 {
		t.Errorf("Expected 2 indices, got %d", len(result.Data))
	}
}

// ============================================================================
// Join Tests
// ============================================================================

func TestInnerJoinI64(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{2, 4, 6}

	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	// Should match: (1, 0) for key 2, (3, 1) for key 4
	if len(leftIndices) != 2 || len(rightIndices) != 2 {
		t.Errorf("Expected 2 matches, got left=%d, right=%d",
			len(leftIndices), len(rightIndices))
	}
}

func TestLeftJoinI64(t *testing.T) {
	leftKeys := []int64{1, 2, 3}
	rightKeys := []int64{2, 4}

	leftIndices, rightIndices := LeftJoinI64(leftKeys, rightKeys)

	// All left rows should be present
	if len(leftIndices) != 3 {
		t.Errorf("Expected 3 left indices, got %d", len(leftIndices))
	}
	if len(rightIndices) != 3 {
		t.Errorf("Expected 3 right indices, got %d", len(rightIndices))
	}
}

// ============================================================================
// Additional Tests for Coverage
// ============================================================================

func TestCountMaskTrue_Empty(t *testing.T) {
	mask := []byte{}
	count := CountMaskTrue(mask)
	if count != 0 {
		t.Errorf("CountMaskTrue empty = %d, want 0", count)
	}
}

func TestCountMaskTrue_AllTrue(t *testing.T) {
	mask := []byte{1, 1, 1, 1, 1}
	count := CountMaskTrue(mask)
	if count != 5 {
		t.Errorf("CountMaskTrue = %d, want 5", count)
	}
}

func TestMinF32Additional(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	min := MinF32(data)
	if min != 1.0 {
		t.Errorf("MinF32 = %f, want 1.0", min)
	}
}

func TestMaxF32Additional(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0, 9.0}
	max := MaxF32(data)
	if max != 9.0 {
		t.Errorf("MaxF32 = %f, want 9.0", max)
	}
}

func TestAddScalarF32Additional(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0}
	AddScalarF32(data, 10.0)

	expected := []float32{11.0, 12.0, 13.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("AddScalarF32[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestMulScalarF32Additional(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0}
	MulScalarF32(data, 2.0)

	expected := []float32{2.0, 4.0, 6.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("MulScalarF32[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestMinI32Additional(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	min := MinI32(data)
	if min != 1 {
		t.Errorf("MinI32 = %d, want 1", min)
	}
}

func TestMaxI32Additional(t *testing.T) {
	data := []int32{5, 2, 8, 1, 9}
	max := MaxI32(data)
	if max != 9 {
		t.Errorf("MaxI32 = %d, want 9", max)
	}
}

func TestAddScalarI32Additional(t *testing.T) {
	data := []int32{1, 2, 3}
	AddScalarI32(data, 10)

	expected := []int32{11, 12, 13}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("AddScalarI32[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestMulScalarI32Additional(t *testing.T) {
	data := []int32{1, 2, 3}
	MulScalarI32(data, 2)

	expected := []int32{2, 4, 6}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("MulScalarI32[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// ============================================================================
// ColumnBool Get and Data Tests
// ============================================================================

func TestColumnBoolGetAndData(t *testing.T) {
	data := []bool{true, false, true, true, false}
	col := NewColumnBool(data)
	if col == nil {
		t.Fatal("Failed to create Bool column")
	}

	// Test Get method
	if col.Get(0) != true {
		t.Errorf("Get(0) = %v, want true", col.Get(0))
	}
	if col.Get(1) != false {
		t.Errorf("Get(1) = %v, want false", col.Get(1))
	}
	if col.Get(4) != false {
		t.Errorf("Get(4) = %v, want false", col.Get(4))
	}

	// Test Data method
	view := col.Data()
	if len(view) != 5 {
		t.Errorf("Data() length = %d, want 5", len(view))
	}
	for i, v := range data {
		if view[i] != v {
			t.Errorf("Data()[%d] = %v, want %v", i, view[i], v)
		}
	}
}

func TestColumnBoolCounts(t *testing.T) {
	data := []bool{true, false, true, true, false, true}

	trueCount := CountTrue(data)
	if trueCount != 4 {
		t.Errorf("CountTrue() = %d, want 4", trueCount)
	}

	falseCount := CountFalse(data)
	if falseCount != 2 {
		t.Errorf("CountFalse() = %d, want 2", falseCount)
	}
}

// ============================================================================
// BuildJoinHashTable and ProbeJoinHashTable Tests
// ============================================================================

func TestBuildAndProbeJoinHashTable(t *testing.T) {
	// Build hash table
	buildHashes := []uint64{100, 200, 300, 400}
	tableSize := uint32(8)
	table := make([]int32, tableSize)
	next := make([]int32, len(buildHashes))

	BuildJoinHashTable(buildHashes, table, next, tableSize)

	// Verify table was built (not all -1)
	foundEntry := false
	for _, v := range table {
		if v != -1 {
			foundEntry = true
			break
		}
	}
	if !foundEntry {
		t.Error("BuildJoinHashTable did not populate table")
	}

	// Probe hash table
	probeHashes := []uint64{100, 200, 500} // 100 and 200 should match
	probeKeys := []int64{1, 2, 5}
	buildKeys := []int64{1, 2, 3, 4}
	outProbe := make([]int32, 10)
	outBuild := make([]int32, 10)

	matches := ProbeJoinHashTable(probeHashes, probeKeys, buildKeys, table, next, tableSize, outProbe, outBuild, 10)

	// Should find matches for hash 100 (key 1) and hash 200 (key 2)
	if matches < 2 {
		t.Errorf("ProbeJoinHashTable matches = %d, want at least 2", matches)
	}
}

// ============================================================================
// ComputeGroupIDsWithKeys Tests
// ============================================================================

func TestComputeGroupIDsWithKeys(t *testing.T) {
	hashes := []uint64{100, 200, 100, 300, 200}
	keys := []int64{1, 2, 1, 3, 2} // Keys corresponding to hashes

	groupIDs, numGroups := ComputeGroupIDsWithKeys(hashes, keys)

	// Verify we got group IDs
	if len(groupIDs) != 5 {
		t.Errorf("GroupIDs length = %d, want 5", len(groupIDs))
	}

	if numGroups < 3 {
		t.Errorf("numGroups = %d, want at least 3", numGroups)
	}

	// Same hash+key combinations should map to same group
	if groupIDs[0] != groupIDs[2] {
		t.Error("Hash 100 + Key 1 should map to same group")
	}
	if groupIDs[1] != groupIDs[4] {
		t.Error("Hash 200 + Key 2 should map to same group")
	}
}

// ============================================================================
// ComputeGroupIDsExt Tests
// ============================================================================

func TestComputeGroupIDsExtended(t *testing.T) {
	hashes := []uint64{100, 200, 100, 300, 200, 100}
	result := ComputeGroupIDsExt(hashes)

	if result == nil {
		t.Fatal("ComputeGroupIDsExt returned nil")
	}

	if len(result.GroupIDs) != 6 {
		t.Errorf("groupIDs length = %d, want 6", len(result.GroupIDs))
	}

	if result.NumGroups != 3 {
		t.Errorf("numGroups = %d, want 3", result.NumGroups)
	}

	// firstRowIdx should have first occurrence of each group
	if len(result.FirstRowIdx) < result.NumGroups {
		t.Errorf("firstRowIdx length = %d, want at least %d", len(result.FirstRowIdx), result.NumGroups)
	}

	// counts should show count per group
	if len(result.GroupCounts) < result.NumGroups {
		t.Errorf("counts length = %d, want at least %d", len(result.GroupCounts), result.NumGroups)
	}

	// Verify total count equals input length
	totalCount := 0
	for i := 0; i < result.NumGroups; i++ {
		totalCount += result.GroupCounts[i]
	}
	if totalCount != 6 {
		t.Errorf("Total count = %d, want 6", totalCount)
	}
}

// ============================================================================
// GroupBySumE2E Tests
// ============================================================================

func TestGroupBySumE2E_Extended(t *testing.T) {
	keys := []int64{1, 2, 1, 2, 1}
	values := []float64{10.0, 20.0, 30.0, 40.0, 50.0}

	result := GroupBySumE2E(keys, values)
	if result == nil {
		t.Fatal("GroupBySumE2E returned nil")
	}

	// Group 1: 10 + 30 + 50 = 90
	// Group 2: 20 + 40 = 60
	if result.NumGroups < 2 {
		t.Errorf("NumGroups = %d, want at least 2", result.NumGroups)
	}
}

// ============================================================================
// GroupByMultiAggE2E Tests
// ============================================================================

func TestGroupByMultiAggE2E_Extended(t *testing.T) {
	keys := []int64{1, 2, 1, 2, 1}
	values := []float64{10.0, 20.0, 30.0, 40.0, 50.0}

	result := GroupByMultiAggE2E(keys, values)
	if result == nil {
		t.Fatal("GroupByMultiAggE2E returned nil")
	}

	if result.NumGroups < 2 {
		t.Errorf("NumGroups = %d, want at least 2", result.NumGroups)
	}

	// Verify we have all aggregation results
	if len(result.Sums) != int(result.NumGroups) {
		t.Errorf("Sums length = %d, want %d", len(result.Sums), result.NumGroups)
	}
	if len(result.Mins) != int(result.NumGroups) {
		t.Errorf("Mins length = %d, want %d", len(result.Mins), result.NumGroups)
	}
	if len(result.Maxs) != int(result.NumGroups) {
		t.Errorf("Maxs length = %d, want %d", len(result.Maxs), result.NumGroups)
	}
	if len(result.Counts) != int(result.NumGroups) {
		t.Errorf("Counts length = %d, want %d", len(result.Counts), result.NumGroups)
	}
}

// ============================================================================
// InnerJoinI64E2E Tests
// ============================================================================

func TestInnerJoinI64E2E(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{2, 4, 6, 8}

	result := InnerJoinI64E2E(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("InnerJoinI64E2E returned nil")
	}

	// Keys 2 and 4 should match
	if result.NumMatches < 2 {
		t.Errorf("NumMatches = %d, want at least 2", result.NumMatches)
	}
}

// ============================================================================
// LeftJoinI64E2E Tests
// ============================================================================

func TestLeftJoinI64E2E(t *testing.T) {
	leftKeys := []int64{1, 2, 3}
	rightKeys := []int64{2, 4, 6}

	result := LeftJoinI64E2E(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("LeftJoinI64E2E returned nil")
	}

	// All left keys should be present
	if result.NumRows != 3 {
		t.Errorf("NumRows = %d, want 3", result.NumRows)
	}
}

// ============================================================================
// ParallelInnerJoinI64 Tests
// ============================================================================

func TestParallelInnerJoinI64(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{2, 4, 6}

	result := ParallelInnerJoinI64(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("ParallelInnerJoinI64 returned nil")
	}

	// Keys 2 and 4 should match
	if result.NumMatches < 2 {
		t.Errorf("NumMatches = %d, want at least 2", result.NumMatches)
	}
}

// ============================================================================
// ParallelLeftJoinI64 Tests
// ============================================================================

func TestParallelLeftJoinI64(t *testing.T) {
	leftKeys := []int64{1, 2, 3}
	rightKeys := []int64{2, 4}

	result := ParallelLeftJoinI64(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("ParallelLeftJoinI64 returned nil")
	}

	// All left keys should be present
	if result.NumRows != 3 {
		t.Errorf("NumRows = %d, want 3", result.NumRows)
	}
}

// ============================================================================
// AggregateMinMaxByGroup Tests
// ============================================================================

func TestAggregateMinF64ByGroup(t *testing.T) {
	data := []float64{10.0, 5.0, 3.0, 8.0, 1.0}
	groupIDs := []uint32{0, 0, 1, 1, 1}
	outMins := make([]float64, 2)
	// Initialize with large values
	for i := range outMins {
		outMins[i] = math.MaxFloat64
	}

	AggregateMinF64ByGroup(data, groupIDs, outMins)

	// Group 0: min(10, 5) = 5
	// Group 1: min(3, 8, 1) = 1
	if outMins[0] != 5.0 {
		t.Errorf("Group 0 min = %f, want 5.0", outMins[0])
	}
	if outMins[1] != 1.0 {
		t.Errorf("Group 1 min = %f, want 1.0", outMins[1])
	}
}

func TestAggregateMaxF64ByGroup(t *testing.T) {
	data := []float64{10.0, 5.0, 3.0, 8.0, 1.0}
	groupIDs := []uint32{0, 0, 1, 1, 1}
	outMaxs := make([]float64, 2)
	// Initialize with small values
	for i := range outMaxs {
		outMaxs[i] = -math.MaxFloat64
	}

	AggregateMaxF64ByGroup(data, groupIDs, outMaxs)

	// Group 0: max(10, 5) = 10
	// Group 1: max(3, 8, 1) = 8
	if outMaxs[0] != 10.0 {
		t.Errorf("Group 0 max = %f, want 10.0", outMaxs[0])
	}
	if outMaxs[1] != 8.0 {
		t.Errorf("Group 1 max = %f, want 8.0", outMaxs[1])
	}
}

func TestAggregateMinI64ByGroup(t *testing.T) {
	data := []int64{10, 5, 3, 8, 1}
	groupIDs := []uint32{0, 0, 1, 1, 1}
	outMins := make([]int64, 2)
	for i := range outMins {
		outMins[i] = math.MaxInt64
	}

	AggregateMinI64ByGroup(data, groupIDs, outMins)

	if outMins[0] != 5 {
		t.Errorf("Group 0 min = %d, want 5", outMins[0])
	}
	if outMins[1] != 1 {
		t.Errorf("Group 1 min = %d, want 1", outMins[1])
	}
}

func TestAggregateMaxI64ByGroup(t *testing.T) {
	data := []int64{10, 5, 3, 8, 1}
	groupIDs := []uint32{0, 0, 1, 1, 1}
	outMaxs := make([]int64, 2)
	for i := range outMaxs {
		outMaxs[i] = math.MinInt64
	}

	AggregateMaxI64ByGroup(data, groupIDs, outMaxs)

	if outMaxs[0] != 10 {
		t.Errorf("Group 0 max = %d, want 10", outMaxs[0])
	}
	if outMaxs[1] != 8 {
		t.Errorf("Group 1 max = %d, want 8", outMaxs[1])
	}
}

// ============================================================================
// Empty Slice Edge Cases
// ============================================================================

func TestEmptySliceEdgeCases(t *testing.T) {
	// Hash functions with empty slices
	emptyI64 := []int64{}
	emptyHashes := []uint64{}
	HashI64Column(emptyI64, emptyHashes) // Should not panic

	// Gather with empty slices
	emptyF64 := []float64{}
	emptyI32 := []int32{}
	GatherF64(emptyF64, emptyI32, emptyF64) // Should not panic

	// GroupBy aggregations with empty slices
	emptyU32 := []uint32{}
	AggregateSumF64ByGroup(emptyF64, emptyU32, emptyF64)

	// ComputeGroupIDs with empty
	groupIDs, numGroups := ComputeGroupIDs(emptyHashes)
	if len(groupIDs) != 0 {
		t.Errorf("Empty groupIDs length = %d, want 0", len(groupIDs))
	}
	if numGroups != 0 {
		t.Errorf("Empty numGroups = %d, want 0", numGroups)
	}
}

// ============================================================================
// ComputeGroupIDsZeroCopy Tests
// ============================================================================

func TestComputeGroupIDsZeroCopy(t *testing.T) {
	hashes := []uint64{100, 200, 100, 300}
	result := ComputeGroupIDsZeroCopy(hashes)

	if result == nil {
		t.Fatal("ComputeGroupIDsZeroCopy returned nil")
	}

	// Verify group IDs are accessible
	groupIDs := result.GroupIDs()
	if len(groupIDs) != 4 {
		t.Errorf("GroupIDs length = %d, want 4", len(groupIDs))
	}

	// Verify number of groups
	numGroups := result.NumGroups()
	if numGroups < 3 {
		t.Errorf("NumGroups = %d, want at least 3", numGroups)
	}
}

// ============================================================================
// Column-based Arithmetic Tests
// ============================================================================

func TestAddF64Columns(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{10.0, 20.0, 30.0}
	result := make([]float64, 3)

	AddF64(a, b, result)

	expected := []float64{11.0, 22.0, 33.0}
	for i, v := range expected {
		if math.Abs(result[i]-v) > 0.0001 {
			t.Errorf("AddF64[%d] = %f, want %f", i, result[i], v)
		}
	}
}

func TestSubF64Columns(t *testing.T) {
	a := []float64{10.0, 20.0, 30.0}
	b := []float64{1.0, 2.0, 3.0}
	result := make([]float64, 3)

	SubF64(a, b, result)

	expected := []float64{9.0, 18.0, 27.0}
	for i, v := range expected {
		if math.Abs(result[i]-v) > 0.0001 {
			t.Errorf("SubF64[%d] = %f, want %f", i, result[i], v)
		}
	}
}

func TestMulF64Columns(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{10.0, 20.0, 30.0}
	result := make([]float64, 3)

	MulF64(a, b, result)

	expected := []float64{10.0, 40.0, 90.0}
	for i, v := range expected {
		if math.Abs(result[i]-v) > 0.0001 {
			t.Errorf("MulF64[%d] = %f, want %f", i, result[i], v)
		}
	}
}

func TestDivF64Columns(t *testing.T) {
	a := []float64{10.0, 20.0, 30.0}
	b := []float64{2.0, 4.0, 5.0}
	result := make([]float64, 3)

	DivF64(a, b, result)

	expected := []float64{5.0, 5.0, 6.0}
	for i, v := range expected {
		if math.Abs(result[i]-v) > 0.0001 {
			t.Errorf("DivF64[%d] = %f, want %f", i, result[i], v)
		}
	}
}

func TestAddI64Columns(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{10, 20, 30}
	result := make([]int64, 3)

	AddI64(a, b, result)

	expected := []int64{11, 22, 33}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("AddI64[%d] = %d, want %d", i, result[i], v)
		}
	}
}

func TestSubI64Columns(t *testing.T) {
	a := []int64{10, 20, 30}
	b := []int64{1, 2, 3}
	result := make([]int64, 3)

	SubI64(a, b, result)

	expected := []int64{9, 18, 27}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("SubI64[%d] = %d, want %d", i, result[i], v)
		}
	}
}

func TestMulI64Columns(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{10, 20, 30}
	result := make([]int64, 3)

	MulI64(a, b, result)

	expected := []int64{10, 40, 90}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("MulI64[%d] = %d, want %d", i, result[i], v)
		}
	}
}

// ============================================================================
// ColumnF64 method tests with empty data
// ============================================================================

func TestColumnF64EmptyCreation(t *testing.T) {
	col := NewColumnF64([]float64{})
	if col != nil {
		t.Error("NewColumnF64 with empty slice should return nil")
	}
}

func TestColumnF64NilMethods(t *testing.T) {
	// Test that methods handle nil ptr gracefully
	var col *ColumnF64

	if col != nil {
		// If we somehow have a nil pointer, methods should handle it
		// This tests the defensive nil checks in the methods
	}

	// Create a column with nil ptr directly to test nil checks
	col = &ColumnF64{ptr: nil}

	if col.Len() != 0 {
		t.Errorf("Nil column Len() = %d, want 0", col.Len())
	}
	if col.Get(0) != 0 {
		t.Errorf("Nil column Get(0) = %v, want 0", col.Get(0))
	}
	if col.Data() != nil {
		t.Errorf("Nil column Data() should be nil")
	}
	if col.Sum() != 0 {
		t.Errorf("Nil column Sum() = %v, want 0", col.Sum())
	}
	if col.Min() != 0 {
		t.Errorf("Nil column Min() = %v, want 0", col.Min())
	}
	if col.Max() != 0 {
		t.Errorf("Nil column Max() = %v, want 0", col.Max())
	}
	if col.Mean() != 0 {
		t.Errorf("Nil column Mean() = %v, want 0", col.Mean())
	}
}

func TestColumnI64NilMethods(t *testing.T) {
	col := &ColumnI64{ptr: nil}

	if col.Len() != 0 {
		t.Errorf("Nil column Len() = %d, want 0", col.Len())
	}
	if col.Get(0) != 0 {
		t.Errorf("Nil column Get(0) = %v, want 0", col.Get(0))
	}
	if col.Data() != nil {
		t.Errorf("Nil column Data() should be nil")
	}
}

func TestColumnI32NilMethods(t *testing.T) {
	col := &ColumnI32{ptr: nil}

	if col.Len() != 0 {
		t.Errorf("Nil column Len() = %d, want 0", col.Len())
	}
	if col.Get(0) != 0 {
		t.Errorf("Nil column Get(0) = %v, want 0", col.Get(0))
	}
	if col.Data() != nil {
		t.Errorf("Nil column Data() should be nil")
	}
}

func TestColumnF32NilMethods(t *testing.T) {
	col := &ColumnF32{ptr: nil}

	if col.Len() != 0 {
		t.Errorf("Nil column Len() = %d, want 0", col.Len())
	}
	if col.Get(0) != 0 {
		t.Errorf("Nil column Get(0) = %v, want 0", col.Get(0))
	}
	if col.Data() != nil {
		t.Errorf("Nil column Data() should be nil")
	}
}

func TestColumnBoolNilMethods(t *testing.T) {
	col := &ColumnBool{ptr: nil}

	if col.Len() != 0 {
		t.Errorf("Nil column Len() = %d, want 0", col.Len())
	}
	if col.Get(0) != false {
		t.Errorf("Nil column Get(0) = %v, want false", col.Get(0))
	}
	if col.Data() != nil {
		t.Errorf("Nil column Data() should be nil")
	}
}
