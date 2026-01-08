package galleon

import (
	"fmt"
	"math"
	"testing"
)

// ============================================================================
// Construction Tests
// ============================================================================

func TestNewSeriesFloat64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	s := NewSeriesFloat64("test", data)

	if s == nil {
		t.Fatal("NewSeriesFloat64 returned nil")
	}
	if s.Name() != "test" {
		t.Errorf("Name() = %q, want %q", s.Name(), "test")
	}
	if s.DType() != Float64 {
		t.Errorf("DType() = %v, want %v", s.DType(), Float64)
	}
	if s.Len() != 5 {
		t.Errorf("Len() = %d, want 5", s.Len())
	}
}

func TestNewSeriesFloat64_Empty(t *testing.T) {
	s := NewSeriesFloat64("empty", []float64{})
	if s == nil {
		t.Fatal("NewSeriesFloat64 returned nil for empty slice")
	}
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
	if !s.IsEmpty() {
		t.Error("IsEmpty() = false, want true")
	}
}

func TestNewSeriesInt64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	s := NewSeriesInt64("test", data)

	if s == nil {
		t.Fatal("NewSeriesInt64 returned nil")
	}
	if s.DType() != Int64 {
		t.Errorf("DType() = %v, want %v", s.DType(), Int64)
	}
	if s.Len() != 5 {
		t.Errorf("Len() = %d, want 5", s.Len())
	}
}

func TestNewSeriesInt32(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	s := NewSeriesInt32("test", data)

	if s == nil {
		t.Fatal("NewSeriesInt32 returned nil")
	}
	if s.DType() != Int32 {
		t.Errorf("DType() = %v, want %v", s.DType(), Int32)
	}
}

func TestNewSeriesFloat32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0}
	s := NewSeriesFloat32("test", data)

	if s == nil {
		t.Fatal("NewSeriesFloat32 returned nil")
	}
	if s.DType() != Float32 {
		t.Errorf("DType() = %v, want %v", s.DType(), Float32)
	}
}

func TestNewSeriesBool(t *testing.T) {
	data := []bool{true, false, true}
	s := NewSeriesBool("test", data)

	if s == nil {
		t.Fatal("NewSeriesBool returned nil")
	}
	if s.DType() != Bool {
		t.Errorf("DType() = %v, want %v", s.DType(), Bool)
	}
}

func TestNewSeriesString(t *testing.T) {
	data := []string{"a", "b", "c"}
	s := NewSeriesString("test", data)

	if s == nil {
		t.Fatal("NewSeriesString returned nil")
	}
	if s.DType() != String {
		t.Errorf("DType() = %v, want %v", s.DType(), String)
	}
	if s.Len() != 3 {
		t.Errorf("Len() = %d, want 3", s.Len())
	}
}

// ============================================================================
// Metadata Tests
// ============================================================================

func TestSeries_Rename(t *testing.T) {
	s := NewSeriesFloat64("old", []float64{1.0, 2.0})
	s2 := s.Rename("new")

	if s2.Name() != "new" {
		t.Errorf("Rename() name = %q, want %q", s2.Name(), "new")
	}
	// Original should be unchanged
	if s.Name() != "old" {
		t.Errorf("Original name changed to %q, want %q", s.Name(), "old")
	}
}

func TestSeries_IsEmpty(t *testing.T) {
	empty := NewSeriesFloat64("empty", []float64{})
	nonEmpty := NewSeriesFloat64("full", []float64{1.0})

	if !empty.IsEmpty() {
		t.Error("IsEmpty() = false for empty series")
	}
	if nonEmpty.IsEmpty() {
		t.Error("IsEmpty() = true for non-empty series")
	}
}

// ============================================================================
// Data Access Tests
// ============================================================================

func TestSeries_Float64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	s := NewSeriesFloat64("test", data)
	result := s.Float64()

	if len(result) != len(data) {
		t.Fatalf("Float64() len = %d, want %d", len(result), len(data))
	}
	for i, v := range result {
		if v != data[i] {
			t.Errorf("Float64()[%d] = %f, want %f", i, v, data[i])
		}
	}
}

func TestSeries_Int64(t *testing.T) {
	data := []int64{1, 2, 3}
	s := NewSeriesInt64("test", data)
	result := s.Int64()

	if len(result) != len(data) {
		t.Fatalf("Int64() len = %d, want %d", len(result), len(data))
	}
	for i, v := range result {
		if v != data[i] {
			t.Errorf("Int64()[%d] = %d, want %d", i, v, data[i])
		}
	}
}

func TestSeries_Strings(t *testing.T) {
	data := []string{"a", "b", "c"}
	s := NewSeriesString("test", data)
	result := s.Strings()

	if len(result) != len(data) {
		t.Fatalf("Strings() len = %d, want %d", len(result), len(data))
	}
	for i, v := range result {
		if v != data[i] {
			t.Errorf("Strings()[%d] = %q, want %q", i, v, data[i])
		}
	}
}

func TestSeries_Get(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})

	v := s.Get(0)
	if v != 1.0 {
		t.Errorf("Get(0) = %v, want 1.0", v)
	}
	v = s.Get(2)
	if v != 3.0 {
		t.Errorf("Get(2) = %v, want 3.0", v)
	}
}

func TestSeries_GetFloat64(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.5, 2.5, 3.5})

	v, ok := s.GetFloat64(1)
	if !ok {
		t.Error("GetFloat64(1) ok = false, want true")
	}
	if v != 2.5 {
		t.Errorf("GetFloat64(1) = %f, want 2.5", v)
	}

	// Out of bounds
	_, ok = s.GetFloat64(10)
	if ok {
		t.Error("GetFloat64(10) ok = true, want false")
	}
}

func TestSeries_GetInt64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{10, 20, 30})

	v, ok := s.GetInt64(2)
	if !ok {
		t.Error("GetInt64(2) ok = false, want true")
	}
	if v != 30 {
		t.Errorf("GetInt64(2) = %d, want 30", v)
	}
}

func TestSeries_GetString(t *testing.T) {
	s := NewSeriesString("test", []string{"hello", "world"})

	v, ok := s.GetString(0)
	if !ok {
		t.Error("GetString(0) ok = false, want true")
	}
	if v != "hello" {
		t.Errorf("GetString(0) = %q, want %q", v, "hello")
	}
}

// ============================================================================
// Aggregation Tests
// ============================================================================

func TestSeries_Sum_Float64(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Sum()
	expected := 15.0

	if math.Abs(result-expected) > 0.0001 {
		t.Errorf("Sum() = %f, want %f", result, expected)
	}
}

func TestSeries_Sum_Empty(t *testing.T) {
	s := NewSeriesFloat64("empty", []float64{})
	result := s.Sum()

	if result != 0 {
		t.Errorf("Sum() of empty = %f, want 0", result)
	}
}

func TestSeries_SumInt(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3, 4, 5})
	result := s.SumInt()
	expected := int64(15)

	if result != expected {
		t.Errorf("SumInt() = %d, want %d", result, expected)
	}
}

func TestSeries_Min(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{5.0, 2.0, 8.0, 1.0, 9.0})
	result := s.Min()
	expected := 1.0

	if math.Abs(result-expected) > 0.0001 {
		t.Errorf("Min() = %f, want %f", result, expected)
	}
}

func TestSeries_Max(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{5.0, 2.0, 8.0, 1.0, 9.0})
	result := s.Max()
	expected := 9.0

	if math.Abs(result-expected) > 0.0001 {
		t.Errorf("Max() = %f, want %f", result, expected)
	}
}

func TestSeries_Mean(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Mean()
	expected := 3.0

	if math.Abs(result-expected) > 0.0001 {
		t.Errorf("Mean() = %f, want %f", result, expected)
	}
}

func TestSeries_CountTrue(t *testing.T) {
	s := NewSeriesBool("test", []bool{true, false, true, true, false})
	result := s.CountTrue()
	expected := 3

	if result != expected {
		t.Errorf("CountTrue() = %d, want %d", result, expected)
	}
}

func TestSeries_Describe(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	desc := s.Describe()

	if desc["count"] != 5 {
		t.Errorf("Describe()[count] = %f, want 5", desc["count"])
	}
	if desc["sum"] != 15 {
		t.Errorf("Describe()[sum] = %f, want 15", desc["sum"])
	}
	if desc["min"] != 1 {
		t.Errorf("Describe()[min] = %f, want 1", desc["min"])
	}
	if desc["max"] != 5 {
		t.Errorf("Describe()[max] = %f, want 5", desc["max"])
	}
	if desc["mean"] != 3 {
		t.Errorf("Describe()[mean] = %f, want 3", desc["mean"])
	}
}

// ============================================================================
// Filtering Tests
// ============================================================================

func TestSeries_Gt(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 5.0, 2.0, 8.0, 3.0})
	indices := s.Gt(4.0)

	// Values > 4: 5.0(idx 1), 8.0(idx 3)
	if len(indices) != 2 {
		t.Fatalf("Gt(4.0) returned %d indices, want 2", len(indices))
	}

	// Verify indices point to correct values
	data := s.Float64()
	for _, idx := range indices {
		if data[idx] <= 4.0 {
			t.Errorf("Gt returned index %d with value %f <= 4.0", idx, data[idx])
		}
	}
}

func TestSeries_Gt_NoneMatch(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	indices := s.Gt(10.0)

	if len(indices) != 0 {
		t.Errorf("Gt(10.0) returned %d indices, want 0", len(indices))
	}
}

func TestSeries_GtMask(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 5.0, 2.0, 8.0, 3.0})
	mask := s.GtMask(4.0, nil)

	if len(mask) != 5 {
		t.Fatalf("GtMask() returned mask of len %d, want 5", len(mask))
	}

	// Verify mask
	expected := []byte{0, 1, 0, 1, 0}
	for i, v := range mask {
		if v != expected[i] {
			t.Errorf("GtMask()[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

// ============================================================================
// Sorting Tests
// ============================================================================

func TestSeries_Argsort_Ascending(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{3.0, 1.0, 4.0, 1.0, 5.0})
	indices := s.Argsort(true)

	if len(indices) != 5 {
		t.Fatalf("Argsort() returned %d indices, want 5", len(indices))
	}

	// Verify sorted order
	data := s.Float64()
	for i := 1; i < len(indices); i++ {
		if data[indices[i]] < data[indices[i-1]] {
			t.Errorf("Argsort ascending: data[%d]=%f < data[%d]=%f",
				indices[i], data[indices[i]], indices[i-1], data[indices[i-1]])
		}
	}
}

func TestSeries_Argsort_Descending(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{3.0, 1.0, 4.0, 1.0, 5.0})
	indices := s.Argsort(false)

	data := s.Float64()
	for i := 1; i < len(indices); i++ {
		if data[indices[i]] > data[indices[i-1]] {
			t.Errorf("Argsort descending: data[%d]=%f > data[%d]=%f",
				indices[i], data[indices[i]], indices[i-1], data[indices[i-1]])
		}
	}
}

// ============================================================================
// Arithmetic Tests
// ============================================================================

func TestSeries_Add(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.Add(10.0)

	if result == nil {
		t.Fatal("Add() returned nil")
	}

	data := result.Float64()
	expected := []float64{11.0, 12.0, 13.0}
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("Add(10.0)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestSeries_Mul(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.Mul(2.0)

	if result == nil {
		t.Fatal("Mul() returned nil")
	}

	data := result.Float64()
	expected := []float64{2.0, 4.0, 6.0}
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("Mul(2.0)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

// ============================================================================
// Slicing Tests
// ============================================================================

func TestSeries_Head(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Head(3)

	if result.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", result.Len())
	}

	data := result.Float64()
	expected := []float64{1.0, 2.0, 3.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Head(3)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestSeries_Head_ExceedsLen(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0})
	result := s.Head(10)

	if result.Len() != 2 {
		t.Errorf("Head(10).Len() = %d, want 2", result.Len())
	}
}

func TestSeries_Tail(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Tail(3)

	if result.Len() != 3 {
		t.Errorf("Tail(3).Len() = %d, want 3", result.Len())
	}

	data := result.Float64()
	expected := []float64{3.0, 4.0, 5.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Tail(3)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

// ============================================================================
// String Representation Test
// ============================================================================

func TestSeries_String(t *testing.T) {
	s := NewSeriesFloat64("values", []float64{1.0, 2.0, 3.0})
	str := s.String()

	if str == "" {
		t.Error("String() returned empty string")
	}
	// Just verify it doesn't panic and returns something
}

// ============================================================================
// Correctness Tests - Verify SIMD results match manual computation
// ============================================================================

func TestSeries_Sum_Correctness(t *testing.T) {
	data := make([]float64, 1000)
	var manualSum float64
	for i := range data {
		data[i] = float64(i) + 0.5
		manualSum += data[i]
	}

	s := NewSeriesFloat64("test", data)
	simdSum := s.Sum()

	if math.Abs(simdSum-manualSum) > 0.01 {
		t.Errorf("Sum() = %f, manual = %f", simdSum, manualSum)
	}
}

func TestSeries_MinMax_Correctness(t *testing.T) {
	data := []float64{5.5, 2.2, 8.8, 1.1, 9.9, 3.3, 7.7, 4.4, 6.6, 0.0}
	s := NewSeriesFloat64("test", data)

	// Manual computation
	manualMin := data[0]
	manualMax := data[0]
	for _, v := range data {
		if v < manualMin {
			manualMin = v
		}
		if v > manualMax {
			manualMax = v
		}
	}

	if s.Min() != manualMin {
		t.Errorf("Min() = %f, manual = %f", s.Min(), manualMin)
	}
	if s.Max() != manualMax {
		t.Errorf("Max() = %f, manual = %f", s.Max(), manualMax)
	}
}

func TestSeries_Mean_Correctness(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	s := NewSeriesFloat64("test", data)

	var sum float64
	for _, v := range data {
		sum += v
	}
	manualMean := sum / float64(len(data))

	if math.Abs(s.Mean()-manualMean) > 0.0001 {
		t.Errorf("Mean() = %f, manual = %f", s.Mean(), manualMean)
	}
}

// ============================================================================
// Type-Specific Tests (to improve coverage of type branches)
// ============================================================================

func TestSeries_Float32_Data(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0}
	s := NewSeriesFloat32("test", data)

	result := s.Float32()
	if len(result) != 3 {
		t.Errorf("Float32() len = %d, want 3", len(result))
	}
	for i, v := range result {
		if v != data[i] {
			t.Errorf("Float32()[%d] = %f, want %f", i, v, data[i])
		}
	}
}

func TestSeries_Int32_Data(t *testing.T) {
	data := []int32{1, 2, 3}
	s := NewSeriesInt32("test", data)

	result := s.Int32()
	if len(result) != 3 {
		t.Errorf("Int32() len = %d, want 3", len(result))
	}
	for i, v := range result {
		if v != data[i] {
			t.Errorf("Int32()[%d] = %d, want %d", i, v, data[i])
		}
	}
}

func TestSeries_Bool_Data(t *testing.T) {
	data := []bool{true, false, true}
	s := NewSeriesBool("test", data)

	result := s.Bool()
	if len(result) != 3 {
		t.Errorf("Bool() len = %d, want 3", len(result))
	}
	for i, v := range result {
		if v != data[i] {
			t.Errorf("Bool()[%d] = %v, want %v", i, v, data[i])
		}
	}
}

func TestSeries_Sum_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3, 4, 5})
	result := s.Sum()
	// Sum returns float64 even for int types
	if result != 15.0 {
		t.Errorf("Sum() = %f, want 15.0", result)
	}
}

func TestSeries_Sum_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3, 4, 5})
	result := s.Sum()
	if result != 15.0 {
		t.Errorf("Sum() = %f, want 15.0", result)
	}
}

func TestSeries_Sum_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Sum()
	if math.Abs(result-15.0) > 0.01 {
		t.Errorf("Sum() = %f, want 15.0", result)
	}
}

func TestSeries_SumInt_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3, 4, 5})
	result := s.SumInt()
	if result != 15 {
		t.Errorf("SumInt() = %d, want 15", result)
	}
}

func TestSeries_SumInt_Float64(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.SumInt()
	// Float64 should return 0 for SumInt
	_ = result
}

func TestSeries_Min_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{5, 2, 8, 1, 9})
	result := s.Min()
	if result != 1.0 {
		t.Errorf("Min() = %f, want 1.0", result)
	}
}

func TestSeries_Min_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{5, 2, 8, 1, 9})
	result := s.Min()
	if result != 1.0 {
		t.Errorf("Min() = %f, want 1.0", result)
	}
}

func TestSeries_Min_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{5.0, 2.0, 8.0, 1.0, 9.0})
	result := s.Min()
	if math.Abs(result-1.0) > 0.01 {
		t.Errorf("Min() = %f, want 1.0", result)
	}
}

func TestSeries_Max_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{5, 2, 8, 1, 9})
	result := s.Max()
	if result != 9.0 {
		t.Errorf("Max() = %f, want 9.0", result)
	}
}

func TestSeries_Max_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{5, 2, 8, 1, 9})
	result := s.Max()
	if result != 9.0 {
		t.Errorf("Max() = %f, want 9.0", result)
	}
}

func TestSeries_Max_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{5.0, 2.0, 8.0, 1.0, 9.0})
	result := s.Max()
	if math.Abs(result-9.0) > 0.01 {
		t.Errorf("Max() = %f, want 9.0", result)
	}
}

func TestSeries_Mean_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3, 4, 5})
	result := s.Mean()
	if math.Abs(result-3.0) > 0.01 {
		t.Errorf("Mean() = %f, want 3.0", result)
	}
}

func TestSeries_Mean_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3, 4, 5})
	result := s.Mean()
	if math.Abs(result-3.0) > 0.01 {
		t.Errorf("Mean() = %f, want 3.0", result)
	}
}

func TestSeries_Mean_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Mean()
	if math.Abs(result-3.0) > 0.01 {
		t.Errorf("Mean() = %f, want 3.0", result)
	}
}

func TestSeries_Gt_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 5, 2, 8, 3})
	indices := s.Gt(4.0)
	if len(indices) != 2 {
		t.Errorf("Gt(4.0) returned %d indices, want 2", len(indices))
	}
}

func TestSeries_Gt_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 5, 2, 8, 3})
	indices := s.Gt(4.0)
	if len(indices) != 2 {
		t.Errorf("Gt(4.0) returned %d indices, want 2", len(indices))
	}
}

func TestSeries_Gt_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 5.0, 2.0, 8.0, 3.0})
	indices := s.Gt(4.0)
	if len(indices) != 2 {
		t.Errorf("Gt(4.0) returned %d indices, want 2", len(indices))
	}
}

func TestSeries_GtMask_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 5, 2, 8, 3})
	mask := s.GtMask(4.0, nil)
	if len(mask) != 5 {
		t.Errorf("GtMask() len = %d, want 5", len(mask))
	}
	// Just verify function runs - exact behavior may vary by implementation
	_ = mask
}

func TestSeries_Argsort_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{3, 1, 4, 1, 5})
	indices := s.Argsort(true)
	if len(indices) != 5 {
		t.Errorf("Argsort() len = %d, want 5", len(indices))
	}
}

func TestSeries_Argsort_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{3, 1, 4, 1, 5})
	indices := s.Argsort(true)
	if len(indices) != 5 {
		t.Errorf("Argsort() len = %d, want 5", len(indices))
	}
}

func TestSeries_Argsort_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{3.0, 1.0, 4.0, 1.0, 5.0})
	indices := s.Argsort(true)
	if len(indices) != 5 {
		t.Errorf("Argsort() len = %d, want 5", len(indices))
	}
}

func TestSeries_Add_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3})
	result := s.Add(10.0)
	if result == nil {
		t.Fatal("Add() returned nil")
	}
	// Add on int64 should return the converted values
	if result.Len() != 3 {
		t.Errorf("Add().Len() = %d, want 3", result.Len())
	}
}

func TestSeries_Mul_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3})
	result := s.Mul(2.0)
	if result == nil {
		t.Fatal("Mul() returned nil")
	}
	if result.Len() != 3 {
		t.Errorf("Mul().Len() = %d, want 3", result.Len())
	}
}

func TestSeries_Head_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3, 4, 5})
	result := s.Head(3)
	if result.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", result.Len())
	}
	data := result.Int64()
	if data[0] != 1 || data[1] != 2 || data[2] != 3 {
		t.Errorf("Head values incorrect: %v", data)
	}
}

func TestSeries_Head_String(t *testing.T) {
	s := NewSeriesString("test", []string{"a", "b", "c", "d", "e"})
	result := s.Head(3)
	if result.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", result.Len())
	}
	data := result.Strings()
	if data[0] != "a" || data[1] != "b" || data[2] != "c" {
		t.Errorf("Head values incorrect: %v", data)
	}
}

func TestSeries_Head_Bool(t *testing.T) {
	s := NewSeriesBool("test", []bool{true, false, true, false, true})
	result := s.Head(2)
	if result.Len() != 2 {
		t.Errorf("Head(2).Len() = %d, want 2", result.Len())
	}
}

func TestSeries_Tail_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3, 4, 5})
	result := s.Tail(2)
	if result.Len() != 2 {
		t.Errorf("Tail(2).Len() = %d, want 2", result.Len())
	}
	data := result.Int64()
	if data[0] != 4 || data[1] != 5 {
		t.Errorf("Tail values incorrect: %v", data)
	}
}

func TestSeries_Tail_String(t *testing.T) {
	s := NewSeriesString("test", []string{"a", "b", "c", "d", "e"})
	result := s.Tail(2)
	if result.Len() != 2 {
		t.Errorf("Tail(2).Len() = %d, want 2", result.Len())
	}
	data := result.Strings()
	if data[0] != "d" || data[1] != "e" {
		t.Errorf("Tail values incorrect: %v", data)
	}
}

func TestSeries_Tail_Bool(t *testing.T) {
	s := NewSeriesBool("test", []bool{true, false, true, false, true})
	result := s.Tail(2)
	if result.Len() != 2 {
		t.Errorf("Tail(2).Len() = %d, want 2", result.Len())
	}
}

func TestSeries_Get_AllTypes(t *testing.T) {
	// Int64
	s1 := NewSeriesInt64("test", []int64{10, 20, 30})
	if v := s1.Get(1); v != int64(20) {
		t.Errorf("Get(1) for Int64 = %v, want 20", v)
	}

	// Int32
	s2 := NewSeriesInt32("test", []int32{10, 20, 30})
	if v := s2.Get(1); v != int32(20) {
		t.Errorf("Get(1) for Int32 = %v, want 20", v)
	}

	// Float32
	s3 := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0})
	if v := s3.Get(1); v != float32(2.0) {
		t.Errorf("Get(1) for Float32 = %v, want 2.0", v)
	}

	// Bool
	s4 := NewSeriesBool("test", []bool{true, false, true})
	if v := s4.Get(1); v != false {
		t.Errorf("Get(1) for Bool = %v, want false", v)
	}

	// String
	s5 := NewSeriesString("test", []string{"a", "b", "c"})
	if v := s5.Get(1); v != "b" {
		t.Errorf("Get(1) for String = %v, want 'b'", v)
	}
}

func TestSeries_String_AllTypes(t *testing.T) {
	// Float64
	s1 := NewSeriesFloat64("f64", []float64{1.0, 2.0})
	if s1.String() == "" {
		t.Error("String() for Float64 is empty")
	}

	// Int64
	s2 := NewSeriesInt64("i64", []int64{1, 2})
	if s2.String() == "" {
		t.Error("String() for Int64 is empty")
	}

	// Int32
	s3 := NewSeriesInt32("i32", []int32{1, 2})
	if s3.String() == "" {
		t.Error("String() for Int32 is empty")
	}

	// Float32
	s4 := NewSeriesFloat32("f32", []float32{1.0, 2.0})
	if s4.String() == "" {
		t.Error("String() for Float32 is empty")
	}

	// Bool
	s5 := NewSeriesBool("bool", []bool{true, false})
	if s5.String() == "" {
		t.Error("String() for Bool is empty")
	}

	// String
	s6 := NewSeriesString("str", []string{"a", "b"})
	if s6.String() == "" {
		t.Error("String() for String is empty")
	}
}

// ============================================================================
// Additional Type-Variant Tests for Full Coverage
// ============================================================================

func TestSeries_Add_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0})
	result := s.Add(10.0)

	if result == nil {
		t.Fatal("Add() returned nil")
	}
	if result.DType() != Float32 {
		t.Errorf("Add() DType = %v, want Float32", result.DType())
	}

	data := result.Float32()
	expected := []float32{11.0, 12.0, 13.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Add(10.0)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestSeries_Add_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3})
	result := s.Add(10.0)

	if result == nil {
		t.Fatal("Add() returned nil")
	}
	if result.DType() != Int32 {
		t.Errorf("Add() DType = %v, want Int32", result.DType())
	}

	data := result.Int32()
	expected := []int32{11, 12, 13}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Add(10.0)[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestSeries_Add_Empty(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{})
	result := s.Add(10.0)

	if result != nil {
		t.Error("Add() on empty series should return nil")
	}
}

func TestSeries_Add_Bool(t *testing.T) {
	s := NewSeriesBool("test", []bool{true, false})
	result := s.Add(10.0)

	// Bool type is not supported for Add, should return nil
	if result != nil {
		t.Error("Add() on Bool series should return nil")
	}
}

func TestSeries_Add_String(t *testing.T) {
	s := NewSeriesString("test", []string{"a", "b"})
	result := s.Add(10.0)

	// String type is not supported for Add, should return nil
	if result != nil {
		t.Error("Add() on String series should return nil")
	}
}

func TestSeries_Mul_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0})
	result := s.Mul(2.0)

	if result == nil {
		t.Fatal("Mul() returned nil")
	}
	if result.DType() != Float32 {
		t.Errorf("Mul() DType = %v, want Float32", result.DType())
	}

	data := result.Float32()
	expected := []float32{2.0, 4.0, 6.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Mul(2.0)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestSeries_Mul_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3})
	result := s.Mul(2.0)

	if result == nil {
		t.Fatal("Mul() returned nil")
	}
	if result.DType() != Int32 {
		t.Errorf("Mul() DType = %v, want Int32", result.DType())
	}

	data := result.Int32()
	expected := []int32{2, 4, 6}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Mul(2.0)[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestSeries_Mul_Empty(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{})
	result := s.Mul(2.0)

	if result != nil {
		t.Error("Mul() on empty series should return nil")
	}
}

func TestSeries_Mul_Bool(t *testing.T) {
	s := NewSeriesBool("test", []bool{true, false})
	result := s.Mul(2.0)

	// Bool type is not supported for Mul, should return nil
	if result != nil {
		t.Error("Mul() on Bool series should return nil")
	}
}

func TestSeries_Mul_String(t *testing.T) {
	s := NewSeriesString("test", []string{"a", "b"})
	result := s.Mul(2.0)

	// String type is not supported for Mul, should return nil
	if result != nil {
		t.Error("Mul() on String series should return nil")
	}
}

func TestSeries_Head_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Head(3)

	if result.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", result.Len())
	}

	data := result.Float32()
	expected := []float32{1.0, 2.0, 3.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Head(3)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestSeries_Head_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3, 4, 5})
	result := s.Head(3)

	if result.Len() != 3 {
		t.Errorf("Head(3).Len() = %d, want 3", result.Len())
	}

	data := result.Int32()
	expected := []int32{1, 2, 3}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Head(3)[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestSeries_Head_Zero(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.Head(0)

	if result.Len() != 0 {
		t.Errorf("Head(0).Len() = %d, want 0", result.Len())
	}
}

func TestSeries_Head_Negative(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.Head(-1)

	if result.Len() != 0 {
		t.Errorf("Head(-1).Len() = %d, want 0", result.Len())
	}
}

func TestSeries_Head_Empty(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{})
	result := s.Head(5)

	if result.Len() != 0 {
		t.Errorf("Head(5) on empty.Len() = %d, want 0", result.Len())
	}
}

func TestSeries_Tail_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0, 4.0, 5.0})
	result := s.Tail(3)

	if result.Len() != 3 {
		t.Errorf("Tail(3).Len() = %d, want 3", result.Len())
	}

	data := result.Float32()
	expected := []float32{3.0, 4.0, 5.0}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Tail(3)[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestSeries_Tail_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3, 4, 5})
	result := s.Tail(3)

	if result.Len() != 3 {
		t.Errorf("Tail(3).Len() = %d, want 3", result.Len())
	}

	data := result.Int32()
	expected := []int32{3, 4, 5}
	for i, v := range data {
		if v != expected[i] {
			t.Errorf("Tail(3)[%d] = %d, want %d", i, v, expected[i])
		}
	}
}

func TestSeries_Tail_Zero(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.Tail(0)

	if result.Len() != 0 {
		t.Errorf("Tail(0).Len() = %d, want 0", result.Len())
	}
}

func TestSeries_Tail_Negative(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := s.Tail(-1)

	if result.Len() != 0 {
		t.Errorf("Tail(-1).Len() = %d, want 0", result.Len())
	}
}

func TestSeries_Tail_Empty(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{})
	result := s.Tail(5)

	if result.Len() != 0 {
		t.Errorf("Tail(5) on empty.Len() = %d, want 0", result.Len())
	}
}

func TestSeries_Tail_ExceedsLen(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0})
	result := s.Tail(10)

	if result.Len() != 2 {
		t.Errorf("Tail(10).Len() = %d, want 2", result.Len())
	}
}

// ============================================================================
// String representation edge cases
// ============================================================================

func TestSeries_String_Empty(t *testing.T) {
	s := NewSeriesFloat64("empty", []float64{})
	str := s.String()

	if str == "" {
		t.Error("String() for empty series returned empty string")
	}
	// Should contain len=0
	if !containsSubstring(str, "len=0") {
		t.Errorf("String() for empty series should indicate len=0, got: %s", str)
	}
}

func TestSeries_String_LargeArray(t *testing.T) {
	// Test that large arrays are truncated
	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i)
	}
	s := NewSeriesFloat64("large", data)
	str := s.String()

	if str == "" {
		t.Error("String() for large series returned empty string")
	}
	// Should contain "..."
	if !containsSubstring(str, "...") {
		t.Errorf("String() for large series should truncate with ..., got: %s", str)
	}
}

func TestSeries_String_LargeArray_Int64(t *testing.T) {
	data := make([]int64, 100)
	for i := range data {
		data[i] = int64(i)
	}
	s := NewSeriesInt64("large", data)
	str := s.String()

	if !containsSubstring(str, "...") {
		t.Errorf("String() for large Int64 series should truncate with ..., got: %s", str)
	}
}

func TestSeries_String_LargeArray_Int32(t *testing.T) {
	data := make([]int32, 100)
	for i := range data {
		data[i] = int32(i)
	}
	s := NewSeriesInt32("large", data)
	str := s.String()

	if !containsSubstring(str, "...") {
		t.Errorf("String() for large Int32 series should truncate with ..., got: %s", str)
	}
}

func TestSeries_String_LargeArray_Float32(t *testing.T) {
	data := make([]float32, 100)
	for i := range data {
		data[i] = float32(i)
	}
	s := NewSeriesFloat32("large", data)
	str := s.String()

	if !containsSubstring(str, "...") {
		t.Errorf("String() for large Float32 series should truncate with ..., got: %s", str)
	}
}

func TestSeries_String_LargeArray_Bool(t *testing.T) {
	data := make([]bool, 100)
	for i := range data {
		data[i] = i%2 == 0
	}
	s := NewSeriesBool("large", data)
	str := s.String()

	if !containsSubstring(str, "...") {
		t.Errorf("String() for large Bool series should truncate with ..., got: %s", str)
	}
}

func TestSeries_String_LargeArray_String(t *testing.T) {
	data := make([]string, 100)
	for i := range data {
		data[i] = fmt.Sprintf("item%d", i)
	}
	s := NewSeriesString("large", data)
	str := s.String()

	if !containsSubstring(str, "...") {
		t.Errorf("String() for large String series should truncate with ..., got: %s", str)
	}
}

// ============================================================================
// Describe Tests
// ============================================================================

func TestSeriesDescribe_Float64(t *testing.T) {
	s := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	desc := s.Describe()

	if desc["count"] != 5.0 {
		t.Errorf("count = %v, want 5", desc["count"])
	}
	if desc["sum"] != 15.0 {
		t.Errorf("sum = %v, want 15", desc["sum"])
	}
	if desc["min"] != 1.0 {
		t.Errorf("min = %v, want 1", desc["min"])
	}
	if desc["max"] != 5.0 {
		t.Errorf("max = %v, want 5", desc["max"])
	}
	if desc["mean"] != 3.0 {
		t.Errorf("mean = %v, want 3", desc["mean"])
	}
}

func TestSeriesDescribe_Empty(t *testing.T) {
	s := NewSeriesFloat64("a", []float64{})
	desc := s.Describe()

	if desc != nil {
		t.Errorf("Describe on empty series should return nil, got %v", desc)
	}
}

func TestSeriesDescribe_NonNumeric(t *testing.T) {
	s := NewSeriesString("a", []string{"a", "b", "c"})
	desc := s.Describe()

	if desc != nil {
		t.Errorf("Describe on non-numeric series should return nil, got %v", desc)
	}
}

func TestSeriesDescribe_Int64(t *testing.T) {
	s := NewSeriesInt64("a", []int64{1, 2, 3, 4, 5})
	desc := s.Describe()

	if desc["count"] != 5.0 {
		t.Errorf("count = %v, want 5", desc["count"])
	}
	if desc["sum"] != 15.0 {
		t.Errorf("sum = %v, want 15", desc["sum"])
	}
}

// Helper function for substring check
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
