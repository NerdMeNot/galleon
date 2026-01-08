package galleon

import (
	"testing"
)

// ============================================================================
// CGO Header Validation Tests
// These tests ensure that all exported Zig functions are properly linked
// and callable from Go. A compilation failure here indicates a mismatch
// between the C header and Zig exports.
// ============================================================================

func TestCGO_ThreadConfiguration(t *testing.T) {
	// Test thread configuration functions are linkable
	SetMaxThreads(4)
	maxThreads := GetMaxThreads()
	if maxThreads <= 0 {
		t.Errorf("GetMaxThreads returned invalid value: %d", maxThreads)
	}

	config := GetThreadConfig()
	if config.MaxThreads <= 0 {
		t.Errorf("GetThreadConfig returned invalid MaxThreads: %d", config.MaxThreads)
	}

	// Reset to auto-detect
	SetMaxThreads(0)
}

// ============================================================================
// Float64 Operations
// ============================================================================

func TestCGO_Float64_Aggregations(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}

	sum := SumF64(data)
	if sum != 15.0 {
		t.Errorf("SumF64 failed: got %f", sum)
	}

	min := MinF64(data)
	if min != 1.0 {
		t.Errorf("MinF64 failed: got %f", min)
	}

	max := MaxF64(data)
	if max != 5.0 {
		t.Errorf("MaxF64 failed: got %f", max)
	}

	mean := MeanF64(data)
	if mean != 3.0 {
		t.Errorf("MeanF64 failed: got %f", mean)
	}
}

func TestCGO_Float64_Arithmetic(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0}
	b := []float64{4.0, 5.0, 6.0}
	out := make([]float64, 3)

	AddF64(a, b, out)
	if out[0] != 5.0 || out[1] != 7.0 || out[2] != 9.0 {
		t.Errorf("AddF64 failed: got %v", out)
	}

	SubF64(a, b, out)
	if out[0] != -3.0 || out[1] != -3.0 || out[2] != -3.0 {
		t.Errorf("SubF64 failed: got %v", out)
	}

	MulF64(a, b, out)
	if out[0] != 4.0 || out[1] != 10.0 || out[2] != 18.0 {
		t.Errorf("MulF64 failed: got %v", out)
	}

	DivF64(b, a, out)
	if out[0] != 4.0 || out[1] != 2.5 || out[2] != 2.0 {
		t.Errorf("DivF64 failed: got %v", out)
	}
}

func TestCGO_Float64_ScalarOps(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}

	AddScalarF64(data, 10.0)
	if data[0] != 11.0 || data[1] != 12.0 || data[2] != 13.0 {
		t.Errorf("AddScalarF64 failed: got %v", data)
	}

	data = []float64{1.0, 2.0, 3.0}
	MulScalarF64(data, 2.0)
	if data[0] != 2.0 || data[1] != 4.0 || data[2] != 6.0 {
		t.Errorf("MulScalarF64 failed: got %v", data)
	}
}

func TestCGO_Float64_Comparisons(t *testing.T) {
	a := []float64{1.0, 5.0, 3.0}
	b := []float64{2.0, 4.0, 3.0}
	out := make([]byte, 3)

	CmpGtF64(a, b, out)
	if out[0] != 0 || out[1] == 0 || out[2] != 0 {
		t.Errorf("CmpGtF64 failed: got %v", out)
	}

	CmpGeF64(a, b, out)
	if out[0] != 0 || out[1] == 0 || out[2] == 0 {
		t.Errorf("CmpGeF64 failed: got %v", out)
	}

	CmpLtF64(a, b, out)
	if out[0] == 0 || out[1] != 0 || out[2] != 0 {
		t.Errorf("CmpLtF64 failed: got %v", out)
	}

	CmpLeF64(a, b, out)
	if out[0] == 0 || out[1] != 0 || out[2] == 0 {
		t.Errorf("CmpLeF64 failed: got %v", out)
	}

	CmpEqF64(a, b, out)
	if out[0] != 0 || out[1] != 0 || out[2] == 0 {
		t.Errorf("CmpEqF64 failed: got %v", out)
	}

	CmpNeF64(a, b, out)
	if out[0] == 0 || out[1] == 0 || out[2] != 0 {
		t.Errorf("CmpNeF64 failed: got %v", out)
	}
}

func TestCGO_Float64_Filtering(t *testing.T) {
	data := []float64{1.0, 5.0, 3.0, 7.0, 2.0}

	indices := FilterGreaterThanF64(data, 3.0)
	if len(indices) != 2 { // 5.0 and 7.0
		t.Errorf("FilterGreaterThanF64 failed: got %d indices", len(indices))
	}

	mask := FilterMaskGreaterThanF64(data, 3.0)
	trueCount := 0
	for _, m := range mask {
		if m {
			trueCount++
		}
	}
	if trueCount != 2 {
		t.Errorf("FilterMaskGreaterThanF64 failed: got %d true values", trueCount)
	}
}

func TestCGO_Float64_Sorting(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0}

	ascIndices := ArgsortF64(data, true)
	if data[ascIndices[0]] != 1.0 || data[ascIndices[3]] != 8.0 {
		t.Errorf("ArgsortF64 ascending failed")
	}

	descIndices := ArgsortF64(data, false)
	if data[descIndices[0]] != 8.0 || data[descIndices[3]] != 1.0 {
		t.Errorf("ArgsortF64 descending failed")
	}
}

// ============================================================================
// Int64 Operations
// ============================================================================

func TestCGO_Int64_Aggregations(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}

	sum := SumI64(data)
	if sum != 15 {
		t.Errorf("SumI64 failed: got %d", sum)
	}

	min := MinI64(data)
	if min != 1 {
		t.Errorf("MinI64 failed: got %d", min)
	}

	max := MaxI64(data)
	if max != 5 {
		t.Errorf("MaxI64 failed: got %d", max)
	}
}

func TestCGO_Int64_Arithmetic(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{4, 5, 6}
	out := make([]int64, 3)

	AddI64(a, b, out)
	if out[0] != 5 || out[1] != 7 || out[2] != 9 {
		t.Errorf("AddI64 failed: got %v", out)
	}

	SubI64(a, b, out)
	if out[0] != -3 || out[1] != -3 || out[2] != -3 {
		t.Errorf("SubI64 failed: got %v", out)
	}

	MulI64(a, b, out)
	if out[0] != 4 || out[1] != 10 || out[2] != 18 {
		t.Errorf("MulI64 failed: got %v", out)
	}
}

func TestCGO_Int64_ScalarOps(t *testing.T) {
	data := []int64{1, 2, 3}

	AddScalarI64(data, 10)
	if data[0] != 11 || data[1] != 12 || data[2] != 13 {
		t.Errorf("AddScalarI64 failed: got %v", data)
	}

	data = []int64{1, 2, 3}
	MulScalarI64(data, 2)
	if data[0] != 2 || data[1] != 4 || data[2] != 6 {
		t.Errorf("MulScalarI64 failed: got %v", data)
	}
}

func TestCGO_Int64_Filtering(t *testing.T) {
	data := []int64{1, 5, 3, 7, 2}

	indices := FilterGreaterThanI64(data, 3)
	if len(indices) != 2 { // 5 and 7
		t.Errorf("FilterGreaterThanI64 failed: got %d indices", len(indices))
	}
}

func TestCGO_Int64_Sorting(t *testing.T) {
	data := []int64{5, 2, 8, 1}

	ascIndices := ArgsortI64(data, true)
	if data[ascIndices[0]] != 1 || data[ascIndices[3]] != 8 {
		t.Errorf("ArgsortI64 ascending failed")
	}
}

// ============================================================================
// Int32 Operations
// ============================================================================

func TestCGO_Int32_Aggregations(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}

	sum := SumI32(data)
	if sum != 15 {
		t.Errorf("SumI32 failed: got %d", sum)
	}

	min := MinI32(data)
	if min != 1 {
		t.Errorf("MinI32 failed: got %d", min)
	}

	max := MaxI32(data)
	if max != 5 {
		t.Errorf("MaxI32 failed: got %d", max)
	}
}

func TestCGO_Int32_ScalarOps(t *testing.T) {
	data := []int32{1, 2, 3}

	AddScalarI32(data, 10)
	if data[0] != 11 || data[1] != 12 || data[2] != 13 {
		t.Errorf("AddScalarI32 failed: got %v", data)
	}

	data = []int32{1, 2, 3}
	MulScalarI32(data, 2)
	if data[0] != 2 || data[1] != 4 || data[2] != 6 {
		t.Errorf("MulScalarI32 failed: got %v", data)
	}
}

func TestCGO_Int32_Filtering(t *testing.T) {
	data := []int32{1, 5, 3, 7, 2}

	indices := FilterGreaterThanI32(data, 3)
	if len(indices) != 2 { // 5 and 7
		t.Errorf("FilterGreaterThanI32 failed: got %d indices", len(indices))
	}
}

func TestCGO_Int32_Sorting(t *testing.T) {
	data := []int32{5, 2, 8, 1}

	ascIndices := ArgsortI32(data, true)
	if data[ascIndices[0]] != 1 || data[ascIndices[3]] != 8 {
		t.Errorf("ArgsortI32 ascending failed")
	}
}

// ============================================================================
// Float32 Operations
// ============================================================================

func TestCGO_Float32_Aggregations(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}

	sum := SumF32(data)
	if sum != 15.0 {
		t.Errorf("SumF32 failed: got %f", sum)
	}

	min := MinF32(data)
	if min != 1.0 {
		t.Errorf("MinF32 failed: got %f", min)
	}

	max := MaxF32(data)
	if max != 5.0 {
		t.Errorf("MaxF32 failed: got %f", max)
	}

	mean := MeanF32(data)
	if mean != 3.0 {
		t.Errorf("MeanF32 failed: got %f", mean)
	}
}

func TestCGO_Float32_ScalarOps(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0}

	AddScalarF32(data, 10.0)
	if data[0] != 11.0 || data[1] != 12.0 || data[2] != 13.0 {
		t.Errorf("AddScalarF32 failed: got %v", data)
	}

	data = []float32{1.0, 2.0, 3.0}
	MulScalarF32(data, 2.0)
	if data[0] != 2.0 || data[1] != 4.0 || data[2] != 6.0 {
		t.Errorf("MulScalarF32 failed: got %v", data)
	}
}

func TestCGO_Float32_Filtering(t *testing.T) {
	data := []float32{1.0, 5.0, 3.0, 7.0, 2.0}

	indices := FilterGreaterThanF32(data, 3.0)
	if len(indices) != 2 { // 5.0 and 7.0
		t.Errorf("FilterGreaterThanF32 failed: got %d indices", len(indices))
	}
}

func TestCGO_Float32_Sorting(t *testing.T) {
	data := []float32{5.0, 2.0, 8.0, 1.0}

	ascIndices := ArgsortF32(data, true)
	if data[ascIndices[0]] != 1.0 || data[ascIndices[3]] != 8.0 {
		t.Errorf("ArgsortF32 ascending failed")
	}
}

// ============================================================================
// Boolean Operations
// ============================================================================

func TestCGO_Bool_Counting(t *testing.T) {
	data := []bool{true, false, true, true, false}

	trueCount := CountTrue(data)
	if trueCount != 3 {
		t.Errorf("CountTrue failed: got %d", trueCount)
	}

	falseCount := CountFalse(data)
	if falseCount != 2 {
		t.Errorf("CountFalse failed: got %d", falseCount)
	}
}

// ============================================================================
// Mask Operations
// ============================================================================

func TestCGO_Mask_Operations(t *testing.T) {
	mask := []byte{1, 0, 1, 1, 0, 1}

	count := CountMaskTrue(mask)
	if count != 4 {
		t.Errorf("CountMaskTrue failed: got %d", count)
	}

	indices := make([]uint32, count)
	n := IndicesFromMask(mask, indices)
	if n != 4 {
		t.Errorf("IndicesFromMask returned wrong count: got %d", n)
	}
}

// ============================================================================
// Hashing Operations
// ============================================================================

func TestCGO_Hash_Int64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	hashes := make([]uint64, len(data))

	HashI64Column(data, hashes)

	// Verify hashes are computed (non-zero for non-zero input)
	for i, h := range hashes {
		if h == 0 && data[i] != 0 {
			t.Logf("Warning: Hash of %d is 0", data[i])
		}
	}
}

func TestCGO_Hash_Int32(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	hashes := make([]uint64, len(data))

	HashI32Column(data, hashes)
	// Just verify it doesn't crash
}

func TestCGO_Hash_Float64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	hashes := make([]uint64, len(data))

	HashF64Column(data, hashes)
	// Just verify it doesn't crash
}

func TestCGO_Hash_Float32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	hashes := make([]uint64, len(data))

	HashF32Column(data, hashes)
	// Just verify it doesn't crash
}

func TestCGO_Hash_Combine(t *testing.T) {
	hash1 := []uint64{100, 200, 300}
	hash2 := []uint64{400, 500, 600}
	out := make([]uint64, 3)

	CombineHashes(hash1, hash2, out)
	// Just verify it doesn't crash and produces results
	for i, h := range out {
		if h == 0 {
			t.Logf("Warning: Combined hash %d is 0", i)
		}
	}
}

// ============================================================================
// Gather Operations
// ============================================================================

func TestCGO_Gather_F64(t *testing.T) {
	src := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	indices := []int32{2, 0, 4, 1}
	dst := make([]float64, len(indices))

	GatherF64(src, indices, dst)

	expected := []float64{30.0, 10.0, 50.0, 20.0}
	for i := range dst {
		if dst[i] != expected[i] {
			t.Errorf("GatherF64 mismatch at %d: got %f, want %f", i, dst[i], expected[i])
		}
	}
}

func TestCGO_Gather_I64(t *testing.T) {
	src := []int64{10, 20, 30, 40, 50}
	indices := []int32{2, 0, 4, 1}
	dst := make([]int64, len(indices))

	GatherI64(src, indices, dst)

	expected := []int64{30, 10, 50, 20}
	for i := range dst {
		if dst[i] != expected[i] {
			t.Errorf("GatherI64 mismatch at %d: got %d, want %d", i, dst[i], expected[i])
		}
	}
}

func TestCGO_Gather_I32(t *testing.T) {
	src := []int32{10, 20, 30, 40, 50}
	indices := []int32{2, 0, 4, 1}
	dst := make([]int32, len(indices))

	GatherI32(src, indices, dst)

	expected := []int32{30, 10, 50, 20}
	for i := range dst {
		if dst[i] != expected[i] {
			t.Errorf("GatherI32 mismatch at %d: got %d, want %d", i, dst[i], expected[i])
		}
	}
}

func TestCGO_Gather_F32(t *testing.T) {
	src := []float32{10.0, 20.0, 30.0, 40.0, 50.0}
	indices := []int32{2, 0, 4, 1}
	dst := make([]float32, len(indices))

	GatherF32(src, indices, dst)

	expected := []float32{30.0, 10.0, 50.0, 20.0}
	for i := range dst {
		if dst[i] != expected[i] {
			t.Errorf("GatherF32 mismatch at %d: got %f, want %f", i, dst[i], expected[i])
		}
	}
}

// ============================================================================
// GroupBy Aggregation Operations
// ============================================================================

func TestCGO_GroupBy_SumF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	groupIDs := []uint32{0, 0, 1, 1, 0}
	outSums := make([]float64, 2)

	AggregateSumF64ByGroup(data, groupIDs, outSums)

	// Group 0: 1.0 + 2.0 + 5.0 = 8.0
	// Group 1: 3.0 + 4.0 = 7.0
	if outSums[0] != 8.0 || outSums[1] != 7.0 {
		t.Errorf("AggregateSumF64ByGroup failed: got %v", outSums)
	}
}

func TestCGO_GroupBy_SumI64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	groupIDs := []uint32{0, 0, 1, 1, 0}
	outSums := make([]int64, 2)

	AggregateSumI64ByGroup(data, groupIDs, outSums)

	if outSums[0] != 8 || outSums[1] != 7 {
		t.Errorf("AggregateSumI64ByGroup failed: got %v", outSums)
	}
}

func TestCGO_GroupBy_MinF64(t *testing.T) {
	data := []float64{5.0, 2.0, 3.0, 1.0, 4.0}
	groupIDs := []uint32{0, 0, 1, 1, 0}
	outMins := make([]float64, 2)
	// Must initialize to MaxFloat64 as per function docs
	for i := range outMins {
		outMins[i] = 1e308 // Large value for min comparison
	}

	AggregateMinF64ByGroup(data, groupIDs, outMins)

	// Group 0: min(5, 2, 4) = 2
	// Group 1: min(3, 1) = 1
	if outMins[0] != 2.0 || outMins[1] != 1.0 {
		t.Errorf("AggregateMinF64ByGroup failed: got %v", outMins)
	}
}

func TestCGO_GroupBy_MaxF64(t *testing.T) {
	data := []float64{5.0, 2.0, 3.0, 1.0, 4.0}
	groupIDs := []uint32{0, 0, 1, 1, 0}
	outMaxs := make([]float64, 2)
	// Must initialize to -MaxFloat64 as per function docs
	for i := range outMaxs {
		outMaxs[i] = -1e308 // Small value for max comparison
	}

	AggregateMaxF64ByGroup(data, groupIDs, outMaxs)

	// Group 0: max(5, 2, 4) = 5
	// Group 1: max(3, 1) = 3
	if outMaxs[0] != 5.0 || outMaxs[1] != 3.0 {
		t.Errorf("AggregateMaxF64ByGroup failed: got %v", outMaxs)
	}
}

func TestCGO_GroupBy_Count(t *testing.T) {
	groupIDs := []uint32{0, 0, 1, 1, 0, 2, 2}
	outCounts := make([]uint64, 3)

	CountByGroup(groupIDs, outCounts)

	// Group 0: 3, Group 1: 2, Group 2: 2
	if outCounts[0] != 3 || outCounts[1] != 2 || outCounts[2] != 2 {
		t.Errorf("CountByGroup failed: got %v", outCounts)
	}
}

// ============================================================================
// Join Operations
// ============================================================================

func TestCGO_InnerJoin_E2E(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4}
	rightKeys := []int64{2, 3, 5}

	result := InnerJoinI64E2E(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("InnerJoinI64E2E returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	// Expected matches: 2 and 3
	if len(result.LeftIndices) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(result.LeftIndices))
	}
}

func TestCGO_InnerJoin_Simple(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4}
	rightKeys := []int64{2, 3, 5}

	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	if len(leftIndices) != len(rightIndices) {
		t.Errorf("Index array lengths differ: %d vs %d",
			len(leftIndices), len(rightIndices))
	}

	if len(leftIndices) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(leftIndices))
	}
}

func TestCGO_ParallelInnerJoin(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{2, 4, 6}

	result := ParallelInnerJoinI64(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("ParallelInnerJoinI64 returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	// Expected matches: 2 and 4
	if len(result.LeftIndices) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(result.LeftIndices))
	}
}

func TestCGO_LeftJoin_E2E(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4}
	rightKeys := []int64{2, 3, 5}

	result := LeftJoinI64E2E(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("LeftJoinI64E2E returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	// All left rows should be preserved
	if len(result.LeftIndices) < 4 {
		t.Errorf("Left join should preserve at least 4 rows, got %d",
			len(result.LeftIndices))
	}
}

func TestCGO_LeftJoin_Simple(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4}
	rightKeys := []int64{2, 3, 5}

	leftIndices, rightIndices := LeftJoinI64(leftKeys, rightKeys)

	if len(leftIndices) != len(rightIndices) {
		t.Errorf("Index array lengths differ")
	}

	// All left rows should be preserved
	if len(leftIndices) < 4 {
		t.Errorf("Left join should preserve at least 4 rows, got %d",
			len(leftIndices))
	}
}

func TestCGO_ParallelLeftJoin(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{2, 4, 6}

	result := ParallelLeftJoinI64(leftKeys, rightKeys)
	if result == nil {
		t.Fatal("ParallelLeftJoinI64 returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	// All left rows should be preserved
	if len(result.LeftIndices) < 5 {
		t.Errorf("Left join should preserve at least 5 rows, got %d",
			len(result.LeftIndices))
	}
}

// ============================================================================
// GroupBy Hash Table Operations
// ============================================================================

func TestCGO_ComputeGroupIDs(t *testing.T) {
	hashes := []uint64{100, 200, 100, 300, 200}

	groupIDs, numGroups := ComputeGroupIDs(hashes)

	if numGroups <= 0 || numGroups > len(hashes) {
		t.Errorf("Invalid numGroups: %d", numGroups)
	}

	if len(groupIDs) != len(hashes) {
		t.Errorf("GroupIDs length mismatch: got %d, want %d",
			len(groupIDs), len(hashes))
	}
}

func TestCGO_ComputeGroupIDsExt(t *testing.T) {
	hashes := []uint64{100, 200, 100, 300, 200}

	result := ComputeGroupIDsExt(hashes)
	if result == nil {
		t.Fatal("ComputeGroupIDsExt returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	if result.NumGroups <= 0 || result.NumGroups > len(hashes) {
		t.Errorf("Invalid NumGroups: %d", result.NumGroups)
	}
}

func TestCGO_GroupBySumE2E(t *testing.T) {
	keyData := []int64{1, 1, 2, 2, 2}
	valueData := []float64{10.0, 20.0, 30.0, 40.0, 50.0}

	result := GroupBySumE2E(keyData, valueData)
	if result == nil {
		t.Fatal("GroupBySumE2E returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	if result.NumGroups != 2 {
		t.Errorf("Expected 2 groups, got %d", result.NumGroups)
	}
}

func TestCGO_GroupByMultiAggE2E(t *testing.T) {
	keyData := []int64{1, 1, 2, 2, 2}
	valueData := []float64{10.0, 20.0, 30.0, 40.0, 50.0}

	result := GroupByMultiAggE2E(keyData, valueData)
	if result == nil {
		t.Fatal("GroupByMultiAggE2E returned nil")
	}
	// Note: result has unexported free() method - GC will handle cleanup

	if result.NumGroups != 2 {
		t.Errorf("Expected 2 groups, got %d", result.NumGroups)
	}

	// Verify we have sum, min, max, count for each group
	if len(result.Sums) != 2 || len(result.Mins) != 2 ||
		len(result.Maxs) != 2 || len(result.Counts) != 2 {
		t.Error("Missing aggregation results")
	}
}

// ============================================================================
// Column Types (ColumnF64, ColumnI64, etc.)
// ============================================================================

func TestCGO_ColumnF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewColumnF64(data)
	if col == nil {
		t.Fatal("NewColumnF64 returned nil")
	}
	// Note: col has unexported free() method - GC will handle cleanup

	if col.Len() != len(data) {
		t.Errorf("Column length mismatch: got %d, want %d", col.Len(), len(data))
	}

	colData := col.Data()
	for i := range data {
		if colData[i] != data[i] {
			t.Errorf("Data mismatch at %d: got %f, want %f", i, colData[i], data[i])
		}
	}
}

func TestCGO_ColumnI64(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5}
	col := NewColumnI64(data)
	if col == nil {
		t.Fatal("NewColumnI64 returned nil")
	}
	// Note: col has unexported free() method - GC will handle cleanup

	if col.Len() != len(data) {
		t.Errorf("Column length mismatch")
	}
}

func TestCGO_ColumnI32(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	col := NewColumnI32(data)
	if col == nil {
		t.Fatal("NewColumnI32 returned nil")
	}
	// Note: col has unexported free() method - GC will handle cleanup

	if col.Len() != len(data) {
		t.Errorf("Column length mismatch")
	}
}

func TestCGO_ColumnF32(t *testing.T) {
	data := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	col := NewColumnF32(data)
	if col == nil {
		t.Fatal("NewColumnF32 returned nil")
	}
	// Note: col has unexported free() method - GC will handle cleanup

	if col.Len() != len(data) {
		t.Errorf("Column length mismatch")
	}
}

func TestCGO_ColumnBool(t *testing.T) {
	data := []bool{true, false, true, false}
	col := NewColumnBool(data)
	if col == nil {
		t.Fatal("NewColumnBool returned nil")
	}
	// Note: col has unexported free() method - GC will handle cleanup

	if col.Len() != len(data) {
		t.Errorf("Column length mismatch")
	}
}
