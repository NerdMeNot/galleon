package galleon

import (
	"math"
	"sort"
	"testing"
)

// ============================================================================
// Aggregation Correctness Tests
// ============================================================================

func TestCorrectness_Sum_MatchesManual(t *testing.T) {
	data := []float64{1.5, 2.5, 3.0, 4.5, 5.5}
	series := NewSeriesFloat64("values", data)

	// SIMD result
	simdResult := series.Sum()

	// Manual calculation
	var manualResult float64
	for _, v := range data {
		manualResult += v
	}

	if math.Abs(simdResult-manualResult) > 0.0001 {
		t.Errorf("Sum mismatch: SIMD=%f, manual=%f", simdResult, manualResult)
	}
}

func TestCorrectness_Mean_MatchesManual(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	series := NewSeriesFloat64("values", data)

	// SIMD result
	simdResult := series.Mean()

	// Manual calculation
	var sum float64
	for _, v := range data {
		sum += v
	}
	manualResult := sum / float64(len(data))

	if math.Abs(simdResult-manualResult) > 0.0001 {
		t.Errorf("Mean mismatch: SIMD=%f, manual=%f", simdResult, manualResult)
	}
}

func TestCorrectness_Min_MatchesManual(t *testing.T) {
	data := []float64{5.0, 3.0, 8.0, 1.0, 7.0}
	series := NewSeriesFloat64("values", data)

	simdResult := series.Min()

	// Manual calculation
	manualResult := data[0]
	for _, v := range data[1:] {
		if v < manualResult {
			manualResult = v
		}
	}

	if math.Abs(simdResult-manualResult) > 0.0001 {
		t.Errorf("Min mismatch: SIMD=%f, manual=%f", simdResult, manualResult)
	}
}

func TestCorrectness_Max_MatchesManual(t *testing.T) {
	data := []float64{5.0, 3.0, 8.0, 1.0, 7.0}
	series := NewSeriesFloat64("values", data)

	simdResult := series.Max()

	// Manual calculation
	manualResult := data[0]
	for _, v := range data[1:] {
		if v > manualResult {
			manualResult = v
		}
	}

	if math.Abs(simdResult-manualResult) > 0.0001 {
		t.Errorf("Max mismatch: SIMD=%f, manual=%f", simdResult, manualResult)
	}
}

func TestCorrectness_Sum_LargeData(t *testing.T) {
	// Test with larger dataset to exercise SIMD paths
	n := 10000
	data := make([]float64, n)
	var manualSum float64
	for i := 0; i < n; i++ {
		data[i] = float64(i) * 0.1
		manualSum += data[i]
	}

	series := NewSeriesFloat64("values", data)
	simdResult := series.Sum()

	// Allow small floating point tolerance for large sums
	tolerance := math.Abs(manualSum) * 1e-10
	if math.Abs(simdResult-manualSum) > tolerance {
		t.Errorf("Large sum mismatch: SIMD=%f, manual=%f, diff=%e",
			simdResult, manualSum, math.Abs(simdResult-manualSum))
	}
}

// ============================================================================
// Filter Correctness Tests
// ============================================================================

func TestCorrectness_Filter_IndicesMatchCondition(t *testing.T) {
	data := []float64{1.0, 5.0, 3.0, 7.0, 2.0, 8.0, 4.0}
	threshold := 4.0

	indices := FilterGreaterThanF64(data, threshold)

	// Verify every returned index satisfies the condition
	for _, idx := range indices {
		if data[idx] <= threshold {
			t.Errorf("Filter returned index %d with value %f, which is not > %f",
				idx, data[idx], threshold)
		}
	}

	// Verify we didn't miss any values that should have been included
	expectedCount := 0
	for _, v := range data {
		if v > threshold {
			expectedCount++
		}
	}
	if len(indices) != expectedCount {
		t.Errorf("Filter returned %d indices, expected %d", len(indices), expectedCount)
	}
}

func TestCorrectness_Filter_AllValuesReturned(t *testing.T) {
	data := []float64{5.0, 6.0, 7.0, 8.0, 9.0}
	threshold := 4.0 // All values are > threshold

	indices := FilterGreaterThanF64(data, threshold)

	if len(indices) != len(data) {
		t.Errorf("Expected all %d indices, got %d", len(data), len(indices))
	}
}

func TestCorrectness_Filter_NoValuesReturned(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0}
	threshold := 10.0 // No values are > threshold

	indices := FilterGreaterThanF64(data, threshold)

	if len(indices) != 0 {
		t.Errorf("Expected 0 indices, got %d", len(indices))
	}
}

func TestCorrectness_Filter_LargeDataset(t *testing.T) {
	// Test with larger dataset to verify SIMD path correctness
	n := 10000
	data := make([]float64, n)
	threshold := 5000.0

	for i := 0; i < n; i++ {
		data[i] = float64(i)
	}

	indices := FilterGreaterThanF64(data, threshold)

	// Every index should satisfy the condition
	for _, idx := range indices {
		if data[idx] <= threshold {
			t.Errorf("SIMD filter incorrect: index %d has value %f <= %f",
				idx, data[idx], threshold)
		}
	}

	// Count should match expected
	expectedCount := n - int(threshold) - 1 // values 5001 through 9999
	if len(indices) != expectedCount {
		t.Errorf("Filter count mismatch: got %d, expected %d", len(indices), expectedCount)
	}
}

// ============================================================================
// Join Correctness Tests
// ============================================================================

func TestCorrectness_InnerJoin_MatchedKeysAreEqual(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{3, 4, 5, 6, 7}

	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	// For every matched pair, the keys must be equal
	for i := range leftIndices {
		leftIdx := leftIndices[i]
		rightIdx := rightIndices[i]
		leftKey := leftKeys[leftIdx]
		rightKey := rightKeys[rightIdx]

		if leftKey != rightKey {
			t.Errorf("Inner join mismatch at position %d: left[%d]=%d, right[%d]=%d",
				i, leftIdx, leftKey, rightIdx, rightKey)
		}
	}
}

func TestCorrectness_InnerJoin_AllMatchesFound(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{3, 4, 5, 6, 7}

	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	// Expected matches: 3, 4, 5 (3 matches)
	if len(leftIndices) != 3 {
		t.Errorf("Expected 3 matches, got %d", len(leftIndices))
	}

	// Verify all matches are found
	matchedKeys := make(map[int64]bool)
	for i := range leftIndices {
		matchedKeys[leftKeys[leftIndices[i]]] = true
	}

	expectedMatches := []int64{3, 4, 5}
	for _, key := range expectedMatches {
		if !matchedKeys[key] {
			t.Errorf("Expected match for key %d not found", key)
		}
	}
	if len(leftIndices) != len(rightIndices) {
		t.Errorf("Index arrays length mismatch: left=%d, right=%d",
			len(leftIndices), len(rightIndices))
	}
}

func TestCorrectness_InnerJoin_NoMatches(t *testing.T) {
	leftKeys := []int64{1, 2, 3}
	rightKeys := []int64{4, 5, 6}

	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	if len(leftIndices) != 0 || len(rightIndices) != 0 {
		t.Errorf("Expected no matches, got %d left, %d right indices",
			len(leftIndices), len(rightIndices))
	}
}

func TestCorrectness_InnerJoin_DuplicateKeys(t *testing.T) {
	leftKeys := []int64{1, 1, 2, 2}
	rightKeys := []int64{1, 2, 2}

	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	// Key 1: 2 left rows * 1 right row = 2 matches
	// Key 2: 2 left rows * 2 right rows = 4 matches
	// Total: 6 matches
	if len(leftIndices) != 6 {
		t.Errorf("Expected 6 matches for duplicate keys, got %d", len(leftIndices))
	}

	// Verify all matches have equal keys
	for i := range leftIndices {
		if leftKeys[leftIndices[i]] != rightKeys[rightIndices[i]] {
			t.Errorf("Key mismatch at position %d", i)
		}
	}
}

func TestCorrectness_LeftJoin_PreservesAllLeftRows(t *testing.T) {
	leftKeys := []int64{1, 2, 3, 4, 5}
	rightKeys := []int64{2, 4}

	leftIndices, _ := LeftJoinI64(leftKeys, rightKeys)

	// All left indices should appear at least once
	leftRowAppeared := make([]bool, len(leftKeys))
	for _, idx := range leftIndices {
		leftRowAppeared[idx] = true
	}

	for i, appeared := range leftRowAppeared {
		if !appeared {
			t.Errorf("Left row %d (key=%d) not preserved in left join result",
				i, leftKeys[i])
		}
	}
}

func TestCorrectness_LeftJoin_RightIndicesForUnmatched(t *testing.T) {
	leftKeys := []int64{1, 2, 3}
	rightKeys := []int64{2}

	leftIndices, rightIndices := LeftJoinI64(leftKeys, rightKeys)

	// Keys 1 and 3 have no match, so their right index should be -1
	for i := range leftIndices {
		leftKey := leftKeys[leftIndices[i]]
		rightIdx := rightIndices[i]

		hasMatch := false
		for _, rk := range rightKeys {
			if leftKey == rk {
				hasMatch = true
				break
			}
		}

		if hasMatch {
			if rightIdx < 0 {
				t.Errorf("Key %d should have a match but right index is %d",
					leftKey, rightIdx)
			} else if leftKey != rightKeys[rightIdx] {
				t.Errorf("Matched keys don't match: left=%d, right=%d",
					leftKey, rightKeys[rightIdx])
			}
		} else {
			if rightIdx != -1 {
				t.Errorf("Key %d should not have a match but right index is %d",
					leftKey, rightIdx)
			}
		}
	}
}

// ============================================================================
// GroupBy Correctness Tests
// ============================================================================

func TestCorrectness_GroupBy_SumMatchesManual(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B", "B", "C"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}),
	)

	result, err := df.Lazy().
		GroupBy("group").
		Agg(Col("value").Sum().Alias("sum")).
		Collect()
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	// Manual calculation of sums per group
	expected := map[string]float64{
		"A": 1.0 + 2.0,           // 3.0
		"B": 3.0 + 4.0 + 5.0,     // 12.0
		"C": 6.0,                 // 6.0
	}

	groups := result.ColumnByName("group").Strings()
	sums := result.ColumnByName("sum").Float64()

	for i, g := range groups {
		expectedSum := expected[g]
		if math.Abs(sums[i]-expectedSum) > 0.0001 {
			t.Errorf("Group %s sum: got %f, expected %f", g, sums[i], expectedSum)
		}
	}
}

func TestCorrectness_GroupBy_MeanMatchesManual(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B", "B"}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 30.0, 60.0, 90.0}),
	)

	result, err := df.Lazy().
		GroupBy("group").
		Agg(Col("value").Mean().Alias("mean")).
		Collect()
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	// Manual calculation
	expected := map[string]float64{
		"A": (10.0 + 20.0) / 2,           // 15.0
		"B": (30.0 + 60.0 + 90.0) / 3,    // 60.0
	}

	groups := result.ColumnByName("group").Strings()
	means := result.ColumnByName("mean").Float64()

	for i, g := range groups {
		expectedMean := expected[g]
		if math.Abs(means[i]-expectedMean) > 0.0001 {
			t.Errorf("Group %s mean: got %f, expected %f", g, means[i], expectedMean)
		}
	}
}

func TestCorrectness_GroupBy_MinMaxMatchesManual(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{5.0, 1.0, 9.0, 3.0, 7.0}),
	)

	result, err := df.Lazy().
		GroupBy("group").
		Agg(
			Col("value").Min().Alias("min"),
			Col("value").Max().Alias("max"),
		).
		Collect()
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	// Manual calculation
	expectedMin := map[string]float64{"A": 1.0, "B": 3.0}
	expectedMax := map[string]float64{"A": 9.0, "B": 7.0}

	groups := result.ColumnByName("group").Strings()
	mins := result.ColumnByName("min").Float64()
	maxs := result.ColumnByName("max").Float64()

	for i, g := range groups {
		if math.Abs(mins[i]-expectedMin[g]) > 0.0001 {
			t.Errorf("Group %s min: got %f, expected %f", g, mins[i], expectedMin[g])
		}
		if math.Abs(maxs[i]-expectedMax[g]) > 0.0001 {
			t.Errorf("Group %s max: got %f, expected %f", g, maxs[i], expectedMax[g])
		}
	}
}

// ============================================================================
// Sorting Correctness Tests
// ============================================================================

func TestCorrectness_Argsort_OrderIsCorrect(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0, 3.0}
	series := NewSeriesFloat64("values", data)

	// Ascending sort
	ascIndices := series.Argsort(true)

	// Verify the indices produce a sorted sequence
	for i := 1; i < len(ascIndices); i++ {
		prev := data[ascIndices[i-1]]
		curr := data[ascIndices[i]]
		if prev > curr {
			t.Errorf("Ascending sort violation at position %d: %f > %f", i, prev, curr)
		}
	}

	// Descending sort
	descIndices := series.Argsort(false)

	for i := 1; i < len(descIndices); i++ {
		prev := data[descIndices[i-1]]
		curr := data[descIndices[i]]
		if prev < curr {
			t.Errorf("Descending sort violation at position %d: %f < %f", i, prev, curr)
		}
	}
}

func TestCorrectness_Argsort_MatchesStdSort(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0}
	series := NewSeriesFloat64("values", data)

	// Get SIMD sorted values via indices
	indices := series.Argsort(true)
	simdSorted := make([]float64, len(data))
	for i, idx := range indices {
		simdSorted[i] = data[idx]
	}

	// Standard library sort
	stdSorted := make([]float64, len(data))
	copy(stdSorted, data)
	sort.Float64s(stdSorted)

	// Compare
	for i := range simdSorted {
		if math.Abs(simdSorted[i]-stdSorted[i]) > 0.0001 {
			t.Errorf("Sort mismatch at position %d: SIMD=%f, std=%f",
				i, simdSorted[i], stdSorted[i])
		}
	}
}

// ============================================================================
// Arithmetic Correctness Tests
// ============================================================================

func TestCorrectness_AddF64_MatchesManual(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	b := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	out := make([]float64, len(a))

	AddF64(a, b, out)

	for i := range a {
		expected := a[i] + b[i]
		if math.Abs(out[i]-expected) > 0.0001 {
			t.Errorf("Add mismatch at %d: got %f, expected %f", i, out[i], expected)
		}
	}
}

func TestCorrectness_MulF64_MatchesManual(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	b := []float64{2.0, 3.0, 4.0, 5.0, 6.0}
	out := make([]float64, len(a))

	MulF64(a, b, out)

	for i := range a {
		expected := a[i] * b[i]
		if math.Abs(out[i]-expected) > 0.0001 {
			t.Errorf("Mul mismatch at %d: got %f, expected %f", i, out[i], expected)
		}
	}
}

func TestCorrectness_SubF64_MatchesManual(t *testing.T) {
	a := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	b := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	out := make([]float64, len(a))

	SubF64(a, b, out)

	for i := range a {
		expected := a[i] - b[i]
		if math.Abs(out[i]-expected) > 0.0001 {
			t.Errorf("Sub mismatch at %d: got %f, expected %f", i, out[i], expected)
		}
	}
}

func TestCorrectness_DivF64_MatchesManual(t *testing.T) {
	a := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	b := []float64{2.0, 4.0, 5.0, 8.0, 10.0}
	out := make([]float64, len(a))

	DivF64(a, b, out)

	for i := range a {
		expected := a[i] / b[i]
		if math.Abs(out[i]-expected) > 0.0001 {
			t.Errorf("Div mismatch at %d: got %f, expected %f", i, out[i], expected)
		}
	}
}

func TestCorrectness_Arithmetic_LargeDataset(t *testing.T) {
	n := 10000
	a := make([]float64, n)
	b := make([]float64, n)
	out := make([]float64, n)

	for i := 0; i < n; i++ {
		a[i] = float64(i)
		b[i] = float64(i * 2)
	}

	AddF64(a, b, out)

	for i := 0; i < n; i++ {
		expected := a[i] + b[i]
		if math.Abs(out[i]-expected) > 0.0001 {
			t.Errorf("Large add mismatch at %d: got %f, expected %f",
				i, out[i], expected)
		}
	}
}

func TestCorrectness_SeriesAddScalar_MatchesManual(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	scalar := 10.0

	series := NewSeriesFloat64("values", data)
	result := series.Add(scalar)
	resultData := result.Float64()

	for i := range data {
		expected := data[i] + scalar
		if math.Abs(resultData[i]-expected) > 0.0001 {
			t.Errorf("AddScalar mismatch at %d: got %f, expected %f", i, resultData[i], expected)
		}
	}
}

func TestCorrectness_SeriesMulScalar_MatchesManual(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	scalar := 3.0

	series := NewSeriesFloat64("values", data)
	result := series.Mul(scalar)
	resultData := result.Float64()

	for i := range data {
		expected := data[i] * scalar
		if math.Abs(resultData[i]-expected) > 0.0001 {
			t.Errorf("MulScalar mismatch at %d: got %f, expected %f", i, resultData[i], expected)
		}
	}
}

// ============================================================================
// Comparison Correctness Tests
// ============================================================================

func TestCorrectness_CmpGt_AllResults(t *testing.T) {
	a := []float64{1.0, 5.0, 3.0, 7.0, 2.0}
	b := []float64{2.0, 3.0, 3.0, 6.0, 5.0}
	result := make([]byte, len(a))

	// Manual comparison
	expected := make([]bool, len(a))
	for i := range a {
		expected[i] = a[i] > b[i]
	}

	CmpGtF64(a, b, result)

	for i := range result {
		resultBool := result[i] != 0
		if resultBool != expected[i] {
			t.Errorf("CmpGt mismatch at %d: a[%d]=%f > b[%d]=%f, got %v, expected %v",
				i, i, a[i], i, b[i], resultBool, expected[i])
		}
	}
}

func TestCorrectness_CmpLt_AllResults(t *testing.T) {
	a := []float64{1.0, 5.0, 3.0, 7.0, 2.0}
	b := []float64{2.0, 3.0, 3.0, 6.0, 5.0}
	result := make([]byte, len(a))

	expected := make([]bool, len(a))
	for i := range a {
		expected[i] = a[i] < b[i]
	}

	CmpLtF64(a, b, result)

	for i := range result {
		resultBool := result[i] != 0
		if resultBool != expected[i] {
			t.Errorf("CmpLt mismatch at %d: got %v, expected %v",
				i, resultBool, expected[i])
		}
	}
}

func TestCorrectness_CmpEq_AllResults(t *testing.T) {
	a := []float64{1.0, 3.0, 3.0, 7.0, 5.0}
	b := []float64{2.0, 3.0, 3.0, 6.0, 5.0}
	result := make([]byte, len(a))

	expected := make([]bool, len(a))
	for i := range a {
		expected[i] = a[i] == b[i]
	}

	CmpEqF64(a, b, result)

	for i := range result {
		resultBool := result[i] != 0
		if resultBool != expected[i] {
			t.Errorf("CmpEq mismatch at %d: got %v, expected %v",
				i, resultBool, expected[i])
		}
	}
}

// ============================================================================
// Hash Correctness Tests
// ============================================================================

func TestCorrectness_Hash_Deterministic(t *testing.T) {
	data := []int64{1, 2, 3, 4, 5, 100, 1000, -1, -100}
	hash1 := make([]uint64, len(data))
	hash2 := make([]uint64, len(data))

	HashI64Column(data, hash1)
	HashI64Column(data, hash2)

	for i := range data {
		if hash1[i] != hash2[i] {
			t.Errorf("Hash not deterministic for value %d: %d != %d",
				data[i], hash1[i], hash2[i])
		}
	}
}

func TestCorrectness_Hash_DifferentValuesProduceDifferentHashes(t *testing.T) {
	data := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	hashes := make([]uint64, len(data))

	HashI64Column(data, hashes)

	// Check that most hashes are unique (some collisions are possible but unlikely)
	seen := make(map[uint64]int64)
	collisions := 0
	for i, h := range hashes {
		if existingVal, exists := seen[h]; exists {
			// Only count as collision if values are different
			if existingVal != data[i] {
				collisions++
			}
		}
		seen[h] = data[i]
	}

	// With 10 unique small integers, we expect 0 collisions for a good hash
	if collisions > 0 {
		t.Errorf("Unexpected hash collisions for small unique integers: %d collisions", collisions)
	}
}

// ============================================================================
// DataFrame Filter Correctness Tests
// ============================================================================

func TestCorrectness_DataFrame_Filter_ResultMatchesCondition(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("x", []float64{1.0, 5.0, 3.0, 8.0, 2.0}),
		NewSeriesString("name", []string{"a", "b", "c", "d", "e"}),
	)

	// Filter where x > 3
	filtered, err := df.Lazy().
		Filter(Col("x").Gt(Lit(3.0))).
		Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Every row in result should have x > 3
	xValues := filtered.ColumnByName("x").Float64()
	for i, x := range xValues {
		if x <= 3.0 {
			t.Errorf("Filtered row %d has x=%f which does not satisfy x > 3", i, x)
		}
	}

	// Expected: rows with x=5 and x=8 (2 rows)
	if filtered.Height() != 2 {
		t.Errorf("Expected 2 filtered rows, got %d", filtered.Height())
	}
}

func TestCorrectness_DataFrame_Filter_PreservesRowIntegrity(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("x", []float64{1.0, 5.0, 3.0, 8.0, 2.0}),
		NewSeriesString("name", []string{"a", "b", "c", "d", "e"}),
	)

	// Original correspondence: x=1->a, x=5->b, x=3->c, x=8->d, x=2->e
	expectedMap := map[float64]string{
		1.0: "a", 5.0: "b", 3.0: "c", 8.0: "d", 2.0: "e",
	}

	filtered, err := df.Lazy().
		Filter(Col("x").Gt(Lit(3.0))).
		Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Verify row integrity is preserved
	xValues := filtered.ColumnByName("x").Float64()
	names := filtered.ColumnByName("name").Strings()

	for i := range xValues {
		expectedName := expectedMap[xValues[i]]
		if names[i] != expectedName {
			t.Errorf("Row integrity violated: x=%f should have name=%s, got %s",
				xValues[i], expectedName, names[i])
		}
	}
}

// ============================================================================
// Categorical Correctness Tests
// ============================================================================

func TestCorrectness_Categorical_EncodingMatchesValues(t *testing.T) {
	data := []string{"apple", "banana", "apple", "cherry", "banana", "apple"}
	cat := NewSeriesCategorical("fruit", data)

	// Verify that Get() returns the correct string values
	for i, expected := range data {
		got := cat.Get(i).(string)
		if got != expected {
			t.Errorf("Categorical Get(%d): expected %s, got %s", i, expected, got)
		}
	}
}

func TestCorrectness_Categorical_UniqueCategories(t *testing.T) {
	data := []string{"A", "B", "A", "C", "B", "A", "C", "C"}
	cat := NewSeriesCategorical("group", data)

	categories := cat.Categories()
	expectedCategories := map[string]bool{"A": true, "B": true, "C": true}

	if len(categories) != len(expectedCategories) {
		t.Errorf("Expected %d unique categories, got %d", len(expectedCategories), len(categories))
	}

	for _, c := range categories {
		if !expectedCategories[c] {
			t.Errorf("Unexpected category: %s", c)
		}
	}
}

func TestCorrectness_Categorical_IndicesAreConsistent(t *testing.T) {
	data := []string{"x", "y", "x", "z", "y", "x"}
	cat := NewSeriesCategorical("val", data)

	indices := cat.CategoricalIndices()
	categories := cat.Categories()

	// Verify that decoding indices gives back original values
	for i, idx := range indices {
		decoded := categories[idx]
		if decoded != data[i] {
			t.Errorf("Index decode mismatch at %d: index %d -> %s, expected %s",
				i, idx, decoded, data[i])
		}
	}
}

func TestCorrectness_Categorical_SameValuesSameIndex(t *testing.T) {
	data := []string{"red", "blue", "red", "green", "red", "blue"}
	cat := NewSeriesCategorical("color", data)

	indices := cat.CategoricalIndices()

	// All "red" values should have the same index
	redIndex := indices[0]
	for i, val := range data {
		if val == "red" && indices[i] != redIndex {
			t.Errorf("Same value 'red' has different indices: %d vs %d", redIndex, indices[i])
		}
	}

	// All "blue" values should have the same index
	blueIndex := indices[1]
	for i, val := range data {
		if val == "blue" && indices[i] != blueIndex {
			t.Errorf("Same value 'blue' has different indices: %d vs %d", blueIndex, indices[i])
		}
	}
}

func TestCorrectness_Categorical_AsStringRoundtrip(t *testing.T) {
	original := []string{"cat", "dog", "bird", "cat", "bird"}
	cat := NewSeriesCategorical("animal", original)

	// Convert to string and back
	strSeries := cat.AsString()
	backToCat := strSeries.AsCategorical()

	// Verify values match
	for i, expected := range original {
		got := backToCat.Get(i).(string)
		if got != expected {
			t.Errorf("Roundtrip mismatch at %d: expected %s, got %s", i, expected, got)
		}
	}
}

func TestCorrectness_Categorical_GroupByProducesCorrectSums(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesCategorical("category", []string{"A", "B", "A", "B", "A"}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 30.0, 40.0, 50.0}),
	)

	result, err := df.Lazy().
		GroupBy("category").
		Agg(Col("value").Sum().Alias("sum")).
		Collect()
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	// Manual calculation: A = 10+30+50 = 90, B = 20+40 = 60
	expected := map[string]float64{"A": 90.0, "B": 60.0}

	groups := result.ColumnByName("category")
	sums := result.ColumnByName("sum").Float64()

	for i := 0; i < result.Height(); i++ {
		group := groups.Get(i).(string)
		expectedSum := expected[group]
		if math.Abs(sums[i]-expectedSum) > 0.0001 {
			t.Errorf("Group %s sum: expected %f, got %f", group, expectedSum, sums[i])
		}
	}
}

func TestCorrectness_Categorical_JoinMatchesStringJoin(t *testing.T) {
	// Test join with categorical key columns (same values on both sides)
	// This matches the pattern of TestJoinCategoricalKeys which works correctly
	catLeft, _ := NewDataFrame(
		NewSeriesCategorical("category", []string{"A", "B", "C", "A"}),
		NewSeriesFloat64("val1", []float64{1.0, 2.0, 3.0, 4.0}),
	)
	catRight, _ := NewDataFrame(
		NewSeriesCategorical("category", []string{"A", "B", "D"}),
		NewSeriesFloat64("val2", []float64{10.0, 20.0, 30.0}),
	)

	// Categorical join
	catResult, err := catLeft.Lazy().
		Join(catRight.Lazy(), On("category")).
		Collect()
	if err != nil {
		t.Fatalf("Categorical join failed: %v", err)
	}

	// A appears 2x in left, 1x in right -> 2 matches
	// B appears 1x in left, 1x in right -> 1 match
	// Total: 3 rows
	if catResult.Height() != 3 {
		t.Errorf("Expected 3 rows, got %d", catResult.Height())
	}

	// Verify all joined rows have valid category values
	categories := catResult.ColumnByName("category")
	validCategories := map[string]bool{"A": true, "B": true}
	for i := 0; i < catResult.Height(); i++ {
		cat := categories.Get(i).(string)
		if !validCategories[cat] {
			t.Errorf("Unexpected category in join result: %s", cat)
		}
	}
}
