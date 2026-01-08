package galleon

import (
	"sync/atomic"
	"testing"
)

// ============================================================================
// ParallelConfig Tests
// ============================================================================

func TestDefaultParallelConfig(t *testing.T) {
	cfg := DefaultParallelConfig()

	if cfg == nil {
		t.Fatal("DefaultParallelConfig returned nil")
	}
	if cfg.MinRowsForParallel <= 0 {
		t.Errorf("MinRowsForParallel should be positive, got %d", cfg.MinRowsForParallel)
	}
	if cfg.MorselSize <= 0 {
		t.Errorf("MorselSize should be positive, got %d", cfg.MorselSize)
	}
	if !cfg.Enabled {
		t.Error("Enabled should be true by default")
	}
}

func TestSetGetParallelConfig(t *testing.T) {
	// Save original config
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	// Set custom config
	custom := &ParallelConfig{
		MinRowsForParallel: 1000,
		MorselSize:         512,
		MaxWorkers:         2,
		Enabled:            false,
	}
	SetParallelConfig(custom)

	got := GetParallelConfig()
	if got.MinRowsForParallel != 1000 {
		t.Errorf("MinRowsForParallel = %d, want 1000", got.MinRowsForParallel)
	}
	if got.MorselSize != 512 {
		t.Errorf("MorselSize = %d, want 512", got.MorselSize)
	}
	if got.MaxWorkers != 2 {
		t.Errorf("MaxWorkers = %d, want 2", got.MaxWorkers)
	}
	if got.Enabled {
		t.Error("Enabled should be false")
	}

	// Setting nil should not change config
	SetParallelConfig(nil)
	if GetParallelConfig() != custom {
		t.Error("SetParallelConfig(nil) should not change config")
	}
}

func TestParallelConfig_NumWorkers(t *testing.T) {
	cfg := &ParallelConfig{MaxWorkers: 4}
	if cfg.numWorkers() != 4 {
		t.Errorf("numWorkers() = %d, want 4", cfg.numWorkers())
	}

	cfg.MaxWorkers = 0
	workers := cfg.numWorkers()
	if workers <= 0 {
		t.Errorf("numWorkers() with MaxWorkers=0 should use GOMAXPROCS, got %d", workers)
	}
}

func TestParallelConfig_ShouldParallelize(t *testing.T) {
	cfg := &ParallelConfig{
		MinRowsForParallel: 1000,
		Enabled:            true,
	}

	if cfg.shouldParallelize(500) {
		t.Error("Should not parallelize 500 rows when min is 1000")
	}
	if !cfg.shouldParallelize(2000) {
		t.Error("Should parallelize 2000 rows when min is 1000")
	}

	cfg.Enabled = false
	if cfg.shouldParallelize(2000) {
		t.Error("Should not parallelize when disabled")
	}
}

// ============================================================================
// Morsel Iterator Tests
// ============================================================================

func TestNewMorselIterator(t *testing.T) {
	mi := NewMorselIterator(100, 10)

	if mi.totalRows != 100 {
		t.Errorf("totalRows = %d, want 100", mi.totalRows)
	}
	if mi.morselSize != 10 {
		t.Errorf("morselSize = %d, want 10", mi.morselSize)
	}

	// Test default morsel size
	mi2 := NewMorselIterator(100, 0)
	if mi2.morselSize <= 0 {
		t.Error("morselSize should use default when 0")
	}
}

func TestMorselIterator_Next(t *testing.T) {
	mi := NewMorselIterator(25, 10)

	// First morsel: 0-10
	m1 := mi.Next()
	if m1 == nil || m1.Start != 0 || m1.End != 10 {
		t.Errorf("First morsel = %v, want {0, 10}", m1)
	}

	// Second morsel: 10-20
	m2 := mi.Next()
	if m2 == nil || m2.Start != 10 || m2.End != 20 {
		t.Errorf("Second morsel = %v, want {10, 20}", m2)
	}

	// Third morsel: 20-25 (partial)
	m3 := mi.Next()
	if m3 == nil || m3.Start != 20 || m3.End != 25 {
		t.Errorf("Third morsel = %v, want {20, 25}", m3)
	}

	// Fourth call should return nil
	m4 := mi.Next()
	if m4 != nil {
		t.Errorf("Fourth morsel should be nil, got %v", m4)
	}
}

func TestMorselIterator_Empty(t *testing.T) {
	mi := NewMorselIterator(0, 10)
	m := mi.Next()
	if m != nil {
		t.Errorf("Empty iterator should return nil, got %v", m)
	}
}

// ============================================================================
// Parallel Execution Tests
// ============================================================================

func TestParallelFor_Sequential(t *testing.T) {
	// Save and restore config
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	// Force sequential execution
	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10000,
		MorselSize:         100,
		Enabled:            true,
	})

	sum := int64(0)
	ParallelFor(100, func(start, end int) {
		for i := start; i < end; i++ {
			atomic.AddInt64(&sum, int64(i))
		}
	})

	// Sum of 0..99 = 4950
	expected := int64(99 * 100 / 2)
	if sum != expected {
		t.Errorf("Sum = %d, want %d", sum, expected)
	}
}

func TestParallelFor_Parallel(t *testing.T) {
	// Save and restore config
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	// Force parallel execution
	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10,
		MorselSize:         100,
		MaxWorkers:         4,
		Enabled:            true,
	})

	sum := int64(0)
	ParallelFor(1000, func(start, end int) {
		localSum := int64(0)
		for i := start; i < end; i++ {
			localSum += int64(i)
		}
		atomic.AddInt64(&sum, localSum)
	})

	// Sum of 0..999 = 499500
	expected := int64(999 * 1000 / 2)
	if sum != expected {
		t.Errorf("Sum = %d, want %d", sum, expected)
	}
}

func TestParallelForWithResult(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10,
		MorselSize:         100,
		MaxWorkers:         4,
		Enabled:            true,
	})

	results := ParallelForWithResult(500, func(start, end int) int {
		sum := 0
		for i := start; i < end; i++ {
			sum += i
		}
		return sum
	})

	// Sum all partial results
	total := 0
	for _, r := range results {
		total += r
	}

	// Sum of 0..499 = 124750
	expected := 499 * 500 / 2
	if total != expected {
		t.Errorf("Total = %d, want %d", total, expected)
	}
}

func TestParallelMap(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 5,
		MorselSize:         10,
		MaxWorkers:         2,
		Enabled:            true,
	})

	// Test parallel map (doubles each value)
	results := ParallelMap(100, func(i int) int {
		return i * 2
	})

	if len(results) != 100 {
		t.Errorf("Results length = %d, want 100", len(results))
	}

	for i, r := range results {
		if r != i*2 {
			t.Errorf("results[%d] = %d, want %d", i, r, i*2)
		}
	}
}

func TestParallelMapSlice(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 5,
		MorselSize:         10,
		MaxWorkers:         2,
		Enabled:            true,
	})

	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results := ParallelMapSlice(input, func(x int) int {
		return x * x
	})

	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	for i, r := range results {
		if r != expected[i] {
			t.Errorf("results[%d] = %d, want %d", i, r, expected[i])
		}
	}
}

// ============================================================================
// Partitioned Hash Index Tests
// ============================================================================

func TestNewPartitionedHashIndex(t *testing.T) {
	phi := NewPartitionedHashIndex(4)
	if phi.numParts != 4 {
		t.Errorf("numParts = %d, want 4", phi.numParts)
	}

	// Test power of 2 adjustment
	phi2 := NewPartitionedHashIndex(5)
	if phi2.numParts != 8 {
		t.Errorf("numParts for 5 = %d, want 8 (next power of 2)", phi2.numParts)
	}

	// Test default
	phi3 := NewPartitionedHashIndex(0)
	if phi3.numParts <= 0 {
		t.Error("numParts should be positive for 0 input")
	}
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
	}

	for _, tc := range tests {
		result := nextPowerOf2(tc.input)
		if result != tc.expected {
			t.Errorf("nextPowerOf2(%d) = %d, want %d", tc.input, result, tc.expected)
		}
	}
}

func TestPartitionedHashIndex_BuildAndLookup(t *testing.T) {
	phi := NewPartitionedHashIndex(4)

	hashes := []uint64{100, 200, 100, 300, 200, 100}
	phi.BuildParallel(hashes)

	// Lookup hash 100 (should return indices 0, 2, 5)
	indices100 := phi.Lookup(100)
	if len(indices100) != 3 {
		t.Errorf("Lookup(100) returned %d indices, want 3", len(indices100))
	}

	// Verify all indices point to hash 100
	for _, idx := range indices100 {
		if hashes[idx] != 100 {
			t.Errorf("Lookup(100) returned index %d which has hash %d", idx, hashes[idx])
		}
	}

	// Lookup hash 200 (should return indices 1, 4)
	indices200 := phi.Lookup(200)
	if len(indices200) != 2 {
		t.Errorf("Lookup(200) returned %d indices, want 2", len(indices200))
	}

	// Lookup non-existent hash
	indices999 := phi.Lookup(999)
	if len(indices999) != 0 {
		t.Errorf("Lookup(999) should return empty, got %v", indices999)
	}
}

// ============================================================================
// Parallel Reduce Tests
// ============================================================================

func TestParallelReduceFloat64_Sequential(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10000,
		Enabled:            true,
	})

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	sum := ParallelReduceFloat64(data, 0, func(a, b float64) float64 {
		return a + b
	})

	if sum != 15.0 {
		t.Errorf("Sum = %f, want 15.0", sum)
	}
}

func TestParallelReduceFloat64_Parallel(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10,
		MorselSize:         100,
		MaxWorkers:         4,
		Enabled:            true,
	})

	// Create large data
	data := make([]float64, 1000)
	var expected float64
	for i := range data {
		data[i] = float64(i)
		expected += float64(i)
	}

	sum := ParallelReduceFloat64(data, 0, func(a, b float64) float64 {
		return a + b
	})

	if sum != expected {
		t.Errorf("Sum = %f, want %f", sum, expected)
	}
}

func TestParallelReduceInt64_Sequential(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10000,
		Enabled:            true,
	})

	data := []int64{1, 2, 3, 4, 5}
	sum := ParallelReduceInt64(data, 0, func(a, b int64) int64 {
		return a + b
	})

	if sum != 15 {
		t.Errorf("Sum = %d, want 15", sum)
	}
}

func TestParallelReduceInt64_Parallel(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10,
		MorselSize:         100,
		MaxWorkers:         4,
		Enabled:            true,
	})

	data := make([]int64, 1000)
	var expected int64
	for i := range data {
		data[i] = int64(i)
		expected += int64(i)
	}

	sum := ParallelReduceInt64(data, 0, func(a, b int64) int64 {
		return a + b
	})

	if sum != expected {
		t.Errorf("Sum = %d, want %d", sum, expected)
	}
}

// ============================================================================
// Parallel Build Columns Tests
// ============================================================================

func TestParallelBuildColumns(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		Enabled: true,
	})

	cols := ParallelBuildColumns(3, func(colIdx int) *Series {
		data := make([]float64, 5)
		for i := range data {
			data[i] = float64(colIdx*10 + i)
		}
		return NewSeriesFloat64("col", data)
	})

	if len(cols) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(cols))
	}

	for colIdx, col := range cols {
		data := col.Float64()
		for i, v := range data {
			expected := float64(colIdx*10 + i)
			if v != expected {
				t.Errorf("cols[%d][%d] = %f, want %f", colIdx, i, v, expected)
			}
		}
	}
}

func TestParallelBuildColumns_Disabled(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		Enabled: false,
	})

	cols := ParallelBuildColumns(2, func(colIdx int) *Series {
		return NewSeriesFloat64("col", []float64{float64(colIdx)})
	})

	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
}

// ============================================================================
// Cost-Based Parallelization Tests
// ============================================================================

func TestEstimatedCostPerRow(t *testing.T) {
	// Just verify each operation type returns a positive cost
	ops := []OperationType{
		OpFilter, OpSort, OpJoinBuild, OpJoinProbe,
		OpGroupByHash, OpGroupByAgg, OpGather,
	}

	for _, op := range ops {
		cost := EstimatedCostPerRow(op)
		if cost <= 0 {
			t.Errorf("Cost for %v should be positive, got %d", op, cost)
		}
	}

	// Test unknown operation
	unknownCost := EstimatedCostPerRow(OperationType(999))
	if unknownCost <= 0 {
		t.Errorf("Cost for unknown op should be positive, got %d", unknownCost)
	}
}

func TestShouldParallelizeOp(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MaxWorkers: 4,
		Enabled:    true,
	})

	// Small data should not parallelize
	if ShouldParallelizeOp(OpFilter, 10) {
		t.Error("Should not parallelize 10 rows for filter")
	}

	// Large data should parallelize
	if !ShouldParallelizeOp(OpSort, 1000000) {
		t.Error("Should parallelize 1M rows for sort")
	}

	// Disabled config
	SetParallelConfig(&ParallelConfig{Enabled: false})
	if ShouldParallelizeOp(OpSort, 1000000) {
		t.Error("Should not parallelize when disabled")
	}
}

// ============================================================================
// Join Match Collection Tests
// ============================================================================

func TestCollectJoinMatches_Sequential(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 10000,
		Enabled:            true,
	})

	// Build right index
	rightHashes := []uint64{100, 200, 300}
	rightIndex := NewPartitionedHashIndex(4)
	rightIndex.BuildParallel(rightHashes)

	// Left hashes with some matches
	leftHashes := []uint64{100, 400, 200, 500}

	matches := CollectJoinMatches(
		len(leftHashes),
		leftHashes,
		rightIndex,
		func(leftRow int, rightRows []int) []JoinMatch {
			result := make([]JoinMatch, len(rightRows))
			for i, r := range rightRows {
				result[i] = JoinMatch{LeftIdx: leftRow, RightIdx: r}
			}
			return result
		},
	)

	// Should have 2 matches: leftRow 0 -> rightRow 0, leftRow 2 -> rightRow 1
	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}
}

func TestCollectJoinMatches_Parallel(t *testing.T) {
	original := GetParallelConfig()
	defer SetParallelConfig(original)

	SetParallelConfig(&ParallelConfig{
		MinRowsForParallel: 5,
		MorselSize:         10,
		MaxWorkers:         2,
		Enabled:            true,
	})

	// Create larger dataset
	n := 100
	rightHashes := make([]uint64, n)
	for i := range rightHashes {
		rightHashes[i] = uint64(i % 10) // 10 unique values
	}

	rightIndex := NewPartitionedHashIndex(4)
	rightIndex.BuildParallel(rightHashes)

	leftHashes := make([]uint64, n)
	for i := range leftHashes {
		leftHashes[i] = uint64(i % 10)
	}

	matches := CollectJoinMatches(
		len(leftHashes),
		leftHashes,
		rightIndex,
		func(leftRow int, rightRows []int) []JoinMatch {
			result := make([]JoinMatch, len(rightRows))
			for i, r := range rightRows {
				result[i] = JoinMatch{LeftIdx: leftRow, RightIdx: r}
			}
			return result
		},
	)

	// Each of 100 left rows matches 10 right rows = 1000 matches
	expectedMatches := n * (n / 10)
	if len(matches) != expectedMatches {
		t.Errorf("Expected %d matches, got %d", expectedMatches, len(matches))
	}

	// Verify each match has correct hash
	for _, m := range matches {
		if leftHashes[m.LeftIdx] != rightHashes[m.RightIdx] {
			t.Errorf("Match mismatch: left[%d]=%d, right[%d]=%d",
				m.LeftIdx, leftHashes[m.LeftIdx],
				m.RightIdx, rightHashes[m.RightIdx])
		}
	}
}
