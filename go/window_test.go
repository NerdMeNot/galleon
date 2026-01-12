package galleon

import (
	"math"
	"testing"
)

func TestLagF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	out := make([]float64, len(data))

	LagF64(data, 2, 0.0, out)

	expected := []float64{0.0, 0.0, 1.0, 2.0, 3.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("LagF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestLeadF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	out := make([]float64, len(data))

	LeadF64(data, 2, 0.0, out)

	expected := []float64{3.0, 4.0, 5.0, 0.0, 0.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("LeadF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestRowNumber(t *testing.T) {
	out := make([]uint32, 5)
	RowNumber(out)

	for i := 0; i < 5; i++ {
		if out[i] != uint32(i+1) {
			t.Errorf("RowNumber out[%d] = %v, want %v", i, out[i], i+1)
		}
	}
}

func TestRowNumberPartitioned(t *testing.T) {
	partitionIDs := []uint32{0, 0, 0, 1, 1}
	out := make([]uint32, len(partitionIDs))

	RowNumberPartitioned(partitionIDs, out)

	expected := []uint32{1, 2, 3, 1, 2}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("RowNumberPartitioned out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestRankF64(t *testing.T) {
	// Data with ties
	data := []float64{1.0, 2.0, 2.0, 3.0, 3.0}
	out := make([]uint32, len(data))

	RankF64(data, out)

	expected := []uint32{1, 2, 2, 4, 4} // Rank with gaps
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("RankF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestDenseRankF64(t *testing.T) {
	// Data with ties
	data := []float64{1.0, 2.0, 2.0, 3.0, 3.0}
	out := make([]uint32, len(data))

	DenseRankF64(data, out)

	expected := []uint32{1, 2, 2, 3, 3} // Dense rank (no gaps)
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("DenseRankF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCumSumF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	out := make([]float64, len(data))

	CumSumF64(data, out)

	expected := []float64{1.0, 3.0, 6.0, 10.0, 15.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("CumSumF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCumSumPartitionedF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	partitionIDs := []uint32{0, 0, 0, 1, 1}
	out := make([]float64, len(data))

	CumSumPartitionedF64(data, partitionIDs, out)

	expected := []float64{1.0, 3.0, 6.0, 4.0, 9.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("CumSumPartitionedF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCumMinF64(t *testing.T) {
	data := []float64{5.0, 3.0, 4.0, 2.0, 6.0}
	out := make([]float64, len(data))

	CumMinF64(data, out)

	expected := []float64{5.0, 3.0, 3.0, 2.0, 2.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("CumMinF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCumMaxF64(t *testing.T) {
	data := []float64{1.0, 3.0, 2.0, 5.0, 4.0}
	out := make([]float64, len(data))

	CumMaxF64(data, out)

	expected := []float64{1.0, 3.0, 3.0, 5.0, 5.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("CumMaxF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestRollingSumF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	out := make([]float64, len(data))

	RollingSumF64(data, 3, 1, out)

	expected := []float64{1.0, 3.0, 6.0, 9.0, 12.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("RollingSumF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestRollingMeanF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	out := make([]float64, len(data))

	RollingMeanF64(data, 3, 1, out)

	expected := []float64{1.0, 1.5, 2.0, 3.0, 4.0}
	for i, exp := range expected {
		if math.Abs(out[i]-exp) > 0.001 {
			t.Errorf("RollingMeanF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestRollingMinF64(t *testing.T) {
	data := []float64{3.0, 1.0, 4.0, 1.0, 5.0}
	out := make([]float64, len(data))

	RollingMinF64(data, 3, 1, out)

	expected := []float64{3.0, 1.0, 1.0, 1.0, 1.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("RollingMinF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestRollingMaxF64(t *testing.T) {
	data := []float64{1.0, 3.0, 2.0, 5.0, 4.0}
	out := make([]float64, len(data))

	RollingMaxF64(data, 3, 1, out)

	expected := []float64{1.0, 3.0, 3.0, 5.0, 5.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("RollingMaxF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestDiffF64(t *testing.T) {
	data := []float64{1.0, 3.0, 6.0, 10.0, 15.0}
	out := make([]float64, len(data))

	DiffF64(data, 0.0, out)

	expected := []float64{0.0, 2.0, 3.0, 4.0, 5.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("DiffF64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestPctChangeF64(t *testing.T) {
	data := []float64{100.0, 110.0, 99.0, 110.0}
	out := make([]float64, len(data))

	PctChangeF64(data, out)

	// First value is NaN
	if !math.IsNaN(out[0]) {
		t.Errorf("PctChangeF64 out[0] should be NaN, got %v", out[0])
	}

	expected := []float64{0.0, 0.1, -0.1, 0.1111}
	for i := 1; i < len(expected); i++ {
		if math.Abs(out[i]-expected[i]) > 0.001 {
			t.Errorf("PctChangeF64 out[%d] = %v, want %v", i, out[i], expected[i])
		}
	}
}
