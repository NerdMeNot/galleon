package galleon

import (
	"math"
	"testing"
)

func TestMedianF64(t *testing.T) {
	// Odd length - exact middle value
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	median, valid := MedianF64(data)
	if !valid {
		t.Fatal("MedianF64 returned invalid for valid input")
	}
	if math.Abs(median-5.0) > 0.0001 {
		t.Errorf("MedianF64 = %v, want 5.0", median)
	}

	// Even length - average of two middle values
	data = []float64{1.0, 2.0, 3.0, 4.0}
	median, valid = MedianF64(data)
	if !valid {
		t.Fatal("MedianF64 returned invalid for valid input")
	}
	if math.Abs(median-2.5) > 0.0001 {
		t.Errorf("MedianF64 = %v, want 2.5", median)
	}

	// Single element
	data = []float64{42.0}
	median, valid = MedianF64(data)
	if !valid {
		t.Fatal("MedianF64 returned invalid for single element")
	}
	if median != 42.0 {
		t.Errorf("MedianF64 = %v, want 42.0", median)
	}

	// Empty slice
	_, valid = MedianF64([]float64{})
	if valid {
		t.Error("MedianF64 should return invalid for empty slice")
	}
}

func TestQuantileF64(t *testing.T) {
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}

	// 50th percentile (median)
	q50, valid := QuantileF64(data, 0.5)
	if !valid {
		t.Fatal("QuantileF64(0.5) returned invalid")
	}
	if math.Abs(q50-5.5) > 0.0001 {
		t.Errorf("QuantileF64(0.5) = %v, want 5.5", q50)
	}

	// 25th percentile
	q25, valid := QuantileF64(data, 0.25)
	if !valid {
		t.Fatal("QuantileF64(0.25) returned invalid")
	}
	if math.Abs(q25-3.25) > 0.0001 {
		t.Errorf("QuantileF64(0.25) = %v, want 3.25", q25)
	}

	// 75th percentile
	q75, valid := QuantileF64(data, 0.75)
	if !valid {
		t.Fatal("QuantileF64(0.75) returned invalid")
	}
	if math.Abs(q75-7.75) > 0.0001 {
		t.Errorf("QuantileF64(0.75) = %v, want 7.75", q75)
	}

	// 0th percentile (min)
	q0, valid := QuantileF64(data, 0.0)
	if !valid {
		t.Fatal("QuantileF64(0.0) returned invalid")
	}
	if q0 != 1.0 {
		t.Errorf("QuantileF64(0.0) = %v, want 1.0", q0)
	}

	// 100th percentile (max)
	q100, valid := QuantileF64(data, 1.0)
	if !valid {
		t.Fatal("QuantileF64(1.0) returned invalid")
	}
	if q100 != 10.0 {
		t.Errorf("QuantileF64(1.0) = %v, want 10.0", q100)
	}

	// Invalid q values
	_, valid = QuantileF64(data, -0.1)
	if valid {
		t.Error("QuantileF64 should return invalid for q < 0")
	}

	_, valid = QuantileF64(data, 1.1)
	if valid {
		t.Error("QuantileF64 should return invalid for q > 1")
	}
}

func TestSkewnessF64(t *testing.T) {
	// Symmetric distribution should have skewness near 0
	symmetric := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}
	skew, valid := SkewnessF64(symmetric)
	if !valid {
		t.Fatal("SkewnessF64 returned invalid for symmetric data")
	}
	if math.Abs(skew) > 0.2 {
		t.Errorf("SkewnessF64 of symmetric data = %v, want near 0", skew)
	}

	// Right-skewed distribution (positive skew)
	rightSkewed := []float64{1.0, 1.0, 1.0, 2.0, 2.0, 3.0, 10.0}
	skew, valid = SkewnessF64(rightSkewed)
	if !valid {
		t.Fatal("SkewnessF64 returned invalid for right-skewed data")
	}
	if skew <= 0 {
		t.Errorf("SkewnessF64 of right-skewed data = %v, want > 0", skew)
	}

	// Insufficient data (n < 3)
	_, valid = SkewnessF64([]float64{1.0, 2.0})
	if valid {
		t.Error("SkewnessF64 should return invalid for n < 3")
	}
}

func TestKurtosisF64(t *testing.T) {
	// Test that kurtosis returns valid result for sufficient data
	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	kurt, valid := KurtosisF64(data)
	if !valid {
		t.Fatal("KurtosisF64 returned invalid for valid data")
	}
	// Uniform-like distribution has negative excess kurtosis
	if math.IsNaN(kurt) {
		t.Error("KurtosisF64 returned NaN")
	}

	// Heavy-tailed distribution (outliers)
	heavyTailed := []float64{-100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0}
	kurt, valid = KurtosisF64(heavyTailed)
	if !valid {
		t.Fatal("KurtosisF64 returned invalid for heavy-tailed data")
	}
	// Just verify it returns a reasonable value
	if math.IsNaN(kurt) {
		t.Error("KurtosisF64 returned NaN for heavy-tailed data")
	}

	// Insufficient data (n < 4)
	_, valid = KurtosisF64([]float64{1.0, 2.0, 3.0})
	if valid {
		t.Error("KurtosisF64 should return invalid for n < 4")
	}
}

func TestCorrelationF64(t *testing.T) {
	// Perfect positive correlation
	x := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	y := []float64{2.0, 4.0, 6.0, 8.0, 10.0}
	corr, valid := CorrelationF64(x, y)
	if !valid {
		t.Fatal("CorrelationF64 returned invalid for perfect positive correlation")
	}
	if math.Abs(corr-1.0) > 0.0001 {
		t.Errorf("CorrelationF64 = %v, want 1.0", corr)
	}

	// Perfect negative correlation
	y = []float64{10.0, 8.0, 6.0, 4.0, 2.0}
	corr, valid = CorrelationF64(x, y)
	if !valid {
		t.Fatal("CorrelationF64 returned invalid for perfect negative correlation")
	}
	if math.Abs(corr+1.0) > 0.0001 {
		t.Errorf("CorrelationF64 = %v, want -1.0", corr)
	}

	// No correlation (approximately)
	y = []float64{5.0, 3.0, 5.0, 3.0, 5.0}
	corr, valid = CorrelationF64(x, y)
	if !valid {
		t.Fatal("CorrelationF64 returned invalid for uncorrelated data")
	}
	if math.Abs(corr) > 0.5 {
		t.Errorf("CorrelationF64 = %v, want near 0", corr)
	}

	// Length mismatch
	_, valid = CorrelationF64([]float64{1.0, 2.0, 3.0}, []float64{1.0, 2.0})
	if valid {
		t.Error("CorrelationF64 should return invalid for length mismatch")
	}

	// Zero variance
	_, valid = CorrelationF64([]float64{1.0, 1.0, 1.0}, []float64{1.0, 2.0, 3.0})
	if valid {
		t.Error("CorrelationF64 should return invalid for zero variance")
	}
}

func TestVarianceF64(t *testing.T) {
	// Known variance: [2, 4, 4, 4, 5, 5, 7, 9] has variance ≈ 4.571
	data := []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}
	variance, valid := VarianceF64(data)
	if !valid {
		t.Fatal("VarianceF64 returned invalid for valid data")
	}
	if math.Abs(variance-4.571) > 0.01 {
		t.Errorf("VarianceF64 = %v, want ~4.571", variance)
	}

	// Insufficient data (n < 2)
	_, valid = VarianceF64([]float64{1.0})
	if valid {
		t.Error("VarianceF64 should return invalid for n < 2")
	}
}

func TestStdDevF64(t *testing.T) {
	// Known stddev: sqrt(4.571) ≈ 2.138
	data := []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}
	stddev, valid := StdDevF64(data)
	if !valid {
		t.Fatal("StdDevF64 returned invalid for valid data")
	}
	if math.Abs(stddev-2.138) > 0.01 {
		t.Errorf("StdDevF64 = %v, want ~2.138", stddev)
	}

	// Insufficient data (n < 2)
	_, valid = StdDevF64([]float64{1.0})
	if valid {
		t.Error("StdDevF64 should return invalid for n < 2")
	}
}

func TestMedianF64Large(t *testing.T) {
	// Test with larger data to exercise SIMD paths
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i + 1)
	}

	median, valid := MedianF64(data)
	if !valid {
		t.Fatal("MedianF64 returned invalid for large dataset")
	}
	// For 1..1000, median = (500 + 501) / 2 = 500.5
	if math.Abs(median-500.5) > 0.0001 {
		t.Errorf("MedianF64 of 1..1000 = %v, want 500.5", median)
	}
}

func TestCorrelationF64Large(t *testing.T) {
	// Test with larger data to exercise SIMD paths
	n := 1000
	x := make([]float64, n)
	y := make([]float64, n)
	for i := 0; i < n; i++ {
		x[i] = float64(i)
		y[i] = float64(i) * 2.0 // Perfect positive correlation
	}

	corr, valid := CorrelationF64(x, y)
	if !valid {
		t.Fatal("CorrelationF64 returned invalid for large dataset")
	}
	if math.Abs(corr-1.0) > 0.0001 {
		t.Errorf("CorrelationF64 = %v, want 1.0", corr)
	}
}

// ============================================================================
// Expression Builder API Tests
// ============================================================================

func TestMedianExprBuilder(t *testing.T) {
	expr := Col("x").Median()

	if expr == nil {
		t.Fatal("Median expr should not be nil")
	}

	expected := `col("x").median()`
	if expr.String() != expected {
		t.Errorf("Median.String() = %q, want %q", expr.String(), expected)
	}
}

func TestQuantileExprBuilder(t *testing.T) {
	expr := Col("x").Quantile(0.95)

	if expr == nil {
		t.Fatal("Quantile expr should not be nil")
	}

	expected := `col("x").quantile(0.95)`
	if expr.String() != expected {
		t.Errorf("Quantile.String() = %q, want %q", expr.String(), expected)
	}
}

func TestSkewExprBuilder(t *testing.T) {
	expr := Col("x").Skew()

	if expr == nil {
		t.Fatal("Skew expr should not be nil")
	}

	expected := `col("x").skewness()`
	if expr.String() != expected {
		t.Errorf("Skew.String() = %q, want %q", expr.String(), expected)
	}
}

func TestKurtExprBuilder(t *testing.T) {
	expr := Col("x").Kurt()

	if expr == nil {
		t.Fatal("Kurt expr should not be nil")
	}

	expected := `col("x").kurtosis()`
	if expr.String() != expected {
		t.Errorf("Kurt.String() = %q, want %q", expr.String(), expected)
	}
}

func TestCorrelationExprBuilder(t *testing.T) {
	expr := Col("x").Corr(Col("y"))

	if expr == nil {
		t.Fatal("Correlation expr should not be nil")
	}

	expected := `corr(col("x"), col("y"))`
	if expr.String() != expected {
		t.Errorf("Corr.String() = %q, want %q", expr.String(), expected)
	}
}

func TestCorrFunctionBuilder(t *testing.T) {
	expr := Corr(Col("a"), Col("b"))

	if expr == nil {
		t.Fatal("Corr function expr should not be nil")
	}

	expected := `corr(col("a"), col("b"))`
	if expr.String() != expected {
		t.Errorf("Corr.String() = %q, want %q", expr.String(), expected)
	}
}
