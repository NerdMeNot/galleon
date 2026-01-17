package galleon

import (
	"math"
	"sort"
	"testing"
)

func TestArrowGroupBySum(t *testing.T) {
	// Create DataFrame with groups: 1, 1, 2, 2, 3
	keys := NewSeriesI64("key", []int64{1, 1, 2, 2, 3})
	values := NewSeriesF64("value", []float64{10.0, 20.0, 30.0, 40.0, 50.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy sum
	result := df.GroupBy("key").Sum("value")
	if result == nil {
		t.Fatal("GroupBy Sum returned nil")
	}

	// Check result
	if result.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", result.Height())
	}

	// Build a map of key -> sum for order-independent testing
	keyCol := result.Column("key")
	sumCol := result.Column("value_sum")

	if keyCol == nil || sumCol == nil {
		t.Fatal("Missing columns in result")
	}

	sums := make(map[int64]float64)
	for i := 0; i < result.Height(); i++ {
		k, _ := keyCol.AtI64(i)
		v, _ := sumCol.AtF64(i)
		sums[k] = v
	}

	// Verify sums: 1->30, 2->70, 3->50
	expected := map[int64]float64{1: 30.0, 2: 70.0, 3: 50.0}
	for k, expectedSum := range expected {
		if actualSum, ok := sums[k]; !ok {
			t.Errorf("Missing key %d in result", k)
		} else if math.Abs(actualSum-expectedSum) > 0.001 {
			t.Errorf("Key %d: expected sum %.1f, got %.1f", k, expectedSum, actualSum)
		}
	}
}

func TestArrowGroupByMean(t *testing.T) {
	// Create DataFrame with groups: 1, 1, 2, 2, 3
	keys := NewSeriesI64("key", []int64{1, 1, 2, 2, 3})
	values := NewSeriesF64("value", []float64{10.0, 20.0, 30.0, 40.0, 50.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy mean
	result := df.GroupBy("key").Mean("value")
	if result == nil {
		t.Fatal("GroupBy Mean returned nil")
	}

	// Check result
	if result.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", result.Height())
	}

	// Build a map of key -> mean for order-independent testing
	keyCol := result.Column("key")
	meanCol := result.Column("value_mean")

	if keyCol == nil || meanCol == nil {
		t.Fatal("Missing columns in result")
	}

	means := make(map[int64]float64)
	for i := 0; i < result.Height(); i++ {
		k, _ := keyCol.AtI64(i)
		v, _ := meanCol.AtF64(i)
		means[k] = v
	}

	// Verify means: 1->15, 2->35, 3->50
	expected := map[int64]float64{1: 15.0, 2: 35.0, 3: 50.0}
	for k, expectedMean := range expected {
		if actualMean, ok := means[k]; !ok {
			t.Errorf("Missing key %d in result", k)
		} else if math.Abs(actualMean-expectedMean) > 0.001 {
			t.Errorf("Key %d: expected mean %.1f, got %.1f", k, expectedMean, actualMean)
		}
	}
}

func TestArrowGroupByCount(t *testing.T) {
	// Create DataFrame with groups: 1, 1, 1, 2, 2, 3
	keys := NewSeriesI64("key", []int64{1, 1, 1, 2, 2, 3})
	values := NewSeriesF64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy count
	result := df.GroupBy("key").Count()
	if result == nil {
		t.Fatal("GroupBy Count returned nil")
	}

	// Check result
	if result.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", result.Height())
	}

	// Build a map of key -> count for order-independent testing
	keyCol := result.Column("key")
	countCol := result.Column("count")

	if keyCol == nil || countCol == nil {
		t.Fatal("Missing columns in result")
	}

	counts := make(map[int64]float64)
	for i := 0; i < result.Height(); i++ {
		k, _ := keyCol.AtI64(i)
		v, _ := countCol.AtF64(i) // count is stored as float64 in the Sum result
		counts[k] = v
	}

	// Verify counts: 1->3, 2->2, 3->1
	expected := map[int64]float64{1: 3.0, 2: 2.0, 3: 1.0}
	for k, expectedCount := range expected {
		if actualCount, ok := counts[k]; !ok {
			t.Errorf("Missing key %d in result", k)
		} else if math.Abs(actualCount-expectedCount) > 0.001 {
			t.Errorf("Key %d: expected count %.0f, got %.0f", k, expectedCount, actualCount)
		}
	}
}

func TestArrowGroupByAgg(t *testing.T) {
	// Create DataFrame with groups
	keys := NewSeriesI64("key", []int64{1, 1, 2, 2, 3})
	values := NewSeriesF64("value", []float64{10.0, 20.0, 30.0, 40.0, 50.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy with single aggregation
	result := df.GroupBy("key").Agg(map[string]string{"value": "sum"})
	if result == nil {
		t.Fatal("GroupBy Agg returned nil")
	}

	if result.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", result.Height())
	}
}

func TestArrowGroupByEmpty(t *testing.T) {
	// Create empty DataFrame
	keys := NewSeriesI64("key", []int64{})
	values := NewSeriesF64("value", []float64{})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy sum on empty
	result := df.GroupBy("key").Sum("value")
	if result == nil {
		t.Fatal("GroupBy Sum on empty returned nil")
	}

	if result.Height() != 0 {
		t.Errorf("Expected 0 groups for empty input, got %d", result.Height())
	}
}

func TestArrowGroupByInvalidColumn(t *testing.T) {
	keys := NewSeriesI64("key", []int64{1, 2, 3})
	values := NewSeriesF64("value", []float64{1.0, 2.0, 3.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy with non-existent column
	gb := df.GroupBy("nonexistent")
	if gb != nil {
		t.Error("Expected nil for non-existent groupby column")
	}

	// Sum with non-existent column
	result := df.GroupBy("key").Sum("nonexistent")
	if result != nil {
		t.Error("Expected nil for non-existent value column")
	}
}

func TestArrowGroupByManyGroups(t *testing.T) {
	// Create DataFrame with many groups
	n := 1000
	keys := make([]int64, n)
	values := make([]float64, n)

	for i := 0; i < n; i++ {
		keys[i] = int64(i % 100) // 100 groups
		values[i] = float64(i)
	}

	keysSeries := NewSeriesI64("key", keys)
	valuesSeries := NewSeriesF64("value", values)

	df := NewDataFrame()
	df = df.WithColumn(keysSeries)
	df = df.WithColumn(valuesSeries)

	// GroupBy sum
	result := df.GroupBy("key").Sum("value")
	if result == nil {
		t.Fatal("GroupBy Sum returned nil")
	}

	if result.Height() != 100 {
		t.Errorf("Expected 100 groups, got %d", result.Height())
	}

	// Verify that all group sums are correct
	// Each group i has values: i, i+100, i+200, ..., i+900 = 10i + (0+100+200+...+900) = 10i + 4500
	keyCol := result.Column("key")
	sumCol := result.Column("value_sum")

	for i := 0; i < result.Height(); i++ {
		k, _ := keyCol.AtI64(i)
		s, _ := sumCol.AtF64(i)
		expectedSum := float64(10*k + 4500)
		if math.Abs(s-expectedSum) > 0.001 {
			t.Errorf("Key %d: expected sum %.1f, got %.1f", k, expectedSum, s)
			break // Only report first error to avoid spam
		}
	}
}

func TestArrowGroupBySingleGroup(t *testing.T) {
	// All values in same group
	keys := NewSeriesI64("key", []int64{1, 1, 1, 1, 1})
	values := NewSeriesF64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy sum
	result := df.GroupBy("key").Sum("value")
	if result == nil {
		t.Fatal("GroupBy Sum returned nil")
	}

	if result.Height() != 1 {
		t.Errorf("Expected 1 group, got %d", result.Height())
	}

	keyCol := result.Column("key")
	sumCol := result.Column("value_sum")

	k, _ := keyCol.AtI64(0)
	s, _ := sumCol.AtF64(0)

	if k != 1 {
		t.Errorf("Expected key 1, got %d", k)
	}
	if math.Abs(s-15.0) > 0.001 {
		t.Errorf("Expected sum 15.0, got %.1f", s)
	}
}

func TestArrowGroupByNilDataFrame(t *testing.T) {
	var df *DataFrame = nil

	// GroupBy on nil DataFrame
	gb := df.GroupBy("key")
	if gb != nil {
		t.Error("Expected nil for GroupBy on nil DataFrame")
	}
}

func TestArrowGroupByNoColumns(t *testing.T) {
	keys := NewSeriesI64("key", []int64{1, 2, 3})
	values := NewSeriesF64("value", []float64{1.0, 2.0, 3.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy with no columns
	gb := df.GroupBy()
	if gb != nil {
		t.Error("Expected nil for GroupBy with no columns")
	}
}

func TestArrowGroupByResultColumns(t *testing.T) {
	// Verify column names are correct
	keys := NewSeriesI64("dept_id", []int64{1, 1, 2})
	values := NewSeriesF64("salary", []float64{1000.0, 2000.0, 3000.0})

	df := NewDataFrame()
	df = df.WithColumn(keys)
	df = df.WithColumn(values)

	// GroupBy sum
	result := df.GroupBy("dept_id").Sum("salary")
	if result == nil {
		t.Fatal("GroupBy Sum returned nil")
	}

	// Check column names
	colNames := result.ColumnNames()
	sort.Strings(colNames) // Sort for deterministic comparison

	expected := []string{"dept_id", "salary_sum"}
	sort.Strings(expected)

	if len(colNames) != len(expected) {
		t.Errorf("Expected %d columns, got %d", len(expected), len(colNames))
	}

	for i, name := range expected {
		if colNames[i] != name {
			t.Errorf("Expected column %s, got %s", name, colNames[i])
		}
	}
}
