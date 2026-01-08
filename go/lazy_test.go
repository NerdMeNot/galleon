package galleon

import (
	"testing"
)

func TestLazyFrameSelect(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
		NewSeriesFloat64("b", []float64{1.1, 2.2, 3.3}),
		NewSeriesString("c", []string{"x", "y", "z"}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Test lazy select
	result, err := df.Lazy().
		Select(Col("a"), Col("b")).
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Width() != 2 {
		t.Errorf("expected 2 columns, got %d", result.Width())
	}
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestLazyFrameFilter(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3, 4, 5}),
		NewSeriesFloat64("b", []float64{1.1, 2.2, 3.3, 4.4, 5.5}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Test lazy filter: a > 2
	result, err := df.Lazy().
		Filter(Col("a").Gt(Lit(2))).
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 3 {
		t.Errorf("expected 3 rows (a > 2), got %d", result.Height())
	}

	// Verify first row has a=3
	col := result.ColumnByName("a")
	if col == nil {
		t.Fatal("column 'a' not found")
	}
	if v, _ := col.GetInt64(0); v != 3 {
		t.Errorf("expected first value to be 3, got %d", v)
	}
}

func TestLazyFrameWithColumn(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Test lazy with_column: b = a * 2
	result, err := df.Lazy().
		WithColumn("b", Col("a").Mul(Lit(2))).
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Width() != 2 {
		t.Errorf("expected 2 columns, got %d", result.Width())
	}

	col := result.ColumnByName("b")
	if col == nil {
		t.Fatal("column 'b' not found")
	}

	// The scalar multiplication returns a series of the same type as input (int64)
	// or float64 if the scalar was float
	expected := []int64{2, 4, 6}
	for i, exp := range expected {
		val := col.Get(i)
		// Could be int64 or float64 depending on implementation
		switch v := val.(type) {
		case int64:
			if v != exp {
				t.Errorf("row %d: expected %d, got %d", i, exp, v)
			}
		case float64:
			if v != float64(exp) {
				t.Errorf("row %d: expected %f, got %f", i, float64(exp), v)
			}
		default:
			t.Errorf("row %d: unexpected type %T", i, val)
		}
	}
}

func TestLazyFrameGroupBy(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesString("key", []string{"a", "a", "b", "b", "b"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Test lazy groupby sum
	result, err := df.Lazy().
		GroupBy("key").
		Agg(Col("value").Sum().Alias("sum")).
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 groups, got %d", result.Height())
	}

	// Check that sum column exists
	sumCol := result.ColumnByName("sum")
	if sumCol == nil {
		t.Fatal("column 'sum' not found")
	}
}

func TestLazyFrameJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("value", []float64{1.1, 2.2, 4.4}),
	)

	// Test lazy join
	result, err := left.Lazy().
		Join(right.Lazy(), On("id")).
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Should have 2 matching rows (id=1 and id=2)
	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

func TestLazyFrameChaining(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3, 4, 5}),
		NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0, 40.0, 50.0}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Test chaining: filter -> with_column -> select
	result, err := df.Lazy().
		Filter(Col("a").Gt(Lit(2))).          // a > 2 -> rows 3,4,5
		WithColumn("c", Col("b").Mul(Lit(2))). // c = b * 2
		Select(Col("a"), Col("c")).            // select only a and c
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
	if result.Width() != 2 {
		t.Errorf("expected 2 columns, got %d", result.Width())
	}

	// Verify column c exists and has correct values
	cCol := result.ColumnByName("c")
	if cCol == nil {
		t.Fatal("column 'c' not found")
	}

	// c should be [60, 80, 100] (b values 30,40,50 * 2)
	expected := []float64{60.0, 80.0, 100.0}
	for i, exp := range expected {
		val := cCol.Get(i)
		switch v := val.(type) {
		case float64:
			if v != exp {
				t.Errorf("row %d: expected %f, got %f", i, exp, v)
			}
		default:
			t.Errorf("row %d: expected float64, got %T = %v", i, val, val)
		}
	}
}

func TestLazyFrameExplain(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
	)

	lf := df.Lazy().
		Filter(Col("a").Gt(Lit(1))).
		Select(Col("a"))

	// Just ensure it doesn't panic
	desc := lf.Describe()
	if desc == "" {
		t.Error("Describe returned empty string")
	}

	explained := lf.Explain()
	if explained == "" {
		t.Error("Explain returned empty string")
	}
}

func TestLazyFrameSortAndLimit(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{3, 1, 4, 1, 5}),
		NewSeriesString("b", []string{"c", "a", "d", "b", "e"}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Test sort and limit
	result, err := df.Lazy().
		Sort("a", true). // Sort ascending by a
		Head(3).         // Take first 3
		Collect()

	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}

	// First row should have a=1
	col := result.ColumnByName("a")
	if v, _ := col.GetInt64(0); v != 1 {
		t.Errorf("expected first value to be 1, got %d", v)
	}
}
