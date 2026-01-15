package galleon

import (
	"testing"
)

func TestInnerJoin(t *testing.T) {
	// Left DataFrame: customers
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3, 4}),
		NewSeriesString("name", []string{"Alice", "Bob", "Carol", "Dave"}),
	)

	// Right DataFrame: orders
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 2, 5}),
		NewSeriesFloat64("amount", []float64{100, 200, 150, 300}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Should have 3 rows (id 1, 2, 2)
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}

	// Should have 3 columns: id, name, amount
	if result.Width() != 3 {
		t.Errorf("expected 3 columns, got %d", result.Width())
	}

	// Verify columns exist
	if result.ColumnByName("id") == nil {
		t.Error("missing 'id' column")
	}
	if result.ColumnByName("name") == nil {
		t.Error("missing 'name' column")
	}
	if result.ColumnByName("amount") == nil {
		t.Error("missing 'amount' column")
	}
}

func TestLeftJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3, 4}),
		NewSeriesString("name", []string{"Alice", "Bob", "Carol", "Dave"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 5}),
		NewSeriesFloat64("amount", []float64{100, 200, 300}),
	)

	result, err := left.LeftJoin(right, On("id"))
	if err != nil {
		t.Fatalf("failed to left join: %v", err)
	}

	// Should have 4 rows (all left rows preserved)
	if result.Height() != 4 {
		t.Errorf("expected 4 rows, got %d", result.Height())
	}

	// Check that Carol (id=3) and Dave (id=4) have 0 amount (null)
	idCol := result.ColumnByName("id")
	amountCol := result.ColumnByName("amount")

	for i := 0; i < result.Height(); i++ {
		id, _ := idCol.GetInt64(i)
		amount, _ := amountCol.GetFloat64(i)

		if id == 3 || id == 4 {
			if amount != 0 {
				t.Errorf("expected null (0) amount for id %d, got %f", id, amount)
			}
		}
	}
}

func TestRightJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("name", []string{"Alice", "Bob"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesFloat64("amount", []float64{100, 200, 300}),
	)

	result, err := left.RightJoin(right, On("id"))
	if err != nil {
		t.Fatalf("failed to right join: %v", err)
	}

	// Should have 3 rows (all right rows preserved)
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestOuterJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"Alice", "Bob", "Carol"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{2, 3, 4}),
		NewSeriesFloat64("amount", []float64{200, 300, 400}),
	)

	result, err := left.OuterJoin(right, On("id"))
	if err != nil {
		t.Fatalf("failed to outer join: %v", err)
	}

	// Should have 4 rows: id 1 (left only), 2 (both), 3 (both), 4 (right only)
	if result.Height() != 4 {
		t.Errorf("expected 4 rows, got %d", result.Height())
	}
}

func TestCrossJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("color", []string{"red", "blue"}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("size", []string{"S", "M", "L"}),
	)

	result, err := left.CrossJoin(right)
	if err != nil {
		t.Fatalf("failed to cross join: %v", err)
	}

	// Should have 2 * 3 = 6 rows
	if result.Height() != 6 {
		t.Errorf("expected 6 rows, got %d", result.Height())
	}

	// Should have 2 columns
	if result.Width() != 2 {
		t.Errorf("expected 2 columns, got %d", result.Width())
	}
}

func TestJoinDifferentColumnNames(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("user_id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"Alice", "Bob", "Carol"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("customer_id", []int64{1, 2, 4}),
		NewSeriesFloat64("balance", []float64{100, 200, 400}),
	)

	result, err := left.Join(right, LeftOn("user_id").RightOn("customer_id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Should have 2 rows (ids 1 and 2 match)
	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}

	// Both user_id and customer_id should be in result
	if result.ColumnByName("user_id") == nil {
		t.Error("missing 'user_id' column")
	}
	if result.ColumnByName("customer_id") == nil {
		t.Error("missing 'customer_id' column")
	}
}

func TestJoinColumnNameCollision(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("value", []string{"left1", "left2"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("value", []string{"right1", "right2"}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Should have value and value_right columns
	if result.ColumnByName("value") == nil {
		t.Error("missing 'value' column")
	}
	if result.ColumnByName("value_right") == nil {
		t.Error("missing 'value_right' column")
	}
}

func TestJoinCustomSuffix(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("data", []string{"a", "b"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("data", []string{"x", "y"}),
	)

	result, err := left.Join(right, On("id").WithSuffix("_r"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if result.ColumnByName("data_r") == nil {
		t.Error("missing 'data_r' column with custom suffix")
	}
}

func TestJoinMultipleKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("year", []int64{2020, 2020, 2021, 2021}),
		NewSeriesString("region", []string{"east", "west", "east", "west"}),
		NewSeriesFloat64("sales", []float64{100, 200, 150, 250}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("year", []int64{2020, 2021}),
		NewSeriesString("region", []string{"east", "west"}),
		NewSeriesFloat64("target", []float64{120, 300}),
	)

	result, err := left.Join(right, On("year", "region"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Should have 2 rows: (2020, east) and (2021, west)
	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

func TestJoinEmptyDataFrame(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{}),
		NewSeriesFloat64("value", []float64{}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join with empty DataFrame: %v", err)
	}

	// Inner join with empty right should produce 0 rows
	if result.Height() != 0 {
		t.Errorf("expected 0 rows, got %d", result.Height())
	}
}

func TestLeftJoinEmptyRight(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{}),
		NewSeriesFloat64("value", []float64{}),
	)

	result, err := left.LeftJoin(right, On("id"))
	if err != nil {
		t.Fatalf("failed to left join: %v", err)
	}

	// Left join should preserve all left rows
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestJoinInvalidColumn(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	_, err := left.Join(right, On("nonexistent"))
	if err == nil {
		t.Error("expected error for nonexistent column")
	}
}

func TestJoinPreservesTypes(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesFloat64("float_col", []float64{1.5, 2.5}),
		NewSeriesInt32("int32_col", []int32{10, 20}),
		NewSeriesBool("bool_col", []bool{true, false}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("str_col", []string{"a", "b"}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Verify types are preserved
	if result.ColumnByName("float_col").DType() != Float64 {
		t.Error("float_col should be Float64")
	}
	if result.ColumnByName("int32_col").DType() != Int32 {
		t.Error("int32_col should be Int32")
	}
	if result.ColumnByName("bool_col").DType() != Bool {
		t.Error("bool_col should be Bool")
	}
	if result.ColumnByName("str_col").DType() != String {
		t.Error("str_col should be String")
	}
}

func TestJoinDuplicateMatches(t *testing.T) {
	// Many-to-many join
	left, _ := NewDataFrame(
		NewSeriesInt64("key", []int64{1, 1, 2}),
		NewSeriesString("left_val", []string{"a", "b", "c"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("key", []int64{1, 1, 2}),
		NewSeriesString("right_val", []string{"x", "y", "z"}),
	)

	result, err := left.Join(right, On("key"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// 2 left rows with key=1 * 2 right rows with key=1 = 4 rows
	// 1 left row with key=2 * 1 right row with key=2 = 1 row
	// Total = 5 rows
	if result.Height() != 5 {
		t.Errorf("expected 5 rows for many-to-many join, got %d", result.Height())
	}
}

func TestJoinStringKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "UK", "DE"}),
		NewSeriesString("country", []string{"United States", "United Kingdom", "Germany"}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "UK", "FR"}),
		NewSeriesInt64("population", []int64{330, 67, 65}),
	)

	result, err := left.Join(right, On("code"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Should have 2 rows (US and UK match)
	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

// Additional tests for coverage

func TestJoinFloat32Keys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesFloat32("key", []float32{1.0, 2.0, 3.0}),
		NewSeriesString("name", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesFloat32("key", []float32{1.0, 2.0, 4.0}),
		NewSeriesInt64("value", []int64{100, 200, 400}),
	)

	result, err := left.Join(right, On("key"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

func TestJoinFloat64Keys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesFloat64("key", []float64{1.0, 2.0, 3.0}),
		NewSeriesString("name", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesFloat64("key", []float64{1.0, 2.0, 4.0}),
		NewSeriesInt64("value", []int64{100, 200, 400}),
	)

	result, err := left.Join(right, On("key"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

func TestJoinInt32Keys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt32("key", []int32{1, 2, 3}),
		NewSeriesString("name", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt32("key", []int32{1, 2, 4}),
		NewSeriesInt64("value", []int64{100, 200, 400}),
	)

	result, err := left.Join(right, On("key"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

func TestJoinBoolKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesBool("active", []bool{true, false, true}),
		NewSeriesString("name", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesBool("active", []bool{true, false}),
		NewSeriesInt64("count", []int64{100, 200}),
	)

	result, err := left.Join(right, On("active"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// true matches 2 left rows, false matches 1 = 3 rows
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestLeftJoinStringKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "UK", "DE"}),
		NewSeriesString("country", []string{"United States", "United Kingdom", "Germany"}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "FR"}),
		NewSeriesInt64("population", []int64{330, 65}),
	)

	result, err := left.LeftJoin(right, On("code"))
	if err != nil {
		t.Fatalf("failed to left join: %v", err)
	}

	// All 3 left rows preserved
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestRightJoinStringKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "UK"}),
		NewSeriesString("country", []string{"United States", "United Kingdom"}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "UK", "FR"}),
		NewSeriesInt64("population", []int64{330, 67, 65}),
	)

	result, err := left.RightJoin(right, On("code"))
	if err != nil {
		t.Fatalf("failed to right join: %v", err)
	}

	// All 3 right rows preserved
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestOuterJoinStringKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("code", []string{"US", "UK"}),
		NewSeriesString("name", []string{"A", "B"}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("code", []string{"UK", "FR"}),
		NewSeriesInt64("value", []int64{1, 2}),
	)

	result, err := left.OuterJoin(right, On("code"))
	if err != nil {
		t.Fatalf("failed to outer join: %v", err)
	}

	// US (left only), UK (both), FR (right only) = 3 rows
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}
}

func TestJoinMissingLeftColumn(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	_, err := left.Join(right, LeftOn("nonexistent").RightOn("id"))
	if err == nil {
		t.Error("expected error for nonexistent left column")
	}
}

func TestJoinMissingRightColumn(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	_, err := left.Join(right, LeftOn("id").RightOn("nonexistent"))
	if err == nil {
		t.Error("expected error for nonexistent right column")
	}
}

func TestJoinMismatchedKeyLengths(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesInt64("key2", []int64{1, 2}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	_, err := left.Join(right, LeftOn("id", "key2").RightOn("id"))
	if err == nil {
		t.Error("expected error for mismatched key lengths")
	}
}

func TestJoinNoJoinSpec(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
	)

	_, err := left.Join(right, JoinOptions{suffix: "_right"})
	if err == nil {
		t.Error("expected error when no join columns specified")
	}
}

func TestCrossJoinEmptyLeft(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("a", []string{}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("b", []string{"x", "y"}),
	)

	result, err := left.CrossJoin(right)
	if err != nil {
		t.Fatalf("failed to cross join: %v", err)
	}

	if result.Height() != 0 {
		t.Errorf("expected 0 rows, got %d", result.Height())
	}
}

func TestCrossJoinColumnCollision(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesString("name", []string{"A", "B"}),
	)

	right, _ := NewDataFrame(
		NewSeriesString("name", []string{"X", "Y"}),
	)

	result, err := left.CrossJoin(right)
	if err != nil {
		t.Fatalf("failed to cross join: %v", err)
	}

	if result.ColumnByName("name") == nil {
		t.Error("missing 'name' column")
	}
	if result.ColumnByName("name_right") == nil {
		t.Error("missing 'name_right' column")
	}
}

func TestJoinFloat32Values(t *testing.T) {
	// Test that Float32 values are correctly gathered
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesFloat32("val", []float32{1.5, 2.5}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesFloat32("other", []float32{10.5, 20.5}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Verify Float32 values preserved
	valCol := result.ColumnByName("val")
	if valCol.DType() != Float32 {
		t.Error("val should remain Float32")
	}

	otherCol := result.ColumnByName("other")
	if otherCol.DType() != Float32 {
		t.Error("other should remain Float32")
	}
}

func TestJoinInt32Values(t *testing.T) {
	// Test that Int32 values are correctly gathered
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesInt32("val", []int32{10, 20}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesInt32("other", []int32{100, 200}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Verify Int32 values preserved
	valCol := result.ColumnByName("val")
	if valCol.DType() != Int32 {
		t.Error("val should remain Int32")
	}

	otherCol := result.ColumnByName("other")
	if otherCol.DType() != Int32 {
		t.Error("other should remain Int32")
	}
}

func TestLeftJoinAllMatches(t *testing.T) {
	// When all left rows match, no nulls needed
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("name", []string{"A", "B"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesFloat64("value", []float64{100, 200}),
	)

	result, err := left.LeftJoin(right, On("id"))
	if err != nil {
		t.Fatalf("failed to left join: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}

	// All values should be non-zero
	valCol := result.ColumnByName("value")
	for i := 0; i < result.Height(); i++ {
		v, _ := valCol.GetFloat64(i)
		if v == 0 {
			t.Errorf("expected non-zero value at row %d", i)
		}
	}
}

func TestJoinLeftOnRightOn(t *testing.T) {
	// Test using both LeftOn and RightOn with different column names
	left, _ := NewDataFrame(
		NewSeriesInt64("left_key", []int64{1, 2, 3}),
		NewSeriesString("left_val", []string{"A", "B", "C"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("right_key", []int64{2, 3, 4}),
		NewSeriesString("right_val", []string{"X", "Y", "Z"}),
	)

	result, err := left.Join(right, LeftOn("left_key").RightOn("right_key"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Should have 2 matches (2, 3)
	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}

	// Both key columns should be present
	if result.ColumnByName("left_key") == nil {
		t.Error("missing 'left_key' column")
	}
	if result.ColumnByName("right_key") == nil {
		t.Error("missing 'right_key' column")
	}
}

func TestJoinEmptyLeftAndRight(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{}),
		NewSeriesString("name", []string{}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{}),
		NewSeriesFloat64("value", []float64{}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if result.Height() != 0 {
		t.Errorf("expected 0 rows, got %d", result.Height())
	}
}

func TestDefaultJoinOptions(t *testing.T) {
	opts := DefaultJoinOptions()
	if opts.suffix != "_right" {
		t.Errorf("expected suffix '_right', got '%s'", opts.suffix)
	}
	if opts.how != InnerJoin {
		t.Errorf("expected InnerJoin, got %d", opts.how)
	}
}

func BenchmarkInnerJoin(b *testing.B) {
	// Create DataFrames with 10k rows
	n := 10000
	leftIds := make([]int64, n)
	leftVals := make([]float64, n)
	rightIds := make([]int64, n)
	rightVals := make([]float64, n)

	for i := 0; i < n; i++ {
		leftIds[i] = int64(i % 1000) // 1000 unique keys
		leftVals[i] = float64(i)
		rightIds[i] = int64(i % 1000)
		rightVals[i] = float64(i * 2)
	}

	left, _ := NewDataFrame(
		NewSeriesInt64("id", leftIds),
		NewSeriesFloat64("left_val", leftVals),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", rightIds),
		NewSeriesFloat64("right_val", rightVals),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.Join(right, On("id"))
	}
}

func BenchmarkLeftJoin(b *testing.B) {
	n := 10000
	leftIds := make([]int64, n)
	rightIds := make([]int64, n/2)

	for i := 0; i < n; i++ {
		leftIds[i] = int64(i)
	}
	for i := 0; i < n/2; i++ {
		rightIds[i] = int64(i * 2) // Only even numbers
	}

	left, _ := NewDataFrame(
		NewSeriesInt64("id", leftIds),
		NewSeriesFloat64("val", make([]float64, n)),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", rightIds),
		NewSeriesFloat64("val", make([]float64, n/2)),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		left.LeftJoin(right, On("id"))
	}
}

// ============================================================================
// Categorical Join Tests
// ============================================================================

func TestJoinCategoricalKeys(t *testing.T) {
	// Test join with categorical key columns
	left, _ := NewDataFrame(
		NewSeriesCategorical("category", []string{"A", "B", "C", "A"}),
		NewSeriesInt64("left_val", []int64{1, 2, 3, 4}),
	)

	right, _ := NewDataFrame(
		NewSeriesCategorical("category", []string{"A", "B", "D"}),
		NewSeriesFloat64("right_val", []float64{10, 20, 30}),
	)

	result, err := left.Join(right, On("category"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Inner join: A matches (twice from left), B matches once
	// Expected: 3 rows (A-10, A-10, B-20)
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}

	// Verify the category column is categorical
	catCol := result.ColumnByName("category")
	if catCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype, got %s", catCol.DType())
	}
}

func TestLeftJoinCategoricalKeys(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesCategorical("fruit", []string{"apple", "banana", "cherry"}),
		NewSeriesInt64("qty", []int64{10, 20, 30}),
	)

	right, _ := NewDataFrame(
		NewSeriesCategorical("fruit", []string{"apple", "cherry"}),
		NewSeriesFloat64("price", []float64{1.5, 2.5}),
	)

	result, err := left.LeftJoin(right, On("fruit"))
	if err != nil {
		t.Fatalf("failed to left join: %v", err)
	}

	// Left join: all left rows preserved (3 rows)
	if result.Height() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Height())
	}

	// Categorical key should be preserved
	fruitCol := result.ColumnByName("fruit")
	if fruitCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype, got %s", fruitCol.DType())
	}
}

func TestJoinCategoricalValue(t *testing.T) {
	// Test that categorical value columns are correctly transferred in join
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesCategorical("status", []string{"active", "inactive", "active"}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesFloat64("score", []float64{100, 200}),
	)

	result, err := left.Join(right, On("id"))
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	// Check that status column (categorical) is preserved
	statusCol := result.ColumnByName("status")
	if statusCol == nil {
		t.Fatal("status column missing from result")
	}
	if statusCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype for status, got %s", statusCol.DType())
	}

	// Verify values are correct
	if statusCol.Get(0) != "active" {
		t.Errorf("Expected 'active' for first row, got '%v'", statusCol.Get(0))
	}
}
