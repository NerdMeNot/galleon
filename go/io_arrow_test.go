package galleon

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestArrow_ExportNumeric(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("f64", []float64{1.0, 2.0, 3.0}),
		NewSeriesInt64("i64", []int64{10, 20, 30}),
		NewSeriesFloat32("f32", []float32{0.1, 0.2, 0.3}),
		NewSeriesInt32("i32", []int32{100, 200, 300}),
	)

	record, err := df.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	// Verify schema
	schema := record.Schema()
	if schema.NumFields() != 4 {
		t.Errorf("Expected 4 fields, got %d", schema.NumFields())
	}

	// Verify data
	if record.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", record.NumRows())
	}
}

func TestArrow_ExportString(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("name", []string{"Alice", "Bob", "Carol"}),
	)

	record, err := df.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	if record.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", record.NumCols())
	}
	if record.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", record.NumRows())
	}
}

func TestArrow_ExportCategorical(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesCategorical("fruit", []string{"apple", "banana", "apple", "cherry"}),
	)

	record, err := df.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	// Should be exported as dictionary-encoded array
	schema := record.Schema()
	field := schema.Field(0)

	// Verify it's a dictionary type
	if field.Type.ID().String() != "DICTIONARY" {
		t.Errorf("Expected DICTIONARY type, got %s", field.Type.ID())
	}
}

func TestArrow_Roundtrip_Numeric(t *testing.T) {
	// Create original DataFrame
	original, _ := NewDataFrame(
		NewSeriesFloat64("f64", []float64{1.1, 2.2, 3.3}),
		NewSeriesInt64("i64", []int64{100, 200, 300}),
	)

	// Export to Arrow
	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	// Import back
	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	// Verify dimensions
	if imported.Height() != original.Height() {
		t.Errorf("Height: expected %d, got %d", original.Height(), imported.Height())
	}
	if imported.Width() != original.Width() {
		t.Errorf("Width: expected %d, got %d", original.Width(), imported.Width())
	}

	// Verify data
	origF64 := original.ColumnByName("f64").Float64()
	impF64 := imported.ColumnByName("f64").Float64()
	for i := range origF64 {
		if origF64[i] != impF64[i] {
			t.Errorf("f64[%d]: expected %f, got %f", i, origF64[i], impF64[i])
		}
	}

	origI64 := original.ColumnByName("i64").Int64()
	impI64 := imported.ColumnByName("i64").Int64()
	for i := range origI64 {
		if origI64[i] != impI64[i] {
			t.Errorf("i64[%d]: expected %d, got %d", i, origI64[i], impI64[i])
		}
	}
}

func TestArrow_Roundtrip_String(t *testing.T) {
	original, _ := NewDataFrame(
		NewSeriesString("name", []string{"Alice", "Bob", "Carol"}),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	origStrs := original.ColumnByName("name").Strings()
	impStrs := imported.ColumnByName("name").Strings()
	for i := range origStrs {
		if origStrs[i] != impStrs[i] {
			t.Errorf("name[%d]: expected %s, got %s", i, origStrs[i], impStrs[i])
		}
	}
}

func TestArrow_Roundtrip_Bool(t *testing.T) {
	original, _ := NewDataFrame(
		NewSeriesBool("flag", []bool{true, false, true, false}),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	origBools := original.ColumnByName("flag").Bool()
	impBools := imported.ColumnByName("flag").Bool()
	for i := range origBools {
		if origBools[i] != impBools[i] {
			t.Errorf("flag[%d]: expected %v, got %v", i, origBools[i], impBools[i])
		}
	}
}

func TestArrow_Roundtrip_Categorical(t *testing.T) {
	original, _ := NewDataFrame(
		NewSeriesCategorical("fruit", []string{"apple", "banana", "apple", "cherry", "banana"}),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	// Imported should be categorical
	impCol := imported.ColumnByName("fruit")
	if impCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype, got %s", impCol.DType())
	}

	// Verify values match (as strings)
	for i := 0; i < original.Height(); i++ {
		origVal := original.ColumnByName("fruit").Get(i)
		impVal := impCol.Get(i)
		if origVal != impVal {
			t.Errorf("fruit[%d]: expected %v, got %v", i, origVal, impVal)
		}
	}
}

// ============================================================================
// Arrow Correctness Tests
// ============================================================================

func TestArrow_Correctness_Float64Values(t *testing.T) {
	// Test with specific values including edge cases
	values := []float64{0.0, -1.5, 1.5, 3.14159, -999.999, 1e10, -1e-10}
	original, _ := NewDataFrame(
		NewSeriesFloat64("f64", values),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	impValues := imported.ColumnByName("f64").Float64()
	for i, expected := range values {
		if impValues[i] != expected {
			t.Errorf("f64[%d]: expected %v, got %v", i, expected, impValues[i])
		}
	}
}

func TestArrow_Correctness_Int64Values(t *testing.T) {
	// Test with specific values including edge cases
	values := []int64{0, -1, 1, 9223372036854775807, -9223372036854775808, 12345678901234}
	original, _ := NewDataFrame(
		NewSeriesInt64("i64", values),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	impValues := imported.ColumnByName("i64").Int64()
	for i, expected := range values {
		if impValues[i] != expected {
			t.Errorf("i64[%d]: expected %v, got %v", i, expected, impValues[i])
		}
	}
}

func TestArrow_Correctness_StringValues(t *testing.T) {
	// Test with various string values
	values := []string{"", "hello", "world", "with spaces", "unicode: 日本語", "emoji: 🎉"}
	original, _ := NewDataFrame(
		NewSeriesString("str", values),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	impValues := imported.ColumnByName("str").Strings()
	for i, expected := range values {
		if impValues[i] != expected {
			t.Errorf("str[%d]: expected %q, got %q", i, expected, impValues[i])
		}
	}
}

func TestArrow_Correctness_BoolValues(t *testing.T) {
	values := []bool{true, false, true, true, false, false, true}
	original, _ := NewDataFrame(
		NewSeriesBool("bool", values),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	impValues := imported.ColumnByName("bool").Bool()
	for i, expected := range values {
		if impValues[i] != expected {
			t.Errorf("bool[%d]: expected %v, got %v", i, expected, impValues[i])
		}
	}
}

func TestArrow_Correctness_MixedTypes(t *testing.T) {
	original, _ := NewDataFrame(
		NewSeriesFloat64("f64", []float64{1.1, 2.2, 3.3}),
		NewSeriesInt64("i64", []int64{100, 200, 300}),
		NewSeriesString("str", []string{"a", "b", "c"}),
		NewSeriesBool("bool", []bool{true, false, true}),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	// Verify each column type is preserved
	if imported.ColumnByName("f64").DType() != Float64 {
		t.Errorf("f64 dtype: expected Float64, got %s", imported.ColumnByName("f64").DType())
	}
	if imported.ColumnByName("i64").DType() != Int64 {
		t.Errorf("i64 dtype: expected Int64, got %s", imported.ColumnByName("i64").DType())
	}
	if imported.ColumnByName("str").DType() != String {
		t.Errorf("str dtype: expected String, got %s", imported.ColumnByName("str").DType())
	}
	if imported.ColumnByName("bool").DType() != Bool {
		t.Errorf("bool dtype: expected Bool, got %s", imported.ColumnByName("bool").DType())
	}

	// Verify values
	f64 := imported.ColumnByName("f64").Float64()
	if f64[0] != 1.1 || f64[1] != 2.2 || f64[2] != 3.3 {
		t.Errorf("f64 values mismatch")
	}

	i64 := imported.ColumnByName("i64").Int64()
	if i64[0] != 100 || i64[1] != 200 || i64[2] != 300 {
		t.Errorf("i64 values mismatch")
	}

	str := imported.ColumnByName("str").Strings()
	if str[0] != "a" || str[1] != "b" || str[2] != "c" {
		t.Errorf("str values mismatch")
	}

	bools := imported.ColumnByName("bool").Bool()
	if bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("bool values mismatch")
	}
}

func TestArrow_Correctness_LargeDataset(t *testing.T) {
	n := 10000
	floats := make([]float64, n)
	ints := make([]int64, n)
	for i := 0; i < n; i++ {
		floats[i] = float64(i) * 0.123
		ints[i] = int64(i * 7)
	}

	original, _ := NewDataFrame(
		NewSeriesFloat64("f64", floats),
		NewSeriesInt64("i64", ints),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	// Verify dimensions
	if imported.Height() != n {
		t.Errorf("Height: expected %d, got %d", n, imported.Height())
	}

	// Spot check values
	impFloats := imported.ColumnByName("f64").Float64()
	impInts := imported.ColumnByName("i64").Int64()

	for i := 0; i < n; i++ {
		if impFloats[i] != floats[i] {
			t.Errorf("f64[%d]: expected %v, got %v", i, floats[i], impFloats[i])
			break
		}
		if impInts[i] != ints[i] {
			t.Errorf("i64[%d]: expected %v, got %v", i, ints[i], impInts[i])
			break
		}
	}
}

func TestArrow_Correctness_ColumnOrder(t *testing.T) {
	// Verify column order is preserved
	original, _ := NewDataFrame(
		NewSeriesFloat64("first", []float64{1.0}),
		NewSeriesInt64("second", []int64{2}),
		NewSeriesString("third", []string{"three"}),
		NewSeriesBool("fourth", []bool{true}),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	expectedOrder := []string{"first", "second", "third", "fourth"}
	actualOrder := imported.Columns()

	for i, expected := range expectedOrder {
		if actualOrder[i] != expected {
			t.Errorf("Column order[%d]: expected %s, got %s", i, expected, actualOrder[i])
		}
	}
}

func TestArrow_Correctness_CategoricalRoundtrip(t *testing.T) {
	// Test categorical with various values
	values := []string{"red", "green", "blue", "red", "green", "red", "blue", "blue"}
	original, _ := NewDataFrame(
		NewSeriesCategorical("color", values),
	)

	record, err := original.ToArrow(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrow failed: %v", err)
	}
	defer record.Release()

	imported, err := NewDataFrameFromArrow(record)
	if err != nil {
		t.Fatalf("NewDataFrameFromArrow failed: %v", err)
	}

	// Verify type is categorical
	if imported.ColumnByName("color").DType() != Categorical {
		t.Errorf("Expected Categorical dtype, got %s", imported.ColumnByName("color").DType())
	}

	// Verify all values match
	for i, expected := range values {
		got := imported.ColumnByName("color").Get(i).(string)
		if got != expected {
			t.Errorf("color[%d]: expected %s, got %s", i, expected, got)
		}
	}
}
