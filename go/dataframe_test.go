package galleon

import (
	"testing"
)

func TestDataFrameCreate(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0})

	df, err := NewDataFrame(col1, col2)
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}

	if df.Height() != 3 {
		t.Errorf("Height() = %d, want 3", df.Height())
	}

	if df.Width() != 2 {
		t.Errorf("Width() = %d, want 2", df.Width())
	}

	h, w := df.Shape()
	if h != 3 || w != 2 {
		t.Errorf("Shape() = (%d, %d), want (3, 2)", h, w)
	}
}

func TestDataFrameColumnAccess(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0})

	df, _ := NewDataFrame(col1, col2)

	// By index
	c := df.Column(0)
	if c == nil || c.Name() != "a" {
		t.Error("Column(0) should return column 'a'")
	}

	c = df.Column(1)
	if c == nil || c.Name() != "b" {
		t.Error("Column(1) should return column 'b'")
	}

	c = df.Column(5)
	if c != nil {
		t.Error("Column(5) should return nil")
	}

	// By name
	c = df.ColumnByName("a")
	if c == nil || c.Name() != "a" {
		t.Error("ColumnByName('a') should return column 'a'")
	}

	c = df.ColumnByName("nonexistent")
	if c != nil {
		t.Error("ColumnByName('nonexistent') should return nil")
	}
}

func TestDataFrameSelect(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0})
	col3 := NewSeriesFloat64("c", []float64{7.0, 8.0, 9.0})

	df, _ := NewDataFrame(col1, col2, col3)

	// Select single column
	df2, err := df.Select("b")
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if df2.Width() != 1 {
		t.Errorf("Select('b') Width = %d, want 1", df2.Width())
	}

	// Select multiple columns
	df3, err := df.Select("c", "a")
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	if df3.Width() != 2 {
		t.Errorf("Select('c', 'a') Width = %d, want 2", df3.Width())
	}

	// Verify order
	cols := df3.Columns()
	if cols[0] != "c" || cols[1] != "a" {
		t.Errorf("Select columns = %v, want [c, a]", cols)
	}

	// Select nonexistent column
	_, err = df.Select("nonexistent")
	if err == nil {
		t.Error("Select('nonexistent') should fail")
	}
}

func TestDataFrameDrop(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0})
	col3 := NewSeriesFloat64("c", []float64{7.0, 8.0, 9.0})

	df, _ := NewDataFrame(col1, col2, col3)

	df2, err := df.Drop("b")
	if err != nil {
		t.Fatalf("Drop failed: %v", err)
	}
	if df2.Width() != 2 {
		t.Errorf("Drop('b') Width = %d, want 2", df2.Width())
	}

	cols := df2.Columns()
	if cols[0] != "a" || cols[1] != "c" {
		t.Errorf("Drop columns = %v, want [a, c]", cols)
	}
}

func TestDataFrameHeadTail(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})

	df, _ := NewDataFrame(col1)

	// Head
	df2 := df.Head(3)
	if df2.Height() != 3 {
		t.Errorf("Head(3) Height = %d, want 3", df2.Height())
	}
	data := df2.Column(0).Float64()
	if data[0] != 1.0 || data[2] != 3.0 {
		t.Errorf("Head(3) data = %v, want [1, 2, 3]", data)
	}

	// Tail
	df3 := df.Tail(2)
	if df3.Height() != 2 {
		t.Errorf("Tail(2) Height = %d, want 2", df3.Height())
	}
	data = df3.Column(0).Float64()
	if data[0] != 4.0 || data[1] != 5.0 {
		t.Errorf("Tail(2) data = %v, want [4, 5]", data)
	}
}

func TestDataFrameFilter(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	col2 := NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0, 40.0, 50.0})

	df, _ := NewDataFrame(col1, col2)

	// Filter by mask
	mask := []byte{1, 0, 1, 0, 1} // select rows 0, 2, 4
	df2, err := df.FilterByMask(mask)
	if err != nil {
		t.Fatalf("FilterByMask failed: %v", err)
	}
	if df2.Height() != 3 {
		t.Errorf("FilterByMask Height = %d, want 3", df2.Height())
	}

	dataA := df2.ColumnByName("a").Float64()
	if dataA[0] != 1.0 || dataA[1] != 3.0 || dataA[2] != 5.0 {
		t.Errorf("FilterByMask a = %v, want [1, 3, 5]", dataA)
	}

	dataB := df2.ColumnByName("b").Float64()
	if dataB[0] != 10.0 || dataB[1] != 30.0 || dataB[2] != 50.0 {
		t.Errorf("FilterByMask b = %v, want [10, 30, 50]", dataB)
	}
}

func TestDataFrameFilterByIndices(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})

	df, _ := NewDataFrame(col1)

	indices := []uint32{4, 2, 0}
	df2, err := df.FilterByIndices(indices)
	if err != nil {
		t.Fatalf("FilterByIndices failed: %v", err)
	}
	if df2.Height() != 3 {
		t.Errorf("FilterByIndices Height = %d, want 3", df2.Height())
	}

	data := df2.Column(0).Float64()
	if data[0] != 5.0 || data[1] != 3.0 || data[2] != 1.0 {
		t.Errorf("FilterByIndices data = %v, want [5, 3, 1]", data)
	}
}

func TestDataFrameSortBy(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{3.0, 1.0, 4.0, 1.0, 5.0})
	col2 := NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0, 40.0, 50.0})

	df, _ := NewDataFrame(col1, col2)

	// Sort ascending
	df2, err := df.SortBy("a", true)
	if err != nil {
		t.Fatalf("SortBy failed: %v", err)
	}

	dataA := df2.ColumnByName("a").Float64()
	if dataA[0] != 1.0 || dataA[1] != 1.0 || dataA[4] != 5.0 {
		t.Errorf("SortBy ascending a = %v", dataA)
	}

	// Sort descending
	df3, err := df.SortBy("a", false)
	if err != nil {
		t.Fatalf("SortBy failed: %v", err)
	}

	dataA = df3.ColumnByName("a").Float64()
	if dataA[0] != 5.0 || dataA[4] != 1.0 {
		t.Errorf("SortBy descending a = %v", dataA)
	}
}

func TestDataFrameWithColumn(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0})

	df, _ := NewDataFrame(col1, col2)

	// Add new column
	col3 := NewSeriesFloat64("c", []float64{7.0, 8.0, 9.0})
	df2, err := df.WithColumn(col3)
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}
	if df2.Width() != 3 {
		t.Errorf("WithColumn Width = %d, want 3", df2.Width())
	}

	// Replace existing column
	col1New := NewSeriesFloat64("a", []float64{100.0, 200.0, 300.0})
	df3, err := df.WithColumn(col1New)
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}
	if df3.Width() != 2 {
		t.Errorf("WithColumn replace Width = %d, want 2", df3.Width())
	}
	data := df3.ColumnByName("a").Float64()
	if data[0] != 100.0 {
		t.Errorf("WithColumn replaced value = %v, want 100", data[0])
	}
}

func TestDataFrameRename(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0})

	df, _ := NewDataFrame(col1, col2)

	df2, err := df.Rename("a", "x")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	cols := df2.Columns()
	if cols[0] != "x" || cols[1] != "b" {
		t.Errorf("Rename columns = %v, want [x, b]", cols)
	}

	// Original unchanged
	if df.Columns()[0] != "a" {
		t.Error("Original DataFrame should not be modified")
	}
}

func TestDataFrameDescribe(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	col2 := NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0, 40.0, 50.0})

	df, _ := NewDataFrame(col1, col2)

	desc := df.Describe()

	if desc["a"]["count"] != 5.0 {
		t.Errorf("Describe a.count = %v, want 5", desc["a"]["count"])
	}
	if desc["a"]["mean"] != 3.0 {
		t.Errorf("Describe a.mean = %v, want 3", desc["a"]["mean"])
	}
	if desc["b"]["sum"] != 150.0 {
		t.Errorf("Describe b.sum = %v, want 150", desc["b"]["sum"])
	}
}

func TestDataFrameString(t *testing.T) {
	col1 := NewSeriesFloat64("price", []float64{1.5, 2.5, 3.5})
	col2 := NewSeriesFloat64("qty", []float64{100.0, 200.0, 300.0})

	df, _ := NewDataFrame(col1, col2)

	str := df.String()
	if len(str) == 0 {
		t.Error("String() should return non-empty string")
	}

	// Check it contains key info
	if !contains(str, "3 rows") || !contains(str, "2 columns") {
		t.Errorf("String() = %s, should contain dimensions", str)
	}
}

func TestDataFrameEmpty(t *testing.T) {
	df, err := NewDataFrame()
	if err != nil {
		t.Fatalf("NewDataFrame() failed: %v", err)
	}

	if df.Height() != 0 {
		t.Errorf("Empty DataFrame Height = %d, want 0", df.Height())
	}
	if df.Width() != 0 {
		t.Errorf("Empty DataFrame Width = %d, want 0", df.Width())
	}
}

func TestDataFrameLengthMismatch(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("b", []float64{4.0, 5.0}) // Different length

	_, err := NewDataFrame(col1, col2)
	if err == nil {
		t.Error("NewDataFrame with mismatched column lengths should fail")
	}
}

func TestDataFrameDuplicateColumns(t *testing.T) {
	col1 := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	col2 := NewSeriesFloat64("a", []float64{4.0, 5.0, 6.0}) // Same name

	_, err := NewDataFrame(col1, col2)
	if err == nil {
		t.Error("NewDataFrame with duplicate column names should fail")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Additional tests for coverage

func TestDataFrameSchema(t *testing.T) {
	col1 := NewSeriesInt64("id", []int64{1, 2, 3})
	col2 := NewSeriesFloat64("value", []float64{1.5, 2.5, 3.5})
	col3 := NewSeriesString("name", []string{"a", "b", "c"})
	col4 := NewSeriesBool("active", []bool{true, false, true})

	df, _ := NewDataFrame(col1, col2, col3, col4)

	schema := df.Schema()
	if schema.Len() != 4 {
		t.Errorf("Schema Len = %d, want 4", schema.Len())
	}

	// Check column names
	names := schema.Names()
	if names[0] != "id" || names[1] != "value" || names[2] != "name" || names[3] != "active" {
		t.Errorf("Schema Names = %v", names)
	}

	// Check dtypes
	dtypes := schema.DTypes()
	if dtypes[0] != Int64 {
		t.Errorf("Schema dtype[0] = %v, want Int64", dtypes[0])
	}
	if dtypes[1] != Float64 {
		t.Errorf("Schema dtype[1] = %v, want Float64", dtypes[1])
	}
	if dtypes[2] != String {
		t.Errorf("Schema dtype[2] = %v, want String", dtypes[2])
	}
	if dtypes[3] != Bool {
		t.Errorf("Schema dtype[3] = %v, want Bool", dtypes[3])
	}
}

func TestDataFrameFilterByIndicesAllTypes(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("f64", []float64{1, 2, 3, 4, 5}),
		NewSeriesFloat32("f32", []float32{1, 2, 3, 4, 5}),
		NewSeriesInt64("i64", []int64{1, 2, 3, 4, 5}),
		NewSeriesInt32("i32", []int32{1, 2, 3, 4, 5}),
		NewSeriesBool("bool", []bool{true, false, true, false, true}),
		NewSeriesString("str", []string{"a", "b", "c", "d", "e"}),
	)

	indices := []uint32{4, 2, 0}
	df2, err := df.FilterByIndices(indices)
	if err != nil {
		t.Fatalf("FilterByIndices failed: %v", err)
	}
	if df2.Height() != 3 {
		t.Errorf("FilterByIndices Height = %d, want 3", df2.Height())
	}

	// Check Float32
	f32 := df2.ColumnByName("f32").Float32()
	if f32[0] != 5 || f32[1] != 3 || f32[2] != 1 {
		t.Errorf("Float32 = %v, want [5, 3, 1]", f32)
	}

	// Check Int32
	i32 := df2.ColumnByName("i32").Int32()
	if i32[0] != 5 || i32[1] != 3 || i32[2] != 1 {
		t.Errorf("Int32 = %v, want [5, 3, 1]", i32)
	}

	// Check Bool - indices [4, 2, 0] from [true, false, true, false, true] = [true, true, true]
	b := df2.ColumnByName("bool").Bool()
	if !b[0] || !b[1] || !b[2] {
		t.Errorf("Bool = %v, want [true, true, true]", b)
	}

	// Check String
	s := df2.ColumnByName("str").Strings()
	if s[0] != "e" || s[1] != "c" || s[2] != "a" {
		t.Errorf("String = %v, want [e, c, a]", s)
	}
}

func TestDataFrameDescribeAllTypes(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("f64", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat32("f32", []float32{1.0, 2.0, 3.0}),
		NewSeriesInt64("i64", []int64{1, 2, 3}),
		NewSeriesInt32("i32", []int32{1, 2, 3}),
		NewSeriesBool("bool", []bool{true, false, true}),
		NewSeriesString("str", []string{"a", "b", "c"}),
	)

	desc := df.Describe()

	// Float64
	if desc["f64"]["count"] != 3.0 {
		t.Errorf("f64 count = %v, want 3", desc["f64"]["count"])
	}

	// Float32 should also be described
	if desc["f32"]["count"] != 3.0 {
		t.Errorf("f32 count = %v, want 3", desc["f32"]["count"])
	}

	// Int64
	if desc["i64"]["count"] != 3.0 {
		t.Errorf("i64 count = %v, want 3", desc["i64"]["count"])
	}

	// Int32
	if desc["i32"]["count"] != 3.0 {
		t.Errorf("i32 count = %v, want 3", desc["i32"]["count"])
	}
}

func TestDataFrameDescribeNoNumeric(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesBool("bool", []bool{true, false, true}),
		NewSeriesString("str", []string{"a", "b", "c"}),
	)

	desc := df.Describe()
	if desc != nil {
		t.Errorf("Describe with no numeric columns should return nil, got %v", desc)
	}
}

func TestDataFrameDescribeParallel(t *testing.T) {
	// Create large dataset to trigger parallel path
	n := 10000 // Above MinRowsForParallel default of 8192
	f64Data := make([]float64, n)
	i64Data := make([]int64, n)
	f32Data := make([]float32, n)
	for i := 0; i < n; i++ {
		f64Data[i] = float64(i)
		i64Data[i] = int64(i)
		f32Data[i] = float32(i)
	}

	df, _ := NewDataFrame(
		NewSeriesFloat64("f64", f64Data),
		NewSeriesInt64("i64", i64Data),
		NewSeriesFloat32("f32", f32Data),
	)

	desc := df.Describe()

	// Verify all columns are described
	if len(desc) != 3 {
		t.Errorf("Describe should have 3 columns, got %d", len(desc))
	}

	// Check counts
	if desc["f64"]["count"] != float64(n) {
		t.Errorf("f64 count = %v, want %d", desc["f64"]["count"], n)
	}
	if desc["i64"]["count"] != float64(n) {
		t.Errorf("i64 count = %v, want %d", desc["i64"]["count"], n)
	}
	if desc["f32"]["count"] != float64(n) {
		t.Errorf("f32 count = %v, want %d", desc["f32"]["count"], n)
	}
}

func TestDataFrameFilterByMaskAllTypes(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat32("f32", []float32{1, 2, 3}),
		NewSeriesInt32("i32", []int32{10, 20, 30}),
		NewSeriesBool("bool", []bool{true, false, true}),
		NewSeriesString("str", []string{"a", "b", "c"}),
	)

	mask := []byte{1, 0, 1}
	df2, err := df.FilterByMask(mask)
	if err != nil {
		t.Fatalf("FilterByMask failed: %v", err)
	}
	if df2.Height() != 2 {
		t.Errorf("FilterByMask Height = %d, want 2", df2.Height())
	}

	// Check Float32
	f32 := df2.ColumnByName("f32").Float32()
	if f32[0] != 1 || f32[1] != 3 {
		t.Errorf("Float32 = %v, want [1, 3]", f32)
	}

	// Check Int32
	i32 := df2.ColumnByName("i32").Int32()
	if i32[0] != 10 || i32[1] != 30 {
		t.Errorf("Int32 = %v, want [10, 30]", i32)
	}

	// Check Bool
	b := df2.ColumnByName("bool").Bool()
	if !b[0] || !b[1] {
		t.Errorf("Bool = %v, want [true, true]", b)
	}

	// Check String
	s := df2.ColumnByName("str").Strings()
	if s[0] != "a" || s[1] != "c" {
		t.Errorf("String = %v, want [a, c]", s)
	}
}

func TestDataFrameHeadTailEdgeCases(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3, 4, 5}),
	)

	// Head more than available
	df2 := df.Head(100)
	if df2.Height() != 5 {
		t.Errorf("Head(100) Height = %d, want 5", df2.Height())
	}

	// Tail more than available
	df3 := df.Tail(100)
	if df3.Height() != 5 {
		t.Errorf("Tail(100) Height = %d, want 5", df3.Height())
	}
}

func TestDataFrameSortByNonexistent(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
	)

	_, err := df.SortBy("nonexistent", true)
	if err == nil {
		t.Error("SortBy nonexistent column should fail")
	}
}

func TestDataFrameRenameNonexistent(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
	)

	_, err := df.Rename("nonexistent", "b")
	if err == nil {
		t.Error("Rename nonexistent column should fail")
	}
}

func TestDataFrameWithColumnLengthMismatch(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
	)

	newCol := NewSeriesInt64("b", []int64{1, 2}) // Wrong length
	_, err := df.WithColumn(newCol)
	if err == nil {
		t.Error("WithColumn with mismatched length should fail")
	}
}

func TestDataFrameFilterByMaskLengthMismatch(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
	)

	mask := []byte{1, 0} // Wrong length
	_, err := df.FilterByMask(mask)
	if err == nil {
		t.Error("FilterByMask with mismatched length should fail")
	}
}
