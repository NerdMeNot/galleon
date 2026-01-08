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
