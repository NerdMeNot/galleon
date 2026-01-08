package galleon

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCSVReadWrite(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
		NewSeriesFloat64("value", []float64{1.1, 2.2, 3.3, 4.4, 5.5}),
		NewSeriesString("name", []string{"alice", "bob", "carol", "dave", "eve"}),
		NewSeriesBool("active", []bool{true, false, true, false, true}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write to CSV
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := df.WriteCSV(csvPath); err != nil {
		t.Fatalf("failed to write CSV: %v", err)
	}

	// Read back
	df2, err := ReadCSV(csvPath)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	// Verify dimensions
	if df2.Height() != df.Height() {
		t.Errorf("height mismatch: got %d, want %d", df2.Height(), df.Height())
	}
	if df2.Width() != df.Width() {
		t.Errorf("width mismatch: got %d, want %d", df2.Width(), df.Width())
	}

	// Verify column names
	for _, col := range df.Columns() {
		if df2.ColumnByName(col) == nil {
			t.Errorf("missing column: %s", col)
		}
	}
}

func TestCSVReadWriteBuffer(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
		NewSeriesFloat64("b", []float64{1.5, 2.5, 3.5}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write to buffer
	var buf bytes.Buffer
	if err := df.WriteCSVToWriter(&buf); err != nil {
		t.Fatalf("failed to write CSV to buffer: %v", err)
	}

	// Read from buffer
	df2, err := ReadCSVFromReader(&buf)
	if err != nil {
		t.Fatalf("failed to read CSV from buffer: %v", err)
	}

	if df2.Height() != 3 {
		t.Errorf("height mismatch: got %d, want 3", df2.Height())
	}
}

func TestCSVOptions(t *testing.T) {
	csv := "id;name;value\n1;alice;10.5\n2;bob;20.5\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ';',
			HasHeader:  true,
			InferTypes: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.Height() != 2 {
		t.Errorf("height mismatch: got %d, want 2", df.Height())
	}
	if df.Width() != 3 {
		t.Errorf("width mismatch: got %d, want 3", df.Width())
	}
}

func TestJSONRecordsReadWrite(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesFloat64("value", []float64{1.1, 2.2, 3.3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write to JSON
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "test.json")
	if err := df.WriteJSON(jsonPath); err != nil {
		t.Fatalf("failed to write JSON: %v", err)
	}

	// Read back
	df2, err := ReadJSON(jsonPath)
	if err != nil {
		t.Fatalf("failed to read JSON: %v", err)
	}

	// Verify dimensions
	if df2.Height() != df.Height() {
		t.Errorf("height mismatch: got %d, want %d", df2.Height(), df.Height())
	}
}

func TestJSONColumnsFormat(t *testing.T) {
	json := `{"a":[1,2,3],"b":[4.1,5.2,6.3]}`

	df, err := ReadJSONFromReader(
		bytes.NewReader([]byte(json)),
		JSONReadOptions{Format: JSONColumns},
	)
	if err != nil {
		t.Fatalf("failed to read JSON: %v", err)
	}

	if df.Height() != 3 {
		t.Errorf("height mismatch: got %d, want 3", df.Height())
	}
	if df.Width() != 2 {
		t.Errorf("width mismatch: got %d, want 2", df.Width())
	}
}

func TestJSONRecordsFormat(t *testing.T) {
	json := `[{"a":1,"b":4.1},{"a":2,"b":5.2},{"a":3,"b":6.3}]`

	df, err := ReadJSONFromReader(
		bytes.NewReader([]byte(json)),
		JSONReadOptions{Format: JSONRecords},
	)
	if err != nil {
		t.Fatalf("failed to read JSON: %v", err)
	}

	if df.Height() != 3 {
		t.Errorf("height mismatch: got %d, want 3", df.Height())
	}
}

func TestJSONWriteOptions(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("x", []int64{1, 2}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write with indent
	var buf bytes.Buffer
	if err := df.WriteJSONToWriter(&buf, JSONWriteOptions{
		Format: JSONRecords,
		Indent: "  ",
	}); err != nil {
		t.Fatalf("failed to write JSON: %v", err)
	}

	// Should contain newlines due to indentation
	if !bytes.Contains(buf.Bytes(), []byte("\n")) {
		t.Error("expected indented JSON to contain newlines")
	}
}

func TestParquetReadWrite(t *testing.T) {
	// Create a DataFrame with various types
	df, err := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
		NewSeriesFloat64("value", []float64{1.1, 2.2, 3.3, 4.4, 5.5}),
		NewSeriesBool("flag", []bool{true, false, true, false, true}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test.parquet")
	if err := df.WriteParquet(parquetPath); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Verify file was created
	info, err := os.Stat(parquetPath)
	if err != nil {
		t.Fatalf("failed to stat parquet file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("parquet file is empty")
	}

	// Read back
	df2, err := ReadParquet(parquetPath)
	if err != nil {
		t.Fatalf("failed to read Parquet: %v", err)
	}

	// Verify dimensions
	if df2.Height() != df.Height() {
		t.Errorf("height mismatch: got %d, want %d", df2.Height(), df.Height())
	}
	if df2.Width() != df.Width() {
		t.Errorf("width mismatch: got %d, want %d", df2.Width(), df.Width())
	}
}

func TestParquetColumnSelection(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3}),
		NewSeriesInt64("b", []int64{4, 5, 6}),
		NewSeriesInt64("c", []int64{7, 8, 9}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test.parquet")
	if err := df.WriteParquet(parquetPath); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Read only specific columns
	df2, err := ReadParquet(parquetPath, ParquetReadOptions{
		Columns: []string{"a", "c"},
	})
	if err != nil {
		t.Fatalf("failed to read Parquet: %v", err)
	}

	if df2.Width() != 2 {
		t.Errorf("width mismatch: got %d, want 2", df2.Width())
	}
	if df2.ColumnByName("a") == nil || df2.ColumnByName("c") == nil {
		t.Error("missing expected columns")
	}
	if df2.ColumnByName("b") != nil {
		t.Error("should not have column b")
	}
}

func TestParquetMaxRows(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrame(
		NewSeriesInt64("x", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	// Write to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test.parquet")
	if err := df.WriteParquet(parquetPath); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	// Read with max rows limit
	df2, err := ReadParquet(parquetPath, ParquetReadOptions{
		MaxRows: 5,
	})
	if err != nil {
		t.Fatalf("failed to read Parquet: %v", err)
	}

	if df2.Height() != 5 {
		t.Errorf("height mismatch: got %d, want 5", df2.Height())
	}
}

func TestEmptyDataFrameIO(t *testing.T) {
	df, _ := NewDataFrame()

	tmpDir := t.TempDir()

	// CSV
	csvPath := filepath.Join(tmpDir, "empty.csv")
	if err := df.WriteCSV(csvPath); err != nil {
		t.Errorf("failed to write empty CSV: %v", err)
	}

	// JSON
	jsonPath := filepath.Join(tmpDir, "empty.json")
	if err := df.WriteJSON(jsonPath); err != nil {
		t.Errorf("failed to write empty JSON: %v", err)
	}

	// Parquet - should not error for empty DataFrame
	parquetPath := filepath.Join(tmpDir, "empty.parquet")
	if err := df.WriteParquet(parquetPath); err != nil {
		t.Errorf("failed to write empty Parquet: %v", err)
	}
}

func TestCSVTypeInference(t *testing.T) {
	csv := `int,float,bool,str
1,1.5,true,hello
2,2.5,false,world
3,3.5,true,test`

	df, err := ReadCSVFromReader(bytes.NewReader([]byte(csv)))
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	// Check inferred types
	tests := []struct {
		col   string
		dtype DType
	}{
		{"int", Int64},
		{"float", Float64},
		{"bool", Bool},
		{"str", String},
	}

	for _, tt := range tests {
		col := df.ColumnByName(tt.col)
		if col == nil {
			t.Errorf("missing column: %s", tt.col)
			continue
		}
		if col.DType() != tt.dtype {
			t.Errorf("column %s: got dtype %v, want %v", tt.col, col.DType(), tt.dtype)
		}
	}
}
