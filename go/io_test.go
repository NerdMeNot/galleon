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

// Additional I/O tests for coverage

func TestDefaultCSVReadOptions(t *testing.T) {
	opts := DefaultCSVReadOptions()
	if opts.Delimiter != ',' {
		t.Errorf("expected delimiter ',', got '%c'", opts.Delimiter)
	}
	if !opts.HasHeader {
		t.Error("expected HasHeader to be true")
	}
	if !opts.InferTypes {
		t.Error("expected InferTypes to be true")
	}
	if !opts.TrimSpace {
		t.Error("expected TrimSpace to be true")
	}
}

func TestDefaultCSVWriteOptions(t *testing.T) {
	opts := DefaultCSVWriteOptions()
	if opts.Delimiter != ',' {
		t.Errorf("expected delimiter ',', got '%c'", opts.Delimiter)
	}
	if !opts.WriteHeader {
		t.Error("expected WriteHeader to be true")
	}
}

func TestCSVNoHeader(t *testing.T) {
	csv := "1,2.5,true\n3,4.5,false\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  false,
			InferTypes: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	// Should generate column names like column_0, column_1, etc.
	if df.Height() != 2 {
		t.Errorf("height mismatch: got %d, want 2", df.Height())
	}
	if df.Width() != 3 {
		t.Errorf("width mismatch: got %d, want 3", df.Width())
	}
}

func TestCSVWithColumnNames(t *testing.T) {
	csv := "1,2.5,true\n3,4.5,false\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:   ',',
			HasHeader:   false,
			ColumnNames: []string{"id", "value", "flag"},
			InferTypes:  true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.ColumnByName("id") == nil {
		t.Error("missing 'id' column")
	}
	if df.ColumnByName("value") == nil {
		t.Error("missing 'value' column")
	}
	if df.ColumnByName("flag") == nil {
		t.Error("missing 'flag' column")
	}
}

func TestCSVSkipRows(t *testing.T) {
	// Skip rows must have same field count as data, so use consistent format
	csv := "skip,skip\nalso,skip\nid,value\n1,100\n2,200\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  true,
			SkipRows:   2,
			InferTypes: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.Height() != 2 {
		t.Errorf("height mismatch: got %d, want 2", df.Height())
	}
}

func TestCSVMaxRows(t *testing.T) {
	csv := "id,value\n1,100\n2,200\n3,300\n4,400\n5,500\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  true,
			MaxRows:    3,
			InferTypes: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.Height() != 3 {
		t.Errorf("height mismatch: got %d, want 3", df.Height())
	}
}

func TestCSVCommentCharacter(t *testing.T) {
	csv := "id,value\n# this is a comment\n1,100\n2,200\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  true,
			Comment:    '#',
			InferTypes: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.Height() != 2 {
		t.Errorf("height mismatch: got %d, want 2", df.Height())
	}
}

func TestCSVWithColumnTypes(t *testing.T) {
	csv := "id,value\n1,100\n2,200\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter: ',',
			HasHeader: true,
			ColumnTypes: map[string]DType{
				"id":    String, // Force string instead of int
				"value": Float64,
			},
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.ColumnByName("id").DType() != String {
		t.Error("id should be String")
	}
	if df.ColumnByName("value").DType() != Float64 {
		t.Error("value should be Float64")
	}
}

func TestCSVWriteWithOptions(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2}),
		NewSeriesFloat64("b", []float64{1.5, 2.5}),
	)

	var buf bytes.Buffer
	err := df.WriteCSVToWriter(&buf, CSVWriteOptions{
		Delimiter:   ';',
		WriteHeader: true,
	})
	if err != nil {
		t.Fatalf("failed to write CSV: %v", err)
	}

	content := buf.String()
	if !bytes.Contains([]byte(content), []byte(";")) {
		t.Error("expected semicolon delimiter in output")
	}
}

func TestCSVWriteWithoutHeader(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2}),
	)

	var buf bytes.Buffer
	err := df.WriteCSVToWriter(&buf, CSVWriteOptions{
		Delimiter:   ',',
		WriteHeader: false,
	})
	if err != nil {
		t.Fatalf("failed to write CSV: %v", err)
	}

	content := buf.String()
	if bytes.Contains([]byte(content), []byte("a")) {
		t.Error("header should not be present")
	}
}

func TestCSVWriteFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat32("val", []float32{1.5, 2.5}),
	)

	var buf bytes.Buffer
	err := df.WriteCSVToWriter(&buf)
	if err != nil {
		t.Fatalf("failed to write CSV: %v", err)
	}

	// Should contain the values
	content := buf.String()
	if !bytes.Contains([]byte(content), []byte("1.5")) {
		t.Error("expected float32 value in output")
	}
}

func TestCSVWriteInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt32("val", []int32{100, 200}),
	)

	var buf bytes.Buffer
	err := df.WriteCSVToWriter(&buf)
	if err != nil {
		t.Fatalf("failed to write CSV: %v", err)
	}

	content := buf.String()
	if !bytes.Contains([]byte(content), []byte("100")) {
		t.Error("expected int32 value in output")
	}
}

func TestCSVNullHandling(t *testing.T) {
	csv := "id,value\n1,100\n,null\n3,NA\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  true,
			InferTypes: true,
			NullValues: []string{"", "null", "NA"},
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.Height() != 3 {
		t.Errorf("height mismatch: got %d, want 3", df.Height())
	}
}

func TestCSVReadFileNotFound(t *testing.T) {
	_, err := ReadCSV("/nonexistent/path/to/file.csv")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestCSVWriteFileError(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1}),
	)

	// Try to write to a directory (should fail)
	err := df.WriteCSV("/")
	if err == nil {
		t.Error("expected error writing to invalid path")
	}
}

func TestJSONWriteColumnsFormat(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2}),
		NewSeriesFloat64("b", []float64{1.5, 2.5}),
	)

	var buf bytes.Buffer
	err := df.WriteJSONToWriter(&buf, JSONWriteOptions{
		Format: JSONColumns,
	})
	if err != nil {
		t.Fatalf("failed to write JSON: %v", err)
	}

	// Should be in columns format
	content := buf.String()
	if !bytes.Contains([]byte(content), []byte(`"a":`)) {
		t.Error("expected column format in output")
	}
}

func TestJSONReadFileNotFound(t *testing.T) {
	_, err := ReadJSON("/nonexistent/path/to/file.json")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestJSONWriteFileError(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1}),
	)

	err := df.WriteJSON("/")
	if err == nil {
		t.Error("expected error writing to invalid path")
	}
}

func TestParquetReadFileNotFound(t *testing.T) {
	_, err := ReadParquet("/nonexistent/path/to/file.parquet")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestParquetWriteFileError(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1}),
	)

	err := df.WriteParquet("/nonexistent/dir/file.parquet")
	if err == nil {
		t.Error("expected error writing to invalid path")
	}
}

func TestCSVBoolTypeInference(t *testing.T) {
	csv := "flag\ntrue\nfalse\ntrue\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  true,
			InferTypes: true,
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	col := df.ColumnByName("flag")
	if col.DType() != Bool {
		t.Errorf("expected Bool type, got %v", col.DType())
	}
}

func TestCSVAllNullColumn(t *testing.T) {
	// All null values should default to String type
	csv := "val\nNA\nnull\n"

	df, err := ReadCSVFromReader(
		bytes.NewReader([]byte(csv)),
		CSVReadOptions{
			Delimiter:  ',',
			HasHeader:  true,
			InferTypes: true,
			NullValues: []string{"", "null", "NA"},
		},
	)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	if df.Height() != 2 {
		t.Errorf("height mismatch: got %d, want 2", df.Height())
	}

	// All null values should result in String type
	col := df.ColumnByName("val")
	if col == nil {
		t.Error("missing 'val' column")
	}
	if col.DType() != String {
		t.Errorf("expected String type for all-null column, got %v", col.DType())
	}
}

func TestJSONWriteBool(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesBool("flag", []bool{true, false}),
	)

	var buf bytes.Buffer
	err := df.WriteJSONToWriter(&buf, JSONWriteOptions{Format: JSONRecords})
	if err != nil {
		t.Fatalf("failed to write JSON: %v", err)
	}

	content := buf.String()
	if !bytes.Contains([]byte(content), []byte("true")) {
		t.Error("expected boolean true in output")
	}
}

func TestParquetWithAllTypes(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("int64_col", []int64{1, 2}),
		NewSeriesInt32("int32_col", []int32{10, 20}),
		NewSeriesFloat64("float64_col", []float64{1.5, 2.5}),
		NewSeriesFloat32("float32_col", []float32{0.5, 1.5}),
		NewSeriesBool("bool_col", []bool{true, false}),
		NewSeriesString("string_col", []string{"a", "b"}),
	)

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.parquet")

	if err := df.WriteParquet(path); err != nil {
		t.Fatalf("failed to write Parquet: %v", err)
	}

	df2, err := ReadParquet(path)
	if err != nil {
		t.Fatalf("failed to read Parquet: %v", err)
	}

	if df2.Width() != 6 {
		t.Errorf("width mismatch: got %d, want 6", df2.Width())
	}
}

// ============================================================================
// appendNullValue Tests
// ============================================================================

func TestAppendNullValue_Float64(t *testing.T) {
	b := &colBuilder{dtype: Float64}
	appendNullValue(b)
	if len(b.f64Data) != 1 || b.f64Data[0] != 0 {
		t.Errorf("expected f64Data=[0], got %v", b.f64Data)
	}
}

func TestAppendNullValue_Float32(t *testing.T) {
	b := &colBuilder{dtype: Float32}
	appendNullValue(b)
	if len(b.f32Data) != 1 || b.f32Data[0] != 0 {
		t.Errorf("expected f32Data=[0], got %v", b.f32Data)
	}
}

func TestAppendNullValue_Int64(t *testing.T) {
	b := &colBuilder{dtype: Int64}
	appendNullValue(b)
	if len(b.i64Data) != 1 || b.i64Data[0] != 0 {
		t.Errorf("expected i64Data=[0], got %v", b.i64Data)
	}
}

func TestAppendNullValue_Int32(t *testing.T) {
	b := &colBuilder{dtype: Int32}
	appendNullValue(b)
	if len(b.i32Data) != 1 || b.i32Data[0] != 0 {
		t.Errorf("expected i32Data=[0], got %v", b.i32Data)
	}
}

func TestAppendNullValue_Bool(t *testing.T) {
	b := &colBuilder{dtype: Bool}
	appendNullValue(b)
	if len(b.boolData) != 1 || b.boolData[0] != false {
		t.Errorf("expected boolData=[false], got %v", b.boolData)
	}
}

func TestAppendNullValue_String(t *testing.T) {
	b := &colBuilder{dtype: String}
	appendNullValue(b)
	if len(b.strData) != 1 || b.strData[0] != "" {
		t.Errorf("expected strData=[\"\"], got %v", b.strData)
	}
}
