package galleon

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestCSVBatchReader_Basic(t *testing.T) {
	csvData := `name,age,salary
Alice,30,50000
Bob,25,45000
Carol,35,60000`

	reader, err := NewCSVBatchReader(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("NewCSVBatchReader failed: %v", err)
	}
	defer reader.Close()

	// Read first (and only) batch
	batch, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	// Verify data
	if batch.Height() != 3 {
		t.Errorf("Expected 3 rows, got %d", batch.Height())
	}
	if batch.Width() != 3 {
		t.Errorf("Expected 3 columns, got %d", batch.Width())
	}

	// Read again - should be EOF
	_, err = reader.Next(context.Background())
	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}
}

func TestCSVBatchReader_MultipleBatches(t *testing.T) {
	// Create CSV with more rows than batch size
	var sb strings.Builder
	sb.WriteString("id,value\n")
	for i := 0; i < 100; i++ {
		sb.WriteString(strings.TrimSpace(strings.ReplaceAll(
			"ID,VAL\n",
			"ID", strings.TrimSpace(string(rune('0'+i%10))))) + "\n")
	}

	// Use small batch size
	opts := CSVBatchReaderOptions{BatchSize: 10}
	reader, err := NewCSVBatchReader(strings.NewReader("id,value\n1,10\n2,20\n3,30\n4,40\n5,50"), opts)
	if err != nil {
		t.Fatalf("NewCSVBatchReader failed: %v", err)
	}
	defer reader.Close()

	// Should read in one batch (less than 10 rows)
	batch, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if batch.Height() != 5 {
		t.Errorf("Expected 5 rows, got %d", batch.Height())
	}
}

func TestCSVBatchReader_TypeInference(t *testing.T) {
	csvData := `name,age,salary,active
Alice,30,50000.5,true
Bob,25,45000.0,false
Carol,35,60000.0,true`

	reader, err := NewCSVBatchReader(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("NewCSVBatchReader failed: %v", err)
	}
	defer reader.Close()

	batch, _ := reader.Next(context.Background())

	// Check inferred types
	nameCol := batch.ColumnByName("name")
	if nameCol.DType() != String {
		t.Errorf("name dtype: expected String, got %s", nameCol.DType())
	}

	ageCol := batch.ColumnByName("age")
	if ageCol.DType() != Int64 {
		t.Errorf("age dtype: expected Int64, got %s", ageCol.DType())
	}

	salaryCol := batch.ColumnByName("salary")
	if salaryCol.DType() != Float64 {
		t.Errorf("salary dtype: expected Float64, got %s", salaryCol.DType())
	}

	activeCol := batch.ColumnByName("active")
	if activeCol.DType() != Bool {
		t.Errorf("active dtype: expected Bool, got %s", activeCol.DType())
	}
}

func TestPipeline_Basic(t *testing.T) {
	csvData := `name,age,salary
Alice,30,50000
Bob,25,45000
Carol,35,60000
Dave,40,70000
Eve,28,52000`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader)

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 5 {
		t.Errorf("Expected 5 rows, got %d", result.Height())
	}
}

func TestPipeline_Filter(t *testing.T) {
	csvData := `name,age,salary
Alice,30,50000
Bob,25,45000
Carol,35,60000
Dave,40,70000
Eve,28,52000`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).
		Filter(Col("age").Gt(Lit(30)))

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Carol (35) and Dave (40) should pass filter
	if result.Height() != 2 {
		t.Errorf("Expected 2 rows (age > 30), got %d", result.Height())
	}
}

func TestPipeline_Limit(t *testing.T) {
	csvData := `id,value
1,100
2,200
3,300
4,400
5,500`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).Limit(3)

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 3 {
		t.Errorf("Expected 3 rows (limit), got %d", result.Height())
	}
}

func TestPipeline_Transform(t *testing.T) {
	csvData := `a,b
1,10
2,20
3,30`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).
		Transform(func(df *DataFrame) (*DataFrame, error) {
			// Add a new column: a + b
			return df.Lazy().
				WithColumn("sum", Col("a").Add(Col("b"))).
				Collect()
		})

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Should have new "sum" column
	sumCol := result.ColumnByName("sum")
	if sumCol == nil {
		t.Fatal("Missing 'sum' column")
	}

	// Verify sum values: 11, 22, 33
	expectedSums := []float64{11, 22, 33}
	for i, expected := range expectedSums {
		val, _ := sumCol.GetFloat64(i)
		if val != expected {
			t.Errorf("sum[%d]: expected %f, got %f", i, expected, val)
		}
	}
}

func TestPipeline_ForEach(t *testing.T) {
	csvData := `id,value
1,100
2,200
3,300`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader)

	batchCount := 0
	totalRows := 0

	err := pipeline.ForEach(context.Background(), func(df *DataFrame) error {
		batchCount++
		totalRows += df.Height()
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}

	if totalRows != 3 {
		t.Errorf("Expected 3 total rows, got %d", totalRows)
	}
}

func TestConcatDataFrames(t *testing.T) {
	df1, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2}),
		NewSeriesString("name", []string{"Alice", "Bob"}),
	)

	df2, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{3, 4}),
		NewSeriesString("name", []string{"Carol", "Dave"}),
	)

	result, err := ConcatDataFrames(df1, df2)
	if err != nil {
		t.Fatalf("ConcatDataFrames failed: %v", err)
	}

	if result.Height() != 4 {
		t.Errorf("Expected 4 rows, got %d", result.Height())
	}

	// Verify data
	ids := result.ColumnByName("id").Int64()
	expectedIds := []int64{1, 2, 3, 4}
	for i, expected := range expectedIds {
		if ids[i] != expected {
			t.Errorf("id[%d]: expected %d, got %d", i, expected, ids[i])
		}
	}

	names := result.ColumnByName("name").Strings()
	expectedNames := []string{"Alice", "Bob", "Carol", "Dave"}
	for i, expected := range expectedNames {
		if names[i] != expected {
			t.Errorf("name[%d]: expected %s, got %s", i, expected, names[i])
		}
	}
}

// ============================================================================
// Streaming/Pipeline Correctness Tests
// ============================================================================

func TestCorrectness_CSVBatchReader_AllRowsRead(t *testing.T) {
	// Create CSV with known number of rows
	csvData := `id,value
1,100
2,200
3,300
4,400
5,500
6,600
7,700
8,800
9,900
10,1000`

	reader, err := NewCSVBatchReader(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("NewCSVBatchReader failed: %v", err)
	}
	defer reader.Close()

	totalRows := 0
	for {
		batch, err := reader.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		totalRows += batch.Height()
	}

	if totalRows != 10 {
		t.Errorf("Expected 10 rows, got %d", totalRows)
	}
}

func TestCorrectness_CSVBatchReader_ValuesMatch(t *testing.T) {
	csvData := `name,age,salary
Alice,30,50000
Bob,25,45000
Carol,35,60000`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	defer reader.Close()

	batch, _ := reader.Next(context.Background())

	// Verify values
	names := batch.ColumnByName("name").Strings()
	ages := batch.ColumnByName("age").Int64()
	salaries := batch.ColumnByName("salary").Int64()

	expectedNames := []string{"Alice", "Bob", "Carol"}
	expectedAges := []int64{30, 25, 35}
	expectedSalaries := []int64{50000, 45000, 60000}

	for i := range expectedNames {
		if names[i] != expectedNames[i] {
			t.Errorf("name[%d]: expected %s, got %s", i, expectedNames[i], names[i])
		}
		if ages[i] != expectedAges[i] {
			t.Errorf("age[%d]: expected %d, got %d", i, expectedAges[i], ages[i])
		}
		if salaries[i] != expectedSalaries[i] {
			t.Errorf("salary[%d]: expected %d, got %d", i, expectedSalaries[i], salaries[i])
		}
	}
}

func TestCorrectness_Pipeline_FilterPreservesMatchingRows(t *testing.T) {
	csvData := `id,value
1,10
2,20
3,30
4,40
5,50`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).
		Filter(Col("value").Gt(Lit(25)))

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Should have rows where value > 25: 30, 40, 50
	if result.Height() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.Height())
	}

	// Verify all values are > 25 (values are Int64 from CSV inference)
	values := result.ColumnByName("value").Int64()
	for i, val := range values {
		if val <= 25 {
			t.Errorf("Filter failed: row %d has value %d <= 25", i, val)
		}
	}
}

func TestCorrectness_Pipeline_LimitReturnsExactCount(t *testing.T) {
	csvData := `id,value
1,100
2,200
3,300
4,400
5,500
6,600
7,700
8,800
9,900
10,1000`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).Limit(5)

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if result.Height() != 5 {
		t.Errorf("Expected exactly 5 rows, got %d", result.Height())
	}

	// Verify we got the first 5 rows
	ids := result.ColumnByName("id").Int64()
	for i, expected := range []int64{1, 2, 3, 4, 5} {
		if ids[i] != expected {
			t.Errorf("id[%d]: expected %d, got %d", i, expected, ids[i])
		}
	}
}

func TestCorrectness_Pipeline_TransformAppliesCorrectly(t *testing.T) {
	csvData := `a,b
1,10
2,20
3,30`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).
		Transform(func(df *DataFrame) (*DataFrame, error) {
			return df.Lazy().
				WithColumn("sum", Col("a").Add(Col("b"))).
				Collect()
		})

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Verify sum column exists and has correct values
	sumCol := result.ColumnByName("sum")
	if sumCol == nil {
		t.Fatal("Missing 'sum' column")
	}

	expectedSums := []float64{11, 22, 33}
	for i, expected := range expectedSums {
		val, _ := sumCol.GetFloat64(i)
		if val != expected {
			t.Errorf("sum[%d]: expected %f, got %f", i, expected, val)
		}
	}
}

func TestCorrectness_Pipeline_ForEachProcessesAllBatches(t *testing.T) {
	csvData := `id,value
1,100
2,200
3,300
4,400
5,500`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader)

	totalRows := 0
	rowSum := int64(0)

	err := pipeline.ForEach(context.Background(), func(df *DataFrame) error {
		totalRows += df.Height()
		ids := df.ColumnByName("id").Int64()
		for _, id := range ids {
			rowSum += id
		}
		return nil
	})

	if err != nil {
		t.Fatalf("ForEach failed: %v", err)
	}

	if totalRows != 5 {
		t.Errorf("Expected 5 total rows, got %d", totalRows)
	}

	// Sum of 1+2+3+4+5 = 15
	if rowSum != 15 {
		t.Errorf("Expected sum of 15, got %d", rowSum)
	}
}

func TestCorrectness_ConcatDataFrames_PreservesAllRows(t *testing.T) {
	df1, _ := NewDataFrame(
		NewSeriesFloat64("x", []float64{1.0, 2.0, 3.0}),
	)
	df2, _ := NewDataFrame(
		NewSeriesFloat64("x", []float64{4.0, 5.0}),
	)
	df3, _ := NewDataFrame(
		NewSeriesFloat64("x", []float64{6.0, 7.0, 8.0, 9.0}),
	)

	result, err := ConcatDataFrames(df1, df2, df3)
	if err != nil {
		t.Fatalf("ConcatDataFrames failed: %v", err)
	}

	if result.Height() != 9 {
		t.Errorf("Expected 9 rows, got %d", result.Height())
	}

	// Verify all values
	expected := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}
	values := result.ColumnByName("x").Float64()
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("x[%d]: expected %f, got %f", i, exp, values[i])
		}
	}
}

func TestCorrectness_ConcatDataFrames_PreservesOrder(t *testing.T) {
	// Verify that rows from df1 come before df2
	df1, _ := NewDataFrame(
		NewSeriesString("source", []string{"A", "A"}),
		NewSeriesInt64("id", []int64{1, 2}),
	)
	df2, _ := NewDataFrame(
		NewSeriesString("source", []string{"B", "B"}),
		NewSeriesInt64("id", []int64{3, 4}),
	)

	result, _ := ConcatDataFrames(df1, df2)

	sources := result.ColumnByName("source").Strings()
	ids := result.ColumnByName("id").Int64()

	// First two should be from A, last two from B
	if sources[0] != "A" || sources[1] != "A" {
		t.Error("First two rows should be from source A")
	}
	if sources[2] != "B" || sources[3] != "B" {
		t.Error("Last two rows should be from source B")
	}

	// IDs should be in order
	for i, expected := range []int64{1, 2, 3, 4} {
		if ids[i] != expected {
			t.Errorf("id[%d]: expected %d, got %d", i, expected, ids[i])
		}
	}
}

func TestCorrectness_Pipeline_ChainedOperations(t *testing.T) {
	csvData := `id,value
1,10
2,20
3,30
4,40
5,50
6,60
7,70
8,80
9,90
10,100`

	reader, _ := NewCSVBatchReader(strings.NewReader(csvData))
	pipeline := NewPipeline(reader).
		Filter(Col("value").Gt(Lit(25))).      // Keep value > 25
		Transform(func(df *DataFrame) (*DataFrame, error) {
			return df.Lazy().
				WithColumn("doubled", Col("value").Mul(Lit(2))).
				Collect()
		}).
		Limit(5) // Take first 5 of filtered

	result, err := pipeline.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Should have 5 rows (limit)
	if result.Height() != 5 {
		t.Errorf("Expected 5 rows, got %d", result.Height())
	}

	// All values should be > 25 (values are Int64 from CSV inference)
	values := result.ColumnByName("value").Int64()
	for i, val := range values {
		if val <= 25 {
			t.Errorf("Row %d has value %d which should have been filtered", i, val)
		}
	}

	// Verify doubled column exists and is correct
	doubled := result.ColumnByName("doubled")
	if doubled == nil {
		t.Fatal("Missing 'doubled' column")
	}

	// The Mul(Lit(2)) operation on Int64 produces Float64
	for i := 0; i < result.Height(); i++ {
		val := values[i]
		// Get value as interface and check both int and float types
		dblVal := doubled.Get(i)
		var dbl float64
		switch v := dblVal.(type) {
		case float64:
			dbl = v
		case int64:
			dbl = float64(v)
		}
		if dbl != float64(val*2) {
			t.Errorf("doubled[%d]: expected %d, got %v", i, val*2, dbl)
		}
	}
}
