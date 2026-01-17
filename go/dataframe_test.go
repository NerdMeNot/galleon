package galleon

import (
	"testing"
)

// ============================================================================
// Creation Tests
// ============================================================================

func TestNewDataFrame(t *testing.T) {
	df := NewDataFrame()
	if df == nil {
		t.Fatal("NewDataFrame returned nil")
	}
	if df.Height() != 0 {
		t.Errorf("Empty DataFrame Height() = %d, want 0", df.Height())
	}
	if df.Width() != 0 {
		t.Errorf("Empty DataFrame Width() = %d, want 0", df.Width())
	}
}

func TestArrowDataFrameAddColumn(t *testing.T) {
	df := NewDataFrame()

	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()
	s2 := NewSeriesI64("b", []int64{10, 20, 30})
	defer s2.Release()

	df.AddColumn(s1).AddColumn(s2)

	if df.Width() != 2 {
		t.Errorf("Width() = %d, want 2", df.Width())
	}
	if df.Height() != 3 {
		t.Errorf("Height() = %d, want 3", df.Height())
	}

	names := df.ColumnNames()
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Errorf("ColumnNames() = %v, want [a, b]", names)
	}
}

func TestFromColumns(t *testing.T) {
	s1 := NewSeriesF64("x", []float64{1.0, 2.0, 3.0})
	defer s1.Release()
	s2 := NewSeriesF64("y", []float64{4.0, 5.0, 6.0})
	defer s2.Release()

	df := FromColumns(s1, s2)
	if df == nil {
		t.Fatal("FromColumns returned nil")
	}

	if df.Width() != 2 {
		t.Errorf("Width() = %d, want 2", df.Width())
	}
	if df.Height() != 3 {
		t.Errorf("Height() = %d, want 3", df.Height())
	}
}

func TestFromColumnsLengthMismatch(t *testing.T) {
	s1 := NewSeriesF64("x", []float64{1.0, 2.0, 3.0})
	defer s1.Release()
	s2 := NewSeriesF64("y", []float64{4.0, 5.0})
	defer s2.Release()

	df := FromColumns(s1, s2)
	if df != nil {
		t.Error("FromColumns with mismatched lengths should return nil")
	}
}

func TestFromMapF64(t *testing.T) {
	data := map[string][]float64{
		"a": {1.0, 2.0, 3.0},
		"b": {4.0, 5.0, 6.0},
	}

	df := FromMapF64(data)
	if df == nil {
		t.Fatal("FromMapF64 returned nil")
	}

	if df.Width() != 2 {
		t.Errorf("Width() = %d, want 2", df.Width())
	}
	if df.Height() != 3 {
		t.Errorf("Height() = %d, want 3", df.Height())
	}
}

func TestFromMapI64(t *testing.T) {
	data := map[string][]int64{
		"x": {1, 2, 3},
		"y": {10, 20, 30},
	}

	df := FromMapI64(data)
	if df == nil {
		t.Fatal("FromMapI64 returned nil")
	}

	if df.Width() != 2 {
		t.Errorf("Width() = %d, want 2", df.Width())
	}
	if df.Height() != 3 {
		t.Errorf("Height() = %d, want 3", df.Height())
	}
}

// ============================================================================
// Access Tests
// ============================================================================

func TestArrowDataFrameColumn(t *testing.T) {
	s := NewSeriesF64("test", []float64{1.0, 2.0, 3.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	col := df.Column("test")
	if col == nil {
		t.Fatal("Column returned nil for existing column")
	}
	if col.Name() != "test" {
		t.Errorf("Column name = %s, want test", col.Name())
	}

	missing := df.Column("nonexistent")
	if missing != nil {
		t.Error("Column should return nil for nonexistent column")
	}
}

func TestArrowDataFrameColumnNames(t *testing.T) {
	s1 := NewSeriesF64("first", []float64{1.0})
	defer s1.Release()
	s2 := NewSeriesF64("second", []float64{2.0})
	defer s2.Release()
	s3 := NewSeriesF64("third", []float64{3.0})
	defer s3.Release()

	df := NewDataFrame().AddColumn(s1).AddColumn(s2).AddColumn(s3)

	names := df.ColumnNames()
	if len(names) != 3 {
		t.Fatalf("ColumnNames() length = %d, want 3", len(names))
	}
	if names[0] != "first" || names[1] != "second" || names[2] != "third" {
		t.Errorf("ColumnNames() = %v, want [first, second, third]", names)
	}
}

// ============================================================================
// Selection Tests
// ============================================================================

func TestArrowDataFrameSelect(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{3.0, 4.0})
	defer s2.Release()
	s3 := NewSeriesF64("c", []float64{5.0, 6.0})
	defer s3.Release()

	df := NewDataFrame().AddColumn(s1).AddColumn(s2).AddColumn(s3)

	selected := df.Select("a", "c")
	if selected.Width() != 2 {
		t.Errorf("Select Width() = %d, want 2", selected.Width())
	}

	names := selected.ColumnNames()
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Errorf("Select ColumnNames() = %v, want [a, c]", names)
	}

	if selected.Column("b") != nil {
		t.Error("Selected DataFrame should not have column 'b'")
	}
}

func TestArrowDataFrameSelectNonexistent(t *testing.T) {
	s := NewSeriesF64("a", []float64{1.0, 2.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	selected := df.Select("a", "nonexistent")
	if selected.Width() != 1 {
		t.Errorf("Select with nonexistent Width() = %d, want 1", selected.Width())
	}
}

func TestArrowDataFrameDrop(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0})
	defer s1.Release()
	s2 := NewSeriesF64("b", []float64{3.0, 4.0})
	defer s2.Release()
	s3 := NewSeriesF64("c", []float64{5.0, 6.0})
	defer s3.Release()

	df := NewDataFrame().AddColumn(s1).AddColumn(s2).AddColumn(s3)

	dropped := df.Drop("b")
	if dropped.Width() != 2 {
		t.Errorf("Drop Width() = %d, want 2", dropped.Width())
	}

	names := dropped.ColumnNames()
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Errorf("Drop ColumnNames() = %v, want [a, c]", names)
	}
}

// ============================================================================
// Operation Tests
// ============================================================================

func TestArrowDataFrameFilter(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	defer s1.Release()
	s2 := NewSeriesI64("b", []int64{10, 20, 30, 40, 50})
	defer s2.Release()

	df := NewDataFrame().AddColumn(s1).AddColumn(s2)

	mask := []bool{true, false, true, false, true}
	filtered := df.Filter(mask)
	if filtered == nil {
		t.Fatal("Filter returned nil")
	}

	if filtered.Height() != 3 {
		t.Errorf("Filtered Height() = %d, want 3", filtered.Height())
	}

	// Check values in column a
	colA := filtered.Column("a")
	valuesA := colA.ToFloat64()
	expectedA := []float64{1.0, 3.0, 5.0}
	for i, exp := range expectedA {
		if valuesA[i] != exp {
			t.Errorf("Filtered a[%d] = %v, want %v", i, valuesA[i], exp)
		}
	}

	// Check values in column b
	colB := filtered.Column("b")
	valuesB := colB.ToInt64()
	expectedB := []int64{10, 30, 50}
	for i, exp := range expectedB {
		if valuesB[i] != exp {
			t.Errorf("Filtered b[%d] = %v, want %v", i, valuesB[i], exp)
		}
	}
}

func TestArrowDataFrameFilterMaskMismatch(t *testing.T) {
	s := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	mask := []bool{true, false} // Wrong length
	filtered := df.Filter(mask)
	if filtered != nil {
		t.Error("Filter with wrong mask length should return nil")
	}
}

func TestArrowDataFrameSort(t *testing.T) {
	s1 := NewSeriesF64("value", []float64{3.0, 1.0, 4.0, 1.0, 5.0})
	defer s1.Release()
	s2 := NewSeriesI64("id", []int64{1, 2, 3, 4, 5})
	defer s2.Release()

	df := NewDataFrame().AddColumn(s1).AddColumn(s2)

	sorted := df.Sort("value", true)
	if sorted == nil {
		t.Fatal("Sort returned nil")
	}

	// Check values are sorted
	values := sorted.Column("value").ToFloat64()
	expected := []float64{1.0, 1.0, 3.0, 4.0, 5.0}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Sorted value[%d] = %v, want %v", i, values[i], exp)
		}
	}

	// Check ids followed the sort
	ids := sorted.Column("id").ToInt64()
	// Original: value=[3,1,4,1,5], id=[1,2,3,4,5]
	// Sorted ascending: value=[1,1,3,4,5], id should be [2,4,1,3,5] or [4,2,1,3,5]
	// The exact order of equal elements depends on sort stability
	if ids[2] != 1 || ids[3] != 3 || ids[4] != 5 {
		t.Errorf("Sorted ids = %v, expected positions 2,3,4 to be 1,3,5", ids)
	}
}

func TestArrowDataFrameSortDescending(t *testing.T) {
	s := NewSeriesF64("x", []float64{1.0, 3.0, 2.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	sorted := df.Sort("x", false)
	if sorted == nil {
		t.Fatal("Sort descending returned nil")
	}

	values := sorted.Column("x").ToFloat64()
	expected := []float64{3.0, 2.0, 1.0}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Sorted desc[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowDataFrameRename(t *testing.T) {
	s := NewSeriesF64("old_name", []float64{1.0, 2.0, 3.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	renamed := df.Rename("old_name", "new_name")
	if renamed == nil {
		t.Fatal("Rename returned nil")
	}

	if renamed.Column("old_name") != nil {
		t.Error("Renamed DataFrame should not have old column name")
	}

	newCol := renamed.Column("new_name")
	if newCol == nil {
		t.Fatal("Renamed DataFrame should have new column name")
	}
	if newCol.Name() != "new_name" {
		t.Errorf("Renamed column name = %s, want new_name", newCol.Name())
	}

	// Verify values are preserved
	values := newCol.ToFloat64()
	expected := []float64{1.0, 2.0, 3.0}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Renamed values[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowDataFrameWithColumn(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()

	df := NewDataFrame().AddColumn(s1)

	s2 := NewSeriesI64("b", []int64{10, 20, 30})
	defer s2.Release()

	df2 := df.WithColumn(s2)
	if df2 == nil {
		t.Fatal("WithColumn returned nil")
	}

	if df2.Width() != 2 {
		t.Errorf("WithColumn Width() = %d, want 2", df2.Width())
	}

	// Original should be unchanged
	if df.Width() != 1 {
		t.Errorf("Original Width() = %d, want 1", df.Width())
	}
}

func TestArrowDataFrameWithColumnReplace(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()

	df := NewDataFrame().AddColumn(s1)

	s2 := NewSeriesF64("a", []float64{10.0, 20.0, 30.0})
	defer s2.Release()

	df2 := df.WithColumn(s2)
	if df2 == nil {
		t.Fatal("WithColumn replace returned nil")
	}

	if df2.Width() != 1 {
		t.Errorf("WithColumn replace Width() = %d, want 1", df2.Width())
	}

	values := df2.Column("a").ToFloat64()
	if values[0] != 10.0 {
		t.Errorf("WithColumn replace values[0] = %v, want 10.0", values[0])
	}
}

func TestArrowDataFrameWithColumnLengthMismatch(t *testing.T) {
	s1 := NewSeriesF64("a", []float64{1.0, 2.0, 3.0})
	defer s1.Release()

	df := NewDataFrame().AddColumn(s1)

	s2 := NewSeriesF64("b", []float64{1.0, 2.0}) // Wrong length
	defer s2.Release()

	df2 := df.WithColumn(s2)
	if df2 != nil {
		t.Error("WithColumn with wrong length should return nil")
	}
}

func TestArrowDataFrameHead(t *testing.T) {
	s := NewSeriesF64("x", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	head := df.Head(3)
	if head == nil {
		t.Fatal("Head returned nil")
	}

	if head.Height() != 3 {
		t.Errorf("Head Height() = %d, want 3", head.Height())
	}

	values := head.Column("x").ToFloat64()
	expected := []float64{1.0, 2.0, 3.0}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Head[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowDataFrameTail(t *testing.T) {
	s := NewSeriesF64("x", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	tail := df.Tail(2)
	if tail == nil {
		t.Fatal("Tail returned nil")
	}

	if tail.Height() != 2 {
		t.Errorf("Tail Height() = %d, want 2", tail.Height())
	}

	values := tail.Column("x").ToFloat64()
	expected := []float64{4.0, 5.0}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Tail[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowDataFrameSlice(t *testing.T) {
	s := NewSeriesF64("x", []float64{1.0, 2.0, 3.0, 4.0, 5.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	sliced := df.Slice(1, 4)
	if sliced == nil {
		t.Fatal("Slice returned nil")
	}

	if sliced.Height() != 3 {
		t.Errorf("Slice Height() = %d, want 3", sliced.Height())
	}

	values := sliced.Column("x").ToFloat64()
	expected := []float64{2.0, 3.0, 4.0}
	for i, exp := range expected {
		if values[i] != exp {
			t.Errorf("Slice[%d] = %v, want %v", i, values[i], exp)
		}
	}
}

func TestArrowDataFrameClone(t *testing.T) {
	s := NewSeriesF64("x", []float64{1.0, 2.0, 3.0})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)
	clone := df.Clone()

	if clone.Width() != df.Width() {
		t.Errorf("Clone Width() = %d, want %d", clone.Width(), df.Width())
	}
	if clone.Height() != df.Height() {
		t.Errorf("Clone Height() = %d, want %d", clone.Height(), df.Height())
	}

	// Verify column names match
	origNames := df.ColumnNames()
	cloneNames := clone.ColumnNames()
	for i := range origNames {
		if origNames[i] != cloneNames[i] {
			t.Errorf("Clone ColumnNames[%d] = %s, want %s", i, cloneNames[i], origNames[i])
		}
	}
}

// ============================================================================
// Integration Tests
// ============================================================================

func TestArrowDataFrameChainedOperations(t *testing.T) {
	// Create a DataFrame
	values := NewSeriesF64("value", []float64{5.0, 2.0, 8.0, 1.0, 9.0, 3.0})
	defer values.Release()
	ids := NewSeriesI64("id", []int64{1, 2, 3, 4, 5, 6})
	defer ids.Release()

	df := FromColumns(values, ids)
	if df == nil {
		t.Fatal("FromColumns returned nil")
	}

	// Chain: Filter (value > 3) -> Sort by value -> Head(3)
	mask := df.Column("value").GtF64(3.0)
	filtered := df.Filter(mask)
	if filtered == nil {
		t.Fatal("Filter returned nil")
	}

	sorted := filtered.Sort("value", true)
	if sorted == nil {
		t.Fatal("Sort returned nil")
	}

	head := sorted.Head(3)
	if head == nil {
		t.Fatal("Head returned nil")
	}

	// Should have values: 5.0, 8.0, 9.0 (sorted, filtered > 3)
	resultValues := head.Column("value").ToFloat64()
	expected := []float64{5.0, 8.0, 9.0}
	for i, exp := range expected {
		if resultValues[i] != exp {
			t.Errorf("Chained result[%d] = %v, want %v", i, resultValues[i], exp)
		}
	}
}

func TestArrowDataFrameWithNulls(t *testing.T) {
	// Create series with nulls
	s := NewSeriesF64WithNulls("x", []float64{1.0, 2.0, 3.0, 4.0}, []bool{true, false, true, true})
	defer s.Release()

	df := NewDataFrame().AddColumn(s)

	// Filter should preserve nulls
	mask := []bool{true, true, false, true}
	filtered := df.Filter(mask)
	if filtered == nil {
		t.Fatal("Filter with nulls returned nil")
	}

	col := filtered.Column("x")
	// After filter: indices 0, 1, 3 remain
	// Original validity: [true, false, true, true]
	// Result should be: [true, false, true]
	if col.IsValid(0) != true {
		t.Error("Filtered null[0] should be valid")
	}
	if col.IsValid(1) != false {
		t.Error("Filtered null[1] should be null")
	}
	if col.IsValid(2) != true {
		t.Error("Filtered null[2] should be valid")
	}
}

// ============================================================================
// Join Tests
// ============================================================================

func TestInnerJoin(t *testing.T) {
	// Left DataFrame: employees
	empIds := NewSeriesI64("emp_id", []int64{1, 2, 3, 4, 5})
	defer empIds.Release()
	empNames := NewSeriesI64("name_id", []int64{101, 102, 103, 104, 105})
	defer empNames.Release()
	leftDf := FromColumns(empIds, empNames)

	// Right DataFrame: departments
	deptEmpIds := NewSeriesI64("emp_id", []int64{1, 3, 5, 7})
	defer deptEmpIds.Release()
	deptIds := NewSeriesI64("dept_id", []int64{10, 20, 30, 40})
	defer deptIds.Release()
	rightDf := FromColumns(deptEmpIds, deptIds)

	// Inner join on emp_id
	result := InnerJoin(leftDf, rightDf, "emp_id", "emp_id")
	if result == nil {
		t.Fatal("InnerJoin returned nil")
	}

	// Should have 3 matches: emp_id 1, 3, 5
	if result.Height() != 3 {
		t.Errorf("Inner join Height() = %d, want 3", result.Height())
	}

	// Check columns exist
	if result.Column("emp_id") == nil {
		t.Error("Missing emp_id column")
	}
	if result.Column("name_id") == nil {
		t.Error("Missing name_id column")
	}
	if result.Column("emp_id_right") == nil {
		t.Error("Missing emp_id_right column")
	}
	if result.Column("dept_id") == nil {
		t.Error("Missing dept_id column")
	}

	// Check dept_id values (should be 10, 20, 30 for emp_id 1, 3, 5)
	deptValues := result.Column("dept_id").ToInt64()
	expectedDept := []int64{10, 20, 30}
	for i, exp := range expectedDept {
		if deptValues[i] != exp {
			t.Errorf("dept_id[%d] = %v, want %v", i, deptValues[i], exp)
		}
	}
}

func TestInnerJoinNoMatches(t *testing.T) {
	leftIds := NewSeriesI64("id", []int64{1, 2, 3})
	defer leftIds.Release()
	leftDf := NewDataFrame().AddColumn(leftIds)

	rightIds := NewSeriesI64("id", []int64{10, 20, 30})
	defer rightIds.Release()
	rightDf := NewDataFrame().AddColumn(rightIds)

	result := InnerJoin(leftDf, rightDf, "id", "id")
	if result == nil {
		t.Fatal("InnerJoin returned nil")
	}

	if result.Height() != 0 {
		t.Errorf("Inner join with no matches Height() = %d, want 0", result.Height())
	}
}

func TestInnerJoinDuplicates(t *testing.T) {
	// Left has duplicate keys
	leftIds := NewSeriesI64("id", []int64{1, 1, 2, 2})
	defer leftIds.Release()
	leftVals := NewSeriesI64("left_val", []int64{100, 101, 200, 201})
	defer leftVals.Release()
	leftDf := FromColumns(leftIds, leftVals)

	// Right has duplicate keys
	rightIds := NewSeriesI64("id", []int64{1, 2, 2})
	defer rightIds.Release()
	rightVals := NewSeriesI64("right_val", []int64{10, 20, 21})
	defer rightVals.Release()
	rightDf := FromColumns(rightIds, rightVals)

	result := InnerJoin(leftDf, rightDf, "id", "id")
	if result == nil {
		t.Fatal("InnerJoin with duplicates returned nil")
	}

	// Expected matches:
	// (1,100) x (1,10) = 1 match
	// (1,101) x (1,10) = 1 match
	// (2,200) x (2,20), (2,21) = 2 matches
	// (2,201) x (2,20), (2,21) = 2 matches
	// Total: 6 matches
	if result.Height() != 6 {
		t.Errorf("Inner join with duplicates Height() = %d, want 6", result.Height())
	}
}

func TestLeftJoin(t *testing.T) {
	// Left DataFrame: all employees
	empIds := NewSeriesI64("emp_id", []int64{1, 2, 3, 4, 5})
	defer empIds.Release()
	salaries := NewSeriesF64("salary", []float64{50000, 60000, 70000, 80000, 90000})
	defer salaries.Release()
	leftDf := FromColumns(empIds, salaries)

	// Right DataFrame: only some departments
	deptEmpIds := NewSeriesI64("emp_id", []int64{1, 3, 5})
	defer deptEmpIds.Release()
	deptIds := NewSeriesI64("dept_id", []int64{10, 20, 30})
	defer deptIds.Release()
	rightDf := FromColumns(deptEmpIds, deptIds)

	result := LeftJoin(leftDf, rightDf, "emp_id", "emp_id")
	if result == nil {
		t.Fatal("LeftJoin returned nil")
	}

	// Should have 5 rows (all left rows)
	if result.Height() != 5 {
		t.Errorf("Left join Height() = %d, want 5", result.Height())
	}

	// Check that unmatched rows have null dept_id
	deptCol := result.Column("dept_id")
	if deptCol == nil {
		t.Fatal("Missing dept_id column")
	}
	empIdCol := result.Column("emp_id")
	if empIdCol == nil {
		t.Fatal("Missing emp_id column")
	}

	// Check null/valid pattern based on emp_id values
	empIdValues := empIdCol.ToInt64()
	matchingEmpIds := map[int64]bool{1: true, 3: true, 5: true}

	validCount := 0
	nullCount := 0
	for i := 0; i < result.Height(); i++ {
		empId := empIdValues[i]
		if matchingEmpIds[empId] {
			// This emp_id should have a match
			if !deptCol.IsValid(i) {
				t.Errorf("Left join: emp_id=%d should have valid dept_id", empId)
			}
			validCount++
		} else {
			// This emp_id should be null
			if deptCol.IsValid(i) {
				t.Errorf("Left join: emp_id=%d should have null dept_id", empId)
			}
			nullCount++
		}
	}

	if validCount != 3 {
		t.Errorf("Left join: expected 3 valid dept_id, got %d", validCount)
	}
	if nullCount != 2 {
		t.Errorf("Left join: expected 2 null dept_id, got %d", nullCount)
	}
}

func TestLeftJoinAllMatch(t *testing.T) {
	leftIds := NewSeriesI64("id", []int64{1, 2, 3})
	defer leftIds.Release()
	leftDf := NewDataFrame().AddColumn(leftIds)

	rightIds := NewSeriesI64("id", []int64{1, 2, 3})
	defer rightIds.Release()
	rightVals := NewSeriesI64("val", []int64{10, 20, 30})
	defer rightVals.Release()
	rightDf := FromColumns(rightIds, rightVals)

	result := LeftJoin(leftDf, rightDf, "id", "id")
	if result == nil {
		t.Fatal("LeftJoin returned nil")
	}

	if result.Height() != 3 {
		t.Errorf("Left join all match Height() = %d, want 3", result.Height())
	}

	// All rows should be valid
	valCol := result.Column("val")
	for i := 0; i < 3; i++ {
		if !valCol.IsValid(i) {
			t.Errorf("Left join all match: row %d should be valid", i)
		}
	}
}

func TestLeftJoinNoMatch(t *testing.T) {
	leftIds := NewSeriesI64("id", []int64{1, 2, 3})
	defer leftIds.Release()
	leftDf := NewDataFrame().AddColumn(leftIds)

	rightIds := NewSeriesI64("id", []int64{10, 20, 30})
	defer rightIds.Release()
	rightVals := NewSeriesI64("val", []int64{100, 200, 300})
	defer rightVals.Release()
	rightDf := FromColumns(rightIds, rightVals)

	result := LeftJoin(leftDf, rightDf, "id", "id")
	if result == nil {
		t.Fatal("LeftJoin returned nil")
	}

	// All left rows preserved
	if result.Height() != 3 {
		t.Errorf("Left join no match Height() = %d, want 3", result.Height())
	}

	// All right columns should be null
	valCol := result.Column("val")
	for i := 0; i < 3; i++ {
		if valCol.IsValid(i) {
			t.Errorf("Left join no match: row %d should be null", i)
		}
	}
}

func TestArrowJoinInvalidKey(t *testing.T) {
	leftDf := NewDataFrame().AddColumn(NewSeriesI64("a", []int64{1, 2, 3}))
	rightDf := NewDataFrame().AddColumn(NewSeriesI64("b", []int64{1, 2, 3}))

	// Invalid left key
	result := InnerJoin(leftDf, rightDf, "nonexistent", "b")
	if result != nil {
		t.Error("InnerJoin with invalid left key should return nil")
	}

	// Invalid right key
	result = InnerJoin(leftDf, rightDf, "a", "nonexistent")
	if result != nil {
		t.Error("InnerJoin with invalid right key should return nil")
	}
}

func TestArrowJoinTypeMismatch(t *testing.T) {
	// Float64 keys not supported yet
	leftDf := NewDataFrame().AddColumn(NewSeriesF64("a", []float64{1.0, 2.0}))
	rightDf := NewDataFrame().AddColumn(NewSeriesI64("b", []int64{1, 2}))

	result := InnerJoin(leftDf, rightDf, "a", "b")
	if result != nil {
		t.Error("InnerJoin with type mismatch should return nil")
	}
}
