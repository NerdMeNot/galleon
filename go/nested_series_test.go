package galleon

import (
	"math"
	"testing"
)

// ============================================================================
// StructSeries Tests
// ============================================================================

func TestNewStructSeries(t *testing.T) {
	fields := map[string]*Series{
		"x": NewSeriesFloat64("x", []float64{1.0, 2.0, 3.0}),
		"y": NewSeriesInt64("y", []int64{10, 20, 30}),
	}

	ss, err := NewStructSeries("point", fields)
	if err != nil {
		t.Fatalf("NewStructSeries failed: %v", err)
	}

	if ss.Name() != "point" {
		t.Errorf("Name() = %v, want %v", ss.Name(), "point")
	}

	if ss.DType() != Struct {
		t.Errorf("DType() = %v, want %v", ss.DType(), Struct)
	}

	if ss.Len() != 3 {
		t.Errorf("Len() = %v, want %v", ss.Len(), 3)
	}
}

func TestStructSeriesField(t *testing.T) {
	fields := map[string]*Series{
		"a": NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		"b": NewSeriesString("b", []string{"x", "y", "z"}),
	}

	ss, err := NewStructSeries("data", fields)
	if err != nil {
		t.Fatalf("NewStructSeries failed: %v", err)
	}

	// Test Field access
	fieldA := ss.Field("a")
	if fieldA == nil {
		t.Fatal("Field('a') returned nil")
	}
	if fieldA.DType() != Float64 {
		t.Errorf("Field('a').DType() = %v, want %v", fieldA.DType(), Float64)
	}

	fieldB := ss.Field("b")
	if fieldB == nil {
		t.Fatal("Field('b') returned nil")
	}
	if fieldB.DType() != String {
		t.Errorf("Field('b').DType() = %v, want %v", fieldB.DType(), String)
	}

	// Test non-existent field
	fieldC := ss.Field("c")
	if fieldC != nil {
		t.Error("Field('c') should return nil for non-existent field")
	}
}

func TestStructSeriesGetRow(t *testing.T) {
	fields := map[string]*Series{
		"name": NewSeriesString("name", []string{"Alice", "Bob"}),
		"age":  NewSeriesInt64("age", []int64{30, 25}),
	}

	ss, err := NewStructSeries("person", fields)
	if err != nil {
		t.Fatalf("NewStructSeries failed: %v", err)
	}

	row0 := ss.GetRow(0)
	if row0["name"] != "Alice" {
		t.Errorf("GetRow(0)['name'] = %v, want 'Alice'", row0["name"])
	}
	if row0["age"] != int64(30) {
		t.Errorf("GetRow(0)['age'] = %v, want 30", row0["age"])
	}

	row1 := ss.GetRow(1)
	if row1["name"] != "Bob" {
		t.Errorf("GetRow(1)['name'] = %v, want 'Bob'", row1["name"])
	}
}

func TestStructSeriesUnnest(t *testing.T) {
	fields := map[string]*Series{
		"x": NewSeriesFloat64("x", []float64{1.0, 2.0}),
		"y": NewSeriesFloat64("y", []float64{3.0, 4.0}),
	}

	ss, err := NewStructSeries("coords", fields)
	if err != nil {
		t.Fatalf("NewStructSeries failed: %v", err)
	}

	unnested := ss.Unnest()
	if len(unnested) != 2 {
		t.Errorf("Unnest() returned %d fields, want 2", len(unnested))
	}

	if unnested["x"] == nil || unnested["y"] == nil {
		t.Error("Unnest() should contain 'x' and 'y' fields")
	}
}

func TestStructSeriesMismatchedLengths(t *testing.T) {
	fields := map[string]*Series{
		"a": NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		"b": NewSeriesFloat64("b", []float64{1.0, 2.0}), // Different length
	}

	_, err := NewStructSeries("bad", fields)
	if err == nil {
		t.Error("NewStructSeries should fail with mismatched field lengths")
	}
}

// ============================================================================
// ListSeries Tests
// ============================================================================

func TestNewListSeriesFromSlicesF64(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0},
		{4.0, 5.0},
		{6.0},
		{},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	if ls.Name() != "values" {
		t.Errorf("Name() = %v, want %v", ls.Name(), "values")
	}

	if ls.DType() != List {
		t.Errorf("DType() = %v, want %v", ls.DType(), List)
	}

	if ls.Len() != 4 {
		t.Errorf("Len() = %v, want %v", ls.Len(), 4)
	}

	if ls.ElementType() != Float64 {
		t.Errorf("ElementType() = %v, want %v", ls.ElementType(), Float64)
	}
}

func TestListSeriesGetListLen(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0},
		{4.0, 5.0},
		{6.0},
		{},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	expected := []int{3, 2, 1, 0}
	for i, exp := range expected {
		if ls.GetListLen(i) != exp {
			t.Errorf("GetListLen(%d) = %v, want %v", i, ls.GetListLen(i), exp)
		}
	}
}

func TestListSeriesGetListF64(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0},
		{4.0, 5.0},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	list0 := ls.GetListF64(0)
	if len(list0) != 3 {
		t.Fatalf("GetListF64(0) length = %v, want 3", len(list0))
	}
	for i, exp := range []float64{1.0, 2.0, 3.0} {
		if list0[i] != exp {
			t.Errorf("GetListF64(0)[%d] = %v, want %v", i, list0[i], exp)
		}
	}

	list1 := ls.GetListF64(1)
	if len(list1) != 2 {
		t.Fatalf("GetListF64(1) length = %v, want 2", len(list1))
	}
	for i, exp := range []float64{4.0, 5.0} {
		if list1[i] != exp {
			t.Errorf("GetListF64(1)[%d] = %v, want %v", i, list1[i], exp)
		}
	}
}

func TestListSeriesGetElement(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0},
		{4.0, 5.0},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	if ls.GetElement(0, 0).(float64) != 1.0 {
		t.Errorf("GetElement(0, 0) = %v, want 1.0", ls.GetElement(0, 0))
	}
	if ls.GetElement(0, 2).(float64) != 3.0 {
		t.Errorf("GetElement(0, 2) = %v, want 3.0", ls.GetElement(0, 2))
	}
	if ls.GetElement(1, 0).(float64) != 4.0 {
		t.Errorf("GetElement(1, 0) = %v, want 4.0", ls.GetElement(1, 0))
	}

	// Out of bounds
	if ls.GetElement(0, 5) != nil {
		t.Error("GetElement(0, 5) should return nil for out of bounds")
	}
}

func TestListSeriesExplode(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0},
		{3.0, 4.0, 5.0},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	exploded, rowIndices := ls.Explode()

	if exploded.Len() != 5 {
		t.Errorf("Exploded length = %v, want 5", exploded.Len())
	}

	expectedValues := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	explodedData := exploded.Float64()
	for i, exp := range expectedValues {
		if explodedData[i] != exp {
			t.Errorf("Exploded[%d] = %v, want %v", i, explodedData[i], exp)
		}
	}

	expectedRowIndices := []int32{0, 0, 1, 1, 1}
	for i, exp := range expectedRowIndices {
		if rowIndices[i] != exp {
			t.Errorf("RowIndices[%d] = %v, want %v", i, rowIndices[i], exp)
		}
	}
}

func TestListSeriesListLengths(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0},
		{4.0, 5.0},
		{},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	lengths := ls.ListLengths()
	if lengths.Len() != 3 {
		t.Fatalf("ListLengths length = %v, want 3", lengths.Len())
	}

	lengthData := lengths.Int32()
	expected := []int32{3, 2, 0}
	for i, exp := range expected {
		if lengthData[i] != exp {
			t.Errorf("ListLengths[%d] = %v, want %v", i, lengthData[i], exp)
		}
	}
}

func TestListSeriesListSum(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0}, // sum = 6
		{4.0, 5.0},      // sum = 9
		{10.0},          // sum = 10
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	sums := ls.ListSum()
	if sums.Len() != 3 {
		t.Fatalf("ListSum length = %v, want 3", sums.Len())
	}

	sumData := sums.Float64()
	expected := []float64{6.0, 9.0, 10.0}
	for i, exp := range expected {
		if sumData[i] != exp {
			t.Errorf("ListSum[%d] = %v, want %v", i, sumData[i], exp)
		}
	}
}

func TestListSeriesListMean(t *testing.T) {
	data := [][]float64{
		{1.0, 2.0, 3.0}, // mean = 2
		{4.0, 6.0},      // mean = 5
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	means := ls.ListMean()
	meanData := means.Float64()

	if math.Abs(meanData[0]-2.0) > 0.001 {
		t.Errorf("ListMean[0] = %v, want 2.0", meanData[0])
	}
	if math.Abs(meanData[1]-5.0) > 0.001 {
		t.Errorf("ListMean[1] = %v, want 5.0", meanData[1])
	}
}

func TestListSeriesListMinMax(t *testing.T) {
	data := [][]float64{
		{3.0, 1.0, 2.0},
		{5.0, 9.0, 7.0},
	}

	ls := NewListSeriesFromSlicesF64("values", data)

	mins := ls.ListMin()
	minData := mins.Float64()
	if minData[0] != 1.0 {
		t.Errorf("ListMin[0] = %v, want 1.0", minData[0])
	}
	if minData[1] != 5.0 {
		t.Errorf("ListMin[1] = %v, want 5.0", minData[1])
	}

	maxs := ls.ListMax()
	maxData := maxs.Float64()
	if maxData[0] != 3.0 {
		t.Errorf("ListMax[0] = %v, want 3.0", maxData[0])
	}
	if maxData[1] != 9.0 {
		t.Errorf("ListMax[1] = %v, want 9.0", maxData[1])
	}
}

func TestNewListSeriesFromSlicesI64(t *testing.T) {
	data := [][]int64{
		{1, 2, 3},
		{4, 5},
	}

	ls := NewListSeriesFromSlicesI64("ids", data)

	if ls.ElementType() != Int64 {
		t.Errorf("ElementType() = %v, want %v", ls.ElementType(), Int64)
	}

	list0 := ls.GetListI64(0)
	if len(list0) != 3 {
		t.Fatalf("GetListI64(0) length = %v, want 3", len(list0))
	}
	for i, exp := range []int64{1, 2, 3} {
		if list0[i] != exp {
			t.Errorf("GetListI64(0)[%d] = %v, want %v", i, list0[i], exp)
		}
	}
}

func TestNewListSeriesFromSlicesString(t *testing.T) {
	data := [][]string{
		{"a", "b"},
		{"c", "d", "e"},
	}

	ls := NewListSeriesFromSlicesString("tags", data)

	if ls.ElementType() != String {
		t.Errorf("ElementType() = %v, want %v", ls.ElementType(), String)
	}

	if ls.GetListLen(0) != 2 {
		t.Errorf("GetListLen(0) = %v, want 2", ls.GetListLen(0))
	}
	if ls.GetListLen(1) != 3 {
		t.Errorf("GetListLen(1) = %v, want 3", ls.GetListLen(1))
	}
}

// ============================================================================
// DType Tests for Nested Types
// ============================================================================

func TestDTypeIsNested(t *testing.T) {
	nestedTypes := []DType{Struct, List, Array}
	for _, dtype := range nestedTypes {
		if !dtype.IsNested() {
			t.Errorf("%v.IsNested() = false, want true", dtype)
		}
	}

	nonNestedTypes := []DType{Float64, Int64, String, Bool}
	for _, dtype := range nonNestedTypes {
		if dtype.IsNested() {
			t.Errorf("%v.IsNested() = true, want false", dtype)
		}
	}
}

func TestStructType(t *testing.T) {
	st := NewStructType([]StructField{
		{Name: "x", DType: Float64},
		{Name: "y", DType: Int64},
	})

	if st.NumFields() != 2 {
		t.Errorf("NumFields() = %v, want 2", st.NumFields())
	}

	field, ok := st.GetField("x")
	if !ok || field.DType != Float64 {
		t.Error("GetField('x') should return Float64 field")
	}

	idx, ok := st.GetFieldIndex("y")
	if !ok || idx != 1 {
		t.Errorf("GetFieldIndex('y') = %v, want 1", idx)
	}
}

func TestListType(t *testing.T) {
	lt := NewListType(Float64)

	if lt.ElementType != Float64 {
		t.Errorf("ElementType = %v, want %v", lt.ElementType, Float64)
	}

	if lt.String() != "List[Float64]" {
		t.Errorf("String() = %v, want 'List[Float64]'", lt.String())
	}
}

// ============================================================================
// Expression Tests for Struct/List
// ============================================================================

func TestStructFieldExpr(t *testing.T) {
	expr := Col("point").Field("x")

	if expr.String() != `col("point").field("x")` {
		t.Errorf("String() = %v, want col(\"point\").field(\"x\")", expr.String())
	}

	cloned := expr.Clone().(*StructFieldExpr)
	if cloned.FieldName != "x" {
		t.Errorf("Clone().FieldName = %v, want 'x'", cloned.FieldName)
	}
}

func TestStructOfExpr(t *testing.T) {
	expr := StructOf(map[string]Expr{
		"a": Col("x"),
		"b": Lit(10),
	})

	if len(expr.Fields) != 2 {
		t.Errorf("len(Fields) = %v, want 2", len(expr.Fields))
	}
}

func TestListNamespace(t *testing.T) {
	col := Col("values")

	getExpr := col.List().Get(0)
	if getExpr.Index != 0 {
		t.Errorf("List().Get(0).Index = %v, want 0", getExpr.Index)
	}

	lenExpr := col.List().Len()
	if lenExpr.String() != `col("values").list.len()` {
		t.Errorf("List().Len().String() = %v", lenExpr.String())
	}

	sumExpr := col.List().Sum()
	if sumExpr.String() != `col("values").list.sum()` {
		t.Errorf("List().Sum().String() = %v", sumExpr.String())
	}
}

func TestExplodeExpr(t *testing.T) {
	expr := Col("list_col").Explode()

	if expr.String() != `col("list_col").explode()` {
		t.Errorf("String() = %v, want col(\"list_col\").explode()", expr.String())
	}
}
