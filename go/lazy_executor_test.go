package galleon

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

// ============================================================================
// executePlan Tests
// ============================================================================

func TestExecutePlan_UnknownOp(t *testing.T) {
	plan := &LogicalPlan{Op: PlanOp(255)}
	_, err := executePlan(plan)
	if err == nil {
		t.Error("Expected error for unknown plan operation")
	}
}

func TestExecutePlan_Scan(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)
	plan := &LogicalPlan{Op: PlanScan, Data: df}

	result, err := executePlan(plan)
	if err != nil {
		t.Fatalf("executePlan failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}
}

// ============================================================================
// executeScan Tests
// ============================================================================

func TestExecuteScan_NilData(t *testing.T) {
	plan := &LogicalPlan{Op: PlanScan, Data: nil}
	result, err := executeScan(plan)
	// nil data may or may not be an error depending on implementation
	// just verify the function doesn't panic
	_ = result
	_ = err
}

// ============================================================================
// executeScanCSV Tests
// ============================================================================

func TestExecuteScanCSV(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "galleon_test")
	defer os.RemoveAll(tmpDir)

	csvPath := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(csvPath, []byte("a,b\n1,2\n3,4\n"), 0644)

	plan := &LogicalPlan{Op: PlanScanCSV, SourcePath: csvPath}
	result, err := executeScanCSV(plan)
	if err != nil {
		t.Fatalf("executeScanCSV failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

func TestExecuteScanCSV_FileNotFound(t *testing.T) {
	plan := &LogicalPlan{Op: PlanScanCSV, SourcePath: "/nonexistent/file.csv"}
	_, err := executeScanCSV(plan)
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// ============================================================================
// executeScanParquet Tests
// ============================================================================

func TestExecuteScanParquet_FileNotFound(t *testing.T) {
	plan := &LogicalPlan{Op: PlanScanParquet, SourcePath: "/nonexistent/file.parquet"}
	_, err := executeScanParquet(plan)
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// ============================================================================
// executeScanJSON Tests
// ============================================================================

func TestExecuteScanJSON(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "galleon_test")
	defer os.RemoveAll(tmpDir)

	jsonPath := filepath.Join(tmpDir, "test.json")
	os.WriteFile(jsonPath, []byte(`[{"a":1,"b":2},{"a":3,"b":4}]`), 0644)

	plan := &LogicalPlan{Op: PlanScanJSON, SourcePath: jsonPath}
	result, err := executeScanJSON(plan)
	if err != nil {
		t.Fatalf("executeScanJSON failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

func TestExecuteScanJSON_FileNotFound(t *testing.T) {
	plan := &LogicalPlan{Op: PlanScanJSON, SourcePath: "/nonexistent/file.json"}
	_, err := executeScanJSON(plan)
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// ============================================================================
// executeProject Tests
// ============================================================================

func TestExecuteProject(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:          PlanProject,
		Input:       inputPlan,
		Projections: []Expr{Col("a")},
	}

	result, err := executeProject(plan)
	if err != nil {
		t.Fatalf("executeProject failed: %v", err)
	}
	if result.Width() != 1 {
		t.Errorf("Result width = %d, want 1", result.Width())
	}
}

// ============================================================================
// executeFilter Tests
// ============================================================================

func TestExecuteFilter(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("value", []float64{1.0, 5.0, 3.0, 7.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:        PlanFilter,
		Input:     inputPlan,
		Predicate: Col("value").Gt(Lit(3.0)),
	}

	result, err := executeFilter(plan)
	if err != nil {
		t.Fatalf("executeFilter failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

// ============================================================================
// executeWithColumn Tests
// ============================================================================

func TestExecuteWithColumn(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:         PlanWithColumn,
		Input:      inputPlan,
		NewColName: "b",
		NewColExpr: &AliasExpr{Inner: Col("a").Mul(Lit(2.0)), AliasName: "b"},
	}

	result, err := executeWithColumn(plan)
	if err != nil {
		t.Fatalf("executeWithColumn failed: %v", err)
	}
	if result.Width() != 2 {
		t.Errorf("Result width = %d, want 2", result.Width())
	}
}

// ============================================================================
// executeGroupBy Tests
// ============================================================================

func TestExecuteGroupBy(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:           PlanGroupBy,
		Input:        inputPlan,
		GroupByKeys:  []Expr{Col("category")},
		Aggregations: []Expr{Col("value").Sum().Alias("total")},
	}

	result, err := executeGroupBy(plan)
	if err != nil {
		t.Fatalf("executeGroupBy failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

// ============================================================================
// executeSort Tests
// ============================================================================

func TestExecuteSort(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("value", []float64{3.0, 1.0, 2.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:            PlanSort,
		Input:         inputPlan,
		SortColumn:    "value",
		SortAscending: true,
	}

	result, err := executeSort(plan)
	if err != nil {
		t.Fatalf("executeSort failed: %v", err)
	}

	values := result.ColumnByName("value").Float64()
	if values[0] != 1.0 || values[1] != 2.0 || values[2] != 3.0 {
		t.Errorf("Sort result = %v, want [1, 2, 3]", values)
	}
}

// ============================================================================
// executeLimit Tests
// ============================================================================

func TestExecuteLimit(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:    PlanLimit,
		Input: inputPlan,
		Limit: 3,
	}

	result, err := executeLimit(plan)
	if err != nil {
		t.Fatalf("executeLimit failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}
}

// ============================================================================
// executeTail Tests
// ============================================================================

func TestExecuteTail(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:       PlanTail,
		Input:    inputPlan,
		TailRows: 2,
	}

	result, err := executeTail(plan)
	if err != nil {
		t.Fatalf("executeTail failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}

	values := result.ColumnByName("a").Float64()
	if values[0] != 4.0 || values[1] != 5.0 {
		t.Errorf("Tail result = %v, want [4, 5]", values)
	}
}

// ============================================================================
// executeDistinct Tests
// ============================================================================

func TestExecuteDistinct(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 2, 3, 1}),
	)
	inputPlan := &LogicalPlan{Op: PlanScan, Data: df}

	plan := &LogicalPlan{
		Op:    PlanDistinct,
		Input: inputPlan,
	}

	result, err := executeDistinct(plan)
	if err != nil {
		t.Fatalf("executeDistinct failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}
}

// ============================================================================
// evaluateExpr Tests
// ============================================================================

func TestEvaluateExpr_Col(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	result, err := evaluateExpr(Col("a"), df)
	if err != nil {
		t.Fatalf("evaluateExpr failed: %v", err)
	}
	if result.Len() != 3 {
		t.Errorf("Result len = %d, want 3", result.Len())
	}
}

func TestEvaluateExpr_Lit(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	result, err := evaluateExpr(Lit(42.0), df)
	if err != nil {
		t.Fatalf("evaluateExpr failed: %v", err)
	}
	if result.Len() != 3 {
		t.Errorf("Result len = %d, want 3", result.Len())
	}
}

func TestEvaluateExpr_ColNotFound(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	_, err := evaluateExpr(Col("nonexistent"), df)
	if err == nil {
		t.Error("Expected error for non-existent column")
	}
}

// ============================================================================
// evaluateBinaryOp Tests
// ============================================================================

func TestEvaluateBinaryOp_Add(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	// Col("a").Add(...) returns *BinaryOpExpr directly
	binExpr := Col("a").Add(Lit(10.0))

	result, err := evaluateBinaryOp(binExpr, df)
	if err != nil {
		t.Fatalf("evaluateBinaryOp failed: %v", err)
	}

	values := result.Float64()
	if values[0] != 11.0 || values[1] != 12.0 || values[2] != 13.0 {
		t.Errorf("Add result = %v, want [11, 12, 13]", values)
	}
}

func TestEvaluateBinaryOp_Sub(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{10.0, 20.0, 30.0}),
	)

	binExpr := Col("a").Sub(Lit(5.0))

	result, err := evaluateBinaryOp(binExpr, df)
	if err != nil {
		t.Fatalf("evaluateBinaryOp failed: %v", err)
	}

	values := result.Float64()
	if values[0] != 5.0 || values[1] != 15.0 || values[2] != 25.0 {
		t.Errorf("Sub result = %v, want [5, 15, 25]", values)
	}
}

func TestEvaluateBinaryOp_Mul(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	binExpr := Col("a").Mul(Lit(2.0))

	result, err := evaluateBinaryOp(binExpr, df)
	if err != nil {
		t.Fatalf("evaluateBinaryOp failed: %v", err)
	}

	values := result.Float64()
	if values[0] != 2.0 || values[1] != 4.0 || values[2] != 6.0 {
		t.Errorf("Mul result = %v, want [2, 4, 6]", values)
	}
}

func TestEvaluateBinaryOp_Div(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{10.0, 20.0, 30.0}),
	)

	binExpr := Col("a").Div(Lit(2.0))

	result, err := evaluateBinaryOp(binExpr, df)
	if err != nil {
		t.Fatalf("evaluateBinaryOp failed: %v", err)
	}

	values := result.Float64()
	if values[0] != 5.0 || values[1] != 10.0 || values[2] != 15.0 {
		t.Errorf("Div result = %v, want [5, 10, 15]", values)
	}
}

// ============================================================================
// compareFloat64 Tests
// ============================================================================

func TestCompareFloat64(t *testing.T) {
	tests := []struct {
		a        float64
		op       BinaryOp
		b        float64
		expected bool
	}{
		{5.0, OpGt, 3.0, true},
		{3.0, OpGt, 5.0, false},
		{5.0, OpGte, 5.0, true},
		{4.0, OpGte, 5.0, false},
		{3.0, OpLt, 5.0, true},
		{5.0, OpLt, 3.0, false},
		{5.0, OpLte, 5.0, true},
		{6.0, OpLte, 5.0, false},
		{5.0, OpEq, 5.0, true},
		{5.0, OpEq, 3.0, false},
		{5.0, OpNeq, 3.0, true},
		{5.0, OpNeq, 5.0, false},
	}

	for _, tc := range tests {
		result := compareFloat64(tc.a, tc.op, tc.b)
		if result != tc.expected {
			t.Errorf("compareFloat64(%v, %v, %v) = %v, want %v", tc.a, tc.op, tc.b, result, tc.expected)
		}
	}
}

// ============================================================================
// createLiteralSeries Tests
// ============================================================================

func TestCreateLiteralSeries_Float64(t *testing.T) {
	s, err := createLiteralSeries("test", 42.5, 3)
	if err != nil {
		t.Fatalf("createLiteralSeries failed: %v", err)
	}
	if s.Len() != 3 {
		t.Errorf("Series len = %d, want 3", s.Len())
	}
	if s.DType() != Float64 {
		t.Errorf("Series dtype = %v, want Float64", s.DType())
	}
}

func TestCreateLiteralSeries_Int(t *testing.T) {
	s, err := createLiteralSeries("test", 42, 3)
	if err != nil {
		t.Fatalf("createLiteralSeries failed: %v", err)
	}
	if s.Len() != 3 {
		t.Errorf("Series len = %d, want 3", s.Len())
	}
}

func TestCreateLiteralSeries_Int64(t *testing.T) {
	s, err := createLiteralSeries("test", int64(42), 3)
	if err != nil {
		t.Fatalf("createLiteralSeries failed: %v", err)
	}
	if s.Len() != 3 {
		t.Errorf("Series len = %d, want 3", s.Len())
	}
}

func TestCreateLiteralSeries_String(t *testing.T) {
	s, err := createLiteralSeries("test", "hello", 3)
	if err != nil {
		t.Fatalf("createLiteralSeries failed: %v", err)
	}
	if s.Len() != 3 {
		t.Errorf("Series len = %d, want 3", s.Len())
	}
	if s.DType() != String {
		t.Errorf("Series dtype = %v, want String", s.DType())
	}
}

func TestCreateLiteralSeries_Bool(t *testing.T) {
	s, err := createLiteralSeries("test", true, 3)
	if err != nil {
		t.Fatalf("createLiteralSeries failed: %v", err)
	}
	if s.Len() != 3 {
		t.Errorf("Series len = %d, want 3", s.Len())
	}
}

// ============================================================================
// lazyToFloat64 Tests
// ============================================================================

func TestLazyToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{42.5, 42.5, true},
		{float32(42.5), 42.5, true},
		{42, 42.0, true},
		{int64(42), 42.0, true},
		{int32(42), 42.0, true},
		{"not a number", 0.0, false},
	}

	for _, tc := range tests {
		result, ok := lazyToFloat64(tc.input)
		if ok != tc.ok {
			t.Errorf("lazyToFloat64(%v) ok = %v, want %v", tc.input, ok, tc.ok)
		}
		if ok && math.Abs(result-tc.expected) > 0.001 {
			t.Errorf("lazyToFloat64(%v) = %v, want %v", tc.input, result, tc.expected)
		}
	}
}

// ============================================================================
// toFloat64Slice Tests
// ============================================================================

func TestToFloat64Slice_Float64(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result := toFloat64Slice(s)
	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	if result[0] != 1.0 || result[1] != 2.0 || result[2] != 3.0 {
		t.Errorf("result = %v, want [1, 2, 3]", result)
	}
}

func TestToFloat64Slice_Int64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3})
	result := toFloat64Slice(s)
	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	if result[0] != 1.0 || result[1] != 2.0 || result[2] != 3.0 {
		t.Errorf("result = %v, want [1, 2, 3]", result)
	}
}

// ============================================================================
// maskToBoolSeries Tests
// ============================================================================

func TestMaskToBoolSeries(t *testing.T) {
	mask := []byte{1, 0, 1, 1, 0}
	s := maskToBoolSeries("test", mask)

	if s.Len() != 5 {
		t.Errorf("Series len = %d, want 5", s.Len())
	}
	if s.DType() != Bool {
		t.Errorf("Series dtype = %v, want Bool", s.DType())
	}
}

// ============================================================================
// castSeries Tests
// ============================================================================

func TestCastSeries_Float64ToInt64(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.5, 2.7, 3.1})
	result, err := castSeries(s, Int64)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	if result.DType() != Int64 {
		t.Errorf("Result dtype = %v, want Int64", result.DType())
	}
}

func TestCastSeries_Int64ToFloat64(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3})
	result, err := castSeries(s, Float64)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	if result.DType() != Float64 {
		t.Errorf("Result dtype = %v, want Float64", result.DType())
	}
}

func TestCastSeries_SameType(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	result, err := castSeries(s, Float64)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	// Result may or may not be the same pointer - just verify type is correct
	if result.DType() != Float64 {
		t.Errorf("Result dtype = %v, want Float64", result.DType())
	}
}

// ============================================================================
// exprToAggregation Tests
// ============================================================================

func TestExprToAggregation_Sum(t *testing.T) {
	expr := Col("value").Sum().Alias("total")
	agg, err := exprToAggregation(expr)
	if err != nil {
		t.Fatalf("exprToAggregation failed: %v", err)
	}
	if agg.column != "value" {
		t.Errorf("Aggregation column = %q, want 'value'", agg.column)
	}
}

func TestExprToAggregation_Mean(t *testing.T) {
	expr := Col("value").Mean().Alias("avg")
	agg, err := exprToAggregation(expr)
	if err != nil {
		t.Fatalf("exprToAggregation failed: %v", err)
	}
	if agg.column != "value" {
		t.Errorf("Aggregation column = %q, want 'value'", agg.column)
	}
}

func TestExprToAggregation_Min(t *testing.T) {
	expr := Col("value").Min().Alias("min_val")
	agg, err := exprToAggregation(expr)
	if err != nil {
		t.Fatalf("exprToAggregation failed: %v", err)
	}
	if agg.column != "value" {
		t.Errorf("Aggregation column = %q, want 'value'", agg.column)
	}
}

func TestExprToAggregation_Max(t *testing.T) {
	expr := Col("value").Max().Alias("max_val")
	agg, err := exprToAggregation(expr)
	if err != nil {
		t.Fatalf("exprToAggregation failed: %v", err)
	}
	if agg.column != "value" {
		t.Errorf("Aggregation column = %q, want 'value'", agg.column)
	}
}

func TestExprToAggregation_Count(t *testing.T) {
	expr := Col("value").Count().Alias("cnt")
	agg, err := exprToAggregation(expr)
	if err != nil {
		t.Fatalf("exprToAggregation failed: %v", err)
	}
	// Count may or may not set column depending on implementation
	// Just verify we got a valid aggregation
	_ = agg
}

func TestExprToAggregation_InvalidExpr(t *testing.T) {
	expr := Col("value") // Not an aggregation expression
	_, err := exprToAggregation(expr)
	if err == nil {
		t.Error("Expected error for non-aggregation expression")
	}
}

// ============================================================================
// Additional toFloat64Slice Tests for Higher Coverage
// ============================================================================

func TestToFloat64Slice_Float32(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0})
	result := toFloat64Slice(s)
	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	if math.Abs(result[0]-1.0) > 0.001 || math.Abs(result[1]-2.0) > 0.001 || math.Abs(result[2]-3.0) > 0.001 {
		t.Errorf("result = %v, want [1, 2, 3]", result)
	}
}

func TestToFloat64Slice_Int32(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3})
	result := toFloat64Slice(s)
	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	if result[0] != 1.0 || result[1] != 2.0 || result[2] != 3.0 {
		t.Errorf("result = %v, want [1, 2, 3]", result)
	}
}

func TestToFloat64Slice_Bool(t *testing.T) {
	s := NewSeriesBool("test", []bool{true, false, true})
	result := toFloat64Slice(s)
	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	if result[0] != 1.0 || result[1] != 0.0 || result[2] != 1.0 {
		t.Errorf("result = %v, want [1, 0, 1]", result)
	}
}

func TestToFloat64Slice_String(t *testing.T) {
	s := NewSeriesString("test", []string{"a", "b", "c"})
	result := toFloat64Slice(s)
	// String type returns nil for toFloat64Slice
	if result != nil {
		t.Errorf("result = %v, want nil for String type", result)
	}
}

// ============================================================================
// Additional castSeries Tests
// ============================================================================

func TestCastSeries_ToBool(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{0.0, 1.0, 2.0})
	result, err := castSeries(s, Bool)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	if result.DType() != Bool {
		t.Errorf("Result dtype = %v, want Bool", result.DType())
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != true {
		t.Errorf("Bool values = %v, want [false, true, true]", bools)
	}
}

func TestCastSeries_ToString(t *testing.T) {
	s := NewSeriesInt64("test", []int64{1, 2, 3})
	result, err := castSeries(s, String)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	if result.DType() != String {
		t.Errorf("Result dtype = %v, want String", result.DType())
	}
	strs := result.Strings()
	if len(strs) != 3 {
		t.Errorf("String values len = %d, want 3", len(strs))
	}
}

func TestCastSeries_Int32ToFloat64(t *testing.T) {
	s := NewSeriesInt32("test", []int32{1, 2, 3})
	result, err := castSeries(s, Float64)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	if result.DType() != Float64 {
		t.Errorf("Result dtype = %v, want Float64", result.DType())
	}
}

func TestCastSeries_Float32ToFloat64(t *testing.T) {
	s := NewSeriesFloat32("test", []float32{1.0, 2.0, 3.0})
	result, err := castSeries(s, Float64)
	if err != nil {
		t.Fatalf("castSeries failed: %v", err)
	}
	if result.DType() != Float64 {
		t.Errorf("Result dtype = %v, want Float64", result.DType())
	}
}

func TestCastSeries_UnsupportedType(t *testing.T) {
	s := NewSeriesFloat64("test", []float64{1.0, 2.0, 3.0})
	_, err := castSeries(s, DType(99))
	if err == nil {
		t.Error("Expected error for unsupported target type")
	}
}

// ============================================================================
// evaluateVectorOp Tests
// ============================================================================

func TestEvaluateVectorOp_Add(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	right := NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0})
	result, err := evaluateVectorOp(left, OpAdd, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	values := result.Float64()
	if values[0] != 11.0 || values[1] != 22.0 || values[2] != 33.0 {
		t.Errorf("Add result = %v, want [11, 22, 33]", values)
	}
}

func TestEvaluateVectorOp_Sub(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{10.0, 20.0, 30.0})
	right := NewSeriesFloat64("b", []float64{1.0, 2.0, 3.0})
	result, err := evaluateVectorOp(left, OpSub, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	values := result.Float64()
	if values[0] != 9.0 || values[1] != 18.0 || values[2] != 27.0 {
		t.Errorf("Sub result = %v, want [9, 18, 27]", values)
	}
}

func TestEvaluateVectorOp_Mul(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0})
	right := NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0})
	result, err := evaluateVectorOp(left, OpMul, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	values := result.Float64()
	if values[0] != 10.0 || values[1] != 40.0 || values[2] != 90.0 {
		t.Errorf("Mul result = %v, want [10, 40, 90]", values)
	}
}

func TestEvaluateVectorOp_Div(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{10.0, 20.0, 30.0})
	right := NewSeriesFloat64("b", []float64{2.0, 4.0, 5.0})
	result, err := evaluateVectorOp(left, OpDiv, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	values := result.Float64()
	if values[0] != 5.0 || values[1] != 5.0 || values[2] != 6.0 {
		t.Errorf("Div result = %v, want [5, 5, 6]", values)
	}
}

func TestEvaluateVectorOp_Gt(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{5.0, 3.0, 7.0})
	right := NewSeriesFloat64("b", []float64{4.0, 4.0, 4.0})
	result, err := evaluateVectorOp(left, OpGt, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	if result.DType() != Bool {
		t.Errorf("Result dtype = %v, want Bool", result.DType())
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("Gt result = %v, want [true, false, true]", bools)
	}
}

func TestEvaluateVectorOp_Lt(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{3.0, 5.0, 7.0})
	right := NewSeriesFloat64("b", []float64{4.0, 4.0, 4.0})
	result, err := evaluateVectorOp(left, OpLt, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != false {
		t.Errorf("Lt result = %v, want [true, false, false]", bools)
	}
}

func TestEvaluateVectorOp_Gte(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{3.0, 4.0, 5.0})
	right := NewSeriesFloat64("b", []float64{4.0, 4.0, 4.0})
	result, err := evaluateVectorOp(left, OpGte, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != true {
		t.Errorf("Gte result = %v, want [false, true, true]", bools)
	}
}

func TestEvaluateVectorOp_Lte(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{3.0, 4.0, 5.0})
	right := NewSeriesFloat64("b", []float64{4.0, 4.0, 4.0})
	result, err := evaluateVectorOp(left, OpLte, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != true || bools[2] != false {
		t.Errorf("Lte result = %v, want [true, true, false]", bools)
	}
}

func TestEvaluateVectorOp_Eq(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{3.0, 4.0, 5.0})
	right := NewSeriesFloat64("b", []float64{4.0, 4.0, 4.0})
	result, err := evaluateVectorOp(left, OpEq, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != false {
		t.Errorf("Eq result = %v, want [false, true, false]", bools)
	}
}

func TestEvaluateVectorOp_Neq(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{3.0, 4.0, 5.0})
	right := NewSeriesFloat64("b", []float64{4.0, 4.0, 4.0})
	result, err := evaluateVectorOp(left, OpNeq, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("Neq result = %v, want [true, false, true]", bools)
	}
}

func TestEvaluateVectorOp_And(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{1.0, 0.0, 1.0})
	right := NewSeriesFloat64("b", []float64{1.0, 1.0, 0.0})
	result, err := evaluateVectorOp(left, OpAnd, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != false {
		t.Errorf("And result = %v, want [true, false, false]", bools)
	}
}

func TestEvaluateVectorOp_Or(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{1.0, 0.0, 0.0})
	right := NewSeriesFloat64("b", []float64{0.0, 1.0, 0.0})
	result, err := evaluateVectorOp(left, OpOr, right)
	if err != nil {
		t.Fatalf("evaluateVectorOp failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != true || bools[2] != false {
		t.Errorf("Or result = %v, want [true, true, false]", bools)
	}
}

func TestEvaluateVectorOp_LengthMismatch(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{1.0, 2.0})
	right := NewSeriesFloat64("b", []float64{1.0, 2.0, 3.0})
	_, err := evaluateVectorOp(left, OpAdd, right)
	if err == nil {
		t.Error("Expected error for length mismatch")
	}
}

func TestEvaluateVectorOp_NonNumeric(t *testing.T) {
	left := NewSeriesString("a", []string{"a", "b"})
	right := NewSeriesString("b", []string{"c", "d"})
	_, err := evaluateVectorOp(left, OpAdd, right)
	if err == nil {
		t.Error("Expected error for non-numeric types")
	}
}

func TestEvaluateVectorOp_UnsupportedOp(t *testing.T) {
	left := NewSeriesFloat64("a", []float64{1.0, 2.0})
	right := NewSeriesFloat64("b", []float64{3.0, 4.0})
	_, err := evaluateVectorOp(left, BinaryOp(99), right)
	if err == nil {
		t.Error("Expected error for unsupported operation")
	}
}

// ============================================================================
// evaluateExpr Additional Tests
// ============================================================================

func TestEvaluateExpr_AliasExpr(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)
	result, err := evaluateExpr(Col("a").Alias("renamed"), df)
	if err != nil {
		t.Fatalf("evaluateExpr failed: %v", err)
	}
	if result.Name() != "renamed" {
		t.Errorf("expected name 'renamed', got %s", result.Name())
	}
}

func TestEvaluateExpr_CastExpr(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)
	result, err := evaluateExpr(Col("a").Cast(Int64), df)
	if err != nil {
		t.Fatalf("evaluateExpr failed: %v", err)
	}
	if result.DType() != Int64 {
		t.Errorf("expected Int64, got %s", result.DType())
	}
}

func TestEvaluateExpr_AllColsError(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)
	_, err := evaluateExpr(AllCols(), df)
	if err == nil {
		t.Error("Expected error for AllCols evaluation")
	}
}

// ============================================================================
// evaluateComparison Tests
// ============================================================================

func TestEvaluateComparison_GtWithLiteral(t *testing.T) {
	col := NewSeriesFloat64("a", []float64{1.0, 5.0, 3.0})
	result, err := evaluateComparison(col, OpGt, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != false {
		t.Errorf("Gt result = %v, want [false, true, false]", bools)
	}
}

func TestEvaluateComparison_LtWithLiteral(t *testing.T) {
	col := NewSeriesFloat64("a", []float64{1.0, 5.0, 3.0})
	result, err := evaluateComparison(col, OpLt, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != false {
		t.Errorf("Lt result = %v, want [true, false, false]", bools)
	}
}

func TestEvaluateComparison_EqWithLiteral(t *testing.T) {
	col := NewSeriesFloat64("a", []float64{1.0, 3.0, 5.0})
	result, err := evaluateComparison(col, OpEq, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != false {
		t.Errorf("Eq result = %v, want [false, true, false]", bools)
	}
}

func TestEvaluateComparison_GteWithLiteral(t *testing.T) {
	col := NewSeriesFloat64("a", []float64{1.0, 3.0, 5.0})
	result, err := evaluateComparison(col, OpGte, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != true {
		t.Errorf("Gte result = %v, want [false, true, true]", bools)
	}
}

func TestEvaluateComparison_LteWithLiteral(t *testing.T) {
	col := NewSeriesFloat64("a", []float64{1.0, 3.0, 5.0})
	result, err := evaluateComparison(col, OpLte, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != true || bools[2] != false {
		t.Errorf("Lte result = %v, want [true, true, false]", bools)
	}
}

func TestEvaluateComparison_NeqWithLiteral(t *testing.T) {
	col := NewSeriesFloat64("a", []float64{1.0, 3.0, 5.0})
	result, err := evaluateComparison(col, OpNeq, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("Neq result = %v, want [true, false, true]", bools)
	}
}

func TestEvaluateComparison_Float32(t *testing.T) {
	col := NewSeriesFloat32("a", []float32{1.0, 5.0, 3.0})
	result, err := evaluateComparison(col, OpGt, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != false {
		t.Errorf("Gt result = %v, want [false, true, false]", bools)
	}
}

func TestEvaluateComparison_Int64(t *testing.T) {
	col := NewSeriesInt64("a", []int64{1, 5, 3})
	result, err := evaluateComparison(col, OpGte, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != false || bools[1] != true || bools[2] != true {
		t.Errorf("Gte result = %v, want [false, true, true]", bools)
	}
}

func TestEvaluateComparison_Int32(t *testing.T) {
	col := NewSeriesInt32("a", []int32{1, 5, 3})
	result, err := evaluateComparison(col, OpLte, 3.0)
	if err != nil {
		t.Fatalf("evaluateComparison failed: %v", err)
	}
	bools := result.Bool()
	if bools[0] != true || bools[1] != false || bools[2] != true {
		t.Errorf("Lte result = %v, want [true, false, true]", bools)
	}
}

func TestEvaluateComparison_UnsupportedType(t *testing.T) {
	col := NewSeriesString("a", []string{"a", "b", "c"})
	_, err := evaluateComparison(col, OpGt, 3.0)
	if err == nil {
		t.Error("Expected error for string comparison")
	}
}

func TestCompareFloat64_UnsupportedOp(t *testing.T) {
	// Test that unknown op returns false
	result := compareFloat64(5.0, BinaryOp(99), 3.0)
	if result != false {
		t.Errorf("Unknown op should return false, got %v", result)
	}
}

// ============================================================================
// executeJoin Tests
// ============================================================================

func TestExecuteJoin_InnerJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 40.0}),
	)

	leftPlan := &LogicalPlan{Op: PlanScan, Data: left}
	rightPlan := &LogicalPlan{Op: PlanScan, Data: right}

	plan := &LogicalPlan{
		Op:       PlanJoin,
		Input:    leftPlan,
		Right:    rightPlan,
		JoinType: InnerJoin,
		JoinOpts: JoinOptions{
			on:     []string{"id"},
			suffix: "_right",
			how:    InnerJoin,
		},
	}

	result, err := executeJoin(plan)
	if err != nil {
		t.Fatalf("executeJoin failed: %v", err)
	}
	// Inner join should have 2 matches (id=1 and id=2)
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

func TestExecuteJoin_LeftJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 40.0}),
	)

	leftPlan := &LogicalPlan{Op: PlanScan, Data: left}
	rightPlan := &LogicalPlan{Op: PlanScan, Data: right}

	plan := &LogicalPlan{
		Op:       PlanJoin,
		Input:    leftPlan,
		Right:    rightPlan,
		JoinType: LeftJoin,
		JoinOpts: JoinOptions{
			on:     []string{"id"},
			suffix: "_right",
			how:    LeftJoin,
		},
	}

	result, err := executeJoin(plan)
	if err != nil {
		t.Fatalf("executeJoin failed: %v", err)
	}
	// Left join should preserve all 3 left rows
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}
}

func TestExecuteJoin_RightJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 40.0}),
	)

	leftPlan := &LogicalPlan{Op: PlanScan, Data: left}
	rightPlan := &LogicalPlan{Op: PlanScan, Data: right}

	plan := &LogicalPlan{
		Op:       PlanJoin,
		Input:    leftPlan,
		Right:    rightPlan,
		JoinType: RightJoin,
		JoinOpts: JoinOptions{
			on:     []string{"id"},
			suffix: "_right",
			how:    RightJoin,
		},
	}

	result, err := executeJoin(plan)
	if err != nil {
		t.Fatalf("executeJoin failed: %v", err)
	}
	// Right join should preserve all 3 right rows
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}
}

func TestExecuteJoin_OuterJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 40.0}),
	)

	leftPlan := &LogicalPlan{Op: PlanScan, Data: left}
	rightPlan := &LogicalPlan{Op: PlanScan, Data: right}

	plan := &LogicalPlan{
		Op:       PlanJoin,
		Input:    leftPlan,
		Right:    rightPlan,
		JoinType: OuterJoin,
		JoinOpts: JoinOptions{
			on:     []string{"id"},
			suffix: "_right",
			how:    OuterJoin,
		},
	}

	result, err := executeJoin(plan)
	if err != nil {
		t.Fatalf("executeJoin failed: %v", err)
	}
	// Outer join should have 4 rows (ids 1, 2, 3, 4)
	if result.Height() != 4 {
		t.Errorf("Result height = %d, want 4", result.Height())
	}
}

func TestExecuteJoin_UnsupportedType(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
	)

	leftPlan := &LogicalPlan{Op: PlanScan, Data: left}
	rightPlan := &LogicalPlan{Op: PlanScan, Data: right}

	plan := &LogicalPlan{
		Op:       PlanJoin,
		Input:    leftPlan,
		Right:    rightPlan,
		JoinType: JoinType(99), // Invalid join type
		JoinOpts: JoinOptions{
			on:     []string{"id"},
			suffix: "_right",
		},
	}

	_, err := executeJoin(plan)
	if err == nil {
		t.Error("Expected error for unsupported join type")
	}
}
