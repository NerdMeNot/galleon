package galleon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ============================================================================
// LazyFrame Creation Tests
// ============================================================================

func TestLazyFrame_Lazy(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesInt64("b", []int64{10, 20, 30}),
	)

	lf := df.Lazy()
	if lf == nil {
		t.Fatal("Lazy() returned nil")
	}
	if lf.plan == nil {
		t.Fatal("LazyFrame.plan is nil")
	}
	if lf.plan.Op != PlanScan {
		t.Errorf("plan.Op = %v, want PlanScan", lf.plan.Op)
	}
}

func TestLazyFrame_ScanCSV(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "galleon_test")
	defer os.RemoveAll(tmpDir)

	csvPath := filepath.Join(tmpDir, "test.csv")
	os.WriteFile(csvPath, []byte("a,b\n1,2\n3,4\n"), 0644)

	lf := ScanCSV(csvPath)
	if lf == nil {
		t.Fatal("ScanCSV returned nil")
	}
	if lf.plan.Op != PlanScanCSV {
		t.Errorf("plan.Op = %v, want PlanScanCSV", lf.plan.Op)
	}
	if lf.plan.SourcePath != csvPath {
		t.Errorf("plan.SourcePath = %q, want %q", lf.plan.SourcePath, csvPath)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

func TestLazyFrame_ScanParquet(t *testing.T) {
	lf := ScanParquet("/path/to/file.parquet")
	if lf == nil {
		t.Fatal("ScanParquet returned nil")
	}
	if lf.plan.Op != PlanScanParquet {
		t.Errorf("plan.Op = %v, want PlanScanParquet", lf.plan.Op)
	}
}

func TestLazyFrame_ScanJSON(t *testing.T) {
	lf := ScanJSON("/path/to/file.json")
	if lf == nil {
		t.Fatal("ScanJSON returned nil")
	}
	if lf.plan.Op != PlanScanJSON {
		t.Errorf("plan.Op = %v, want PlanScanJSON", lf.plan.Op)
	}
}

// ============================================================================
// LazyFrame Operations Tests
// ============================================================================

func TestLazyFrame_Select(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0}),
		NewSeriesFloat64("c", []float64{7.0, 8.0, 9.0}),
	)

	lf := df.Lazy().Select(Col("a"), Col("c"))
	if lf.plan.Op != PlanProject {
		t.Errorf("plan.Op = %v, want PlanProject", lf.plan.Op)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Width() != 2 {
		t.Errorf("Result width = %d, want 2", result.Width())
	}
}

func TestLazyFrame_Filter(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("value", []float64{1.0, 5.0, 3.0, 7.0, 2.0}),
	)

	lf := df.Lazy().Filter(Col("value").Gt(Lit(3.0)))
	if lf.plan.Op != PlanFilter {
		t.Errorf("plan.Op = %v, want PlanFilter", lf.plan.Op)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2 (values > 3)", result.Height())
	}
}

func TestLazyFrame_WithColumn(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	lf := df.Lazy().WithColumn("b", Col("a").Mul(Lit(2.0)))
	if lf.plan.Op != PlanWithColumn {
		t.Errorf("plan.Op = %v, want PlanWithColumn", lf.plan.Op)
	}
	if lf.plan.NewColName != "b" {
		t.Errorf("plan.NewColName = %q, want 'b'", lf.plan.NewColName)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Width() != 2 {
		t.Errorf("Result width = %d, want 2", result.Width())
	}
}

func TestLazyFrame_Sort(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("value", []float64{3.0, 1.0, 2.0}),
	)

	// Sort ascending
	lf := df.Lazy().Sort("value", true)
	if lf.plan.Op != PlanSort {
		t.Errorf("plan.Op = %v, want PlanSort", lf.plan.Op)
	}
	if !lf.plan.SortAscending {
		t.Error("plan.SortAscending = false, want true")
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	values := result.ColumnByName("value").Float64()
	if values[0] != 1.0 || values[1] != 2.0 || values[2] != 3.0 {
		t.Errorf("Sort ascending failed: %v", values)
	}

	// Sort descending
	lf2 := df.Lazy().Sort("value", false)
	result2, err := lf2.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	values2 := result2.ColumnByName("value").Float64()
	if values2[0] != 3.0 || values2[1] != 2.0 || values2[2] != 1.0 {
		t.Errorf("Sort descending failed: %v", values2)
	}
}

func TestLazyFrame_Head(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	lf := df.Lazy().Head(3)
	if lf.plan.Op != PlanLimit {
		t.Errorf("plan.Op = %v, want PlanLimit", lf.plan.Op)
	}
	if lf.plan.Limit != 3 {
		t.Errorf("plan.Limit = %d, want 3", lf.plan.Limit)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}
}

func TestLazyFrame_Tail(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	lf := df.Lazy().Tail(2)
	if lf.plan.Op != PlanTail {
		t.Errorf("plan.Op = %v, want PlanTail", lf.plan.Op)
	}
	if lf.plan.TailRows != 2 {
		t.Errorf("plan.TailRows = %d, want 2", lf.plan.TailRows)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}
}

func TestLazyFrame_Distinct(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 2, 3, 1}),
	)

	lf := df.Lazy().Distinct()
	if lf.plan.Op != PlanDistinct {
		t.Errorf("plan.Op = %v, want PlanDistinct", lf.plan.Op)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3 (distinct values)", result.Height())
	}
}

// ============================================================================
// LazyFrame Join Tests
// ============================================================================

func TestLazyFrame_Join(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("score", []float64{85.5, 92.3, 78.9}),
	)

	lf := left.Lazy().Join(right.Lazy(), On("id"))
	if lf.plan.Op != PlanJoin {
		t.Errorf("plan.Op = %v, want PlanJoin", lf.plan.Op)
	}
	if lf.plan.JoinType != InnerJoin {
		t.Errorf("plan.JoinType = %v, want InnerJoin", lf.plan.JoinType)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("Inner join result height = %d, want 2", result.Height())
	}
}

func TestLazyFrame_LeftJoin(t *testing.T) {
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
	)
	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 4}),
		NewSeriesFloat64("score", []float64{85.5, 92.3, 78.9}),
	)

	lf := left.Lazy().LeftJoin(right.Lazy(), On("id"))
	if lf.plan.JoinType != LeftJoin {
		t.Errorf("plan.JoinType = %v, want LeftJoin", lf.plan.JoinType)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Left join result height = %d, want 3", result.Height())
	}
}

// ============================================================================
// LazyGroupBy Tests
// ============================================================================

func TestLazyFrame_GroupBy(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B", "B", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	lgb := df.Lazy().GroupBy("category")
	if lgb == nil {
		t.Fatal("GroupBy returned nil")
	}
}

func TestLazyGroupBy_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B", "B", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	lf := df.Lazy().GroupBy("category").Agg(
		Col("value").Sum().Alias("total"),
	)
	if lf.plan.Op != PlanGroupBy {
		t.Errorf("plan.Op = %v, want PlanGroupBy", lf.plan.Op)
	}

	result, err := lf.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("GroupBy result height = %d, want 2", result.Height())
	}
}

func TestLazyGroupBy_Sum(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0}),
	)

	result, err := df.Lazy().GroupBy("category").Sum("value").Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("GroupBy.Sum result height = %d, want 2", result.Height())
	}
}

func TestLazyGroupBy_Mean(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B"}),
		NewSeriesFloat64("value", []float64{2.0, 4.0, 6.0}),
	)

	result, err := df.Lazy().GroupBy("category").Mean("value").Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("GroupBy.Mean result height = %d, want 2", result.Height())
	}
}

func TestLazyGroupBy_Min(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 5.0, 3.0}),
	)

	result, err := df.Lazy().GroupBy("category").Min("value").Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("GroupBy.Min result height = %d, want 2", result.Height())
	}
}

func TestLazyGroupBy_Max(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 5.0, 3.0}),
	)

	result, err := df.Lazy().GroupBy("category").Max("value").Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("GroupBy.Max result height = %d, want 2", result.Height())
	}
}

func TestLazyGroupBy_Count(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "B", "B", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	result, err := df.Lazy().GroupBy("category").Count().Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if result.Height() != 2 {
		t.Errorf("GroupBy.Count result height = %d, want 2", result.Height())
	}
}

// ============================================================================
// LazyFrame Describe/Explain Tests
// ============================================================================

func TestLazyFrame_Describe(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	lf := df.Lazy().Filter(Col("a").Gt(Lit(1.0)))
	desc := lf.Describe()

	if desc == "" {
		t.Error("Describe returned empty string")
	}
	if !strings.Contains(desc, "Filter") {
		t.Error("Describe should contain 'Filter'")
	}
	if !strings.Contains(desc, "Scan") {
		t.Error("Describe should contain 'Scan'")
	}
}

func TestLazyFrame_Explain(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	lf := df.Lazy().Select(Col("a"))
	explain := lf.Explain()

	if explain == "" {
		t.Error("Explain returned empty string")
	}
}

// ============================================================================
// PlanOp String Tests
// ============================================================================

func TestPlanOp_String(t *testing.T) {
	tests := []struct {
		op       PlanOp
		expected string
	}{
		{PlanScan, "Scan"},
		{PlanScanCSV, "ScanCSV"},
		{PlanScanParquet, "ScanParquet"},
		{PlanScanJSON, "ScanJSON"},
		{PlanProject, "Project"},
		{PlanFilter, "Filter"},
		{PlanWithColumn, "WithColumn"},
		{PlanGroupBy, "GroupBy"},
		{PlanJoin, "Join"},
		{PlanSort, "Sort"},
		{PlanLimit, "Limit"},
		{PlanTail, "Tail"},
		{PlanDistinct, "Distinct"},
	}

	for _, tc := range tests {
		result := tc.op.String()
		if result != tc.expected {
			t.Errorf("PlanOp(%d).String() = %q, want %q", tc.op, result, tc.expected)
		}
	}

	// Test unknown
	unknown := PlanOp(255)
	if unknown.String() != "Unknown" {
		t.Errorf("Unknown PlanOp.String() = %q, want 'Unknown'", unknown.String())
	}
}

// ============================================================================
// describePlan Tests
// ============================================================================

func TestDescribePlan_AllOperations(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	// Test Scan plan
	scanPlan := &LogicalPlan{Op: PlanScan, Data: df}
	desc := describePlan(scanPlan, 0)
	if !strings.Contains(desc, "Scan") {
		t.Error("describePlan for Scan missing 'Scan'")
	}

	// Test ScanCSV plan
	csvPlan := &LogicalPlan{Op: PlanScanCSV, SourcePath: "/test.csv"}
	desc = describePlan(csvPlan, 0)
	if !strings.Contains(desc, "ScanCSV") || !strings.Contains(desc, "/test.csv") {
		t.Error("describePlan for ScanCSV missing info")
	}

	// Test Project plan
	projPlan := &LogicalPlan{
		Op:          PlanProject,
		Input:       scanPlan,
		Projections: []Expr{Col("a")},
	}
	desc = describePlan(projPlan, 0)
	if !strings.Contains(desc, "Project") {
		t.Error("describePlan for Project missing 'Project'")
	}

	// Test Filter plan
	filterPlan := &LogicalPlan{
		Op:        PlanFilter,
		Input:     scanPlan,
		Predicate: Col("a").Gt(Lit(1.0)),
	}
	desc = describePlan(filterPlan, 0)
	if !strings.Contains(desc, "Filter") {
		t.Error("describePlan for Filter missing 'Filter'")
	}

	// Test GroupBy plan
	groupPlan := &LogicalPlan{
		Op:           PlanGroupBy,
		Input:        scanPlan,
		GroupByKeys:  []Expr{Col("a")},
		Aggregations: []Expr{Col("a").Sum()},
	}
	desc = describePlan(groupPlan, 0)
	if !strings.Contains(desc, "GroupBy") {
		t.Error("describePlan for GroupBy missing 'GroupBy'")
	}

	// Test Sort plan
	sortPlan := &LogicalPlan{
		Op:            PlanSort,
		Input:         scanPlan,
		SortColumn:    "a",
		SortAscending: true,
	}
	desc = describePlan(sortPlan, 0)
	if !strings.Contains(desc, "Sort") || !strings.Contains(desc, "asc=true") {
		t.Error("describePlan for Sort missing info")
	}

	// Test Limit plan
	limitPlan := &LogicalPlan{
		Op:    PlanLimit,
		Input: scanPlan,
		Limit: 10,
	}
	desc = describePlan(limitPlan, 0)
	if !strings.Contains(desc, "Limit") || !strings.Contains(desc, "10") {
		t.Error("describePlan for Limit missing info")
	}

	// Test Tail plan
	tailPlan := &LogicalPlan{
		Op:       PlanTail,
		Input:    scanPlan,
		TailRows: 5,
	}
	desc = describePlan(tailPlan, 0)
	if !strings.Contains(desc, "Tail") || !strings.Contains(desc, "5") {
		t.Error("describePlan for Tail missing info")
	}

	// Test Distinct plan
	distinctPlan := &LogicalPlan{
		Op:    PlanDistinct,
		Input: scanPlan,
	}
	desc = describePlan(distinctPlan, 0)
	if !strings.Contains(desc, "Distinct") {
		t.Error("describePlan for Distinct missing 'Distinct'")
	}
}

// ============================================================================
// Chained Operations Tests
// ============================================================================

func TestLazyFrame_ChainedOperations(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("value", []float64{1.0, 5.0, 3.0, 7.0, 2.0, 8.0, 4.0}),
	)

	result, err := df.Lazy().
		Filter(Col("value").Gt(Lit(2.0))).
		Sort("value", true).
		Head(3).
		Collect()

	if err != nil {
		t.Fatalf("Chained operations failed: %v", err)
	}
	if result.Height() != 3 {
		t.Errorf("Result height = %d, want 3", result.Height())
	}

	values := result.ColumnByName("value").Float64()
	if values[0] != 3.0 || values[1] != 4.0 || values[2] != 5.0 {
		t.Errorf("Chained result values = %v, want [3, 4, 5]", values)
	}
}
