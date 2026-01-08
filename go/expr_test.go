package galleon

import (
	"math"
	"testing"
)

// ============================================================================
// Column Expression Tests
// ============================================================================

func TestExpr_Col_Basic(t *testing.T) {
	_, err := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0}),
	)
	if err != nil {
		t.Fatalf("NewDataFrame failed: %v", err)
	}

	// Test Col expression
	expr := Col("a")
	if expr == nil {
		t.Fatal("Col() returned nil")
	}
}

func TestExpr_Lit_Float(t *testing.T) {
	expr := Lit(42.0)
	if expr == nil {
		t.Fatal("Lit() returned nil")
	}
}

func TestExpr_Lit_Int(t *testing.T) {
	expr := Lit(42)
	if expr == nil {
		t.Fatal("Lit() returned nil")
	}
}

func TestExpr_Lit_String(t *testing.T) {
	expr := Lit("hello")
	if expr == nil {
		t.Fatal("Lit() returned nil")
	}
}

// ============================================================================
// Arithmetic Expression Tests
// ============================================================================

func TestExpr_Add(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat64("b", []float64{4.0, 5.0, 6.0}),
	)

	lf := df.Lazy()
	result, err := lf.WithColumn("c", Col("a").Add(Col("b"))).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	c := result.ColumnByName("c")
	if c == nil {
		t.Fatal("Column 'c' not found")
	}

	expected := []float64{5.0, 7.0, 9.0}
	data := c.Float64()
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("c[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestExpr_Sub(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{10.0, 20.0, 30.0}),
		NewSeriesFloat64("b", []float64{1.0, 2.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.WithColumn("c", Col("a").Sub(Col("b"))).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	c := result.ColumnByName("c")
	expected := []float64{9.0, 18.0, 27.0}
	data := c.Float64()
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("c[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestExpr_Mul(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{2.0, 3.0, 4.0}),
		NewSeriesFloat64("b", []float64{5.0, 6.0, 7.0}),
	)

	lf := df.Lazy()
	result, err := lf.WithColumn("c", Col("a").Mul(Col("b"))).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	c := result.ColumnByName("c")
	expected := []float64{10.0, 18.0, 28.0}
	data := c.Float64()
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("c[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestExpr_Div(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{10.0, 20.0, 30.0}),
		NewSeriesFloat64("b", []float64{2.0, 4.0, 5.0}),
	)

	lf := df.Lazy()
	result, err := lf.WithColumn("c", Col("a").Div(Col("b"))).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	c := result.ColumnByName("c")
	expected := []float64{5.0, 5.0, 6.0}
	data := c.Float64()
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("c[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestExpr_AddLiteral(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.WithColumn("b", Col("a").Add(Lit(10.0))).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	b := result.ColumnByName("b")
	expected := []float64{11.0, 12.0, 13.0}
	data := b.Float64()
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("b[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

// ============================================================================
// Comparison Expression Tests
// ============================================================================

func TestExpr_Gt(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 5.0, 3.0, 7.0}),
	)

	lf := df.Lazy()
	result, err := lf.Filter(Col("a").Gt(Lit(4.0))).Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have 2 rows (5.0 and 7.0)
	if result.Height() != 2 {
		t.Errorf("Filter result height = %d, want 2", result.Height())
	}
}

func TestExpr_Lt(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 5.0, 3.0, 7.0}),
	)

	lf := df.Lazy()
	result, err := lf.Filter(Col("a").Lt(Lit(4.0))).Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have 2 rows (1.0 and 3.0)
	if result.Height() != 2 {
		t.Errorf("Filter result height = %d, want 2", result.Height())
	}
}

func TestExpr_Eq(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 2.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.Filter(Col("a").Eq(Lit(2.0))).Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have 2 rows (both 2.0s)
	if result.Height() != 2 {
		t.Errorf("Filter result height = %d, want 2", result.Height())
	}
}

func TestExpr_Gte(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0}),
	)

	lf := df.Lazy()
	result, err := lf.Filter(Col("a").Gte(Lit(3.0))).Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have 2 rows (3.0 and 4.0)
	if result.Height() != 2 {
		t.Errorf("Filter result height = %d, want 2", result.Height())
	}
}

func TestExpr_Lte(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0, 4.0}),
	)

	lf := df.Lazy()
	result, err := lf.Filter(Col("a").Lte(Lit(2.0))).Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have 2 rows (1.0 and 2.0)
	if result.Height() != 2 {
		t.Errorf("Filter result height = %d, want 2", result.Height())
	}
}

// ============================================================================
// Aggregation Expression Tests
// ============================================================================

func TestExpr_Sum_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Sum().Alias("total")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}

	// Verify sums
	groups := result.ColumnByName("group").Strings()
	totals := result.ColumnByName("total").Float64()

	for i, g := range groups {
		if g == "A" && math.Abs(totals[i]-3.0) > 0.001 {
			t.Errorf("Group A total = %f, want 3.0", totals[i])
		}
		if g == "B" && math.Abs(totals[i]-7.0) > 0.001 {
			t.Errorf("Group B total = %f, want 7.0", totals[i])
		}
	}
}

func TestExpr_Mean_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 10.0, 20.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Mean().Alias("avg")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	groups := result.ColumnByName("group").Strings()
	avgs := result.ColumnByName("avg").Float64()

	for i, g := range groups {
		if g == "A" && math.Abs(avgs[i]-2.0) > 0.001 {
			t.Errorf("Group A avg = %f, want 2.0", avgs[i])
		}
		if g == "B" && math.Abs(avgs[i]-15.0) > 0.001 {
			t.Errorf("Group B avg = %f, want 15.0", avgs[i])
		}
	}
}

func TestExpr_Min_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{5.0, 2.0, 8.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Min().Alias("minimum")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	groups := result.ColumnByName("group").Strings()
	mins := result.ColumnByName("minimum").Float64()

	for i, g := range groups {
		if g == "A" && math.Abs(mins[i]-2.0) > 0.001 {
			t.Errorf("Group A min = %f, want 2.0", mins[i])
		}
		if g == "B" && math.Abs(mins[i]-3.0) > 0.001 {
			t.Errorf("Group B min = %f, want 3.0", mins[i])
		}
	}
}

func TestExpr_Max_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{5.0, 2.0, 8.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Max().Alias("maximum")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	groups := result.ColumnByName("group").Strings()
	maxs := result.ColumnByName("maximum").Float64()

	for i, g := range groups {
		if g == "A" && math.Abs(maxs[i]-5.0) > 0.001 {
			t.Errorf("Group A max = %f, want 5.0", maxs[i])
		}
		if g == "B" && math.Abs(maxs[i]-8.0) > 0.001 {
			t.Errorf("Group B max = %f, want 8.0", maxs[i])
		}
	}
}

// ============================================================================
// Alias Tests
// ============================================================================

func TestExpr_Alias(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.WithColumn("b", Col("a").Add(Lit(10.0)).Alias("result")).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	// The column should be named "b" since that's the WithColumn name
	b := result.ColumnByName("b")
	if b == nil {
		t.Fatal("Column 'b' not found")
	}
}

// ============================================================================
// Complex Expression Chaining Tests
// ============================================================================

func TestExpr_ChainedArithmetic(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat64("b", []float64{2.0, 3.0, 4.0}),
	)

	// Test a * b (column multiplication)
	lf := df.Lazy()
	result, err := lf.WithColumn("c", Col("a").Mul(Col("b"))).Collect()
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}

	c := result.ColumnByName("c")
	expected := []float64{2.0, 6.0, 12.0} // 1*2=2, 2*3=6, 3*4=12
	data := c.Float64()
	for i, v := range data {
		if math.Abs(v-expected[i]) > 0.0001 {
			t.Errorf("c[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestExpr_MultipleWithColumns(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 3.0}),
		NewSeriesFloat64("b", []float64{2.0, 3.0, 4.0}),
	)

	// Test adding two derived columns in sequence
	lf := df.Lazy()
	result, err := lf.
		WithColumn("sum", Col("a").Add(Col("b"))).
		WithColumn("product", Col("a").Mul(Col("b"))).
		Collect()
	if err != nil {
		t.Fatalf("WithColumn chain failed: %v", err)
	}

	// Verify sum column
	sumCol := result.ColumnByName("sum")
	expectedSum := []float64{3.0, 5.0, 7.0}
	sumData := sumCol.Float64()
	for i, v := range sumData {
		if math.Abs(v-expectedSum[i]) > 0.0001 {
			t.Errorf("sum[%d] = %f, want %f", i, v, expectedSum[i])
		}
	}

	// Verify product column
	prodCol := result.ColumnByName("product")
	expectedProd := []float64{2.0, 6.0, 12.0}
	prodData := prodCol.Float64()
	for i, v := range prodData {
		if math.Abs(v-expectedProd[i]) > 0.0001 {
			t.Errorf("product[%d] = %f, want %f", i, v, expectedProd[i])
		}
	}
}

// ============================================================================
// Expression String Representation Tests
// ============================================================================

func TestExpr_Col_String(t *testing.T) {
	expr := Col("my_column")
	str := expr.String()
	if str != `col("my_column")` {
		t.Errorf("Col.String() = %q, want %q", str, `col("my_column")`)
	}
}

func TestExpr_Lit_StringRepr(t *testing.T) {
	expr := Lit(42.5)
	str := expr.String()
	if str != "lit(42.5)" {
		t.Errorf("Lit.String() = %q, want %q", str, "lit(42.5)")
	}
}

func TestExpr_BinaryOp_String(t *testing.T) {
	expr := Col("a").Add(Col("b"))
	str := expr.String()
	// Should contain both columns and the operator
	if str != `(col("a") + col("b"))` {
		t.Errorf("BinaryOp.String() = %q", str)
	}
}

// ============================================================================
// Expression Clone Tests
// ============================================================================

func TestExpr_Col_Clone(t *testing.T) {
	original := Col("test")
	cloned := original.Clone()

	if cloned == nil {
		t.Fatal("Clone returned nil")
	}

	// Cloned should be equal but not the same object
	colCloned, ok := cloned.(*ColExpr)
	if !ok {
		t.Fatal("Clone should return *ColExpr")
	}
	if colCloned.Name != "test" {
		t.Errorf("Cloned name = %q, want 'test'", colCloned.Name)
	}
}

func TestExpr_Lit_Clone(t *testing.T) {
	original := Lit(42.0)
	cloned := original.Clone()

	if cloned == nil {
		t.Fatal("Clone returned nil")
	}

	litCloned, ok := cloned.(*LitExpr)
	if !ok {
		t.Fatal("Clone should return *LitExpr")
	}
	if litCloned.Value != 42.0 {
		t.Errorf("Cloned value = %v, want 42.0", litCloned.Value)
	}
}

func TestExpr_BinaryOp_Clone(t *testing.T) {
	original := Col("a").Add(Col("b"))
	cloned := original.Clone()

	if cloned == nil {
		t.Fatal("Clone returned nil")
	}

	binCloned, ok := cloned.(*BinaryOpExpr)
	if !ok {
		t.Fatal("Clone should return *BinaryOpExpr")
	}
	if binCloned.Op != OpAdd {
		t.Errorf("Cloned op = %v, want OpAdd", binCloned.Op)
	}
}

// ============================================================================
// Additional Comparison Tests
// ============================================================================

func TestExpr_Neq(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("a", []float64{1.0, 2.0, 2.0, 3.0}),
	)

	lf := df.Lazy()
	result, err := lf.Filter(Col("a").Neq(Lit(2.0))).Collect()
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Should have 2 rows (1.0 and 3.0, not the 2.0s)
	if result.Height() != 2 {
		t.Errorf("Filter result height = %d, want 2", result.Height())
	}

	data := result.ColumnByName("a").Float64()
	for _, v := range data {
		if v == 2.0 {
			t.Errorf("Found 2.0 in result, should have been filtered out")
		}
	}
}

// ============================================================================
// Logical Expression Tests
// ============================================================================

func TestExpr_And(t *testing.T) {
	// Test And expression creation
	left := Col("a").Gt(Lit(2.0))
	right := Col("a").Lt(Lit(6.0))
	andExpr := left.And(right)

	if andExpr == nil {
		t.Fatal("And expression should not be nil")
	}

	// andExpr is already *BinaryOpExpr, check Op directly
	if andExpr.Op != OpAnd {
		t.Errorf("And expression Op = %v, want OpAnd", andExpr.Op)
	}
}

func TestExpr_Or(t *testing.T) {
	// Test Or expression creation
	left := Col("a").Lt(Lit(2.0))
	right := Col("a").Gt(Lit(6.0))
	orExpr := left.Or(right)

	if orExpr == nil {
		t.Fatal("Or expression should not be nil")
	}

	// orExpr is already *BinaryOpExpr, check Op directly
	if orExpr.Op != OpOr {
		t.Errorf("Or expression Op = %v, want OpOr", orExpr.Op)
	}
}

// ============================================================================
// Additional Aggregation Tests
// ============================================================================

func TestExpr_Count_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0, 4.0, 5.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Count().Alias("cnt")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	groups := result.ColumnByName("group").Strings()
	counts := result.ColumnByName("cnt")
	if counts == nil {
		t.Fatal("Count column not found")
	}

	// Verify counts
	for i, g := range groups {
		var expectedCount int64
		if g == "A" {
			expectedCount = 3
		} else if g == "B" {
			expectedCount = 2
		}

		// Count may return different types, check the value
		countData := counts.Int64()
		if len(countData) > i && countData[i] != expectedCount {
			t.Errorf("Group %s count = %d, want %d", g, countData[i], expectedCount)
		}
	}
}

func TestExpr_First_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 30.0, 40.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").First().Alias("first_val")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}

	// First value for each group
	groups := result.ColumnByName("group").Strings()
	firsts := result.ColumnByName("first_val").Float64()

	for i, g := range groups {
		if g == "A" && firsts[i] != 10.0 {
			t.Errorf("Group A first = %f, want 10.0", firsts[i])
		}
		if g == "B" && firsts[i] != 30.0 {
			t.Errorf("Group B first = %f, want 30.0", firsts[i])
		}
	}
}

func TestExpr_Last_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B"}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 30.0, 40.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Last().Alias("last_val")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("Result height = %d, want 2", result.Height())
	}

	// Last value for each group
	groups := result.ColumnByName("group").Strings()
	lasts := result.ColumnByName("last_val").Float64()

	for i, g := range groups {
		if g == "A" && lasts[i] != 20.0 {
			t.Errorf("Group A last = %f, want 20.0", lasts[i])
		}
		if g == "B" && lasts[i] != 40.0 {
			t.Errorf("Group B last = %f, want 40.0", lasts[i])
		}
	}
}

func TestExpr_Std_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "A"}),
		NewSeriesFloat64("value", []float64{2.0, 4.0, 4.0, 4.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Std().Alias("std_dev")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	stds := result.ColumnByName("std_dev")
	if stds == nil {
		t.Fatal("Std column not found")
	}

	// Sample std dev of [2, 4, 4, 4] is 1.0 (using N-1 formula)
	// mean = 3.5, sum_sq_diff = 3.0, sample_var = 3.0/3 = 1.0, std = 1.0
	stdData := stds.Float64()
	if len(stdData) > 0 && math.Abs(stdData[0]-1.0) > 0.1 {
		t.Errorf("Std dev = %f, expected approximately 1.0", stdData[0])
	}
}

func TestExpr_Var_Agg(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "A"}),
		NewSeriesFloat64("value", []float64{2.0, 4.0, 4.0, 4.0}),
	)

	lf := df.Lazy()
	result, err := lf.GroupBy("group").Agg(Col("value").Var().Alias("variance")).Collect()
	if err != nil {
		t.Fatalf("GroupBy.Agg failed: %v", err)
	}

	vars := result.ColumnByName("variance")
	if vars == nil {
		t.Fatal("Variance column not found")
	}

	// Sample variance of [2, 4, 4, 4] is 1.0 (using N-1 formula)
	// mean = 3.5, sum_sq_diff = 3.0, sample_var = 3.0/3 = 1.0
	varData := vars.Float64()
	if len(varData) > 0 && math.Abs(varData[0]-1.0) > 0.1 {
		t.Errorf("Variance = %f, expected approximately 1.0", varData[0])
	}
}

// ============================================================================
// Expression columns() Tests
// ============================================================================

func TestExpr_Columns(t *testing.T) {
	// ColExpr should return its column name
	col := Col("test")
	cols := col.columns()
	if len(cols) != 1 || cols[0] != "test" {
		t.Errorf("Col.columns() = %v, want [test]", cols)
	}

	// LitExpr should return empty
	lit := Lit(42)
	litCols := lit.columns()
	if len(litCols) != 0 {
		t.Errorf("Lit.columns() = %v, want []", litCols)
	}

	// BinaryOp should return columns from both sides
	binOp := Col("a").Add(Col("b"))
	binCols := binOp.columns()
	if len(binCols) != 2 {
		t.Errorf("BinaryOp.columns() length = %d, want 2", len(binCols))
	}
}

// ============================================================================
// AggExpr Tests
// ============================================================================

func TestExpr_AggExpr_Alias(t *testing.T) {
	aggExpr := Col("value").Sum()
	aliased := aggExpr.Alias("total")

	if aliased == nil {
		t.Fatal("AggExpr.Alias() returned nil")
	}

	// aliased is already *AliasExpr (returned directly by Alias())
	if aliased.AliasName != "total" {
		t.Errorf("Alias name = %q, want 'total'", aliased.AliasName)
	}
}

// ============================================================================
// Additional Expression Tests for Coverage
// ============================================================================

func TestExpr_ColExpr_And(t *testing.T) {
	andExpr := Col("a").And(Col("b"))
	if andExpr == nil {
		t.Fatal("ColExpr.And() returned nil")
	}
	cols := andExpr.columns()
	if len(cols) != 2 {
		t.Errorf("And.columns() = %v, want 2 columns", cols)
	}
}

func TestExpr_ColExpr_Or(t *testing.T) {
	orExpr := Col("a").Or(Col("b"))
	if orExpr == nil {
		t.Fatal("ColExpr.Or() returned nil")
	}
	cols := orExpr.columns()
	if len(cols) != 2 {
		t.Errorf("Or.columns() = %v, want 2 columns", cols)
	}
}

func TestExpr_ColExpr_Alias(t *testing.T) {
	col := Col("original")
	aliased := col.Alias("renamed")
	if aliased == nil {
		t.Fatal("ColExpr.Alias() returned nil")
	}
	if aliased.AliasName != "renamed" {
		t.Errorf("Alias name = %q, want 'renamed'", aliased.AliasName)
	}
}

func TestExpr_ColExpr_Cast(t *testing.T) {
	col := Col("value")
	casted := col.Cast(Float64)
	if casted == nil {
		t.Fatal("ColExpr.Cast() returned nil")
	}
}

func TestExpr_CastExpr_Methods(t *testing.T) {
	col := Col("value")
	casted := col.Cast(Float64)

	// Test String method
	str := casted.String()
	if str == "" {
		t.Error("CastExpr.String() returned empty string")
	}

	// Test Clone
	cloned := casted.Clone()
	if cloned == nil {
		t.Fatal("CastExpr.Clone() returned nil")
	}

	// Test columns
	cols := casted.columns()
	if len(cols) != 1 || cols[0] != "value" {
		t.Errorf("CastExpr.columns() = %v, want [value]", cols)
	}
}

func TestExpr_CastExpr_Alias(t *testing.T) {
	col := Col("value")
	casted := col.Cast(Float64)
	aliased := casted.Alias("new_name")
	if aliased == nil {
		t.Fatal("CastExpr.Alias() returned nil")
	}
	if aliased.AliasName != "new_name" {
		t.Errorf("Alias name = %q, want 'new_name'", aliased.AliasName)
	}
}

func TestExpr_AllCols(t *testing.T) {
	allCols := AllCols()
	if allCols == nil {
		t.Fatal("AllCols() returned nil")
	}

	// Test String method
	str := allCols.String()
	if str == "" {
		t.Error("AllColsExpr.String() returned empty string")
	}

	// Test Clone
	cloned := allCols.Clone()
	if cloned == nil {
		t.Fatal("AllColsExpr.Clone() returned nil")
	}

	// Test columns
	cols := allCols.columns()
	if cols != nil {
		t.Errorf("AllColsExpr.columns() = %v, want nil", cols)
	}
}

func TestExpr_AliasExpr_Methods(t *testing.T) {
	col := Col("value")
	aliased := col.Alias("new_name")

	// Test String
	str := aliased.String()
	if str == "" {
		t.Error("AliasExpr.String() returned empty string")
	}

	// Test Clone
	cloned := aliased.Clone()
	if cloned == nil {
		t.Fatal("AliasExpr.Clone() returned nil")
	}

	// Test columns
	cols := aliased.columns()
	if len(cols) != 1 || cols[0] != "value" {
		t.Errorf("AliasExpr.columns() = %v, want [value]", cols)
	}
}

func TestExpr_BinaryOpExpr_String(t *testing.T) {
	// Test all operations
	ops := []struct {
		expr Expr
		op   string
	}{
		{Col("a").Add(Col("b")), "+"},
		{Col("a").Sub(Col("b")), "-"},
		{Col("a").Mul(Col("b")), "*"},
		{Col("a").Div(Col("b")), "/"},
	}

	for _, tt := range ops {
		str := tt.expr.String()
		if str == "" {
			t.Errorf("BinaryOpExpr(%s).String() returned empty string", tt.op)
		}
	}
}

func TestExpr_CompareExpr_String(t *testing.T) {
	// Test all comparison operations
	ops := []struct {
		expr Expr
		name string
	}{
		{Col("a").Gt(Lit(5.0)), "Gt"},
		{Col("a").Lt(Lit(5.0)), "Lt"},
		{Col("a").Eq(Lit(5.0)), "Eq"},
		{Col("a").Neq(Lit(5.0)), "Neq"},
		{Col("a").Gte(Lit(5.0)), "Gte"},
		{Col("a").Lte(Lit(5.0)), "Lte"},
	}

	for _, tt := range ops {
		str := tt.expr.String()
		if str == "" {
			t.Errorf("CompareExpr(%s).String() returned empty string", tt.name)
		}
	}
}

func TestExpr_BinaryOpExpr_Clone(t *testing.T) {
	binOp := Col("a").Add(Col("b"))
	cloned := binOp.Clone()

	if cloned == nil {
		t.Fatal("BinaryOpExpr.Clone() returned nil")
	}

	// Verify the clone has the same string representation
	if binOp.String() != cloned.String() {
		t.Errorf("Clone.String() = %q, want %q", cloned.String(), binOp.String())
	}
}

func TestExpr_CompareExpr_Clone(t *testing.T) {
	cmpExpr := Col("a").Gt(Lit(5.0))
	cloned := cmpExpr.Clone()

	if cloned == nil {
		t.Fatal("CompareExpr.Clone() returned nil")
	}
}

func TestExpr_AggExpr_Clone(t *testing.T) {
	aggExpr := Col("a").Sum()
	cloned := aggExpr.Clone()

	if cloned == nil {
		t.Fatal("AggExpr.Clone() returned nil")
	}
}

func TestExpr_LitExpr_Clone(t *testing.T) {
	lit := Lit(42.0)
	cloned := lit.Clone()

	if cloned == nil {
		t.Fatal("LitExpr.Clone() returned nil")
	}
}

func TestExpr_ColExpr_Clone(t *testing.T) {
	col := Col("test")
	cloned := col.Clone()

	if cloned == nil {
		t.Fatal("ColExpr.Clone() returned nil")
	}

	if cloned.String() != col.String() {
		t.Errorf("Clone.String() = %q, want %q", cloned.String(), col.String())
	}
}

// ============================================================================
// BinaryOp String() Tests
// ============================================================================

func TestBinaryOp_String_AllTypes(t *testing.T) {
	tests := []struct {
		op       BinaryOp
		expected string
	}{
		{OpAdd, "+"},
		{OpSub, "-"},
		{OpMul, "*"},
		{OpDiv, "/"},
		{OpGt, ">"},
		{OpLt, "<"},
		{OpEq, "=="},
		{OpNeq, "!="},
		{OpGte, ">="},
		{OpLte, "<="},
		{OpAnd, "and"},
		{OpOr, "or"},
	}

	for _, tt := range tests {
		result := tt.op.String()
		if result != tt.expected {
			t.Errorf("BinaryOp(%d).String() = %q, want %q", tt.op, result, tt.expected)
		}
	}

	// Test unknown op
	unknownOp := BinaryOp(99)
	if unknownOp.String() != "?" {
		t.Errorf("Unknown BinaryOp.String() = %q, want '?'", unknownOp.String())
	}
}

// ============================================================================
// AggType String() Tests
// ============================================================================

func TestAggType_String_AllTypes(t *testing.T) {
	tests := []struct {
		agg      AggType
		expected string
	}{
		{AggTypeSum, "sum"},
		{AggTypeMean, "mean"},
		{AggTypeMin, "min"},
		{AggTypeMax, "max"},
		{AggTypeCount, "count"},
		{AggTypeFirst, "first"},
		{AggTypeLast, "last"},
		{AggTypeStd, "std"},
		{AggTypeVar, "var"},
	}

	for _, tt := range tests {
		result := tt.agg.String()
		if result != tt.expected {
			t.Errorf("AggType(%d).String() = %q, want %q", tt.agg, result, tt.expected)
		}
	}

	// Test unknown agg type
	unknownAgg := AggType(99)
	if unknownAgg.String() != "?" {
		t.Errorf("Unknown AggType.String() = %q, want '?'", unknownAgg.String())
	}
}

// ============================================================================
// AggExpr String() Tests
// ============================================================================

func TestAggExpr_String_AllTypes(t *testing.T) {
	col := Col("value")

	tests := []struct {
		expr    *AggExpr
		pattern string
	}{
		{col.Sum(), "sum()"},
		{col.Mean(), "mean()"},
		{col.Min(), "min()"},
		{col.Max(), "max()"},
		{col.Count(), "count()"},
		{col.First(), "first()"},
		{col.Last(), "last()"},
		{col.Std(), "std()"},
		{col.Var(), "var()"},
	}

	for _, tt := range tests {
		str := tt.expr.String()
		if str == "" {
			t.Errorf("AggExpr.String() returned empty string for %s", tt.pattern)
		}
		// Just verify it contains the expected function name
		if !containsSubstr(str, tt.pattern) {
			t.Errorf("AggExpr.String() = %q, should contain %q", str, tt.pattern)
		}
	}
}

// Helper to check substring in the string
func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================================================================
// Optimizer-related Expression Tests
// ============================================================================

func TestExpr_SingleFilter(t *testing.T) {
	// Test single filter to trigger optimizer path
	df, _ := NewDataFrame(
		NewSeriesInt64("a", []int64{1, 2, 3, 4, 5}),
		NewSeriesFloat64("b", []float64{10.0, 20.0, 30.0, 40.0, 50.0}),
	)

	lf := df.Lazy()
	result, err := lf.
		Filter(Col("a").Gt(Lit(2))).
		Select(Col("a"), Col("b")).
		Collect()

	if err != nil {
		t.Fatalf("Filter + Select failed: %v", err)
	}

	// Should have rows where a > 2: 3, 4, 5
	if result.Height() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.Height())
	}
}

func TestExpr_FilterBeforeJoin(t *testing.T) {
	// Test filter before join
	left, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
		NewSeriesFloat64("value", []float64{10.0, 20.0, 30.0, 40.0, 50.0}),
	)

	right, _ := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"a", "b", "c"}),
	)

	result, err := left.Lazy().
		Filter(Col("id").Lt(Lit(4))).
		Join(right.Lazy(), On("id")).
		Collect()

	if err != nil {
		t.Fatalf("Filter before join failed: %v", err)
	}

	// Filter (id < 4) leaves ids [1, 2, 3] which all match in right
	if result.Height() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.Height())
	}
}
