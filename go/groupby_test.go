package galleon

import (
	"math"
	"testing"
)

func TestGroupByBasic(t *testing.T) {
	// Create a DataFrame with groups
	df, err := NewDataFrame(
		NewSeriesString("region", []string{"east", "west", "east", "west", "east"}),
		NewSeriesFloat64("sales", []float64{100, 200, 150, 250, 175}),
		NewSeriesInt64("units", []int64{10, 20, 15, 25, 18}),
	)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	gb := df.GroupBy("region")
	if gb.NumGroups() != 2 {
		t.Errorf("expected 2 groups, got %d", gb.NumGroups())
	}
}

func TestGroupBySum(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("region", []string{"east", "west", "east", "west", "east"}),
		NewSeriesFloat64("sales", []float64{100, 200, 150, 250, 175}),
	)

	result, err := df.GroupBy("region").Sum("sales")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}

	// Find east and west sums
	regionCol := result.ColumnByName("region")
	salesCol := result.ColumnByName("sales_sum")

	for i := 0; i < result.Height(); i++ {
		region, _ := regionCol.GetString(i)
		sum, _ := salesCol.GetFloat64(i)

		switch region {
		case "east":
			if sum != 425 { // 100 + 150 + 175
				t.Errorf("east sum: expected 425, got %f", sum)
			}
		case "west":
			if sum != 450 { // 200 + 250
				t.Errorf("west sum: expected 450, got %f", sum)
			}
		}
	}
}

func TestGroupByMean(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "B", "A", "B"}),
		NewSeriesFloat64("value", []float64{10, 20, 30, 40}),
	)

	result, err := df.GroupBy("category").Mean("value")
	if err != nil {
		t.Fatalf("failed to compute mean: %v", err)
	}

	catCol := result.ColumnByName("category")
	meanCol := result.ColumnByName("value_mean")

	for i := 0; i < result.Height(); i++ {
		cat, _ := catCol.GetString(i)
		mean, _ := meanCol.GetFloat64(i)

		switch cat {
		case "A":
			if mean != 20 { // (10 + 30) / 2
				t.Errorf("A mean: expected 20, got %f", mean)
			}
		case "B":
			if mean != 30 { // (20 + 40) / 2
				t.Errorf("B mean: expected 30, got %f", mean)
			}
		}
	}
}

func TestGroupByMinMax(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"X", "Y", "X", "Y", "X"}),
		NewSeriesInt64("value", []int64{5, 10, 3, 8, 7}),
	)

	// Test Min
	minResult, err := df.GroupBy("group").Min("value")
	if err != nil {
		t.Fatalf("failed to compute min: %v", err)
	}

	// Test Max
	maxResult, err := df.GroupBy("group").Max("value")
	if err != nil {
		t.Fatalf("failed to compute max: %v", err)
	}

	// Verify min values
	groupCol := minResult.ColumnByName("group")
	minCol := minResult.ColumnByName("value_min")

	for i := 0; i < minResult.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		min, _ := minCol.GetInt64(i)

		switch grp {
		case "X":
			if min != 3 {
				t.Errorf("X min: expected 3, got %d", min)
			}
		case "Y":
			if min != 8 {
				t.Errorf("Y min: expected 8, got %d", min)
			}
		}
	}

	// Verify max values
	groupCol = maxResult.ColumnByName("group")
	maxCol := maxResult.ColumnByName("value_max")

	for i := 0; i < maxResult.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		max, _ := maxCol.GetInt64(i)

		switch grp {
		case "X":
			if max != 7 {
				t.Errorf("X max: expected 7, got %d", max)
			}
		case "Y":
			if max != 10 {
				t.Errorf("Y max: expected 10, got %d", max)
			}
		}
	}
}

func TestGroupByCount(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("status", []string{"active", "inactive", "active", "active", "inactive"}),
		NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
	)

	result, err := df.GroupBy("status").Count()
	if err != nil {
		t.Fatalf("failed to compute count: %v", err)
	}

	statusCol := result.ColumnByName("status")
	countCol := result.ColumnByName("count")

	for i := 0; i < result.Height(); i++ {
		status, _ := statusCol.GetString(i)
		count, _ := countCol.GetInt64(i)

		switch status {
		case "active":
			if count != 3 {
				t.Errorf("active count: expected 3, got %d", count)
			}
		case "inactive":
			if count != 2 {
				t.Errorf("inactive count: expected 2, got %d", count)
			}
		}
	}
}

func TestGroupByMultipleAggregations(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("department", []string{"sales", "eng", "sales", "eng", "sales"}),
		NewSeriesFloat64("salary", []float64{50000, 80000, 60000, 90000, 55000}),
		NewSeriesInt64("years", []int64{2, 5, 3, 7, 1}),
	)

	result, err := df.GroupBy("department").Agg(
		AggSum("salary").Alias("total_salary"),
		AggMean("salary").Alias("avg_salary"),
		AggMin("years").Alias("min_years"),
		AggMax("years").Alias("max_years"),
		AggCount().Alias("num_employees"),
	)
	if err != nil {
		t.Fatalf("failed to compute aggregations: %v", err)
	}

	// Verify we have all columns
	expectedCols := []string{"department", "total_salary", "avg_salary", "min_years", "max_years", "num_employees"}
	for _, col := range expectedCols {
		if result.ColumnByName(col) == nil {
			t.Errorf("missing column: %s", col)
		}
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Height())
	}
}

func TestGroupByMultipleKeys(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("region", []string{"east", "east", "west", "west", "east"}),
		NewSeriesString("category", []string{"A", "B", "A", "B", "A"}),
		NewSeriesFloat64("value", []float64{10, 20, 30, 40, 50}),
	)

	result, err := df.GroupBy("region", "category").Sum("value")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	// Should have 4 groups: (east, A), (east, B), (west, A), (west, B)
	if result.Height() != 4 {
		t.Errorf("expected 4 groups, got %d", result.Height())
	}
}

func TestGroupByFirstLast(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "B", "B"}),
		NewSeriesInt64("value", []int64{1, 2, 3, 10, 20}),
	)

	firstResult, err := df.GroupBy("group").First("value")
	if err != nil {
		t.Fatalf("failed to compute first: %v", err)
	}

	lastResult, err := df.GroupBy("group").Last("value")
	if err != nil {
		t.Fatalf("failed to compute last: %v", err)
	}

	// Check first values
	groupCol := firstResult.ColumnByName("group")
	firstCol := firstResult.ColumnByName("value_first")

	for i := 0; i < firstResult.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		first, _ := firstCol.GetInt64(i)

		switch grp {
		case "A":
			if first != 1 {
				t.Errorf("A first: expected 1, got %d", first)
			}
		case "B":
			if first != 10 {
				t.Errorf("B first: expected 10, got %d", first)
			}
		}
	}

	// Check last values
	groupCol = lastResult.ColumnByName("group")
	lastCol := lastResult.ColumnByName("value_last")

	for i := 0; i < lastResult.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		last, _ := lastCol.GetInt64(i)

		switch grp {
		case "A":
			if last != 3 {
				t.Errorf("A last: expected 3, got %d", last)
			}
		case "B":
			if last != 20 {
				t.Errorf("B last: expected 20, got %d", last)
			}
		}
	}
}

func TestGroupByStdVar(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "B", "B", "B"}),
		NewSeriesFloat64("value", []float64{2, 4, 6, 10, 20, 30}),
	)

	varResult, err := df.GroupBy("group").Var("value")
	if err != nil {
		t.Fatalf("failed to compute variance: %v", err)
	}

	stdResult, err := df.GroupBy("group").Std("value")
	if err != nil {
		t.Fatalf("failed to compute std: %v", err)
	}

	groupCol := varResult.ColumnByName("group")
	varCol := varResult.ColumnByName("value_var")
	stdCol := stdResult.ColumnByName("value_std")

	for i := 0; i < varResult.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		variance, _ := varCol.GetFloat64(i)
		std, _ := stdCol.GetFloat64(i)

		// Verify std = sqrt(var)
		if math.Abs(std-math.Sqrt(variance)) > 0.0001 {
			t.Errorf("group %s: std (%f) != sqrt(var) (%f)", grp, std, math.Sqrt(variance))
		}

		// For group A: values 2,4,6, mean=4, var = ((2-4)^2 + (4-4)^2 + (6-4)^2) / 2 = 4
		if grp == "A" && math.Abs(variance-4) > 0.0001 {
			t.Errorf("A variance: expected 4, got %f", variance)
		}
	}
}

func TestGroupByMedian(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A", "B", "B", "B", "B"}),
		NewSeriesFloat64("value", []float64{1, 2, 3, 10, 20, 30, 40}),
	)

	result, err := df.GroupBy("group").Agg(AggMedian("value"))
	if err != nil {
		t.Fatalf("failed to compute median: %v", err)
	}

	groupCol := result.ColumnByName("group")
	medianCol := result.ColumnByName("value_median")

	for i := 0; i < result.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		median, _ := medianCol.GetFloat64(i)

		switch grp {
		case "A":
			if median != 2 { // median of [1, 2, 3]
				t.Errorf("A median: expected 2, got %f", median)
			}
		case "B":
			if median != 25 { // median of [10, 20, 30, 40] = (20+30)/2
				t.Errorf("B median: expected 25, got %f", median)
			}
		}
	}
}

func TestGroupByCountDistinct(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("category", []string{"A", "A", "A", "B", "B"}),
		NewSeriesString("product", []string{"x", "y", "x", "p", "p"}),
	)

	result, err := df.GroupBy("category").Agg(AggCountDistinct("product"))
	if err != nil {
		t.Fatalf("failed to compute count distinct: %v", err)
	}

	catCol := result.ColumnByName("category")
	nuniqueCol := result.ColumnByName("product_nunique")

	for i := 0; i < result.Height(); i++ {
		cat, _ := catCol.GetString(i)
		nunique, _ := nuniqueCol.GetInt64(i)

		switch cat {
		case "A":
			if nunique != 2 { // x, y
				t.Errorf("A nunique: expected 2, got %d", nunique)
			}
		case "B":
			if nunique != 1 { // p
				t.Errorf("B nunique: expected 1, got %d", nunique)
			}
		}
	}
}

func TestGroupByEmptyDataFrame(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{}),
		NewSeriesFloat64("value", []float64{}),
	)

	result, err := df.GroupBy("group").Sum("value")
	if err != nil {
		t.Fatalf("failed on empty DataFrame: %v", err)
	}

	if result.Height() != 0 {
		t.Errorf("expected 0 rows for empty DataFrame, got %d", result.Height())
	}
}

func TestGroupByInvalidColumn(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "B"}),
		NewSeriesFloat64("value", []float64{1, 2}),
	)

	_, err := df.GroupBy("group").Sum("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent column")
	}
}

func TestGroupByIntegerKey(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt64("year", []int64{2020, 2021, 2020, 2021, 2020}),
		NewSeriesFloat64("revenue", []float64{100, 150, 120, 180, 110}),
	)

	result, err := df.GroupBy("year").Sum("revenue")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 groups, got %d", result.Height())
	}

	yearCol := result.ColumnByName("year")
	sumCol := result.ColumnByName("revenue_sum")

	for i := 0; i < result.Height(); i++ {
		year, _ := yearCol.GetInt64(i)
		sum, _ := sumCol.GetFloat64(i)

		switch year {
		case 2020:
			if sum != 330 { // 100 + 120 + 110
				t.Errorf("2020 sum: expected 330, got %f", sum)
			}
		case 2021:
			if sum != 330 { // 150 + 180
				t.Errorf("2021 sum: expected 330, got %f", sum)
			}
		}
	}
}

func TestGroupByWithAlias(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("dept", []string{"A", "B", "A"}),
		NewSeriesFloat64("salary", []float64{100, 200, 150}),
	)

	result, err := df.GroupBy("dept").Agg(
		AggSum("salary").Alias("total"),
		AggMean("salary").Alias("average"),
	)
	if err != nil {
		t.Fatalf("failed to compute aggregations: %v", err)
	}

	// Check custom column names
	if result.ColumnByName("total") == nil {
		t.Error("missing 'total' column")
	}
	if result.ColumnByName("average") == nil {
		t.Error("missing 'average' column")
	}
}

func BenchmarkGroupBySum(b *testing.B) {
	// Create a DataFrame with 100k rows and 100 groups
	n := 100000
	groups := make([]string, n)
	values := make([]float64, n)

	for i := 0; i < n; i++ {
		groups[i] = string(rune('A' + (i % 100)))
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesString("group", groups),
		NewSeriesFloat64("value", values),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.GroupBy("group").Sum("value")
	}
}

func BenchmarkGroupByMultipleAggs(b *testing.B) {
	n := 100000
	groups := make([]string, n)
	values := make([]float64, n)

	for i := 0; i < n; i++ {
		groups[i] = string(rune('A' + (i % 100)))
		values[i] = float64(i)
	}

	df, _ := NewDataFrame(
		NewSeriesString("group", groups),
		NewSeriesFloat64("value", values),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.GroupBy("group").Agg(
			AggSum("value"),
			AggMean("value"),
			AggMin("value"),
			AggMax("value"),
			AggCount(),
		)
	}
}
