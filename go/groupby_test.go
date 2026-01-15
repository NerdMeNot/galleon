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

// Additional tests for coverage

func TestGroupByNoKeys(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("value", []float64{1, 2, 3}),
	)

	gb := df.GroupBy()
	_, err := gb.Sum("value")
	if err == nil {
		t.Error("expected error for GroupBy with no keys")
	}
}

func TestGroupByFloat32Key(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat32("key", []float32{1.0, 2.0, 1.0, 2.0}),
		NewSeriesFloat64("value", []float64{10, 20, 30, 40}),
	)

	result, err := df.GroupBy("key").Sum("value")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 groups, got %d", result.Height())
	}
}

func TestGroupByInt32Key(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesInt32("key", []int32{1, 2, 1, 2}),
		NewSeriesFloat64("value", []float64{10, 20, 30, 40}),
	)

	result, err := df.GroupBy("key").Sum("value")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 groups, got %d", result.Height())
	}
}

func TestGroupByBoolKey(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesBool("active", []bool{true, false, true, false, true}),
		NewSeriesFloat64("score", []float64{10, 20, 30, 40, 50}),
	)

	result, err := df.GroupBy("active").Sum("score")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	if result.Height() != 2 {
		t.Errorf("expected 2 groups, got %d", result.Height())
	}

	// Verify sums: true = 10+30+50 = 90, false = 20+40 = 60
	activeCol := result.ColumnByName("active")
	sumCol := result.ColumnByName("score_sum")

	for i := 0; i < result.Height(); i++ {
		active := activeCol.Bool()[i]
		sum, _ := sumCol.GetFloat64(i)

		if active && sum != 90 {
			t.Errorf("true sum: expected 90, got %f", sum)
		}
		if !active && sum != 60 {
			t.Errorf("false sum: expected 60, got %f", sum)
		}
	}
}

func TestGroupBySumFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "B", "A", "B"}),
		NewSeriesFloat32("value", []float32{1.0, 2.0, 3.0, 4.0}),
	)

	result, err := df.GroupBy("group").Sum("value")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	groupCol := result.ColumnByName("group")
	sumCol := result.ColumnByName("value_sum")

	for i := 0; i < result.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		sum, _ := sumCol.GetFloat64(i)

		switch grp {
		case "A":
			if sum != 4 { // 1 + 3
				t.Errorf("A sum: expected 4, got %f", sum)
			}
		case "B":
			if sum != 6 { // 2 + 4
				t.Errorf("B sum: expected 6, got %f", sum)
			}
		}
	}
}

func TestGroupBySumInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "B", "A", "B"}),
		NewSeriesInt32("value", []int32{1, 2, 3, 4}),
	)

	result, err := df.GroupBy("group").Sum("value")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	groupCol := result.ColumnByName("group")
	sumCol := result.ColumnByName("value_sum")

	for i := 0; i < result.Height(); i++ {
		grp, _ := groupCol.GetString(i)
		sum, _ := sumCol.GetFloat64(i)

		switch grp {
		case "A":
			if sum != 4 {
				t.Errorf("A sum: expected 4, got %f", sum)
			}
		case "B":
			if sum != 6 {
				t.Errorf("B sum: expected 6, got %f", sum)
			}
		}
	}
}

func TestGroupByMeanFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A"}),
		NewSeriesFloat32("value", []float32{2.0, 4.0}),
	)

	result, err := df.GroupBy("group").Mean("value")
	if err != nil {
		t.Fatalf("failed to compute mean: %v", err)
	}

	meanCol := result.ColumnByName("value_mean")
	mean, _ := meanCol.GetFloat64(0)

	if mean != 3.0 {
		t.Errorf("expected mean 3.0, got %f", mean)
	}
}

func TestGroupByMeanInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A"}),
		NewSeriesInt32("value", []int32{2, 4}),
	)

	result, err := df.GroupBy("group").Mean("value")
	if err != nil {
		t.Fatalf("failed to compute mean: %v", err)
	}

	meanCol := result.ColumnByName("value_mean")
	mean, _ := meanCol.GetFloat64(0)

	if mean != 3.0 {
		t.Errorf("expected mean 3.0, got %f", mean)
	}
}

func TestGroupByMinFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesFloat32("value", []float32{3.0, 1.0, 2.0}),
	)

	result, err := df.GroupBy("group").Min("value")
	if err != nil {
		t.Fatalf("failed to compute min: %v", err)
	}

	minCol := result.ColumnByName("value_min")
	min, _ := minCol.GetFloat64(0)

	if min != 1.0 {
		t.Errorf("expected min 1.0, got %f", min)
	}
}

func TestGroupByMinInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesInt32("value", []int32{3, 1, 2}),
	)

	result, err := df.GroupBy("group").Min("value")
	if err != nil {
		t.Fatalf("failed to compute min: %v", err)
	}

	minCol := result.ColumnByName("value_min")
	min := minCol.Int32()[0]

	if min != 1 {
		t.Errorf("expected min 1, got %d", min)
	}
}

func TestGroupByMinString(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesString("value", []string{"cherry", "apple", "banana"}),
	)

	result, err := df.GroupBy("group").Min("value")
	if err != nil {
		t.Fatalf("failed to compute min: %v", err)
	}

	minCol := result.ColumnByName("value_min")
	min := minCol.Strings()[0]

	if min != "apple" {
		t.Errorf("expected min 'apple', got '%s'", min)
	}
}

func TestGroupByMaxFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesFloat32("value", []float32{1.0, 3.0, 2.0}),
	)

	result, err := df.GroupBy("group").Max("value")
	if err != nil {
		t.Fatalf("failed to compute max: %v", err)
	}

	maxCol := result.ColumnByName("value_max")
	max, _ := maxCol.GetFloat64(0)

	if max != 3.0 {
		t.Errorf("expected max 3.0, got %f", max)
	}
}

func TestGroupByMaxInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesInt32("value", []int32{1, 3, 2}),
	)

	result, err := df.GroupBy("group").Max("value")
	if err != nil {
		t.Fatalf("failed to compute max: %v", err)
	}

	maxCol := result.ColumnByName("value_max")
	max := maxCol.Int32()[0]

	if max != 3 {
		t.Errorf("expected max 3, got %d", max)
	}
}

func TestGroupByMaxString(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesString("value", []string{"cherry", "apple", "banana"}),
	)

	result, err := df.GroupBy("group").Max("value")
	if err != nil {
		t.Fatalf("failed to compute max: %v", err)
	}

	maxCol := result.ColumnByName("value_max")
	max := maxCol.Strings()[0]

	if max != "cherry" {
		t.Errorf("expected max 'cherry', got '%s'", max)
	}
}

func TestGroupByFirstFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesFloat32("value", []float32{1.0, 2.0, 3.0}),
	)

	result, err := df.GroupBy("group").First("value")
	if err != nil {
		t.Fatalf("failed to compute first: %v", err)
	}

	firstCol := result.ColumnByName("value_first")
	first := firstCol.Float32()[0]

	if first != 1.0 {
		t.Errorf("expected first 1.0, got %f", first)
	}
}

func TestGroupByFirstInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesInt32("value", []int32{1, 2, 3}),
	)

	result, err := df.GroupBy("group").First("value")
	if err != nil {
		t.Fatalf("failed to compute first: %v", err)
	}

	firstCol := result.ColumnByName("value_first")
	first := firstCol.Int32()[0]

	if first != 1 {
		t.Errorf("expected first 1, got %d", first)
	}
}

func TestGroupByFirstBool(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesBool("value", []bool{true, false, true}),
	)

	result, err := df.GroupBy("group").First("value")
	if err != nil {
		t.Fatalf("failed to compute first: %v", err)
	}

	firstCol := result.ColumnByName("value_first")
	first := firstCol.Bool()[0]

	if !first {
		t.Errorf("expected first true, got %v", first)
	}
}

func TestGroupByLastFloat32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesFloat32("value", []float32{1.0, 2.0, 3.0}),
	)

	result, err := df.GroupBy("group").Last("value")
	if err != nil {
		t.Fatalf("failed to compute last: %v", err)
	}

	lastCol := result.ColumnByName("value_last")
	last := lastCol.Float32()[0]

	if last != 3.0 {
		t.Errorf("expected last 3.0, got %f", last)
	}
}

func TestGroupByLastInt32(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesInt32("value", []int32{1, 2, 3}),
	)

	result, err := df.GroupBy("group").Last("value")
	if err != nil {
		t.Fatalf("failed to compute last: %v", err)
	}

	lastCol := result.ColumnByName("value_last")
	last := lastCol.Int32()[0]

	if last != 3 {
		t.Errorf("expected last 3, got %d", last)
	}
}

func TestGroupByLastBool(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "A"}),
		NewSeriesBool("value", []bool{true, false, true}),
	)

	result, err := df.GroupBy("group").Last("value")
	if err != nil {
		t.Fatalf("failed to compute last: %v", err)
	}

	lastCol := result.ColumnByName("value_last")
	last := lastCol.Bool()[0]

	if !last {
		t.Errorf("expected last true, got %v", last)
	}
}

func TestGroupByInvalidKeyColumn(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "B"}),
		NewSeriesFloat64("value", []float64{1, 2}),
	)

	gb := df.GroupBy("nonexistent")
	if gb.NumGroups() != 0 {
		t.Error("expected 0 groups for invalid key column")
	}
}

func TestGroupByUnsupportedSumDtype(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "B"}),
		NewSeriesString("value", []string{"x", "y"}),
	)

	_, err := df.GroupBy("group").Sum("value")
	if err == nil {
		t.Error("expected error for sum on string column")
	}
}

func TestGroupByUnsupportedMeanDtype(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "B"}),
		NewSeriesString("value", []string{"x", "y"}),
	)

	_, err := df.GroupBy("group").Mean("value")
	if err == nil {
		t.Error("expected error for mean on string column")
	}
}

func TestGroupByMultiKeyFloat64(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesFloat64("key1", []float64{1.0, 1.0, 2.0, 2.0}),
		NewSeriesFloat64("key2", []float64{10.0, 20.0, 10.0, 20.0}),
		NewSeriesFloat64("value", []float64{100, 200, 300, 400}),
	)

	result, err := df.GroupBy("key1", "key2").Sum("value")
	if err != nil {
		t.Fatalf("failed to compute sum: %v", err)
	}

	if result.Height() != 4 {
		t.Errorf("expected 4 groups, got %d", result.Height())
	}
}

func TestGroupByToFloat64Conversion(t *testing.T) {
	// Test toFloat64 helper with various types
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A", "A", "B", "B"}),
		NewSeriesFloat32("val_f32", []float32{1.5, 2.5, 3.5, 4.5}),
		NewSeriesInt32("val_i32", []int32{1, 2, 3, 4}),
		NewSeriesInt64("val_i64", []int64{10, 20, 30, 40}),
	)

	// Variance and Std use toFloat64 internally
	_, err := df.GroupBy("group").Var("val_f32")
	if err != nil {
		t.Fatalf("failed to compute variance for float32: %v", err)
	}

	_, err = df.GroupBy("group").Var("val_i32")
	if err != nil {
		t.Fatalf("failed to compute variance for int32: %v", err)
	}

	_, err = df.GroupBy("group").Var("val_i64")
	if err != nil {
		t.Fatalf("failed to compute variance for int64: %v", err)
	}
}

func TestGroupBySingleRow(t *testing.T) {
	df, _ := NewDataFrame(
		NewSeriesString("group", []string{"A"}),
		NewSeriesFloat64("value", []float64{100}),
	)

	// Std/Var of single element should be NaN
	stdResult, err := df.GroupBy("group").Std("value")
	if err != nil {
		t.Fatalf("failed to compute std: %v", err)
	}

	stdCol := stdResult.ColumnByName("value_std")
	std, _ := stdCol.GetFloat64(0)

	if !math.IsNaN(std) {
		t.Errorf("expected NaN for std of single element, got %f", std)
	}
}

func TestGroupByCategoricalKey(t *testing.T) {
	// Test GroupBy with categorical key column
	df, err := NewDataFrame(
		NewSeriesCategorical("fruit", []string{"apple", "banana", "apple", "cherry", "banana", "apple"}),
		NewSeriesFloat64("value", []float64{1, 2, 3, 4, 5, 6}),
	)
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}

	result, err := df.GroupBy("fruit").Sum("value")
	if err != nil {
		t.Fatalf("GroupBy Sum failed: %v", err)
	}

	// Should have 3 groups: apple, banana, cherry
	if result.Height() != 3 {
		t.Errorf("Expected 3 groups, got %d", result.Height())
	}

	// The key column should still be categorical
	keyCol := result.ColumnByName("fruit")
	if keyCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype for key column, got %s", keyCol.DType())
	}
}

func TestGroupByCategoricalKeySum(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesCategorical("category", []string{"A", "B", "A", "B", "A"}),
		NewSeriesFloat64("value", []float64{10, 20, 30, 40, 50}),
	)
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}

	result, err := df.GroupBy("category").Sum("value")
	if err != nil {
		t.Fatalf("GroupBy Sum failed: %v", err)
	}

	// Verify sums: A = 10+30+50 = 90, B = 20+40 = 60
	sumCol := result.ColumnByName("value_sum")
	keyCol := result.ColumnByName("category")

	for i := 0; i < result.Height(); i++ {
		key := keyCol.Get(i)
		val, _ := sumCol.GetFloat64(i)
		switch key {
		case "A":
			if val != 90 {
				t.Errorf("Sum for A: expected 90, got %f", val)
			}
		case "B":
			if val != 60 {
				t.Errorf("Sum for B: expected 60, got %f", val)
			}
		}
	}
}

func TestGroupByCategoricalFirstLast(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesCategorical("group", []string{"X", "Y", "X", "Y"}),
		NewSeriesCategorical("fruit", []string{"apple", "banana", "cherry", "date"}),
	)
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}

	// Test First aggregation on categorical column
	firstResult, err := df.GroupBy("group").First("fruit")
	if err != nil {
		t.Fatalf("GroupBy First failed: %v", err)
	}

	if firstResult.Height() != 2 {
		t.Errorf("Expected 2 groups, got %d", firstResult.Height())
	}

	// Result column should be categorical
	fruitCol := firstResult.ColumnByName("fruit_first")
	if fruitCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype for first result, got %s", fruitCol.DType())
	}

	// Test Last aggregation
	lastResult, err := df.GroupBy("group").Last("fruit")
	if err != nil {
		t.Fatalf("GroupBy Last failed: %v", err)
	}

	lastFruitCol := lastResult.ColumnByName("fruit_last")
	if lastFruitCol.DType() != Categorical {
		t.Errorf("Expected Categorical dtype for last result, got %s", lastFruitCol.DType())
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
