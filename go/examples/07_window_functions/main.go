package main

import (
	"fmt"
	"math"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Window Functions Example ===\n")

	// Create sample time series data (daily stock prices)
	dates := []string{
		"2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
		"2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12",
	}
	closePrices := []float64{
		100.0, 102.5, 101.0, 105.0, 103.5,
		106.0, 108.5, 107.0, 110.0, 109.5,
	}
	volumes := []float64{
		1000, 1200, 950, 1500, 1100,
		1300, 1400, 1250, 1600, 1450,
	}

	df, err := galleon.NewDataFrame(
		galleon.NewSeriesString("date", dates),
		galleon.NewSeriesFloat64("close", closePrices),
		galleon.NewSeriesFloat64("volume", volumes),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Original Data:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: Shift Operations (Lag/Lead)
	fmt.Println("Example 1: Shift Operations")
	fmt.Println("---------------------------")

	result1, err := df.Lazy().
		WithColumn("prev_close", galleon.Col("close").Lag(1, 0.0)).
		WithColumn("next_close", galleon.Col("close").Lead(1, 0.0)).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("With Lag and Lead:")
	fmt.Println(result1)
	fmt.Println()

	// Example 2: Difference and Percent Change
	fmt.Println("Example 2: Price Changes")
	fmt.Println("------------------------")

	result2, err := df.Lazy().
		WithColumn("price_change", galleon.Col("close").Diff()).
		WithColumn("pct_change", galleon.Col("close").PctChange()).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Daily Changes:")
	fmt.Println(result2)
	fmt.Println()

	// Example 3: Cumulative Functions
	fmt.Println("Example 3: Cumulative Metrics")
	fmt.Println("-----------------------------")

	result3, err := df.Lazy().
		WithColumn("cumulative_volume", galleon.Col("volume").CumSum()).
		WithColumn("running_max_price", galleon.Col("close").CumMax()).
		WithColumn("running_min_price", galleon.Col("close").CumMin()).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Cumulative Metrics:")
	fmt.Println(result3)
	fmt.Println()

	// Example 4: Rolling Window Functions
	fmt.Println("Example 4: Moving Averages")
	fmt.Println("-------------------------")

	result4, err := df.Lazy().
		WithColumn("ma3", galleon.Col("close").RollingMean(3, 1)).       // 3-period MA, require at least 1 value
		WithColumn("ma5", galleon.Col("close").RollingMean(5, 3)).       // 5-period MA, require at least 3 values
		WithColumn("rolling_vol_3d", galleon.Col("volume").RollingSum(3, 1)). // 3-day rolling volume
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Moving Averages:")
	fmt.Println(result4)
	fmt.Println()

	// Example 5: Complete Technical Analysis
	fmt.Println("Example 5: Technical Analysis Dashboard")
	fmt.Println("---------------------------------------")

	technical, err := df.Lazy().
		// Price changes
		WithColumn("daily_return", galleon.Col("close").PctChange()).
		WithColumn("price_change", galleon.Col("close").Diff()).
		// Moving averages
		WithColumn("sma5", galleon.Col("close").RollingMean(5)).
		// Running metrics
		WithColumn("max_price_so_far", galleon.Col("close").CumMax()).
		// Volume analysis
		WithColumn("volume_ma3", galleon.Col("volume").RollingMean(3)).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Technical Analysis:")
	fmt.Println(technical)
	fmt.Println()

	// Example 6: Calculating Drawdown (manual calculation)
	fmt.Println("Example 6: Drawdown Analysis")
	fmt.Println("----------------------------")

	// First pass: get cumulative max
	withMax, _ := df.Lazy().
		WithColumn("running_max", galleon.Col("close").CumMax()).
		Collect()

	// Calculate drawdown manually (since chained arithmetic isn't supported)
	closeData := withMax.ColumnByName("close").Float64()
	maxData := withMax.ColumnByName("running_max").Float64()
	drawdownData := make([]float64, len(closeData))
	for i := range closeData {
		if maxData[i] > 0 {
			drawdownData[i] = ((closeData[i] - maxData[i]) / maxData[i]) * 100.0
		}
	}

	drawdownDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("date", dates),
		galleon.NewSeriesFloat64("close", closeData),
		galleon.NewSeriesFloat64("running_max", maxData),
		galleon.NewSeriesFloat64("drawdown", drawdownData),
	)

	fmt.Println("Drawdown Analysis (% from peak):")
	fmt.Println(drawdownDF)
	fmt.Println()

	// Example 7: Week-over-Week Comparison
	fmt.Println("Example 7: Week-over-Week Analysis")
	fmt.Println("----------------------------------")

	// Calculate 5-day (week) changes
	weekOverWeek, err := df.Lazy().
		WithColumn("price_5d_ago", galleon.Col("close").Lag(5, 0.0)).
		WithColumn("wow_change", galleon.Col("close").DiffN(5)).
		Collect()
	if err != nil {
		panic(err)
	}

	// Calculate percent change manually for non-zero values
	wowData := weekOverWeek.ColumnByName("wow_change").Float64()
	prevData := weekOverWeek.ColumnByName("price_5d_ago").Float64()
	pctWow := make([]float64, len(wowData))
	for i := range wowData {
		if prevData[i] > 0 {
			pctWow[i] = (wowData[i] / prevData[i]) * 100
		} else {
			pctWow[i] = math.NaN()
		}
	}

	finalWow, _ := weekOverWeek.WithColumnSeries(
		galleon.NewSeriesFloat64("wow_pct_change", pctWow),
	)

	fmt.Println("Week-over-Week Comparison:")
	fmt.Println(finalWow)
	fmt.Println()

	// Example 8: Simple Volatility (using Std aggregation)
	fmt.Println("Example 8: Volatility Analysis")
	fmt.Println("------------------------------")

	// Calculate returns first
	withReturns, _ := df.Lazy().
		WithColumn("return", galleon.Col("close").PctChange()).
		Collect()

	// Get the returns data
	returns := withReturns.ColumnByName("return").Float64()

	// Calculate simple statistics on returns
	returnSeries := galleon.NewSeriesFloat64("returns", returns[1:]) // Skip first NaN
	stats := returnSeries.Describe()

	fmt.Println("Return Statistics:")
	fmt.Printf("  Mean Return: %.4f\n", stats["mean"])
	fmt.Printf("  Std Dev (Volatility): %.4f\n", stats["std"])
	fmt.Printf("  Min Return: %.4f\n", stats["min"])
	fmt.Printf("  Max Return: %.4f\n", stats["max"])
	fmt.Println()

	fmt.Println("Returns Data:")
	fmt.Println(withReturns)
	fmt.Println()

	fmt.Println("Window Functions Examples Complete!")
}
