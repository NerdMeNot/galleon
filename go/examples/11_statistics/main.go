package main

import (
	"fmt"
	"math"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Advanced Statistics Example ===\n")

	// Example 1: Basic Statistical Aggregations
	fmt.Println("Example 1: Complete Statistical Summary")
	fmt.Println("---------------------------------------")

	// Sample dataset - exam scores
	scores := []float64{65, 72, 68, 85, 92, 78, 88, 76, 82, 90, 74, 80, 86, 94, 70}
	students := make([]int64, len(scores))
	for i := range students {
		students[i] = int64(i + 1)
	}

	scoresDF, err := galleon.NewDataFrame(
		galleon.NewSeriesInt64("student_id", students),
		galleon.NewSeriesFloat64("score", scores),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Exam Scores:")
	fmt.Println(scoresDF)
	fmt.Println()

	// Compute comprehensive statistics
	stats, err := scoresDF.Lazy().
		Select(
			galleon.Col("score").Count().Alias("count"),
			galleon.Col("score").Mean().Alias("mean"),
			galleon.Col("score").Median().Alias("median"),
			galleon.Col("score").Std().Alias("std_dev"),
			galleon.Col("score").Min().Alias("min"),
			galleon.Col("score").Max().Alias("max"),
			galleon.Col("score").Quantile(0.25).Alias("q25"),
			galleon.Col("score").Quantile(0.75).Alias("q75"),
			galleon.Col("score").Skewness().Alias("skewness"),
			galleon.Col("score").Kurtosis().Alias("kurtosis"),
		).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Statistical Summary:")
	fmt.Println(stats)
	fmt.Println()

	// Example 2: Quantile Analysis
	fmt.Println("Example 2: Quantile Analysis")
	fmt.Println("----------------------------")

	// Income data
	incomes := []float64{
		25000, 30000, 35000, 40000, 45000, 50000, 55000, 60000,
		65000, 70000, 80000, 90000, 100000, 120000, 150000,
	}

	incomeDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesFloat64("income", incomes),
	)

	fmt.Println("Income Distribution:")
	fmt.Println(incomeDF)
	fmt.Println()

	// Calculate key percentiles
	percentiles, _ := incomeDF.Lazy().
		Select(
			galleon.Col("income").Min().Alias("minimum"),
			galleon.Col("income").Quantile(0.25).Alias("p25_lower_quartile"),
			galleon.Col("income").Median().Alias("p50_median"),
			galleon.Col("income").Quantile(0.75).Alias("p75_upper_quartile"),
			galleon.Col("income").Quantile(0.90).Alias("p90"),
			galleon.Col("income").Quantile(0.95).Alias("p95"),
			galleon.Col("income").Max().Alias("maximum"),
		).
		Collect()

	fmt.Println("Income Percentiles:")
	fmt.Println(percentiles)
	fmt.Println()

	// Example 3: Distribution Shape Analysis
	fmt.Println("Example 3: Distribution Shape (Skewness & Kurtosis)")
	fmt.Println("---------------------------------------------------")

	// Three different distributions
	normalDist := []float64{50, 52, 48, 51, 49, 50, 48, 52, 49, 51}
	rightSkewed := []float64{10, 12, 11, 13, 15, 20, 25, 30, 45, 60}
	leftSkewed := []float64{60, 55, 50, 48, 45, 40, 35, 30, 25, 20}

	distNames := []string{
		"Normal", "Normal", "Normal", "Normal", "Normal",
		"Normal", "Normal", "Normal", "Normal", "Normal",
		"RightSkewed", "RightSkewed", "RightSkewed", "RightSkewed", "RightSkewed",
		"RightSkewed", "RightSkewed", "RightSkewed", "RightSkewed", "RightSkewed",
		"LeftSkewed", "LeftSkewed", "LeftSkewed", "LeftSkewed", "LeftSkewed",
		"LeftSkewed", "LeftSkewed", "LeftSkewed", "LeftSkewed", "LeftSkewed",
	}
	distValues := append(append(normalDist, rightSkewed...), leftSkewed...)

	distDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("distribution", distNames),
		galleon.NewSeriesFloat64("value", distValues),
	)

	fmt.Println("Sample Data:")
	fmt.Println(distDF.Head(5))
	fmt.Println("...")
	fmt.Println()

	// Analyze shape by distribution type (GroupBy takes strings)
	shapeAnalysis, _ := distDF.Lazy().
		GroupBy("distribution").
		Agg(
			galleon.Col("value").Mean().Alias("mean"),
			galleon.Col("value").Median().Alias("median"),
			galleon.Col("value").Skewness().Alias("skewness"),
			galleon.Col("value").Kurtosis().Alias("kurtosis"),
		).
		Collect()

	fmt.Println("Distribution Shape Analysis:")
	fmt.Println(shapeAnalysis)
	fmt.Println()
	fmt.Println("Interpretation:")
	fmt.Println("  Skewness > 0: Right-skewed (tail on right)")
	fmt.Println("  Skewness < 0: Left-skewed (tail on left)")
	fmt.Println("  Skewness ~ 0: Symmetric")
	fmt.Println("  Kurtosis > 0: Heavy-tailed (outliers)")
	fmt.Println("  Kurtosis < 0: Light-tailed")
	fmt.Println()

	// Example 4: Correlation Analysis
	fmt.Println("Example 4: Correlation Analysis")
	fmt.Println("-------------------------------")

	// Study hours vs exam scores
	studyHours := []float64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	examScores := []float64{55, 60, 65, 70, 75, 80, 85, 90, 92, 95}

	studyDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesFloat64("study_hours", studyHours),
		galleon.NewSeriesFloat64("exam_score", examScores),
	)

	fmt.Println("Study Hours vs Exam Scores:")
	fmt.Println(studyDF)
	fmt.Println()

	// Calculate correlation manually using Series methods
	studySeries := studyDF.ColumnByName("study_hours")
	scoreSeries := studyDF.ColumnByName("exam_score")

	// Get data and means
	studyData := studySeries.Float64()
	scoreData := scoreSeries.Float64()
	studyMean := studySeries.Mean()
	scoreMean := scoreSeries.Mean()

	// Calculate standard deviations manually
	var studyVar, scoreVar float64
	for i := range studyData {
		studyVar += (studyData[i] - studyMean) * (studyData[i] - studyMean)
		scoreVar += (scoreData[i] - scoreMean) * (scoreData[i] - scoreMean)
	}
	n := float64(len(studyData) - 1)
	studyStd := math.Sqrt(studyVar / n)
	scoreStd := math.Sqrt(scoreVar / n)

	// Calculate covariance and Pearson correlation
	var covariance float64
	for i := range studyData {
		covariance += (studyData[i] - studyMean) * (scoreData[i] - scoreMean)
	}
	covariance /= n
	correlation := covariance / (studyStd * scoreStd)

	fmt.Printf("Correlation between Study Hours and Exam Score: %.4f\n", correlation)
	fmt.Println()
	fmt.Println("Interpretation:")
	fmt.Println("  Correlation ~ 1: Strong positive relationship")
	fmt.Println("  Correlation ~ 0: No relationship")
	fmt.Println("  Correlation ~ -1: Strong negative relationship")
	fmt.Println()

	// Example 5: Group Statistics
	fmt.Println("Example 5: Statistical Analysis by Group")
	fmt.Println("----------------------------------------")

	// Sales performance across regions
	regions := []string{
		"North", "North", "North", "North", "North",
		"South", "South", "South", "South", "South",
		"East", "East", "East", "East", "East",
		"West", "West", "West", "West", "West",
	}
	salesValues := []float64{
		100, 120, 110, 130, 115,
		150, 160, 155, 170, 165,
		90, 95, 85, 100, 92,
		140, 135, 145, 138, 142,
	}

	salesDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("region", regions),
		galleon.NewSeriesFloat64("sales", salesValues),
	)

	fmt.Println("Sales by Region (sample):")
	fmt.Println(salesDF.Head(10))
	fmt.Println()

	// Comprehensive statistics by region (GroupBy takes strings)
	regionalStats, _ := salesDF.Lazy().
		GroupBy("region").
		Agg(
			galleon.Col("sales").Count().Alias("count"),
			galleon.Col("sales").Mean().Alias("mean"),
			galleon.Col("sales").Median().Alias("median"),
			galleon.Col("sales").Std().Alias("std_dev"),
			galleon.Col("sales").Min().Alias("min"),
			galleon.Col("sales").Max().Alias("max"),
			galleon.Col("sales").Quantile(0.25).Alias("q25"),
			galleon.Col("sales").Quantile(0.75).Alias("q75"),
		).
		Collect()

	fmt.Println("Regional Sales Statistics:")
	fmt.Println(regionalStats)
	fmt.Println()

	// Example 6: Outlier Detection
	fmt.Println("Example 6: Outlier Detection using IQR")
	fmt.Println("--------------------------------------")

	// Response times (some with outliers)
	responseTimes := []float64{
		100, 105, 98, 102, 110, 95, 108, 103, 99, 107,
		101, 104, 250, 106, 97, 109, 102, 300, 105, 98,
	}

	responseDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesFloat64("response_time_ms", responseTimes),
	)

	fmt.Println("Response Times (ms):")
	fmt.Println(responseDF)
	fmt.Println()

	// Calculate IQR for outlier detection
	iqrStats, _ := responseDF.Lazy().
		Select(
			galleon.Col("response_time_ms").Quantile(0.25).Alias("q25"),
			galleon.Col("response_time_ms").Quantile(0.75).Alias("q75"),
		).
		Collect()

	fmt.Println("IQR Statistics:")
	fmt.Println(iqrStats)
	fmt.Println()

	q25 := iqrStats.ColumnByName("q25").Float64()[0]
	q75 := iqrStats.ColumnByName("q75").Float64()[0]
	iqr := q75 - q25
	lowerBound := q25 - 1.5*iqr
	upperBound := q75 + 1.5*iqr

	fmt.Printf("IQR: %.2f\n", iqr)
	fmt.Printf("Lower Bound: %.2f\n", lowerBound)
	fmt.Printf("Upper Bound: %.2f\n", upperBound)
	fmt.Println()

	// Find outliers manually (filter values outside bounds)
	fmt.Println("Detected Outliers:")
	for _, v := range responseTimes {
		if v < lowerBound || v > upperBound {
			fmt.Printf("  %.2f ms (outlier)\n", v)
		}
	}
	fmt.Println()

	// Example 7: Performance Benchmarking
	fmt.Println("Example 7: Performance Benchmarking Analysis")
	fmt.Println("--------------------------------------------")

	// Algorithm execution times
	algorithms := []string{
		"Algo_A", "Algo_A", "Algo_A", "Algo_A", "Algo_A",
		"Algo_B", "Algo_B", "Algo_B", "Algo_B", "Algo_B",
		"Algo_C", "Algo_C", "Algo_C", "Algo_C", "Algo_C",
	}
	execTimes := []float64{
		100, 105, 98, 102, 103,
		85, 88, 82, 90, 87,
		120, 125, 118, 122, 121,
	}

	benchmarkDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("algorithm", algorithms),
		galleon.NewSeriesFloat64("execution_time_ms", execTimes),
	)

	fmt.Println("Benchmark Data:")
	fmt.Println(benchmarkDF)
	fmt.Println()

	// Statistical comparison (GroupBy and Sort take strings)
	benchmarkStats, _ := benchmarkDF.Lazy().
		GroupBy("algorithm").
		Agg(
			galleon.Col("execution_time_ms").Mean().Alias("avg_time"),
			galleon.Col("execution_time_ms").Median().Alias("median_time"),
			galleon.Col("execution_time_ms").Std().Alias("std_dev"),
			galleon.Col("execution_time_ms").Min().Alias("best_time"),
			galleon.Col("execution_time_ms").Max().Alias("worst_time"),
			galleon.Col("execution_time_ms").Quantile(0.95).Alias("p95"),
		).
		Sort("median_time", true).
		Collect()

	fmt.Println("Benchmark Results (sorted by median):")
	fmt.Println(benchmarkStats)
	fmt.Println()

	// Example 8: Quality Control Analysis
	fmt.Println("Example 8: Quality Control - Process Capability")
	fmt.Println("-----------------------------------------------")

	// Manufacturing measurements (target: 100mm +/- 2mm)
	measurements := []float64{
		99.8, 100.2, 99.9, 100.1, 100.0, 99.7, 100.3, 99.9,
		100.2, 99.8, 100.1, 99.9, 100.0, 100.2, 99.8, 100.1,
	}

	qcDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesFloat64("measurement_mm", measurements),
	)

	fmt.Println("Manufacturing Measurements (mm):")
	fmt.Println(qcDF)
	fmt.Println()

	// Process capability statistics
	qcStats, _ := qcDF.Lazy().
		Select(
			galleon.Col("measurement_mm").Count().Alias("sample_size"),
			galleon.Col("measurement_mm").Mean().Alias("mean"),
			galleon.Col("measurement_mm").Std().Alias("std_dev"),
			galleon.Col("measurement_mm").Min().Alias("min"),
			galleon.Col("measurement_mm").Max().Alias("max"),
			galleon.Col("measurement_mm").Quantile(0.01).Alias("p1"),
			galleon.Col("measurement_mm").Quantile(0.99).Alias("p99"),
		).
		Collect()

	fmt.Println("Process Statistics:")
	fmt.Println(qcStats)
	fmt.Println()

	mean := qcStats.ColumnByName("mean").Float64()[0]
	stdDev := qcStats.ColumnByName("std_dev").Float64()[0]
	target := 100.0
	tolerance := 2.0

	fmt.Printf("Target: %.1f mm +/- %.1f mm\n", target, tolerance)
	fmt.Printf("Actual Mean: %.2f mm\n", mean)
	fmt.Printf("Std Dev: %.3f mm\n", stdDev)
	fmt.Printf("Range: [%.2f, %.2f]\n",
		qcStats.ColumnByName("min").Float64()[0],
		qcStats.ColumnByName("max").Float64()[0])
	fmt.Println()

	// Check if process is in control (mean close to target, low variation)
	if mean > target-0.5 && mean < target+0.5 && stdDev < 0.5 {
		fmt.Println("Process is in control (mean near target, low variation)")
	} else {
		fmt.Println("Process needs adjustment")
	}
	fmt.Println()

	fmt.Println("Advanced Statistics Examples Complete!")
	fmt.Println("\nKey Takeaways:")
	fmt.Println("- Median is robust to outliers, mean is sensitive")
	fmt.Println("- Quantiles provide detailed distribution understanding")
	fmt.Println("- Skewness measures distribution asymmetry")
	fmt.Println("- Kurtosis measures tail heaviness")
	fmt.Println("- IQR method is effective for outlier detection")
	fmt.Println("- GroupBy enables comparative statistical analysis")
}
