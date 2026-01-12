package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Reshape Operations Example ===")

	// Example 1: Basic Pivot (Long to Wide)
	fmt.Println("Example 1: Basic Pivot - Long to Wide Format")
	fmt.Println("---------------------------------------------")

	// Long format sales data
	dates := []string{
		"2024-01-01", "2024-01-01", "2024-01-01",
		"2024-01-02", "2024-01-02", "2024-01-02",
		"2024-01-03", "2024-01-03", "2024-01-03",
	}
	products := []string{
		"Laptop", "Phone", "Tablet",
		"Laptop", "Phone", "Tablet",
		"Laptop", "Phone", "Tablet",
	}
	sales := []float64{
		1200.0, 800.0, 500.0,
		1500.0, 900.0, 550.0,
		1300.0, 850.0, 600.0,
	}

	longDF, err := galleon.NewDataFrame(
		galleon.NewSeriesString("date", dates),
		galleon.NewSeriesString("product", products),
		galleon.NewSeriesFloat64("sales", sales),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Original Long Format:")
	fmt.Println(longDF)
	fmt.Println()

	// Pivot: products as columns, dates as rows
	wideDF, err := longDF.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "date",
			Column: "product",
			Values: "sales",
			AggFn:  galleon.AggTypeSum,
		}).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Pivoted Wide Format (products as columns):")
	fmt.Println(wideDF)
	fmt.Println()

	// Example 2: Pivot with Different Aggregations
	fmt.Println("Example 2: Pivot with Aggregations")
	fmt.Println("----------------------------------")

	// Sales data with multiple transactions per day/product
	dates2 := []string{
		"2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01",
		"2024-01-02", "2024-01-02", "2024-01-02", "2024-01-02",
	}
	products2 := []string{
		"Laptop", "Laptop", "Phone", "Phone",
		"Laptop", "Laptop", "Phone", "Phone",
	}
	quantities := []float64{
		2.0, 3.0, 5.0, 4.0,
		1.0, 2.0, 6.0, 3.0,
	}

	df2, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("date", dates2),
		galleon.NewSeriesString("product", products2),
		galleon.NewSeriesFloat64("quantity", quantities),
	)

	fmt.Println("Original Data (multiple transactions):")
	fmt.Println(df2)
	fmt.Println()

	// Pivot with SUM aggregation
	sumPivot, _ := df2.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "date",
			Column: "product",
			Values: "quantity",
			AggFn:  galleon.AggTypeSum,
		}).
		Collect()

	fmt.Println("Pivot with SUM (total quantity per product per day):")
	fmt.Println(sumPivot)
	fmt.Println()

	// Pivot with MEAN aggregation
	meanPivot, _ := df2.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "date",
			Column: "product",
			Values: "quantity",
			AggFn:  galleon.AggTypeMean,
		}).
		Collect()

	fmt.Println("Pivot with MEAN (average quantity per transaction):")
	fmt.Println(meanPivot)
	fmt.Println()

	// Pivot with COUNT aggregation
	countPivot, _ := df2.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "date",
			Column: "product",
			Values: "quantity",
			AggFn:  galleon.AggTypeCount,
		}).
		Collect()

	fmt.Println("Pivot with COUNT (number of transactions):")
	fmt.Println(countPivot)
	fmt.Println()

	// Example 3: Basic Melt (Wide to Long)
	fmt.Println("Example 3: Basic Melt - Wide to Long Format")
	fmt.Println("-------------------------------------------")

	// Wide format data
	wideDates := []string{"2024-01-01", "2024-01-02", "2024-01-03"}
	laptopSales := []float64{1200.0, 1500.0, 1300.0}
	phoneSales := []float64{800.0, 900.0, 850.0}
	tabletSales := []float64{500.0, 550.0, 600.0}

	wideDF2, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("date", wideDates),
		galleon.NewSeriesFloat64("Laptop", laptopSales),
		galleon.NewSeriesFloat64("Phone", phoneSales),
		galleon.NewSeriesFloat64("Tablet", tabletSales),
	)

	fmt.Println("Original Wide Format:")
	fmt.Println(wideDF2)
	fmt.Println()

	// Melt to long format
	meltedDF, err := wideDF2.Lazy().
		Melt(galleon.MeltOptions{
			IDVars:    []string{"date"},
			ValueVars: []string{"Laptop", "Phone", "Tablet"},
			VarName:   "product",
			ValueName: "sales",
		}).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("Melted Long Format:")
	fmt.Println(meltedDF)
	fmt.Println()

	// Example 4: Melt with Auto-Detection
	fmt.Println("Example 4: Melt with Auto Value Detection")
	fmt.Println("-----------------------------------------")

	// When ValueVars is not specified, all non-ID columns are melted
	autoMelted, _ := wideDF2.Lazy().
		Melt(galleon.MeltOptions{
			IDVars:    []string{"date"},
			VarName:   "product",
			ValueName: "sales",
			// ValueVars omitted - auto-detects Laptop, Phone, Tablet
		}).
		Collect()

	fmt.Println("Auto-detected value columns:")
	fmt.Println(autoMelted)
	fmt.Println()

	// Example 5: Round-Trip Transformation
	fmt.Println("Example 5: Round-Trip (Melt → Pivot)")
	fmt.Println("------------------------------------")

	// Start with wide format
	original, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("region", []string{"North", "South", "East"}),
		galleon.NewSeriesFloat64("Q1", []float64{100.0, 150.0, 120.0}),
		galleon.NewSeriesFloat64("Q2", []float64{110.0, 160.0, 130.0}),
		galleon.NewSeriesFloat64("Q3", []float64{120.0, 170.0, 140.0}),
	)

	fmt.Println("Original Wide Format:")
	fmt.Println(original)
	fmt.Println()

	// Melt to long
	melted, _ := original.Lazy().
		Melt(galleon.MeltOptions{
			IDVars:    []string{"region"},
			VarName:   "quarter",
			ValueName: "revenue",
		}).
		Collect()

	fmt.Println("After Melt (long format):")
	fmt.Println(melted)
	fmt.Println()

	// Pivot back to wide
	pivoted, _ := melted.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "region",
			Column: "quarter",
			Values: "revenue",
			AggFn:  galleon.AggTypeSum,
		}).
		Collect()

	fmt.Println("After Pivot (back to wide):")
	fmt.Println(pivoted)
	fmt.Println()

	// Example 6: Sales Report Pivot
	fmt.Println("Example 6: Real-World Sales Report")
	fmt.Println("----------------------------------")

	// Transaction log data
	regions := []string{
		"North", "North", "North",
		"South", "South", "South",
		"East", "East", "East",
	}
	months := []string{
		"Jan", "Feb", "Mar",
		"Jan", "Feb", "Mar",
		"Jan", "Feb", "Mar",
	}
	revenue := []float64{
		10000.0, 12000.0, 11000.0,
		15000.0, 16000.0, 17000.0,
		12000.0, 13000.0, 14000.0,
	}

	salesLog, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("region", regions),
		galleon.NewSeriesString("month", months),
		galleon.NewSeriesFloat64("revenue", revenue),
	)

	fmt.Println("Transaction Log:")
	fmt.Println(salesLog)
	fmt.Println()

	// Create sales report with months as columns
	salesReport, _ := salesLog.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "region",
			Column: "month",
			Values: "revenue",
			AggFn:  galleon.AggTypeSum,
		}).
		Collect()

	fmt.Println("Sales Report (regions × months):")
	fmt.Println(salesReport)
	fmt.Println()

	// Example 7: Survey Data Transformation
	fmt.Println("Example 7: Survey Response Transformation")
	fmt.Println("-----------------------------------------")

	// Wide survey data (one row per respondent)
	respondents := []string{"Alice", "Bob", "Carol"}
	q1Scores := []float64{5.0, 4.0, 5.0}
	q2Scores := []float64{4.0, 5.0, 3.0}
	q3Scores := []float64{5.0, 5.0, 4.0}

	surveyWide, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("respondent", respondents),
		galleon.NewSeriesFloat64("question_1", q1Scores),
		galleon.NewSeriesFloat64("question_2", q2Scores),
		galleon.NewSeriesFloat64("question_3", q3Scores),
	)

	fmt.Println("Survey Data (wide format):")
	fmt.Println(surveyWide)
	fmt.Println()

	// Melt for analysis (one row per response)
	surveyLong, _ := surveyWide.Lazy().
		Melt(galleon.MeltOptions{
			IDVars:    []string{"respondent"},
			VarName:   "question",
			ValueName: "score",
		}).
		Collect()

	fmt.Println("Survey Data (long format for analysis):")
	fmt.Println(surveyLong)
	fmt.Println()

	// Example 8: Time Series Pivot
	fmt.Println("Example 8: Time Series Pivot")
	fmt.Println("----------------------------")

	// Sensor readings over time
	timestamps := []string{
		"09:00", "09:00", "09:00",
		"10:00", "10:00", "10:00",
		"11:00", "11:00", "11:00",
	}
	sensors := []string{
		"temp", "humidity", "pressure",
		"temp", "humidity", "pressure",
		"temp", "humidity", "pressure",
	}
	readings := []float64{
		22.5, 65.0, 1013.0,
		23.0, 63.0, 1012.0,
		23.5, 62.0, 1011.0,
	}

	sensorData, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("time", timestamps),
		galleon.NewSeriesString("sensor", sensors),
		galleon.NewSeriesFloat64("reading", readings),
	)

	fmt.Println("Sensor Readings (long format):")
	fmt.Println(sensorData)
	fmt.Println()

	// Pivot to have sensors as columns
	sensorWide, _ := sensorData.Lazy().
		Pivot(galleon.PivotOptions{
			Index:  "time",
			Column: "sensor",
			Values: "reading",
			AggFn:  galleon.AggTypeMean,
		}).
		Collect()

	fmt.Println("Sensor Readings (wide format with sensors as columns):")
	fmt.Println(sensorWide)
	fmt.Println()

	fmt.Println("✓ Reshape Operations Examples Complete!")
}
