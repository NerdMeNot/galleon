// Example 03: GroupBy Operations
//
// This example demonstrates:
// - GroupBy with various aggregations
// - Multiple aggregations at once
// - GroupBy with multiple keys
//
// Run: go run main.go

package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Galleon GroupBy Example ===\n")

	// Create sample sales data
	df, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("region", []string{
			"North", "South", "North", "South", "East", "West",
			"North", "South", "East", "West", "North", "East",
		}),
		galleon.NewSeriesString("product", []string{
			"A", "A", "B", "B", "A", "A",
			"B", "A", "B", "B", "A", "A",
		}),
		galleon.NewSeriesFloat64("sales", []float64{
			100.5, 200.3, 150.7, 180.2, 220.1, 175.8,
			130.4, 195.6, 210.9, 165.3, 185.7, 240.2,
		}),
		galleon.NewSeriesInt64("quantity", []int64{
			10, 20, 15, 18, 22, 17, 13, 19, 21, 16, 18, 24,
		}),
	)

	fmt.Printf("Sales Data: %d records\n", df.Height())
	printDataFrame(df)

	// =========================================================================
	// Basic GroupBy - Sum
	// =========================================================================
	fmt.Println("\n1. GroupBy Region - Sum of Sales")
	fmt.Println("-" + string(make([]byte, 40)))

	grouped := df.GroupBy("region")
	sumResult := grouped.Sum("sales")
	printDataFrame(sumResult)

	// =========================================================================
	// GroupBy - Count
	// =========================================================================
	fmt.Println("\n2. GroupBy Region - Count")
	fmt.Println("-" + string(make([]byte, 40)))

	countResult := grouped.Count()
	printDataFrame(countResult)

	// =========================================================================
	// GroupBy - Mean
	// =========================================================================
	fmt.Println("\n3. GroupBy Region - Mean Sales")
	fmt.Println("-" + string(make([]byte, 40)))

	meanResult := grouped.Mean("sales")
	printDataFrame(meanResult)

	// =========================================================================
	// GroupBy - Min/Max
	// =========================================================================
	fmt.Println("\n4. GroupBy Region - Min and Max Sales")
	fmt.Println("-" + string(make([]byte, 40)))

	minResult := grouped.Min("sales")
	fmt.Println("Minimum:")
	printDataFrame(minResult)

	maxResult := grouped.Max("sales")
	fmt.Println("\nMaximum:")
	printDataFrame(maxResult)

	// =========================================================================
	// Multiple Aggregations
	// =========================================================================
	fmt.Println("\n5. Multiple Aggregations")
	fmt.Println("-" + string(make([]byte, 40)))

	multiAgg := df.GroupBy("region").Agg(
		galleon.Col("sales").Sum().Alias("total_sales"),
		galleon.Col("sales").Mean().Alias("avg_sales"),
		galleon.Col("quantity").Sum().Alias("total_qty"),
	)
	printDataFrame(multiAgg)

	// =========================================================================
	// GroupBy Multiple Keys
	// =========================================================================
	fmt.Println("\n6. GroupBy Multiple Keys (region, product)")
	fmt.Println("-" + string(make([]byte, 40)))

	multiKey := df.GroupBy("region", "product").Sum("sales")
	printDataFrame(multiKey)

	// =========================================================================
	// Chained Operations
	// =========================================================================
	fmt.Println("\n7. Chained: Filter + GroupBy + Sort")
	fmt.Println("-" + string(make([]byte, 40)))

	// Get total sales by region for sales > 150, sorted by total
	result := df.
		Filter(galleon.Col("sales").Gt(galleon.Lit(150.0))).
		GroupBy("region").
		Agg(galleon.Col("sales").Sum().Alias("total_sales")).
		Sort("total_sales", false)

	fmt.Println("Total sales by region (where sales > 150):")
	printDataFrame(result)

	fmt.Println("\n=== Example Complete ===")
}

func printDataFrame(df *galleon.DataFrame) {
	if df == nil {
		fmt.Println("  (nil DataFrame)")
		return
	}

	// Print header
	cols := df.ColumnNames()
	fmt.Print("  ")
	for _, name := range cols {
		fmt.Printf("%-15s", name)
	}
	fmt.Println()

	// Print separator
	fmt.Print("  ")
	for range cols {
		fmt.Print("---------------")
	}
	fmt.Println()

	// Print rows
	for i := 0; i < df.Height(); i++ {
		fmt.Print("  ")
		for _, name := range cols {
			col := df.Column(name)
			switch col.DType() {
			case galleon.Int64:
				fmt.Printf("%-15d", col.Int64()[i])
			case galleon.Float64:
				fmt.Printf("%-15.2f", col.Float64()[i])
			case galleon.String:
				fmt.Printf("%-15s", col.Strings()[i])
			default:
				fmt.Printf("%-15v", "?")
			}
		}
		fmt.Println()
	}
}
