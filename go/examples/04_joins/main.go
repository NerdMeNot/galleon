// Example 04: Join Operations
//
// This example demonstrates:
// - Inner joins
// - Left joins
// - Multi-key joins
// - Join with different column names
//
// Run: go run main.go

package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Galleon Join Example ===\n")

	// =========================================================================
	// Create Sample Data
	// =========================================================================
	fmt.Println("Sample Data")
	fmt.Println("-" + string(make([]byte, 40)))

	// Orders table
	orders, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("order_id", []int64{1, 2, 3, 4, 5}),
		galleon.NewSeriesInt64("customer_id", []int64{101, 102, 101, 103, 104}),
		galleon.NewSeriesFloat64("amount", []float64{150.0, 200.0, 75.0, 300.0, 125.0}),
	)
	fmt.Println("\nOrders:")
	printDataFrame(orders)

	// Customers table
	customers, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("customer_id", []int64{101, 102, 103, 105}),
		galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie", "Eve"}),
		galleon.NewSeriesString("city", []string{"NYC", "LA", "Chicago", "Boston"}),
	)
	fmt.Println("\nCustomers:")
	printDataFrame(customers)

	// =========================================================================
	// Inner Join
	// =========================================================================
	fmt.Println("\n1. Inner Join (orders + customers)")
	fmt.Println("-" + string(make([]byte, 40)))

	innerResult, err := orders.Join(customers, galleon.On("customer_id"))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %d rows (only matching customers)\n", innerResult.Height())
	printDataFrame(innerResult)

	// =========================================================================
	// Left Join
	// =========================================================================
	fmt.Println("\n2. Left Join (orders + customers)")
	fmt.Println("-" + string(make([]byte, 40)))

	leftResult, err := orders.LeftJoin(customers, galleon.On("customer_id"))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Result: %d rows (all orders, even without customer match)\n", leftResult.Height())
	printDataFrame(leftResult)

	// =========================================================================
	// Join with Different Column Names
	// =========================================================================
	fmt.Println("\n3. Join with Different Column Names")
	fmt.Println("-" + string(make([]byte, 40)))

	// Products with different column naming
	products, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("prod_id", []int64{1, 2, 3}),
		galleon.NewSeriesString("prod_name", []string{"Widget", "Gadget", "Gizmo"}),
		galleon.NewSeriesFloat64("price", []float64{9.99, 19.99, 29.99}),
	)

	orderItems, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("order_id", []int64{1, 1, 2, 3, 3}),
		galleon.NewSeriesInt64("product_id", []int64{1, 2, 1, 2, 3}),
		galleon.NewSeriesInt64("qty", []int64{2, 1, 3, 1, 2}),
	)

	fmt.Println("Products:")
	printDataFrame(products)
	fmt.Println("\nOrder Items:")
	printDataFrame(orderItems)

	joinedItems, err := orderItems.Join(products,
		galleon.LeftOn("product_id").RightOn("prod_id"),
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("\nJoined (product_id = prod_id):")
	printDataFrame(joinedItems)

	// =========================================================================
	// Large-Scale Join Example
	// =========================================================================
	fmt.Println("\n4. Large-Scale Join (Performance Demo)")
	fmt.Println("-" + string(make([]byte, 40)))

	// Generate larger datasets
	n := 10000
	leftIds := make([]int64, n)
	leftVals := make([]float64, n)
	for i := 0; i < n; i++ {
		leftIds[i] = int64(i % 1000) // 1000 unique keys
		leftVals[i] = float64(i) * 0.1
	}

	rightIds := make([]int64, n/2)
	rightVals := make([]float64, n/2)
	for i := 0; i < n/2; i++ {
		rightIds[i] = int64(i % 1000)
		rightVals[i] = float64(i) * 0.2
	}

	leftDf, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", leftIds),
		galleon.NewSeriesFloat64("left_val", leftVals),
	)

	rightDf, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", rightIds),
		galleon.NewSeriesFloat64("right_val", rightVals),
	)

	fmt.Printf("Left table:  %d rows\n", leftDf.Height())
	fmt.Printf("Right table: %d rows\n", rightDf.Height())

	// Perform join
	result, _ := leftDf.Join(rightDf, galleon.On("id"))
	fmt.Printf("Inner join result: %d rows\n", result.Height())

	leftResult2, _ := leftDf.LeftJoin(rightDf, galleon.On("id"))
	fmt.Printf("Left join result:  %d rows\n", leftResult2.Height())

	// =========================================================================
	// Join + Aggregation Pipeline
	// =========================================================================
	fmt.Println("\n5. Join + Aggregation Pipeline")
	fmt.Println("-" + string(make([]byte, 40)))

	// Join orders with customers, then aggregate by city
	pipeline, _ := orders.Join(customers, galleon.On("customer_id"))
	aggregated, err := pipeline.GroupBy("city").Agg(
		galleon.AggSum("amount").Alias("total_amount"),
		galleon.AggCount().Alias("order_count"),
	)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Total sales by city:")
	printDataFrame(aggregated)

	fmt.Println("\n=== Example Complete ===")
}

func printDataFrame(df *galleon.DataFrame) {
	if df == nil {
		fmt.Println("  (nil DataFrame)")
		return
	}

	// Print header
	cols := df.Columns()
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

	// Limit rows for display
	maxRows := 10
	rows := df.Height()
	if rows > maxRows {
		rows = maxRows
	}

	// Print rows
	for i := 0; i < rows; i++ {
		fmt.Print("  ")
		for _, name := range cols {
			col := df.ColumnByName(name)
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

	if df.Height() > maxRows {
		fmt.Printf("  ... and %d more rows\n", df.Height()-maxRows)
	}
}
