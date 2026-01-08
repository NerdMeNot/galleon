// Example 05: Lazy Evaluation
//
// This example demonstrates:
// - LazyFrame API
// - Deferred execution
// - Query optimization
// - Complex query building
//
// Run: go run main.go

package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Galleon Lazy Evaluation Example ===")

	// =========================================================================
	// Create Sample Data
	// =========================================================================
	fmt.Println("1. Creating Base DataFrame")
	fmt.Println("-" + string(make([]byte, 40)))

	// Create a sample dataset
	n := 1000
	ids := make([]int64, n)
	categories := make([]string, n)
	values := make([]float64, n)
	quantities := make([]int64, n)

	cats := []string{"Electronics", "Clothing", "Food", "Books", "Home"}
	for i := 0; i < n; i++ {
		ids[i] = int64(i + 1)
		categories[i] = cats[i%len(cats)]
		values[i] = float64(10 + (i % 100))
		quantities[i] = int64(1 + (i % 20))
	}

	df, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", ids),
		galleon.NewSeriesString("category", categories),
		galleon.NewSeriesFloat64("price", values),
		galleon.NewSeriesInt64("quantity", quantities),
	)

	fmt.Printf("Base DataFrame: %d rows × %d columns\n", df.Height(), df.Width())

	// =========================================================================
	// Basic Lazy Operations
	// =========================================================================
	fmt.Println("\n2. Basic Lazy Operations")
	fmt.Println("-" + string(make([]byte, 40)))

	// Convert to LazyFrame
	lazy := df.Lazy()
	fmt.Println("Created LazyFrame (no computation yet)")

	// Chain operations - nothing executes yet
	query := lazy.
		Filter(galleon.Col("price").Gt(galleon.Lit(50.0))).
		Select(galleon.Col("category"), galleon.Col("price"), galleon.Col("quantity"))

	fmt.Println("Built query with Filter + Select (still no computation)")

	// Execute and collect results
	result, err := query.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Collected result: %d rows\n", result.Height())

	// =========================================================================
	// Lazy GroupBy with Aggregations
	// =========================================================================
	fmt.Println("\n3. Lazy GroupBy with Aggregations")
	fmt.Println("-" + string(make([]byte, 40)))

	groupedQuery := df.Lazy().
		GroupBy("category").
		Agg(
			galleon.Col("price").Sum().Alias("total_revenue"),
			galleon.Col("price").Mean().Alias("avg_price"),
			galleon.Col("quantity").Sum().Alias("total_qty"),
		)

	fmt.Println("Built GroupBy query (deferred)")

	groupedResult, err := groupedQuery.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Category Statistics:")
	printDataFrame(groupedResult)

	// =========================================================================
	// Lazy Sort and Limit
	// =========================================================================
	fmt.Println("\n4. Lazy Sort and Limit")
	fmt.Println("-" + string(make([]byte, 40)))

	// Sort by price descending and take top 10
	sortQuery := df.Lazy().
		Sort("price", false).
		Head(10)

	fmt.Println("Built Sort + Head query (deferred)")

	sortResult, err := sortQuery.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Top 10 by price:")
	printDataFrame(sortResult)

	// =========================================================================
	// Filter + Sort Pipeline
	// =========================================================================
	fmt.Println("\n5. Filter + Sort Pipeline")
	fmt.Println("-" + string(make([]byte, 40)))

	pipelineQuery := df.Lazy().
		Filter(galleon.Col("price").Gt(galleon.Lit(80.0))).
		Sort("quantity", false).
		Head(5)

	pipelineResult, err := pipelineQuery.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Top 5 by quantity (where price > 80):")
	printDataFrame(pipelineResult)

	// =========================================================================
	// Expression Building
	// =========================================================================
	fmt.Println("\n6. Expression Building")
	fmt.Println("-" + string(make([]byte, 40)))

	// Build expressions programmatically
	priceCol := galleon.Col("price")

	// Various expression types
	fmt.Println("Expression examples:")
	fmt.Println("  - Column reference: Col(\"price\")")
	fmt.Println("  - Literal: Lit(100.0)")
	fmt.Println("  - Comparison: Col(\"price\").Gt(Lit(50.0))")
	fmt.Println("  - Aggregation: Col(\"price\").Sum()")
	fmt.Println("  - Alias: Col(\"price\").Sum().Alias(\"total\")")

	// Use expressions in query
	exprQuery := df.Lazy().
		Filter(priceCol.Gt(galleon.Lit(40.0))).
		GroupBy("category").
		Agg(
			galleon.Col("price").Sum().Alias("total_price"),
		)

	exprResult, _ := exprQuery.Collect()
	fmt.Println("\nTotal price by category (price > 40):")
	printDataFrame(exprResult)

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
		fmt.Printf("%-16s", name)
	}
	fmt.Println()

	// Print separator
	fmt.Print("  ")
	for range cols {
		fmt.Print("----------------")
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
				fmt.Printf("%-16d", col.Int64()[i])
			case galleon.Float64:
				fmt.Printf("%-16.2f", col.Float64()[i])
			case galleon.String:
				fmt.Printf("%-16s", col.Strings()[i])
			default:
				fmt.Printf("%-16v", "?")
			}
		}
		fmt.Println()
	}

	if df.Height() > maxRows {
		fmt.Printf("  ... and %d more rows\n", df.Height()-maxRows)
	}
}
