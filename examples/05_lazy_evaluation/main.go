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
	fmt.Println("=== Galleon Lazy Evaluation Example ===\n")

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
		GroupBy(galleon.Col("category")).
		Agg(
			galleon.Col("price").Sum().Alias("total_revenue"),
			galleon.Col("price").Mean().Alias("avg_price"),
			galleon.Col("quantity").Sum().Alias("total_qty"),
		).
		Sort(galleon.Col("total_revenue"), false)

	fmt.Println("Built GroupBy query (deferred)")

	groupedResult, err := groupedQuery.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Category Statistics:")
	printDataFrame(groupedResult)

	// =========================================================================
	// Complex Query Pipeline
	// =========================================================================
	fmt.Println("\n4. Complex Query Pipeline")
	fmt.Println("-" + string(make([]byte, 40)))

	// Build a complex query step by step
	complexQuery := df.Lazy().
		// Step 1: Filter high-value items
		Filter(galleon.Col("price").Gt(galleon.Lit(30.0))).
		// Step 2: Add computed column
		WithColumn(
			"total_value",
			galleon.Col("price").Mul(galleon.Col("quantity")),
		).
		// Step 3: Select relevant columns
		Select(
			galleon.Col("id"),
			galleon.Col("category"),
			galleon.Col("price"),
			galleon.Col("quantity"),
			galleon.Col("total_value"),
		).
		// Step 4: Sort by total value
		Sort(galleon.Col("total_value"), false).
		// Step 5: Limit to top 10
		Limit(10)

	fmt.Println("Query plan: Filter → WithColumn → Select → Sort → Limit")

	topItems, err := complexQuery.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("\nTop 10 items by total value:")
	printDataFrame(topItems)

	// =========================================================================
	// Multiple Aggregations per Group
	// =========================================================================
	fmt.Println("\n5. Statistical Summary by Category")
	fmt.Println("-" + string(make([]byte, 40)))

	statsQuery := df.Lazy().
		GroupBy(galleon.Col("category")).
		Agg(
			galleon.Col("price").Count().Alias("count"),
			galleon.Col("price").Sum().Alias("sum"),
			galleon.Col("price").Mean().Alias("mean"),
			galleon.Col("price").Min().Alias("min"),
			galleon.Col("price").Max().Alias("max"),
		)

	stats, err := statsQuery.Collect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Price statistics by category:")
	printDataFrame(stats)

	// =========================================================================
	// Expression Building
	// =========================================================================
	fmt.Println("\n6. Expression Building")
	fmt.Println("-" + string(make([]byte, 40)))

	// Build expressions programmatically
	priceCol := galleon.Col("price")
	qtyCol := galleon.Col("quantity")

	// Various expression types
	fmt.Println("Expression examples:")
	fmt.Println("  - Column reference: Col(\"price\")")
	fmt.Println("  - Literal: Lit(100.0)")
	fmt.Println("  - Binary: Col(\"price\").Mul(Col(\"quantity\"))")
	fmt.Println("  - Comparison: Col(\"price\").Gt(Lit(50.0))")
	fmt.Println("  - Aggregation: Col(\"price\").Sum()")
	fmt.Println("  - Alias: Col(\"price\").Sum().Alias(\"total\")")

	// Use expressions in query
	exprQuery := df.Lazy().
		Filter(priceCol.Gt(galleon.Lit(40.0))).
		WithColumn("revenue", priceCol.Mul(qtyCol)).
		GroupBy(galleon.Col("category")).
		Agg(
			galleon.Col("revenue").Sum().Alias("total_revenue"),
		)

	exprResult, _ := exprQuery.Collect()
	fmt.Println("\nRevenue by category (price > 40):")
	printDataFrame(exprResult)

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
			col := df.Column(name)
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
