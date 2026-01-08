// Example 02: Filtering and Sorting
//
// This example demonstrates:
// - Filtering data with masks
// - Sorting DataFrames
// - Selecting and dropping columns
//
// Run: go run main.go

package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Galleon Filtering and Sorting Example ===\n")

	// Create sample data
	df, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
		galleon.NewSeriesFloat64("value", []float64{15.5, 8.2, 23.1, 5.7, 19.3, 12.8, 7.4, 28.9, 11.2, 3.6}),
		galleon.NewSeriesString("category", []string{"A", "B", "A", "B", "A", "B", "A", "B", "A", "B"}),
	)

	fmt.Printf("Original DataFrame: %d rows\n", df.Height())
	printDataFrame(df)

	// =========================================================================
	// Filtering with Masks
	// =========================================================================
	fmt.Println("\n1. Filtering with Masks")
	fmt.Println("-" + string(make([]byte, 40)))

	// Get values > 10
	values := df.Column("value").Float64()
	mask := galleon.FilterMaskGreaterThanF64(values, 10.0)

	// Count matching
	count := 0
	for _, m := range mask {
		if m {
			count++
		}
	}
	fmt.Printf("Values > 10: %d rows\n", count)

	// Convert bool mask to byte mask for FilterByMask
	byteMask := make([]byte, len(mask))
	for i, m := range mask {
		if m {
			byteMask[i] = 1
		}
	}

	filtered, err := df.FilterByMask(byteMask)
	if err != nil {
		fmt.Printf("Error filtering: %v\n", err)
		return
	}

	fmt.Printf("\nFiltered DataFrame (value > 10): %d rows\n", filtered.Height())
	printDataFrame(filtered)

	// =========================================================================
	// Using Expression-based Filter
	// =========================================================================
	fmt.Println("\n2. Expression-based Filter")
	fmt.Println("-" + string(make([]byte, 40)))

	// Filter using expression syntax
	filtered2 := df.Filter(galleon.Col("value").Gt(galleon.Lit(15.0)))
	fmt.Printf("Filtered (value > 15): %d rows\n", filtered2.Height())
	printDataFrame(filtered2)

	// =========================================================================
	// Sorting
	// =========================================================================
	fmt.Println("\n3. Sorting")
	fmt.Println("-" + string(make([]byte, 40)))

	// Sort by value ascending
	sorted := df.Sort("value", true)
	fmt.Println("Sorted by value (ascending):")
	printDataFrame(sorted)

	// Sort by value descending
	sortedDesc := df.Sort("value", false)
	fmt.Println("\nSorted by value (descending):")
	printDataFrame(sortedDesc)

	// =========================================================================
	// Column Selection
	// =========================================================================
	fmt.Println("\n4. Column Selection")
	fmt.Println("-" + string(make([]byte, 40)))

	// Select specific columns
	selected := df.Select("id", "value")
	fmt.Println("Selected columns (id, value):")
	printDataFrame(selected)

	// Drop a column
	dropped := df.Drop("category")
	fmt.Println("\nAfter dropping 'category':")
	printDataFrame(dropped)

	// =========================================================================
	// Head and Tail
	// =========================================================================
	fmt.Println("\n5. Head and Tail")
	fmt.Println("-" + string(make([]byte, 40)))

	head := df.Head(3)
	fmt.Println("First 3 rows:")
	printDataFrame(head)

	tail := df.Tail(3)
	fmt.Println("\nLast 3 rows:")
	printDataFrame(tail)

	// =========================================================================
	// Combining Operations
	// =========================================================================
	fmt.Println("\n6. Combining Operations")
	fmt.Println("-" + string(make([]byte, 40)))

	// Filter, sort, and select in one chain
	result := df.
		Filter(galleon.Col("value").Gt(galleon.Lit(10.0))).
		Sort("value", false).
		Select("id", "value").
		Head(5)

	fmt.Println("Top 5 by value (where value > 10):")
	printDataFrame(result)

	fmt.Println("\n=== Example Complete ===")
}

func printDataFrame(df *galleon.DataFrame) {
	// Print header
	cols := df.ColumnNames()
	fmt.Print("  ")
	for _, name := range cols {
		fmt.Printf("%-12s", name)
	}
	fmt.Println()

	// Print rows
	for i := 0; i < df.Height(); i++ {
		fmt.Print("  ")
		for _, name := range cols {
			col := df.Column(name)
			switch col.DType() {
			case galleon.Int64:
				fmt.Printf("%-12d", col.Int64()[i])
			case galleon.Float64:
				fmt.Printf("%-12.2f", col.Float64()[i])
			case galleon.String:
				fmt.Printf("%-12s", col.Strings()[i])
			}
		}
		fmt.Println()
	}
}
