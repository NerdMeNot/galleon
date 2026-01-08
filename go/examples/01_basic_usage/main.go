// Example 01: Basic Usage
//
// This example demonstrates the fundamental operations of Galleon:
// - Creating Series and DataFrames
// - Basic aggregations
// - Accessing data
//
// Run: go run main.go

package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Galleon Basic Usage Example ===\n")

	// =========================================================================
	// Creating Series
	// =========================================================================
	fmt.Println("1. Creating Series")
	fmt.Println("-" + string(make([]byte, 40)))

	// Integer series
	ids := galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5})
	fmt.Printf("Integer Series: %v\n", ids.Int64())

	// Float series
	values := galleon.NewSeriesFloat64("value", []float64{10.5, 20.3, 15.7, 8.2, 25.1})
	fmt.Printf("Float Series: %v\n", values.Float64())

	// String series
	names := galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"})
	fmt.Printf("String Series: %v\n", names.Strings())

	// =========================================================================
	// Creating DataFrames
	// =========================================================================
	fmt.Println("\n2. Creating DataFrames")
	fmt.Println("-" + string(make([]byte, 40)))

	df, err := galleon.NewDataFrame(ids, values, names)
	if err != nil {
		fmt.Printf("Error creating DataFrame: %v\n", err)
		return
	}

	fmt.Printf("DataFrame shape: %d rows × %d columns\n", df.Height(), df.Width())
	fmt.Printf("Column names: %v\n", df.Columns())

	// =========================================================================
	// Basic Aggregations
	// =========================================================================
	fmt.Println("\n3. Basic Aggregations")
	fmt.Println("-" + string(make([]byte, 40)))

	valueCol := df.ColumnByName("value")
	fmt.Printf("Sum:   %.2f\n", valueCol.Sum())
	fmt.Printf("Min:   %.2f\n", valueCol.Min())
	fmt.Printf("Max:   %.2f\n", valueCol.Max())
	fmt.Printf("Mean:  %.2f\n", valueCol.Mean())
	fmt.Printf("Count: %d\n", valueCol.Len())

	// =========================================================================
	// Element-wise Operations
	// =========================================================================
	fmt.Println("\n4. Element-wise Operations")
	fmt.Println("-" + string(make([]byte, 40)))

	// Create a copy for modification
	data := make([]float64, len(values.Float64()))
	copy(data, values.Float64())

	// Add scalar
	galleon.AddScalarF64(data, 10.0)
	fmt.Printf("After adding 10: %v\n", data)

	// Multiply by scalar
	copy(data, values.Float64())
	galleon.MulScalarF64(data, 2.0)
	fmt.Printf("After multiplying by 2: %v\n", data)

	// =========================================================================
	// Accessing Data
	// =========================================================================
	fmt.Println("\n5. Accessing Data")
	fmt.Println("-" + string(make([]byte, 40)))

	fmt.Println("Row-by-row access:")
	for i := 0; i < df.Height(); i++ {
		id := df.ColumnByName("id").Int64()[i]
		val := df.ColumnByName("value").Float64()[i]
		name := df.ColumnByName("name").Strings()[i]
		fmt.Printf("  Row %d: id=%d, value=%.1f, name=%s\n", i, id, val, name)
	}

	// =========================================================================
	// Type Information
	// =========================================================================
	fmt.Println("\n6. Type Information")
	fmt.Println("-" + string(make([]byte, 40)))

	for _, name := range df.Columns() {
		col := df.ColumnByName(name)
		fmt.Printf("Column '%s': dtype=%v, len=%d\n", name, col.DType(), col.Len())
	}

	fmt.Println("\n=== Example Complete ===")
}
