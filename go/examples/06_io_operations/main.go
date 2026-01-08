// Example 06: I/O Operations
//
// This example demonstrates:
// - Reading and writing CSV files
// - Reading and writing JSON files
// - Reading and writing Parquet files
// - I/O options and customization
//
// Run: go run main.go

package main

import (
	"fmt"
	"os"
	"path/filepath"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Galleon I/O Operations Example ===\n")

	// Create temp directory for examples
	tmpDir, err := os.MkdirTemp("", "galleon_io_example")
	if err != nil {
		fmt.Printf("Error creating temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("Using temp directory: %s\n", tmpDir)

	// Create sample data
	df, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
		galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie", "David", "Eve"}),
		galleon.NewSeriesFloat64("score", []float64{85.5, 92.3, 78.9, 88.1, 95.7}),
		galleon.NewSeriesString("grade", []string{"B", "A", "C", "B", "A"}),
	)

	fmt.Println("\nOriginal DataFrame:")
	printDataFrame(df)

	// =========================================================================
	// CSV Operations
	// =========================================================================
	fmt.Println("\n1. CSV Operations")
	fmt.Println("-" + string(make([]byte, 40)))

	csvPath := filepath.Join(tmpDir, "data.csv")

	// Write CSV
	err = df.WriteCSV(csvPath)
	if err != nil {
		fmt.Printf("Error writing CSV: %v\n", err)
		return
	}
	fmt.Printf("Wrote CSV to: %s\n", csvPath)

	// Show file contents
	content, _ := os.ReadFile(csvPath)
	fmt.Println("\nCSV file contents:")
	fmt.Println(string(content))

	// Read CSV
	readDf, err := galleon.ReadCSV(csvPath)
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}
	fmt.Println("Read CSV back:")
	printDataFrame(readDf)

	// =========================================================================
	// CSV with Custom Options
	// =========================================================================
	fmt.Println("\n2. CSV with Custom Options")
	fmt.Println("-" + string(make([]byte, 40)))

	// Write with semicolon delimiter
	csvPath2 := filepath.Join(tmpDir, "data_semicolon.csv")
	writeOpts := galleon.DefaultCSVWriteOptions()
	writeOpts.Delimiter = ';'
	err = df.WriteCSV(csvPath2, writeOpts)
	if err != nil {
		fmt.Printf("Error writing CSV: %v\n", err)
		return
	}

	content2, _ := os.ReadFile(csvPath2)
	fmt.Println("CSV with semicolon delimiter:")
	fmt.Println(string(content2))

	// Read with custom options
	readOpts := galleon.DefaultCSVReadOptions()
	readOpts.Delimiter = ';'
	readDf2, err := galleon.ReadCSV(csvPath2, readOpts)
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}
	fmt.Println("Read back:")
	printDataFrame(readDf2)

	// =========================================================================
	// JSON Operations
	// =========================================================================
	fmt.Println("\n3. JSON Operations")
	fmt.Println("-" + string(make([]byte, 40)))

	jsonPath := filepath.Join(tmpDir, "data.json")

	// Write JSON
	err = df.WriteJSON(jsonPath)
	if err != nil {
		fmt.Printf("Error writing JSON: %v\n", err)
		return
	}
	fmt.Printf("Wrote JSON to: %s\n", jsonPath)

	// Show file contents
	jsonContent, _ := os.ReadFile(jsonPath)
	fmt.Println("\nJSON file contents:")
	fmt.Println(string(jsonContent))

	// Read JSON
	jsonDf, err := galleon.ReadJSON(jsonPath)
	if err != nil {
		fmt.Printf("Error reading JSON: %v\n", err)
		return
	}
	fmt.Println("\nRead JSON back:")
	printDataFrame(jsonDf)

	// =========================================================================
	// Parquet Operations
	// =========================================================================
	fmt.Println("\n4. Parquet Operations")
	fmt.Println("-" + string(make([]byte, 40)))

	parquetPath := filepath.Join(tmpDir, "data.parquet")

	// Write Parquet
	err = df.WriteParquet(parquetPath)
	if err != nil {
		fmt.Printf("Error writing Parquet: %v\n", err)
		return
	}
	fmt.Printf("Wrote Parquet to: %s\n", parquetPath)

	// Get file size
	fi, _ := os.Stat(parquetPath)
	fmt.Printf("Parquet file size: %d bytes\n", fi.Size())

	// Read Parquet
	parquetDf, err := galleon.ReadParquet(parquetPath)
	if err != nil {
		fmt.Printf("Error reading Parquet: %v\n", err)
		return
	}
	fmt.Println("\nRead Parquet back:")
	printDataFrame(parquetDf)

	// =========================================================================
	// Lazy I/O (Scan)
	// =========================================================================
	fmt.Println("\n5. Lazy I/O (ScanCSV)")
	fmt.Println("-" + string(make([]byte, 40)))

	// Create a larger CSV for demonstration
	largeCSV := filepath.Join(tmpDir, "large.csv")
	largeContent := "id,category,value\n"
	categories := []string{"A", "B", "C"}
	for i := 1; i <= 100; i++ {
		largeContent += fmt.Sprintf("%d,%s,%.2f\n", i, categories[i%3], float64(i)*1.5)
	}
	os.WriteFile(largeCSV, []byte(largeContent), 0644)

	// Scan CSV lazily - filter and aggregate without loading all data
	lazyResult, err := galleon.ScanCSV(largeCSV).
		Filter(galleon.Col("value").Gt(galleon.Lit(50.0))).
		GroupBy("category").
		Agg(galleon.Col("value").Sum().Alias("total")).
		Collect()

	if err != nil {
		fmt.Printf("Error in lazy scan: %v\n", err)
		return
	}

	fmt.Println("Lazy scan result (filter value > 50, group by category):")
	printDataFrame(lazyResult)

	// =========================================================================
	// Type Specification in CSV
	// =========================================================================
	fmt.Println("\n6. Type Specification in CSV")
	fmt.Println("-" + string(make([]byte, 40)))

	// Create CSV with mixed types that might be ambiguous
	mixedCSV := filepath.Join(tmpDir, "mixed.csv")
	mixedContent := `id,value,flag
1,100,true
2,200,false
3,300,true`
	os.WriteFile(mixedCSV, []byte(mixedContent), 0644)

	// Read with explicit type specification
	typeOpts := galleon.DefaultCSVReadOptions()
	typeOpts.ColumnTypes = map[string]galleon.DType{
		"id":    galleon.Int64,
		"value": galleon.Float64,
		"flag":  galleon.String, // Keep as string instead of bool
	}

	typedDf, err := galleon.ReadCSV(mixedCSV, typeOpts)
	if err != nil {
		fmt.Printf("Error reading typed CSV: %v\n", err)
		return
	}

	fmt.Println("CSV with explicit types:")
	printDataFrame(typedDf)

	// Show column types
	fmt.Println("\nColumn types:")
	for _, name := range typedDf.Columns() {
		col := typedDf.ColumnByName(name)
		fmt.Printf("  %s: %v\n", name, col.DType())
	}

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

	// Print rows
	maxRows := 10
	rows := df.Height()
	if rows > maxRows {
		rows = maxRows
	}

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
				s := col.Strings()[i]
				if s == "" {
					s = "<null>"
				}
				fmt.Printf("%-15s", s)
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
