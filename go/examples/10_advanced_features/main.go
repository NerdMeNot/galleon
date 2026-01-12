package main

import (
	"fmt"
	"math"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== Advanced Features: UDF and Caching ===\n")

	// Example 1: Simple User-Defined Function
	fmt.Println("Example 1: Simple UDF - Price Markup")
	fmt.Println("------------------------------------")

	// Product prices
	products := []string{"Laptop", "Phone", "Tablet", "Monitor", "Keyboard"}
	costPrices := []float64{800.0, 500.0, 300.0, 250.0, 50.0}

	df, err := galleon.NewDataFrame(
		galleon.NewSeriesString("product", products),
		galleon.NewSeriesFloat64("cost", costPrices),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Original Data:")
	fmt.Println(df)
	fmt.Println()

	// Apply 20% markup using UDF
	withMarkup, err := df.Lazy().
		Apply("cost", func(s *galleon.Series) (*galleon.Series, error) {
			data := s.Float64()
			result := make([]float64, len(data))
			for i, v := range data {
				result[i] = v * 1.20 // 20% markup
			}
			return galleon.NewSeriesFloat64("retail_price", result), nil
		}).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("With 20% Markup Applied:")
	fmt.Println(withMarkup)
	fmt.Println()

	// Example 2: Complex Business Logic UDF
	fmt.Println("Example 2: Complex UDF - Tiered Discount")
	fmt.Println("----------------------------------------")

	// Sales data
	salesData := []float64{50.0, 150.0, 300.0, 550.0, 1200.0}
	customers := []string{"Alice", "Bob", "Carol", "Dave", "Eve"}

	salesDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("customer", customers),
		galleon.NewSeriesFloat64("sales", salesData),
	)

	fmt.Println("Original Sales:")
	fmt.Println(salesDF)
	fmt.Println()

	// Apply tiered discount: 0-100: 0%, 100-500: 10%, 500+: 20%
	withDiscount, _ := salesDF.Lazy().
		Apply("sales", func(s *galleon.Series) (*galleon.Series, error) {
			data := s.Float64()
			finalPrice := make([]float64, len(data))

			for i, amount := range data {
				var discount float64
				if amount < 100 {
					discount = 0.0
				} else if amount < 500 {
					discount = 0.10
				} else {
					discount = 0.20
				}
				finalPrice[i] = amount * (1 - discount)
			}

			return galleon.NewSeriesFloat64("final_price", finalPrice), nil
		}).
		Collect()

	fmt.Println("With Tiered Discount Applied:")
	fmt.Println(withDiscount)
	fmt.Println()

	// Example 3: Mathematical Transformation UDF
	fmt.Println("Example 3: UDF - Log Transform")
	fmt.Println("------------------------------")

	// Skewed data
	values := []float64{1.0, 10.0, 100.0, 1000.0, 10000.0}
	ids := []int64{1, 2, 3, 4, 5}

	skewedDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", ids),
		galleon.NewSeriesFloat64("value", values),
	)

	fmt.Println("Skewed Data:")
	fmt.Println(skewedDF)
	fmt.Println()

	// Apply log transformation to normalize
	logTransformed, _ := skewedDF.Lazy().
		Apply("value", func(s *galleon.Series) (*galleon.Series, error) {
			data := s.Float64()
			result := make([]float64, len(data))
			for i, v := range data {
				if v > 0 {
					result[i] = math.Log10(v)
				} else {
					result[i] = math.NaN()
				}
			}
			return galleon.NewSeriesFloat64("log_value", result), nil
		}).
		Collect()

	fmt.Println("Log-Transformed Data:")
	fmt.Println(logTransformed)
	fmt.Println()

	// Example 4: Basic Caching
	fmt.Println("Example 4: Basic Caching")
	fmt.Println("-----------------------")

	// Large dataset simulation
	largeIDs := make([]int64, 1000)
	largeValues := make([]float64, 1000)
	for i := 0; i < 1000; i++ {
		largeIDs[i] = int64(i)
		largeValues[i] = float64(i * 10)
	}

	largeDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("id", largeIDs),
		galleon.NewSeriesFloat64("value", largeValues),
	)

	fmt.Println("Dataset size: 1000 rows")
	fmt.Println()

	// Create expensive aggregation and cache it
	cached := largeDF.Lazy().
		Filter(galleon.Col("value").Gt(galleon.Lit(5000.0))).
		Cache() // Materialize the filtered result

	// Use cached result multiple times
	result1, _ := cached.
		Select(galleon.Col("value").Mean().Alias("mean")).
		Collect()

	fmt.Println("First query (mean):")
	fmt.Println(result1)
	fmt.Println()

	result2, _ := cached.
		Select(galleon.Col("value").Max().Alias("max")).
		Collect()

	fmt.Println("Second query (max) - reuses cache:")
	fmt.Println(result2)
	fmt.Println()

	// Example 5: Caching Expensive Joins
	fmt.Println("Example 5: Caching Join Results")
	fmt.Println("-------------------------------")

	// Users table
	userIDs := []int64{1, 2, 3, 4, 5}
	userNames := []string{"Alice", "Bob", "Carol", "Dave", "Eve"}
	users, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("user_id", userIDs),
		galleon.NewSeriesString("name", userNames),
	)

	// Orders table
	orderIDs := []int64{101, 102, 103, 104, 105, 106, 107, 108}
	orderUserIDs := []int64{1, 2, 1, 3, 2, 4, 1, 5}
	orderAmounts := []float64{100.0, 150.0, 200.0, 120.0, 180.0, 90.0, 250.0, 130.0}
	orders, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("order_id", orderIDs),
		galleon.NewSeriesInt64("user_id", orderUserIDs),
		galleon.NewSeriesFloat64("amount", orderAmounts),
	)

	fmt.Println("Users:")
	fmt.Println(users)
	fmt.Println()
	fmt.Println("Orders:")
	fmt.Println(orders)
	fmt.Println()

	// Join and cache the result
	joined := orders.Lazy().
		Join(users.Lazy(), galleon.On("user_id")).
		Cache() // Cache the joined table

	// Multiple aggregations on the cached join
	// Note: GroupBy takes string column names, not expressions
	totalByUser, _ := joined.
		GroupBy("name").
		Agg(galleon.Col("amount").Sum().Alias("total_amount")).
		Collect()

	fmt.Println("Total Amount by User (from cache):")
	fmt.Println(totalByUser)
	fmt.Println()

	countByUser, _ := joined.
		GroupBy("name").
		Agg(galleon.Col("order_id").Count().Alias("order_count")).
		Collect()

	fmt.Println("Order Count by User (from cache):")
	fmt.Println(countByUser)
	fmt.Println()

	// Example 6: Chaining UDFs
	fmt.Println("Example 6: Chaining Multiple UDFs")
	fmt.Println("---------------------------------")

	// Temperature data in Fahrenheit
	fahrenheit := []float64{32.0, 68.0, 86.0, 104.0, 122.0}
	locations := []string{"City A", "City B", "City C", "City D", "City E"}

	tempDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("location", locations),
		galleon.NewSeriesFloat64("temp_f", fahrenheit),
	)

	fmt.Println("Original Temperatures (Fahrenheit):")
	fmt.Println(tempDF)
	fmt.Println()

	// UDF 1: Convert F to C
	withCelsius, _ := tempDF.Lazy().
		Apply("temp_f", func(s *galleon.Series) (*galleon.Series, error) {
			data := s.Float64()
			result := make([]float64, len(data))
			for i, f := range data {
				result[i] = (f - 32) * 5 / 9
			}
			return galleon.NewSeriesFloat64("temp_c", result), nil
		}).
		Collect()

	// UDF 2: Categorize temperature
	withCategory, _ := withCelsius.Lazy().
		Apply("temp_c", func(s *galleon.Series) (*galleon.Series, error) {
			data := s.Float64()
			categories := make([]string, len(data))
			for i, c := range data {
				if c < 10 {
					categories[i] = "Cold"
				} else if c < 25 {
					categories[i] = "Moderate"
				} else {
					categories[i] = "Hot"
				}
			}
			return galleon.NewSeriesString("category", categories), nil
		}).
		Collect()

	fmt.Println("With Celsius and Category:")
	fmt.Println(withCategory)
	fmt.Println()

	// Example 7: UDF with Error Handling
	fmt.Println("Example 7: UDF with Error Handling")
	fmt.Println("----------------------------------")

	// Data with potential division by zero
	numerators := []float64{10.0, 20.0, 30.0, 40.0}
	denominators := []float64{2.0, 0.0, 5.0, 8.0}

	divisionDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesFloat64("numerator", numerators),
		galleon.NewSeriesFloat64("denominator", denominators),
	)

	fmt.Println("Original Data:")
	fmt.Println(divisionDF)
	fmt.Println()

	// Safe division with error handling
	// Note: UDF operates on one column, so we need to pass denominator data through closure
	safeDivision, _ := divisionDF.Lazy().
		Apply("numerator", func(s *galleon.Series) (*galleon.Series, error) {
			nums := s.Float64()
			result := make([]float64, len(nums))
			for i, n := range nums {
				d := denominators[i]
				if d == 0 {
					result[i] = math.NaN() // Handle division by zero
				} else {
					result[i] = n / d
				}
			}
			return galleon.NewSeriesFloat64("result", result), nil
		}).
		Collect()

	fmt.Println("Safe Division Results (NaN for division by zero):")
	fmt.Println(safeDivision)
	fmt.Println()

	// Example 8: Performance Comparison with Caching
	fmt.Println("Example 8: Cache Performance Benefits")
	fmt.Println("-------------------------------------")

	// Create dataset for aggregation
	categories := make([]string, 10000)
	amounts := make([]float64, 10000)
	for i := 0; i < 10000; i++ {
		categories[i] = fmt.Sprintf("Category%d", i%10)
		amounts[i] = float64(i)
	}

	perfDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("category", categories),
		galleon.NewSeriesFloat64("amount", amounts),
	)

	fmt.Println("Dataset: 10,000 rows")
	fmt.Println()

	// Expensive aggregation cached
	// Note: GroupBy takes string column names
	expensiveAgg := perfDF.Lazy().
		GroupBy("category").
		Agg(
			galleon.Col("amount").Sum().Alias("total"),
			galleon.Col("amount").Mean().Alias("mean"),
			galleon.Col("amount").Count().Alias("count"),
		).
		Cache() // Cache the aggregation result

	// Query 1: Filter high totals
	highTotals, _ := expensiveAgg.
		Filter(galleon.Col("total").Gt(galleon.Lit(4000000.0))).
		Collect()

	fmt.Println("High Total Categories (from cache):")
	fmt.Println(highTotals)
	fmt.Println()

	// Query 2: Sort by mean (using string column name)
	sortedByMean, _ := expensiveAgg.
		Sort("mean", false).
		Head(3).
		Collect()

	fmt.Println("Top 3 by Mean (from cache):")
	fmt.Println(sortedByMean)
	fmt.Println()

	// Query 3: Count categories
	categoryCount, _ := expensiveAgg.
		Select(galleon.Col("category").Count().Alias("num_categories")).
		Collect()

	fmt.Println("Number of Categories (from cache):")
	fmt.Println(categoryCount)
	fmt.Println()

	fmt.Println("Advanced Features Examples Complete!")
	fmt.Println("\nKey Takeaways:")
	fmt.Println("- UDFs enable custom business logic not available in built-in operations")
	fmt.Println("- Caching materializes expensive computations for reuse")
	fmt.Println("- Cache is especially valuable for joins and aggregations")
	fmt.Println("- UDFs can be chained for complex transformations")
	fmt.Println("- Always handle edge cases (division by zero, NaN) in UDFs")
}
