package main

import (
	"fmt"

	galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
	fmt.Println("=== String Operations Example ===\n")

	// Example 1: Basic Text Transformations
	fmt.Println("Example 1: Text Case Transformations")
	fmt.Println("------------------------------------")

	// Sample user data with mixed case
	names := []string{"Alice Smith", "bob jones", "CAROL WHITE", "Dave Brown"}
	emails := []string{
		"Alice.Smith@EXAMPLE.com",
		"bob.jones@example.COM",
		"CAROL.WHITE@EXAMPLE.COM",
		"dave.brown@example.com",
	}

	df, err := galleon.NewDataFrame(
		galleon.NewSeriesString("name", names),
		galleon.NewSeriesString("email", emails),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("Original Data:")
	fmt.Println(df)
	fmt.Println()

	// Normalize to lowercase
	normalized, err := df.Lazy().
		WithColumn("name_lower", galleon.Col("name").Str().Lower()).
		WithColumn("email_clean", galleon.Col("email").Str().Lower()).
		Collect()
	if err != nil {
		panic(err)
	}

	fmt.Println("After Lowercase Normalization:")
	fmt.Println(normalized)
	fmt.Println()

	// Convert to uppercase
	uppercase, _ := df.Lazy().
		WithColumn("name_upper", galleon.Col("name").Str().Upper()).
		Collect()

	fmt.Println("Uppercase Names:")
	fmt.Println(uppercase)
	fmt.Println()

	// Example 2: Text Cleaning with Trim
	fmt.Println("Example 2: Text Cleaning with Trim")
	fmt.Println("----------------------------------")

	// Data with extra whitespace
	messyNames := []string{"  Alice  ", "Bob   ", "  Carol", "   Dave   "}
	messyComments := []string{
		"  Great product!  ",
		"Fast shipping   ",
		"  Excellent service  ",
		"   Highly recommended   ",
	}

	messyDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("name", messyNames),
		galleon.NewSeriesString("comment", messyComments),
	)

	fmt.Println("Before Trimming:")
	fmt.Println(messyDF)
	fmt.Println()

	// Trim whitespace
	cleanDF, _ := messyDF.Lazy().
		WithColumn("name_clean", galleon.Col("name").Str().Trim()).
		WithColumn("comment_clean", galleon.Col("comment").Str().Trim()).
		Collect()

	fmt.Println("After Trimming:")
	fmt.Println(cleanDF)
	fmt.Println()

	// Example 3: Pattern Matching with Contains
	fmt.Println("Example 3: Pattern Matching")
	fmt.Println("---------------------------")

	// Log entries
	logMessages := []string{
		"INFO: Application started",
		"ERROR: Connection failed",
		"WARNING: High memory usage",
		"INFO: Request processed",
		"ERROR: Database timeout",
		"DEBUG: Cache hit",
	}
	timestamps := []string{
		"2024-01-01 10:00:00",
		"2024-01-01 10:05:00",
		"2024-01-01 10:10:00",
		"2024-01-01 10:15:00",
		"2024-01-01 10:20:00",
		"2024-01-01 10:25:00",
	}

	logsDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("timestamp", timestamps),
		galleon.NewSeriesString("message", logMessages),
	)

	fmt.Println("All Logs:")
	fmt.Println(logsDF)
	fmt.Println()

	// Filter for ERROR messages
	errors, _ := logsDF.Lazy().
		Filter(galleon.Col("message").Str().Contains("ERROR")).
		Collect()

	fmt.Println("ERROR Messages Only:")
	fmt.Println(errors)
	fmt.Println()

	// Filter for WARNING or ERROR
	issues, _ := logsDF.Lazy().
		WithColumn("is_error", galleon.Col("message").Str().Contains("ERROR")).
		WithColumn("is_warning", galleon.Col("message").Str().Contains("WARNING")).
		Collect()

	fmt.Println("With Error/Warning Flags:")
	fmt.Println(issues)
	fmt.Println()

	// Example 4: StartsWith and EndsWith
	fmt.Println("Example 4: Prefix and Suffix Matching")
	fmt.Println("-------------------------------------")

	// File listing
	files := []string{
		"report.pdf",
		"data.csv",
		"backup.tar.gz",
		"image.png",
		"document.pdf",
		"config.json",
		"archive.zip",
	}

	filesDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("filename", files),
	)

	fmt.Println("All Files:")
	fmt.Println(filesDF)
	fmt.Println()

	// Filter PDF files
	pdfFiles, _ := filesDF.Lazy().
		Filter(galleon.Col("filename").Str().EndsWith(".pdf")).
		Collect()

	fmt.Println("PDF Files Only:")
	fmt.Println(pdfFiles)
	fmt.Println()

	// Filter files starting with 'report' or 'data'
	dataFiles, _ := filesDF.Lazy().
		WithColumn("is_report", galleon.Col("filename").Str().StartsWith("report")).
		WithColumn("is_data", galleon.Col("filename").Str().StartsWith("data")).
		Collect()

	fmt.Println("With Report/Data Flags:")
	fmt.Println(dataFiles)
	fmt.Println()

	// Example 5: String Replacement
	fmt.Println("Example 5: String Replacement")
	fmt.Println("-----------------------------")

	// Product codes with old prefix
	oldCodes := []string{
		"OLD-LAPTOP-001",
		"OLD-PHONE-002",
		"OLD-TABLET-003",
		"OLD-MONITOR-004",
	}
	products := []string{"Laptop", "Phone", "Tablet", "Monitor"}

	codesDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("product", products),
		galleon.NewSeriesString("old_code", oldCodes),
	)

	fmt.Println("Original Product Codes:")
	fmt.Println(codesDF)
	fmt.Println()

	// Replace OLD prefix with NEW
	updatedCodes, _ := codesDF.Lazy().
		WithColumn("new_code", galleon.Col("old_code").Str().Replace("OLD", "NEW")).
		Collect()

	fmt.Println("Updated Product Codes:")
	fmt.Println(updatedCodes)
	fmt.Println()

	// Example 6: Email Validation Pipeline
	fmt.Println("Example 6: Email Processing Pipeline")
	fmt.Println("------------------------------------")

	// User emails to validate
	userEmails := []string{
		"alice@company.com",
		"bob@COMPANY.COM",
		"  carol@company.com  ",
		"dave@external.org",
		"eve@company.com",
	}
	userIDs := []int64{1, 2, 3, 4, 5}

	emailsDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("user_id", userIDs),
		galleon.NewSeriesString("email", userEmails),
	)

	fmt.Println("Raw Email Data:")
	fmt.Println(emailsDF)
	fmt.Println()

	// Clean and validate company emails
	processedEmails, _ := emailsDF.Lazy().
		// Normalize: lowercase and trim
		WithColumn("email_clean", galleon.Col("email").Str().Lower().Str().Trim()).
		// Check if company email
		WithColumn("is_company_email", galleon.Col("email_clean").Str().Contains("@company.com")).
		// Filter to company emails only
		Filter(galleon.Col("is_company_email").Eq(true)).
		Select(
			galleon.Col("user_id"),
			galleon.Col("email_clean"),
		).
		Collect()

	fmt.Println("Processed Company Emails:")
	fmt.Println(processedEmails)
	fmt.Println()

	// Example 7: Text Categorization
	fmt.Println("Example 7: Text Categorization")
	fmt.Println("------------------------------")

	// Customer feedback
	feedback := []string{
		"Great product, fast shipping!",
		"Product arrived damaged",
		"Excellent customer service",
		"Slow delivery, not happy",
		"Amazing quality, highly recommend",
		"Packaging was poor",
		"Fast shipping, great price",
		"Product not as described",
	}
	feedbackIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8}

	feedbackDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesInt64("feedback_id", feedbackIDs),
		galleon.NewSeriesString("text", feedback),
	)

	fmt.Println("Customer Feedback:")
	fmt.Println(feedbackDF)
	fmt.Println()

	// Categorize by keywords
	categorized, _ := feedbackDF.Lazy().
		WithColumn("text_lower", galleon.Col("text").Str().Lower()).
		WithColumn("mentions_shipping", galleon.Col("text_lower").Str().Contains("shipping")).
		WithColumn("mentions_quality", galleon.Col("text_lower").Str().Contains("quality")).
		WithColumn("mentions_damage", galleon.Col("text_lower").Str().Contains("damage")).
		WithColumn("mentions_service", galleon.Col("text_lower").Str().Contains("service")).
		Select(
			galleon.Col("feedback_id"),
			galleon.Col("text"),
			galleon.Col("mentions_shipping"),
			galleon.Col("mentions_quality"),
			galleon.Col("mentions_damage"),
			galleon.Col("mentions_service"),
		).
		Collect()

	fmt.Println("Categorized Feedback:")
	fmt.Println(categorized)
	fmt.Println()

	// Example 8: URL Processing
	fmt.Println("Example 8: URL Processing")
	fmt.Println("------------------------")

	// Website URLs
	urls := []string{
		"https://example.com/page",
		"http://example.com/api/data",
		"https://secure.example.com/login",
		"http://www.example.com/about",
		"https://api.example.com/v1/users",
	}

	urlsDF, _ := galleon.NewDataFrame(
		galleon.NewSeriesString("url", urls),
	)

	fmt.Println("URLs:")
	fmt.Println(urlsDF)
	fmt.Println()

	// Analyze URL patterns
	urlAnalysis, _ := urlsDF.Lazy().
		WithColumn("is_secure", galleon.Col("url").Str().StartsWith("https://")).
		WithColumn("is_api", galleon.Col("url").Str().Contains("/api/")).
		WithColumn("is_subdomain", galleon.Col("url").Str().Contains("://api.")).
		Collect()

	fmt.Println("URL Analysis:")
	fmt.Println(urlAnalysis)
	fmt.Println()

	// Filter secure API URLs
	secureAPIs, _ := urlsDF.Lazy().
		Filter(galleon.Col("url").Str().StartsWith("https://")).
		Filter(galleon.Col("url").Str().Contains("/api/")).
		Collect()

	fmt.Println("Secure API URLs Only:")
	fmt.Println(secureAPIs)
	fmt.Println()

	fmt.Println("✓ String Operations Examples Complete!")
}
