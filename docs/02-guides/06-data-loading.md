# Data Loading Guide

Galleon provides multiple ways to load data into DataFrames, from simple slices to complex structs and maps.

## Table of Contents

- [Creating DataFrames from Slices](#creating-dataframes-from-slices)
- [Loading from Structs](#loading-from-structs)
- [Loading from Maps](#loading-from-maps)
- [Loading from Files](#loading-from-files)
- [Type Auto-Detection](#type-auto-detection)
- [Best Practices](#best-practices)

## Creating DataFrames from Slices

The most basic way to create a DataFrame is from typed slices.

### Manual Series Construction

```go
df, err := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesFloat64("value", []float64{10.5, 20.3, 15.7}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Carol"}),
)
```

### Generic Series Constructor

The `NewSeries` function automatically detects types:

```go
id, _ := galleon.NewSeries("id", []int64{1, 2, 3})
value, _ := galleon.NewSeries("value", []float64{10.5, 20.3, 15.7})
name, _ := galleon.NewSeries("name", []string{"Alice", "Bob", "Carol"})

df, err := galleon.NewDataFrame(id, value, name)
```

Supported types:
- `[]float64` → Float64 series
- `[]float32` → Float32 series
- `[]int64` → Int64 series
- `[]int32` → Int32 series
- `[]int` → Int64 series (converted)
- `[]bool` → Bool series
- `[]string` → String series

## Loading from Structs

`FromStructs` creates DataFrames from slices of structs using reflection.

### Basic Struct Loading

```go
type Person struct {
    ID   int64
    Name string
    Age  int64
}

people := []Person{
    {ID: 1, Name: "Alice", Age: 30},
    {ID: 2, Name: "Bob", Age: 25},
    {ID: 3, Name: "Carol", Age: 35},
}

df, err := galleon.FromStructs(people)
if err != nil {
    panic(err)
}

fmt.Println(df)
// Output:
// +----+-------+-----+
// | ID | Name  | Age |
// +----+-------+-----+
// | 1  | Alice | 30  |
// | 2  | Bob   | 25  |
// | 3  | Carol | 35  |
// +----+-------+-----+
```

### Struct Tags for Column Names

Use `galleon:"column_name"` tags to customize column names:

```go
type User struct {
    UserID   int64   `galleon:"user_id"`
    FullName string  `galleon:"name"`
    Email    string  `galleon:"email"`
    Balance  float64 `galleon:"balance"`
}

users := []User{
    {UserID: 1, FullName: "Alice Smith", Email: "alice@example.com", Balance: 100.50},
    {UserID: 2, FullName: "Bob Jones", Email: "bob@example.com", Balance: 250.75},
}

df, _ := galleon.FromStructs(users)
fmt.Println(df)
// Column names: user_id, name, email, balance
```

### Skipping Fields

Use `galleon:"-"` to skip fields:

```go
type Order struct {
    OrderID     int64   `galleon:"order_id"`
    CustomerID  int64   `galleon:"customer_id"`
    Amount      float64 `galleon:"amount"`
    InternalKey string  `galleon:"-"`  // Skipped
}

orders := []Order{
    {OrderID: 101, CustomerID: 1, Amount: 99.99, InternalKey: "secret"},
    {OrderID: 102, CustomerID: 2, Amount: 149.99, InternalKey: "internal"},
}

df, _ := galleon.FromStructs(orders)
// DataFrame has columns: order_id, customer_id, amount
// InternalKey is not included
```

### Pointer to Struct Slices

Works with pointers too:

```go
type Product struct {
    SKU   string  `galleon:"sku"`
    Price float64 `galleon:"price"`
}

products := []*Product{
    {SKU: "LAPTOP-001", Price: 999.99},
    {SKU: "MOUSE-002", Price: 29.99},
}

df, _ := galleon.FromStructs(products)
```

### Supported Struct Field Types

| Go Type | Galleon Type | Notes |
|---------|--------------|-------|
| `int`, `int64` | Int64 | Converted to Int64 |
| `int32` | Int32 | - |
| `uint64` | UInt64 | - |
| `uint32` | UInt32 | - |
| `float64` | Float64 | - |
| `float32` | Float32 | - |
| `string` | String | - |
| `bool` | Bool | - |
| `*int64`, `*float64`, etc. | Same as value type | Nil becomes null |

### Nested Structs (Future)

Nested structs will be supported with Struct series (see Nested Data Types guide).

## Loading from Maps

`FromRecords` creates DataFrames from slices of maps with automatic type detection.

### Basic Map Loading

```go
records := []map[string]interface{}{
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.3},
    {"id": 3, "name": "Carol", "score": 91.2},
}

df, err := galleon.FromRecords(records)
if err != nil {
    panic(err)
}

fmt.Println(df)
// Auto-detected types:
//   id: Int64
//   name: String
//   score: Float64
```

### Type Detection Rules

The first non-nil value determines the column type:

```go
records := []map[string]interface{}{
    {"x": nil, "y": "hello"},      // y: String
    {"x": 42, "y": "world"},       // x: Int64
    {"x": 100, "y": "!"},
}

df, _ := galleon.FromRecords(records)
// Column types: x=Int64, y=String
```

### Mixed Data Example

```go
transactions := []map[string]interface{}{
    {
        "transaction_id": 1001,
        "amount": 99.99,
        "currency": "USD",
        "completed": true,
    },
    {
        "transaction_id": 1002,
        "amount": 149.50,
        "currency": "EUR",
        "completed": false,
    },
}

df, _ := galleon.FromRecords(transactions)
// Types: transaction_id=Int64, amount=Float64, currency=String, completed=Bool
```

### Handling Missing Keys

Missing keys in some records are treated as null:

```go
records := []map[string]interface{}{
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob"},              // age missing
    {"id": 3, "name": "Carol", "age": 35},
}

df, _ := galleon.FromRecords(records)
// Row 2 has null for 'age'
```

### Column Order

Column order is determined by the first record:

```go
records := []map[string]interface{}{
    {"z": 1, "a": 2, "m": 3},  // Order: z, a, m
    {"a": 4, "z": 5, "m": 6},  // Same order preserved
}

df, _ := galleon.FromRecords(records)
// Columns in order: z, a, m
```

## Loading from Files

See [I/O API Guide](../03-api/04-io.md) for comprehensive file loading documentation.

### Quick Reference

```go
// CSV
df, _ := galleon.ReadCSV("data.csv")

// JSON
df, _ := galleon.ReadJSON("data.json")

// Parquet
df, _ := galleon.ReadParquet("data.parquet")

// Lazy scanning
result, _ := galleon.ScanCSV("large.csv").
    Filter(galleon.Col("x").Gt(galleon.Lit(0))).
    Collect()
```

## Type Auto-Detection

### FromRecords Type Detection

`FromRecords` uses the following priority for type detection:

1. Scan all records to find first non-nil value for each column
2. Determine type based on Go type:
   - `int`, `int64` → Int64
   - `int32` → Int32
   - `float64` → Float64
   - `float32` → Float32
   - `string` → String
   - `bool` → Bool
3. All values in that column must be compatible with detected type

### FromStructs Type Detection

`FromStructs` uses struct field types directly:

- No type inference needed
- Compile-time type safety
- Faster than map-based loading

## Best Practices

### 1. Use Structs for Known Schemas

**Recommended** when you know the data structure:

```go
type Employee struct {
    ID     int64   `galleon:"employee_id"`
    Name   string  `galleon:"name"`
    Salary float64 `galleon:"salary"`
}

employees := []Employee{ /* ... */ }
df, _ := galleon.FromStructs(employees)
```

**Benefits:**
- Compile-time type checking
- Better performance (no reflection type inference)
- Self-documenting code
- IDE autocomplete support

### 2. Use Maps for Dynamic Data

**Recommended** when schema varies or is unknown:

```go
// JSON API response with varying fields
apiResponse := []map[string]interface{}{ /* ... */ }
df, _ := galleon.FromRecords(apiResponse)
```

**Benefits:**
- Flexible schema
- Works with dynamic JSON/API data
- Handles missing fields gracefully

### 3. Use Struct Tags for Clean Column Names

```go
// Bad: Column names match Go naming (ID, UserName, EmailAddress)
type User struct {
    ID           int64
    UserName     string
    EmailAddress string
}

// Good: Clean, consistent column names
type User struct {
    ID           int64  `galleon:"id"`
    UserName     string `galleon:"username"`
    EmailAddress string `galleon:"email"`
}
```

### 4. Skip Sensitive or Internal Fields

```go
type User struct {
    ID       int64  `galleon:"id"`
    Name     string `galleon:"name"`
    Password string `galleon:"-"`  // Never include in DataFrame
    Salt     string `galleon:"-"`
}
```

### 5. Validate After Loading

```go
df, err := galleon.FromRecords(records)
if err != nil {
    return fmt.Errorf("failed to load records: %w", err)
}

// Validate schema
requiredColumns := []string{"id", "name", "email"}
for _, col := range requiredColumns {
    if df.ColumnByName(col) == nil {
        return fmt.Errorf("missing required column: %s", col)
    }
}

// Validate data
if df.Len() == 0 {
    return fmt.Errorf("no data loaded")
}
```

### 6. Handle Large Datasets

For large datasets, consider lazy loading:

```go
// Instead of loading entire file
df, _ := galleon.ReadCSV("huge_file.csv")  // Loads all into memory

// Use lazy scanning with filtering
result, _ := galleon.ScanCSV("huge_file.csv").
    Filter(galleon.Col("date").Gte(galleon.Lit("2024-01-01"))).
    Select(galleon.Col("id"), galleon.Col("amount")).
    Collect()  // Only loads filtered/selected data
```

## Complete Examples

### Example 1: ETL Pipeline with Structs

```go
type RawTransaction struct {
    TransactionID string  `galleon:"transaction_id"`
    Amount        float64 `galleon:"amount"`
    Currency      string  `galleon:"currency"`
    Timestamp     string  `galleon:"timestamp"`
}

func loadTransactions(path string) (*galleon.DataFrame, error) {
    // Read from JSON file
    file, _ := os.ReadFile(path)
    var transactions []RawTransaction
    json.Unmarshal(file, &transactions)

    // Convert to DataFrame
    df, err := galleon.FromStructs(transactions)
    if err != nil {
        return nil, err
    }

    // Clean and transform
    result, err := df.Lazy().
        Filter(galleon.Col("amount").Gt(galleon.Lit(0.0))).
        Filter(galleon.Col("currency").Eq(galleon.Lit("USD"))).
        WithColumn("amount_cents",
            galleon.Col("amount").Mul(galleon.Lit(100.0))).
        Collect()

    return result, err
}
```

### Example 2: API Response to DataFrame

```go
func processAPIResponse(response []byte) (*galleon.DataFrame, error) {
    // Parse JSON API response
    var records []map[string]interface{}
    if err := json.Unmarshal(response, &records); err != nil {
        return nil, err
    }

    // Convert to DataFrame with auto type detection
    df, err := galleon.FromRecords(records)
    if err != nil {
        return nil, err
    }

    // Analyze
    stats, _ := df.Lazy().
        GroupBy(galleon.Col("category")).
        Agg(
            galleon.Col("value").Sum().Alias("total"),
            galleon.Col("value").Count().Alias("count"),
        ).
        Collect()

    return stats, nil
}
```

### Example 3: Combining Multiple Sources

```go
type Customer struct {
    CustomerID int64  `galleon:"customer_id"`
    Name       string `galleon:"name"`
}

// Load customers from struct
customers := []Customer{
    {CustomerID: 1, Name: "Alice"},
    {CustomerID: 2, Name: "Bob"},
}
customerDF, _ := galleon.FromStructs(customers)

// Load orders from map (dynamic JSON)
orders := []map[string]interface{}{
    {"order_id": 101, "customer_id": 1, "amount": 99.99},
    {"order_id": 102, "customer_id": 2, "amount": 149.50},
}
orderDF, _ := galleon.FromRecords(orders)

// Join them
result, _ := orderDF.Join(customerDF, galleon.On("customer_id"))
```

## Performance Considerations

### FromStructs vs FromRecords

**FromStructs is faster** because:
- No type inference at runtime
- Direct field access via reflection
- Type checking at compile time

**Benchmark comparison** (1000 rows):
- `FromStructs`: ~100µs
- `FromRecords`: ~200µs

### Memory Usage

Both methods create in-memory copies:
- Use lazy scanning for large files
- Consider streaming processing for huge datasets

### Type Safety

**FromStructs:**
- ✅ Compile-time type safety
- ✅ IDE support
- ✅ Refactoring-friendly

**FromRecords:**
- ⚠️ Runtime type detection
- ⚠️ Type errors at load time
- ⚠️ Less tooling support

## Summary

| Method | Best For | Type Safety | Performance | Flexibility |
|--------|----------|-------------|-------------|-------------|
| `NewDataFrame` | Known, simple data | ⭐⭐⭐ | ⭐⭐⭐ | ⭐ |
| `FromStructs` | Known schema, structs | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| `FromRecords` | Dynamic/unknown schema | ⭐ | ⭐⭐ | ⭐⭐⭐ |
| File I/O | Large datasets | ⭐⭐ | ⭐⭐ | ⭐⭐ |
| Lazy Scan | Huge files | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |

**General rule:** Use `FromStructs` when possible, `FromRecords` when needed, lazy scanning for large files.
