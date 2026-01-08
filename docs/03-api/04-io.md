# I/O API Reference

Galleon supports reading and writing DataFrames in various formats including CSV, JSON, and Parquet.

## CSV Operations

### ReadCSV

Reads a CSV file into a DataFrame.

```go
func ReadCSV(path string, opts CSVReadOptions) (*DataFrame, error)
```

**Example:**
```go
df, err := galleon.ReadCSV("data.csv", galleon.DefaultCSVReadOptions())
```

### CSVReadOptions

Configuration for CSV reading.

```go
type CSVReadOptions struct {
    // Delimiter character (default: ',')
    Delimiter rune

    // Whether file has header row (default: true)
    HasHeader bool

    // Column names if no header
    ColumnNames []string

    // Column types (optional, inferred if not specified)
    ColumnTypes map[string]DType

    // Whether to infer types automatically (default: true)
    InferTypes bool

    // Values to treat as null
    NullValues []string

    // Number of rows to skip at start
    SkipRows int

    // Maximum rows to read (0 = all)
    MaxRows int

    // Comment character (lines starting with this are skipped)
    CommentChar rune
}
```

**Example with options:**
```go
opts := galleon.CSVReadOptions{
    Delimiter:   ';',
    HasHeader:   true,
    NullValues:  []string{"NA", "null", ""},
    ColumnTypes: map[string]galleon.DType{
        "id":    galleon.Int64,
        "value": galleon.Float64,
    },
}
df, err := galleon.ReadCSV("data.csv", opts)
```

### DefaultCSVReadOptions

Returns default CSV read options.

```go
func DefaultCSVReadOptions() CSVReadOptions
```

### WriteCSV

Writes a DataFrame to a CSV file.

```go
func (df *DataFrame) WriteCSV(path string) error
func (df *DataFrame) WriteCSVWithOptions(path string, opts CSVWriteOptions) error
```

### CSVWriteOptions

Configuration for CSV writing.

```go
type CSVWriteOptions struct {
    // Delimiter character (default: ',')
    Delimiter rune

    // Whether to write header row (default: true)
    WriteHeader bool

    // String to represent null values (default: "")
    NullString string

    // Quote character (default: '"')
    QuoteChar rune

    // Line ending (default: "\n")
    LineEnding string
}
```

### DefaultCSVWriteOptions

Returns default CSV write options.

```go
func DefaultCSVWriteOptions() CSVWriteOptions
```

## JSON Operations

### ReadJSON

Reads a JSON file into a DataFrame.

```go
func ReadJSON(path string) (*DataFrame, error)
```

Supports two formats:
1. **Array of objects**: `[{"a": 1}, {"a": 2}]`
2. **Column-oriented**: `{"a": [1, 2], "b": [3, 4]}`

**Example:**
```go
df, err := galleon.ReadJSON("data.json")
```

### WriteJSON

Writes a DataFrame to a JSON file (array of objects format).

```go
func (df *DataFrame) WriteJSON(path string) error
```

**Output format:**
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
```

### WriteJSONL

Writes a DataFrame to JSON Lines format (one JSON object per line).

```go
func (df *DataFrame) WriteJSONL(path string) error
```

**Output format:**
```json
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
```

## Parquet Operations

### ReadParquet

Reads a Parquet file into a DataFrame.

```go
func ReadParquet(path string) (*DataFrame, error)
```

**Example:**
```go
df, err := galleon.ReadParquet("data.parquet")
```

**Supported types:**
- Integers (INT32, INT64)
- Floats (FLOAT, DOUBLE)
- Strings (BYTE_ARRAY with UTF8)
- Booleans

## Lazy I/O (Scanning)

Lazy scanning defers reading until execution and enables optimizations.

### ScanCSV

Creates a LazyFrame that scans a CSV file.

```go
func ScanCSV(path string) *LazyFrame
func ScanCSVWithOptions(path string, opts CSVReadOptions) *LazyFrame
```

**Example:**
```go
// Filter and aggregate without loading entire file
result, _ := galleon.ScanCSV("large_file.csv").
    Filter(Col("value").Gt(Lit(100))).
    GroupBy(Col("category")).
    Agg(Col("value").Sum()).
    Collect()
```

### ScanParquet

Creates a LazyFrame that scans a Parquet file.

```go
func ScanParquet(path string) *LazyFrame
```

**Benefits of lazy scanning:**
- Projection pushdown: Only read needed columns
- Predicate pushdown: Filter during read (where supported)
- Memory efficiency: Process in chunks

## Error Handling

All I/O functions return errors for common issues:

```go
df, err := galleon.ReadCSV("data.csv", opts)
if err != nil {
    switch {
    case os.IsNotExist(err):
        // File not found
    case strings.Contains(err.Error(), "parse"):
        // Parse error
    default:
        // Other error
    }
}
```

## Type Inference

When `InferTypes: true` (default), types are inferred:

| Sample Value | Inferred Type |
|--------------|---------------|
| "123" | Int64 |
| "123.45" | Float64 |
| "true"/"false" | Bool |
| "2024-01-15" | String* |
| "hello" | String |

*Date parsing can be enabled with custom options.

## Complete Examples

### CSV Round-Trip

```go
// Create DataFrame
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
    galleon.NewSeriesFloat64("score", []float64{85.5, 92.3, 78.9}),
)

// Write to CSV
err := df.WriteCSV("output.csv")

// Read back
df2, err := galleon.ReadCSV("output.csv", galleon.DefaultCSVReadOptions())
```

### Large File Processing

```go
// Process large CSV file lazily
result, err := galleon.ScanCSV("very_large.csv").
    // Filter early to reduce data
    Filter(galleon.Col("status").Eq(galleon.Lit("active"))).
    // Select only needed columns
    Select(
        galleon.Col("id"),
        galleon.Col("amount"),
    ).
    // Aggregate
    GroupBy(galleon.Col("id")).
    Agg(galleon.Col("amount").Sum().Alias("total")).
    // Sort and limit
    Sort(galleon.Col("total"), false).
    Limit(100).
    Collect()
```

### Mixed Format Pipeline

```go
// Read from Parquet
sales, _ := galleon.ReadParquet("sales.parquet")

// Read from CSV
products, _ := galleon.ReadCSV("products.csv", galleon.DefaultCSVReadOptions())

// Join and aggregate
result, _ := sales.Join(products, galleon.On("product_id"))

// Write results
result.WriteJSON("results.json")
```
