# Type System Reference

Galleon uses Apache Arrow-compatible types for efficient memory layout and interoperability.

## Data Types

### DType Constants

```go
const (
    Float64  DType = iota  // 64-bit floating point
    Float32                // 32-bit floating point
    Int64                  // 64-bit signed integer
    Int32                  // 32-bit signed integer
    UInt64                 // 64-bit unsigned integer
    UInt32                 // 32-bit unsigned integer
    Bool                   // Boolean (true/false)
    String                 // UTF-8 string
    DateTime               // Date and time
    Duration               // Time duration
    Null                   // Null/missing value
)
```

### Type Properties

| Type | Size (bytes) | Range | Use Case |
|------|--------------|-------|----------|
| Float64 | 8 | ~1.8e308 | General numeric, high precision |
| Float32 | 4 | ~3.4e38 | Memory-constrained, moderate precision |
| Int64 | 8 | -9.2e18 to 9.2e18 | Large integers, IDs |
| Int32 | 4 | -2.1e9 to 2.1e9 | Moderate integers |
| UInt64 | 8 | 0 to 1.8e19 | Non-negative large integers |
| UInt32 | 4 | 0 to 4.3e9 | Non-negative integers |
| Bool | 1 | true/false | Flags, filters |
| String | Variable | - | Text data |

## Creating Typed Series

### Numeric Series

```go
// Float64
f64 := galleon.NewSeriesFloat64("values", []float64{1.1, 2.2, 3.3})

// Float32
f32 := galleon.NewSeriesFloat32("values", []float32{1.1, 2.2, 3.3})

// Int64
i64 := galleon.NewSeriesInt64("ids", []int64{1, 2, 3})

// Int32
i32 := galleon.NewSeriesInt32("counts", []int32{10, 20, 30})
```

### String and Bool Series

```go
// String
str := galleon.NewSeriesString("names", []string{"Alice", "Bob", "Charlie"})

// Bool
bools := galleon.NewSeriesBool("flags", []bool{true, false, true})
```

## Type Checking

### Series DType

```go
series := galleon.NewSeriesInt64("id", []int64{1, 2, 3})
dtype := series.DType()

switch dtype {
case galleon.Int64:
    fmt.Println("Int64 series")
case galleon.Float64:
    fmt.Println("Float64 series")
case galleon.String:
    fmt.Println("String series")
default:
    fmt.Println("Other type")
}
```

### DataFrame Schema

```go
df, _ := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesString("name", []string{"A", "B", "C"}),
    galleon.NewSeriesFloat64("value", []float64{1.0, 2.0, 3.0}),
)

schema := df.Schema()
for _, field := range schema.Fields {
    fmt.Printf("%s: %v\n", field.Name, field.Type)
}
// Output:
// id: Int64
// name: String
// value: Float64
```

## Type Casting

### Series Casting

```go
// Int64 to Float64
intSeries := galleon.NewSeriesInt64("values", []int64{1, 2, 3})
floatSeries := intSeries.AsFloat64()

// Float64 to Int64 (truncates decimals)
floatSeries := galleon.NewSeriesFloat64("values", []float64{1.7, 2.3, 3.9})
intSeries := floatSeries.AsInt64()  // [1, 2, 3]

// Numeric to String
numSeries := galleon.NewSeriesInt64("ids", []int64{1, 2, 3})
strSeries := numSeries.AsString()  // ["1", "2", "3"]
```

### Expression Casting

```go
// Cast in expressions
df = df.WithColumn("id_float", galleon.Col("id").Cast(galleon.Float64))

// Filter with type consideration
df = df.Filter(galleon.Col("value").Cast(galleon.Int64).Eq(galleon.Lit(100)))
```

## Type Inference

### CSV Reading

When reading CSV files, types are inferred if not specified:

```go
// Auto-inference
df, _ := galleon.ReadCSV("data.csv", galleon.CSVReadOptions{
    InferTypes: true,  // Default
})

// Explicit types
df, _ := galleon.ReadCSV("data.csv", galleon.CSVReadOptions{
    InferTypes: false,
    ColumnTypes: map[string]galleon.DType{
        "id":    galleon.Int64,
        "price": galleon.Float64,
        "name":  galleon.String,
    },
})
```

### Inference Rules

| Pattern | Inferred Type |
|---------|---------------|
| "123" | Int64 |
| "-456" | Int64 |
| "12.34" | Float64 |
| "-0.5" | Float64 |
| "1e10" | Float64 |
| "true", "false" | Bool |
| "TRUE", "FALSE" | Bool |
| Other | String |

## Null Handling

### Null Values

Galleon uses validity bitmaps to track null values:

```go
// Create series with nulls (using NaN for float)
values := galleon.NewSeriesFloat64("values", []float64{1.0, math.NaN(), 3.0})

// Check for nulls
isNull := values.IsNull()  // [false, true, false]

// Fill nulls
filled := values.FillNull(0.0)  // [1.0, 0.0, 3.0]
```

### Null in Expressions

```go
// Filter nulls
df = df.Filter(galleon.Col("value").IsNotNull())

// Replace nulls
df = df.WithColumn("value", galleon.Col("value").FillNull(galleon.Lit(0)))
```

### Null Values in CSV

```go
opts := galleon.CSVReadOptions{
    NullValues: []string{"NA", "null", "", "-"},
}
df, _ := galleon.ReadCSV("data.csv", opts)
```

## Type Compatibility

### Arithmetic Operations

| Left Type | Right Type | Result Type |
|-----------|------------|-------------|
| Int64 | Int64 | Int64 |
| Int64 | Float64 | Float64 |
| Float64 | Float64 | Float64 |
| Int32 | Int32 | Int32 |
| Int32 | Int64 | Int64 |

### Comparison Operations

| Left Type | Right Type | Result Type |
|-----------|------------|-------------|
| Any numeric | Any numeric | Bool |
| String | String | Bool |
| Bool | Bool | Bool |

### Aggregation Results

| Input Type | Sum | Mean | Min/Max |
|------------|-----|------|---------|
| Int64 | Int64 | Float64 | Int64 |
| Int32 | Int64 | Float64 | Int32 |
| Float64 | Float64 | Float64 | Float64 |
| Float32 | Float32 | Float32 | Float32 |

## Memory Layout

### Arrow Compatibility

Galleon uses Arrow-compatible memory layout:

```
Series Memory Layout:
┌──────────────────────────────────────┐
│ Validity Bitmap (1 bit per element)  │
├──────────────────────────────────────┤
│ Values Buffer (type-specific)        │
│ - Float64: 8 bytes × N elements      │
│ - Int32: 4 bytes × N elements        │
│ - Bool: 1 bit × N elements           │
└──────────────────────────────────────┘

String Series Layout:
┌──────────────────────────────────────┐
│ Validity Bitmap                      │
├──────────────────────────────────────┤
│ Offsets (Int32/Int64)                │
├──────────────────────────────────────┤
│ Data Buffer (UTF-8 bytes)            │
└──────────────────────────────────────┘
```

### Alignment

Data is aligned for SIMD operations:
- Float64: 64-byte aligned
- Float32: 32-byte aligned
- Int64: 64-byte aligned
- Int32: 32-byte aligned

## Type Best Practices

### 1. Use Appropriate Precision

```go
// Use Int32 if values fit (saves memory)
if maxValue < 2_000_000_000 {
    series = galleon.NewSeriesInt32("counts", data)
}

// Use Float32 for large datasets with moderate precision needs
series = galleon.NewSeriesFloat32("temperatures", largeData)
```

### 2. Specify Types for CSV

```go
// Explicit types prevent inference errors
opts := galleon.CSVReadOptions{
    ColumnTypes: map[string]galleon.DType{
        "zipcode": galleon.String,  // Preserve leading zeros
        "amount":  galleon.Float64,
        "count":   galleon.Int64,
    },
}
```

### 3. Check Types Before Operations

```go
// Verify type before extraction
if series.DType() == galleon.Float64 {
    data := series.Float64()
    // Process float data
}
```

## Type Conversion Table

| From → To | Float64 | Float32 | Int64 | Int32 | String | Bool |
|-----------|---------|---------|-------|-------|--------|------|
| Float64 | - | Truncate | Truncate | Truncate | Format | >0 |
| Float32 | Extend | - | Truncate | Truncate | Format | >0 |
| Int64 | Exact | Truncate | - | Truncate | Format | ≠0 |
| Int32 | Exact | Exact | Extend | - | Format | ≠0 |
| String | Parse | Parse | Parse | Parse | - | Parse |
| Bool | 0/1 | 0/1 | 0/1 | 0/1 | "true"/"false" | - |
