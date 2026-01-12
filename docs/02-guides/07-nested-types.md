# Nested Data Types Guide

Galleon supports nested data types for hierarchical and complex data structures: **Struct**, **List**, and **Array** series.

## Table of Contents

- [Overview](#overview)
- [Struct Series](#struct-series)
- [List Series](#list-series)
- [Array Series](#array-series)
- [Operations on Nested Types](#operations-on-nested-types)
- [Use Cases](#use-cases)
- [Best Practices](#best-practices)

## Overview

Nested types allow storing complex data in a single column:

| Type | Description | Example Use Case |
|------|-------------|------------------|
| **Struct** | Named fields of different types | User profiles, coordinates |
| **List** | Variable-length sequences | Tags, product categories |
| **Array** | Fixed-length sequences | RGB colors, embeddings |

### Why Nested Types?

**Traditional approach** (flat schema):
```
user_id | user_name | user_email | user_age
1       | Alice     | alice@...  | 30
```

**Nested approach** (Struct column):
```
user_id | user_info (struct)
1       | {name: "Alice", email: "alice@...", age: 30}
```

**Benefits:**
- Logical grouping of related fields
- Cleaner schema for complex data
- Better alignment with JSON/Parquet formats
- Efficient storage of hierarchical data

## Struct Series

A Struct series stores structured records with named fields of different types.

### Creating Struct Series

```go
// Create from field map
nameField := galleon.NewSeriesString("name", []string{"Alice", "Bob", "Carol"})
ageField := galleon.NewSeriesInt64("age", []int64{30, 25, 35})
emailField := galleon.NewSeriesString("email", []string{
    "alice@example.com",
    "bob@example.com",
    "carol@example.com",
})

fields := map[string]*galleon.Series{
    "name":  nameField,
    "age":   ageField,
    "email": emailField,
}

structSeries := galleon.NewStructSeries("user_info", fields)
fmt.Println(structSeries.Len())  // 3
```

### Creating from Series List

```go
// Alternative: create from ordered series
fieldNames := []string{"name", "age", "email"}
series := []*galleon.Series{nameField, ageField, emailField}

structSeries := galleon.NewStructSeriesFromSeries("user_info", fieldNames, series)
```

### Accessing Struct Fields

```go
// Get individual field
nameColumn := structSeries.Field("name")
fmt.Println(nameColumn.Strings())  // ["Alice", "Bob", "Carol"]

ageColumn := structSeries.Field("age")
fmt.Println(ageColumn.Int64())  // [30, 25, 35]

// Get all field names
fields := structSeries.FieldNames()
fmt.Println(fields)  // ["name", "age", "email"]

// Get all fields as map
allFields := structSeries.Fields()
```

### Accessing Individual Struct Values

```go
// Get entire struct at row index
row := structSeries.GetRow(0)
fmt.Println(row)
// Output: map[string]interface{}{
//     "name": "Alice",
//     "age": int64(30),
//     "email": "alice@example.com",
// }

// Iterate through all rows
for i := 0; i < structSeries.Len(); i++ {
    user := structSeries.GetRow(i)
    fmt.Printf("User %d: %v\n", i, user)
}
```

### Type Information

```go
// Get struct type metadata
structType := structSeries.StructType()
fmt.Println(structType.NumFields())  // 3

// Get field types
for _, field := range structType.Fields() {
    fmt.Printf("%s: %v\n", field.Name, field.Type)
}
// Output:
// name: String
// age: Int64
// email: String
```

### Unnesting Structs

Convert struct columns back to flat columns:

```go
// Unnest without prefix
df := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    structSeries,
)
// Columns: id, user_info

unnested, _ := structSeries.Unnest()
// Result: separate columns for name, age, email

// Unnest with prefix
unnestedPrefixed, _ := structSeries.UnnestPrefixed("user_")
// Result: columns named user_name, user_age, user_email
```

### Use in DataFrames

```go
// DataFrame with struct column
df := galleon.NewDataFrame(
    galleon.NewSeriesInt64("user_id", []int64{1, 2, 3}),
    structSeries,
)

fmt.Println(df)
// +----------+-----------------------------------------+
// | user_id  | user_info                               |
// +----------+-----------------------------------------+
// | 1        | {name: Alice, age: 30, email: alice@...}|
// | 2        | {name: Bob, age: 25, email: bob@...}    |
// | 3        | {name: Carol, age: 35, email: carol@...}|
// +----------+-----------------------------------------+
```

## List Series

A List series stores variable-length sequences in each row.

### Creating List Series

#### From Slice of Slices (Float64)

```go
// Variable-length lists
data := [][]float64{
    {1.0, 2.0, 3.0},           // 3 elements
    {4.0, 5.0},                // 2 elements
    {6.0, 7.0, 8.0, 9.0},      // 4 elements
    {},                         // 0 elements
}

listSeries := galleon.NewListSeriesFromSlicesF64("scores", data)
fmt.Println(listSeries.Len())  // 4 rows
```

#### From Slice of Slices (Int64)

```go
tags := [][]int64{
    {1, 2, 3},
    {4, 5},
    {6},
}

listSeries := galleon.NewListSeriesFromSlicesI64("tag_ids", tags)
```

#### From Slice of Slices (String)

```go
categories := [][]string{
    {"electronics", "computers", "laptops"},
    {"books", "fiction"},
    {"clothing", "shoes"},
}

listSeries := galleon.NewListSeriesFromSlicesString("categories", categories)
```

#### Manual Construction (Advanced)

```go
// Flatten all values into single array
allValues := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}

// Offsets mark where each list starts
// offsets[i] = start index of row i
// offsets[i+1] = end index (exclusive)
offsets := []int32{0, 3, 5, 9, 9}  // Row 0: [0,3), Row 1: [3,5), Row 2: [5,9), Row 3: [9,9)

valuesSeriesFloat64 := galleon.NewSeriesFloat64("values", allValues)
listSeries := galleon.NewListSeries("scores", offsets, valuesSeries)
```

### Accessing List Elements

```go
tags := [][]int64{
    {1, 2, 3},
    {4, 5},
    {6, 7, 8, 9},
}
listSeries := galleon.NewListSeriesFromSlicesI64("tags", tags)

// Get length of list at row
len0 := listSeries.GetListLen(0)  // 3
len1 := listSeries.GetListLen(1)  // 2

// Get entire list at row
list0 := listSeries.GetListI64(0)  // []int64{1, 2, 3}
list1 := listSeries.GetListI64(1)  // []int64{4, 5}

// Get specific element from list
elem := listSeries.GetElement(0, 1)  // Second element of first list: 2
```

### List Lengths

```go
// Get series of list lengths
lengths := listSeries.ListLengths()
fmt.Println(lengths.Int64())  // [3, 2, 4]
```

### List Aggregations

Compute aggregations within each list:

```go
scores := [][]float64{
    {90.0, 85.0, 88.0},
    {75.0, 80.0},
    {95.0, 92.0, 90.0, 93.0},
}
listSeries := galleon.NewListSeriesFromSlicesF64("scores", scores)

// Sum of each list
sums := listSeries.ListSum()
fmt.Println(sums.Float64())  // [263.0, 155.0, 370.0]

// Mean of each list
means := listSeries.ListMean()
fmt.Println(means.Float64())  // [87.67, 77.5, 92.5]

// Min of each list
mins := listSeries.ListMin()
fmt.Println(mins.Float64())  // [85.0, 75.0, 90.0]

// Max of each list
maxs := listSeries.ListMax()
fmt.Println(maxs.Float64())  // [90.0, 80.0, 95.0]
```

### Exploding Lists

Convert lists to multiple rows (unnest):

```go
tags := [][]string{
    {"python", "golang", "rust"},
    {"javascript", "typescript"},
}
listSeries := galleon.NewListSeriesFromSlicesString("tags", tags)

// Explode to one row per element
exploded := listSeries.Explode()
fmt.Println(exploded.Strings())
// ["python", "golang", "rust", "javascript", "typescript"]
```

With DataFrame context:

```go
df := galleon.NewDataFrame(
    galleon.NewSeriesInt64("product_id", []int64{1, 2}),
    listSeries,
)
// +------------+-----------------------------+
// | product_id | tags                        |
// +------------+-----------------------------+
// | 1          | ["python", "golang", "rust"]|
// | 2          | ["javascript", "typescript"]|
// +------------+-----------------------------+

explodedDF := df.Lazy().
    WithColumn("tag", galleon.Col("tags").List().Explode()).
    Select(galleon.Col("product_id"), galleon.Col("tag")).
    Collect()
// +------------+------------+
// | product_id | tag        |
// +------------+------------+
// | 1          | python     |
// | 1          | golang     |
// | 1          | rust       |
// | 2          | javascript |
// | 2          | typescript |
// +------------+------------+
```

### Type Information

```go
listType := listSeries.ListType()
fmt.Println(listType.ElementType())  // Float64, Int64, String, etc.
```

## Array Series

Array series store fixed-length sequences (coming soon).

### Creating Array Series (Future)

```go
// RGB colors (fixed length: 3)
colors := [][]uint8{
    {255, 0, 0},      // Red
    {0, 255, 0},      // Green
    {0, 0, 255},      // Blue
}

arraySeries := galleon.NewArraySeriesFromSlicesU8("rgb", colors, 3)
```

### Use Cases

- Image embeddings (fixed-size vectors)
- RGB/RGBA color values
- Coordinate tuples (x, y, z)
- Fixed-size feature vectors for ML

## Operations on Nested Types

### Expression API for Nested Types

#### Struct Field Access

```go
// Access struct field in expressions
result, _ := df.Lazy().
    Select(
        galleon.Col("user_info").Field("name").Alias("name"),
        galleon.Col("user_info").Field("age").Alias("age"),
    ).
    Collect()
```

#### List Operations

```go
// Get list element by index
result, _ := df.Lazy().
    WithColumn("first_tag", galleon.Col("tags").List().Get(0)).
    Collect()

// Get list length
result, _ := df.Lazy().
    WithColumn("num_tags", galleon.Col("tags").List().Len()).
    Collect()

// List aggregations
result, _ := df.Lazy().
    WithColumn("avg_score", galleon.Col("scores").List().Mean()).
    WithColumn("max_score", galleon.Col("scores").List().Max()).
    Collect()

// Explode lists
result, _ := df.Lazy().
    Select(
        galleon.Col("product_id"),
        galleon.Col("tags").List().Explode().Alias("tag"),
    ).
    Collect()
```

### Filtering with Nested Types

```go
// Filter by struct field
result, _ := df.Lazy().
    Filter(galleon.Col("user_info").Field("age").Gt(galleon.Lit(25))).
    Collect()

// Filter by list length
result, _ := df.Lazy().
    Filter(galleon.Col("tags").List().Len().Gt(galleon.Lit(2))).
    Collect()

// Filter by list aggregation
result, _ := df.Lazy().
    Filter(galleon.Col("scores").List().Mean().Gt(galleon.Lit(80.0))).
    Collect()
```

### GroupBy with Nested Types

```go
// Group by struct field
result, _ := df.Lazy().
    GroupBy(galleon.Col("user_info").Field("city")).
    Agg(galleon.Col("amount").Sum().Alias("total")).
    Collect()

// Aggregate lists
result, _ := df.Lazy().
    GroupBy(galleon.Col("category")).
    Agg(
        galleon.Col("tags").List().Len().Mean().Alias("avg_tags"),
        galleon.Col("scores").List().Mean().Mean().Alias("avg_of_avgs"),
    ).
    Collect()
```

## Use Cases

### Use Case 1: JSON Data with Nested Objects

```go
// API response with nested user data
type APIResponse struct {
    UserID int64
    User   struct {
        Name  string
        Email string
        Age   int64
    }
    Purchases []float64
}

// Store as struct and list columns
nameCol := galleon.NewSeriesString("name", names)
emailCol := galleon.NewSeriesString("email", emails)
ageCol := galleon.NewSeriesInt64("age", ages)

userStructCol := galleon.NewStructSeries("user", map[string]*galleon.Series{
    "name":  nameCol,
    "email": emailCol,
    "age":   ageCol,
})

purchasesListCol := galleon.NewListSeriesFromSlicesF64("purchases", purchaseLists)

df := galleon.NewDataFrame(
    galleon.NewSeriesInt64("user_id", userIDs),
    userStructCol,
    purchasesListCol,
)
```

### Use Case 2: Multi-Valued Attributes

```go
// Product catalog with multiple categories and tags
products := []struct {
    SKU        string
    Categories []string
    Tags       []string
    Prices     []float64  // Historical prices
}{
    {
        SKU:        "LAPTOP-001",
        Categories: []string{"Electronics", "Computers", "Laptops"},
        Tags:       []string{"gaming", "high-performance"},
        Prices:     []float64{999.99, 949.99, 899.99},
    },
    // ...
}

// Store categories, tags, and prices as list columns
categoriesCol := galleon.NewListSeriesFromSlicesString("categories", ...)
tagsCol := galleon.NewListSeriesFromSlicesString("tags", ...)
pricesCol := galleon.NewListSeriesFromSlicesF64("price_history", ...)
```

### Use Case 3: Time Series with Metadata

```go
// Sensor data with location metadata
locationCol := galleon.NewStructSeries("location", map[string]*galleon.Series{
    "latitude":  latCol,
    "longitude": lonCol,
    "altitude":  altCol,
})

readingsCol := galleon.NewListSeriesFromSlicesF64("readings", hourlyReadings)

df := galleon.NewDataFrame(
    galleon.NewSeriesString("sensor_id", sensorIDs),
    locationCol,
    readingsCol,
)

// Analyze
result, _ := df.Lazy().
    Filter(galleon.Col("location").Field("altitude").Gt(galleon.Lit(1000.0))).
    WithColumn("avg_reading", galleon.Col("readings").List().Mean()).
    Collect()
```

### Use Case 4: Document Storage

```go
// Document with metadata and content sections
metadataCol := galleon.NewStructSeries("metadata", map[string]*galleon.Series{
    "author":      authorCol,
    "created_at":  createdCol,
    "tags":        tagCountCol,
})

// Each document has multiple sections
sectionsCol := galleon.NewListSeriesFromSlicesString("sections", documentSections)

df := galleon.NewDataFrame(
    galleon.NewSeriesString("doc_id", docIDs),
    metadataCol,
    sectionsCol,
)
```

## Best Practices

### 1. When to Use Nested Types

**Use Struct when:**
- Logically related fields belong together
- You're working with JSON/Parquet data
- You want cleaner schemas for complex data

**Use List when:**
- Number of elements varies per row
- You have multi-valued attributes
- You're storing sequences or collections

**Use Array when (future):**
- All sequences have the same length
- You're storing embeddings or fixed-size vectors
- You need better performance than List

### 2. Performance Considerations

**Struct Series:**
- ✅ Efficient: Fields stored as separate columns internally
- ✅ Fast field access
- ⚠️ Unnesting creates copies

**List Series:**
- ✅ Compact storage using offsets
- ⚠️ Element access requires offset calculation
- ⚠️ Exploding creates many rows

### 3. Type Safety

```go
// Good: Check field exists before accessing
if userCol := structSeries.Field("email"); userCol != nil {
    emails := userCol.Strings()
}

// Good: Validate list element type
if listSeries.ElementType() == galleon.Float64 {
    values := listSeries.GetListF64(0)
}
```

### 4. Memory Usage

Lists store values contiguously:
```go
// Efficient: Single underlying array
values := []float64{1, 2, 3, 4, 5, 6}
offsets := []int32{0, 2, 5, 6}  // [[1,2], [3,4,5], [6]]
```

Structs store each field separately:
```go
// Each field is a separate Series
// Total memory = sum of field memories
```

### 5. Schema Evolution

```go
// Adding fields to struct requires recreation
oldStruct := galleon.NewStructSeries("data", oldFields)

// Add new field
newFields := oldStruct.Fields()
newFields["new_field"] = newFieldSeries

newStruct := galleon.NewStructSeries("data", newFields)
```

## Summary

| Feature | Struct | List | Array (future) |
|---------|--------|------|----------------|
| **Structure** | Named fields | Variable-length | Fixed-length |
| **Element types** | Mixed | Uniform | Uniform |
| **Access** | By field name | By index | By index |
| **Size per row** | Fixed fields | Variable | Fixed |
| **Best for** | Records | Collections | Vectors |
| **Unnest** | To columns | To rows | To columns |

**Nested types enable:**
- Cleaner schemas for complex data
- Better JSON/Parquet compatibility
- Logical grouping of related data
- Efficient storage of hierarchical structures

**Next Steps:**
- See [Expression API](../03-api/03-expressions.md) for nested type operations
- See [I/O Guide](../03-api/04-io.md) for reading nested Parquet/JSON
- See [Data Loading Guide](06-data-loading.md) for creating DataFrames with nested types
