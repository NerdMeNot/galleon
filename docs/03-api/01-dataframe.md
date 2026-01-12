# DataFrame API Reference

The `DataFrame` is the primary data structure in Galleon, representing a table with named columns of various types.

## Creating DataFrames

### NewDataFrame

Creates a new DataFrame from one or more Series.

```go
func NewDataFrame(series ...*Series) (*DataFrame, error)
```

**Example:**
```go
df, err := galleon.NewDataFrame(
    galleon.NewSeriesInt64("id", []int64{1, 2, 3}),
    galleon.NewSeriesFloat64("value", []float64{1.5, 2.5, 3.5}),
)
```

### NewEmptyDataFrame

Creates an empty DataFrame with a specified schema.

```go
func NewEmptyDataFrame(schema Schema) *DataFrame
```

### FromStructs

Creates a DataFrame from a slice of structs using reflection.

```go
func FromStructs(data interface{}) (*DataFrame, error)
```

**Supported struct tags:**
- `galleon:"column_name"` - Rename field in DataFrame
- `galleon:"-"` - Skip field

**Example:**
```go
type User struct {
    UserID   int64   `galleon:"user_id"`
    FullName string  `galleon:"name"`
    Email    string  `galleon:"email"`
    Internal string  `galleon:"-"`  // Skipped
}

users := []User{
    {UserID: 1, FullName: "Alice", Email: "alice@example.com", Internal: "secret"},
    {UserID: 2, FullName: "Bob", Email: "bob@example.com", Internal: "hidden"},
}

df, err := galleon.FromStructs(users)
// DataFrame has columns: user_id, name, email
```

**Supported field types:**
- `int`, `int64`, `int32` → Int64/Int32
- `uint64`, `uint32` → UInt64/UInt32
- `float64`, `float32` → Float64/Float32
- `string` → String
- `bool` → Bool
- Pointer types (`*int64`, etc.) → same as value type (nil becomes null)

**See also:** [Data Loading Guide](../02-guides/06-data-loading.md#loading-from-structs)

### FromRecords

Creates a DataFrame from a slice of maps with automatic type detection.

```go
func FromRecords(records []map[string]interface{}) (*DataFrame, error)
```

**Type detection rules:**
- First non-nil value determines column type
- All values must be compatible with detected type
- Missing keys treated as null

**Example:**
```go
records := []map[string]interface{}{
    {"id": 1, "name": "Alice", "score": 95.5, "active": true},
    {"id": 2, "name": "Bob", "score": 87.3, "active": false},
    {"id": 3, "name": "Carol", "score": 91.2, "active": true},
}

df, err := galleon.FromRecords(records)
// Auto-detected types:
//   id: Int64
//   name: String
//   score: Float64
//   active: Bool
```

**Column order:** Determined by first record

**Supported value types:**
- `int`, `int64` → Int64
- `int32` → Int32
- `float64` → Float64
- `float32` → Float32
- `string` → String
- `bool` → Bool

**See also:** [Data Loading Guide](../02-guides/06-data-loading.md#loading-from-maps)

## Properties

### Height

Returns the number of rows.

```go
func (df *DataFrame) Height() int
```

### Width

Returns the number of columns.

```go
func (df *DataFrame) Width() int
```

### ColumnNames

Returns the names of all columns.

```go
func (df *DataFrame) ColumnNames() []string
```

### Schema

Returns the DataFrame schema (column names and types).

```go
func (df *DataFrame) Schema() Schema
```

## Column Access

### Column

Returns a Series by name.

```go
func (df *DataFrame) Column(name string) *Series
```

**Example:**
```go
values := df.Column("value")
data := values.Float64()
```

### ColumnByIndex

Returns a Series by index.

```go
func (df *DataFrame) ColumnByIndex(index int) *Series
```

### HasColumn

Checks if a column exists.

```go
func (df *DataFrame) HasColumn(name string) bool
```

## Selection Operations

### Select

Returns a new DataFrame with only the specified columns.

```go
func (df *DataFrame) Select(columns ...string) *DataFrame
```

**Example:**
```go
selected := df.Select("id", "value")
```

### Drop

Returns a new DataFrame without the specified columns.

```go
func (df *DataFrame) Drop(columns ...string) *DataFrame
```

**Example:**
```go
dropped := df.Drop("temp_column")
```

### Head

Returns the first n rows.

```go
func (df *DataFrame) Head(n int) *DataFrame
```

### Tail

Returns the last n rows.

```go
func (df *DataFrame) Tail(n int) *DataFrame
```

### Slice

Returns rows from start to end (exclusive).

```go
func (df *DataFrame) Slice(start, end int) *DataFrame
```

## Filtering

### Filter

Filters rows based on an expression.

```go
func (df *DataFrame) Filter(expr Expr) *DataFrame
```

**Example:**
```go
filtered := df.Filter(Col("value").Gt(Lit(10.0)))
```

### FilterByMask

Filters rows based on a boolean mask.

```go
func (df *DataFrame) FilterByMask(mask []byte) (*DataFrame, error)
```

**Example:**
```go
mask := galleon.FilterMaskGreaterThanF64(values, 10.0)
byteMask := make([]byte, len(mask))
for i, m := range mask {
    if m { byteMask[i] = 1 }
}
filtered, _ := df.FilterByMask(byteMask)
```

## Sorting

### Sort

Sorts by a single column.

```go
func (df *DataFrame) Sort(column string, ascending bool) *DataFrame
```

**Example:**
```go
sorted := df.Sort("value", false)  // descending
```

### SortBy

Sorts by multiple columns.

```go
func (df *DataFrame) SortBy(columns ...SortColumn) *DataFrame

type SortColumn struct {
    Name       string
    Descending bool
}
```

**Example:**
```go
sorted := df.SortBy(
    galleon.SortColumn{Name: "category", Descending: false},
    galleon.SortColumn{Name: "value", Descending: true},
)
```

## Transformation

### WithColumn

Adds or replaces a column.

```go
func (df *DataFrame) WithColumn(name string, expr Expr) *DataFrame
```

**Example:**
```go
df = df.WithColumn("doubled", Col("value").Mul(Lit(2.0)))
```

### Rename

Renames columns.

```go
func (df *DataFrame) Rename(mapping map[string]string) *DataFrame
```

**Example:**
```go
renamed := df.Rename(map[string]string{
    "old_name": "new_name",
})
```

## GroupBy Operations

### GroupBy

Groups the DataFrame by one or more columns.

```go
func (df *DataFrame) GroupBy(columns ...string) *GroupBy
```

**Example:**
```go
grouped := df.GroupBy("category")
result := grouped.Sum("value")
```

See [GroupBy Guide](../02-guides/02-groupby.md) for detailed documentation.

## Join Operations

### Join

Performs an inner join with another DataFrame.

```go
func (df *DataFrame) Join(other *DataFrame, opts ...JoinOption) (*DataFrame, error)
```

**Example:**
```go
result, _ := left.Join(right, galleon.On("id"))
```

### LeftJoin

Performs a left join with another DataFrame.

```go
func (df *DataFrame) LeftJoin(other *DataFrame, opts ...JoinOption) (*DataFrame, error)
```

### RightJoin

Performs a right join with another DataFrame.

```go
func (df *DataFrame) RightJoin(other *DataFrame, opts ...JoinOption) (*DataFrame, error)
```

### Join Options

```go
// Same column name in both tables
On(columns ...string) JoinOption

// Different column names
LeftOn(columns ...string) JoinOption
RightOn(columns ...string) JoinOption

// Suffix for duplicate columns
Suffix(suffix string) JoinOption
```

See [Join Operations](../02-guides/03-joins.md) for detailed documentation.

## Lazy Evaluation

### Lazy

Converts to a LazyFrame for deferred execution.

```go
func (df *DataFrame) Lazy() *LazyFrame
```

**Example:**
```go
result, _ := df.Lazy().
    Filter(Col("value").Gt(Lit(100))).
    Select(Col("id"), Col("value")).
    Collect()
```

## I/O Operations

### WriteCSV

Writes DataFrame to CSV file.

```go
func (df *DataFrame) WriteCSV(path string) error
func (df *DataFrame) WriteCSVWithOptions(path string, opts CSVWriteOptions) error
```

### WriteJSON

Writes DataFrame to JSON file.

```go
func (df *DataFrame) WriteJSON(path string) error
```

### WriteJSONL

Writes DataFrame to JSON Lines file.

```go
func (df *DataFrame) WriteJSONL(path string) error
```

## Utility Methods

### Clone

Creates a deep copy of the DataFrame.

```go
func (df *DataFrame) Clone() *DataFrame
```

### String

Returns a string representation.

```go
func (df *DataFrame) String() string
```

### Describe

Returns statistical summary.

```go
func (df *DataFrame) Describe() *DataFrame
```

## Complete Example

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Create DataFrame
    df, _ := galleon.NewDataFrame(
        galleon.NewSeriesInt64("id", []int64{1, 2, 3, 4, 5}),
        galleon.NewSeriesString("category", []string{"A", "B", "A", "B", "A"}),
        galleon.NewSeriesFloat64("value", []float64{10, 20, 30, 40, 50}),
    )

    // Chain operations
    result := df.
        Filter(galleon.Col("value").Gt(galleon.Lit(15.0))).
        GroupBy("category").
        Agg(
            galleon.Col("value").Sum().Alias("total"),
            galleon.Col("value").Mean().Alias("average"),
        ).
        Sort("total", false)

    fmt.Printf("Result: %d rows\n", result.Height())
}
```
