# Series API Reference

The `Series` represents a single column of data with a name and data type.

## Creating Series

### NewSeriesInt64

Creates an Int64 series.

```go
func NewSeriesInt64(name string, data []int64) *Series
```

### NewSeriesFloat64

Creates a Float64 series.

```go
func NewSeriesFloat64(name string, data []float64) *Series
```

### NewSeriesString

Creates a String series.

```go
func NewSeriesString(name string, data []string) *Series
```

### NewSeriesInt32

Creates an Int32 series.

```go
func NewSeriesInt32(name string, data []int32) *Series
```

### NewSeriesFloat32

Creates a Float32 series.

```go
func NewSeriesFloat32(name string, data []float32) *Series
```

### NewSeriesBool

Creates a Bool series.

```go
func NewSeriesBool(name string, data []bool) *Series
```

### NewSeries (Generic)

Creates a series with automatic type inference.

```go
func NewSeries(name string, data interface{}) (*Series, error)
```

**Supported types:**
- `[]float64` → Float64 series
- `[]float32` → Float32 series
- `[]int64` → Int64 series
- `[]int32` → Int32 series
- `[]int` → Int64 series (converted)
- `[]bool` → Bool series
- `[]string` → String series

**Example:**
```go
// Type inferred automatically
s1, _ := galleon.NewSeries("values", []float64{1.0, 2.0, 3.0})
s2, _ := galleon.NewSeries("names", []string{"Alice", "Bob"})
s3, _ := galleon.NewSeries("flags", []bool{true, false, true})
```

## Properties

### Name

Returns the series name.

```go
func (s *Series) Name() string
```

### Len

Returns the number of elements.

```go
func (s *Series) Len() int
```

### DType

Returns the data type.

```go
func (s *Series) DType() DType
```

**DType constants:**
```go
const (
    Float64 DType = iota
    Float32
    Int64
    Int32
    UInt64
    UInt32
    Bool
    String
    DateTime
    Duration
    Null
)
```

## Data Access

### Int64

Returns data as []int64.

```go
func (s *Series) Int64() []int64
```

### Float64

Returns data as []float64.

```go
func (s *Series) Float64() []float64
```

### Strings

Returns data as []string.

```go
func (s *Series) Strings() []string
```

### Int32

Returns data as []int32.

```go
func (s *Series) Int32() []int32
```

### Float32

Returns data as []float32.

```go
func (s *Series) Float32() []float32
```

### Bools

Returns data as []bool.

```go
func (s *Series) Bools() []bool
```

## Aggregations

All aggregation methods use SIMD acceleration when available.

### Sum

Returns the sum of all values.

```go
func (s *Series) Sum() float64
```

### Min

Returns the minimum value.

```go
func (s *Series) Min() float64
```

### Max

Returns the maximum value.

```go
func (s *Series) Max() float64
```

### Mean

Returns the arithmetic mean.

```go
func (s *Series) Mean() float64
```

### Std

Returns the standard deviation.

```go
func (s *Series) Std() float64
```

### Var

Returns the variance.

```go
func (s *Series) Var() float64
```

### Count

Returns the number of non-null values.

```go
func (s *Series) Count() int
```

## Element-wise Operations

### Add

Adds a scalar to all elements.

```go
func (s *Series) Add(value float64) *Series
```

### Sub

Subtracts a scalar from all elements.

```go
func (s *Series) Sub(value float64) *Series
```

### Mul

Multiplies all elements by a scalar.

```go
func (s *Series) Mul(value float64) *Series
```

### Div

Divides all elements by a scalar.

```go
func (s *Series) Div(value float64) *Series
```

### AddSeries

Element-wise addition with another series.

```go
func (s *Series) AddSeries(other *Series) *Series
```

### SubSeries

Element-wise subtraction with another series.

```go
func (s *Series) SubSeries(other *Series) *Series
```

### MulSeries

Element-wise multiplication with another series.

```go
func (s *Series) MulSeries(other *Series) *Series
```

### DivSeries

Element-wise division with another series.

```go
func (s *Series) DivSeries(other *Series) *Series
```

## Comparison Operations

### Gt

Returns boolean mask where values > threshold.

```go
func (s *Series) Gt(value float64) []bool
```

### Lt

Returns boolean mask where values < threshold.

```go
func (s *Series) Lt(value float64) []bool
```

### Eq

Returns boolean mask where values == value.

```go
func (s *Series) Eq(value interface{}) []bool
```

### Gte

Returns boolean mask where values >= threshold.

```go
func (s *Series) Gte(value float64) []bool
```

### Lte

Returns boolean mask where values <= threshold.

```go
func (s *Series) Lte(value float64) []bool
```

### Neq

Returns boolean mask where values != value.

```go
func (s *Series) Neq(value interface{}) []bool
```

### EqString

String equality comparison (for String series).

```go
func (s *Series) EqString(value string) []bool
```

**Example:**
```go
names := galleon.NewSeriesString("names", []string{"Alice", "Bob", "Alice"})
mask := names.EqString("Alice")  // [true, false, true]
```

### NeqString

String inequality comparison (for String series).

```go
func (s *Series) NeqString(value string) []bool
```

**Example:**
```go
status := galleon.NewSeriesString("status", []string{"active", "inactive", "active"})
mask := status.NeqString("active")  // [false, true, false]
```

## Sorting

### Sort

Returns a sorted copy.

```go
func (s *Series) Sort(ascending bool) *Series
```

### Argsort

Returns indices that would sort the series.

```go
func (s *Series) Argsort(ascending bool) []uint32
```

## Utility Methods

### Clone

Creates a deep copy.

```go
func (s *Series) Clone() *Series
```

### Rename

Returns a copy with a new name.

```go
func (s *Series) Rename(name string) *Series
```

### Head

Returns the first n elements.

```go
func (s *Series) Head(n int) *Series
```

### Tail

Returns the last n elements.

```go
func (s *Series) Tail(n int) *Series
```

### Slice

Returns elements from start to end.

```go
func (s *Series) Slice(start, end int) *Series
```

### Unique

Returns unique values.

```go
func (s *Series) Unique() *Series
```

### NUnique

Returns the number of unique values.

```go
func (s *Series) NUnique() int
```

### IsEmpty

Checks if series has zero elements.

```go
func (s *Series) IsEmpty() bool
```

**Example:**
```go
empty := galleon.NewSeriesInt64("empty", []int64{})
fmt.Println(empty.IsEmpty())  // true
```

### Get

Returns value at index as `interface{}`.

```go
func (s *Series) Get(index int) interface{}
```

**Example:**
```go
s := galleon.NewSeriesFloat64("values", []float64{1.5, 2.5, 3.5})
value := s.Get(1)  // 2.5 (as interface{})
```

### GetFloat64

Type-safe getter for Float64 series.

```go
func (s *Series) GetFloat64(index int) float64
```

### GetInt64

Type-safe getter for Int64 series.

```go
func (s *Series) GetInt64(index int) int64
```

### GetString

Type-safe getter for String series.

```go
func (s *Series) GetString(index int) string
```

**Example:**
```go
names := galleon.NewSeriesString("names", []string{"Alice", "Bob", "Carol"})
name := names.GetString(1)  // "Bob"
```

### CountTrue

For Bool series, counts the number of true values.

```go
func (s *Series) CountTrue() int
```

**Example:**
```go
flags := galleon.NewSeriesBool("active", []bool{true, false, true, true})
count := flags.CountTrue()  // 3
```

### Describe

Returns descriptive statistics for the series.

```go
func (s *Series) Describe() map[string]float64
```

Returns a map with keys:
- `"count"` - Number of non-null values
- `"mean"` - Arithmetic mean
- `"std"` - Standard deviation
- `"min"` - Minimum value
- `"25%"` - 25th percentile
- `"50%"` - Median (50th percentile)
- `"75%"` - 75th percentile
- `"max"` - Maximum value

**Example:**
```go
values := galleon.NewSeriesFloat64("values", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
stats := values.Describe()
fmt.Printf("Mean: %.2f, Median: %.2f, Std: %.2f\n",
    stats["mean"], stats["50%"], stats["std"])
```

## Type Casting

### AsFloat64

Converts to Float64 series.

```go
func (s *Series) AsFloat64() *Series
```

### AsInt64

Converts to Int64 series.

```go
func (s *Series) AsInt64() *Series
```

### AsString

Converts to String series.

```go
func (s *Series) AsString() *Series
```

## Low-Level SIMD Functions

These functions operate directly on slices for maximum performance.

### SumF64

```go
func SumF64(data []float64) float64
```

### MinF64

```go
func MinF64(data []float64) float64
```

### MaxF64

```go
func MaxF64(data []float64) float64
```

### MeanF64

```go
func MeanF64(data []float64) float64
```

### AddScalarF64

```go
func AddScalarF64(data []float64, scalar float64)
```

### MulScalarF64

```go
func MulScalarF64(data []float64, scalar float64)
```

### FilterMaskGreaterThanF64

```go
func FilterMaskGreaterThanF64(data []float64, threshold float64) []bool
```

### ArgsortF64

```go
func ArgsortF64(data []float64, ascending bool) []uint32
```

## Complete Example

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Create series
    values := galleon.NewSeriesFloat64("values", []float64{
        10.5, 20.3, 15.7, 8.2, 25.1, 12.8, 19.4,
    })

    // Aggregations
    fmt.Printf("Sum:  %.2f\n", values.Sum())
    fmt.Printf("Min:  %.2f\n", values.Min())
    fmt.Printf("Max:  %.2f\n", values.Max())
    fmt.Printf("Mean: %.2f\n", values.Mean())

    // Element-wise operations
    doubled := values.Mul(2.0)
    fmt.Printf("Doubled: %v\n", doubled.Float64())

    // Comparison
    mask := values.Gt(15.0)
    fmt.Printf("Values > 15: %v\n", mask)

    // Sorting
    sorted := values.Sort(true)
    fmt.Printf("Sorted: %v\n", sorted.Float64())

    // Direct SIMD access
    data := values.Float64()
    sum := galleon.SumF64(data)
    fmt.Printf("Direct SIMD sum: %.2f\n", sum)
}
```
