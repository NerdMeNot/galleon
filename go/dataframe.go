package galleon

import (
	"fmt"
	"strings"
	"sync"
)

// DataFrame represents a collection of named columns (Series)
type DataFrame struct {
	columns []*Series
	schema  *Schema
	height  int // number of rows
}

// NewDataFrame creates a new DataFrame from a slice of Series
func NewDataFrame(columns ...*Series) (*DataFrame, error) {
	if len(columns) == 0 {
		return &DataFrame{
			columns: nil,
			schema:  &Schema{names: []string{}, dtypes: []DType{}},
			height:  0,
		}, nil
	}

	// Validate all columns have the same length
	height := columns[0].Len()
	names := make([]string, len(columns))
	dtypes := make([]DType, len(columns))

	for i, col := range columns {
		if col.Len() != height {
			return nil, fmt.Errorf("column '%s' has length %d, expected %d", col.Name(), col.Len(), height)
		}
		names[i] = col.Name()
		dtypes[i] = col.DType()
	}

	schema, err := NewSchema(names, dtypes)
	if err != nil {
		return nil, err
	}

	return &DataFrame{
		columns: columns,
		schema:  schema,
		height:  height,
	}, nil
}

// Height returns the number of rows
func (df *DataFrame) Height() int {
	return df.height
}

// Width returns the number of columns
func (df *DataFrame) Width() int {
	return len(df.columns)
}

// Shape returns (height, width)
func (df *DataFrame) Shape() (int, int) {
	return df.height, len(df.columns)
}

// Schema returns the DataFrame schema
func (df *DataFrame) Schema() *Schema {
	return df.schema
}

// Columns returns a copy of the column names
func (df *DataFrame) Columns() []string {
	return df.schema.Names()
}

// Column returns the Series at the given index
func (df *DataFrame) Column(index int) *Series {
	if index < 0 || index >= len(df.columns) {
		return nil
	}
	return df.columns[index]
}

// ColumnByName returns the Series with the given name
func (df *DataFrame) ColumnByName(name string) *Series {
	idx, ok := df.schema.GetIndex(name)
	if !ok {
		return nil
	}
	return df.columns[idx]
}

// Select returns a new DataFrame with only the specified columns
func (df *DataFrame) Select(names ...string) (*DataFrame, error) {
	cols := make([]*Series, 0, len(names))
	for _, name := range names {
		col := df.ColumnByName(name)
		if col == nil {
			return nil, fmt.Errorf("column '%s' not found", name)
		}
		cols = append(cols, col)
	}
	return NewDataFrame(cols...)
}

// Drop returns a new DataFrame without the specified columns
func (df *DataFrame) Drop(names ...string) (*DataFrame, error) {
	dropSet := make(map[string]bool, len(names))
	for _, name := range names {
		dropSet[name] = true
	}

	cols := make([]*Series, 0, len(df.columns))
	for _, col := range df.columns {
		if !dropSet[col.Name()] {
			cols = append(cols, col)
		}
	}
	return NewDataFrame(cols...)
}

// Head returns a new DataFrame with the first n rows
func (df *DataFrame) Head(n int) *DataFrame {
	if n <= 0 || df.height == 0 {
		return &DataFrame{
			columns: nil,
			schema:  df.schema,
			height:  0,
		}
	}
	if n > df.height {
		n = df.height
	}

	// Build columns in parallel for large DataFrames
	cols := ParallelBuildColumns(len(df.columns), func(colIdx int) *Series {
		return df.columns[colIdx].Head(n)
	})

	result, _ := NewDataFrame(cols...)
	return result
}

// Tail returns a new DataFrame with the last n rows
func (df *DataFrame) Tail(n int) *DataFrame {
	if n <= 0 || df.height == 0 {
		return &DataFrame{
			columns: nil,
			schema:  df.schema,
			height:  0,
		}
	}
	if n > df.height {
		n = df.height
	}

	// Build columns in parallel for large DataFrames
	cols := ParallelBuildColumns(len(df.columns), func(colIdx int) *Series {
		return df.columns[colIdx].Tail(n)
	})

	result, _ := NewDataFrame(cols...)
	return result
}

// FilterByMask returns a new DataFrame with only rows where mask is true
func (df *DataFrame) FilterByMask(mask []byte) (*DataFrame, error) {
	if len(mask) != df.height {
		return nil, fmt.Errorf("mask length %d doesn't match DataFrame height %d", len(mask), df.height)
	}

	// Count matching rows using Zig
	count := CountMaskTrue(mask)

	if count == 0 {
		return NewDataFrame()
	}

	// Build indices array using Zig
	indicesU32 := make([]uint32, count)
	actualCount := IndicesFromMask(mask, indicesU32)
	if actualCount != count {
		indicesU32 = indicesU32[:actualCount]
	}

	// Convert to int indices for compatibility
	indices := make([]int, len(indicesU32))
	for i, idx := range indicesU32 {
		indices[i] = int(idx)
	}

	// Build columns in parallel
	cols := ParallelBuildColumns(len(df.columns), func(colIdx int) *Series {
		col := df.columns[colIdx]
		return filterColumnByIndices(col, indices)
	})

	return NewDataFrame(cols...)
}

// filterColumnByIndices filters a column by row indices
func filterColumnByIndices(col *Series, indices []int) *Series {
	count := len(indices)
	switch col.DType() {
	case Float64:
		data := col.Float64()
		newData := make([]float64, count)
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return NewSeriesFloat64(col.Name(), newData)
	case Float32:
		data := col.Float32()
		newData := make([]float32, count)
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return NewSeriesFloat32(col.Name(), newData)
	case Int64:
		data := col.Int64()
		newData := make([]int64, count)
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return NewSeriesInt64(col.Name(), newData)
	case Int32:
		data := col.Int32()
		newData := make([]int32, count)
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return NewSeriesInt32(col.Name(), newData)
	case Bool:
		data := col.Bool()
		newData := make([]bool, count)
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return NewSeriesBool(col.Name(), newData)
	case String:
		data := col.Strings()
		newData := make([]string, count)
		for i, idx := range indices {
			newData[i] = data[idx]
		}
		return NewSeriesString(col.Name(), newData)
	default:
		return nil
	}
}

// FilterByIndices returns a new DataFrame with only the specified row indices
func (df *DataFrame) FilterByIndices(indices []uint32) (*DataFrame, error) {
	if len(indices) == 0 {
		return NewDataFrame()
	}

	// Convert to int indices
	intIndices := make([]int, len(indices))
	for i, idx := range indices {
		intIndices[i] = int(idx)
	}

	// Validate indices
	for _, idx := range intIndices {
		if idx >= df.height {
			return nil, fmt.Errorf("index %d out of bounds for DataFrame with height %d", idx, df.height)
		}
	}

	// Build columns in parallel
	cols := ParallelBuildColumns(len(df.columns), func(colIdx int) *Series {
		return filterColumnByIndices(df.columns[colIdx], intIndices)
	})

	return NewDataFrame(cols...)
}

// SortBy returns a new DataFrame sorted by the specified column
func (df *DataFrame) SortBy(column string, ascending bool) (*DataFrame, error) {
	col := df.ColumnByName(column)
	if col == nil {
		return nil, fmt.Errorf("column '%s' not found", column)
	}

	indices := col.Argsort(ascending)
	if indices == nil {
		return nil, fmt.Errorf("cannot sort column '%s'", column)
	}

	return df.FilterByIndices(indices)
}

// WithColumn returns a new DataFrame with an additional or replaced column
func (df *DataFrame) WithColumn(col *Series) (*DataFrame, error) {
	if col.Len() != df.height && df.height > 0 {
		return nil, fmt.Errorf("column '%s' has length %d, expected %d", col.Name(), col.Len(), df.height)
	}

	// Check if column already exists
	idx, exists := df.schema.GetIndex(col.Name())

	cols := make([]*Series, len(df.columns))
	copy(cols, df.columns)

	if exists {
		cols[idx] = col
	} else {
		cols = append(cols, col)
	}

	return NewDataFrame(cols...)
}

// Rename returns a new DataFrame with a column renamed
func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	idx, ok := df.schema.GetIndex(oldName)
	if !ok {
		return nil, fmt.Errorf("column '%s' not found", oldName)
	}

	cols := make([]*Series, len(df.columns))
	for i, col := range df.columns {
		if i == idx {
			cols[i] = col.Rename(newName)
		} else {
			cols[i] = col
		}
	}

	return NewDataFrame(cols...)
}

// Describe returns summary statistics for all numeric columns
func (df *DataFrame) Describe() map[string]map[string]float64 {
	// Find numeric columns
	var numericCols []*Series
	for _, col := range df.columns {
		if col.DType().IsNumeric() {
			numericCols = append(numericCols, col)
		}
	}

	if len(numericCols) == 0 {
		return nil
	}

	// Compute stats in parallel
	cfg := globalConfig
	result := make(map[string]map[string]float64, len(numericCols))
	var mu sync.Mutex

	if cfg.shouldParallelize(df.height) && len(numericCols) > 1 {
		var wg sync.WaitGroup
		for _, col := range numericCols {
			wg.Add(1)
			go func(c *Series) {
				defer wg.Done()
				stats := c.Describe()
				mu.Lock()
				result[c.Name()] = stats
				mu.Unlock()
			}(col)
		}
		wg.Wait()
	} else {
		for _, col := range numericCols {
			result[col.Name()] = col.Describe()
		}
	}

	return result
}

// String returns a string representation of the DataFrame
func (df *DataFrame) String() string {
	if df.height == 0 || len(df.columns) == 0 {
		return "DataFrame(empty)"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("DataFrame: %d rows × %d columns\n", df.height, len(df.columns)))

	// Column headers
	sb.WriteString("┌")
	for i := range df.columns {
		if i > 0 {
			sb.WriteString("┬")
		}
		sb.WriteString("──────────────")
	}
	sb.WriteString("┐\n│")

	for i, col := range df.columns {
		if i > 0 {
			sb.WriteString("│")
		}
		name := col.Name()
		if len(name) > 12 {
			name = name[:12]
		}
		sb.WriteString(fmt.Sprintf(" %-12s ", name))
	}
	sb.WriteString("│\n│")

	// Data types
	for i, col := range df.columns {
		if i > 0 {
			sb.WriteString("│")
		}
		dtype := col.DType().String()
		if len(dtype) > 12 {
			dtype = dtype[:12]
		}
		sb.WriteString(fmt.Sprintf(" %-12s ", dtype))
	}
	sb.WriteString("│\n├")

	for i := range df.columns {
		if i > 0 {
			sb.WriteString("┼")
		}
		sb.WriteString("──────────────")
	}
	sb.WriteString("┤\n")

	// Show first few rows
	maxRows := 5
	showRows := df.height
	if showRows > maxRows {
		showRows = maxRows
	}

	for row := 0; row < showRows; row++ {
		sb.WriteString("│")
		for i, col := range df.columns {
			if i > 0 {
				sb.WriteString("│")
			}
			val := col.Get(row)
			var valStr string
			switch v := val.(type) {
			case float64:
				valStr = fmt.Sprintf("%.4f", v)
			default:
				valStr = fmt.Sprintf("%v", v)
			}
			if len(valStr) > 12 {
				valStr = valStr[:12]
			}
			sb.WriteString(fmt.Sprintf(" %12s ", valStr))
		}
		sb.WriteString("│\n")
	}

	if df.height > maxRows {
		sb.WriteString("│")
		for i := range df.columns {
			if i > 0 {
				sb.WriteString("│")
			}
			sb.WriteString("      ...     ")
		}
		sb.WriteString("│\n")
	}

	sb.WriteString("└")
	for i := range df.columns {
		if i > 0 {
			sb.WriteString("┴")
		}
		sb.WriteString("──────────────")
	}
	sb.WriteString("┘")

	return sb.String()
}
