package galleon

import (
	"fmt"
	"reflect"
	"sort"
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

// FromRecords creates a DataFrame from a slice of maps.
// Each map represents a row, with keys as column names.
// All maps should have the same keys for best results.
// Type inference: int/int64 -> Int64, float64 -> Float64, string -> String, bool -> Bool
func FromRecords(records []map[string]interface{}) (*DataFrame, error) {
	if len(records) == 0 {
		return NewDataFrame()
	}

	// Collect all unique column names from all records
	colSet := make(map[string]bool)
	for _, record := range records {
		for key := range record {
			colSet[key] = true
		}
	}

	// Sort column names for deterministic ordering
	colNames := make([]string, 0, len(colSet))
	for name := range colSet {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	// Infer types from first non-nil value in each column
	colTypes := make(map[string]DType)
	for _, name := range colNames {
		for _, record := range records {
			if val, ok := record[name]; ok && val != nil {
				colTypes[name] = inferTypeFromValue(val)
				break
			}
		}
		// Default to String if all nil
		if _, ok := colTypes[name]; !ok {
			colTypes[name] = String
		}
	}

	// Build columns
	numRows := len(records)
	columns := make([]*Series, len(colNames))

	for i, name := range colNames {
		dtype := colTypes[name]
		switch dtype {
		case Float64:
			data := make([]float64, numRows)
			for j, record := range records {
				if val, ok := record[name]; ok && val != nil {
					data[j] = convertToFloat64(val)
				}
			}
			columns[i] = NewSeriesFloat64(name, data)
		case Int64:
			data := make([]int64, numRows)
			for j, record := range records {
				if val, ok := record[name]; ok && val != nil {
					data[j] = convertToInt64(val)
				}
			}
			columns[i] = NewSeriesInt64(name, data)
		case Bool:
			data := make([]bool, numRows)
			for j, record := range records {
				if val, ok := record[name]; ok && val != nil {
					data[j] = convertToBool(val)
				}
			}
			columns[i] = NewSeriesBool(name, data)
		default: // String
			data := make([]string, numRows)
			for j, record := range records {
				if val, ok := record[name]; ok && val != nil {
					data[j] = convertToString(val)
				}
			}
			columns[i] = NewSeriesString(name, data)
		}
	}

	return NewDataFrame(columns...)
}

// FromStructs creates a DataFrame from a slice of structs.
// Uses reflection to extract field names and values.
// Exported fields become columns, field names become column names.
// Supports struct tags: `galleon:"column_name"` to override column names.
func FromStructs(structs interface{}) (*DataFrame, error) {
	v := reflect.ValueOf(structs)
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("FromStructs requires a slice, got %s", v.Kind())
	}

	if v.Len() == 0 {
		return NewDataFrame()
	}

	// Get the element type
	elemType := v.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("FromStructs requires a slice of structs, got slice of %s", elemType.Kind())
	}

	// Extract field info
	type fieldInfo struct {
		name    string // column name
		index   int    // struct field index
		kind    reflect.Kind
		dtype   DType
	}

	var fields []fieldInfo
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		if !field.IsExported() {
			continue
		}

		// Get column name from tag or field name
		colName := field.Name
		if tag := field.Tag.Get("galleon"); tag != "" {
			if tag == "-" {
				continue // Skip this field
			}
			colName = tag
		}

		dtype := reflectKindToDType(field.Type.Kind())
		fields = append(fields, fieldInfo{
			name:  colName,
			index: i,
			kind:  field.Type.Kind(),
			dtype: dtype,
		})
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no exported fields found in struct")
	}

	// Build columns
	numRows := v.Len()
	columns := make([]*Series, len(fields))

	for i, f := range fields {
		switch f.dtype {
		case Float64:
			data := make([]float64, numRows)
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = elem.Field(f.index).Float()
			}
			columns[i] = NewSeriesFloat64(f.name, data)
		case Float32:
			data := make([]float32, numRows)
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = float32(elem.Field(f.index).Float())
			}
			columns[i] = NewSeriesFloat32(f.name, data)
		case Int64:
			data := make([]int64, numRows)
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = elem.Field(f.index).Int()
			}
			columns[i] = NewSeriesInt64(f.name, data)
		case Int32:
			data := make([]int32, numRows)
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = int32(elem.Field(f.index).Int())
			}
			columns[i] = NewSeriesInt32(f.name, data)
		case UInt64:
			data := make([]int64, numRows) // Store as int64
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = int64(elem.Field(f.index).Uint())
			}
			columns[i] = NewSeriesInt64(f.name, data)
		case UInt32:
			data := make([]int32, numRows) // Store as int32
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = int32(elem.Field(f.index).Uint())
			}
			columns[i] = NewSeriesInt32(f.name, data)
		case Bool:
			data := make([]bool, numRows)
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = elem.Field(f.index).Bool()
			}
			columns[i] = NewSeriesBool(f.name, data)
		default: // String
			data := make([]string, numRows)
			for j := 0; j < numRows; j++ {
				elem := v.Index(j)
				if elem.Kind() == reflect.Ptr {
					elem = elem.Elem()
				}
				data[j] = elem.Field(f.index).String()
			}
			columns[i] = NewSeriesString(f.name, data)
		}
	}

	return NewDataFrame(columns...)
}

// Helper functions for type inference and conversion (FromRecords/FromStructs)

func inferTypeFromValue(val interface{}) DType {
	switch val.(type) {
	case float64, float32:
		return Float64
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return Int64
	case bool:
		return Bool
	default:
		return String
	}
}

func convertToFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	default:
		return 0
	}
}

func convertToInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func convertToBool(val interface{}) bool {
	if b, ok := val.(bool); ok {
		return b
	}
	return false
}

func convertToString(val interface{}) string {
	if s, ok := val.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", val)
}

func reflectKindToDType(k reflect.Kind) DType {
	switch k {
	case reflect.Float64:
		return Float64
	case reflect.Float32:
		return Float32
	case reflect.Int64, reflect.Int:
		return Int64
	case reflect.Int32, reflect.Int16, reflect.Int8:
		return Int32
	case reflect.Uint64, reflect.Uint:
		return UInt64
	case reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return UInt32
	case reflect.Bool:
		return Bool
	default:
		return String
	}
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

// Select returns a new DataFrame with columns computed from expressions.
// This is the unified API matching LazyFrame.Select.
//
// Example:
//
//	df.Select(Col("a"), Col("b").Mul(Lit(2)).Alias("double_b"))
func (df *DataFrame) Select(exprs ...Expr) (*DataFrame, error) {
	if len(exprs) == 0 {
		return NewDataFrame()
	}

	columns := make([]*Series, 0, len(exprs))
	for _, expr := range exprs {
		// Handle * (all columns) specially
		if _, ok := expr.(*allColsExpr); ok {
			for _, col := range df.columns {
				columns = append(columns, col)
			}
			continue
		}

		col, err := evaluateExpr(expr, df)
		if err != nil {
			return nil, fmt.Errorf("select error: %w", err)
		}
		columns = append(columns, col)
	}

	return NewDataFrame(columns...)
}

// SelectColumns returns a new DataFrame with only the specified columns by name.
// This is a convenience method; prefer Select(Col("a"), Col("b")) for consistency.
//
// Deprecated: Use Select(Col("a"), Col("b")) instead.
func (df *DataFrame) SelectColumns(names ...string) (*DataFrame, error) {
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

// Filter returns a new DataFrame with only rows where the predicate is true.
// This is the unified API matching LazyFrame.Filter.
//
// Example:
//
//	df.Filter(Col("age").Gt(Lit(30)))
//	df.Filter(Col("active").Eq(Lit(true)))
func (df *DataFrame) Filter(predicate Expr) (*DataFrame, error) {
	mask, err := evaluatePredicate(predicate, df)
	if err != nil {
		return nil, fmt.Errorf("filter error: %w", err)
	}
	return df.FilterByMask(mask)
}

// FilterByMask returns a new DataFrame with only rows where mask is true.
// This is a lower-level method; prefer Filter(predicate) for consistency.
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

// WithColumn returns a new DataFrame with an additional or replaced column.
// The expression is evaluated and the result is assigned to the given column name.
// This is the unified API matching LazyFrame.WithColumn.
//
// Example:
//
//	df.WithColumn("double_x", Col("x").Mul(Lit(2)))
func (df *DataFrame) WithColumn(name string, expr Expr) (*DataFrame, error) {
	// Evaluate the expression
	col, err := evaluateExpr(expr, df)
	if err != nil {
		return nil, fmt.Errorf("with_column error: %w", err)
	}

	// Rename to the target name
	col = col.Rename(name)

	// Use the series-based implementation
	return df.WithColumnSeries(col)
}

// WithColumnSeries returns a new DataFrame with an additional or replaced column.
// This is a lower-level method that takes a pre-built Series.
//
// Deprecated: Use WithColumn(name, expr) instead for consistency with LazyFrame.
func (df *DataFrame) WithColumnSeries(col *Series) (*DataFrame, error) {
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
