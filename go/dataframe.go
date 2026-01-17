package galleon

/*
#include "galleon.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// DataFrame represents a DataFrame backed by Arrow columns.
// All columns are Series stored in Zig memory with SIMD operations.
type DataFrame struct {
	columns  map[string]*Series
	colOrder []string // Preserve insertion order
}

// ============================================================================
// Creation
// ============================================================================

// NewDataFrame creates an empty DataFrame.
func NewDataFrame() *DataFrame {
	return &DataFrame{
		columns:  make(map[string]*Series),
		colOrder: make([]string, 0),
	}
}

// AddColumn adds a column to the DataFrame.
// If a column with the same name exists, it is replaced.
// Returns the DataFrame for method chaining.
func (df *DataFrame) AddColumn(series *Series) *DataFrame {
	if series == nil {
		return df
	}

	name := series.Name()

	// Check if column already exists
	if _, exists := df.columns[name]; !exists {
		df.colOrder = append(df.colOrder, name)
	}

	df.columns[name] = series
	return df
}

// FromColumns creates an DataFrame from multiple Series.
// All series must have the same length.
func FromColumns(series ...*Series) *DataFrame {
	df := NewDataFrame()

	if len(series) == 0 {
		return df
	}

	// Verify all series have the same length
	expectedLen := series[0].Len()
	for _, s := range series {
		if s == nil {
			continue
		}
		if s.Len() != expectedLen {
			return nil // Length mismatch
		}
	}

	for _, s := range series {
		if s != nil {
			df.AddColumn(s)
		}
	}

	return df
}

// FromMapF64 creates an DataFrame from a map of float64 slices.
// All slices must have the same length.
func FromMapF64(data map[string][]float64) *DataFrame {
	df := NewDataFrame()

	if len(data) == 0 {
		return df
	}

	// Verify all slices have the same length
	var expectedLen int
	first := true
	for _, values := range data {
		if first {
			expectedLen = len(values)
			first = false
		} else if len(values) != expectedLen {
			return nil // Length mismatch
		}
	}

	for name, values := range data {
		series := NewSeriesF64(name, values)
		df.AddColumn(series)
	}

	return df
}

// FromMapI64 creates an DataFrame from a map of int64 slices.
// All slices must have the same length.
func FromMapI64(data map[string][]int64) *DataFrame {
	df := NewDataFrame()

	if len(data) == 0 {
		return df
	}

	// Verify all slices have the same length
	var expectedLen int
	first := true
	for _, values := range data {
		if first {
			expectedLen = len(values)
			first = false
		} else if len(values) != expectedLen {
			return nil // Length mismatch
		}
	}

	for name, values := range data {
		series := NewSeriesI64(name, values)
		df.AddColumn(series)
	}

	return df
}

// ============================================================================
// Access
// ============================================================================

// Column returns the Series with the given name, or nil if not found.
func (df *DataFrame) Column(name string) *Series {
	return df.columns[name]
}

// ColumnNames returns the names of all columns in insertion order.
func (df *DataFrame) ColumnNames() []string {
	result := make([]string, len(df.colOrder))
	copy(result, df.colOrder)
	return result
}

// Height returns the number of rows in the DataFrame.
// Returns 0 for an empty DataFrame.
func (df *DataFrame) Height() int {
	if len(df.columns) == 0 {
		return 0
	}
	// All columns have the same length, so just return the first one's length
	for _, col := range df.columns {
		return col.Len()
	}
	return 0
}

// Width returns the number of columns in the DataFrame.
func (df *DataFrame) Width() int {
	return len(df.columns)
}

// ============================================================================
// Selection
// ============================================================================

// Select returns a new DataFrame with only the specified columns.
// Columns that don't exist are silently ignored.
func (df *DataFrame) Select(columns ...string) *DataFrame {
	result := NewDataFrame()

	for _, name := range columns {
		if series, exists := df.columns[name]; exists {
			result.columns[name] = series
			result.colOrder = append(result.colOrder, name)
		}
	}

	return result
}

// Drop returns a new DataFrame without the specified columns.
func (df *DataFrame) Drop(columns ...string) *DataFrame {
	// Create a set of columns to drop
	dropSet := make(map[string]bool)
	for _, name := range columns {
		dropSet[name] = true
	}

	result := NewDataFrame()

	for _, name := range df.colOrder {
		if !dropSet[name] {
			result.columns[name] = df.columns[name]
			result.colOrder = append(result.colOrder, name)
		}
	}

	return result
}

// ============================================================================
// Operations
// ============================================================================

// Filter returns a new DataFrame with rows where mask is true.
// The mask length must match the DataFrame height.
func (df *DataFrame) Filter(mask []bool) *DataFrame {
	if len(mask) != df.Height() {
		return nil
	}

	result := NewDataFrame()

	for _, name := range df.colOrder {
		col := df.columns[name]
		filtered := col.Filter(mask)
		if filtered == nil {
			return nil
		}
		result.columns[name] = filtered
		result.colOrder = append(result.colOrder, name)
	}

	return result
}

// Sort returns a new DataFrame sorted by the specified column.
// Uses a single CGO call to sort + gather all columns in Zig for maximum efficiency.
func (df *DataFrame) Sort(column string, ascending bool) *DataFrame {
	sortCol := df.columns[column]
	if sortCol == nil {
		return nil
	}

	// Handle empty DataFrame
	if sortCol.Len() == 0 {
		return df.Clone()
	}

	// Collect all column handles for the single CGO call
	cols := make([]unsafe.Pointer, len(df.colOrder))
	for i, name := range df.colOrder {
		cols[i] = df.columns[name].handle
	}

	// Single CGO call: argsort + gather all columns
	sortResult := C.galleon_sort_dataframe_full(
		(*C.ManagedArrowArray)(sortCol.handle),
		(**C.ManagedArrowArray)(unsafe.Pointer(&cols[0])),
		C.size_t(len(cols)),
		C.bool(ascending),
	)

	if sortResult == nil {
		return nil
	}
	defer C.galleon_sort_result_destroy_struct(sortResult)

	// Build result DataFrame from sorted columns
	result := NewDataFrame()
	numRows := int(C.galleon_sort_result_num_rows(sortResult))

	for i, name := range df.colOrder {
		colHandle := C.galleon_sort_result_take_column(sortResult, C.size_t(i))
		if colHandle == nil {
			return nil
		}

		series := &Series{
			handle: unsafe.Pointer(colHandle),
			name:   name,
			dtype:  df.columns[name].dtype,
			length: numRows,
		}
		runtime.SetFinalizer(series, func(s *Series) {
			s.Release()
		})

		result.columns[name] = series
		result.colOrder = append(result.colOrder, name)
	}

	return result
}

// Rename returns a new DataFrame with a column renamed.
func (df *DataFrame) Rename(oldName, newName string) *DataFrame {
	if _, exists := df.columns[oldName]; !exists {
		return df // Column doesn't exist, return unchanged
	}

	result := NewDataFrame()

	for _, name := range df.colOrder {
		col := df.columns[name]
		if name == oldName {
			// Create a new series with the new name
			renamed := renameSeries(col, newName)
			result.columns[newName] = renamed
			result.colOrder = append(result.colOrder, newName)
		} else {
			result.columns[name] = col
			result.colOrder = append(result.colOrder, name)
		}
	}

	return result
}

// renameSeries creates a copy of the series with a new name.
func renameSeries(s *Series, newName string) *Series {
	switch s.DType() {
	case Float64:
		if s.HasNulls() {
			values := s.ToFloat64()
			valid := make([]bool, s.Len())
			for i := 0; i < s.Len(); i++ {
				valid[i] = s.IsValid(i)
			}
			return NewSeriesF64WithNulls(newName, values, valid)
		}
		return NewSeriesF64(newName, s.ToFloat64())

	case Int64:
		if s.HasNulls() {
			values := s.ToInt64()
			valid := make([]bool, s.Len())
			for i := 0; i < s.Len(); i++ {
				valid[i] = s.IsValid(i)
			}
			return NewSeriesI64WithNulls(newName, values, valid)
		}
		return NewSeriesI64(newName, s.ToInt64())

	default:
		return nil
	}
}

// WithColumn returns a new DataFrame with the column added or replaced.
// If a column with the same name exists, it is replaced.
// The series length must match the DataFrame height (unless DataFrame is empty).
func (df *DataFrame) WithColumn(series *Series) *DataFrame {
	if series == nil {
		return df
	}

	// If DataFrame is not empty, verify length matches
	if df.Height() > 0 && series.Len() != df.Height() {
		return nil
	}

	result := NewDataFrame()
	name := series.Name()

	// Copy existing columns
	for _, colName := range df.colOrder {
		if colName != name {
			result.columns[colName] = df.columns[colName]
			result.colOrder = append(result.colOrder, colName)
		}
	}

	// Add/replace the column
	if _, exists := df.columns[name]; exists {
		// Insert at the same position
		newOrder := make([]string, 0, len(df.colOrder))
		for _, colName := range df.colOrder {
			if colName == name {
				newOrder = append(newOrder, name)
			} else {
				newOrder = append(newOrder, colName)
			}
		}
		result.colOrder = newOrder
	} else {
		result.colOrder = append(result.colOrder, name)
	}
	result.columns[name] = series

	return result
}

// Head returns a new DataFrame with the first n rows.
func (df *DataFrame) Head(n int) *DataFrame {
	if n <= 0 {
		return NewDataFrame()
	}
	if n > df.Height() {
		n = df.Height()
	}

	result := NewDataFrame()

	for _, name := range df.colOrder {
		col := df.columns[name]
		result.columns[name] = col.Head(n)
		result.colOrder = append(result.colOrder, name)
	}

	return result
}

// Tail returns a new DataFrame with the last n rows.
func (df *DataFrame) Tail(n int) *DataFrame {
	if n <= 0 {
		return NewDataFrame()
	}
	if n > df.Height() {
		n = df.Height()
	}

	result := NewDataFrame()

	for _, name := range df.colOrder {
		col := df.columns[name]
		result.columns[name] = col.Tail(n)
		result.colOrder = append(result.colOrder, name)
	}

	return result
}

// Slice returns a new DataFrame with rows from start to end (exclusive).
func (df *DataFrame) Slice(start, end int) *DataFrame {
	if start < 0 {
		start = 0
	}
	if end > df.Height() {
		end = df.Height()
	}
	if start >= end {
		return NewDataFrame()
	}

	result := NewDataFrame()

	for _, name := range df.colOrder {
		col := df.columns[name]
		result.columns[name] = col.Slice(start, end)
		result.colOrder = append(result.colOrder, name)
	}

	return result
}

// Clone creates a shallow copy of the DataFrame.
// The underlying Series are shared, not copied.
func (df *DataFrame) Clone() *DataFrame {
	result := NewDataFrame()

	for _, name := range df.colOrder {
		result.columns[name] = df.columns[name]
		result.colOrder = append(result.colOrder, name)
	}

	return result
}

// ============================================================================
// Join Operations
// ============================================================================

// InnerJoin performs an inner join between two DataFrames on the specified key columns.
// Only rows where keys match in both DataFrames are included in the result.
// The result contains all columns from both DataFrames (with right columns suffixed if names conflict).
// This uses a single CGO call to do join + materialize in Zig for maximum performance.
func InnerJoin(left, right *DataFrame, leftOn, rightOn string) *DataFrame {
	leftKey := left.Column(leftOn)
	rightKey := right.Column(rightOn)

	if leftKey == nil || rightKey == nil {
		return nil
	}

	// Both keys must be Int64 for now
	if leftKey.DType() != Int64 || rightKey.DType() != Int64 {
		return nil
	}

	// Collect column handles for CGO call
	leftCols := make([]unsafe.Pointer, len(left.colOrder))
	for i, name := range left.colOrder {
		leftCols[i] = left.columns[name].handle
	}

	rightCols := make([]unsafe.Pointer, len(right.colOrder))
	for i, name := range right.colOrder {
		rightCols[i] = right.columns[name].handle
	}

	// Single CGO call: join + materialize all columns
	joinResult := C.galleon_inner_join_full(
		(*C.ManagedArrowArray)(leftKey.handle),
		(*C.ManagedArrowArray)(rightKey.handle),
		(**C.ManagedArrowArray)(unsafe.Pointer(&leftCols[0])),
		C.size_t(len(leftCols)),
		(**C.ManagedArrowArray)(unsafe.Pointer(&rightCols[0])),
		C.size_t(len(rightCols)),
	)

	if joinResult == nil {
		return nil
	}
	// Use destroy_struct at end since we take ownership of columns
	defer C.galleon_full_join_result_destroy_struct(joinResult)

	numRows := int(C.galleon_full_join_result_num_rows(joinResult))
	if numRows == 0 {
		return NewDataFrame()
	}

	// Build result DataFrame from result columns
	result := NewDataFrame()
	colIdx := 0

	// Add left columns (take ownership)
	for _, name := range left.colOrder {
		colHandle := C.galleon_full_join_result_take_column(joinResult, C.size_t(colIdx))
		if colHandle == nil {
			return nil
		}
		series := seriesFromHandle(unsafe.Pointer(colHandle), name, left.columns[name].DType(), numRows)
		result.columns[name] = series
		result.colOrder = append(result.colOrder, name)
		colIdx++
	}

	// Add right columns, handling name conflicts (take ownership)
	for _, name := range right.colOrder {
		colHandle := C.galleon_full_join_result_take_column(joinResult, C.size_t(colIdx))
		if colHandle == nil {
			return nil
		}

		finalName := name
		if _, exists := result.columns[name]; exists {
			finalName = name + "_right"
		}

		series := seriesFromHandle(unsafe.Pointer(colHandle), finalName, right.columns[name].DType(), numRows)
		result.columns[finalName] = series
		result.colOrder = append(result.colOrder, finalName)
		colIdx++
	}

	return result
}

// LeftJoin performs a left join between two DataFrames on the specified key columns.
// All rows from the left DataFrame are included; unmatched rows have null values for right columns.
// This uses a single CGO call to do join + materialize in Zig for maximum performance.
func LeftJoin(left, right *DataFrame, leftOn, rightOn string) *DataFrame {
	leftKey := left.Column(leftOn)
	rightKey := right.Column(rightOn)

	if leftKey == nil || rightKey == nil {
		return nil
	}

	// Both keys must be Int64 for now
	if leftKey.DType() != Int64 || rightKey.DType() != Int64 {
		return nil
	}

	// Collect column handles for CGO call
	leftCols := make([]unsafe.Pointer, len(left.colOrder))
	for i, name := range left.colOrder {
		leftCols[i] = left.columns[name].handle
	}

	rightCols := make([]unsafe.Pointer, len(right.colOrder))
	for i, name := range right.colOrder {
		rightCols[i] = right.columns[name].handle
	}

	// Single CGO call: join + materialize all columns
	joinResult := C.galleon_left_join_full(
		(*C.ManagedArrowArray)(leftKey.handle),
		(*C.ManagedArrowArray)(rightKey.handle),
		(**C.ManagedArrowArray)(unsafe.Pointer(&leftCols[0])),
		C.size_t(len(leftCols)),
		(**C.ManagedArrowArray)(unsafe.Pointer(&rightCols[0])),
		C.size_t(len(rightCols)),
	)

	if joinResult == nil {
		return nil
	}
	// Use destroy_struct at end since we take ownership of columns
	defer C.galleon_full_join_result_destroy_struct(joinResult)

	numRows := int(C.galleon_full_join_result_num_rows(joinResult))
	if numRows == 0 {
		return NewDataFrame()
	}

	// Build result DataFrame from result columns
	result := NewDataFrame()
	colIdx := 0

	// Add left columns (take ownership)
	for _, name := range left.colOrder {
		colHandle := C.galleon_full_join_result_take_column(joinResult, C.size_t(colIdx))
		if colHandle == nil {
			return nil
		}
		series := seriesFromHandle(unsafe.Pointer(colHandle), name, left.columns[name].DType(), numRows)
		result.columns[name] = series
		result.colOrder = append(result.colOrder, name)
		colIdx++
	}

	// Add right columns, handling name conflicts (take ownership)
	for _, name := range right.colOrder {
		colHandle := C.galleon_full_join_result_take_column(joinResult, C.size_t(colIdx))
		if colHandle == nil {
			return nil
		}

		finalName := name
		if _, exists := result.columns[name]; exists {
			finalName = name + "_right"
		}

		series := seriesFromHandle(unsafe.Pointer(colHandle), finalName, right.columns[name].DType(), numRows)
		result.columns[finalName] = series
		result.colOrder = append(result.colOrder, finalName)
		colIdx++
	}

	return result
}

// seriesFromHandle creates a Series from a Zig-managed handle.
// The handle ownership is transferred to the Series.
func seriesFromHandle(handle unsafe.Pointer, name string, dtype DType, length int) *Series {
	s := &Series{
		handle: handle,
		name:   name,
		dtype:  dtype,
		length: length,
	}
	runtime.SetFinalizer(s, func(s *Series) {
		s.Release()
	})
	return s
}

// gatherSeriesByInt32 creates a new series by gathering elements at the given int32 indices.
// Uses SIMD-accelerated gather via CGO for better performance.
func gatherSeriesByInt32(s *Series, indices []int32) *Series {
	switch s.DType() {
	case Float64:
		values := s.ToFloat64()
		newValues := make([]float64, len(indices))
		GatherF64(values, indices, newValues)

		if s.HasNulls() {
			valid := make([]bool, len(indices))
			for i, idx := range indices {
				valid[i] = s.IsValid(int(idx))
			}
			return NewSeriesF64WithNulls(s.Name(), newValues, valid)
		}
		return NewSeriesF64(s.Name(), newValues)

	case Int64:
		values := s.ToInt64()
		newValues := make([]int64, len(indices))
		GatherI64(values, indices, newValues)

		if s.HasNulls() {
			valid := make([]bool, len(indices))
			for i, idx := range indices {
				valid[i] = s.IsValid(int(idx))
			}
			return NewSeriesI64WithNulls(s.Name(), newValues, valid)
		}
		return NewSeriesI64(s.Name(), newValues)

	default:
		return nil
	}
}

// gatherSeriesByInt32WithNull creates a new series by gathering elements at the given int32 indices.
// Index -1 means null (for left join unmatched rows).
// Uses SIMD-accelerated gather via CGO for better performance.
func gatherSeriesByInt32WithNull(s *Series, indices []int32) *Series {
	switch s.DType() {
	case Float64:
		values := s.ToFloat64()
		newValues := make([]float64, len(indices))
		// CGO gather handles -1 indices (sets to 0)
		GatherF64(values, indices, newValues)

		// Build validity bitmap
		valid := make([]bool, len(indices))
		hasSourceNulls := s.HasNulls()
		for i, idx := range indices {
			if idx < 0 {
				valid[i] = false
			} else if hasSourceNulls {
				valid[i] = s.IsValid(int(idx))
			} else {
				valid[i] = true
			}
		}
		return NewSeriesF64WithNulls(s.Name(), newValues, valid)

	case Int64:
		values := s.ToInt64()
		newValues := make([]int64, len(indices))
		// CGO gather handles -1 indices (sets to 0)
		GatherI64(values, indices, newValues)

		// Build validity bitmap
		valid := make([]bool, len(indices))
		hasSourceNulls := s.HasNulls()
		for i, idx := range indices {
			if idx < 0 {
				valid[i] = false
			} else if hasSourceNulls {
				valid[i] = s.IsValid(int(idx))
			} else {
				valid[i] = true
			}
		}
		return NewSeriesI64WithNulls(s.Name(), newValues, valid)

	default:
		return nil
	}
}
