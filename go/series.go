package galleon

/*
#include "galleon.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// Series represents a single column of data with a name and type
type Series struct {
	name   string
	dtype  DType
	length int

	// Underlying data - only one of these is non-nil based on dtype
	f64Col  *C.ColumnF64
	f32Col  *C.ColumnF32
	i64Col  *C.ColumnI64
	i32Col  *C.ColumnI32
	boolCol *C.ColumnBool

	// String data is stored in Go (Zig doesn't handle strings well via FFI)
	strData []string
}

// NewSeriesFloat64 creates a new Float64 series from a slice
func NewSeriesFloat64(name string, data []float64) *Series {
	if len(data) == 0 {
		return &Series{name: name, dtype: Float64, length: 0}
	}

	ptr := C.galleon_column_f64_create(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	s := &Series{
		name:   name,
		dtype:  Float64,
		length: len(data),
		f64Col: ptr,
	}
	runtime.SetFinalizer(s, (*Series).free)
	return s
}

// NewSeriesFloat32 creates a new Float32 series from a slice
func NewSeriesFloat32(name string, data []float32) *Series {
	if len(data) == 0 {
		return &Series{name: name, dtype: Float32, length: 0}
	}

	ptr := C.galleon_column_f32_create(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	s := &Series{
		name:   name,
		dtype:  Float32,
		length: len(data),
		f32Col: ptr,
	}
	runtime.SetFinalizer(s, (*Series).free)
	return s
}

// NewSeriesInt64 creates a new Int64 series from a slice
func NewSeriesInt64(name string, data []int64) *Series {
	if len(data) == 0 {
		return &Series{name: name, dtype: Int64, length: 0}
	}

	ptr := C.galleon_column_i64_create(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	s := &Series{
		name:   name,
		dtype:  Int64,
		length: len(data),
		i64Col: ptr,
	}
	runtime.SetFinalizer(s, (*Series).free)
	return s
}

// NewSeriesInt32 creates a new Int32 series from a slice
func NewSeriesInt32(name string, data []int32) *Series {
	if len(data) == 0 {
		return &Series{name: name, dtype: Int32, length: 0}
	}

	ptr := C.galleon_column_i32_create(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	s := &Series{
		name:   name,
		dtype:  Int32,
		length: len(data),
		i32Col: ptr,
	}
	runtime.SetFinalizer(s, (*Series).free)
	return s
}

// NewSeriesBool creates a new Bool series from a slice
func NewSeriesBool(name string, data []bool) *Series {
	if len(data) == 0 {
		return &Series{name: name, dtype: Bool, length: 0}
	}

	ptr := C.galleon_column_bool_create(
		(*C.bool)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	s := &Series{
		name:    name,
		dtype:   Bool,
		length:  len(data),
		boolCol: ptr,
	}
	runtime.SetFinalizer(s, (*Series).free)
	return s
}

// NewSeriesString creates a new String series from a slice
func NewSeriesString(name string, data []string) *Series {
	if len(data) == 0 {
		return &Series{name: name, dtype: String, length: 0}
	}

	// Copy the data
	strData := make([]string, len(data))
	copy(strData, data)

	return &Series{
		name:    name,
		dtype:   String,
		length:  len(data),
		strData: strData,
	}
}

// NewSeries creates a Series from any supported slice type.
// This is a convenience constructor that infers the type from the input.
// Supported types: []float64, []float32, []int64, []int32, []bool, []string
func NewSeries(name string, data interface{}) (*Series, error) {
	switch v := data.(type) {
	case []float64:
		return NewSeriesFloat64(name, v), nil
	case []float32:
		return NewSeriesFloat32(name, v), nil
	case []int64:
		return NewSeriesInt64(name, v), nil
	case []int32:
		return NewSeriesInt32(name, v), nil
	case []int:
		// Convert []int to []int64
		data64 := make([]int64, len(v))
		for i, val := range v {
			data64[i] = int64(val)
		}
		return NewSeriesInt64(name, data64), nil
	case []bool:
		return NewSeriesBool(name, v), nil
	case []string:
		return NewSeriesString(name, v), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %T", data)
	}
}

// free releases the underlying Zig memory
func (s *Series) free() {
	if s.f64Col != nil {
		C.galleon_column_f64_destroy(s.f64Col)
		s.f64Col = nil
	}
	if s.f32Col != nil {
		C.galleon_column_f32_destroy(s.f32Col)
		s.f32Col = nil
	}
	if s.i64Col != nil {
		C.galleon_column_i64_destroy(s.i64Col)
		s.i64Col = nil
	}
	if s.i32Col != nil {
		C.galleon_column_i32_destroy(s.i32Col)
		s.i32Col = nil
	}
	if s.boolCol != nil {
		C.galleon_column_bool_destroy(s.boolCol)
		s.boolCol = nil
	}
}

// Name returns the series name
func (s *Series) Name() string {
	return s.name
}

// Rename returns a new series with a different name (shares underlying data)
func (s *Series) Rename(name string) *Series {
	newS := &Series{
		name:    name,
		dtype:   s.dtype,
		length:  s.length,
		f64Col:  s.f64Col,
		f32Col:  s.f32Col,
		i64Col:  s.i64Col,
		i32Col:  s.i32Col,
		boolCol: s.boolCol,
		strData: s.strData,
	}
	// Note: This creates a shallow copy - both series share the same underlying data
	// The finalizer should only be on one of them to avoid double-free
	// For now, we don't set a finalizer on the copy
	return newS
}

// DType returns the data type of the series
func (s *Series) DType() DType {
	return s.dtype
}

// Len returns the number of elements in the series
func (s *Series) Len() int {
	return s.length
}

// IsEmpty returns true if the series has no elements
func (s *Series) IsEmpty() bool {
	return s.length == 0
}

// ============================================================================
// Data Access Methods
// ============================================================================

// Float64 returns the underlying data as []float64
func (s *Series) Float64() []float64 {
	if s.dtype != Float64 || s.f64Col == nil || s.length == 0 {
		return nil
	}
	ptr := C.galleon_column_f64_data(s.f64Col)
	return unsafe.Slice((*float64)(unsafe.Pointer(ptr)), s.length)
}

// Float32 returns the underlying data as []float32
func (s *Series) Float32() []float32 {
	if s.dtype != Float32 || s.f32Col == nil || s.length == 0 {
		return nil
	}
	ptr := C.galleon_column_f32_data(s.f32Col)
	return unsafe.Slice((*float32)(unsafe.Pointer(ptr)), s.length)
}

// Int64 returns the underlying data as []int64
func (s *Series) Int64() []int64 {
	if s.dtype != Int64 || s.i64Col == nil || s.length == 0 {
		return nil
	}
	ptr := C.galleon_column_i64_data(s.i64Col)
	return unsafe.Slice((*int64)(unsafe.Pointer(ptr)), s.length)
}

// Int32 returns the underlying data as []int32
func (s *Series) Int32() []int32 {
	if s.dtype != Int32 || s.i32Col == nil || s.length == 0 {
		return nil
	}
	ptr := C.galleon_column_i32_data(s.i32Col)
	return unsafe.Slice((*int32)(unsafe.Pointer(ptr)), s.length)
}

// Bool returns the underlying data as []bool
func (s *Series) Bool() []bool {
	if s.dtype != Bool || s.boolCol == nil || s.length == 0 {
		return nil
	}
	ptr := C.galleon_column_bool_data(s.boolCol)
	return unsafe.Slice((*bool)(unsafe.Pointer(ptr)), s.length)
}

// Strings returns the underlying data as []string
func (s *Series) Strings() []string {
	if s.dtype != String || s.length == 0 {
		return nil
	}
	return s.strData
}

// Get returns the value at index as interface{}
func (s *Series) Get(index int) interface{} {
	if index < 0 || index >= s.length {
		return nil
	}

	switch s.dtype {
	case Float64:
		if s.f64Col != nil {
			return float64(C.galleon_column_f64_get(s.f64Col, C.size_t(index)))
		}
	case Float32:
		if s.f32Col != nil {
			return float32(C.galleon_column_f32_get(s.f32Col, C.size_t(index)))
		}
	case Int64:
		if s.i64Col != nil {
			return int64(C.galleon_column_i64_get(s.i64Col, C.size_t(index)))
		}
	case Int32:
		if s.i32Col != nil {
			return int32(C.galleon_column_i32_get(s.i32Col, C.size_t(index)))
		}
	case Bool:
		if s.boolCol != nil {
			return bool(C.galleon_column_bool_get(s.boolCol, C.size_t(index)))
		}
	case String:
		if s.strData != nil {
			return s.strData[index]
		}
	}
	return nil
}

// GetFloat64 returns the value at index as float64
func (s *Series) GetFloat64(index int) (float64, bool) {
	if s.dtype != Float64 || s.f64Col == nil || index < 0 || index >= s.length {
		return 0, false
	}
	return float64(C.galleon_column_f64_get(s.f64Col, C.size_t(index))), true
}

// GetInt64 returns the value at index as int64
func (s *Series) GetInt64(index int) (int64, bool) {
	if s.dtype != Int64 || s.i64Col == nil || index < 0 || index >= s.length {
		return 0, false
	}
	return int64(C.galleon_column_i64_get(s.i64Col, C.size_t(index))), true
}

// GetString returns the value at index as string
func (s *Series) GetString(index int) (string, bool) {
	if s.dtype != String || s.strData == nil || index < 0 || index >= s.length {
		return "", false
	}
	return s.strData[index], true
}

// ============================================================================
// Aggregation Operations
// ============================================================================

// Sum returns the sum of all values
func (s *Series) Sum() float64 {
	if s.length == 0 {
		return 0
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return 0
		}
		return SumF64(data)
	case Float32:
		data := s.Float32()
		if data == nil {
			return 0
		}
		return float64(SumF32(data))
	case Int64:
		data := s.Int64()
		if data == nil {
			return 0
		}
		return float64(SumI64(data))
	case Int32:
		data := s.Int32()
		if data == nil {
			return 0
		}
		return float64(SumI32(data))
	}
	return 0
}

// SumInt returns the sum as int64 (for integer types)
func (s *Series) SumInt() int64 {
	if s.length == 0 {
		return 0
	}

	switch s.dtype {
	case Int64:
		data := s.Int64()
		if data == nil {
			return 0
		}
		return SumI64(data)
	case Int32:
		data := s.Int32()
		if data == nil {
			return 0
		}
		return int64(SumI32(data))
	}
	return 0
}

// Min returns the minimum value
func (s *Series) Min() float64 {
	if s.length == 0 {
		return 0
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return 0
		}
		return MinF64(data)
	case Float32:
		data := s.Float32()
		if data == nil {
			return 0
		}
		return float64(MinF32(data))
	case Int64:
		data := s.Int64()
		if data == nil {
			return 0
		}
		return float64(MinI64(data))
	case Int32:
		data := s.Int32()
		if data == nil {
			return 0
		}
		return float64(MinI32(data))
	}
	return 0
}

// Max returns the maximum value
func (s *Series) Max() float64 {
	if s.length == 0 {
		return 0
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return 0
		}
		return MaxF64(data)
	case Float32:
		data := s.Float32()
		if data == nil {
			return 0
		}
		return float64(MaxF32(data))
	case Int64:
		data := s.Int64()
		if data == nil {
			return 0
		}
		return float64(MaxI64(data))
	case Int32:
		data := s.Int32()
		if data == nil {
			return 0
		}
		return float64(MaxI32(data))
	}
	return 0
}

// Mean returns the arithmetic mean
func (s *Series) Mean() float64 {
	if s.length == 0 {
		return 0
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return 0
		}
		return MeanF64(data)
	case Float32:
		data := s.Float32()
		if data == nil {
			return 0
		}
		return float64(MeanF32(data))
	case Int64, Int32:
		return s.Sum() / float64(s.length)
	}
	return 0
}

// CountTrue returns the count of true values (for Bool series)
func (s *Series) CountTrue() int {
	if s.dtype != Bool || s.boolCol == nil || s.length == 0 {
		return 0
	}
	data := s.Bool()
	return CountTrue(data)
}

// ============================================================================
// Filter Operations
// ============================================================================

// Gt returns indices where the condition is true
func (s *Series) Gt(threshold float64) []uint32 {
	if s.length == 0 {
		return nil
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		return FilterGreaterThanF64(data, threshold)
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		return FilterGreaterThanF32(data, float32(threshold))
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		return FilterGreaterThanI64(data, int64(threshold))
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		return FilterGreaterThanI32(data, int32(threshold))
	}
	return nil
}

// GtMask returns a byte mask (0/1) where values are greater than threshold
func (s *Series) GtMask(threshold float64, mask []byte) []byte {
	if s.length == 0 {
		return mask
	}

	if len(mask) < s.length {
		mask = make([]byte, s.length)
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return mask
		}
		return FilterMaskU8GreaterThanF64Into(data, threshold, mask)
	}
	return mask
}

// Lt returns indices where values are less than threshold
func (s *Series) Lt(threshold float64) []uint32 {
	if s.length == 0 {
		return nil
	}

	var indices []uint32
	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		for i, v := range data {
			if v < threshold {
				indices = append(indices, uint32(i))
			}
		}
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		t := float32(threshold)
		for i, v := range data {
			if v < t {
				indices = append(indices, uint32(i))
			}
		}
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		t := int64(threshold)
		for i, v := range data {
			if v < t {
				indices = append(indices, uint32(i))
			}
		}
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		t := int32(threshold)
		for i, v := range data {
			if v < t {
				indices = append(indices, uint32(i))
			}
		}
	}
	return indices
}

// Lte returns indices where values are less than or equal to threshold
func (s *Series) Lte(threshold float64) []uint32 {
	if s.length == 0 {
		return nil
	}

	var indices []uint32
	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		for i, v := range data {
			if v <= threshold {
				indices = append(indices, uint32(i))
			}
		}
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		t := float32(threshold)
		for i, v := range data {
			if v <= t {
				indices = append(indices, uint32(i))
			}
		}
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		t := int64(threshold)
		for i, v := range data {
			if v <= t {
				indices = append(indices, uint32(i))
			}
		}
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		t := int32(threshold)
		for i, v := range data {
			if v <= t {
				indices = append(indices, uint32(i))
			}
		}
	}
	return indices
}

// Gte returns indices where values are greater than or equal to threshold
func (s *Series) Gte(threshold float64) []uint32 {
	if s.length == 0 {
		return nil
	}

	var indices []uint32
	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		for i, v := range data {
			if v >= threshold {
				indices = append(indices, uint32(i))
			}
		}
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		t := float32(threshold)
		for i, v := range data {
			if v >= t {
				indices = append(indices, uint32(i))
			}
		}
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		t := int64(threshold)
		for i, v := range data {
			if v >= t {
				indices = append(indices, uint32(i))
			}
		}
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		t := int32(threshold)
		for i, v := range data {
			if v >= t {
				indices = append(indices, uint32(i))
			}
		}
	}
	return indices
}

// Eq returns indices where values equal the threshold
func (s *Series) Eq(threshold float64) []uint32 {
	if s.length == 0 {
		return nil
	}

	var indices []uint32
	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		for i, v := range data {
			if v == threshold {
				indices = append(indices, uint32(i))
			}
		}
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		t := float32(threshold)
		for i, v := range data {
			if v == t {
				indices = append(indices, uint32(i))
			}
		}
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		t := int64(threshold)
		for i, v := range data {
			if v == t {
				indices = append(indices, uint32(i))
			}
		}
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		t := int32(threshold)
		for i, v := range data {
			if v == t {
				indices = append(indices, uint32(i))
			}
		}
	}
	return indices
}

// Neq returns indices where values do not equal the threshold
func (s *Series) Neq(threshold float64) []uint32 {
	if s.length == 0 {
		return nil
	}

	var indices []uint32
	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		for i, v := range data {
			if v != threshold {
				indices = append(indices, uint32(i))
			}
		}
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		t := float32(threshold)
		for i, v := range data {
			if v != t {
				indices = append(indices, uint32(i))
			}
		}
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		t := int64(threshold)
		for i, v := range data {
			if v != t {
				indices = append(indices, uint32(i))
			}
		}
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		t := int32(threshold)
		for i, v := range data {
			if v != t {
				indices = append(indices, uint32(i))
			}
		}
	}
	return indices
}

// EqString returns indices where string values equal the target (for String series)
func (s *Series) EqString(target string) []uint32 {
	if s.dtype != String || s.length == 0 {
		return nil
	}

	var indices []uint32
	for i, v := range s.strData {
		if v == target {
			indices = append(indices, uint32(i))
		}
	}
	return indices
}

// NeqString returns indices where string values do not equal the target (for String series)
func (s *Series) NeqString(target string) []uint32 {
	if s.dtype != String || s.length == 0 {
		return nil
	}

	var indices []uint32
	for i, v := range s.strData {
		if v != target {
			indices = append(indices, uint32(i))
		}
	}
	return indices
}

// ============================================================================
// Sort Operations
// ============================================================================

// Argsort returns indices that would sort the series
func (s *Series) Argsort(ascending bool) []uint32 {
	if s.length == 0 {
		return nil
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		return ArgsortF64(data, ascending)
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		return ArgsortF32(data, ascending)
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		return ArgsortI64(data, ascending)
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		return ArgsortI32(data, ascending)
	}
	return nil
}

// ============================================================================
// Arithmetic Operations (return new Series)
// ============================================================================

// Add returns a new series with scalar added to each element
func (s *Series) Add(scalar float64) *Series {
	if s.length == 0 {
		return nil
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		newData := make([]float64, len(data))
		copy(newData, data)
		AddScalarF64(newData, scalar)
		return NewSeriesFloat64(s.name, newData)
	case Float32:
		data := s.Float32()
		newData := make([]float32, len(data))
		copy(newData, data)
		AddScalarF32(newData, float32(scalar))
		return NewSeriesFloat32(s.name, newData)
	case Int64:
		data := s.Int64()
		newData := make([]int64, len(data))
		copy(newData, data)
		AddScalarI64(newData, int64(scalar))
		return NewSeriesInt64(s.name, newData)
	case Int32:
		data := s.Int32()
		newData := make([]int32, len(data))
		copy(newData, data)
		AddScalarI32(newData, int32(scalar))
		return NewSeriesInt32(s.name, newData)
	}
	return nil
}

// Mul returns a new series with each element multiplied by scalar
func (s *Series) Mul(scalar float64) *Series {
	if s.length == 0 {
		return nil
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		newData := make([]float64, len(data))
		copy(newData, data)
		MulScalarF64(newData, scalar)
		return NewSeriesFloat64(s.name, newData)
	case Float32:
		data := s.Float32()
		newData := make([]float32, len(data))
		copy(newData, data)
		MulScalarF32(newData, float32(scalar))
		return NewSeriesFloat32(s.name, newData)
	case Int64:
		data := s.Int64()
		newData := make([]int64, len(data))
		copy(newData, data)
		MulScalarI64(newData, int64(scalar))
		return NewSeriesInt64(s.name, newData)
	case Int32:
		data := s.Int32()
		newData := make([]int32, len(data))
		copy(newData, data)
		MulScalarI32(newData, int32(scalar))
		return NewSeriesInt32(s.name, newData)
	}
	return nil
}

// ============================================================================
// Display
// ============================================================================

// String returns a string representation of the series
func (s *Series) String() string {
	if s.length == 0 {
		return fmt.Sprintf("Series('%s', dtype=%s, len=0)", s.name, s.dtype)
	}

	// Show first few and last few elements
	maxShow := 5
	var preview string

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if s.length <= maxShow*2 {
			preview = fmt.Sprintf("%v", data)
		} else {
			preview = fmt.Sprintf("%v ... %v", data[:maxShow], data[s.length-maxShow:])
		}
	case Float32:
		data := s.Float32()
		if s.length <= maxShow*2 {
			preview = fmt.Sprintf("%v", data)
		} else {
			preview = fmt.Sprintf("%v ... %v", data[:maxShow], data[s.length-maxShow:])
		}
	case Int64:
		data := s.Int64()
		if s.length <= maxShow*2 {
			preview = fmt.Sprintf("%v", data)
		} else {
			preview = fmt.Sprintf("%v ... %v", data[:maxShow], data[s.length-maxShow:])
		}
	case Int32:
		data := s.Int32()
		if s.length <= maxShow*2 {
			preview = fmt.Sprintf("%v", data)
		} else {
			preview = fmt.Sprintf("%v ... %v", data[:maxShow], data[s.length-maxShow:])
		}
	case Bool:
		data := s.Bool()
		if s.length <= maxShow*2 {
			preview = fmt.Sprintf("%v", data)
		} else {
			preview = fmt.Sprintf("%v ... %v", data[:maxShow], data[s.length-maxShow:])
		}
	case String:
		if s.length <= maxShow*2 {
			preview = fmt.Sprintf("%v", s.strData)
		} else {
			preview = fmt.Sprintf("%v ... %v", s.strData[:maxShow], s.strData[s.length-maxShow:])
		}
	default:
		preview = "..."
	}

	return fmt.Sprintf("Series('%s', dtype=%s, len=%d)\n%s", s.name, s.dtype, s.length, preview)
}

// Head returns a new series with the first n elements
func (s *Series) Head(n int) *Series {
	if n <= 0 || s.length == 0 {
		return &Series{name: s.name, dtype: s.dtype, length: 0}
	}
	if n > s.length {
		n = s.length
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		return NewSeriesFloat64(s.name, data[:n])
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		return NewSeriesFloat32(s.name, data[:n])
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		return NewSeriesInt64(s.name, data[:n])
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		return NewSeriesInt32(s.name, data[:n])
	case Bool:
		data := s.Bool()
		if data == nil {
			return nil
		}
		return NewSeriesBool(s.name, data[:n])
	case String:
		return NewSeriesString(s.name, s.strData[:n])
	}
	return nil
}

// Tail returns a new series with the last n elements
func (s *Series) Tail(n int) *Series {
	if n <= 0 || s.length == 0 {
		return &Series{name: s.name, dtype: s.dtype, length: 0}
	}
	if n > s.length {
		n = s.length
	}

	switch s.dtype {
	case Float64:
		data := s.Float64()
		if data == nil {
			return nil
		}
		return NewSeriesFloat64(s.name, data[s.length-n:])
	case Float32:
		data := s.Float32()
		if data == nil {
			return nil
		}
		return NewSeriesFloat32(s.name, data[s.length-n:])
	case Int64:
		data := s.Int64()
		if data == nil {
			return nil
		}
		return NewSeriesInt64(s.name, data[s.length-n:])
	case Int32:
		data := s.Int32()
		if data == nil {
			return nil
		}
		return NewSeriesInt32(s.name, data[s.length-n:])
	case Bool:
		data := s.Bool()
		if data == nil {
			return nil
		}
		return NewSeriesBool(s.name, data[s.length-n:])
	case String:
		return NewSeriesString(s.name, s.strData[s.length-n:])
	}
	return nil
}

// Describe returns summary statistics
func (s *Series) Describe() map[string]float64 {
	if s.length == 0 || !s.dtype.IsNumeric() {
		return nil
	}

	return map[string]float64{
		"count": float64(s.length),
		"sum":   s.Sum(),
		"min":   s.Min(),
		"max":   s.Max(),
		"mean":  s.Mean(),
	}
}
