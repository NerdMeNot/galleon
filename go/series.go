package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/../core/include
#cgo LDFLAGS: -L${SRCDIR}/../core/zig-out/lib -lgalleon
#include "galleon.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// Series is a Series backed by Arrow memory layout.
// All Arrow operations are performed in Zig - Go only sends raw data
// and receives results. This ensures zero-copy SIMD operations.
type Series struct {
	handle unsafe.Pointer // *C.ManagedArrowArray
	name   string
	dtype  DType
	length int
}

// NewSeriesF64 creates a Float64 Series from a Go slice.
// The data is copied to Zig-managed Arrow memory.
func NewSeriesF64(name string, data []float64) *Series {
	if len(data) == 0 {
		return &Series{
			handle: nil,
			name:   name,
			dtype:  Float64,
			length: 0,
		}
	}

	handle := C.galleon_series_create_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if handle == nil {
		return nil
	}

	s := &Series{
		handle: unsafe.Pointer(handle),
		name:   name,
		dtype:  Float64,
		length: len(data),
	}

	// Set finalizer to free Zig memory when GC collects this
	runtime.SetFinalizer(s, func(s *Series) {
		s.Release()
	})

	return s
}

// NewSeriesI64 creates an Int64 Series from a Go slice.
func NewSeriesI64(name string, data []int64) *Series {
	if len(data) == 0 {
		return &Series{
			handle: nil,
			name:   name,
			dtype:  Int64,
			length: 0,
		}
	}

	handle := C.galleon_series_create_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if handle == nil {
		return nil
	}

	s := &Series{
		handle: unsafe.Pointer(handle),
		name:   name,
		dtype:  Int64,
		length: len(data),
	}

	runtime.SetFinalizer(s, func(s *Series) {
		s.Release()
	})

	return s
}

// NewSeriesF64WithNulls creates a Float64 Series with null values.
// The valid slice indicates which values are valid (true) vs null (false).
func NewSeriesF64WithNulls(name string, data []float64, valid []bool) *Series {
	if len(data) == 0 {
		return &Series{
			handle: nil,
			name:   name,
			dtype:  Float64,
			length: 0,
		}
	}

	// Convert bool slice to packed bitmap (Arrow format: LSB first)
	bitmapLen := (len(data) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	var nullCount int64

	for i, isValid := range valid {
		if isValid {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			bitmap[byteIdx] |= 1 << bitIdx
		} else {
			nullCount++
		}
	}

	handle := C.galleon_series_create_f64_with_nulls(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&bitmap[0])),
		C.size_t(len(bitmap)),
		C.int64_t(nullCount),
	)

	if handle == nil {
		return nil
	}

	s := &Series{
		handle: unsafe.Pointer(handle),
		name:   name,
		dtype:  Float64,
		length: len(data),
	}

	runtime.SetFinalizer(s, func(s *Series) {
		s.Release()
	})

	return s
}

// NewSeriesI64WithNulls creates an Int64 Series with null values.
func NewSeriesI64WithNulls(name string, data []int64, valid []bool) *Series {
	if len(data) == 0 {
		return &Series{
			handle: nil,
			name:   name,
			dtype:  Int64,
			length: 0,
		}
	}

	// Convert bool slice to packed bitmap
	bitmapLen := (len(data) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	var nullCount int64

	for i, isValid := range valid {
		if isValid {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			bitmap[byteIdx] |= 1 << bitIdx
		} else {
			nullCount++
		}
	}

	handle := C.galleon_series_create_i64_with_nulls(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&bitmap[0])),
		C.size_t(len(bitmap)),
		C.int64_t(nullCount),
	)

	if handle == nil {
		return nil
	}

	s := &Series{
		handle: unsafe.Pointer(handle),
		name:   name,
		dtype:  Int64,
		length: len(data),
	}

	runtime.SetFinalizer(s, func(s *Series) {
		s.Release()
	})

	return s
}

// NewSeriesF32 creates a Float32 Series from a Go slice.
func NewSeriesF32(name string, data []float32) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: Float32, length: 0}
	}

	handle := C.galleon_series_create_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: Float32, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesI32 creates an Int32 Series from a Go slice.
func NewSeriesI32(name string, data []int32) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: Int32, length: 0}
	}

	handle := C.galleon_series_create_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: Int32, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesU64 creates a UInt64 Series from a Go slice.
func NewSeriesU64(name string, data []uint64) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: UInt64, length: 0}
	}

	handle := C.galleon_series_create_u64(
		(*C.uint64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: UInt64, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesU32 creates a UInt32 Series from a Go slice.
func NewSeriesU32(name string, data []uint32) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: UInt32, length: 0}
	}

	handle := C.galleon_series_create_u32(
		(*C.uint32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: UInt32, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesF32WithNulls creates a Float32 Series with null values.
func NewSeriesF32WithNulls(name string, data []float32, valid []bool) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: Float32, length: 0}
	}

	bitmapLen := (len(data) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	var nullCount int64

	for i, isValid := range valid {
		if isValid {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			bitmap[byteIdx] |= 1 << bitIdx
		} else {
			nullCount++
		}
	}

	handle := C.galleon_series_create_f32_with_nulls(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&bitmap[0])),
		C.size_t(len(bitmap)),
		C.int64_t(nullCount),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: Float32, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesI32WithNulls creates an Int32 Series with null values.
func NewSeriesI32WithNulls(name string, data []int32, valid []bool) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: Int32, length: 0}
	}

	bitmapLen := (len(data) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	var nullCount int64

	for i, isValid := range valid {
		if isValid {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			bitmap[byteIdx] |= 1 << bitIdx
		} else {
			nullCount++
		}
	}

	handle := C.galleon_series_create_i32_with_nulls(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&bitmap[0])),
		C.size_t(len(bitmap)),
		C.int64_t(nullCount),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: Int32, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesU64WithNulls creates a UInt64 Series with null values.
func NewSeriesU64WithNulls(name string, data []uint64, valid []bool) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: UInt64, length: 0}
	}

	bitmapLen := (len(data) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	var nullCount int64

	for i, isValid := range valid {
		if isValid {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			bitmap[byteIdx] |= 1 << bitIdx
		} else {
			nullCount++
		}
	}

	handle := C.galleon_series_create_u64_with_nulls(
		(*C.uint64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&bitmap[0])),
		C.size_t(len(bitmap)),
		C.int64_t(nullCount),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: UInt64, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// NewSeriesU32WithNulls creates a UInt32 Series with null values.
func NewSeriesU32WithNulls(name string, data []uint32, valid []bool) *Series {
	if len(data) == 0 {
		return &Series{handle: nil, name: name, dtype: UInt32, length: 0}
	}

	bitmapLen := (len(data) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	var nullCount int64

	for i, isValid := range valid {
		if isValid {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			bitmap[byteIdx] |= 1 << bitIdx
		} else {
			nullCount++
		}
	}

	handle := C.galleon_series_create_u32_with_nulls(
		(*C.uint32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint8_t)(unsafe.Pointer(&bitmap[0])),
		C.size_t(len(bitmap)),
		C.int64_t(nullCount),
	)

	if handle == nil {
		return nil
	}

	s := &Series{handle: unsafe.Pointer(handle), name: name, dtype: UInt32, length: len(data)}
	runtime.SetFinalizer(s, func(s *Series) { s.Release() })
	return s
}

// Release frees the Zig-managed Arrow memory.
// This is called automatically by the finalizer, but can be called
// explicitly for deterministic cleanup.
func (s *Series) Release() {
	if s.handle != nil {
		C.galleon_series_destroy((*C.ManagedArrowArray)(s.handle))
		s.handle = nil
	}
}

// Name returns the series name.
func (s *Series) Name() string {
	return s.name
}

// DType returns the data type.
func (s *Series) DType() DType {
	return s.dtype
}

// Len returns the number of elements.
func (s *Series) Len() int {
	if s.handle == nil {
		return 0
	}
	return int(C.galleon_series_len((*C.ManagedArrowArray)(s.handle)))
}

// NullCount returns the number of null values.
func (s *Series) NullCount() int64 {
	if s.handle == nil {
		return 0
	}
	return int64(C.galleon_series_null_count((*C.ManagedArrowArray)(s.handle)))
}

// HasNulls returns true if the series has any null values.
func (s *Series) HasNulls() bool {
	if s.handle == nil {
		return false
	}
	return bool(C.galleon_series_has_nulls((*C.ManagedArrowArray)(s.handle)))
}

// Sum returns the sum of all values (skipping nulls).
func (s *Series) Sum() float64 {
	if s.handle == nil {
		return 0
	}
	switch s.dtype {
	case Float64:
		return float64(C.galleon_series_sum_f64((*C.ManagedArrowArray)(s.handle)))
	case Float32:
		return float64(C.galleon_series_sum_f32((*C.ManagedArrowArray)(s.handle)))
	case Int64:
		return float64(C.galleon_series_sum_i64((*C.ManagedArrowArray)(s.handle)))
	case Int32:
		return float64(C.galleon_series_sum_i32((*C.ManagedArrowArray)(s.handle)))
	case UInt64:
		return float64(C.galleon_series_sum_u64((*C.ManagedArrowArray)(s.handle)))
	case UInt32:
		return float64(C.galleon_series_sum_u32((*C.ManagedArrowArray)(s.handle)))
	default:
		return 0
	}
}

// SumI64 returns the sum as int64 (for Int64 series).
func (s *Series) SumI64() int64 {
	if s.handle == nil || s.dtype != Int64 {
		return 0
	}
	return int64(C.galleon_series_sum_i64((*C.ManagedArrowArray)(s.handle)))
}

// Min returns the minimum value (skipping nulls).
func (s *Series) Min() float64 {
	if s.handle == nil {
		return 0
	}
	switch s.dtype {
	case Float64:
		return float64(C.galleon_series_min_f64((*C.ManagedArrowArray)(s.handle)))
	case Float32:
		return float64(C.galleon_series_min_f32((*C.ManagedArrowArray)(s.handle)))
	case Int64:
		return float64(C.galleon_series_min_i64((*C.ManagedArrowArray)(s.handle)))
	case Int32:
		return float64(C.galleon_series_min_i32((*C.ManagedArrowArray)(s.handle)))
	case UInt64:
		return float64(C.galleon_series_min_u64((*C.ManagedArrowArray)(s.handle)))
	case UInt32:
		return float64(C.galleon_series_min_u32((*C.ManagedArrowArray)(s.handle)))
	default:
		return 0
	}
}

// MinI64 returns the minimum as int64 (for Int64 series).
func (s *Series) MinI64() int64 {
	if s.handle == nil || s.dtype != Int64 {
		return 0
	}
	return int64(C.galleon_series_min_i64((*C.ManagedArrowArray)(s.handle)))
}

// Max returns the maximum value (skipping nulls).
func (s *Series) Max() float64 {
	if s.handle == nil {
		return 0
	}
	switch s.dtype {
	case Float64:
		return float64(C.galleon_series_max_f64((*C.ManagedArrowArray)(s.handle)))
	case Float32:
		return float64(C.galleon_series_max_f32((*C.ManagedArrowArray)(s.handle)))
	case Int64:
		return float64(C.galleon_series_max_i64((*C.ManagedArrowArray)(s.handle)))
	case Int32:
		return float64(C.galleon_series_max_i32((*C.ManagedArrowArray)(s.handle)))
	case UInt64:
		return float64(C.galleon_series_max_u64((*C.ManagedArrowArray)(s.handle)))
	case UInt32:
		return float64(C.galleon_series_max_u32((*C.ManagedArrowArray)(s.handle)))
	default:
		return 0
	}
}

// MaxI64 returns the maximum as int64 (for Int64 series).
func (s *Series) MaxI64() int64 {
	if s.handle == nil || s.dtype != Int64 {
		return 0
	}
	return int64(C.galleon_series_max_i64((*C.ManagedArrowArray)(s.handle)))
}

// Mean returns the mean of all values (skipping nulls).
func (s *Series) Mean() float64 {
	if s.handle == nil {
		return 0
	}
	switch s.dtype {
	case Float64:
		return float64(C.galleon_series_mean_f64((*C.ManagedArrowArray)(s.handle)))
	case Float32:
		return float64(C.galleon_series_mean_f32((*C.ManagedArrowArray)(s.handle)))
	case Int64, Int32, UInt64, UInt32:
		// For integer types, compute mean as float64
		sum := s.Sum()
		count := float64(s.Len()) - float64(s.NullCount())
		if count > 0 {
			return sum / count
		}
		return 0
	default:
		return 0
	}
}

// Argsort returns the indices that would sort the series.
// The returned slice is owned by the caller.
func (s *Series) Argsort(ascending bool) []uint32 {
	if s.handle == nil || s.length == 0 {
		return nil
	}

	var result *C.SeriesArgsortResult
	switch s.dtype {
	case Float64:
		result = C.galleon_series_argsort_f64((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case Float32:
		result = C.galleon_series_argsort_f32((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case Int64:
		result = C.galleon_series_argsort_i64((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case Int32:
		result = C.galleon_series_argsort_i32((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case UInt64:
		result = C.galleon_series_argsort_u64((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case UInt32:
		result = C.galleon_series_argsort_u32((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	default:
		return nil
	}

	if result == nil {
		return nil
	}
	defer C.galleon_series_argsort_destroy(result)

	// Copy indices to Go slice
	length := int(C.galleon_series_argsort_len(result))
	if length == 0 {
		return nil
	}

	indices := make([]uint32, length)
	cIndices := C.galleon_series_argsort_indices(result)
	for i := 0; i < length; i++ {
		indices[i] = uint32(*(*C.uint32_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cIndices)) + uintptr(i)*4)))
	}

	return indices
}

// Sort returns a new sorted Series.
// The original series is not modified.
func (s *Series) Sort(ascending bool) *Series {
	if s.handle == nil || s.length == 0 {
		return &Series{
			handle: nil,
			name:   s.name,
			dtype:  s.dtype,
			length: 0,
		}
	}

	var newHandle *C.ManagedArrowArray
	switch s.dtype {
	case Float64:
		newHandle = C.galleon_series_sort_f64((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case Float32:
		newHandle = C.galleon_series_sort_f32((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case Int64:
		newHandle = C.galleon_series_sort_i64((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case Int32:
		newHandle = C.galleon_series_sort_i32((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case UInt64:
		newHandle = C.galleon_series_sort_u64((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	case UInt32:
		newHandle = C.galleon_series_sort_u32((*C.ManagedArrowArray)(s.handle), C.bool(ascending))
	default:
		return nil
	}

	if newHandle == nil {
		return nil
	}

	sorted := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(sorted, func(s *Series) {
		s.Release()
	})

	return sorted
}

// SortDesc returns a new Series sorted in descending order.
func (s *Series) SortDesc() *Series {
	return s.Sort(false)
}

// SortAsc returns a new Series sorted in ascending order.
func (s *Series) SortAsc() *Series {
	return s.Sort(true)
}

// --- Data Access Methods ---

// IsValid returns true if the value at the given index is valid (not null).
// Returns false if index is out of bounds.
func (s *Series) IsValid(index int) bool {
	if s.handle == nil || index < 0 || index >= s.length {
		return false
	}
	return bool(C.galleon_series_is_valid((*C.ManagedArrowArray)(s.handle), C.size_t(index)))
}

// AtF64 returns the Float64 value at the given index.
// Returns (value, true) if valid, (0, false) if null or out of bounds.
func (s *Series) AtF64(index int) (float64, bool) {
	if s.handle == nil || index < 0 || index >= s.length {
		return 0, false
	}
	if !s.IsValid(index) {
		return 0, false
	}
	return float64(C.galleon_series_get_f64((*C.ManagedArrowArray)(s.handle), C.size_t(index))), true
}

// AtI64 returns the Int64 value at the given index.
// Returns (value, true) if valid, (0, false) if null or out of bounds.
func (s *Series) AtI64(index int) (int64, bool) {
	if s.handle == nil || index < 0 || index >= s.length {
		return 0, false
	}
	if !s.IsValid(index) {
		return 0, false
	}
	return int64(C.galleon_series_get_i64((*C.ManagedArrowArray)(s.handle), C.size_t(index))), true
}

// AtF32 returns the Float32 value at the given index.
// Returns (value, true) if valid, (0, false) if null or out of bounds.
func (s *Series) AtF32(index int) (float32, bool) {
	if s.handle == nil || index < 0 || index >= s.length {
		return 0, false
	}
	if !s.IsValid(index) {
		return 0, false
	}
	return float32(C.galleon_series_get_f32((*C.ManagedArrowArray)(s.handle), C.size_t(index))), true
}

// AtI32 returns the Int32 value at the given index.
// Returns (value, true) if valid, (0, false) if null or out of bounds.
func (s *Series) AtI32(index int) (int32, bool) {
	if s.handle == nil || index < 0 || index >= s.length {
		return 0, false
	}
	if !s.IsValid(index) {
		return 0, false
	}
	return int32(C.galleon_series_get_i32((*C.ManagedArrowArray)(s.handle), C.size_t(index))), true
}

// AtU64 returns the UInt64 value at the given index.
// Returns (value, true) if valid, (0, false) if null or out of bounds.
func (s *Series) AtU64(index int) (uint64, bool) {
	if s.handle == nil || index < 0 || index >= s.length {
		return 0, false
	}
	if !s.IsValid(index) {
		return 0, false
	}
	return uint64(C.galleon_series_get_u64((*C.ManagedArrowArray)(s.handle), C.size_t(index))), true
}

// AtU32 returns the UInt32 value at the given index.
// Returns (value, true) if valid, (0, false) if null or out of bounds.
func (s *Series) AtU32(index int) (uint32, bool) {
	if s.handle == nil || index < 0 || index >= s.length {
		return 0, false
	}
	if !s.IsValid(index) {
		return 0, false
	}
	return uint32(C.galleon_series_get_u32((*C.ManagedArrowArray)(s.handle), C.size_t(index))), true
}

// Slice returns a new Series containing elements [start, end).
// The new series owns its own memory.
func (s *Series) Slice(start, end int) *Series {
	if s.handle == nil {
		return &Series{
			handle: nil,
			name:   s.name,
			dtype:  s.dtype,
			length: 0,
		}
	}

	// Clamp bounds
	if start < 0 {
		start = 0
	}
	if end > s.length {
		end = s.length
	}
	if start >= end {
		return &Series{
			handle: nil,
			name:   s.name,
			dtype:  s.dtype,
			length: 0,
		}
	}

	var newHandle *C.ManagedArrowArray
	switch s.dtype {
	case Float64:
		newHandle = C.galleon_series_slice_f64((*C.ManagedArrowArray)(s.handle), C.size_t(start), C.size_t(end))
	case Float32:
		newHandle = C.galleon_series_slice_f32((*C.ManagedArrowArray)(s.handle), C.size_t(start), C.size_t(end))
	case Int64:
		newHandle = C.galleon_series_slice_i64((*C.ManagedArrowArray)(s.handle), C.size_t(start), C.size_t(end))
	case Int32:
		newHandle = C.galleon_series_slice_i32((*C.ManagedArrowArray)(s.handle), C.size_t(start), C.size_t(end))
	case UInt64:
		newHandle = C.galleon_series_slice_u64((*C.ManagedArrowArray)(s.handle), C.size_t(start), C.size_t(end))
	case UInt32:
		newHandle = C.galleon_series_slice_u32((*C.ManagedArrowArray)(s.handle), C.size_t(start), C.size_t(end))
	default:
		return nil
	}

	if newHandle == nil {
		return nil
	}

	sliced := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: end - start,
	}

	runtime.SetFinalizer(sliced, func(s *Series) {
		s.Release()
	})

	return sliced
}

// ToFloat64 copies all data to a Go float64 slice.
// Null values are represented as NaN.
func (s *Series) ToFloat64() []float64 {
	if s.handle == nil || s.length == 0 {
		return []float64{}
	}

	result := make([]float64, s.length)
	C.galleon_series_copy_f64(
		(*C.ManagedArrowArray)(s.handle),
		(*C.double)(unsafe.Pointer(&result[0])),
		C.size_t(len(result)),
	)
	return result
}

// ToInt64 copies all data to a Go int64 slice.
// Null values are represented as 0.
func (s *Series) ToInt64() []int64 {
	if s.handle == nil || s.length == 0 {
		return []int64{}
	}

	result := make([]int64, s.length)
	C.galleon_series_copy_i64(
		(*C.ManagedArrowArray)(s.handle),
		(*C.int64_t)(unsafe.Pointer(&result[0])),
		C.size_t(len(result)),
	)
	return result
}

// Values returns the underlying data as a slice.
// Returns the appropriate slice type based on the series DType.
func (s *Series) Values() interface{} {
	switch s.dtype {
	case Float64:
		return s.ToFloat64()
	case Float32:
		return s.ToFloat32()
	case Int64:
		return s.ToInt64()
	case Int32:
		return s.ToInt32()
	case UInt64:
		return s.ToUInt64()
	case UInt32:
		return s.ToUInt32()
	default:
		return nil
	}
}

// ToFloat32 copies all data to a Go float32 slice.
func (s *Series) ToFloat32() []float32 {
	if s.handle == nil || s.length == 0 {
		return []float32{}
	}

	result := make([]float32, s.length)
	C.galleon_series_copy_f32(
		(*C.ManagedArrowArray)(s.handle),
		(*C.float)(unsafe.Pointer(&result[0])),
		C.size_t(len(result)),
	)
	return result
}

// ToInt32 copies all data to a Go int32 slice.
func (s *Series) ToInt32() []int32 {
	if s.handle == nil || s.length == 0 {
		return []int32{}
	}

	result := make([]int32, s.length)
	C.galleon_series_copy_i32(
		(*C.ManagedArrowArray)(s.handle),
		(*C.int32_t)(unsafe.Pointer(&result[0])),
		C.size_t(len(result)),
	)
	return result
}

// ToUInt64 copies all data to a Go uint64 slice.
func (s *Series) ToUInt64() []uint64 {
	if s.handle == nil || s.length == 0 {
		return []uint64{}
	}

	result := make([]uint64, s.length)
	C.galleon_series_copy_u64(
		(*C.ManagedArrowArray)(s.handle),
		(*C.uint64_t)(unsafe.Pointer(&result[0])),
		C.size_t(len(result)),
	)
	return result
}

// ToUInt32 copies all data to a Go uint32 slice.
func (s *Series) ToUInt32() []uint32 {
	if s.handle == nil || s.length == 0 {
		return []uint32{}
	}

	result := make([]uint32, s.length)
	C.galleon_series_copy_u32(
		(*C.ManagedArrowArray)(s.handle),
		(*C.uint32_t)(unsafe.Pointer(&result[0])),
		C.size_t(len(result)),
	)
	return result
}

// Head returns a new Series with the first n elements.
func (s *Series) Head(n int) *Series {
	if n < 0 {
		n = 0
	}
	if n > s.length {
		n = s.length
	}
	return s.Slice(0, n)
}

// Tail returns a new Series with the last n elements.
func (s *Series) Tail(n int) *Series {
	if n < 0 {
		n = 0
	}
	if n > s.length {
		n = s.length
	}
	return s.Slice(s.length-n, s.length)
}

// --- Filter Operations ---

// extractMask extracts a boolean mask from a C SeriesFilterResult
func extractMask(result *C.SeriesFilterResult) []bool {
	if result == nil {
		return nil
	}
	defer C.galleon_series_filter_result_destroy(result)

	length := int(C.galleon_series_filter_result_len(result))
	if length == 0 {
		return []bool{}
	}

	maskPtr := C.galleon_series_filter_result_mask(result)
	mask := make([]bool, length)
	for i := 0; i < length; i++ {
		mask[i] = bool(*(*C.bool)(unsafe.Pointer(uintptr(unsafe.Pointer(maskPtr)) + uintptr(i))))
	}
	return mask
}

// GtF64 returns a boolean mask where values are greater than the given value.
// For Int64 series, use GtI64.
func (s *Series) GtF64(value float64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_gt_f64((*C.ManagedArrowArray)(s.handle), C.double(value))
	return extractMask(result)
}

// GeF64 returns a boolean mask where values are greater than or equal to the given value.
func (s *Series) GeF64(value float64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_ge_f64((*C.ManagedArrowArray)(s.handle), C.double(value))
	return extractMask(result)
}

// LtF64 returns a boolean mask where values are less than the given value.
func (s *Series) LtF64(value float64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_lt_f64((*C.ManagedArrowArray)(s.handle), C.double(value))
	return extractMask(result)
}

// LeF64 returns a boolean mask where values are less than or equal to the given value.
func (s *Series) LeF64(value float64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_le_f64((*C.ManagedArrowArray)(s.handle), C.double(value))
	return extractMask(result)
}

// EqF64 returns a boolean mask where values are equal to the given value.
func (s *Series) EqF64(value float64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_eq_f64((*C.ManagedArrowArray)(s.handle), C.double(value))
	return extractMask(result)
}

// NeF64 returns a boolean mask where values are not equal to the given value.
func (s *Series) NeF64(value float64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_ne_f64((*C.ManagedArrowArray)(s.handle), C.double(value))
	return extractMask(result)
}

// GtI64 returns a boolean mask where values are greater than the given value.
func (s *Series) GtI64(value int64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_gt_i64((*C.ManagedArrowArray)(s.handle), C.int64_t(value))
	return extractMask(result)
}

// GeI64 returns a boolean mask where values are greater than or equal to the given value.
func (s *Series) GeI64(value int64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_ge_i64((*C.ManagedArrowArray)(s.handle), C.int64_t(value))
	return extractMask(result)
}

// LtI64 returns a boolean mask where values are less than the given value.
func (s *Series) LtI64(value int64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_lt_i64((*C.ManagedArrowArray)(s.handle), C.int64_t(value))
	return extractMask(result)
}

// LeI64 returns a boolean mask where values are less than or equal to the given value.
func (s *Series) LeI64(value int64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_le_i64((*C.ManagedArrowArray)(s.handle), C.int64_t(value))
	return extractMask(result)
}

// EqI64 returns a boolean mask where values are equal to the given value.
func (s *Series) EqI64(value int64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_eq_i64((*C.ManagedArrowArray)(s.handle), C.int64_t(value))
	return extractMask(result)
}

// NeI64 returns a boolean mask where values are not equal to the given value.
func (s *Series) NeI64(value int64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_ne_i64((*C.ManagedArrowArray)(s.handle), C.int64_t(value))
	return extractMask(result)
}

// GtF32 returns a boolean mask where values are greater than the given value.
func (s *Series) GtF32(value float32) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_gt_f32((*C.ManagedArrowArray)(s.handle), C.float(value))
	return extractMask(result)
}

// EqF32 returns a boolean mask where values are equal to the given value.
func (s *Series) EqF32(value float32) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_eq_f32((*C.ManagedArrowArray)(s.handle), C.float(value))
	return extractMask(result)
}

// GtI32 returns a boolean mask where values are greater than the given value.
func (s *Series) GtI32(value int32) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_gt_i32((*C.ManagedArrowArray)(s.handle), C.int32_t(value))
	return extractMask(result)
}

// EqI32 returns a boolean mask where values are equal to the given value.
func (s *Series) EqI32(value int32) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_eq_i32((*C.ManagedArrowArray)(s.handle), C.int32_t(value))
	return extractMask(result)
}

// GtU64 returns a boolean mask where values are greater than the given value.
func (s *Series) GtU64(value uint64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_gt_u64((*C.ManagedArrowArray)(s.handle), C.uint64_t(value))
	return extractMask(result)
}

// EqU64 returns a boolean mask where values are equal to the given value.
func (s *Series) EqU64(value uint64) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_eq_u64((*C.ManagedArrowArray)(s.handle), C.uint64_t(value))
	return extractMask(result)
}

// GtU32 returns a boolean mask where values are greater than the given value.
func (s *Series) GtU32(value uint32) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_gt_u32((*C.ManagedArrowArray)(s.handle), C.uint32_t(value))
	return extractMask(result)
}

// EqU32 returns a boolean mask where values are equal to the given value.
func (s *Series) EqU32(value uint32) []bool {
	if s.handle == nil || s.length == 0 {
		return []bool{}
	}
	result := C.galleon_series_eq_u32((*C.ManagedArrowArray)(s.handle), C.uint32_t(value))
	return extractMask(result)
}

// Filter returns a new Series with only elements where mask is true.
func (s *Series) Filter(mask []bool) *Series {
	if s.handle == nil || s.length == 0 || len(mask) == 0 {
		return &Series{
			handle: nil,
			name:   s.name,
			dtype:  s.dtype,
			length: 0,
		}
	}

	// Convert Go bool slice to C bool array
	cMask := make([]C.bool, len(mask))
	for i, v := range mask {
		cMask[i] = C.bool(v)
	}

	var newHandle *C.ManagedArrowArray
	switch s.dtype {
	case Float64:
		newHandle = C.galleon_series_filter_f64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.bool)(unsafe.Pointer(&cMask[0])),
			C.size_t(len(cMask)),
		)
	case Int64:
		newHandle = C.galleon_series_filter_i64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.bool)(unsafe.Pointer(&cMask[0])),
			C.size_t(len(cMask)),
		)
	default:
		return nil
	}

	if newHandle == nil {
		return nil
	}

	// Count true values to get new length
	newLen := 0
	for _, v := range mask[:min(len(mask), s.length)] {
		if v {
			newLen++
		}
	}

	filtered := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: newLen,
	}

	runtime.SetFinalizer(filtered, func(s *Series) {
		s.Release()
	})

	return filtered
}

// Where is an alias for Filter - returns elements where mask is true.
func (s *Series) Where(mask []bool) *Series {
	return s.Filter(mask)
}

// CountMask returns the number of true values in the mask.
func CountMask(mask []bool) int {
	count := 0
	for _, v := range mask {
		if v {
			count++
		}
	}
	return count
}

// ============================================================================
// Arithmetic Operations
// ============================================================================

// Add performs element-wise addition of two Series.
// Both series must have the same length and type.
// Null values propagate: if either operand is null, the result is null.
func (s *Series) Add(other *Series) *Series {
	if s.length != other.length {
		return nil
	}
	if s.dtype != other.dtype {
		return nil
	}

	var newHandle *C.ManagedArrowArray
	switch s.dtype {
	case Float64:
		newHandle = C.galleon_series_add_f64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.ManagedArrowArray)(other.handle),
		)
	case Int64:
		newHandle = C.galleon_series_add_i64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.ManagedArrowArray)(other.handle),
		)
	default:
		return nil
	}

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// Sub performs element-wise subtraction of two Series.
// Both series must have the same length and type.
// Null values propagate: if either operand is null, the result is null.
func (s *Series) Sub(other *Series) *Series {
	if s.length != other.length {
		return nil
	}
	if s.dtype != other.dtype {
		return nil
	}

	var newHandle *C.ManagedArrowArray
	switch s.dtype {
	case Float64:
		newHandle = C.galleon_series_sub_f64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.ManagedArrowArray)(other.handle),
		)
	case Int64:
		newHandle = C.galleon_series_sub_i64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.ManagedArrowArray)(other.handle),
		)
	default:
		return nil
	}

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// Mul performs element-wise multiplication of two Series.
// Both series must have the same length and type.
// Null values propagate: if either operand is null, the result is null.
func (s *Series) Mul(other *Series) *Series {
	if s.length != other.length {
		return nil
	}
	if s.dtype != other.dtype {
		return nil
	}

	var newHandle *C.ManagedArrowArray
	switch s.dtype {
	case Float64:
		newHandle = C.galleon_series_mul_f64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.ManagedArrowArray)(other.handle),
		)
	case Int64:
		newHandle = C.galleon_series_mul_i64(
			(*C.ManagedArrowArray)(s.handle),
			(*C.ManagedArrowArray)(other.handle),
		)
	default:
		return nil
	}

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// Div performs element-wise division of two Series.
// Both series must be Float64 type.
// Null values propagate: if either operand is null, the result is null.
// Division by zero produces Inf or NaN as per IEEE 754.
func (s *Series) Div(other *Series) *Series {
	if s.length != other.length {
		return nil
	}
	if s.dtype != Float64 || other.dtype != Float64 {
		return nil // Division only supported for Float64
	}

	newHandle := C.galleon_series_div_f64(
		(*C.ManagedArrowArray)(s.handle),
		(*C.ManagedArrowArray)(other.handle),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// AddScalar adds a scalar value to each element in the series.
// Null values remain null.
func (s *Series) AddScalar(value float64) *Series {
	if s.dtype != Float64 {
		return nil
	}

	newHandle := C.galleon_series_add_scalar_f64(
		(*C.ManagedArrowArray)(s.handle),
		C.double(value),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// SubScalar subtracts a scalar value from each element in the series.
// Null values remain null.
func (s *Series) SubScalar(value float64) *Series {
	if s.dtype != Float64 {
		return nil
	}

	newHandle := C.galleon_series_sub_scalar_f64(
		(*C.ManagedArrowArray)(s.handle),
		C.double(value),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// MulScalar multiplies each element in the series by a scalar value.
// Null values remain null.
func (s *Series) MulScalar(value float64) *Series {
	if s.dtype != Float64 {
		return nil
	}

	newHandle := C.galleon_series_mul_scalar_f64(
		(*C.ManagedArrowArray)(s.handle),
		C.double(value),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// DivScalar divides each element in the series by a scalar value.
// Null values remain null.
// Division by zero produces Inf or NaN as per IEEE 754.
func (s *Series) DivScalar(value float64) *Series {
	if s.dtype != Float64 {
		return nil
	}

	newHandle := C.galleon_series_div_scalar_f64(
		(*C.ManagedArrowArray)(s.handle),
		C.double(value),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// AddScalarI64 adds a scalar value to each element in an Int64 series.
// Null values remain null.
func (s *Series) AddScalarI64(value int64) *Series {
	if s.dtype != Int64 {
		return nil
	}

	newHandle := C.galleon_series_add_scalar_i64(
		(*C.ManagedArrowArray)(s.handle),
		C.int64_t(value),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

// MulScalarI64 multiplies each element in an Int64 series by a scalar value.
// Null values remain null.
func (s *Series) MulScalarI64(value int64) *Series {
	if s.dtype != Int64 {
		return nil
	}

	newHandle := C.galleon_series_mul_scalar_i64(
		(*C.ManagedArrowArray)(s.handle),
		C.int64_t(value),
	)

	if newHandle == nil {
		return nil
	}

	result := &Series{
		handle: unsafe.Pointer(newHandle),
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	runtime.SetFinalizer(result, func(s *Series) {
		s.Release()
	})

	return result
}

