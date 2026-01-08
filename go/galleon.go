package galleon

// CGO directives are in platform-specific files:
// - cgo_dev.go: Local development (build with -tags dev)
// - cgo_darwin_arm64.go, cgo_darwin_amd64.go: macOS prebuilt
// - cgo_linux_amd64.go, cgo_linux_arm64.go: Linux prebuilt
// - cgo_windows_amd64.go: Windows prebuilt

/*
#include "galleon.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// ============================================================================
// Thread Configuration
// ============================================================================

// SetMaxThreads sets the maximum number of threads to use for parallel operations.
// Pass 0 to use auto-detection based on CPU count (default).
func SetMaxThreads(maxThreads int) {
	C.galleon_set_max_threads(C.size_t(maxThreads))
}

// GetMaxThreads returns the current effective maximum thread count.
func GetMaxThreads() int {
	return int(C.galleon_get_max_threads())
}

// IsThreadsAutoDetected returns true if thread count was auto-detected.
func IsThreadsAutoDetected() bool {
	return bool(C.galleon_is_threads_auto_detected())
}

// ThreadConfig holds thread configuration information
type ThreadConfig struct {
	MaxThreads   int
	AutoDetected bool
}

// GetThreadConfig returns the current thread configuration.
func GetThreadConfig() ThreadConfig {
	return ThreadConfig{
		MaxThreads:   GetMaxThreads(),
		AutoDetected: IsThreadsAutoDetected(),
	}
}

// ============================================================================
// Column Types
// ============================================================================

// ColumnF64 represents a column of float64 values backed by Zig SIMD storage
type ColumnF64 struct {
	ptr *C.ColumnF64
}

// NewColumnF64 creates a new column from a slice of float64 values
func NewColumnF64(data []float64) *ColumnF64 {
	if len(data) == 0 {
		return nil
	}

	ptr := C.galleon_column_f64_create(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	col := &ColumnF64{ptr: ptr}
	runtime.SetFinalizer(col, (*ColumnF64).free)
	return col
}

// free releases the underlying Zig memory
func (c *ColumnF64) free() {
	if c.ptr != nil {
		C.galleon_column_f64_destroy(c.ptr)
		c.ptr = nil
	}
}

// Len returns the number of elements in the column
func (c *ColumnF64) Len() int {
	if c.ptr == nil {
		return 0
	}
	return int(C.galleon_column_f64_len(c.ptr))
}

// Get returns the value at the given index
func (c *ColumnF64) Get(index int) float64 {
	if c.ptr == nil {
		return 0
	}
	return float64(C.galleon_column_f64_get(c.ptr, C.size_t(index)))
}

// Data returns a slice view of the underlying data (read-only, no copy)
func (c *ColumnF64) Data() []float64 {
	if c.ptr == nil {
		return nil
	}
	ptr := C.galleon_column_f64_data(c.ptr)
	length := c.Len()
	return unsafe.Slice((*float64)(unsafe.Pointer(ptr)), length)
}

// Sum returns the sum of all values using SIMD
func (c *ColumnF64) Sum() float64 {
	data := c.Data()
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_sum_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// Min returns the minimum value using SIMD
func (c *ColumnF64) Min() float64 {
	data := c.Data()
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_min_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// Max returns the maximum value using SIMD
func (c *ColumnF64) Max() float64 {
	data := c.Data()
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_max_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// Mean returns the arithmetic mean using SIMD
func (c *ColumnF64) Mean() float64 {
	data := c.Data()
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_mean_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// SumF64 computes the sum of a float64 slice using SIMD
func SumF64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_sum_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MinF64 finds the minimum value in a float64 slice using SIMD
func MinF64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_min_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MaxF64 finds the maximum value in a float64 slice using SIMD
func MaxF64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_max_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MeanF64 computes the mean of a float64 slice using SIMD
func MeanF64(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	return float64(C.galleon_mean_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// AddScalarF64 adds a scalar to every element in place using SIMD
func AddScalarF64(data []float64, scalar float64) {
	if len(data) == 0 {
		return
	}
	C.galleon_add_scalar_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(scalar),
	)
}

// MulScalarF64 multiplies every element by a scalar in place using SIMD
func MulScalarF64(data []float64, scalar float64) {
	if len(data) == 0 {
		return
	}
	C.galleon_mul_scalar_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(scalar),
	)
}

// ============================================================================
// Vector Arithmetic Operations (SIMD)
// ============================================================================

// AddF64 adds two arrays element-wise: out = a + b
func AddF64(a, b, out []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_add_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// SubF64 subtracts two arrays element-wise: out = a - b
func SubF64(a, b, out []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_sub_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// MulF64 multiplies two arrays element-wise: out = a * b
func MulF64(a, b, out []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_mul_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// DivF64 divides two arrays element-wise: out = a / b
func DivF64(a, b, out []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_div_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// AddI64 adds two arrays element-wise: out = a + b
func AddI64(a, b, out []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_add_i64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// SubI64 subtracts two arrays element-wise: out = a - b
func SubI64(a, b, out []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_sub_i64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// MulI64 multiplies two arrays element-wise: out = a * b
func MulI64(a, b, out []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_mul_i64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// ============================================================================
// Array Comparison Operations (SIMD)
// ============================================================================

// CmpGtF64 compares two arrays: out[i] = 1 if a[i] > b[i], else 0
func CmpGtF64(a, b []float64, out []byte) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_cmp_gt_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// CmpGeF64 compares two arrays: out[i] = 1 if a[i] >= b[i], else 0
func CmpGeF64(a, b []float64, out []byte) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_cmp_ge_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// CmpLtF64 compares two arrays: out[i] = 1 if a[i] < b[i], else 0
func CmpLtF64(a, b []float64, out []byte) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_cmp_lt_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// CmpLeF64 compares two arrays: out[i] = 1 if a[i] <= b[i], else 0
func CmpLeF64(a, b []float64, out []byte) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_cmp_le_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// CmpEqF64 compares two arrays: out[i] = 1 if a[i] == b[i], else 0
func CmpEqF64(a, b []float64, out []byte) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_cmp_eq_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// CmpNeF64 compares two arrays: out[i] = 1 if a[i] != b[i], else 0
func CmpNeF64(a, b []float64, out []byte) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.galleon_cmp_ne_f64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// ============================================================================
// Mask to Indices Operations (SIMD)
// ============================================================================

// CountMaskTrue counts the number of non-zero values in a byte mask
func CountMaskTrue(mask []byte) int {
	if len(mask) == 0 {
		return 0
	}
	return int(C.galleon_count_mask_true(
		(*C.uint8_t)(unsafe.Pointer(&mask[0])),
		C.size_t(len(mask)),
	))
}

// IndicesFromMask extracts indices where mask[i] != 0 into a pre-allocated slice
// Returns the number of indices written
func IndicesFromMask(mask []byte, outIndices []uint32) int {
	if len(mask) == 0 || len(outIndices) == 0 {
		return 0
	}
	return int(C.galleon_indices_from_mask(
		(*C.uint8_t)(unsafe.Pointer(&mask[0])),
		C.size_t(len(mask)),
		(*C.uint32_t)(unsafe.Pointer(&outIndices[0])),
		C.size_t(len(outIndices)),
	))
}

// FilterGreaterThanF64 returns indices of elements greater than threshold
func FilterGreaterThanF64(data []float64, threshold float64) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))
	var count C.size_t

	C.galleon_filter_gt_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(threshold),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		&count,
	)

	return indices[:count]
}

// FilterMaskGreaterThanF64 returns a boolean mask where true indicates value > threshold
// This is faster than FilterGreaterThanF64 as it doesn't extract indices
func FilterMaskGreaterThanF64(data []float64, threshold float64) []bool {
	if len(data) == 0 {
		return nil
	}

	mask := make([]bool, len(data))

	C.galleon_filter_mask_gt_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(threshold),
		(*C.bool)(unsafe.Pointer(&mask[0])),
	)

	return mask
}

// FilterMaskGreaterThanF64Pooled returns a pooled boolean mask where true indicates value > threshold
// Call Release() on the returned BoolMask when done to return it to the pool
// This avoids allocation overhead for repeated filter operations
func FilterMaskGreaterThanF64Pooled(data []float64, threshold float64) *BoolMask {
	if len(data) == 0 {
		return nil
	}

	mask := getBoolMask(len(data))

	C.galleon_filter_mask_gt_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(threshold),
		(*C.bool)(unsafe.Pointer(&mask.Data[0])),
	)

	return mask
}

// FilterGreaterThanF64Pooled returns pooled indices of elements greater than threshold
// Call Release() on the returned Uint32Slice when done to return it to the pool
func FilterGreaterThanF64Pooled(data []float64, threshold float64) *Uint32Slice {
	if len(data) == 0 {
		return nil
	}

	indices := getUint32Slice(len(data))
	var count C.size_t

	C.galleon_filter_gt_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(threshold),
		(*C.uint32_t)(unsafe.Pointer(&indices.Data[0])),
		&count,
	)

	indices.Data = indices.Data[:count]
	return indices
}

// FilterMaskGreaterThanF64Into writes filter results into a pre-allocated mask
// Returns the same slice for convenience. No allocation occurs.
func FilterMaskGreaterThanF64Into(data []float64, threshold float64, mask []bool) []bool {
	if len(data) == 0 || len(mask) < len(data) {
		return mask
	}

	C.galleon_filter_mask_gt_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(threshold),
		(*C.bool)(unsafe.Pointer(&mask[0])),
	)

	return mask[:len(data)]
}

// FilterMaskU8GreaterThanF64Into writes filter results as u8 (0/1) into a pre-allocated slice
// This is the fastest filter variant - uses optimized SIMD u8 writes
// Returns the same slice for convenience. No allocation occurs.
func FilterMaskU8GreaterThanF64Into(data []float64, threshold float64, mask []byte) []byte {
	if len(data) == 0 || len(mask) < len(data) {
		return mask
	}

	C.galleon_filter_mask_u8_gt_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.double(threshold),
		(*C.uint8_t)(unsafe.Pointer(&mask[0])),
	)

	return mask[:len(data)]
}

// ArgsortF64 returns indices that would sort the slice
func ArgsortF64(data []float64, ascending bool) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))

	C.galleon_argsort_f64(
		(*C.double)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		C.bool(ascending),
	)

	return indices
}

// ============================================================================
// Int64 Operations
// ============================================================================

// ColumnI64 represents a column of int64 values backed by Zig SIMD storage
type ColumnI64 struct {
	ptr *C.ColumnI64
}

// NewColumnI64 creates a new column from a slice of int64 values
func NewColumnI64(data []int64) *ColumnI64 {
	if len(data) == 0 {
		return nil
	}

	ptr := C.galleon_column_i64_create(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	col := &ColumnI64{ptr: ptr}
	runtime.SetFinalizer(col, (*ColumnI64).free)
	return col
}

func (c *ColumnI64) free() {
	if c.ptr != nil {
		C.galleon_column_i64_destroy(c.ptr)
		c.ptr = nil
	}
}

func (c *ColumnI64) Len() int {
	if c.ptr == nil {
		return 0
	}
	return int(C.galleon_column_i64_len(c.ptr))
}

func (c *ColumnI64) Get(index int) int64 {
	if c.ptr == nil {
		return 0
	}
	return int64(C.galleon_column_i64_get(c.ptr, C.size_t(index)))
}

func (c *ColumnI64) Data() []int64 {
	if c.ptr == nil {
		return nil
	}
	ptr := C.galleon_column_i64_data(c.ptr)
	length := c.Len()
	return unsafe.Slice((*int64)(unsafe.Pointer(ptr)), length)
}

// SumI64 computes the sum of an int64 slice using SIMD
func SumI64(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	return int64(C.galleon_sum_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MinI64 finds the minimum value in an int64 slice using SIMD
func MinI64(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	return int64(C.galleon_min_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MaxI64 finds the maximum value in an int64 slice using SIMD
func MaxI64(data []int64) int64 {
	if len(data) == 0 {
		return 0
	}
	return int64(C.galleon_max_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// AddScalarI64 adds a scalar to every element in place using SIMD
func AddScalarI64(data []int64, scalar int64) {
	if len(data) == 0 {
		return
	}
	C.galleon_add_scalar_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int64_t(scalar),
	)
}

// MulScalarI64 multiplies every element by a scalar in place using SIMD
func MulScalarI64(data []int64, scalar int64) {
	if len(data) == 0 {
		return
	}
	C.galleon_mul_scalar_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int64_t(scalar),
	)
}

// FilterGreaterThanI64 returns indices of elements greater than threshold
func FilterGreaterThanI64(data []int64, threshold int64) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))
	var count C.size_t

	C.galleon_filter_gt_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int64_t(threshold),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		&count,
	)

	return indices[:count]
}

// ArgsortI64 returns indices that would sort the slice
func ArgsortI64(data []int64, ascending bool) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))

	C.galleon_argsort_i64(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		C.bool(ascending),
	)

	return indices
}

// ============================================================================
// Int32 Operations
// ============================================================================

// ColumnI32 represents a column of int32 values backed by Zig SIMD storage
type ColumnI32 struct {
	ptr *C.ColumnI32
}

// NewColumnI32 creates a new column from a slice of int32 values
func NewColumnI32(data []int32) *ColumnI32 {
	if len(data) == 0 {
		return nil
	}

	ptr := C.galleon_column_i32_create(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	col := &ColumnI32{ptr: ptr}
	runtime.SetFinalizer(col, (*ColumnI32).free)
	return col
}

func (c *ColumnI32) free() {
	if c.ptr != nil {
		C.galleon_column_i32_destroy(c.ptr)
		c.ptr = nil
	}
}

func (c *ColumnI32) Len() int {
	if c.ptr == nil {
		return 0
	}
	return int(C.galleon_column_i32_len(c.ptr))
}

func (c *ColumnI32) Get(index int) int32 {
	if c.ptr == nil {
		return 0
	}
	return int32(C.galleon_column_i32_get(c.ptr, C.size_t(index)))
}

func (c *ColumnI32) Data() []int32 {
	if c.ptr == nil {
		return nil
	}
	ptr := C.galleon_column_i32_data(c.ptr)
	length := c.Len()
	return unsafe.Slice((*int32)(unsafe.Pointer(ptr)), length)
}

// SumI32 computes the sum of an int32 slice using SIMD
func SumI32(data []int32) int32 {
	if len(data) == 0 {
		return 0
	}
	return int32(C.galleon_sum_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MinI32 finds the minimum value in an int32 slice using SIMD
func MinI32(data []int32) int32 {
	if len(data) == 0 {
		return 0
	}
	return int32(C.galleon_min_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MaxI32 finds the maximum value in an int32 slice using SIMD
func MaxI32(data []int32) int32 {
	if len(data) == 0 {
		return 0
	}
	return int32(C.galleon_max_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// AddScalarI32 adds a scalar to every element in place using SIMD
func AddScalarI32(data []int32, scalar int32) {
	if len(data) == 0 {
		return
	}
	C.galleon_add_scalar_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int32_t(scalar),
	)
}

// MulScalarI32 multiplies every element by a scalar in place using SIMD
func MulScalarI32(data []int32, scalar int32) {
	if len(data) == 0 {
		return
	}
	C.galleon_mul_scalar_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int32_t(scalar),
	)
}

// FilterGreaterThanI32 returns indices of elements greater than threshold
func FilterGreaterThanI32(data []int32, threshold int32) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))
	var count C.size_t

	C.galleon_filter_gt_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.int32_t(threshold),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		&count,
	)

	return indices[:count]
}

// ArgsortI32 returns indices that would sort the slice
func ArgsortI32(data []int32, ascending bool) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))

	C.galleon_argsort_i32(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		C.bool(ascending),
	)

	return indices
}

// ============================================================================
// Float32 Operations
// ============================================================================

// ColumnF32 represents a column of float32 values backed by Zig SIMD storage
type ColumnF32 struct {
	ptr *C.ColumnF32
}

// NewColumnF32 creates a new column from a slice of float32 values
func NewColumnF32(data []float32) *ColumnF32 {
	if len(data) == 0 {
		return nil
	}

	ptr := C.galleon_column_f32_create(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	col := &ColumnF32{ptr: ptr}
	runtime.SetFinalizer(col, (*ColumnF32).free)
	return col
}

func (c *ColumnF32) free() {
	if c.ptr != nil {
		C.galleon_column_f32_destroy(c.ptr)
		c.ptr = nil
	}
}

func (c *ColumnF32) Len() int {
	if c.ptr == nil {
		return 0
	}
	return int(C.galleon_column_f32_len(c.ptr))
}

func (c *ColumnF32) Get(index int) float32 {
	if c.ptr == nil {
		return 0
	}
	return float32(C.galleon_column_f32_get(c.ptr, C.size_t(index)))
}

func (c *ColumnF32) Data() []float32 {
	if c.ptr == nil {
		return nil
	}
	ptr := C.galleon_column_f32_data(c.ptr)
	length := c.Len()
	return unsafe.Slice((*float32)(unsafe.Pointer(ptr)), length)
}

// SumF32 computes the sum of a float32 slice using SIMD
func SumF32(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}
	return float32(C.galleon_sum_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MinF32 finds the minimum value in a float32 slice using SIMD
func MinF32(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}
	return float32(C.galleon_min_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MaxF32 finds the maximum value in a float32 slice using SIMD
func MaxF32(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}
	return float32(C.galleon_max_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// MeanF32 computes the mean of a float32 slice using SIMD
func MeanF32(data []float32) float32 {
	if len(data) == 0 {
		return 0
	}
	return float32(C.galleon_mean_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// AddScalarF32 adds a scalar to every element in place using SIMD
func AddScalarF32(data []float32, scalar float32) {
	if len(data) == 0 {
		return
	}
	C.galleon_add_scalar_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.float(scalar),
	)
}

// MulScalarF32 multiplies every element by a scalar in place using SIMD
func MulScalarF32(data []float32, scalar float32) {
	if len(data) == 0 {
		return
	}
	C.galleon_mul_scalar_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.float(scalar),
	)
}

// FilterGreaterThanF32 returns indices of elements greater than threshold
func FilterGreaterThanF32(data []float32, threshold float32) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))
	var count C.size_t

	C.galleon_filter_gt_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		C.float(threshold),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		&count,
	)

	return indices[:count]
}

// ArgsortF32 returns indices that would sort the slice
func ArgsortF32(data []float32, ascending bool) []uint32 {
	if len(data) == 0 {
		return nil
	}

	indices := make([]uint32, len(data))

	C.galleon_argsort_f32(
		(*C.float)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		C.bool(ascending),
	)

	return indices
}

// ============================================================================
// Bool Operations
// ============================================================================

// ColumnBool represents a column of bool values backed by Zig storage
type ColumnBool struct {
	ptr *C.ColumnBool
}

// NewColumnBool creates a new column from a slice of bool values
func NewColumnBool(data []bool) *ColumnBool {
	if len(data) == 0 {
		return nil
	}

	ptr := C.galleon_column_bool_create(
		(*C.bool)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	if ptr == nil {
		return nil
	}

	col := &ColumnBool{ptr: ptr}
	runtime.SetFinalizer(col, (*ColumnBool).free)
	return col
}

func (c *ColumnBool) free() {
	if c.ptr != nil {
		C.galleon_column_bool_destroy(c.ptr)
		c.ptr = nil
	}
}

func (c *ColumnBool) Len() int {
	if c.ptr == nil {
		return 0
	}
	return int(C.galleon_column_bool_len(c.ptr))
}

func (c *ColumnBool) Get(index int) bool {
	if c.ptr == nil {
		return false
	}
	return bool(C.galleon_column_bool_get(c.ptr, C.size_t(index)))
}

func (c *ColumnBool) Data() []bool {
	if c.ptr == nil {
		return nil
	}
	ptr := C.galleon_column_bool_data(c.ptr)
	length := c.Len()
	return unsafe.Slice((*bool)(unsafe.Pointer(ptr)), length)
}

// CountTrue counts the number of true values in a bool slice
func CountTrue(data []bool) int {
	if len(data) == 0 {
		return 0
	}
	return int(C.galleon_count_true(
		(*C.bool)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// CountFalse counts the number of false values in a bool slice
func CountFalse(data []bool) int {
	if len(data) == 0 {
		return 0
	}
	return int(C.galleon_count_false(
		(*C.bool)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	))
}

// ============================================================================
// GroupBy Aggregation Functions (Zig SIMD Backend)
// ============================================================================

// AggregateSumF64ByGroup computes sum by group using SIMD
// data: source values
// groupIDs: group index for each row (0 to numGroups-1)
// outSums: pre-allocated output array of size numGroups (must be zero-initialized)
func AggregateSumF64ByGroup(data []float64, groupIDs []uint32, outSums []float64) {
	if len(data) == 0 || len(groupIDs) == 0 || len(outSums) == 0 {
		return
	}
	C.galleon_aggregate_sum_f64_by_group(
		(*C.double)(unsafe.Pointer(&data[0])),
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.double)(unsafe.Pointer(&outSums[0])),
		C.size_t(len(data)),
		C.size_t(len(outSums)),
	)
}

// AggregateSumI64ByGroup computes sum by group using SIMD for int64
func AggregateSumI64ByGroup(data []int64, groupIDs []uint32, outSums []int64) {
	if len(data) == 0 || len(groupIDs) == 0 || len(outSums) == 0 {
		return
	}
	C.galleon_aggregate_sum_i64_by_group(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.int64_t)(unsafe.Pointer(&outSums[0])),
		C.size_t(len(data)),
		C.size_t(len(outSums)),
	)
}

// AggregateMinF64ByGroup computes min by group using SIMD
// outMins must be initialized to math.MaxFloat64
func AggregateMinF64ByGroup(data []float64, groupIDs []uint32, outMins []float64) {
	if len(data) == 0 || len(groupIDs) == 0 || len(outMins) == 0 {
		return
	}
	C.galleon_aggregate_min_f64_by_group(
		(*C.double)(unsafe.Pointer(&data[0])),
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.double)(unsafe.Pointer(&outMins[0])),
		C.size_t(len(data)),
		C.size_t(len(outMins)),
	)
}

// AggregateMinI64ByGroup computes min by group using SIMD for int64
func AggregateMinI64ByGroup(data []int64, groupIDs []uint32, outMins []int64) {
	if len(data) == 0 || len(groupIDs) == 0 || len(outMins) == 0 {
		return
	}
	C.galleon_aggregate_min_i64_by_group(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.int64_t)(unsafe.Pointer(&outMins[0])),
		C.size_t(len(data)),
		C.size_t(len(outMins)),
	)
}

// AggregateMaxF64ByGroup computes max by group using SIMD
// outMaxs must be initialized to -math.MaxFloat64
func AggregateMaxF64ByGroup(data []float64, groupIDs []uint32, outMaxs []float64) {
	if len(data) == 0 || len(groupIDs) == 0 || len(outMaxs) == 0 {
		return
	}
	C.galleon_aggregate_max_f64_by_group(
		(*C.double)(unsafe.Pointer(&data[0])),
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.double)(unsafe.Pointer(&outMaxs[0])),
		C.size_t(len(data)),
		C.size_t(len(outMaxs)),
	)
}

// AggregateMaxI64ByGroup computes max by group using SIMD for int64
func AggregateMaxI64ByGroup(data []int64, groupIDs []uint32, outMaxs []int64) {
	if len(data) == 0 || len(groupIDs) == 0 || len(outMaxs) == 0 {
		return
	}
	C.galleon_aggregate_max_i64_by_group(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.int64_t)(unsafe.Pointer(&outMaxs[0])),
		C.size_t(len(data)),
		C.size_t(len(outMaxs)),
	)
}

// CountByGroup counts elements per group
func CountByGroup(groupIDs []uint32, outCounts []uint64) {
	if len(groupIDs) == 0 || len(outCounts) == 0 {
		return
	}
	C.galleon_count_by_group(
		(*C.uint32_t)(unsafe.Pointer(&groupIDs[0])),
		(*C.uint64_t)(unsafe.Pointer(&outCounts[0])),
		C.size_t(len(groupIDs)),
		C.size_t(len(outCounts)),
	)
}

// HashI64Column computes FNV-1a hashes for int64 column (for groupby/join)
func HashI64Column(data []int64, outHashes []uint64) {
	if len(data) == 0 || len(outHashes) == 0 {
		return
	}
	C.galleon_hash_i64_column(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		(*C.uint64_t)(unsafe.Pointer(&outHashes[0])),
		C.size_t(len(data)),
	)
}

// HashI32Column computes FNV-1a hashes for int32 column
func HashI32Column(data []int32, outHashes []uint64) {
	if len(data) == 0 || len(outHashes) == 0 {
		return
	}
	C.galleon_hash_i32_column(
		(*C.int32_t)(unsafe.Pointer(&data[0])),
		(*C.uint64_t)(unsafe.Pointer(&outHashes[0])),
		C.size_t(len(data)),
	)
}

// HashF64Column computes FNV-1a hashes for float64 column
func HashF64Column(data []float64, outHashes []uint64) {
	if len(data) == 0 || len(outHashes) == 0 {
		return
	}
	C.galleon_hash_f64_column(
		(*C.double)(unsafe.Pointer(&data[0])),
		(*C.uint64_t)(unsafe.Pointer(&outHashes[0])),
		C.size_t(len(data)),
	)
}

// HashF32Column computes FNV-1a hashes for float32 column
func HashF32Column(data []float32, outHashes []uint64) {
	if len(data) == 0 || len(outHashes) == 0 {
		return
	}
	C.galleon_hash_f32_column(
		(*C.float)(unsafe.Pointer(&data[0])),
		(*C.uint64_t)(unsafe.Pointer(&outHashes[0])),
		C.size_t(len(data)),
	)
}

// CombineHashes combines two hash arrays (for multi-key groupby/join)
func CombineHashes(hash1, hash2, outHashes []uint64) {
	if len(hash1) == 0 || len(hash2) == 0 || len(outHashes) == 0 {
		return
	}
	C.galleon_combine_hashes(
		(*C.uint64_t)(unsafe.Pointer(&hash1[0])),
		(*C.uint64_t)(unsafe.Pointer(&hash2[0])),
		(*C.uint64_t)(unsafe.Pointer(&outHashes[0])),
		C.size_t(len(outHashes)),
	)
}

// ============================================================================
// Join Helper Functions (Zig SIMD Backend)
// ============================================================================

// GatherF64 gathers values by indices (-1 means null/zero)
func GatherF64(src []float64, indices []int32, dst []float64) {
	if len(src) == 0 || len(indices) == 0 || len(dst) == 0 {
		return
	}
	C.galleon_gather_f64(
		(*C.double)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.int32_t)(unsafe.Pointer(&indices[0])),
		(*C.double)(unsafe.Pointer(&dst[0])),
		C.size_t(len(dst)),
	)
}

// GatherI64 gathers int64 values by indices
func GatherI64(src []int64, indices []int32, dst []int64) {
	if len(src) == 0 || len(indices) == 0 || len(dst) == 0 {
		return
	}
	C.galleon_gather_i64(
		(*C.int64_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.int32_t)(unsafe.Pointer(&indices[0])),
		(*C.int64_t)(unsafe.Pointer(&dst[0])),
		C.size_t(len(dst)),
	)
}

// GatherI32 gathers int32 values by indices
func GatherI32(src []int32, indices []int32, dst []int32) {
	if len(src) == 0 || len(indices) == 0 || len(dst) == 0 {
		return
	}
	C.galleon_gather_i32(
		(*C.int32_t)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.int32_t)(unsafe.Pointer(&indices[0])),
		(*C.int32_t)(unsafe.Pointer(&dst[0])),
		C.size_t(len(dst)),
	)
}

// GatherF32 gathers float32 values by indices
func GatherF32(src []float32, indices []int32, dst []float32) {
	if len(src) == 0 || len(indices) == 0 || len(dst) == 0 {
		return
	}
	C.galleon_gather_f32(
		(*C.float)(unsafe.Pointer(&src[0])),
		C.size_t(len(src)),
		(*C.int32_t)(unsafe.Pointer(&indices[0])),
		(*C.float)(unsafe.Pointer(&dst[0])),
		C.size_t(len(dst)),
	)
}

// BuildJoinHashTable builds a hash table for join operations
// table and next must be pre-allocated (table: size tableSize, next: size len(hashes))
func BuildJoinHashTable(hashes []uint64, table []int32, next []int32, tableSize uint32) {
	if len(hashes) == 0 || len(table) == 0 {
		return
	}
	C.galleon_build_join_hash_table(
		(*C.uint64_t)(unsafe.Pointer(&hashes[0])),
		C.size_t(len(hashes)),
		(*C.int32_t)(unsafe.Pointer(&table[0])),
		(*C.int32_t)(unsafe.Pointer(&next[0])),
		C.uint32_t(tableSize),
	)
}

// ProbeJoinHashTable probes the hash table to find matching rows
// Returns the number of matches found
func ProbeJoinHashTable(
	probeHashes []uint64,
	probeKeys []int64,
	buildKeys []int64,
	table []int32,
	next []int32,
	tableSize uint32,
	outProbeIndices []int32,
	outBuildIndices []int32,
	maxMatches uint32,
) uint32 {
	if len(probeHashes) == 0 || len(table) == 0 {
		return 0
	}
	return uint32(C.galleon_probe_join_hash_table(
		(*C.uint64_t)(unsafe.Pointer(&probeHashes[0])),
		(*C.int64_t)(unsafe.Pointer(&probeKeys[0])),
		C.size_t(len(probeHashes)),
		(*C.int64_t)(unsafe.Pointer(&buildKeys[0])),
		C.size_t(len(buildKeys)),
		(*C.int32_t)(unsafe.Pointer(&table[0])),
		(*C.int32_t)(unsafe.Pointer(&next[0])),
		C.uint32_t(tableSize),
		(*C.int32_t)(unsafe.Pointer(&outProbeIndices[0])),
		(*C.int32_t)(unsafe.Pointer(&outBuildIndices[0])),
		C.uint32_t(maxMatches),
	))
}

// ============================================================================
// GroupBy Hash Table (Full Zig Implementation)
// ============================================================================

// ComputeGroupIDs builds a hash table in Zig and returns group IDs for each row
// This is the core groupby operation - it assigns each row to a group based on hash
// Returns: (groupIDs []uint32, numGroups int)
func ComputeGroupIDs(hashes []uint64) ([]uint32, int) {
	if len(hashes) == 0 {
		return nil, 0
	}

	// Call Zig to build hash table and compute group IDs
	handle := C.galleon_groupby_compute(
		(*C.uint64_t)(unsafe.Pointer(&hashes[0])),
		C.size_t(len(hashes)),
	)
	if handle == nil {
		return nil, 0
	}
	defer C.galleon_groupby_result_destroy(handle)

	numGroups := int(C.galleon_groupby_result_num_groups(handle))
	groupIDsPtr := C.galleon_groupby_result_group_ids(handle)

	// Copy group IDs to Go slice (we need to copy since we're freeing the handle)
	groupIDs := make([]uint32, len(hashes))
	src := unsafe.Slice((*uint32)(unsafe.Pointer(groupIDsPtr)), len(hashes))
	copy(groupIDs, src)

	return groupIDs, numGroups
}

// ComputeGroupIDsWithKeys builds a hash table with key verification (handles collisions)
// keys: actual key values for collision resolution
// Returns: (groupIDs []uint32, numGroups int)
func ComputeGroupIDsWithKeys(hashes []uint64, keys []int64) ([]uint32, int) {
	if len(hashes) == 0 || len(keys) == 0 {
		return nil, 0
	}

	handle := C.galleon_groupby_compute_with_keys_i64(
		(*C.uint64_t)(unsafe.Pointer(&hashes[0])),
		(*C.int64_t)(unsafe.Pointer(&keys[0])),
		C.size_t(len(hashes)),
	)
	if handle == nil {
		return nil, 0
	}
	defer C.galleon_groupby_result_destroy(handle)

	numGroups := int(C.galleon_groupby_result_num_groups(handle))
	groupIDsPtr := C.galleon_groupby_result_group_ids(handle)

	groupIDs := make([]uint32, len(hashes))
	src := unsafe.Slice((*uint32)(unsafe.Pointer(groupIDsPtr)), len(hashes))
	copy(groupIDs, src)

	return groupIDs, numGroups
}

// GroupByResultExt holds extended groupby result with firstRowIdx and groupCounts
// This eliminates the Go loop that was previously needed after ComputeGroupIDs
type GroupByResultExt struct {
	GroupIDs    []uint32 // Group ID for each row
	NumGroups   int      // Number of unique groups
	FirstRowIdx []int    // First row index for each group
	GroupCounts []int    // Count of rows per group
}

// ComputeGroupIDsExt computes group IDs with firstRowIdx and groupCounts in a single CGO call
// This is faster than ComputeGroupIDs + Go loop for building firstRowIdx/groupCounts
func ComputeGroupIDsExt(hashes []uint64) *GroupByResultExt {
	if len(hashes) == 0 {
		return &GroupByResultExt{
			GroupIDs:    nil,
			NumGroups:   0,
			FirstRowIdx: nil,
			GroupCounts: nil,
		}
	}

	handle := C.galleon_groupby_compute_ext(
		(*C.uint64_t)(unsafe.Pointer(&hashes[0])),
		C.size_t(len(hashes)),
	)
	if handle == nil {
		return nil
	}
	defer C.galleon_groupby_result_ext_destroy(handle)

	numGroups := int(C.galleon_groupby_result_ext_num_groups(handle))

	// Get pointers to Zig-allocated arrays
	groupIDsPtr := C.galleon_groupby_result_ext_group_ids(handle)
	firstRowIdxPtr := C.galleon_groupby_result_ext_first_row_idx(handle)
	groupCountsPtr := C.galleon_groupby_result_ext_group_counts(handle)

	// Copy group IDs
	groupIDs := make([]uint32, len(hashes))
	srcGroupIDs := unsafe.Slice((*uint32)(unsafe.Pointer(groupIDsPtr)), len(hashes))
	copy(groupIDs, srcGroupIDs)

	// Copy firstRowIdx (convert from uint32 to int)
	firstRowIdx := make([]int, numGroups)
	srcFirstRow := unsafe.Slice((*uint32)(unsafe.Pointer(firstRowIdxPtr)), numGroups)
	for i, v := range srcFirstRow {
		firstRowIdx[i] = int(v)
	}

	// Copy groupCounts (convert from uint32 to int)
	groupCounts := make([]int, numGroups)
	srcCounts := unsafe.Slice((*uint32)(unsafe.Pointer(groupCountsPtr)), numGroups)
	for i, v := range srcCounts {
		groupCounts[i] = int(v)
	}

	return &GroupByResultExt{
		GroupIDs:    groupIDs,
		NumGroups:   numGroups,
		FirstRowIdx: firstRowIdx,
		GroupCounts: groupCounts,
	}
}

// ============================================================================
// Zero-Copy GroupBy Result (Phase 1 optimization)
// ============================================================================

// GroupByResultZeroCopy holds groupby result with zero-copy access to Zig memory
// The handle is kept alive and freed when this struct is garbage collected
type GroupByResultZeroCopy struct {
	handle      *C.GroupByResultExtHandle
	numRows     int
	numGroups   int
	groupIDs    []uint32 // View into Zig memory (do not modify)
	firstRowIdx []uint32 // View into Zig memory (do not modify)
	groupCounts []uint32 // View into Zig memory (do not modify)
}

// free releases the Zig memory
func (r *GroupByResultZeroCopy) free() {
	if r.handle != nil {
		C.galleon_groupby_result_ext_destroy(r.handle)
		r.handle = nil
	}
}

// NumGroups returns the number of unique groups
func (r *GroupByResultZeroCopy) NumGroups() int {
	return r.numGroups
}

// GroupIDs returns a view of group IDs (one per row)
func (r *GroupByResultZeroCopy) GroupIDs() []uint32 {
	return r.groupIDs
}

// FirstRowIdx returns a view of first row index per group
func (r *GroupByResultZeroCopy) FirstRowIdxU32() []uint32 {
	return r.firstRowIdx
}

// GroupCounts returns a view of row counts per group
func (r *GroupByResultZeroCopy) GroupCountsU32() []uint32 {
	return r.groupCounts
}

// ComputeGroupIDsZeroCopy computes group IDs with zero-copy access to results
// This avoids copying large arrays from Zig to Go
func ComputeGroupIDsZeroCopy(hashes []uint64) *GroupByResultZeroCopy {
	if len(hashes) == 0 {
		return &GroupByResultZeroCopy{
			handle:      nil,
			numRows:     0,
			numGroups:   0,
			groupIDs:    nil,
			firstRowIdx: nil,
			groupCounts: nil,
		}
	}

	handle := C.galleon_groupby_compute_ext(
		(*C.uint64_t)(unsafe.Pointer(&hashes[0])),
		C.size_t(len(hashes)),
	)
	if handle == nil {
		return nil
	}

	numGroups := int(C.galleon_groupby_result_ext_num_groups(handle))

	// Get pointers to Zig-allocated arrays - NO COPY
	groupIDsPtr := C.galleon_groupby_result_ext_group_ids(handle)
	firstRowIdxPtr := C.galleon_groupby_result_ext_first_row_idx(handle)
	groupCountsPtr := C.galleon_groupby_result_ext_group_counts(handle)

	// Create Go slice views pointing to Zig memory
	result := &GroupByResultZeroCopy{
		handle:      handle,
		numRows:     len(hashes),
		numGroups:   numGroups,
		groupIDs:    unsafe.Slice((*uint32)(unsafe.Pointer(groupIDsPtr)), len(hashes)),
		firstRowIdx: unsafe.Slice((*uint32)(unsafe.Pointer(firstRowIdxPtr)), numGroups),
		groupCounts: unsafe.Slice((*uint32)(unsafe.Pointer(groupCountsPtr)), numGroups),
	}

	// Set finalizer to free Zig memory when Go is done
	runtime.SetFinalizer(result, (*GroupByResultZeroCopy).free)

	return result
}

// ============================================================================
// End-to-End GroupBy (Single CGO Call - Phase 2 Optimization)
// ============================================================================

// GroupBySumE2EResult holds the result of end-to-end groupby sum
type GroupBySumE2EResult struct {
	handle    *C.GroupBySumResultHandle
	NumGroups int
	Keys      []int64   // Unique key values (view into Zig memory)
	Sums      []float64 // Sum values (view into Zig memory)
}

func (r *GroupBySumE2EResult) free() {
	if r.handle != nil {
		C.galleon_groupby_sum_result_destroy(r.handle)
		r.handle = nil
	}
}

// GroupBySumE2E performs end-to-end groupby sum in a single CGO call
// This is the fastest path for single int64 key + float64 value groupby sum
func GroupBySumE2E(keyData []int64, valueData []float64) *GroupBySumE2EResult {
	if len(keyData) == 0 || len(valueData) == 0 || len(keyData) != len(valueData) {
		return nil
	}

	handle := C.galleon_groupby_sum_e2e_i64_f64(
		(*C.int64_t)(unsafe.Pointer(&keyData[0])),
		(*C.double)(unsafe.Pointer(&valueData[0])),
		C.size_t(len(keyData)),
	)
	if handle == nil {
		return nil
	}

	numGroups := int(C.galleon_groupby_sum_result_num_groups(handle))
	keysPtr := C.galleon_groupby_sum_result_keys(handle)
	sumsPtr := C.galleon_groupby_sum_result_sums(handle)

	result := &GroupBySumE2EResult{
		handle:    handle,
		NumGroups: numGroups,
		Keys:      unsafe.Slice((*int64)(unsafe.Pointer(keysPtr)), numGroups),
		Sums:      unsafe.Slice((*float64)(unsafe.Pointer(sumsPtr)), numGroups),
	}

	runtime.SetFinalizer(result, (*GroupBySumE2EResult).free)
	return result
}

// GroupByMultiAggE2EResult holds the result of end-to-end groupby with multiple aggregations
type GroupByMultiAggE2EResult struct {
	handle    *C.GroupByMultiAggResultHandle
	NumGroups int
	Keys      []int64   // Unique key values
	Sums      []float64 // Sum values
	Mins      []float64 // Min values
	Maxs      []float64 // Max values
	Counts    []uint64  // Count values
}

func (r *GroupByMultiAggE2EResult) free() {
	if r.handle != nil {
		C.galleon_groupby_multi_agg_result_destroy(r.handle)
		r.handle = nil
	}
}

// GroupByMultiAggE2E performs end-to-end groupby with sum, min, max, count in a single CGO call
func GroupByMultiAggE2E(keyData []int64, valueData []float64) *GroupByMultiAggE2EResult {
	if len(keyData) == 0 || len(valueData) == 0 || len(keyData) != len(valueData) {
		return nil
	}

	handle := C.galleon_groupby_multi_agg_e2e_i64_f64(
		(*C.int64_t)(unsafe.Pointer(&keyData[0])),
		(*C.double)(unsafe.Pointer(&valueData[0])),
		C.size_t(len(keyData)),
	)
	if handle == nil {
		return nil
	}

	numGroups := int(C.galleon_groupby_multi_agg_result_num_groups(handle))
	keysPtr := C.galleon_groupby_multi_agg_result_keys(handle)
	sumsPtr := C.galleon_groupby_multi_agg_result_sums(handle)
	minsPtr := C.galleon_groupby_multi_agg_result_mins(handle)
	maxsPtr := C.galleon_groupby_multi_agg_result_maxs(handle)
	countsPtr := C.galleon_groupby_multi_agg_result_counts(handle)

	result := &GroupByMultiAggE2EResult{
		handle:    handle,
		NumGroups: numGroups,
		Keys:      unsafe.Slice((*int64)(unsafe.Pointer(keysPtr)), numGroups),
		Sums:      unsafe.Slice((*float64)(unsafe.Pointer(sumsPtr)), numGroups),
		Mins:      unsafe.Slice((*float64)(unsafe.Pointer(minsPtr)), numGroups),
		Maxs:      unsafe.Slice((*float64)(unsafe.Pointer(maxsPtr)), numGroups),
		Counts:    unsafe.Slice((*uint64)(unsafe.Pointer(countsPtr)), numGroups),
	}

	runtime.SetFinalizer(result, (*GroupByMultiAggE2EResult).free)
	return result
}

// ============================================================================
// End-to-End Inner Join (Single CGO Call - Phase 3 Optimization)
// ============================================================================

// InnerJoinE2EResult holds the result of end-to-end inner join
type InnerJoinE2EResult struct {
	handle       *C.InnerJoinResultHandle
	NumMatches   int
	LeftIndices  []int32 // Indices into left table for each match
	RightIndices []int32 // Indices into right table for each match
}

func (r *InnerJoinE2EResult) free() {
	if r.handle != nil {
		C.galleon_inner_join_result_destroy(r.handle)
		r.handle = nil
	}
}

// InnerJoinI64E2E performs end-to-end inner join on int64 keys in a single CGO call
// Returns matching row indices for both tables
func InnerJoinI64E2E(leftKeys []int64, rightKeys []int64) *InnerJoinE2EResult {
	if len(leftKeys) == 0 || len(rightKeys) == 0 {
		return nil
	}

	handle := C.galleon_inner_join_e2e_i64(
		(*C.int64_t)(unsafe.Pointer(&leftKeys[0])),
		C.size_t(len(leftKeys)),
		(*C.int64_t)(unsafe.Pointer(&rightKeys[0])),
		C.size_t(len(rightKeys)),
	)
	if handle == nil {
		return nil
	}

	numMatches := int(C.galleon_inner_join_result_num_matches(handle))
	leftIndicesPtr := C.galleon_inner_join_result_left_indices(handle)
	rightIndicesPtr := C.galleon_inner_join_result_right_indices(handle)

	result := &InnerJoinE2EResult{
		handle:       handle,
		NumMatches:   numMatches,
		LeftIndices:  unsafe.Slice((*int32)(unsafe.Pointer(leftIndicesPtr)), numMatches),
		RightIndices: unsafe.Slice((*int32)(unsafe.Pointer(rightIndicesPtr)), numMatches),
	}

	runtime.SetFinalizer(result, (*InnerJoinE2EResult).free)
	return result
}

// ParallelInnerJoinI64 performs multi-threaded inner join on int64 keys
// Uses multiple threads for the probe phase - faster for large datasets
func ParallelInnerJoinI64(leftKeys []int64, rightKeys []int64) *InnerJoinE2EResult {
	if len(leftKeys) == 0 || len(rightKeys) == 0 {
		return nil
	}

	handle := C.galleon_parallel_inner_join_i64(
		(*C.int64_t)(unsafe.Pointer(&leftKeys[0])),
		C.size_t(len(leftKeys)),
		(*C.int64_t)(unsafe.Pointer(&rightKeys[0])),
		C.size_t(len(rightKeys)),
	)
	if handle == nil {
		return nil
	}

	numMatches := int(C.galleon_inner_join_result_num_matches(handle))
	leftIndicesPtr := C.galleon_inner_join_result_left_indices(handle)
	rightIndicesPtr := C.galleon_inner_join_result_right_indices(handle)

	result := &InnerJoinE2EResult{
		handle:       handle,
		NumMatches:   numMatches,
		LeftIndices:  unsafe.Slice((*int32)(unsafe.Pointer(leftIndicesPtr)), numMatches),
		RightIndices: unsafe.Slice((*int32)(unsafe.Pointer(rightIndicesPtr)), numMatches),
	}

	runtime.SetFinalizer(result, (*InnerJoinE2EResult).free)
	return result
}

// InnerJoinI64 performs inner join and returns Go int slices for join result building
func InnerJoinI64(leftKeys []int64, rightKeys []int64) (leftIndices, rightIndices []int) {
	if len(leftKeys) == 0 || len(rightKeys) == 0 {
		return nil, nil
	}

	// Use parallel version for large datasets
	var result *InnerJoinE2EResult
	if len(leftKeys) >= 100000 {
		result = ParallelInnerJoinI64(leftKeys, rightKeys)
	} else {
		result = InnerJoinI64E2E(leftKeys, rightKeys)
	}
	if result == nil {
		return nil, nil
	}

	// Convert int32 to int
	leftIndices = make([]int, result.NumMatches)
	rightIndices = make([]int, result.NumMatches)
	for i := 0; i < result.NumMatches; i++ {
		leftIndices[i] = int(result.LeftIndices[i])
		rightIndices[i] = int(result.RightIndices[i])
	}

	return leftIndices, rightIndices
}

// LeftJoinE2EResult holds the result of end-to-end left join
type LeftJoinE2EResult struct {
	handle       *C.LeftJoinResultHandle
	NumRows      int
	LeftIndices  []int32 // Indices into left table
	RightIndices []int32 // Indices into right table (-1 for unmatched)
}

func (r *LeftJoinE2EResult) free() {
	if r.handle != nil {
		C.galleon_left_join_result_destroy(r.handle)
		r.handle = nil
	}
}

// LeftJoinI64E2E performs end-to-end left join on int64 keys in a single CGO call
func LeftJoinI64E2E(leftKeys []int64, rightKeys []int64) *LeftJoinE2EResult {
	if len(leftKeys) == 0 {
		return nil
	}

	var rightPtr *C.int64_t
	if len(rightKeys) > 0 {
		rightPtr = (*C.int64_t)(unsafe.Pointer(&rightKeys[0]))
	}

	handle := C.galleon_left_join_i64(
		(*C.int64_t)(unsafe.Pointer(&leftKeys[0])),
		C.size_t(len(leftKeys)),
		rightPtr,
		C.size_t(len(rightKeys)),
	)
	if handle == nil {
		return nil
	}

	numRows := int(C.galleon_left_join_result_num_rows(handle))
	leftIndicesPtr := C.galleon_left_join_result_left_indices(handle)
	rightIndicesPtr := C.galleon_left_join_result_right_indices(handle)

	result := &LeftJoinE2EResult{
		handle:       handle,
		NumRows:      numRows,
		LeftIndices:  unsafe.Slice((*int32)(unsafe.Pointer(leftIndicesPtr)), numRows),
		RightIndices: unsafe.Slice((*int32)(unsafe.Pointer(rightIndicesPtr)), numRows),
	}

	runtime.SetFinalizer(result, (*LeftJoinE2EResult).free)
	return result
}

// ParallelLeftJoinI64 performs multi-threaded left join on int64 keys
func ParallelLeftJoinI64(leftKeys []int64, rightKeys []int64) *LeftJoinE2EResult {
	if len(leftKeys) == 0 {
		return nil
	}

	var rightPtr *C.int64_t
	if len(rightKeys) > 0 {
		rightPtr = (*C.int64_t)(unsafe.Pointer(&rightKeys[0]))
	}

	handle := C.galleon_parallel_left_join_i64(
		(*C.int64_t)(unsafe.Pointer(&leftKeys[0])),
		C.size_t(len(leftKeys)),
		rightPtr,
		C.size_t(len(rightKeys)),
	)
	if handle == nil {
		return nil
	}

	numRows := int(C.galleon_left_join_result_num_rows(handle))
	leftIndicesPtr := C.galleon_left_join_result_left_indices(handle)
	rightIndicesPtr := C.galleon_left_join_result_right_indices(handle)

	result := &LeftJoinE2EResult{
		handle:       handle,
		NumRows:      numRows,
		LeftIndices:  unsafe.Slice((*int32)(unsafe.Pointer(leftIndicesPtr)), numRows),
		RightIndices: unsafe.Slice((*int32)(unsafe.Pointer(rightIndicesPtr)), numRows),
	}

	runtime.SetFinalizer(result, (*LeftJoinE2EResult).free)
	return result
}

// LeftJoinI64 performs left join and returns Go int slices for join result building
// For unmatched left rows, rightIndices will be -1
func LeftJoinI64(leftKeys []int64, rightKeys []int64) (leftIndices, rightIndices []int) {
	if len(leftKeys) == 0 {
		return nil, nil
	}

	// Use parallel version for large datasets
	var result *LeftJoinE2EResult
	if len(leftKeys) >= 100000 {
		result = ParallelLeftJoinI64(leftKeys, rightKeys)
	} else {
		result = LeftJoinI64E2E(leftKeys, rightKeys)
	}
	if result == nil {
		return nil, nil
	}

	// Convert int32 to int
	leftIndices = make([]int, result.NumRows)
	rightIndices = make([]int, result.NumRows)
	for i := 0; i < result.NumRows; i++ {
		leftIndices[i] = int(result.LeftIndices[i])
		rightIndices[i] = int(result.RightIndices[i])
	}

	return leftIndices, rightIndices
}

