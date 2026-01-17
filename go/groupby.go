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

// GroupBy represents a groupby operation on an DataFrame.
// It holds a reference to the source DataFrame and the grouping column(s).
type GroupBy struct {
	df        *DataFrame
	byColumns []string
}

// GroupBy creates a groupby object for the specified columns.
// Currently supports single Int64 column grouping.
func (df *DataFrame) GroupBy(columns ...string) *GroupBy {
	if df == nil || len(columns) == 0 {
		return nil
	}

	// Verify all groupby columns exist
	for _, col := range columns {
		if df.Column(col) == nil {
			return nil
		}
	}

	return &GroupBy{
		df:        df,
		byColumns: columns,
	}
}

// Sum computes the sum of the specified column for each group.
// Returns a new DataFrame with group keys and sum values.
func (g *GroupBy) Sum(column string) *DataFrame {
	if g == nil || len(g.byColumns) != 1 {
		return nil // Currently only single-column groupby supported
	}

	keyCol := g.df.Column(g.byColumns[0])
	valCol := g.df.Column(column)

	if keyCol == nil || valCol == nil {
		return nil
	}

	// Currently only supports I64 keys and F64 values
	if keyCol.DType() != Int64 || valCol.DType() != Float64 {
		return nil
	}

	// Handle empty input
	if keyCol.Len() == 0 || keyCol.handle == nil || valCol.handle == nil {
		return NewDataFrame()
	}

	result := C.galleon_series_groupby_sum_i64_f64(
		(*C.ManagedArrowArray)(keyCol.handle),
		(*C.ManagedArrowArray)(valCol.handle),
	)

	if result == nil {
		return nil
	}

	return groupBySumResultToDataFrame(result, g.byColumns[0], column+"_sum")
}

// Mean computes the mean of the specified column for each group.
// Returns a new DataFrame with group keys and mean values.
func (g *GroupBy) Mean(column string) *DataFrame {
	if g == nil || len(g.byColumns) != 1 {
		return nil
	}

	keyCol := g.df.Column(g.byColumns[0])
	valCol := g.df.Column(column)

	if keyCol == nil || valCol == nil {
		return nil
	}

	if keyCol.DType() != Int64 || valCol.DType() != Float64 {
		return nil
	}

	// Handle empty input
	if keyCol.Len() == 0 || keyCol.handle == nil || valCol.handle == nil {
		return NewDataFrame()
	}

	result := C.galleon_series_groupby_mean_i64_f64(
		(*C.ManagedArrowArray)(keyCol.handle),
		(*C.ManagedArrowArray)(valCol.handle),
	)

	if result == nil {
		return nil
	}

	return groupBySumResultToDataFrame(result, g.byColumns[0], column+"_mean")
}

// Count computes the count of rows for each group.
// Returns a new DataFrame with group keys and counts.
func (g *GroupBy) Count() *DataFrame {
	if g == nil || len(g.byColumns) != 1 {
		return nil
	}

	keyCol := g.df.Column(g.byColumns[0])
	if keyCol == nil {
		return nil
	}

	if keyCol.DType() != Int64 {
		return nil
	}

	// Handle empty input
	if keyCol.Len() == 0 || keyCol.handle == nil {
		return NewDataFrame()
	}

	result := C.galleon_series_groupby_count_i64(
		(*C.ManagedArrowArray)(keyCol.handle),
	)

	if result == nil {
		return nil
	}

	return groupBySumResultToDataFrame(result, g.byColumns[0], "count")
}

// Agg computes multiple aggregations in a single pass.
// aggs is a map of column name to aggregation type: {"value": "sum", "price": "mean"}
// Supported aggregations: "sum", "min", "max", "count", "mean"
// Returns a new DataFrame with group keys and all aggregated columns.
func (g *GroupBy) Agg(aggs map[string]string) *DataFrame {
	if g == nil || len(g.byColumns) != 1 || len(aggs) == 0 {
		return nil
	}

	keyCol := g.df.Column(g.byColumns[0])
	if keyCol == nil || keyCol.DType() != Int64 {
		return nil
	}

	// For efficiency, we try to use multi-agg when possible
	// But we need all aggregations to be on the same column for true efficiency
	// For now, we handle each aggregation separately and merge results

	// Collect all columns that need aggregation
	columns := make([]string, 0, len(aggs))
	for col := range aggs {
		columns = append(columns, col)
	}

	// Check if all aggregations are on the same column and we can use multi-agg
	if len(columns) == 1 {
		col := columns[0]
		valCol := g.df.Column(col)
		if valCol == nil || valCol.DType() != Float64 {
			return nil
		}

		aggType := aggs[col]
		switch aggType {
		case "sum":
			return g.Sum(col)
		case "mean":
			return g.Mean(col)
		case "count":
			return g.Count()
		}
	}

	// For multiple columns/aggregations, use multi-agg if all on same column
	// and return desired aggregations
	if len(aggs) >= 2 {
		// Check if we can use the first column and extract multiple aggs
		firstCol := columns[0]
		allSameCol := true
		for _, col := range columns[1:] {
			if col != firstCol {
				allSameCol = false
				break
			}
		}

		if allSameCol {
			valCol := g.df.Column(firstCol)
			if valCol != nil && valCol.DType() == Float64 {
				return g.multiAgg(keyCol, valCol, firstCol, aggs)
			}
		}
	}

	// Fallback: compute each aggregation separately and merge
	// Start with the first column
	var resultDF *DataFrame
	for col, aggType := range aggs {
		var aggDF *DataFrame
		switch aggType {
		case "sum":
			aggDF = g.Sum(col)
		case "mean":
			aggDF = g.Mean(col)
		case "count":
			aggDF = g.Count()
		default:
			// Unsupported aggregation type
			continue
		}

		if aggDF == nil {
			continue
		}

		if resultDF == nil {
			resultDF = aggDF
		} else {
			// Merge results (join on key column)
			// For simplicity, we assume the keys are in the same order
			for _, aggColName := range aggDF.ColumnNames() {
				if aggColName != g.byColumns[0] {
					resultDF = resultDF.WithColumn(aggDF.Column(aggColName))
				}
			}
		}
	}

	return resultDF
}

// multiAgg uses the multi-aggregation function for efficiency
func (g *GroupBy) multiAgg(keyCol, valCol *Series, colName string, aggs map[string]string) *DataFrame {
	result := C.galleon_series_groupby_multi_agg_i64_f64(
		(*C.ManagedArrowArray)(keyCol.handle),
		(*C.ManagedArrowArray)(valCol.handle),
	)

	if result == nil {
		return nil
	}

	numGroups := int(C.galleon_series_groupby_multi_agg_result_num_groups(result))
	if numGroups == 0 {
		C.galleon_series_groupby_multi_agg_result_destroy(result)
		return NewDataFrame()
	}

	// Create Series from the result arrays
	keysPtr := C.galleon_series_groupby_multi_agg_result_keys(result)

	// Create the result DataFrame with keys
	keysSeries := wrapManagedArrowArrayAsI64(keysPtr, g.byColumns[0], numGroups)
	df := NewDataFrame()
	df = df.WithColumn(keysSeries)

	// Add requested aggregations
	for _, aggType := range aggs {
		switch aggType {
		case "sum":
			sumsPtr := C.galleon_series_groupby_multi_agg_result_sums(result)
			sumsSeries := wrapManagedArrowArrayAsF64(sumsPtr, colName+"_sum", numGroups)
			df = df.WithColumn(sumsSeries)
		case "min":
			minsPtr := C.galleon_series_groupby_multi_agg_result_mins(result)
			minsSeries := wrapManagedArrowArrayAsF64(minsPtr, colName+"_min", numGroups)
			df = df.WithColumn(minsSeries)
		case "max":
			maxsPtr := C.galleon_series_groupby_multi_agg_result_maxs(result)
			maxsSeries := wrapManagedArrowArrayAsF64(maxsPtr, colName+"_max", numGroups)
			df = df.WithColumn(maxsSeries)
		case "count":
			countsPtr := C.galleon_series_groupby_multi_agg_result_counts(result)
			countsSeries := wrapManagedArrowArrayAsI64(countsPtr, "count", numGroups)
			df = df.WithColumn(countsSeries)
		}
	}

	// The multi-agg result still owns the arrays, so we need to be careful
	// We should NOT destroy it while we're using references to its arrays
	// Instead, set a finalizer on the DataFrame to clean up
	runtime.SetFinalizer(df, func(_ *DataFrame) {
		C.galleon_series_groupby_multi_agg_result_destroy(result)
	})

	return df
}

// groupBySumResultToDataFrame converts a GroupBy sum/mean/count result to a DataFrame
func groupBySumResultToDataFrame(result *C.GroupBySumResult, keyName, valueName string) *DataFrame {
	numGroups := int(C.galleon_series_groupby_sum_result_num_groups(result))
	if numGroups == 0 {
		C.galleon_series_groupby_sum_result_destroy(result)
		return NewDataFrame()
	}

	keysPtr := C.galleon_series_groupby_sum_result_keys(result)
	valuesPtr := C.galleon_series_groupby_sum_result_values(result)

	// Create Series from the result arrays
	keysSeries := wrapManagedArrowArrayAsI64(keysPtr, keyName, numGroups)
	valuesSeries := wrapManagedArrowArrayAsF64(valuesPtr, valueName, numGroups)

	// Build DataFrame
	df := NewDataFrame()
	df = df.WithColumn(keysSeries)
	df = df.WithColumn(valuesSeries)

	// Set finalizer to clean up the result when done
	runtime.SetFinalizer(df, func(_ *DataFrame) {
		C.galleon_series_groupby_sum_result_destroy(result)
	})

	return df
}

// wrapManagedArrowArrayAsI64 creates an Series from a const ManagedArrowArray pointer
// NOTE: The caller retains ownership of the array - do not set a finalizer here
func wrapManagedArrowArrayAsI64(arr *C.ManagedArrowArray, name string, length int) *Series {
	s := &Series{
		handle: unsafe.Pointer(arr),
		name:   name,
		dtype:  Int64,
		length: length,
	}
	// Do NOT set finalizer - the parent result struct owns this memory
	return s
}

// wrapManagedArrowArrayAsF64 creates an Series from a const ManagedArrowArray pointer
// NOTE: The caller retains ownership of the array - do not set a finalizer here
func wrapManagedArrowArrayAsF64(arr *C.ManagedArrowArray, name string, length int) *Series {
	s := &Series{
		handle: unsafe.Pointer(arr),
		name:   name,
		dtype:  Float64,
		length: length,
	}
	// Do NOT set finalizer - the parent result struct owns this memory
	return s
}
