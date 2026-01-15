package galleon

import (
	"fmt"
	"hash/maphash"
	"math"
	"sort"
	"sync"
)

// GroupBy represents a grouped DataFrame for aggregation operations
type GroupBy struct {
	df           *DataFrame
	keys         []string   // Column names to group by
	numGroups    int        // Number of unique groups
	rowGroupIDs  []uint32   // Group ID for each row (for Zig SIMD path)
	firstRowIdx  []int      // First row index for each group (for key extraction)
	groupCounts  []int      // Number of rows per group

	// Zero-copy result from Zig (keeps Zig memory alive)
	zeroCopyResult *GroupByResultZeroCopy
}

// GroupBy creates a GroupBy object for aggregation operations
func (df *DataFrame) GroupBy(keys ...string) *GroupBy {
	if len(keys) == 0 {
		return &GroupBy{df: df}
	}

	// Validate keys exist
	for _, key := range keys {
		if df.ColumnByName(key) == nil {
			return &GroupBy{df: df, keys: keys} // Will error on aggregation
		}
	}

	gb := &GroupBy{
		df:   df,
		keys: keys,
	}

	// Group rows by key values using Zig
	gb.computeGroupsZig()

	return gb
}

// computeGroupsZig uses Zig for fast hash table building
func (gb *GroupBy) computeGroupsZig() {
	height := gb.df.Height()
	if height == 0 {
		return
	}

	// Get key columns
	keyCols := make([]*Series, len(gb.keys))
	for i, key := range gb.keys {
		keyCols[i] = gb.df.ColumnByName(key)
	}

	// Compute hashes using Zig SIMD
	hashes := gb.computeKeyHashes(keyCols, height)

	// Use zero-copy version - keeps Zig memory alive, no copying
	result := ComputeGroupIDsZeroCopy(hashes)
	if result == nil {
		// Fallback to old method
		gb.rowGroupIDs, gb.numGroups = ComputeGroupIDs(hashes)
		gb.firstRowIdx = make([]int, gb.numGroups)
		gb.groupCounts = make([]int, gb.numGroups)
		for i := range gb.firstRowIdx {
			gb.firstRowIdx[i] = -1
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if gb.firstRowIdx[gid] == -1 {
				gb.firstRowIdx[gid] = rowIdx
			}
			gb.groupCounts[gid]++
		}
		return
	}

	// Store zero-copy result to keep Zig memory alive
	gb.zeroCopyResult = result
	gb.rowGroupIDs = result.GroupIDs()
	gb.numGroups = result.NumGroups()

	// Convert uint32 views to int slices for compatibility with existing code
	// This is a small overhead for numGroups elements (typically ~1% of data size)
	firstRowU32 := result.FirstRowIdxU32()
	gb.firstRowIdx = make([]int, gb.numGroups)
	for i, v := range firstRowU32 {
		gb.firstRowIdx[i] = int(v)
	}

	countsU32 := result.GroupCountsU32()
	gb.groupCounts = make([]int, gb.numGroups)
	for i, v := range countsU32 {
		gb.groupCounts[i] = int(v)
	}
}

// computeKeyHashes computes hashes for all key columns using Zig SIMD
func (gb *GroupBy) computeKeyHashes(keyCols []*Series, height int) []uint64 {
	if len(keyCols) == 0 {
		return make([]uint64, height)
	}

	// Hash first column
	hashes := make([]uint64, height)
	gb.hashColumn(keyCols[0], hashes)

	// Combine with additional columns
	if len(keyCols) > 1 {
		tempHashes := make([]uint64, height)
		for i := 1; i < len(keyCols); i++ {
			gb.hashColumn(keyCols[i], tempHashes)
			CombineHashes(hashes, tempHashes, hashes)
		}
	}

	return hashes
}

// hashColumn hashes a single column using type-specific Zig functions
func (gb *GroupBy) hashColumn(col *Series, outHashes []uint64) {
	switch col.DType() {
	case Int64:
		HashI64Column(col.Int64(), outHashes)
	case Int32:
		HashI32Column(col.Int32(), outHashes)
	case Float64:
		HashF64Column(col.Float64(), outHashes)
	case Float32:
		HashF32Column(col.Float32(), outHashes)
	case String:
		// Fallback to Go hashing for strings
		data := col.Strings()
		var h maphash.Hash
		for i, s := range data {
			h.Reset()
			h.WriteString(s)
			outHashes[i] = h.Sum64()
		}
	case Categorical:
		// Hash the int32 indices using SIMD (much faster than string hashing!)
		// Categorical columns store strings as integer indices, so we can use
		// fast integer hashing instead of string hashing.
		HashI32Column(col.CategoricalIndices(), outHashes)
	case Bool:
		// Simple hash for bools
		data := col.Bool()
		for i, b := range data {
			if b {
				outHashes[i] = 1
			} else {
				outHashes[i] = 0
			}
		}
	default:
		// Fallback
		var h maphash.Hash
		for i := 0; i < col.Len(); i++ {
			h.Reset()
			h.WriteString(fmt.Sprintf("%v", col.Get(i)))
			outHashes[i] = h.Sum64()
		}
	}
}

// keysEqual checks if two rows have equal key values
func (gb *GroupBy) keysEqual(keyCols []*Series, row1, row2 int) bool {
	for _, col := range keyCols {
		switch col.DType() {
		case Int64:
			if col.Int64()[row1] != col.Int64()[row2] {
				return false
			}
		case Int32:
			if col.Int32()[row1] != col.Int32()[row2] {
				return false
			}
		case Float64:
			if col.Float64()[row1] != col.Float64()[row2] {
				return false
			}
		case Float32:
			if col.Float32()[row1] != col.Float32()[row2] {
				return false
			}
		case String:
			if col.Strings()[row1] != col.Strings()[row2] {
				return false
			}
		case Bool:
			if col.Bool()[row1] != col.Bool()[row2] {
				return false
			}
		case Categorical:
			// Compare indices (faster than string comparison)
			if col.CategoricalIndices()[row1] != col.CategoricalIndices()[row2] {
				return false
			}
		default:
			if col.Get(row1) != col.Get(row2) {
				return false
			}
		}
	}
	return true
}

// NumGroups returns the number of groups
func (gb *GroupBy) NumGroups() int {
	return gb.numGroups
}

// Sum computes the sum of a column for each group
func (gb *GroupBy) Sum(column string) (*DataFrame, error) {
	return gb.Agg(AggSum(column))
}

// Mean computes the mean of a column for each group
func (gb *GroupBy) Mean(column string) (*DataFrame, error) {
	return gb.Agg(AggMean(column))
}

// Min computes the minimum of a column for each group
func (gb *GroupBy) Min(column string) (*DataFrame, error) {
	return gb.Agg(AggMin(column))
}

// Max computes the maximum of a column for each group
func (gb *GroupBy) Max(column string) (*DataFrame, error) {
	return gb.Agg(AggMax(column))
}

// Count counts rows in each group
func (gb *GroupBy) Count() (*DataFrame, error) {
	return gb.Agg(AggCount())
}

// First gets the first value of a column for each group
func (gb *GroupBy) First(column string) (*DataFrame, error) {
	return gb.Agg(AggFirst(column))
}

// Last gets the last value of a column for each group
func (gb *GroupBy) Last(column string) (*DataFrame, error) {
	return gb.Agg(AggLast(column))
}

// Std computes the standard deviation of a column for each group
func (gb *GroupBy) Std(column string) (*DataFrame, error) {
	return gb.Agg(AggStd(column))
}

// Var computes the variance of a column for each group
func (gb *GroupBy) Var(column string) (*DataFrame, error) {
	return gb.Agg(AggVar(column))
}

// Aggregation represents an aggregation operation
type Aggregation struct {
	column string
	op     AggOp
	alias  string
}

// AggOp is the type of aggregation operation
type AggOp int

const (
	AggOpSum AggOp = iota
	AggOpMean
	AggOpMin
	AggOpMax
	AggOpCount
	AggOpFirst
	AggOpLast
	AggOpStd
	AggOpVar
	AggOpMedian
	AggOpCountDistinct
)

// Alias sets a custom name for the aggregation result column
func (a Aggregation) Alias(name string) Aggregation {
	a.alias = name
	return a
}

// Aggregation constructors

func AggSum(column string) Aggregation {
	return Aggregation{column: column, op: AggOpSum, alias: column + "_sum"}
}

func AggMean(column string) Aggregation {
	return Aggregation{column: column, op: AggOpMean, alias: column + "_mean"}
}

func AggMin(column string) Aggregation {
	return Aggregation{column: column, op: AggOpMin, alias: column + "_min"}
}

func AggMax(column string) Aggregation {
	return Aggregation{column: column, op: AggOpMax, alias: column + "_max"}
}

func AggCount() Aggregation {
	return Aggregation{op: AggOpCount, alias: "count"}
}

func AggFirst(column string) Aggregation {
	return Aggregation{column: column, op: AggOpFirst, alias: column + "_first"}
}

func AggLast(column string) Aggregation {
	return Aggregation{column: column, op: AggOpLast, alias: column + "_last"}
}

func AggStd(column string) Aggregation {
	return Aggregation{column: column, op: AggOpStd, alias: column + "_std"}
}

func AggVar(column string) Aggregation {
	return Aggregation{column: column, op: AggOpVar, alias: column + "_var"}
}

func AggMedian(column string) Aggregation {
	return Aggregation{column: column, op: AggOpMedian, alias: column + "_median"}
}

func AggCountDistinct(column string) Aggregation {
	return Aggregation{column: column, op: AggOpCountDistinct, alias: column + "_nunique"}
}

// Agg performs multiple aggregations and returns a new DataFrame
func (gb *GroupBy) Agg(aggs ...Aggregation) (*DataFrame, error) {
	if len(gb.keys) == 0 {
		return nil, fmt.Errorf("no group keys specified")
	}

	// Validate all columns exist
	for _, agg := range aggs {
		if agg.column != "" && gb.df.ColumnByName(agg.column) == nil {
			return nil, fmt.Errorf("column '%s' not found", agg.column)
		}
	}

	numGroups := gb.numGroups
	if numGroups == 0 {
		// Return empty DataFrame with correct schema
		return gb.buildEmptyResult(aggs)
	}

	// Build result columns
	resultCols := make([]*Series, 0, len(gb.keys)+len(aggs))

	// Add key columns
	for keyIdx, keyName := range gb.keys {
		keyCol := gb.df.ColumnByName(keyName)
		resultCols = append(resultCols, gb.buildKeyColumn(keyName, keyCol.DType(), keyIdx))
	}

	// Compute aggregations in parallel using the parallel framework
	numAggs := len(aggs)
	aggCols := make([]*Series, numAggs)
	aggErrs := make([]error, numAggs)

	cfg := globalConfig
	// Use parallelism for multiple aggregations on large datasets
	if numAggs > 1 && cfg.shouldParallelize(numGroups) {
		numWorkers := cfg.numWorkers()
		if numWorkers > numAggs {
			numWorkers = numAggs
		}

		var wg sync.WaitGroup
		aggChan := make(chan int, numAggs)

		// Feed work
		for i := 0; i < numAggs; i++ {
			aggChan <- i
		}
		close(aggChan)

		// Spawn workers
		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range aggChan {
					aggCols[i], aggErrs[i] = gb.computeAggregation(aggs[i])
				}
			}()
		}

		wg.Wait()
	} else {
		// Sequential path for small datasets or single aggregation
		for i, agg := range aggs {
			aggCols[i], aggErrs[i] = gb.computeAggregation(agg)
		}
	}

	// Check for errors
	for i, err := range aggErrs {
		if err != nil {
			return nil, fmt.Errorf("aggregation %s failed: %w", aggs[i].alias, err)
		}
	}

	// Append aggregation columns
	resultCols = append(resultCols, aggCols...)

	return NewDataFrame(resultCols...)
}

func (gb *GroupBy) buildEmptyResult(aggs []Aggregation) (*DataFrame, error) {
	cols := make([]*Series, 0, len(gb.keys)+len(aggs))

	// Key columns
	for _, keyName := range gb.keys {
		keyCol := gb.df.ColumnByName(keyName)
		cols = append(cols, newEmptySeries(keyName, keyCol.DType()))
	}

	// Aggregation columns
	for _, agg := range aggs {
		dtype := Float64
		if agg.op == AggOpCount || agg.op == AggOpCountDistinct {
			dtype = Int64
		} else if agg.column != "" {
			srcCol := gb.df.ColumnByName(agg.column)
			if srcCol != nil && (agg.op == AggOpFirst || agg.op == AggOpLast) {
				dtype = srcCol.DType()
			}
		}
		cols = append(cols, newEmptySeries(agg.alias, dtype))
	}

	return NewDataFrame(cols...)
}

func newEmptySeries(name string, dtype DType) *Series {
	switch dtype {
	case Float64:
		return NewSeriesFloat64(name, []float64{})
	case Float32:
		return NewSeriesFloat32(name, []float32{})
	case Int64:
		return NewSeriesInt64(name, []int64{})
	case Int32:
		return NewSeriesInt32(name, []int32{})
	case Bool:
		return NewSeriesBool(name, []bool{})
	case String:
		return NewSeriesString(name, []string{})
	case Categorical:
		return NewSeriesCategorical(name, []string{})
	default:
		return NewSeriesString(name, []string{})
	}
}

func (gb *GroupBy) buildKeyColumn(name string, dtype DType, keyIdx int) *Series {
	numGroups := gb.numGroups
	keyCol := gb.df.ColumnByName(gb.keys[keyIdx])

	switch dtype {
	case Float64:
		data := make([]float64, numGroups)
		srcData := keyCol.Float64()
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		return NewSeriesFloat64(name, data)

	case Float32:
		data := make([]float32, numGroups)
		srcData := keyCol.Float32()
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		return NewSeriesFloat32(name, data)

	case Int64:
		data := make([]int64, numGroups)
		srcData := keyCol.Int64()
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		return NewSeriesInt64(name, data)

	case Int32:
		data := make([]int32, numGroups)
		srcData := keyCol.Int32()
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		return NewSeriesInt32(name, data)

	case Bool:
		data := make([]bool, numGroups)
		srcData := keyCol.Bool()
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		return NewSeriesBool(name, data)

	case String:
		data := make([]string, numGroups)
		srcData := keyCol.Strings()
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		return NewSeriesString(name, data)

	case Categorical:
		// Extract string values for the group keys
		// The result stays categorical with the same dictionary
		srcIndices := keyCol.CategoricalIndices()
		categories := keyCol.Categories()
		data := make([]string, numGroups)
		for i := 0; i < numGroups; i++ {
			data[i] = categories[srcIndices[gb.firstRowIdx[i]]]
		}
		// Create new categorical with extracted values (may have fewer unique categories)
		result, _ := NewSeriesCategoricalWithCategories(name, data, categories)
		if result != nil {
			return result
		}
		// Fallback: create fresh categorical
		return NewSeriesCategorical(name, data)

	default:
		data := make([]string, numGroups)
		for i := 0; i < numGroups; i++ {
			data[i] = fmt.Sprintf("%v", keyCol.Get(gb.firstRowIdx[i]))
		}
		return NewSeriesString(name, data)
	}
}

func (gb *GroupBy) computeAggregation(agg Aggregation) (*Series, error) {
	numGroups := gb.numGroups

	switch agg.op {
	case AggOpCount:
		data := make([]int64, numGroups)
		for i := 0; i < numGroups; i++ {
			data[i] = int64(gb.groupCounts[i])
		}
		return NewSeriesInt64(agg.alias, data), nil

	case AggOpCountDistinct:
		return gb.computeCountDistinct(agg)

	case AggOpSum:
		return gb.computeSum(agg)

	case AggOpMean:
		return gb.computeMean(agg)

	case AggOpMin:
		return gb.computeMin(agg)

	case AggOpMax:
		return gb.computeMax(agg)

	case AggOpFirst:
		return gb.computeFirst(agg)

	case AggOpLast:
		return gb.computeLast(agg)

	case AggOpStd:
		return gb.computeStd(agg)

	case AggOpVar:
		return gb.computeVar(agg)

	case AggOpMedian:
		return gb.computeMedian(agg)

	default:
		return nil, fmt.Errorf("unknown aggregation operation: %d", agg.op)
	}
}

func (gb *GroupBy) computeSum(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups

	switch col.DType() {
	case Float64:
		srcData := col.Float64()
		data := make([]float64, numGroups)
		// Use Zig SIMD path
		AggregateSumF64ByGroup(srcData, gb.rowGroupIDs, data)
		return NewSeriesFloat64(agg.alias, data), nil

	case Float32:
		srcData := col.Float32()
		data := make([]float64, numGroups)
		// Accumulate by group
		for rowIdx, gid := range gb.rowGroupIDs {
			data[gid] += float64(srcData[rowIdx])
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Int64:
		srcData := col.Int64()
		data := make([]int64, numGroups)
		// Use Zig SIMD path
		AggregateSumI64ByGroup(srcData, gb.rowGroupIDs, data)
		// Convert to float64 for consistency
		result := make([]float64, numGroups)
		for i, v := range data {
			result[i] = float64(v)
		}
		return NewSeriesFloat64(agg.alias, result), nil

	case Int32:
		srcData := col.Int32()
		data := make([]float64, numGroups)
		// Accumulate by group
		for rowIdx, gid := range gb.rowGroupIDs {
			data[gid] += float64(srcData[rowIdx])
		}
		return NewSeriesFloat64(agg.alias, data), nil

	default:
		return nil, fmt.Errorf("sum not supported for dtype %s", col.DType())
	}
}

func (gb *GroupBy) computeMean(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups
	sums := make([]float64, numGroups)
	data := make([]float64, numGroups)

	switch col.DType() {
	case Float64:
		srcData := col.Float64()
		// Use Zig SIMD for sum
		AggregateSumF64ByGroup(srcData, gb.rowGroupIDs, sums)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			} else {
				data[i] = sums[i] / float64(gb.groupCounts[i])
			}
		}

	case Float32:
		srcData := col.Float32()
		for rowIdx, gid := range gb.rowGroupIDs {
			sums[gid] += float64(srcData[rowIdx])
		}
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			} else {
				data[i] = sums[i] / float64(gb.groupCounts[i])
			}
		}

	case Int64:
		srcData := col.Int64()
		i64Sums := make([]int64, numGroups)
		AggregateSumI64ByGroup(srcData, gb.rowGroupIDs, i64Sums)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			} else {
				data[i] = float64(i64Sums[i]) / float64(gb.groupCounts[i])
			}
		}

	case Int32:
		srcData := col.Int32()
		for rowIdx, gid := range gb.rowGroupIDs {
			sums[gid] += float64(srcData[rowIdx])
		}
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			} else {
				data[i] = sums[i] / float64(gb.groupCounts[i])
			}
		}

	default:
		return nil, fmt.Errorf("mean not supported for dtype %s", col.DType())
	}

	return NewSeriesFloat64(agg.alias, data), nil
}

func (gb *GroupBy) computeMin(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups

	switch col.DType() {
	case Float64:
		srcData := col.Float64()
		data := make([]float64, numGroups)
		// Initialize to max value for min aggregation
		for i := range data {
			data[i] = math.MaxFloat64
		}
		// Use Zig SIMD path
		AggregateMinF64ByGroup(srcData, gb.rowGroupIDs, data)
		// Handle empty groups
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			}
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Float32:
		srcData := col.Float32()
		data := make([]float64, numGroups)
		for i := range data {
			data[i] = math.MaxFloat64
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if float64(srcData[rowIdx]) < data[gid] {
				data[gid] = float64(srcData[rowIdx])
			}
		}
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			}
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Int64:
		srcData := col.Int64()
		data := make([]int64, numGroups)
		// Initialize to max value for min aggregation
		for i := range data {
			data[i] = math.MaxInt64
		}
		// Use Zig SIMD path
		AggregateMinI64ByGroup(srcData, gb.rowGroupIDs, data)
		// Handle empty groups - reset to 0
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = 0
			}
		}
		return NewSeriesInt64(agg.alias, data), nil

	case Int32:
		srcData := col.Int32()
		data := make([]int32, numGroups)
		for i := range data {
			data[i] = math.MaxInt32
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if srcData[rowIdx] < data[gid] {
				data[gid] = srcData[rowIdx]
			}
		}
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = 0
			}
		}
		return NewSeriesInt32(agg.alias, data), nil

	case String:
		srcData := col.Strings()
		data := make([]string, numGroups)
		// Initialize with first value in each group
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if srcData[rowIdx] < data[gid] {
				data[gid] = srcData[rowIdx]
			}
		}
		return NewSeriesString(agg.alias, data), nil

	default:
		return nil, fmt.Errorf("min not supported for dtype %s", col.DType())
	}
}

func (gb *GroupBy) computeMax(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups

	switch col.DType() {
	case Float64:
		srcData := col.Float64()
		data := make([]float64, numGroups)
		// Initialize to min value for max aggregation
		for i := range data {
			data[i] = -math.MaxFloat64
		}
		// Use Zig SIMD path
		AggregateMaxF64ByGroup(srcData, gb.rowGroupIDs, data)
		// Handle empty groups
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			}
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Float32:
		srcData := col.Float32()
		data := make([]float64, numGroups)
		for i := range data {
			data[i] = -math.MaxFloat64
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if float64(srcData[rowIdx]) > data[gid] {
				data[gid] = float64(srcData[rowIdx])
			}
		}
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = math.NaN()
			}
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Int64:
		srcData := col.Int64()
		data := make([]int64, numGroups)
		// Initialize to min value for max aggregation
		for i := range data {
			data[i] = math.MinInt64
		}
		// Use Zig SIMD path
		AggregateMaxI64ByGroup(srcData, gb.rowGroupIDs, data)
		// Handle empty groups - reset to 0
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = 0
			}
		}
		return NewSeriesInt64(agg.alias, data), nil

	case Int32:
		srcData := col.Int32()
		data := make([]int32, numGroups)
		for i := range data {
			data[i] = math.MinInt32
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if srcData[rowIdx] > data[gid] {
				data[gid] = srcData[rowIdx]
			}
		}
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] == 0 {
				data[i] = 0
			}
		}
		return NewSeriesInt32(agg.alias, data), nil

	case String:
		srcData := col.Strings()
		data := make([]string, numGroups)
		// Initialize with first value in each group
		for i := 0; i < numGroups; i++ {
			data[i] = srcData[gb.firstRowIdx[i]]
		}
		for rowIdx, gid := range gb.rowGroupIDs {
			if srcData[rowIdx] > data[gid] {
				data[gid] = srcData[rowIdx]
			}
		}
		return NewSeriesString(agg.alias, data), nil

	default:
		return nil, fmt.Errorf("max not supported for dtype %s", col.DType())
	}
}

func (gb *GroupBy) computeFirst(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups

	switch col.DType() {
	case Float64:
		srcData := col.Float64()
		data := make([]float64, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[gb.firstRowIdx[i]]
			}
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Float32:
		srcData := col.Float32()
		data := make([]float32, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[gb.firstRowIdx[i]]
			}
		}
		return NewSeriesFloat32(agg.alias, data), nil

	case Int64:
		srcData := col.Int64()
		data := make([]int64, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[gb.firstRowIdx[i]]
			}
		}
		return NewSeriesInt64(agg.alias, data), nil

	case Int32:
		srcData := col.Int32()
		data := make([]int32, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[gb.firstRowIdx[i]]
			}
		}
		return NewSeriesInt32(agg.alias, data), nil

	case Bool:
		srcData := col.Bool()
		data := make([]bool, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[gb.firstRowIdx[i]]
			}
		}
		return NewSeriesBool(agg.alias, data), nil

	case String:
		srcData := col.Strings()
		data := make([]string, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[gb.firstRowIdx[i]]
			}
		}
		return NewSeriesString(agg.alias, data), nil

	case Categorical:
		srcIndices := col.CategoricalIndices()
		categories := col.Categories()
		data := make([]string, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = categories[srcIndices[gb.firstRowIdx[i]]]
			}
		}
		return NewSeriesCategorical(agg.alias, data), nil

	default:
		return nil, fmt.Errorf("first not supported for dtype %s", col.DType())
	}
}

func (gb *GroupBy) computeLast(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups

	// Find the last row for each group
	lastRowIdx := make([]int, numGroups)
	for rowIdx, gid := range gb.rowGroupIDs {
		lastRowIdx[gid] = rowIdx // Keep updating - final value will be the last row
	}

	switch col.DType() {
	case Float64:
		srcData := col.Float64()
		data := make([]float64, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[lastRowIdx[i]]
			}
		}
		return NewSeriesFloat64(agg.alias, data), nil

	case Float32:
		srcData := col.Float32()
		data := make([]float32, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[lastRowIdx[i]]
			}
		}
		return NewSeriesFloat32(agg.alias, data), nil

	case Int64:
		srcData := col.Int64()
		data := make([]int64, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[lastRowIdx[i]]
			}
		}
		return NewSeriesInt64(agg.alias, data), nil

	case Int32:
		srcData := col.Int32()
		data := make([]int32, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[lastRowIdx[i]]
			}
		}
		return NewSeriesInt32(agg.alias, data), nil

	case Bool:
		srcData := col.Bool()
		data := make([]bool, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[lastRowIdx[i]]
			}
		}
		return NewSeriesBool(agg.alias, data), nil

	case String:
		srcData := col.Strings()
		data := make([]string, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = srcData[lastRowIdx[i]]
			}
		}
		return NewSeriesString(agg.alias, data), nil

	case Categorical:
		srcIndices := col.CategoricalIndices()
		categories := col.Categories()
		data := make([]string, numGroups)
		for i := 0; i < numGroups; i++ {
			if gb.groupCounts[i] > 0 {
				data[i] = categories[srcIndices[lastRowIdx[i]]]
			}
		}
		return NewSeriesCategorical(agg.alias, data), nil

	default:
		return nil, fmt.Errorf("last not supported for dtype %s", col.DType())
	}
}

func (gb *GroupBy) computeStd(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups
	data := make([]float64, numGroups)

	// First pass: compute sums
	sums := make([]float64, numGroups)
	for rowIdx, gid := range gb.rowGroupIDs {
		sums[gid] += toFloat64(col.Get(rowIdx))
	}

	// Compute means
	means := make([]float64, numGroups)
	for i := 0; i < numGroups; i++ {
		if gb.groupCounts[i] > 0 {
			means[i] = sums[i] / float64(gb.groupCounts[i])
		}
	}

	// Second pass: compute sum of squared differences
	sumSq := make([]float64, numGroups)
	for rowIdx, gid := range gb.rowGroupIDs {
		diff := toFloat64(col.Get(rowIdx)) - means[gid]
		sumSq[gid] += diff * diff
	}

	// Compute std dev
	for i := 0; i < numGroups; i++ {
		if gb.groupCounts[i] < 2 {
			data[i] = math.NaN()
		} else {
			variance := sumSq[i] / float64(gb.groupCounts[i]-1) // Sample std dev (ddof=1)
			data[i] = math.Sqrt(variance)
		}
	}

	return NewSeriesFloat64(agg.alias, data), nil
}

func (gb *GroupBy) computeVar(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups
	data := make([]float64, numGroups)

	// First pass: compute sums
	sums := make([]float64, numGroups)
	for rowIdx, gid := range gb.rowGroupIDs {
		sums[gid] += toFloat64(col.Get(rowIdx))
	}

	// Compute means
	means := make([]float64, numGroups)
	for i := 0; i < numGroups; i++ {
		if gb.groupCounts[i] > 0 {
			means[i] = sums[i] / float64(gb.groupCounts[i])
		}
	}

	// Second pass: compute sum of squared differences
	sumSq := make([]float64, numGroups)
	for rowIdx, gid := range gb.rowGroupIDs {
		diff := toFloat64(col.Get(rowIdx)) - means[gid]
		sumSq[gid] += diff * diff
	}

	// Compute variance
	for i := 0; i < numGroups; i++ {
		if gb.groupCounts[i] < 2 {
			data[i] = math.NaN()
		} else {
			data[i] = sumSq[i] / float64(gb.groupCounts[i]-1) // Sample variance (ddof=1)
		}
	}

	return NewSeriesFloat64(agg.alias, data), nil
}

func (gb *GroupBy) computeMedian(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups
	data := make([]float64, numGroups)

	// Collect values per group
	groupValues := make([][]float64, numGroups)
	for i := 0; i < numGroups; i++ {
		groupValues[i] = make([]float64, 0, gb.groupCounts[i])
	}
	for rowIdx, gid := range gb.rowGroupIDs {
		groupValues[gid] = append(groupValues[gid], toFloat64(col.Get(rowIdx)))
	}

	// Compute median for each group
	for i := 0; i < numGroups; i++ {
		values := groupValues[i]
		if len(values) == 0 {
			data[i] = math.NaN()
			continue
		}

		sort.Float64s(values)
		n := len(values)
		if n%2 == 0 {
			data[i] = (values[n/2-1] + values[n/2]) / 2
		} else {
			data[i] = values[n/2]
		}
	}

	return NewSeriesFloat64(agg.alias, data), nil
}

func (gb *GroupBy) computeCountDistinct(agg Aggregation) (*Series, error) {
	col := gb.df.ColumnByName(agg.column)
	numGroups := gb.numGroups
	data := make([]int64, numGroups)

	// Collect unique values per group
	groupSeen := make([]map[interface{}]struct{}, numGroups)
	for i := 0; i < numGroups; i++ {
		groupSeen[i] = make(map[interface{}]struct{})
	}
	for rowIdx, gid := range gb.rowGroupIDs {
		groupSeen[gid][col.Get(rowIdx)] = struct{}{}
	}

	for i := 0; i < numGroups; i++ {
		data[i] = int64(len(groupSeen[i]))
	}

	return NewSeriesInt64(agg.alias, data), nil
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}
