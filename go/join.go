package galleon

import (
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// JoinType represents the type of join operation
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	OuterJoin
	CrossJoin
)

// JoinOptions configures join behavior
type JoinOptions struct {
	on       []string // Columns to join on (same name in both DataFrames)
	leftOn   []string // Left DataFrame join columns
	rightOn  []string // Right DataFrame join columns
	suffix   string   // Suffix for duplicate column names (default "_right")
	how      JoinType // Join type (default InnerJoin)
}

// DefaultJoinOptions returns default join options
func DefaultJoinOptions() JoinOptions {
	return JoinOptions{
		suffix: "_right",
		how:    InnerJoin,
	}
}

// On creates join options for joining on columns with the same name
func On(columns ...string) JoinOptions {
	return JoinOptions{
		on:     columns,
		suffix: "_right",
		how:    InnerJoin,
	}
}

// LeftOn creates join options with different column names for left and right
func LeftOn(columns ...string) JoinOptions {
	return JoinOptions{
		leftOn: columns,
		suffix: "_right",
		how:    InnerJoin,
	}
}

// RightOn specifies right DataFrame columns for the join
func (o JoinOptions) RightOn(columns ...string) JoinOptions {
	o.rightOn = columns
	return o
}

// WithSuffix sets the suffix for duplicate column names
func (o JoinOptions) WithSuffix(suffix string) JoinOptions {
	o.suffix = suffix
	return o
}

// Join performs an inner join with another DataFrame
func (df *DataFrame) Join(other *DataFrame, opts JoinOptions) (*DataFrame, error) {
	opts.how = InnerJoin
	return df.joinWith(other, opts)
}

// LeftJoin performs a left join with another DataFrame
func (df *DataFrame) LeftJoin(other *DataFrame, opts JoinOptions) (*DataFrame, error) {
	opts.how = LeftJoin
	return df.joinWith(other, opts)
}

// RightJoin performs a right join with another DataFrame
func (df *DataFrame) RightJoin(other *DataFrame, opts JoinOptions) (*DataFrame, error) {
	opts.how = RightJoin
	return df.joinWith(other, opts)
}

// OuterJoin performs a full outer join with another DataFrame
func (df *DataFrame) OuterJoin(other *DataFrame, opts JoinOptions) (*DataFrame, error) {
	opts.how = OuterJoin
	return df.joinWith(other, opts)
}

// CrossJoin performs a cross join (cartesian product) with another DataFrame
func (df *DataFrame) CrossJoin(other *DataFrame) (*DataFrame, error) {
	return df.crossJoin(other)
}

func (df *DataFrame) joinWith(other *DataFrame, opts JoinOptions) (*DataFrame, error) {
	// Resolve join columns
	leftCols, rightCols, err := resolveJoinColumns(df, other, opts)
	if err != nil {
		return nil, err
	}

	// Determine output columns
	outputCols, colMapping := resolveOutputColumns(df, other, leftCols, rightCols, opts.suffix)

	// Check for Zig fast path (single i64 key)
	if opts.how == InnerJoin && len(leftCols) == 1 {
		leftKeyCol := df.ColumnByName(leftCols[0])
		rightKeyCol := other.ColumnByName(rightCols[0])
		if leftKeyCol.DType() == Int64 && rightKeyCol.DType() == Int64 {
			// Use optimized Zig path - skip Go hash index build
			return performInnerJoinZigDirect(df, other, leftKeyCol, rightKeyCol, outputCols, colMapping)
		}
	}

	if opts.how == LeftJoin && len(leftCols) == 1 {
		leftKeyCol := df.ColumnByName(leftCols[0])
		rightKeyCol := other.ColumnByName(rightCols[0])
		if leftKeyCol.DType() == Int64 && rightKeyCol.DType() == Int64 {
			// Use optimized Zig path for left join
			return performLeftJoinZigDirect(df, other, leftKeyCol, rightKeyCol, outputCols, colMapping)
		}
	}

	// Build hash index on right DataFrame (only for non-Zig paths)
	rightIndex := buildHashIndex(other, rightCols)

	leftKeyCols := make([]*Series, len(leftCols))
	for i, name := range leftCols {
		leftKeyCols[i] = df.ColumnByName(name)
	}
	rightKeyCols := make([]*Series, len(rightCols))
	for i, name := range rightCols {
		rightKeyCols[i] = other.ColumnByName(name)
	}

	// Perform join based on type
	switch opts.how {
	case InnerJoin:
		return performInnerJoinGo(df, other, leftKeyCols, rightKeyCols, rightIndex, outputCols, colMapping)
	case LeftJoin:
		return performLeftJoinGo(df, other, leftKeyCols, rightKeyCols, rightIndex, outputCols, colMapping)
	case RightJoin:
		return performRightJoin(df, other, leftCols, rightCols, rightIndex, outputCols, colMapping)
	case OuterJoin:
		return performOuterJoin(df, other, leftCols, rightCols, rightIndex, outputCols, colMapping)
	default:
		return nil, fmt.Errorf("unknown join type: %d", opts.how)
	}
}

func resolveJoinColumns(left, right *DataFrame, opts JoinOptions) ([]string, []string, error) {
	var leftCols, rightCols []string

	if len(opts.on) > 0 {
		// Same column names in both DataFrames
		for _, col := range opts.on {
			if left.ColumnByName(col) == nil {
				return nil, nil, fmt.Errorf("column '%s' not found in left DataFrame", col)
			}
			if right.ColumnByName(col) == nil {
				return nil, nil, fmt.Errorf("column '%s' not found in right DataFrame", col)
			}
		}
		leftCols = opts.on
		rightCols = opts.on
	} else if len(opts.leftOn) > 0 && len(opts.rightOn) > 0 {
		// Different column names
		if len(opts.leftOn) != len(opts.rightOn) {
			return nil, nil, fmt.Errorf("leftOn and rightOn must have same length")
		}
		for _, col := range opts.leftOn {
			if left.ColumnByName(col) == nil {
				return nil, nil, fmt.Errorf("column '%s' not found in left DataFrame", col)
			}
		}
		for _, col := range opts.rightOn {
			if right.ColumnByName(col) == nil {
				return nil, nil, fmt.Errorf("column '%s' not found in right DataFrame", col)
			}
		}
		leftCols = opts.leftOn
		rightCols = opts.rightOn
	} else {
		return nil, nil, fmt.Errorf("must specify On or both LeftOn and RightOn")
	}

	return leftCols, rightCols, nil
}

// hashIndex maps hash values to row indices
type hashIndex struct {
	index  map[uint64][]int
	hasher maphash.Hash // Store hasher to ensure consistent seeding
}

// partitionedJoinIndex wraps PartitionedHashIndex for join operations
type partitionedJoinIndex struct {
	phi    *PartitionedHashIndex
	hashes []uint64 // Keep hashes for potential reuse
}

func buildHashIndex(df *DataFrame, keyCols []string) *hashIndex {
	idx := &hashIndex{
		index: make(map[uint64][]int),
	}

	cols := make([]*Series, len(keyCols))
	for i, name := range keyCols {
		cols[i] = df.ColumnByName(name)
	}

	height := df.Height()
	if height == 0 {
		return idx
	}

	// Use Zig SIMD hashing for column data
	hashes := computeJoinKeyHashes(cols, height)

	// Use partitioned parallel build for large datasets
	if ShouldParallelizeOp(OpJoinBuild, height) {
		phi := NewPartitionedHashIndex(0) // Use default partitions
		phi.BuildParallel(hashes)
		// Convert back to regular hashIndex for compatibility
		for p := 0; p < phi.numParts; p++ {
			for hash, rows := range phi.partitions[p] {
				idx.index[hash] = rows
			}
		}
	} else {
		// Sequential build for small datasets
		for rowIdx := 0; rowIdx < height; rowIdx++ {
			hash := hashes[rowIdx]
			idx.index[hash] = append(idx.index[hash], rowIdx)
		}
	}

	return idx
}

// buildPartitionedHashIndex builds a partitioned hash index for parallel probing
func buildPartitionedHashIndex(df *DataFrame, keyCols []string) (*PartitionedHashIndex, []uint64) {
	cols := make([]*Series, len(keyCols))
	for i, name := range keyCols {
		cols[i] = df.ColumnByName(name)
	}

	height := df.Height()
	if height == 0 {
		return NewPartitionedHashIndex(1), nil
	}

	hashes := computeJoinKeyHashes(cols, height)
	phi := NewPartitionedHashIndex(0)
	phi.BuildParallel(hashes)

	return phi, hashes
}

// computeJoinKeyHashes computes hashes for join keys using Zig SIMD
func computeJoinKeyHashes(cols []*Series, height int) []uint64 {
	if len(cols) == 0 || height == 0 {
		return nil
	}

	hashes := make([]uint64, height)

	// Hash first column
	hashColumn(cols[0], hashes)

	// Combine with subsequent columns
	if len(cols) > 1 {
		tempHashes := make([]uint64, height)
		for i := 1; i < len(cols); i++ {
			hashColumn(cols[i], tempHashes)
			CombineHashes(hashes, tempHashes, hashes)
		}
	}

	return hashes
}

// joinHashSeed is a fixed seed for deterministic string hashing in joins
var joinHashSeed = maphash.MakeSeed()

// hashColumn computes hashes for a single column using Zig SIMD
func hashColumn(col *Series, outHashes []uint64) {
	switch col.DType() {
	case Float64:
		HashF64Column(col.Float64(), outHashes)
	case Float32:
		HashF32Column(col.Float32(), outHashes)
	case Int64:
		HashI64Column(col.Int64(), outHashes)
	case Int32:
		HashI32Column(col.Int32(), outHashes)
	case String:
		// Use deterministic FNV-1a hashing for strings (same as Zig)
		data := col.Strings()
		for i := 0; i < len(data); i++ {
			outHashes[i] = fnvHashString(data[i])
		}
	case Bool:
		data := col.Bool()
		for i := 0; i < len(data); i++ {
			if data[i] {
				outHashes[i] = 1
			} else {
				outHashes[i] = 0
			}
		}
	case Categorical:
		// Hash the int32 indices using SIMD (much faster than string hashing!)
		HashI32Column(col.CategoricalIndices(), outHashes)
	default:
		// Fallback using deterministic string conversion
		for i := 0; i < col.Len(); i++ {
			outHashes[i] = fnvHashString(fmt.Sprintf("%v", col.Get(i)))
		}
	}
}

// fnvHashString computes FNV-1a hash for a string (matches Zig implementation)
func fnvHashString(s string) uint64 {
	const fnvOffset = uint64(0xcbf29ce484222325)
	const fnvPrime = uint64(0x100000001b3)

	h := fnvOffset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime
	}
	return h
}

func computeRowHash(h *maphash.Hash, cols []*Series, rowIdx int) uint64 {
	h.Reset()
	for _, col := range cols {
		h.WriteString(fmt.Sprintf("%v", col.Get(rowIdx)))
		h.WriteByte(0)
	}
	return h.Sum64()
}

// colMapping tracks how to build output columns
type colMapping struct {
	fromLeft  bool
	srcCol    int
	isJoinKey bool
}

func resolveOutputColumns(left, right *DataFrame, leftKeys, rightKeys []string, suffix string) ([]string, []colMapping) {
	var outputCols []string
	var mapping []colMapping

	// Build set of right join keys for quick lookup
	rightKeySet := make(map[string]bool)
	for _, k := range rightKeys {
		rightKeySet[k] = true
	}

	// Build set of left column names
	leftColSet := make(map[string]bool)
	for _, name := range left.Columns() {
		leftColSet[name] = true
	}

	// Add all left columns
	for i, name := range left.Columns() {
		outputCols = append(outputCols, name)
		isKey := false
		for _, k := range leftKeys {
			if name == k {
				isKey = true
				break
			}
		}
		mapping = append(mapping, colMapping{fromLeft: true, srcCol: i, isJoinKey: isKey})
	}

	// Add right columns (excluding join keys that match left keys)
	for i, name := range right.Columns() {
		// Skip if this is a join key that exists in left
		if rightKeySet[name] {
			// Check if left has same-named key
			for j, lk := range leftKeys {
				if rightKeys[j] == name && lk == name {
					// Same name in both, skip
					continue
				}
			}
			// Different name, include it
		}

		// Skip join keys with same name
		skip := false
		for j, rk := range rightKeys {
			if name == rk && leftKeys[j] == rk {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		// Handle name collision
		outputName := name
		if leftColSet[name] {
			outputName = name + suffix
		}

		outputCols = append(outputCols, outputName)
		mapping = append(mapping, colMapping{fromLeft: false, srcCol: i, isJoinKey: false})
	}

	return outputCols, mapping
}

func performInnerJoin(left, right *DataFrame, leftKeys, rightKeys []string, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftKeyCols := make([]*Series, len(leftKeys))
	for i, name := range leftKeys {
		leftKeyCols[i] = left.ColumnByName(name)
	}

	rightKeyCols := make([]*Series, len(rightKeys))
	for i, name := range rightKeys {
		rightKeyCols[i] = right.ColumnByName(name)
	}

	leftHeight := left.Height()
	rightHeight := right.Height()

	// Precompute hashes for left keys using Zig SIMD
	leftHashes := computeJoinKeyHashes(leftKeyCols, leftHeight)

	// Fast path: single int64 key - use full Zig pipeline
	if len(leftKeyCols) == 1 && leftKeyCols[0].DType() == Int64 && rightKeyCols[0].DType() == Int64 {
		return performInnerJoinZig(left, right, leftKeyCols[0], rightKeyCols[0], leftHashes, outputCols, mapping)
	}

	// Build right-side hashes for Zig path (if needed later)
	rightHashes := computeJoinKeyHashes(rightKeyCols, rightHeight)
	_ = rightHashes // Used for potential future optimizations

	// Use cost-based decision for parallelization
	if ShouldParallelizeOp(OpJoinProbe, leftHeight) {
		return performInnerJoinParallel(left, right, leftKeyCols, rightKeyCols, leftHashes, rightIndex, outputCols, mapping)
	}

	// Sequential path for small datasets
	var leftIndices, rightIndices []int

	for leftRow := 0; leftRow < leftHeight; leftRow++ {
		hash := leftHashes[leftRow]
		if rightRows, ok := rightIndex.index[hash]; ok {
			for _, rightRow := range rightRows {
				if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
					leftIndices = append(leftIndices, leftRow)
					rightIndices = append(rightIndices, rightRow)
				}
			}
		}
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

// performInnerJoinZig uses full Zig pipeline for single int64 key joins
func performInnerJoinZig(left, right *DataFrame, leftKeyCol, rightKeyCol *Series, leftHashes []uint64, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()
	rightHeight := right.Height()

	// Handle empty DataFrames
	if leftHeight == 0 || rightHeight == 0 {
		return buildJoinResult(left, right, outputCols, mapping, nil, nil)
	}

	leftKeys := leftKeyCol.Int64()
	rightKeys := rightKeyCol.Int64()

	// Build hash table using Zig
	// Use power of 2 size for efficient modulo
	tableSize := uint32(1)
	for tableSize < uint32(rightHeight*2) {
		tableSize *= 2
	}

	table := make([]int32, tableSize)
	next := make([]int32, rightHeight)

	// Compute right hashes and build hash table
	rightHashes := make([]uint64, rightHeight)
	HashI64Column(rightKeys, rightHashes)
	BuildJoinHashTable(rightHashes, table, next, tableSize)

	// Estimate max matches (could be up to leftHeight * rightHeight for many-to-many)
	// Use a reasonable upper bound
	maxMatches := uint32(leftHeight + rightHeight) * 2
	if maxMatches < 1024 {
		maxMatches = 1024
	}

	outProbeIndices := make([]int32, maxMatches)
	outBuildIndices := make([]int32, maxMatches)

	// Probe using Zig
	numMatches := ProbeJoinHashTable(
		leftHashes, leftKeys, rightKeys,
		table, next, tableSize,
		outProbeIndices, outBuildIndices, maxMatches,
	)

	// Convert to int slices for buildJoinResult
	leftIndices := make([]int, numMatches)
	rightIndices := make([]int, numMatches)
	for i := uint32(0); i < numMatches; i++ {
		leftIndices[i] = int(outProbeIndices[i])
		rightIndices[i] = int(outBuildIndices[i])
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

func performInnerJoinParallel(left, right *DataFrame, leftKeyCols, rightKeyCols []*Series, leftHashes []uint64, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()
	cfg := globalConfig
	numWorkers := cfg.numWorkers()

	// Use morsel-based work-stealing
	morselIter := NewMorselIterator(leftHeight, cfg.MorselSize)
	workerResults := make([][]JoinMatch, numWorkers)
	workerIdx := int64(0)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			myIdx := atomic.AddInt64(&workerIdx, 1) - 1
			var matches []JoinMatch

			// Work-stealing: grab morsels until exhausted
			for {
				morsel := morselIter.Next()
				if morsel == nil {
					break
				}

				for leftRow := morsel.Start; leftRow < morsel.End; leftRow++ {
					hash := leftHashes[leftRow]
					if rightRows, ok := rightIndex.index[hash]; ok {
						for _, rightRow := range rightRows {
							if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
								matches = append(matches, JoinMatch{leftRow, rightRow})
							}
						}
					}
				}
			}
			workerResults[myIdx] = matches
		}()
	}

	wg.Wait()

	// Merge results
	totalMatches := 0
	for _, results := range workerResults {
		totalMatches += len(results)
	}

	leftIndices := make([]int, 0, totalMatches)
	rightIndices := make([]int, 0, totalMatches)

	for _, results := range workerResults {
		for _, m := range results {
			leftIndices = append(leftIndices, m.LeftIdx)
			rightIndices = append(rightIndices, m.RightIdx)
		}
	}

	return buildJoinResultParallel(left, right, outputCols, mapping, leftIndices, rightIndices)
}

// performInnerJoinZigDirect uses optimized Zig pipeline directly, skipping Go hash index
func performInnerJoinZigDirect(left, right *DataFrame, leftKeyCol, rightKeyCol *Series, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()
	rightHeight := right.Height()

	// Handle empty DataFrames
	if leftHeight == 0 || rightHeight == 0 {
		return buildJoinResult(left, right, outputCols, mapping, nil, nil)
	}

	leftKeys := leftKeyCol.Int64()
	rightKeys := rightKeyCol.Int64()

	// Single CGO call: hash, build, probe all in Zig
	leftIndices, rightIndices := InnerJoinI64(leftKeys, rightKeys)

	return buildJoinResultParallel(left, right, outputCols, mapping, leftIndices, rightIndices)
}

// performLeftJoinZigDirect uses optimized Zig pipeline for left join
func performLeftJoinZigDirect(left, right *DataFrame, leftKeyCol, rightKeyCol *Series, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()
	rightHeight := right.Height()

	// Handle empty right DataFrame - return all left rows with nulls
	if rightHeight == 0 {
		leftIndices := make([]int, leftHeight)
		rightIndices := make([]int, leftHeight)
		for i := 0; i < leftHeight; i++ {
			leftIndices[i] = i
			rightIndices[i] = -1
		}
		return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
	}

	if leftHeight == 0 {
		return buildJoinResult(left, right, outputCols, mapping, nil, nil)
	}

	leftKeys := leftKeyCol.Int64()
	rightKeys := rightKeyCol.Int64()

	// Single CGO call for left join
	leftIndices, rightIndices := LeftJoinI64(leftKeys, rightKeys)

	return buildJoinResultParallel(left, right, outputCols, mapping, leftIndices, rightIndices)
}

// performInnerJoinGo is the Go fallback for non-i64 keys
func performInnerJoinGo(left, right *DataFrame, leftKeyCols, rightKeyCols []*Series, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()

	// Precompute hashes for left keys using Zig SIMD
	leftHashes := computeJoinKeyHashes(leftKeyCols, leftHeight)

	// Use cost-based decision for parallelization
	if ShouldParallelizeOp(OpJoinProbe, leftHeight) {
		return performInnerJoinParallel(left, right, leftKeyCols, rightKeyCols, leftHashes, rightIndex, outputCols, mapping)
	}

	// Sequential path for small datasets
	var leftIndices, rightIndices []int

	for leftRow := 0; leftRow < leftHeight; leftRow++ {
		hash := leftHashes[leftRow]
		if rightRows, ok := rightIndex.index[hash]; ok {
			for _, rightRow := range rightRows {
				if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
					leftIndices = append(leftIndices, leftRow)
					rightIndices = append(rightIndices, rightRow)
				}
			}
		}
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

// performLeftJoinGo is the Go fallback for non-i64 keys
func performLeftJoinGo(left, right *DataFrame, leftKeyCols, rightKeyCols []*Series, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()

	// Precompute hashes for left keys using Zig SIMD
	leftHashes := computeJoinKeyHashes(leftKeyCols, leftHeight)

	// Use cost-based decision for parallelization
	if ShouldParallelizeOp(OpJoinProbe, leftHeight) {
		return performLeftJoinParallel(left, right, leftKeyCols, rightKeyCols, leftHashes, rightIndex, outputCols, mapping)
	}

	// Sequential path for small datasets
	var leftIndices, rightIndices []int

	for leftRow := 0; leftRow < leftHeight; leftRow++ {
		hash := leftHashes[leftRow]
		matched := false
		if rightRows, ok := rightIndex.index[hash]; ok {
			for _, rightRow := range rightRows {
				if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
					leftIndices = append(leftIndices, leftRow)
					rightIndices = append(rightIndices, rightRow)
					matched = true
				}
			}
		}
		if !matched {
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1)
		}
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

func performLeftJoin(left, right *DataFrame, leftKeys, rightKeys []string, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftKeyCols := make([]*Series, len(leftKeys))
	for i, name := range leftKeys {
		leftKeyCols[i] = left.ColumnByName(name)
	}

	rightKeyCols := make([]*Series, len(rightKeys))
	for i, name := range rightKeys {
		rightKeyCols[i] = right.ColumnByName(name)
	}

	leftHeight := left.Height()

	// Precompute hashes for left keys using Zig SIMD
	leftHashes := computeJoinKeyHashes(leftKeyCols, leftHeight)

	// Use cost-based decision for parallelization
	if ShouldParallelizeOp(OpJoinProbe, leftHeight) {
		return performLeftJoinParallel(left, right, leftKeyCols, rightKeyCols, leftHashes, rightIndex, outputCols, mapping)
	}

	// Sequential path for small datasets
	var leftIndices, rightIndices []int

	for leftRow := 0; leftRow < leftHeight; leftRow++ {
		hash := leftHashes[leftRow]
		matched := false
		if rightRows, ok := rightIndex.index[hash]; ok {
			for _, rightRow := range rightRows {
				if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
					leftIndices = append(leftIndices, leftRow)
					rightIndices = append(rightIndices, rightRow)
					matched = true
				}
			}
		}
		if !matched {
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1)
		}
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

func performLeftJoinParallel(left, right *DataFrame, leftKeyCols, rightKeyCols []*Series, leftHashes []uint64, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftHeight := left.Height()
	cfg := globalConfig
	numWorkers := cfg.numWorkers()

	// Use morsel-based work-stealing
	morselIter := NewMorselIterator(leftHeight, cfg.MorselSize)
	workerResults := make([][]JoinMatch, numWorkers)
	workerIdx := int64(0)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			myIdx := atomic.AddInt64(&workerIdx, 1) - 1
			var matches []JoinMatch

			for {
				morsel := morselIter.Next()
				if morsel == nil {
					break
				}

				for leftRow := morsel.Start; leftRow < morsel.End; leftRow++ {
					hash := leftHashes[leftRow]
					matched := false
					if rightRows, ok := rightIndex.index[hash]; ok {
						for _, rightRow := range rightRows {
							if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
								matches = append(matches, JoinMatch{leftRow, rightRow})
								matched = true
							}
						}
					}
					if !matched {
						matches = append(matches, JoinMatch{leftRow, -1})
					}
				}
			}
			workerResults[myIdx] = matches
		}()
	}

	wg.Wait()

	// Merge results
	totalMatches := 0
	for _, results := range workerResults {
		totalMatches += len(results)
	}

	leftIndices := make([]int, 0, totalMatches)
	rightIndices := make([]int, 0, totalMatches)

	for _, results := range workerResults {
		for _, m := range results {
			leftIndices = append(leftIndices, m.LeftIdx)
			rightIndices = append(rightIndices, m.RightIdx)
		}
	}

	return buildJoinResultParallel(left, right, outputCols, mapping, leftIndices, rightIndices)
}

func performRightJoin(left, right *DataFrame, leftKeys, rightKeys []string, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	// Build index on left instead
	leftIndex := buildHashIndex(left, leftKeys)

	leftKeyCols := make([]*Series, len(leftKeys))
	for i, name := range leftKeys {
		leftKeyCols[i] = left.ColumnByName(name)
	}

	rightKeyCols := make([]*Series, len(rightKeys))
	for i, name := range rightKeys {
		rightKeyCols[i] = right.ColumnByName(name)
	}

	rightHeight := right.Height()

	// Precompute hashes for right keys using Zig SIMD
	rightHashes := computeJoinKeyHashes(rightKeyCols, rightHeight)

	var leftIndices, rightIndices []int

	for rightRow := 0; rightRow < rightHeight; rightRow++ {
		hash := rightHashes[rightRow]
		matched := false
		if leftRows, ok := leftIndex.index[hash]; ok {
			for _, leftRow := range leftRows {
				if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
					leftIndices = append(leftIndices, leftRow)
					rightIndices = append(rightIndices, rightRow)
					matched = true
				}
			}
		}
		if !matched {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, rightRow)
		}
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

func performOuterJoin(left, right *DataFrame, leftKeys, rightKeys []string, rightIndex *hashIndex, outputCols []string, mapping []colMapping) (*DataFrame, error) {
	leftKeyCols := make([]*Series, len(leftKeys))
	for i, name := range leftKeys {
		leftKeyCols[i] = left.ColumnByName(name)
	}

	rightKeyCols := make([]*Series, len(rightKeys))
	for i, name := range rightKeys {
		rightKeyCols[i] = right.ColumnByName(name)
	}

	leftHeight := left.Height()

	// Precompute hashes for left keys using Zig SIMD
	leftHashes := computeJoinKeyHashes(leftKeyCols, leftHeight)

	var leftIndices, rightIndices []int

	// Track which right rows have been matched
	rightMatched := make([]bool, right.Height())

	// First pass: all left rows
	for leftRow := 0; leftRow < leftHeight; leftRow++ {
		hash := leftHashes[leftRow]
		matched := false
		if rightRows, ok := rightIndex.index[hash]; ok {
			for _, rightRow := range rightRows {
				if keysMatch(leftKeyCols, leftRow, rightKeyCols, rightRow) {
					leftIndices = append(leftIndices, leftRow)
					rightIndices = append(rightIndices, rightRow)
					rightMatched[rightRow] = true
					matched = true
				}
			}
		}
		if !matched {
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, -1)
		}
	}

	// Second pass: unmatched right rows
	for rightRow := 0; rightRow < right.Height(); rightRow++ {
		if !rightMatched[rightRow] {
			leftIndices = append(leftIndices, -1)
			rightIndices = append(rightIndices, rightRow)
		}
	}

	return buildJoinResult(left, right, outputCols, mapping, leftIndices, rightIndices)
}

func (df *DataFrame) crossJoin(other *DataFrame) (*DataFrame, error) {
	// Resolve output columns (no join keys)
	var outputCols []string
	var mapping []colMapping

	leftColSet := make(map[string]bool)
	for _, name := range df.Columns() {
		leftColSet[name] = true
	}

	// Add all left columns
	for i, name := range df.Columns() {
		outputCols = append(outputCols, name)
		mapping = append(mapping, colMapping{fromLeft: true, srcCol: i})
	}

	// Add all right columns
	for i, name := range other.Columns() {
		outputName := name
		if leftColSet[name] {
			outputName = name + "_right"
		}
		outputCols = append(outputCols, outputName)
		mapping = append(mapping, colMapping{fromLeft: false, srcCol: i})
	}

	// Generate all row pairs
	var leftIndices, rightIndices []int
	for leftRow := 0; leftRow < df.Height(); leftRow++ {
		for rightRow := 0; rightRow < other.Height(); rightRow++ {
			leftIndices = append(leftIndices, leftRow)
			rightIndices = append(rightIndices, rightRow)
		}
	}

	return buildJoinResult(df, other, outputCols, mapping, leftIndices, rightIndices)
}

func keysMatch(leftCols []*Series, leftRow int, rightCols []*Series, rightRow int) bool {
	for i := range leftCols {
		if !valuesEqual(leftCols[i], leftRow, rightCols[i], rightRow) {
			return false
		}
	}
	return true
}

// valuesEqual compares values at specific rows without fmt.Sprintf
func valuesEqual(left *Series, leftRow int, right *Series, rightRow int) bool {
	// Fast path: same type
	if left.DType() == right.DType() {
		switch left.DType() {
		case Float64:
			return left.Float64()[leftRow] == right.Float64()[rightRow]
		case Float32:
			return left.Float32()[leftRow] == right.Float32()[rightRow]
		case Int64:
			return left.Int64()[leftRow] == right.Int64()[rightRow]
		case Int32:
			return left.Int32()[leftRow] == right.Int32()[rightRow]
		case Bool:
			return left.Bool()[leftRow] == right.Bool()[rightRow]
		case String:
			return left.Strings()[leftRow] == right.Strings()[rightRow]
		case Categorical:
			// Compare actual string values (dictionaries may be different)
			leftCategories := left.Categories()
			rightCategories := right.Categories()
			leftIdx := left.CategoricalIndices()[leftRow]
			rightIdx := right.CategoricalIndices()[rightRow]
			return leftCategories[leftIdx] == rightCategories[rightIdx]
		}
	}
	// Fallback for mixed types
	return fmt.Sprintf("%v", left.Get(leftRow)) == fmt.Sprintf("%v", right.Get(rightRow))
}

func buildJoinResult(left, right *DataFrame, outputCols []string, mapping []colMapping, leftIndices, rightIndices []int) (*DataFrame, error) {
	numRows := len(leftIndices)
	resultCols := make([]*Series, len(outputCols))

	for colIdx, colName := range outputCols {
		m := mapping[colIdx]
		var srcDF *DataFrame
		var indices []int

		if m.fromLeft {
			srcDF = left
			indices = leftIndices
		} else {
			srcDF = right
			indices = rightIndices
		}

		srcCol := srcDF.Column(m.srcCol)
		resultCols[colIdx] = buildJoinColumn(colName, srcCol, indices, numRows)
	}

	return NewDataFrame(resultCols...)
}

// buildJoinResultParallel builds result columns in parallel
func buildJoinResultParallel(left, right *DataFrame, outputCols []string, mapping []colMapping, leftIndices, rightIndices []int) (*DataFrame, error) {
	numRows := len(leftIndices)
	numCols := len(outputCols)
	resultCols := make([]*Series, numCols)

	// Parallel column building
	var wg sync.WaitGroup
	for colIdx := 0; colIdx < numCols; colIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			colName := outputCols[idx]
			m := mapping[idx]
			var srcDF *DataFrame
			var indices []int

			if m.fromLeft {
				srcDF = left
				indices = leftIndices
			} else {
				srcDF = right
				indices = rightIndices
			}

			srcCol := srcDF.Column(m.srcCol)
			resultCols[idx] = buildJoinColumn(colName, srcCol, indices, numRows)
		}(colIdx)
	}

	wg.Wait()

	return NewDataFrame(resultCols...)
}

func buildJoinColumn(name string, src *Series, indices []int, numRows int) *Series {
	// Convert indices to int32 for Zig gather functions
	indices32 := make([]int32, numRows)
	for i, idx := range indices {
		indices32[i] = int32(idx)
	}

	switch src.DType() {
	case Float64:
		data := make([]float64, numRows)
		srcData := src.Float64()
		// Use Zig SIMD gather
		GatherF64(srcData, indices32, data)
		return NewSeriesFloat64(name, data)

	case Float32:
		data := make([]float32, numRows)
		srcData := src.Float32()
		// Use Zig SIMD gather
		GatherF32(srcData, indices32, data)
		return NewSeriesFloat32(name, data)

	case Int64:
		data := make([]int64, numRows)
		srcData := src.Int64()
		// Use Zig SIMD gather
		GatherI64(srcData, indices32, data)
		return NewSeriesInt64(name, data)

	case Int32:
		data := make([]int32, numRows)
		srcData := src.Int32()
		// Use Zig SIMD gather
		GatherI32(srcData, indices32, data)
		return NewSeriesInt32(name, data)

	case Bool:
		data := make([]bool, numRows)
		srcData := src.Bool()
		for i, idx := range indices {
			if idx >= 0 {
				data[i] = srcData[idx]
			}
		}
		return NewSeriesBool(name, data)

	case String:
		data := make([]string, numRows)
		srcData := src.Strings()
		for i, idx := range indices {
			if idx >= 0 {
				data[i] = srcData[idx]
			}
		}
		return NewSeriesString(name, data)

	case Categorical:
		// Gather indices and keep the same dictionary
		srcIndices := src.CategoricalIndices()
		categories := src.Categories()
		newIndices := make([]int32, numRows)
		// Use SIMD gather for int32 indices
		GatherI32(srcIndices, indices32, newIndices)
		// Create new categorical with same dictionary
		result, _ := NewSeriesCategoricalWithCategories(name, nil, categories)
		if result != nil {
			result.catData.Indices = newIndices
			result.length = numRows
			return result
		}
		// Fallback: reconstruct from strings
		data := make([]string, numRows)
		for i, idx := range indices {
			if idx >= 0 && srcIndices[idx] >= 0 && int(srcIndices[idx]) < len(categories) {
				data[i] = categories[srcIndices[idx]]
			}
		}
		return NewSeriesCategorical(name, data)

	default:
		// Fallback to string
		data := make([]string, numRows)
		for i, idx := range indices {
			if idx >= 0 {
				data[i] = fmt.Sprintf("%v", src.Get(idx))
			}
		}
		return NewSeriesString(name, data)
	}
}
