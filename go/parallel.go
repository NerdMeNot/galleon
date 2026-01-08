package galleon

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// ============================================================================
// Parallel Execution Configuration
// ============================================================================

// ParallelConfig controls parallelization behavior
type ParallelConfig struct {
	// MinRowsForParallel is the minimum rows to justify parallel overhead
	MinRowsForParallel int

	// MorselSize is the number of rows per work unit (default 4096)
	MorselSize int

	// MaxWorkers limits the number of worker goroutines (0 = GOMAXPROCS)
	MaxWorkers int

	// Enabled controls whether parallelism is used at all
	Enabled bool
}

// DefaultParallelConfig returns sensible defaults
func DefaultParallelConfig() *ParallelConfig {
	return &ParallelConfig{
		MinRowsForParallel: 8192,  // ~8K rows minimum
		MorselSize:         4096,  // ~4K rows per morsel
		MaxWorkers:         0,     // Use all CPUs
		Enabled:            true,
	}
}

// globalConfig is the default configuration
var globalConfig = DefaultParallelConfig()

// SetParallelConfig sets the global parallelization configuration
func SetParallelConfig(cfg *ParallelConfig) {
	if cfg != nil {
		globalConfig = cfg
	}
}

// GetParallelConfig returns the current configuration
func GetParallelConfig() *ParallelConfig {
	return globalConfig
}

// numWorkers returns the number of workers to use
func (cfg *ParallelConfig) numWorkers() int {
	if cfg.MaxWorkers > 0 {
		return cfg.MaxWorkers
	}
	return runtime.GOMAXPROCS(0)
}

// shouldParallelize determines if an operation should be parallelized
func (cfg *ParallelConfig) shouldParallelize(rows int) bool {
	return cfg.Enabled && rows >= cfg.MinRowsForParallel
}

// ============================================================================
// Morsel-Based Work Distribution
// ============================================================================

// Morsel represents a range of rows to process
type Morsel struct {
	Start int
	End   int
}

// MorselIterator provides work-stealing morsel distribution
type MorselIterator struct {
	totalRows  int
	morselSize int
	nextStart  int64 // atomic counter for work-stealing
}

// NewMorselIterator creates a new morsel iterator
func NewMorselIterator(totalRows, morselSize int) *MorselIterator {
	if morselSize <= 0 {
		morselSize = globalConfig.MorselSize
	}
	return &MorselIterator{
		totalRows:  totalRows,
		morselSize: morselSize,
		nextStart:  0,
	}
}

// Next returns the next morsel, or nil if exhausted
// This is safe for concurrent use (work-stealing)
func (mi *MorselIterator) Next() *Morsel {
	for {
		start := atomic.LoadInt64(&mi.nextStart)
		if int(start) >= mi.totalRows {
			return nil
		}

		end := int(start) + mi.morselSize
		if end > mi.totalRows {
			end = mi.totalRows
		}

		// Try to claim this morsel
		if atomic.CompareAndSwapInt64(&mi.nextStart, start, int64(end)) {
			return &Morsel{Start: int(start), End: end}
		}
		// Another worker claimed it, try again
	}
}

// ============================================================================
// Parallel Execution Helpers
// ============================================================================

// ParallelFor executes fn for each morsel in parallel using work-stealing
func ParallelFor(totalRows int, fn func(start, end int)) {
	cfg := globalConfig
	if !cfg.shouldParallelize(totalRows) {
		// Sequential execution
		fn(0, totalRows)
		return
	}

	numWorkers := cfg.numWorkers()
	morselIter := NewMorselIterator(totalRows, cfg.MorselSize)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				morsel := morselIter.Next()
				if morsel == nil {
					return
				}
				fn(morsel.Start, morsel.End)
			}
		}()
	}
	wg.Wait()
}

// ParallelForWithResult executes fn for each morsel and collects results
func ParallelForWithResult[T any](totalRows int, fn func(start, end int) T) []T {
	cfg := globalConfig
	if !cfg.shouldParallelize(totalRows) {
		// Sequential execution
		return []T{fn(0, totalRows)}
	}

	numWorkers := cfg.numWorkers()
	morselIter := NewMorselIterator(totalRows, cfg.MorselSize)

	// Pre-calculate number of morsels for result slice
	numMorsels := (totalRows + cfg.MorselSize - 1) / cfg.MorselSize
	results := make([]T, numMorsels)
	resultIdx := int64(0)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				morsel := morselIter.Next()
				if morsel == nil {
					return
				}
				result := fn(morsel.Start, morsel.End)
				idx := atomic.AddInt64(&resultIdx, 1) - 1
				if int(idx) < len(results) {
					results[idx] = result
				}
			}
		}()
	}
	wg.Wait()

	// Trim to actual number of results
	actualResults := atomic.LoadInt64(&resultIdx)
	if int(actualResults) < len(results) {
		results = results[:actualResults]
	}
	return results
}

// ParallelMap applies fn to each index in parallel
func ParallelMap[T any](n int, fn func(i int) T) []T {
	results := make([]T, n)

	cfg := globalConfig
	if !cfg.shouldParallelize(n) {
		for i := 0; i < n; i++ {
			results[i] = fn(i)
		}
		return results
	}

	ParallelFor(n, func(start, end int) {
		for i := start; i < end; i++ {
			results[i] = fn(i)
		}
	})
	return results
}

// ParallelMapSlice applies fn to each element in parallel
func ParallelMapSlice[T, R any](slice []T, fn func(T) R) []R {
	return ParallelMap(len(slice), func(i int) R {
		return fn(slice[i])
	})
}

// ============================================================================
// Partitioned Hash Table (Lock-Free)
// ============================================================================

// PartitionedHashIndex is a lock-free partitioned hash table
// Each partition handles keys where: hash % numPartitions == partitionID
type PartitionedHashIndex struct {
	partitions []map[uint64][]int
	numParts   int
}

// NewPartitionedHashIndex creates a new partitioned hash index
func NewPartitionedHashIndex(numPartitions int) *PartitionedHashIndex {
	if numPartitions <= 0 {
		numPartitions = globalConfig.numWorkers()
	}
	// Ensure power of 2 for fast modulo
	numPartitions = nextPowerOf2(numPartitions)

	partitions := make([]map[uint64][]int, numPartitions)
	for i := range partitions {
		partitions[i] = make(map[uint64][]int)
	}
	return &PartitionedHashIndex{
		partitions: partitions,
		numParts:   numPartitions,
	}
}

// nextPowerOf2 returns the next power of 2 >= n
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

// partition returns which partition a hash belongs to
func (phi *PartitionedHashIndex) partition(hash uint64) int {
	// Fast modulo for power of 2
	return int(hash) & (phi.numParts - 1)
}

// BuildParallel builds the hash index from hashes in parallel
// Each partition is built by a separate goroutine (no locks needed)
func (phi *PartitionedHashIndex) BuildParallel(hashes []uint64) {
	numWorkers := phi.numParts

	var wg sync.WaitGroup
	for p := 0; p < numWorkers; p++ {
		wg.Add(1)
		go func(partID int) {
			defer wg.Done()
			table := phi.partitions[partID]
			// Each worker scans ALL hashes but only processes ones belonging to its partition
			for rowIdx, hash := range hashes {
				if phi.partition(hash) == partID {
					table[hash] = append(table[hash], rowIdx)
				}
			}
		}(p)
	}
	wg.Wait()
}

// Lookup returns all row indices matching the hash
func (phi *PartitionedHashIndex) Lookup(hash uint64) []int {
	partID := phi.partition(hash)
	return phi.partitions[partID][hash]
}

// ============================================================================
// Parallel Reduce Operations
// ============================================================================

// ParallelReduceFloat64 reduces a slice using work-stealing
func ParallelReduceFloat64(data []float64, identity float64, combine func(a, b float64) float64) float64 {
	cfg := globalConfig
	if !cfg.shouldParallelize(len(data)) {
		result := identity
		for _, v := range data {
			result = combine(result, v)
		}
		return result
	}

	// Compute partial results per morsel
	partials := ParallelForWithResult(len(data), func(start, end int) float64 {
		result := identity
		for i := start; i < end; i++ {
			result = combine(result, data[i])
		}
		return result
	})

	// Combine partial results
	result := identity
	for _, p := range partials {
		result = combine(result, p)
	}
	return result
}

// ParallelReduceInt64 reduces a slice using work-stealing
func ParallelReduceInt64(data []int64, identity int64, combine func(a, b int64) int64) int64 {
	cfg := globalConfig
	if !cfg.shouldParallelize(len(data)) {
		result := identity
		for _, v := range data {
			result = combine(result, v)
		}
		return result
	}

	partials := ParallelForWithResult(len(data), func(start, end int) int64 {
		result := identity
		for i := start; i < end; i++ {
			result = combine(result, data[i])
		}
		return result
	})

	result := identity
	for _, p := range partials {
		result = combine(result, p)
	}
	return result
}

// ============================================================================
// Parallel Column Operations
// ============================================================================

// ParallelBuildColumns builds multiple columns in parallel
func ParallelBuildColumns(n int, builder func(colIdx int) *Series) []*Series {
	cfg := globalConfig
	if !cfg.Enabled || n <= 1 {
		cols := make([]*Series, n)
		for i := 0; i < n; i++ {
			cols[i] = builder(i)
		}
		return cols
	}

	cols := make([]*Series, n)
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cols[idx] = builder(idx)
		}(i)
	}

	wg.Wait()
	return cols
}

// ============================================================================
// Cost-Based Parallelization Decisions
// ============================================================================

// OperationType represents different operation types for cost estimation
type OperationType int

const (
	OpFilter OperationType = iota
	OpSort
	OpJoinBuild
	OpJoinProbe
	OpGroupByHash
	OpGroupByAgg
	OpGather
)

// EstimatedCostPerRow returns nanoseconds per row for an operation
func EstimatedCostPerRow(op OperationType) int {
	switch op {
	case OpFilter:
		return 2 // Very fast with SIMD
	case OpSort:
		return 50 // O(n log n) amortized
	case OpJoinBuild:
		return 20 // Hash + map insert
	case OpJoinProbe:
		return 30 // Hash + map lookup + key compare
	case OpGroupByHash:
		return 15 // Hash computation
	case OpGroupByAgg:
		return 5 // Accumulation
	case OpGather:
		return 3 // Memory copy
	default:
		return 10
	}
}

// ShouldParallelizeOp decides based on operation type and data size
func ShouldParallelizeOp(op OperationType, rows int) bool {
	cfg := globalConfig
	if !cfg.Enabled {
		return false
	}

	// Estimate total work in nanoseconds
	totalWorkNs := rows * EstimatedCostPerRow(op)

	// Overhead of spawning goroutines + synchronization (~5μs per worker)
	numWorkers := cfg.numWorkers()
	overheadNs := 5000 * numWorkers

	// Only parallelize if work is at least 10x the overhead
	return totalWorkNs > overheadNs*10
}

// ============================================================================
// Parallel Join Match Collection
// ============================================================================

// JoinMatch represents a matching pair of row indices
type JoinMatch struct {
	LeftIdx  int
	RightIdx int
}

// CollectJoinMatches collects join matches in parallel using work-stealing
func CollectJoinMatches(
	leftHeight int,
	leftHashes []uint64,
	rightIndex *PartitionedHashIndex,
	matchFn func(leftRow int, rightRows []int) []JoinMatch,
) []JoinMatch {
	cfg := globalConfig

	if !cfg.shouldParallelize(leftHeight) {
		// Sequential path
		var matches []JoinMatch
		for leftRow := 0; leftRow < leftHeight; leftRow++ {
			hash := leftHashes[leftRow]
			rightRows := rightIndex.Lookup(hash)
			if len(rightRows) > 0 {
				matches = append(matches, matchFn(leftRow, rightRows)...)
			}
		}
		return matches
	}

	// Parallel path with work-stealing
	numWorkers := cfg.numWorkers()
	workerMatches := make([][]JoinMatch, numWorkers)
	workerIdx := int64(0)

	morselIter := NewMorselIterator(leftHeight, cfg.MorselSize)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			myIdx := atomic.AddInt64(&workerIdx, 1) - 1
			var myMatches []JoinMatch

			for {
				morsel := morselIter.Next()
				if morsel == nil {
					break
				}

				for leftRow := morsel.Start; leftRow < morsel.End; leftRow++ {
					hash := leftHashes[leftRow]
					rightRows := rightIndex.Lookup(hash)
					if len(rightRows) > 0 {
						myMatches = append(myMatches, matchFn(leftRow, rightRows)...)
					}
				}
			}

			workerMatches[myIdx] = myMatches
		}()
	}
	wg.Wait()

	// Merge results
	totalMatches := 0
	for _, wm := range workerMatches {
		totalMatches += len(wm)
	}

	result := make([]JoinMatch, 0, totalMatches)
	for _, wm := range workerMatches {
		result = append(result, wm...)
	}

	return result
}
