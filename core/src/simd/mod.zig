//! SIMD-accelerated operations for the Galleon DataFrame library.
//!
//! This module provides high-performance vectorized operations for aggregations,
//! arithmetic, comparisons, filtering, hashing, sorting, joins, and groupby operations.

const std = @import("std");

// Re-export all submodules
pub const core = @import("core.zig");
pub const aggregations = @import("aggregations.zig");
pub const arithmetic = @import("arithmetic.zig");
pub const comparisons = @import("comparisons.zig");
pub const conditionals = @import("conditionals.zig");
pub const statistics = @import("statistics.zig");
pub const window = @import("window.zig");
pub const fold = @import("fold.zig");
pub const filters = @import("filters.zig");
pub const hashing = @import("hashing.zig");
pub const gather = @import("gather.zig");
pub const groupby_agg = @import("groupby_agg.zig");
pub const sorting = @import("sorting.zig");

// Re-export commonly used constants
pub const VECTOR_WIDTH = core.VECTOR_WIDTH;
pub const UNROLL_FACTOR = core.UNROLL_FACTOR;
pub const CHUNK_SIZE = core.CHUNK_SIZE;
pub const MAX_THREADS = core.MAX_THREADS;

// Re-export thread config functions
pub const setMaxThreads = core.setMaxThreads;
pub const getThreadConfig = core.getThreadConfig;
pub const getMaxThreads = core.getMaxThreads;

// Re-export aggregation functions
pub const sum = aggregations.sum;
pub const min = aggregations.min;
pub const max = aggregations.max;
pub const mean = aggregations.mean;
pub const variance = aggregations.variance;
pub const stdDev = aggregations.stdDev;
pub const sumInt = aggregations.sumInt;
pub const minInt = aggregations.minInt;
pub const maxInt = aggregations.maxInt;

// Re-export arithmetic functions
pub const addScalar = arithmetic.addScalar;
pub const mulScalar = arithmetic.mulScalar;
pub const addArrays = arithmetic.addArrays;
pub const addArraysOut = arithmetic.addArraysOut;
pub const subArrays = arithmetic.subArrays;
pub const mulArrays = arithmetic.mulArrays;
pub const divArrays = arithmetic.divArrays;
pub const addScalarInt = arithmetic.addScalarInt;
pub const mulScalarInt = arithmetic.mulScalarInt;

// Re-export comparison functions
pub const cmpGt = comparisons.cmpGt;
pub const cmpGe = comparisons.cmpGe;
pub const cmpLt = comparisons.cmpLt;
pub const cmpLe = comparisons.cmpLe;
pub const cmpEq = comparisons.cmpEq;
pub const cmpNe = comparisons.cmpNe;

// Re-export conditional functions
pub const selectF64 = conditionals.selectF64;
pub const selectI64 = conditionals.selectI64;
pub const selectScalarF64 = conditionals.selectScalarF64;
pub const isNullF64 = conditionals.isNullF64;
pub const isNotNullF64 = conditionals.isNotNullF64;
pub const fillNullF64 = conditionals.fillNullF64;
pub const fillNullForwardF64 = conditionals.fillNullForwardF64;
pub const fillNullBackwardF64 = conditionals.fillNullBackwardF64;
pub const coalesceF64 = conditionals.coalesceF64;
pub const coalesce2F64 = conditionals.coalesce2F64;
pub const countNullF64 = conditionals.countNullF64;
pub const countNotNullF64 = conditionals.countNotNullF64;

// Re-export statistics functions
pub const median = statistics.median;
pub const quantile = statistics.quantile;
pub const skewness = statistics.skewness;
pub const kurtosis = statistics.kurtosis;
pub const correlation = statistics.correlation;
pub const modeInt = statistics.modeInt;

// Re-export window functions
pub const lag = window.lag;
pub const lead = window.lead;
pub const rowNumber = window.rowNumber;
pub const rowNumberPartitioned = window.rowNumberPartitioned;
pub const rank = window.rank;
pub const denseRank = window.denseRank;
pub const cumSum = window.cumSum;
pub const cumSumPartitioned = window.cumSumPartitioned;
pub const cumMin = window.cumMin;
pub const cumMax = window.cumMax;
pub const rollingSum = window.rollingSum;
pub const rollingMean = window.rollingMean;
pub const rollingMin = window.rollingMin;
pub const rollingMax = window.rollingMax;
pub const rollingStd = window.rollingStd;
pub const diff = window.diff;
pub const diffN = window.diffN;
pub const pctChange = window.pctChange;

// Re-export fold/horizontal aggregation functions
pub const sumHorizontal2 = fold.sumHorizontal2;
pub const sumHorizontal3 = fold.sumHorizontal3;
pub const sumHorizontalN = fold.sumHorizontalN;
pub const minHorizontal2 = fold.minHorizontal2;
pub const minHorizontal3 = fold.minHorizontal3;
pub const maxHorizontal2 = fold.maxHorizontal2;
pub const maxHorizontal3 = fold.maxHorizontal3;
pub const minHorizontalN = fold.minHorizontalN;
pub const maxHorizontalN = fold.maxHorizontalN;
pub const meanHorizontalN = fold.meanHorizontalN;
pub const anyHorizontal2 = fold.anyHorizontal2;
pub const allHorizontal2 = fold.allHorizontal2;
pub const productHorizontal2 = fold.productHorizontal2;
pub const productHorizontal3 = fold.productHorizontal3;
pub const countNonNullHorizontal2 = fold.countNonNullHorizontal2;
pub const countNonNullHorizontal3 = fold.countNonNullHorizontal3;

// Re-export filter functions
pub const countMaskTrue = filters.countMaskTrue;
pub const indicesFromMask = filters.indicesFromMask;
pub const countAndExtractIndices = filters.countAndExtractIndices;
pub const filterMaskGreaterThan = filters.filterMaskGreaterThan;
pub const filterMaskU8GreaterThan = filters.filterMaskU8GreaterThan;
pub const filterGreaterThan = filters.filterGreaterThan;
pub const filterGreaterThanInt = filters.filterGreaterThanInt;
pub const filterMaskU8GreaterThanInt = filters.filterMaskU8GreaterThanInt;
pub const countTrue = filters.countTrue;

// Re-export hash functions
pub const hashInt64Column = hashing.hashInt64Column;
pub const hashInt32Column = hashing.hashInt32Column;
pub const hashFloat64Column = hashing.hashFloat64Column;
pub const hashFloat32Column = hashing.hashFloat32Column;
pub const combineHashes = hashing.combineHashes;
pub const hashInt64Columns = hashing.hashInt64Columns;
pub const rapidHash64 = hashing.rapidHash64;
pub const fastIntHash = hashing.fastIntHash;

// Parallel hash functions (using Blitz work-stealing)
pub const parallelHashInt64Column = hashing.parallelHashInt64Column;
pub const parallelHashInt32Column = hashing.parallelHashInt32Column;
pub const parallelHashFloat64Column = hashing.parallelHashFloat64Column;
pub const parallelHashFloat32Column = hashing.parallelHashFloat32Column;
pub const parallelCombineHashes = hashing.parallelCombineHashes;

// Re-export gather functions (generic and type-specific wrappers)
pub const gatherGeneric = gather.gather; // Generic gather for any numeric type
pub const gatherF64 = gather.gatherF64;
pub const gatherI64 = gather.gatherI64;
pub const gatherI32 = gather.gatherI32;
pub const gatherF32 = gather.gatherF32;

// Re-export groupby aggregation functions
pub const aggregateSumByGroup = groupby_agg.aggregateSumByGroup;
pub const aggregateMinByGroup = groupby_agg.aggregateMinByGroup;
pub const aggregateMaxByGroup = groupby_agg.aggregateMaxByGroup;
pub const countByGroup = groupby_agg.countByGroup;

// Sorted-path groupby aggregations (for pre-sorted data)
pub const isSorted = groupby_agg.isSorted;
pub const aggregateSumByGroupSorted = groupby_agg.aggregateSumByGroupSorted;
pub const aggregateMinByGroupSorted = groupby_agg.aggregateMinByGroupSorted;
pub const aggregateMaxByGroupSorted = groupby_agg.aggregateMaxByGroupSorted;
pub const countByGroupSorted = groupby_agg.countByGroupSorted;

// Smart groupby aggregations (auto-detect optimal path)
pub const smartSumByGroup = groupby_agg.smartSumByGroup;
pub const smartMinByGroup = groupby_agg.smartMinByGroup;
pub const smartMaxByGroup = groupby_agg.smartMaxByGroup;
pub const smartCountByGroup = groupby_agg.smartCountByGroup;

// Radix sort-based groupby (for high cardinality)
pub const radixSortSumByGroup = groupby_agg.radixSortSumByGroup;
pub const radixSortMinByGroup = groupby_agg.radixSortMinByGroup;
pub const radixSortMaxByGroup = groupby_agg.radixSortMaxByGroup;

// Optimal groupby (chooses best algorithm)
pub const optimalSumByGroup = groupby_agg.optimalSumByGroup;
pub const optimalMinByGroup = groupby_agg.optimalMinByGroup;
pub const optimalMaxByGroup = groupby_agg.optimalMaxByGroup;

// Re-export sorting functions (64-bit optimized radix sort)
pub const argsortF64 = sorting.argsortF64;
pub const argsortI64 = sorting.argsortI64;
pub const sortF64 = sorting.sortF64;
pub const sortI64 = sorting.sortI64;

// Re-export sorting functions (32-bit using std.sort)
pub const argsortI32 = sorting.argsortI32;
pub const argsortF32 = sorting.argsortF32;
pub const sortI32 = sorting.sortI32;
pub const sortF32 = sorting.sortF32;

// Re-export sorting functions (unsigned integers)
pub const argsortU64 = sorting.argsortU64;
pub const argsortU32 = sorting.argsortU32;

// Key conversion utilities
pub const floatToSortable = sorting.floatToSortable;
pub const i64ToSortable = sorting.i64ToSortable;
pub const isSortedI64Keys = sorting.isSortedI64Keys;

// Parallel gather functions
pub const gatherF64Parallel = sorting.gatherF64;
pub const gatherI64Parallel = sorting.gatherI64;

// Import joins module (contains join algorithms using Swiss Table)
pub const joins = @import("joins.zig");
pub const idx_vec = @import("idx_vec.zig");

// Re-export Swiss Table from swisstable module (pure data structure)
pub const swisstable = @import("../swisstable/lib.zig");
pub const JoinSwissTable = swisstable.Table;

// Legacy-compatible join result types
pub const InnerJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32,
    num_matches: usize,
    owns_memory: bool,

    pub fn deinit(self: *InnerJoinResult, allocator: std.mem.Allocator) void {
        if (self.owns_memory) {
            if (self.left_indices.len > 0) allocator.free(self.left_indices);
            if (self.right_indices.len > 0) allocator.free(self.right_indices);
        }
    }
};

pub const LeftJoinResult = struct {
    left_indices: []i32,
    right_indices: []i32,
    num_rows: usize,
    owns_memory: bool,

    pub fn deinit(self: *LeftJoinResult, allocator: std.mem.Allocator) void {
        if (self.owns_memory) {
            if (self.left_indices.len > 0) allocator.free(self.left_indices);
            if (self.right_indices.len > 0) allocator.free(self.right_indices);
        }
    }
};

// Join functions with legacy signature (allocator, left_keys, right_keys)
pub fn innerJoinSwiss(allocator: std.mem.Allocator, left_keys: []const i64, right_keys: []const i64) !InnerJoinResult {
    const result = try joins.innerJoin(i64, left_keys, right_keys, allocator);
    return InnerJoinResult{
        .left_indices = result.left_indices,
        .right_indices = result.right_indices,
        .num_matches = result.left_indices.len,
        .owns_memory = true,
    };
}

pub fn leftJoinSwiss(allocator: std.mem.Allocator, left_keys: []const i64, right_keys: []const i64) !LeftJoinResult {
    const result = try joins.leftJoin(i64, left_keys, right_keys, allocator);
    return LeftJoinResult{
        .left_indices = result.left_indices,
        .right_indices = result.right_indices,
        .num_rows = result.left_indices.len,
        .owns_memory = true,
    };
}

// Parallel join functions (same implementation - they auto-parallelize based on size)
pub const singlePassParallelInnerJoin = innerJoinSwiss;
pub const singlePassParallelLeftJoin = leftJoinSwiss;

// ============================================================================
// Tests - run all submodule tests
// ============================================================================

test {
    // Import all submodules to run their tests
    _ = core;
    _ = aggregations;
    _ = arithmetic;
    _ = comparisons;
    _ = conditionals;
    _ = statistics;
    _ = window;
    _ = fold;
    _ = filters;
    _ = hashing;
    _ = gather;
    _ = groupby_agg;
    _ = sorting;
    _ = joins;
    _ = idx_vec;
}
