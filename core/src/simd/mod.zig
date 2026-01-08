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
pub const filters = @import("filters.zig");
pub const hashing = @import("hashing.zig");
pub const gather = @import("gather.zig");
pub const groupby_agg = @import("groupby_agg.zig");
pub const sorting = @import("sorting.zig");
pub const joins = @import("joins.zig");

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

// Re-export gather functions
pub const gatherF64 = gather.gatherF64;
pub const gatherI64 = gather.gatherI64;
pub const gatherI32 = gather.gatherI32;
pub const gatherF32 = gather.gatherF32;

// Re-export groupby aggregation functions
pub const aggregateSumByGroup = groupby_agg.aggregateSumByGroup;
pub const aggregateMinByGroup = groupby_agg.aggregateMinByGroup;
pub const aggregateMaxByGroup = groupby_agg.aggregateMaxByGroup;
pub const countByGroup = groupby_agg.countByGroup;

// Re-export sorting functions
pub const argsort = sorting.argsort;
pub const argsortInt = sorting.argsortInt;
pub const argsortParallel = sorting.argsortParallel;
pub const argsortRadixF64 = sorting.argsortRadixF64;
pub const argsortParallelMerge = sorting.argsortParallelMerge;

// Re-export join types and functions
pub const InnerJoinResult = joins.InnerJoinResult;
pub const LeftJoinResult = joins.LeftJoinResult;
pub const SwissJoinTable = joins.SwissJoinTable;
pub const buildJoinHashTable = joins.buildJoinHashTable;
pub const probeJoinHashTable = joins.probeJoinHashTable;
pub const innerJoinI64 = joins.innerJoinI64;
pub const innerJoinI64SinglePass = joins.innerJoinI64SinglePass;
pub const innerJoinI64Swiss = joins.innerJoinI64Swiss;
pub const parallelInnerJoinI64 = joins.parallelInnerJoinI64;
pub const innerJoinI64LockFree = joins.innerJoinI64LockFree;
pub const leftJoinI64 = joins.leftJoinI64;
pub const parallelLeftJoinI64 = joins.parallelLeftJoinI64;
pub const parallelAggregateSumF64ByGroup = joins.parallelAggregateSumF64ByGroup;

// ============================================================================
// Tests - run all submodule tests
// ============================================================================

test {
    // Import all submodules to run their tests
    _ = core;
    _ = aggregations;
    _ = arithmetic;
    _ = comparisons;
    _ = filters;
    _ = hashing;
    _ = gather;
    _ = groupby_agg;
    _ = sorting;
    _ = joins;
}
