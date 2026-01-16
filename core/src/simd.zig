//! SIMD-accelerated operations for the Galleon DataFrame library.
//!
//! This module has been refactored into smaller, focused submodules for better
//! organization and maintainability. This file re-exports all public APIs to
//! maintain backward compatibility.

const mod = @import("simd/mod.zig");

// Re-export submodules
pub const core = mod.core;
pub const aggregations = mod.aggregations;
pub const arithmetic = mod.arithmetic;
pub const comparisons = mod.comparisons;
pub const conditionals = mod.conditionals;
pub const statistics = mod.statistics;
pub const window = mod.window;
pub const fold = mod.fold;
pub const filters = mod.filters;
pub const hashing = mod.hashing;
pub const gather = mod.gather;
pub const groupby_agg = mod.groupby_agg;
pub const sorting = mod.sorting;
pub const joins = mod.joins;

// Re-export commonly used constants
pub const VECTOR_WIDTH = mod.VECTOR_WIDTH;
pub const UNROLL_FACTOR = mod.UNROLL_FACTOR;
pub const CHUNK_SIZE = mod.CHUNK_SIZE;
pub const MAX_THREADS = mod.MAX_THREADS;

// Re-export thread config functions
pub const setMaxThreads = mod.setMaxThreads;
pub const getThreadConfig = mod.getThreadConfig;
pub const getMaxThreads = mod.getMaxThreads;

// Re-export aggregation functions
pub const sum = mod.sum;
pub const min = mod.min;
pub const max = mod.max;
pub const mean = mod.mean;
pub const variance = mod.variance;
pub const stdDev = mod.stdDev;
pub const sumInt = mod.sumInt;
pub const minInt = mod.minInt;
pub const maxInt = mod.maxInt;

// Re-export arithmetic functions
pub const addScalar = mod.addScalar;
pub const mulScalar = mod.mulScalar;
pub const addArrays = mod.addArrays;
pub const addArraysOut = mod.addArraysOut;
pub const subArrays = mod.subArrays;
pub const mulArrays = mod.mulArrays;
pub const divArrays = mod.divArrays;
pub const addScalarInt = mod.addScalarInt;
pub const mulScalarInt = mod.mulScalarInt;

// Re-export comparison functions
pub const cmpGt = mod.cmpGt;
pub const cmpGe = mod.cmpGe;
pub const cmpLt = mod.cmpLt;
pub const cmpLe = mod.cmpLe;
pub const cmpEq = mod.cmpEq;
pub const cmpNe = mod.cmpNe;

// Re-export conditional functions
pub const selectF64 = mod.selectF64;
pub const selectI64 = mod.selectI64;
pub const selectScalarF64 = mod.selectScalarF64;
pub const isNullF64 = mod.isNullF64;
pub const isNotNullF64 = mod.isNotNullF64;
pub const fillNullF64 = mod.fillNullF64;
pub const fillNullForwardF64 = mod.fillNullForwardF64;
pub const fillNullBackwardF64 = mod.fillNullBackwardF64;
pub const coalesceF64 = mod.coalesceF64;
pub const coalesce2F64 = mod.coalesce2F64;
pub const countNullF64 = mod.countNullF64;
pub const countNotNullF64 = mod.countNotNullF64;

// Re-export statistics functions
pub const median = mod.median;
pub const quantile = mod.quantile;
pub const skewness = mod.skewness;
pub const kurtosis = mod.kurtosis;
pub const correlation = mod.correlation;
pub const modeInt = mod.modeInt;

// Re-export window functions
pub const lag = mod.lag;
pub const lead = mod.lead;
pub const rowNumber = mod.rowNumber;
pub const rowNumberPartitioned = mod.rowNumberPartitioned;
pub const rank = mod.rank;
pub const denseRank = mod.denseRank;
pub const cumSum = mod.cumSum;
pub const cumSumPartitioned = mod.cumSumPartitioned;
pub const cumMin = mod.cumMin;
pub const cumMax = mod.cumMax;
pub const rollingSum = mod.rollingSum;
pub const rollingMean = mod.rollingMean;
pub const rollingMin = mod.rollingMin;
pub const rollingMax = mod.rollingMax;
pub const rollingStd = mod.rollingStd;
pub const diff = mod.diff;
pub const diffN = mod.diffN;
pub const pctChange = mod.pctChange;

// Re-export fold/horizontal aggregation functions
pub const sumHorizontal2 = mod.sumHorizontal2;
pub const sumHorizontal3 = mod.sumHorizontal3;
pub const sumHorizontalN = mod.sumHorizontalN;
pub const minHorizontal2 = mod.minHorizontal2;
pub const minHorizontal3 = mod.minHorizontal3;
pub const maxHorizontal2 = mod.maxHorizontal2;
pub const maxHorizontal3 = mod.maxHorizontal3;
pub const minHorizontalN = mod.minHorizontalN;
pub const maxHorizontalN = mod.maxHorizontalN;
pub const meanHorizontalN = mod.meanHorizontalN;
pub const anyHorizontal2 = mod.anyHorizontal2;
pub const allHorizontal2 = mod.allHorizontal2;
pub const productHorizontal2 = mod.productHorizontal2;
pub const productHorizontal3 = mod.productHorizontal3;
pub const countNonNullHorizontal2 = mod.countNonNullHorizontal2;
pub const countNonNullHorizontal3 = mod.countNonNullHorizontal3;

// Re-export filter functions
pub const countMaskTrue = mod.countMaskTrue;
pub const indicesFromMask = mod.indicesFromMask;
pub const countAndExtractIndices = mod.countAndExtractIndices;
pub const filterMaskGreaterThan = mod.filterMaskGreaterThan;
pub const filterMaskU8GreaterThan = mod.filterMaskU8GreaterThan;
pub const filterGreaterThan = mod.filterGreaterThan;
pub const filterGreaterThanInt = mod.filterGreaterThanInt;
pub const filterMaskU8GreaterThanInt = mod.filterMaskU8GreaterThanInt;
pub const countTrue = mod.countTrue;

// Re-export hash functions
pub const hashInt64Column = mod.hashInt64Column;
pub const hashInt32Column = mod.hashInt32Column;
pub const hashFloat64Column = mod.hashFloat64Column;
pub const hashFloat32Column = mod.hashFloat32Column;
pub const combineHashes = mod.combineHashes;
pub const hashInt64Columns = mod.hashInt64Columns;
pub const rapidHash64 = mod.rapidHash64;
pub const fastIntHash = mod.fastIntHash;

// Parallel hash functions (using Blitz work-stealing)
pub const parallelHashInt64Column = mod.parallelHashInt64Column;
pub const parallelHashInt32Column = mod.parallelHashInt32Column;
pub const parallelHashFloat64Column = mod.parallelHashFloat64Column;
pub const parallelHashFloat32Column = mod.parallelHashFloat32Column;
pub const parallelCombineHashes = mod.parallelCombineHashes;

// Re-export gather functions
pub const gatherF64 = mod.gatherF64;
pub const gatherI64 = mod.gatherI64;
pub const gatherI32 = mod.gatherI32;
pub const gatherF32 = mod.gatherF32;

// Re-export groupby aggregation functions
pub const aggregateSumByGroup = mod.aggregateSumByGroup;
pub const aggregateMinByGroup = mod.aggregateMinByGroup;
pub const aggregateMaxByGroup = mod.aggregateMaxByGroup;
pub const countByGroup = mod.countByGroup;

// Re-export sorting functions
pub const argsort = mod.argsort;
pub const argsortInt = mod.argsortInt;
pub const argsortParallel = mod.argsortParallel;
pub const argsortRadixF64 = mod.argsortRadixF64;
pub const argsortParallelMerge = mod.argsortParallelMerge;

// Re-export join types and functions
pub const InnerJoinResult = mod.InnerJoinResult;
pub const LeftJoinResult = mod.LeftJoinResult;
pub const SwissJoinTable = mod.SwissJoinTable;
pub const buildJoinHashTable = mod.buildJoinHashTable;
pub const probeJoinHashTable = mod.probeJoinHashTable;
pub const innerJoinI64 = mod.innerJoinI64;
pub const innerJoinI64SinglePass = mod.innerJoinI64SinglePass;
pub const innerJoinI64Swiss = mod.innerJoinI64Swiss;
pub const parallelInnerJoinI64 = mod.parallelInnerJoinI64;
pub const innerJoinI64LockFree = mod.innerJoinI64LockFree;
pub const innerJoinI64Radix = mod.innerJoinI64Radix;
pub const leftJoinI64 = mod.leftJoinI64;
pub const parallelLeftJoinI64 = mod.parallelLeftJoinI64;
pub const leftJoinI64Radix = mod.leftJoinI64Radix;
pub const innerJoinI64SortMerge = mod.innerJoinI64SortMerge;
pub const leftJoinI64SortMerge = mod.leftJoinI64SortMerge;
pub const innerJoinI64TwoPass = mod.innerJoinI64TwoPass;
pub const innerJoinI64Simd = mod.innerJoinI64Simd;
pub const parallelAggregateSumF64ByGroup = mod.parallelAggregateSumF64ByGroup;

// ============================================================================
// Tests - run all submodule tests
// ============================================================================

test {
    _ = mod;
}
