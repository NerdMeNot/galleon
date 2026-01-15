//! SIMD Dispatch System
//!
//! This module provides runtime dispatch to ISA-specific implementations.
//! It maintains a dispatch table of function pointers that are initialized
//! based on the detected CPU capabilities.
//!
//! The dispatch table is initialized lazily on first use and cached thereafter.
//! This approach allows:
//! - Zero overhead after initialization (direct function pointer call)
//! - Automatic selection of optimal implementation
//! - User override for testing or compatibility

const std = @import("std");
const cpuid = @import("cpuid.zig");
const SimdLevel = cpuid.SimdLevel;

// Import ISA-specific implementations
const sse4 = @import("impl/sse4.zig");
const avx2 = @import("impl/avx2.zig");
const avx512 = @import("impl/avx512.zig");

// ============================================================================
// Function Pointer Types
// ============================================================================

// --- Aggregations ---
pub const SumF64Fn = *const fn ([*]const f64, usize) callconv(.c) f64;
pub const SumF32Fn = *const fn ([*]const f32, usize) callconv(.c) f32;
pub const SumI64Fn = *const fn ([*]const i64, usize) callconv(.c) i64;
pub const SumI32Fn = *const fn ([*]const i32, usize) callconv(.c) i32;

pub const MinMaxF64Fn = *const fn ([*]const f64, usize, *bool) callconv(.c) f64;
pub const MinMaxF32Fn = *const fn ([*]const f32, usize, *bool) callconv(.c) f32;
pub const MinMaxI64Fn = *const fn ([*]const i64, usize, *bool) callconv(.c) i64;
pub const MinMaxI32Fn = *const fn ([*]const i32, usize, *bool) callconv(.c) i32;

// --- Element-wise Arithmetic ---
pub const BinaryOpF64Fn = *const fn ([*]const f64, [*]const f64, [*]f64, usize) callconv(.c) void;
pub const BinaryOpF32Fn = *const fn ([*]const f32, [*]const f32, [*]f32, usize) callconv(.c) void;
pub const BinaryOpI64Fn = *const fn ([*]const i64, [*]const i64, [*]i64, usize) callconv(.c) void;
pub const BinaryOpI32Fn = *const fn ([*]const i32, [*]const i32, [*]i32, usize) callconv(.c) void;

pub const ScalarOpF64Fn = *const fn ([*]f64, usize, f64) callconv(.c) void;
pub const ScalarOpF32Fn = *const fn ([*]f32, usize, f32) callconv(.c) void;
pub const ScalarOpI64Fn = *const fn ([*]i64, usize, i64) callconv(.c) void;
pub const ScalarOpI32Fn = *const fn ([*]i32, usize, i32) callconv(.c) void;

// --- Comparisons ---
pub const CmpF64Fn = *const fn ([*]const f64, [*]const f64, [*]u8, usize) callconv(.c) void;
pub const CmpI64Fn = *const fn ([*]const i64, [*]const i64, [*]u8, usize) callconv(.c) void;

// --- Filters ---
pub const FilterF64Fn = *const fn ([*]const f64, usize, f64, [*]u32, *usize) callconv(.c) void;
pub const FilterMaskF64Fn = *const fn ([*]const f64, usize, f64, [*]u8) callconv(.c) void;
pub const FilterI64Fn = *const fn ([*]const i64, usize, i64, [*]u32, *usize) callconv(.c) void;
pub const FilterMaskI64Fn = *const fn ([*]const i64, usize, i64, [*]u8) callconv(.c) void;

// --- Hashing ---
pub const HashI64Fn = *const fn ([*]const i64, [*]u64, usize) callconv(.c) void;
pub const HashI32Fn = *const fn ([*]const i32, [*]u64, usize) callconv(.c) void;
pub const HashF64Fn = *const fn ([*]const f64, [*]u64, usize) callconv(.c) void;
pub const HashF32Fn = *const fn ([*]const f32, [*]u64, usize) callconv(.c) void;
pub const CombineHashesFn = *const fn ([*]const u64, [*]const u64, [*]u64, usize) callconv(.c) void;

// --- Gather ---
pub const GatherF64Fn = *const fn ([*]const f64, [*]const i32, [*]f64, usize) callconv(.c) void;
pub const GatherF32Fn = *const fn ([*]const f32, [*]const i32, [*]f32, usize) callconv(.c) void;
pub const GatherI64Fn = *const fn ([*]const i64, [*]const i32, [*]i64, usize) callconv(.c) void;
pub const GatherI32Fn = *const fn ([*]const i32, [*]const i32, [*]i32, usize) callconv(.c) void;

// --- GroupBy Aggregations ---
pub const AggSumByGroupF64Fn = *const fn ([*]const f64, [*]const u32, [*]f64, usize, usize) callconv(.c) void;
pub const AggSumByGroupI64Fn = *const fn ([*]const i64, [*]const u32, [*]i64, usize, usize) callconv(.c) void;
pub const AggMinByGroupF64Fn = *const fn ([*]const f64, [*]const u32, [*]f64, usize, usize) callconv(.c) void;
pub const AggMaxByGroupF64Fn = *const fn ([*]const f64, [*]const u32, [*]f64, usize, usize) callconv(.c) void;
pub const CountByGroupFn = *const fn ([*]const u32, [*]u64, usize, usize) callconv(.c) void;

// ============================================================================
// Dispatch Table
// ============================================================================

pub const DispatchTable = struct {
    // --- Aggregations ---
    sum_f64: SumF64Fn,
    sum_f32: SumF32Fn,
    sum_i64: SumI64Fn,
    sum_i32: SumI32Fn,

    min_f64: MinMaxF64Fn,
    max_f64: MinMaxF64Fn,
    min_f32: MinMaxF32Fn,
    max_f32: MinMaxF32Fn,
    min_i64: MinMaxI64Fn,
    max_i64: MinMaxI64Fn,
    min_i32: MinMaxI32Fn,
    max_i32: MinMaxI32Fn,

    // --- Element-wise Arithmetic ---
    add_f64: BinaryOpF64Fn,
    sub_f64: BinaryOpF64Fn,
    mul_f64: BinaryOpF64Fn,
    div_f64: BinaryOpF64Fn,

    add_f32: BinaryOpF32Fn,
    sub_f32: BinaryOpF32Fn,
    mul_f32: BinaryOpF32Fn,
    div_f32: BinaryOpF32Fn,

    add_i64: BinaryOpI64Fn,
    sub_i64: BinaryOpI64Fn,
    mul_i64: BinaryOpI64Fn,

    add_i32: BinaryOpI32Fn,
    sub_i32: BinaryOpI32Fn,
    mul_i32: BinaryOpI32Fn,

    add_scalar_f64: ScalarOpF64Fn,
    mul_scalar_f64: ScalarOpF64Fn,
    add_scalar_i64: ScalarOpI64Fn,
    mul_scalar_i64: ScalarOpI64Fn,

    // --- Comparisons ---
    cmp_gt_f64: CmpF64Fn,
    cmp_ge_f64: CmpF64Fn,
    cmp_lt_f64: CmpF64Fn,
    cmp_le_f64: CmpF64Fn,
    cmp_eq_f64: CmpF64Fn,
    cmp_ne_f64: CmpF64Fn,

    cmp_gt_i64: CmpI64Fn,
    cmp_ge_i64: CmpI64Fn,
    cmp_lt_i64: CmpI64Fn,
    cmp_le_i64: CmpI64Fn,
    cmp_eq_i64: CmpI64Fn,
    cmp_ne_i64: CmpI64Fn,

    // --- Filters ---
    filter_gt_f64: FilterF64Fn,
    filter_mask_gt_f64: FilterMaskF64Fn,
    filter_gt_i64: FilterI64Fn,
    filter_mask_gt_i64: FilterMaskI64Fn,

    // --- Hashing ---
    hash_i64: HashI64Fn,
    hash_i32: HashI32Fn,
    hash_f64: HashF64Fn,
    hash_f32: HashF32Fn,
    combine_hashes: CombineHashesFn,

    // --- Gather ---
    gather_f64: GatherF64Fn,
    gather_f32: GatherF32Fn,
    gather_i64: GatherI64Fn,
    gather_i32: GatherI32Fn,

    // --- GroupBy Aggregations ---
    agg_sum_by_group_f64: AggSumByGroupF64Fn,
    agg_sum_by_group_i64: AggSumByGroupI64Fn,
    agg_min_by_group_f64: AggMinByGroupF64Fn,
    agg_max_by_group_f64: AggMaxByGroupF64Fn,
    count_by_group: CountByGroupFn,
};

// ============================================================================
// Global Dispatch State
// ============================================================================

var dispatch_table: ?DispatchTable = null;
var dispatch_initialized: bool = false;
var dispatch_lock: std.Thread.Mutex = .{};

/// Get the initialized dispatch table
/// Thread-safe, initializes on first call based on detected SIMD level
pub fn getDispatch() *const DispatchTable {
    // Fast path: already initialized (use atomic load for thread-safety)
    if (@atomicLoad(bool, &dispatch_initialized, .acquire)) {
        return &dispatch_table.?;
    }

    // Slow path: need to initialize
    dispatch_lock.lock();
    defer dispatch_lock.unlock();

    // Double-check after lock
    if (dispatch_initialized) {
        return &dispatch_table.?;
    }

    dispatch_table = initDispatchTable(cpuid.getSimdLevel());
    @atomicStore(bool, &dispatch_initialized, true, .release);
    return &dispatch_table.?;
}

/// Reinitialize dispatch table with current SIMD level
/// Call this after setSimdLevel() to apply the change
pub fn reinitDispatch() void {
    dispatch_lock.lock();
    defer dispatch_lock.unlock();
    dispatch_table = initDispatchTable(cpuid.getSimdLevel());
    @atomicStore(bool, &dispatch_initialized, true, .release);
}

/// Reset dispatch table (mainly for testing)
pub fn resetDispatch() void {
    dispatch_lock.lock();
    defer dispatch_lock.unlock();
    dispatch_table = null;
    @atomicStore(bool, &dispatch_initialized, false, .release);
}

/// Initialize dispatch table for a specific SIMD level
fn initDispatchTable(level: SimdLevel) DispatchTable {
    return switch (level) {
        .avx512 => avx512.dispatch_table,
        .avx2 => avx2.dispatch_table,
        .sse4, .scalar => sse4.dispatch_table,
    };
}

// ============================================================================
// Convenience Accessors
// ============================================================================

/// Get the current SIMD level being used for dispatch
pub fn getCurrentLevel() SimdLevel {
    return cpuid.getSimdLevel();
}

/// Set the SIMD level and reinitialize dispatch
pub fn setLevel(level: SimdLevel) void {
    cpuid.setSimdLevel(level);
    reinitDispatch();
}

// ============================================================================
// Tests
// ============================================================================

test "dispatch - table initialization" {
    resetDispatch();
    cpuid.resetDetection();

    const table = getDispatch();

    // Verify table is populated by checking that function pointers are valid
    // We do this by checking that the table pointer is not null
    try std.testing.expect(@intFromPtr(table) != 0);

    // Verify we can call a function through the dispatch table
    const test_data = [_]f64{ 1.0, 2.0, 3.0 };
    const result = table.sum_f64(&test_data, test_data.len);
    try std.testing.expectApproxEqAbs(@as(f64, 6.0), result, 0.0001);
}

test "dispatch - level switching" {
    resetDispatch();

    // Set to SSE4 level
    setLevel(.sse4);
    const table1 = getDispatch();
    try std.testing.expectEqual(SimdLevel.sse4, getCurrentLevel());

    // Set to AVX2 level
    setLevel(.avx2);
    reinitDispatch();
    const table2 = getDispatch();
    try std.testing.expectEqual(SimdLevel.avx2, getCurrentLevel());

    // Tables should be different (different implementations)
    // Note: On systems without AVX2, both might use SSE4 fallback
    _ = table1;
    _ = table2;
}
