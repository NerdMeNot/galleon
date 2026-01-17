const std = @import("std");
const Allocator = std.mem.Allocator;

// Re-export modules
pub const column = @import("column.zig");
pub const simd = @import("simd.zig");
pub const groupby = @import("groupby.zig");
pub const arrow = @import("arrow.zig");

// Runtime dispatch system
pub const cpuid = @import("cpuid.zig");
pub const dispatch = @import("dispatch.zig");

// Blitz: Heartbeat-based work-stealing + SIMD-parallel operations
pub const blitz = @import("blitz.zig");

// ============================================================================
// C ABI Exports - These are called from Go via CGO
// ============================================================================

// --- SIMD Level Detection and Configuration ---

/// Get the detected SIMD level (0=Scalar, 1=SSE4, 2=AVX2, 3=AVX512)
export fn galleon_get_simd_level() u8 {
    return @intFromEnum(cpuid.getSimdLevel());
}

/// Override the SIMD level (for testing or compatibility)
/// Pass 0=Scalar, 1=SSE4, 2=AVX2, 3=AVX512
export fn galleon_set_simd_level(level: u8) void {
    if (level <= @intFromEnum(cpuid.SimdLevel.avx512)) {
        cpuid.setSimdLevel(@enumFromInt(level));
        dispatch.reinitDispatch();
    }
}

/// Get the SIMD level name as a C string
export fn galleon_get_simd_level_name() [*:0]const u8 {
    return switch (cpuid.getSimdLevel()) {
        .scalar => "Scalar",
        .sse4 => "SSE4",
        .avx2 => "AVX2",
        .avx512 => "AVX-512",
    };
}

/// Get the vector width in bytes for the current SIMD level
export fn galleon_get_simd_vector_bytes() usize {
    return cpuid.getSimdLevel().vectorBytes();
}

// --- Column Creation ---

export fn galleon_column_f64_create(data: [*]const f64, len: usize) ?*column.ColumnF64 {
    return column.ColumnF64.createFromSlice(std.heap.c_allocator, data[0..len]) catch null;
}

export fn galleon_column_f64_destroy(col: *column.ColumnF64) void {
    col.deinit();
}

export fn galleon_column_f64_len(col: *const column.ColumnF64) usize {
    return col.len();
}

export fn galleon_column_f64_get(col: *const column.ColumnF64, index: usize) f64 {
    return col.get(index) orelse 0.0;
}

export fn galleon_column_f64_data(col: *const column.ColumnF64) [*]const f64 {
    return col.data().ptr;
}

// --- Aggregations (auto-parallelized via Blitz for large data) ---
// Uses intelligent threshold system - parallelizes based on operation cost and core count

export fn galleon_sum_f64(data: [*]const f64, len: usize) f64 {
    if (blitz.shouldParallelize(.sum, len)) {
        return blitz.parallelSum(f64, data[0..len]);
    }
    return simd.sum(f64, data[0..len]);
}

export fn galleon_min_f64(data: [*]const f64, len: usize) f64 {
    if (blitz.shouldParallelize(.min, len)) {
        return blitz.parallelMin(f64, data[0..len]) orelse 0.0;
    }
    return simd.min(f64, data[0..len]) orelse 0.0;
}

export fn galleon_max_f64(data: [*]const f64, len: usize) f64 {
    if (blitz.shouldParallelize(.max, len)) {
        return blitz.parallelMax(f64, data[0..len]) orelse 0.0;
    }
    return simd.max(f64, data[0..len]) orelse 0.0;
}

export fn galleon_mean_f64(data: [*]const f64, len: usize) f64 {
    if (blitz.shouldParallelize(.mean, len)) {
        if (len == 0) return 0.0;
        return blitz.parallelSum(f64, data[0..len]) / @as(f64, @floatFromInt(len));
    }
    return simd.mean(f64, data[0..len]) orelse 0.0;
}

// --- Thread Configuration ---

/// Set the maximum number of threads to use
/// Pass 0 to use auto-detection (default)
export fn galleon_set_max_threads(max_threads: usize) void {
    simd.setMaxThreads(max_threads);
}

/// Get current thread configuration
/// Returns the effective max threads and whether it was auto-detected
export fn galleon_get_max_threads() usize {
    return simd.getThreadConfig().max_threads;
}

/// Check if thread count was auto-detected
export fn galleon_is_threads_auto_detected() bool {
    return simd.getThreadConfig().auto_detected;
}

// --- Blitz Work-Stealing Thread Pool (diagnostic functions) ---

/// Initialize the Blitz work-stealing thread pool
/// Note: Blitz auto-initializes on first parallel operation, but this allows explicit control
/// Returns true on success, false on failure
export fn blitz_init() bool {
    blitz.init() catch return false;
    return true;
}

/// Shutdown the Blitz thread pool
export fn blitz_deinit() void {
    blitz.deinit();
}

/// Check if Blitz pool is initialized
export fn blitz_is_initialized() bool {
    return blitz.isInitialized();
}

/// Get the number of worker threads
export fn blitz_num_workers() u32 {
    return blitz.numWorkers();
}

// --- Vectorized Operations ---

export fn galleon_add_scalar_f64(data: [*]f64, len: usize, scalar: f64) void {
    simd.addScalar(f64, data[0..len], scalar);
}

export fn galleon_mul_scalar_f64(data: [*]f64, len: usize, scalar: f64) void {
    simd.mulScalar(f64, data[0..len], scalar);
}

export fn galleon_add_arrays_f64(dst: [*]f64, src: [*]const f64, len: usize) void {
    simd.addArrays(f64, dst[0..len], src[0..len]);
}

// --- Vector Arithmetic (out = a op b) ---

export fn galleon_add_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    // Element-wise ops are memory-bound - parallelism adds cache contention
    // Benchmarks showed parallel Add slower than sequential Sub
    simd.addArraysOut(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_sub_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.subArrays(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_mul_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.mulArrays(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_div_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.divArrays(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_add_f32(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) void {
    simd.addArraysOut(f32, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_sub_f32(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) void {
    simd.subArrays(f32, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_mul_f32(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) void {
    simd.mulArrays(f32, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_div_f32(a: [*]const f32, b: [*]const f32, out: [*]f32, len: usize) void {
    simd.divArrays(f32, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_add_i64(a: [*]const i64, b: [*]const i64, out: [*]i64, len: usize) void {
    simd.addArraysOut(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_sub_i64(a: [*]const i64, b: [*]const i64, out: [*]i64, len: usize) void {
    simd.subArrays(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_mul_i64(a: [*]const i64, b: [*]const i64, out: [*]i64, len: usize) void {
    simd.mulArrays(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_add_i32(a: [*]const i32, b: [*]const i32, out: [*]i32, len: usize) void {
    simd.addArraysOut(i32, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_sub_i32(a: [*]const i32, b: [*]const i32, out: [*]i32, len: usize) void {
    simd.subArrays(i32, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_mul_i32(a: [*]const i32, b: [*]const i32, out: [*]i32, len: usize) void {
    simd.mulArrays(i32, a[0..len], b[0..len], out[0..len]);
}

// --- Array Comparison Operations (out = a cmp b) ---

export fn galleon_cmp_gt_f64(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) void {
    simd.cmpGt(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_ge_f64(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) void {
    simd.cmpGe(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_lt_f64(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) void {
    simd.cmpLt(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_le_f64(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) void {
    simd.cmpLe(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_eq_f64(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) void {
    simd.cmpEq(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_ne_f64(a: [*]const f64, b: [*]const f64, out: [*]u8, len: usize) void {
    simd.cmpNe(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_gt_i64(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) void {
    simd.cmpGt(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_ge_i64(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) void {
    simd.cmpGe(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_lt_i64(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) void {
    simd.cmpLt(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_le_i64(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) void {
    simd.cmpLe(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_eq_i64(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) void {
    simd.cmpEq(i64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_cmp_ne_i64(a: [*]const i64, b: [*]const i64, out: [*]u8, len: usize) void {
    simd.cmpNe(i64, a[0..len], b[0..len], out[0..len]);
}

// --- Conditional Operations ---

export fn galleon_select_f64(mask: [*]const u8, then_val: [*]const f64, else_val: [*]const f64, out: [*]f64, len: usize) void {
    simd.selectF64(mask[0..len], then_val[0..len], else_val[0..len], out[0..len]);
}

export fn galleon_select_i64(mask: [*]const u8, then_val: [*]const i64, else_val: [*]const i64, out: [*]i64, len: usize) void {
    simd.selectI64(mask[0..len], then_val[0..len], else_val[0..len], out[0..len]);
}

export fn galleon_select_scalar_f64(mask: [*]const u8, then_val: [*]const f64, else_scalar: f64, out: [*]f64, len: usize) void {
    simd.selectScalarF64(mask[0..len], then_val[0..len], else_scalar, out[0..len]);
}

export fn galleon_is_null_f64(data: [*]const f64, out: [*]u8, len: usize) void {
    simd.isNullF64(data[0..len], out[0..len]);
}

export fn galleon_is_not_null_f64(data: [*]const f64, out: [*]u8, len: usize) void {
    simd.isNotNullF64(data[0..len], out[0..len]);
}

export fn galleon_fill_null_f64(data: [*]const f64, fill_value: f64, out: [*]f64, len: usize) void {
    simd.fillNullF64(data[0..len], fill_value, out[0..len]);
}

export fn galleon_fill_null_forward_f64(data: [*]const f64, out: [*]f64, len: usize) void {
    simd.fillNullForwardF64(data[0..len], out[0..len]);
}

export fn galleon_fill_null_backward_f64(data: [*]const f64, out: [*]f64, len: usize) void {
    simd.fillNullBackwardF64(data[0..len], out[0..len]);
}

export fn galleon_coalesce2_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.coalesce2F64(a[0..len], b[0..len], out[0..len]);
}

export fn galleon_count_null_f64(data: [*]const f64, len: usize) usize {
    return simd.countNullF64(data[0..len]);
}

export fn galleon_count_not_null_f64(data: [*]const f64, len: usize) usize {
    return simd.countNotNullF64(data[0..len]);
}

// --- Statistics Operations ---

export fn galleon_median_f64(data: [*]const f64, len: usize, out_valid: *bool) f64 {
    if (simd.median(f64, data[0..len], std.heap.c_allocator)) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

export fn galleon_quantile_f64(data: [*]const f64, len: usize, q: f64, out_valid: *bool) f64 {
    if (simd.quantile(f64, data[0..len], q, std.heap.c_allocator)) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

export fn galleon_skewness_f64(data: [*]const f64, len: usize, out_valid: *bool) f64 {
    if (simd.skewness(f64, data[0..len])) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

export fn galleon_kurtosis_f64(data: [*]const f64, len: usize, out_valid: *bool) f64 {
    if (simd.kurtosis(f64, data[0..len])) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

export fn galleon_correlation_f64(x: [*]const f64, y: [*]const f64, len: usize, out_valid: *bool) f64 {
    if (simd.correlation(f64, x[0..len], y[0..len])) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

export fn galleon_variance_f64(data: [*]const f64, len: usize, out_valid: *bool) f64 {
    if (simd.variance(f64, data[0..len])) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

export fn galleon_stddev_f64(data: [*]const f64, len: usize, out_valid: *bool) f64 {
    if (simd.stdDev(f64, data[0..len])) |result| {
        out_valid.* = true;
        return result;
    } else {
        out_valid.* = false;
        return 0.0;
    }
}

// --- Window Operations ---

export fn galleon_lag_f64(data: [*]const f64, len: usize, offset: usize, default: f64, out: [*]f64) void {
    simd.lag(f64, data[0..len], offset, default, out[0..len]);
}

export fn galleon_lead_f64(data: [*]const f64, len: usize, offset: usize, default: f64, out: [*]f64) void {
    simd.lead(f64, data[0..len], offset, default, out[0..len]);
}

export fn galleon_lag_i64(data: [*]const i64, len: usize, offset: usize, default: i64, out: [*]i64) void {
    simd.lag(i64, data[0..len], offset, default, out[0..len]);
}

export fn galleon_lead_i64(data: [*]const i64, len: usize, offset: usize, default: i64, out: [*]i64) void {
    simd.lead(i64, data[0..len], offset, default, out[0..len]);
}

export fn galleon_row_number(out: [*]u32, len: usize) void {
    simd.rowNumber(out[0..len]);
}

export fn galleon_row_number_partitioned(partition_ids: [*]const u32, out: [*]u32, len: usize) void {
    simd.rowNumberPartitioned(partition_ids[0..len], out[0..len]);
}

export fn galleon_rank_f64(data: [*]const f64, out: [*]u32, len: usize) void {
    simd.rank(f64, data[0..len], out[0..len]);
}

export fn galleon_dense_rank_f64(data: [*]const f64, out: [*]u32, len: usize) void {
    simd.denseRank(f64, data[0..len], out[0..len]);
}

export fn galleon_cumsum_f64(data: [*]const f64, out: [*]f64, len: usize) void {
    simd.cumSum(f64, data[0..len], out[0..len]);
}

export fn galleon_cumsum_i64(data: [*]const i64, out: [*]i64, len: usize) void {
    simd.cumSum(i64, data[0..len], out[0..len]);
}

export fn galleon_cumsum_partitioned_f64(data: [*]const f64, partition_ids: [*]const u32, out: [*]f64, len: usize) void {
    simd.cumSumPartitioned(f64, data[0..len], partition_ids[0..len], out[0..len]);
}

export fn galleon_cummin_f64(data: [*]const f64, out: [*]f64, len: usize) void {
    simd.cumMin(f64, data[0..len], out[0..len]);
}

export fn galleon_cummax_f64(data: [*]const f64, out: [*]f64, len: usize) void {
    simd.cumMax(f64, data[0..len], out[0..len]);
}

export fn galleon_rolling_sum_f64(data: [*]const f64, len: usize, window_size: usize, min_periods: usize, out: [*]f64) void {
    simd.rollingSum(f64, data[0..len], window_size, min_periods, out[0..len]);
}

export fn galleon_rolling_mean_f64(data: [*]const f64, len: usize, window_size: usize, min_periods: usize, out: [*]f64) void {
    simd.rollingMean(f64, data[0..len], window_size, min_periods, out[0..len]);
}

export fn galleon_rolling_min_f64(data: [*]const f64, len: usize, window_size: usize, min_periods: usize, out: [*]f64) void {
    simd.rollingMin(f64, data[0..len], window_size, min_periods, out[0..len], std.heap.c_allocator);
}

export fn galleon_rolling_max_f64(data: [*]const f64, len: usize, window_size: usize, min_periods: usize, out: [*]f64) void {
    simd.rollingMax(f64, data[0..len], window_size, min_periods, out[0..len], std.heap.c_allocator);
}

export fn galleon_rolling_std_f64(data: [*]const f64, len: usize, window_size: usize, min_periods: usize, out: [*]f64) void {
    simd.rollingStd(f64, data[0..len], window_size, min_periods, out[0..len]);
}

export fn galleon_diff_f64(data: [*]const f64, out: [*]f64, len: usize, default: f64) void {
    simd.diff(f64, data[0..len], default, out[0..len]);
}

export fn galleon_diff_n_f64(data: [*]const f64, out: [*]f64, len: usize, n: usize, default: f64) void {
    simd.diffN(f64, data[0..len], n, default, out[0..len]);
}

export fn galleon_pct_change_f64(data: [*]const f64, out: [*]f64, len: usize) void {
    simd.pctChange(f64, data[0..len], out[0..len]);
}

// --- Fold/Horizontal Aggregation Operations ---

export fn galleon_sum_horizontal2_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.sumHorizontal2(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_sum_horizontal3_f64(a: [*]const f64, b: [*]const f64, c: [*]const f64, out: [*]f64, len: usize) void {
    simd.sumHorizontal3(f64, a[0..len], b[0..len], c[0..len], out[0..len]);
}

export fn galleon_min_horizontal2_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.minHorizontal2(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_min_horizontal3_f64(a: [*]const f64, b: [*]const f64, c: [*]const f64, out: [*]f64, len: usize) void {
    simd.minHorizontal3(f64, a[0..len], b[0..len], c[0..len], out[0..len]);
}

export fn galleon_max_horizontal2_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.maxHorizontal2(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_max_horizontal3_f64(a: [*]const f64, b: [*]const f64, c: [*]const f64, out: [*]f64, len: usize) void {
    simd.maxHorizontal3(f64, a[0..len], b[0..len], c[0..len], out[0..len]);
}

export fn galleon_product_horizontal2_f64(a: [*]const f64, b: [*]const f64, out: [*]f64, len: usize) void {
    simd.productHorizontal2(f64, a[0..len], b[0..len], out[0..len]);
}

export fn galleon_product_horizontal3_f64(a: [*]const f64, b: [*]const f64, c: [*]const f64, out: [*]f64, len: usize) void {
    simd.productHorizontal3(f64, a[0..len], b[0..len], c[0..len], out[0..len]);
}

export fn galleon_any_horizontal2(a: [*]const u8, b: [*]const u8, out: [*]u8, len: usize) void {
    simd.anyHorizontal2(a[0..len], b[0..len], out[0..len]);
}

export fn galleon_all_horizontal2(a: [*]const u8, b: [*]const u8, out: [*]u8, len: usize) void {
    simd.allHorizontal2(a[0..len], b[0..len], out[0..len]);
}

export fn galleon_count_non_null_horizontal2_f64(a: [*]const f64, b: [*]const f64, out: [*]u32, len: usize) void {
    simd.countNonNullHorizontal2(a[0..len], b[0..len], out[0..len]);
}

export fn galleon_count_non_null_horizontal3_f64(a: [*]const f64, b: [*]const f64, c: [*]const f64, out: [*]u32, len: usize) void {
    simd.countNonNullHorizontal3(a[0..len], b[0..len], c[0..len], out[0..len]);
}

// --- Filter Operations ---

export fn galleon_filter_gt_f64(
    data: [*]const f64,
    len: usize,
    threshold: f64,
    out_indices: [*]u32,
    out_count: *usize,
) void {
    const result = simd.filterGreaterThan(f64, data[0..len], threshold, out_indices[0..len]);
    out_count.* = result;
}

export fn galleon_filter_mask_gt_f64(
    data: [*]const f64,
    len: usize,
    threshold: f64,
    out_mask: [*]bool,
) void {
    simd.filterMaskGreaterThan(f64, data[0..len], threshold, out_mask[0..len]);
}

export fn galleon_filter_mask_u8_gt_f64(
    data: [*]const f64,
    len: usize,
    threshold: f64,
    out_mask: [*]u8,
) void {
    simd.filterMaskU8GreaterThan(f64, data[0..len], threshold, out_mask[0..len]);
}

// --- Sort Operations ---

/// Argsort for f64 - returns indices that would sort the array
/// Optimized with 11-bit radix, skip-pass, parallel scatter, and Verge detection
export fn galleon_argsort_f64(
    data: [*]const f64,
    len: usize,
    out_indices: [*]u32,
    ascending: bool,
) void {
    simd.argsortF64(data[0..len], out_indices[0..len], ascending);
}

/// Argsort for i64 - returns indices that would sort the array
export fn galleon_argsort_i64(
    data: [*]const i64,
    len: usize,
    out_indices: [*]u32,
    ascending: bool,
) void {
    simd.argsortI64(data[0..len], out_indices[0..len], ascending);
}

/// Direct sort for f64 - sorts values directly (faster than argsort, no index tracking)
export fn galleon_sort_f64(
    data: [*]const f64,
    len: usize,
    out: [*]f64,
    ascending: bool,
) void {
    simd.sortF64(data[0..len], out[0..len], ascending);
}

/// Direct sort for i64 - sorts values directly (faster than argsort, no index tracking)
export fn galleon_sort_i64(
    data: [*]const i64,
    len: usize,
    out: [*]i64,
    ascending: bool,
) void {
    simd.sortI64(data[0..len], out[0..len], ascending);
}

/// Parallel gather for f64 - uses work-stealing parallelism
export fn galleon_gather_f64_parallel(
    src: [*]const f64,
    src_len: usize,
    indices: [*]const u32,
    indices_len: usize,
    dst: [*]f64,
) void {
    simd.gatherF64Parallel(src[0..src_len], indices[0..indices_len], dst[0..indices_len]);
}

/// Parallel gather for i64 - uses work-stealing parallelism
export fn galleon_gather_i64_parallel(
    src: [*]const i64,
    src_len: usize,
    indices: [*]const u32,
    indices_len: usize,
    dst: [*]i64,
) void {
    simd.gatherI64Parallel(src[0..src_len], indices[0..indices_len], dst[0..indices_len]);
}

// --- Mask to Indices Operations ---

export fn galleon_count_mask_true(mask: [*]const u8, len: usize) usize {
    return simd.countMaskTrue(mask[0..len]);
}

export fn galleon_indices_from_mask(mask: [*]const u8, mask_len: usize, out_indices: [*]u32, max_indices: usize) usize {
    return simd.indicesFromMask(mask[0..mask_len], out_indices[0..max_indices]);
}

// ============================================================================
// Int64 Operations
// ============================================================================

export fn galleon_column_i64_create(data: [*]const i64, len: usize) ?*column.ColumnI64 {
    return column.ColumnI64.createFromSlice(std.heap.c_allocator, data[0..len]) catch null;
}

export fn galleon_column_i64_destroy(col: *column.ColumnI64) void {
    col.deinit();
}

export fn galleon_column_i64_len(col: *const column.ColumnI64) usize {
    return col.len();
}

export fn galleon_column_i64_get(col: *const column.ColumnI64, index: usize) i64 {
    return col.get(index) orelse 0;
}

export fn galleon_column_i64_data(col: *const column.ColumnI64) [*]const i64 {
    return col.data().ptr;
}

export fn galleon_sum_i64(data: [*]const i64, len: usize) i64 {
    if (blitz.shouldParallelize(.sum, len)) {
        return blitz.parallelSumInt(i64, data[0..len]);
    }
    return simd.sumInt(i64, data[0..len]);
}

export fn galleon_min_i64(data: [*]const i64, len: usize) i64 {
    if (blitz.shouldParallelize(.min, len)) {
        return blitz.parallelMinInt(i64, data[0..len]) orelse 0;
    }
    return simd.minInt(i64, data[0..len]) orelse 0;
}

export fn galleon_max_i64(data: [*]const i64, len: usize) i64 {
    if (blitz.shouldParallelize(.max, len)) {
        return blitz.parallelMaxInt(i64, data[0..len]) orelse 0;
    }
    return simd.maxInt(i64, data[0..len]) orelse 0;
}

export fn galleon_add_scalar_i64(data: [*]i64, len: usize, scalar: i64) void {
    simd.addScalarInt(i64, data[0..len], scalar);
}

export fn galleon_mul_scalar_i64(data: [*]i64, len: usize, scalar: i64) void {
    simd.mulScalarInt(i64, data[0..len], scalar);
}

export fn galleon_filter_gt_i64(
    data: [*]const i64,
    len: usize,
    threshold: i64,
    out_indices: [*]u32,
    out_count: *usize,
) void {
    const result = simd.filterGreaterThanInt(i64, data[0..len], threshold, out_indices[0..len]);
    out_count.* = result;
}

export fn galleon_filter_mask_u8_gt_i64(
    data: [*]const i64,
    len: usize,
    threshold: i64,
    out_mask: [*]u8,
) void {
    simd.filterMaskU8GreaterThanInt(i64, data[0..len], threshold, out_mask[0..len]);
}

// ============================================================================
// Int32 Operations
// ============================================================================

export fn galleon_column_i32_create(data: [*]const i32, len: usize) ?*column.ColumnI32 {
    return column.ColumnI32.createFromSlice(std.heap.c_allocator, data[0..len]) catch null;
}

export fn galleon_column_i32_destroy(col: *column.ColumnI32) void {
    col.deinit();
}

export fn galleon_column_i32_len(col: *const column.ColumnI32) usize {
    return col.len();
}

export fn galleon_column_i32_get(col: *const column.ColumnI32, index: usize) i32 {
    return col.get(index) orelse 0;
}

export fn galleon_column_i32_data(col: *const column.ColumnI32) [*]const i32 {
    return col.data().ptr;
}

export fn galleon_sum_i32(data: [*]const i32, len: usize) i32 {
    if (blitz.shouldParallelize(.sum, len)) {
        return blitz.parallelSumInt(i32, data[0..len]);
    }
    return simd.sumInt(i32, data[0..len]);
}

export fn galleon_min_i32(data: [*]const i32, len: usize) i32 {
    if (blitz.shouldParallelize(.min, len)) {
        return blitz.parallelMinInt(i32, data[0..len]) orelse 0;
    }
    return simd.minInt(i32, data[0..len]) orelse 0;
}

export fn galleon_max_i32(data: [*]const i32, len: usize) i32 {
    if (blitz.shouldParallelize(.max, len)) {
        return blitz.parallelMaxInt(i32, data[0..len]) orelse 0;
    }
    return simd.maxInt(i32, data[0..len]) orelse 0;
}

export fn galleon_add_scalar_i32(data: [*]i32, len: usize, scalar: i32) void {
    simd.addScalarInt(i32, data[0..len], scalar);
}

export fn galleon_mul_scalar_i32(data: [*]i32, len: usize, scalar: i32) void {
    simd.mulScalarInt(i32, data[0..len], scalar);
}

export fn galleon_filter_gt_i32(
    data: [*]const i32,
    len: usize,
    threshold: i32,
    out_indices: [*]u32,
    out_count: *usize,
) void {
    const result = simd.filterGreaterThanInt(i32, data[0..len], threshold, out_indices[0..len]);
    out_count.* = result;
}

export fn galleon_filter_mask_u8_gt_i32(
    data: [*]const i32,
    len: usize,
    threshold: i32,
    out_mask: [*]u8,
) void {
    simd.filterMaskU8GreaterThanInt(i32, data[0..len], threshold, out_mask[0..len]);
}

export fn galleon_argsort_i32(
    data: [*]const i32,
    len: usize,
    out_indices: [*]u32,
    ascending: bool,
) void {
    simd.argsortI32(data[0..len], out_indices[0..len], ascending);
}

// ============================================================================
// Float32 Operations
// ============================================================================

export fn galleon_column_f32_create(data: [*]const f32, len: usize) ?*column.ColumnF32 {
    return column.ColumnF32.createFromSlice(std.heap.c_allocator, data[0..len]) catch null;
}

export fn galleon_column_f32_destroy(col: *column.ColumnF32) void {
    col.deinit();
}

export fn galleon_column_f32_len(col: *const column.ColumnF32) usize {
    return col.len();
}

export fn galleon_column_f32_get(col: *const column.ColumnF32, index: usize) f32 {
    return col.get(index) orelse 0.0;
}

export fn galleon_column_f32_data(col: *const column.ColumnF32) [*]const f32 {
    return col.data().ptr;
}

export fn galleon_sum_f32(data: [*]const f32, len: usize) f32 {
    if (blitz.shouldParallelize(.sum, len)) {
        return blitz.parallelSum(f32, data[0..len]);
    }
    return simd.sum(f32, data[0..len]);
}

export fn galleon_min_f32(data: [*]const f32, len: usize) f32 {
    if (blitz.shouldParallelize(.min, len)) {
        return blitz.parallelMin(f32, data[0..len]) orelse 0.0;
    }
    return simd.min(f32, data[0..len]) orelse 0.0;
}

export fn galleon_max_f32(data: [*]const f32, len: usize) f32 {
    if (blitz.shouldParallelize(.max, len)) {
        return blitz.parallelMax(f32, data[0..len]) orelse 0.0;
    }
    return simd.max(f32, data[0..len]) orelse 0.0;
}

export fn galleon_mean_f32(data: [*]const f32, len: usize) f32 {
    if (blitz.shouldParallelize(.mean, len)) {
        if (len == 0) return 0.0;
        return blitz.parallelSum(f32, data[0..len]) / @as(f32, @floatFromInt(len));
    }
    return simd.mean(f32, data[0..len]) orelse 0.0;
}

export fn galleon_add_scalar_f32(data: [*]f32, len: usize, scalar: f32) void {
    simd.addScalar(f32, data[0..len], scalar);
}

export fn galleon_mul_scalar_f32(data: [*]f32, len: usize, scalar: f32) void {
    simd.mulScalar(f32, data[0..len], scalar);
}

export fn galleon_filter_gt_f32(
    data: [*]const f32,
    len: usize,
    threshold: f32,
    out_indices: [*]u32,
    out_count: *usize,
) void {
    const result = simd.filterGreaterThan(f32, data[0..len], threshold, out_indices[0..len]);
    out_count.* = result;
}

export fn galleon_filter_mask_u8_gt_f32(
    data: [*]const f32,
    len: usize,
    threshold: f32,
    out_mask: [*]u8,
) void {
    simd.filterMaskU8GreaterThan(f32, data[0..len], threshold, out_mask[0..len]);
}

export fn galleon_argsort_f32(
    data: [*]const f32,
    len: usize,
    out_indices: [*]u32,
    ascending: bool,
) void {
    simd.argsortF32(data[0..len], out_indices[0..len], ascending);
}

// ============================================================================
// Bool Operations
// ============================================================================

export fn galleon_column_bool_create(data: [*]const bool, len: usize) ?*column.Column(bool) {
    return column.Column(bool).createFromSlice(std.heap.c_allocator, data[0..len]) catch null;
}

export fn galleon_column_bool_destroy(col: *column.Column(bool)) void {
    col.deinit();
}

export fn galleon_column_bool_len(col: *const column.Column(bool)) usize {
    return col.len();
}

export fn galleon_column_bool_get(col: *const column.Column(bool), index: usize) bool {
    return col.get(index) orelse false;
}

export fn galleon_column_bool_data(col: *const column.Column(bool)) [*]const bool {
    return col.data().ptr;
}

export fn galleon_count_true(data: [*]const bool, len: usize) usize {
    return simd.countTrue(data[0..len]);
}

export fn galleon_count_false(data: [*]const bool, len: usize) usize {
    return len - simd.countTrue(data[0..len]);
}

// ============================================================================
// GroupBy Aggregation Functions
// ============================================================================

export fn galleon_aggregate_sum_f64_by_group(
    data: [*]const f64,
    group_ids: [*]const u32,
    out_sums: [*]f64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.aggregateSumByGroup(f64, data[0..data_len], group_ids[0..data_len], out_sums[0..num_groups]);
}

export fn galleon_aggregate_sum_i64_by_group(
    data: [*]const i64,
    group_ids: [*]const u32,
    out_sums: [*]i64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.aggregateSumByGroup(i64, data[0..data_len], group_ids[0..data_len], out_sums[0..num_groups]);
}

export fn galleon_aggregate_min_f64_by_group(
    data: [*]const f64,
    group_ids: [*]const u32,
    out_mins: [*]f64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.aggregateMinByGroup(f64, data[0..data_len], group_ids[0..data_len], out_mins[0..num_groups]);
}

export fn galleon_aggregate_min_i64_by_group(
    data: [*]const i64,
    group_ids: [*]const u32,
    out_mins: [*]i64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.aggregateMinByGroup(i64, data[0..data_len], group_ids[0..data_len], out_mins[0..num_groups]);
}

export fn galleon_aggregate_max_f64_by_group(
    data: [*]const f64,
    group_ids: [*]const u32,
    out_maxs: [*]f64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.aggregateMaxByGroup(f64, data[0..data_len], group_ids[0..data_len], out_maxs[0..num_groups]);
}

export fn galleon_aggregate_max_i64_by_group(
    data: [*]const i64,
    group_ids: [*]const u32,
    out_maxs: [*]i64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.aggregateMaxByGroup(i64, data[0..data_len], group_ids[0..data_len], out_maxs[0..num_groups]);
}

export fn galleon_count_by_group(
    group_ids: [*]const u32,
    out_counts: [*]u64,
    data_len: usize,
    num_groups: usize,
) void {
    simd.countByGroup(group_ids[0..data_len], out_counts[0..num_groups]);
}

export fn galleon_hash_i64_column(
    data: [*]const i64,
    out_hashes: [*]u64,
    len: usize,
) void {
    if (blitz.shouldParallelize(.hash, len)) {
        simd.parallelHashInt64Column(data[0..len], out_hashes[0..len]);
    } else {
        simd.hashInt64Column(data[0..len], out_hashes[0..len]);
    }
}

export fn galleon_hash_i32_column(
    data: [*]const i32,
    out_hashes: [*]u64,
    len: usize,
) void {
    if (blitz.shouldParallelize(.hash, len)) {
        simd.parallelHashInt32Column(data[0..len], out_hashes[0..len]);
    } else {
        simd.hashInt32Column(data[0..len], out_hashes[0..len]);
    }
}

export fn galleon_hash_f64_column(
    data: [*]const f64,
    out_hashes: [*]u64,
    len: usize,
) void {
    if (blitz.shouldParallelize(.hash, len)) {
        simd.parallelHashFloat64Column(data[0..len], out_hashes[0..len]);
    } else {
        simd.hashFloat64Column(data[0..len], out_hashes[0..len]);
    }
}

export fn galleon_hash_f32_column(
    data: [*]const f32,
    out_hashes: [*]u64,
    len: usize,
) void {
    if (blitz.shouldParallelize(.hash, len)) {
        simd.parallelHashFloat32Column(data[0..len], out_hashes[0..len]);
    } else {
        simd.hashFloat32Column(data[0..len], out_hashes[0..len]);
    }
}

export fn galleon_combine_hashes(
    hash1: [*]const u64,
    hash2: [*]const u64,
    out_hashes: [*]u64,
    len: usize,
) void {
    if (blitz.shouldParallelize(.hash, len)) {
        simd.parallelCombineHashes(hash1[0..len], hash2[0..len], out_hashes[0..len]);
    } else {
        simd.combineHashes(hash1[0..len], hash2[0..len], out_hashes[0..len]);
    }
}

// ============================================================================
// Join Helper Functions
// ============================================================================

export fn galleon_gather_f64(
    src: [*]const f64,
    src_len: usize,
    indices: [*]const i32,
    dst: [*]f64,
    dst_len: usize,
) void {
    _ = src_len; // Source length used for bounds checking in simd.gatherF64
    simd.gatherF64(src[0..1 << 30], indices[0..dst_len], dst[0..dst_len]);
}

export fn galleon_gather_i64(
    src: [*]const i64,
    src_len: usize,
    indices: [*]const i32,
    dst: [*]i64,
    dst_len: usize,
) void {
    _ = src_len;
    simd.gatherI64(src[0..1 << 30], indices[0..dst_len], dst[0..dst_len]);
}

export fn galleon_gather_i32(
    src: [*]const i32,
    src_len: usize,
    indices: [*]const i32,
    dst: [*]i32,
    dst_len: usize,
) void {
    _ = src_len;
    simd.gatherI32(src[0..1 << 30], indices[0..dst_len], dst[0..dst_len]);
}

export fn galleon_gather_f32(
    src: [*]const f32,
    src_len: usize,
    indices: [*]const i32,
    dst: [*]f32,
    dst_len: usize,
) void {
    _ = src_len;
    simd.gatherF32(src[0..1 << 30], indices[0..dst_len], dst[0..dst_len]);
}

// ============================================================================
// GroupBy Operations (Full Zig Implementation)
// ============================================================================

/// GroupBy result handle
pub const GroupByResultHandle = struct {
    result: groupby.GroupByResult,
};

/// Extended GroupBy result handle (includes firstRowIdx and groupCounts)
pub const GroupByResultExtHandle = struct {
    result: groupby.GroupByResultExt,
};

/// Compute group IDs from hashes - this is the main groupby entry point
/// Returns a handle that must be freed with galleon_groupby_result_destroy
export fn galleon_groupby_compute(
    hashes: [*]const u64,
    hashes_len: usize,
) ?*GroupByResultHandle {
    const handle = std.heap.c_allocator.create(GroupByResultHandle) catch return null;
    handle.result = groupby.computeGroupIds(std.heap.c_allocator, hashes[0..hashes_len]) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Compute group IDs with key verification (for hash collision handling)
export fn galleon_groupby_compute_with_keys_i64(
    hashes: [*]const u64,
    keys: [*]const i64,
    len: usize,
) ?*GroupByResultHandle {
    const handle = std.heap.c_allocator.create(GroupByResultHandle) catch return null;
    handle.result = groupby.computeGroupIdsWithKeys(
        std.heap.c_allocator,
        hashes[0..len],
        keys[0..len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Get number of groups from result
export fn galleon_groupby_result_num_groups(handle: *const GroupByResultHandle) u32 {
    return handle.result.num_groups;
}

/// Get pointer to group IDs array
export fn galleon_groupby_result_group_ids(handle: *const GroupByResultHandle) [*]const u32 {
    return handle.result.group_ids.ptr;
}

/// Free groupby result
export fn galleon_groupby_result_destroy(handle: *GroupByResultHandle) void {
    handle.result.deinit();
    std.heap.c_allocator.destroy(handle);
}

// ============================================================================
// Extended GroupBy (with firstRowIdx and groupCounts - eliminates Go loops)
// ============================================================================

/// Compute group IDs with firstRowIdx and groupCounts in a single call
/// Returns a handle that must be freed with galleon_groupby_result_ext_destroy
export fn galleon_groupby_compute_ext(
    hashes: [*]const u64,
    hashes_len: usize,
) ?*GroupByResultExtHandle {
    const handle = std.heap.c_allocator.create(GroupByResultExtHandle) catch return null;
    handle.result = groupby.computeGroupIdsExt(std.heap.c_allocator, hashes[0..hashes_len]) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Get number of groups from extended result
export fn galleon_groupby_result_ext_num_groups(handle: *const GroupByResultExtHandle) u32 {
    return handle.result.num_groups;
}

/// Get pointer to group IDs array from extended result
export fn galleon_groupby_result_ext_group_ids(handle: *const GroupByResultExtHandle) [*]const u32 {
    return handle.result.group_ids.ptr;
}

/// Get pointer to firstRowIdx array from extended result
export fn galleon_groupby_result_ext_first_row_idx(handle: *const GroupByResultExtHandle) [*]const u32 {
    return handle.result.first_row_idx.ptr;
}

/// Get pointer to groupCounts array from extended result
export fn galleon_groupby_result_ext_group_counts(handle: *const GroupByResultExtHandle) [*]const u32 {
    return handle.result.group_counts.ptr;
}

/// Free extended groupby result
export fn galleon_groupby_result_ext_destroy(handle: *GroupByResultExtHandle) void {
    handle.result.deinit();
    std.heap.c_allocator.destroy(handle);
}

// ============================================================================
// End-to-End GroupBy (Single CGO Call - Phase 2 Optimization)
// ============================================================================

/// Handle for end-to-end groupby sum result
pub const GroupBySumResultHandle = struct {
    result: groupby.GroupBySumResult,
};

/// End-to-end groupby sum: pass key and value columns, get aggregated result
/// Single CGO call - hash, group, aggregate, extract keys all in Zig
export fn galleon_groupby_sum_e2e_i64_f64(
    key_data: [*]const i64,
    value_data: [*]const f64,
    len: usize,
) ?*GroupBySumResultHandle {
    const handle = std.heap.c_allocator.create(GroupBySumResultHandle) catch return null;
    handle.result = groupby.groupbySumI64KeyF64Value(
        std.heap.c_allocator,
        key_data[0..len],
        value_data[0..len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

export fn galleon_groupby_sum_result_num_groups(handle: *const GroupBySumResultHandle) u32 {
    return handle.result.num_groups;
}

export fn galleon_groupby_sum_result_keys(handle: *const GroupBySumResultHandle) [*]const i64 {
    return handle.result.keys.ptr;
}

export fn galleon_groupby_sum_result_sums(handle: *const GroupBySumResultHandle) [*]const f64 {
    return handle.result.sums.ptr;
}

export fn galleon_groupby_sum_result_destroy(handle: *GroupBySumResultHandle) void {
    handle.result.deinit();
    std.heap.c_allocator.destroy(handle);
}

/// Handle for end-to-end groupby multi-agg result
pub const GroupByMultiAggResultHandle = struct {
    result: groupby.GroupByMultiAggResult,
};

/// End-to-end groupby with sum, min, max, count
export fn galleon_groupby_multi_agg_e2e_i64_f64(
    key_data: [*]const i64,
    value_data: [*]const f64,
    len: usize,
) ?*GroupByMultiAggResultHandle {
    const handle = std.heap.c_allocator.create(GroupByMultiAggResultHandle) catch return null;
    handle.result = groupby.groupbyMultiAggI64KeyF64Value(
        std.heap.c_allocator,
        key_data[0..len],
        value_data[0..len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

export fn galleon_groupby_multi_agg_result_num_groups(handle: *const GroupByMultiAggResultHandle) u32 {
    return handle.result.num_groups;
}

export fn galleon_groupby_multi_agg_result_keys(handle: *const GroupByMultiAggResultHandle) [*]const i64 {
    return handle.result.keys.ptr;
}

export fn galleon_groupby_multi_agg_result_sums(handle: *const GroupByMultiAggResultHandle) [*]const f64 {
    return handle.result.sums.ptr;
}

export fn galleon_groupby_multi_agg_result_mins(handle: *const GroupByMultiAggResultHandle) [*]const f64 {
    return handle.result.mins.ptr;
}

export fn galleon_groupby_multi_agg_result_maxs(handle: *const GroupByMultiAggResultHandle) [*]const f64 {
    return handle.result.maxs.ptr;
}

export fn galleon_groupby_multi_agg_result_counts(handle: *const GroupByMultiAggResultHandle) [*]const u64 {
    return handle.result.counts.ptr;
}

export fn galleon_groupby_multi_agg_result_destroy(handle: *GroupByMultiAggResultHandle) void {
    handle.result.deinit();
    std.heap.c_allocator.destroy(handle);
}

// ============================================================================
// GroupBy Operations
// ============================================================================

/// Aggregate sum f64 by group (uses group IDs from groupby result)
export fn galleon_groupby_sum_f64(
    data: [*]const f64,
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]f64,
    num_groups: usize,
) void {
    groupby.sumByGroup(f64, data[0..data_len], group_ids[0..data_len], out[0..num_groups]);
}

/// Aggregate sum i64 by group
export fn galleon_groupby_sum_i64(
    data: [*]const i64,
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]i64,
    num_groups: usize,
) void {
    groupby.sumByGroup(i64, data[0..data_len], group_ids[0..data_len], out[0..num_groups]);
}

/// Aggregate min f64 by group
export fn galleon_groupby_min_f64(
    data: [*]const f64,
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]f64,
    num_groups: usize,
) void {
    groupby.minByGroup(f64, data[0..data_len], group_ids[0..data_len], out[0..num_groups]);
}

/// Aggregate min i64 by group
export fn galleon_groupby_min_i64(
    data: [*]const i64,
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]i64,
    num_groups: usize,
) void {
    groupby.minByGroup(i64, data[0..data_len], group_ids[0..data_len], out[0..num_groups]);
}

/// Aggregate max f64 by group
export fn galleon_groupby_max_f64(
    data: [*]const f64,
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]f64,
    num_groups: usize,
) void {
    groupby.maxByGroup(f64, data[0..data_len], group_ids[0..data_len], out[0..num_groups]);
}

/// Aggregate max i64 by group
export fn galleon_groupby_max_i64(
    data: [*]const i64,
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]i64,
    num_groups: usize,
) void {
    groupby.maxByGroup(i64, data[0..data_len], group_ids[0..data_len], out[0..num_groups]);
}

/// Count by group
export fn galleon_groupby_count(
    group_ids: [*]const u32,
    data_len: usize,
    out: [*]u64,
    num_groups: usize,
) void {
    groupby.countByGroup(group_ids[0..data_len], out[0..num_groups]);
}

// ============================================================================
// ChunkedColumn V2 Operations
// ============================================================================

const chunked = @import("chunked_column.zig");

/// ChunkedColumn handle for CGO
pub const ChunkedColumnF64Handle = chunked.ChunkedColumn(f64);

/// Create a new chunked column from data
export fn galleon_chunked_f64_create(data: [*]const f64, len: usize) ?*ChunkedColumnF64Handle {
    return chunked.ChunkedColumn(f64).createFromSlice(std.heap.c_allocator, data[0..len]) catch null;
}

/// Destroy a chunked column
export fn galleon_chunked_f64_destroy(col: *ChunkedColumnF64Handle) void {
    col.destroy();
}

/// Get total length of chunked column
export fn galleon_chunked_f64_len(col: *const ChunkedColumnF64Handle) usize {
    return col.total_length;
}

/// Get number of chunks
export fn galleon_chunked_f64_num_chunks(col: *const ChunkedColumnF64Handle) usize {
    return col.num_chunks;
}

/// Get element at index (returns 0 if out of bounds)
export fn galleon_chunked_f64_get(col: *const ChunkedColumnF64Handle, index: usize) f64 {
    return col.get(index) orelse 0.0;
}

/// Copy all data to output buffer (caller must allocate)
export fn galleon_chunked_f64_copy_to_slice(col: *const ChunkedColumnF64Handle, out: [*]f64) void {
    col.copyToSlice(out[0..col.total_length]);
}

/// Parallel sum over all chunks
export fn galleon_chunked_f64_sum(col: *const ChunkedColumnF64Handle) f64 {
    return chunked.OpsF64.sum(col);
}

/// Parallel min over all chunks
export fn galleon_chunked_f64_min(col: *const ChunkedColumnF64Handle) f64 {
    return chunked.OpsF64.min(col) orelse 0.0;
}

/// Parallel max over all chunks
export fn galleon_chunked_f64_max(col: *const ChunkedColumnF64Handle) f64 {
    return chunked.OpsF64.max(col) orelse 0.0;
}

/// Parallel mean over all chunks
export fn galleon_chunked_f64_mean(col: *const ChunkedColumnF64Handle) f64 {
    return chunked.OpsF64.mean(col) orelse 0.0;
}

/// Filter greater than - returns new chunked column
export fn galleon_chunked_f64_filter_gt(col: *const ChunkedColumnF64Handle, threshold: f64) ?*ChunkedColumnF64Handle {
    return chunked.OpsF64.filterGt(col, std.heap.c_allocator, threshold) catch null;
}

/// Filter less than - returns new chunked column
export fn galleon_chunked_f64_filter_lt(col: *const ChunkedColumnF64Handle, threshold: f64) ?*ChunkedColumnF64Handle {
    return chunked.OpsF64.filterLt(col, std.heap.c_allocator, threshold) catch null;
}

/// Argsort result handle
pub const ChunkedSeriesArgsortResult = struct {
    indices: []u32,
    allocator: Allocator,
};

/// Argsort - returns indices that would sort the column
export fn galleon_chunked_f64_argsort(col: *chunked.ChunkedColumn(f64)) ?*ChunkedSeriesArgsortResult {
    const result = chunked.OpsF64.argsort(col, std.heap.c_allocator) catch return null;
    const handle = std.heap.c_allocator.create(ChunkedSeriesArgsortResult) catch {
        result.allocator.free(result.indices);
        return null;
    };
    handle.indices = result.indices;
    handle.allocator = result.allocator;
    return handle;
}

/// Get argsort result length
export fn galleon_chunked_argsort_len(handle: *const ChunkedSeriesArgsortResult) usize {
    return handle.indices.len;
}

/// Get argsort result indices pointer
export fn galleon_chunked_argsort_indices(handle: *const ChunkedSeriesArgsortResult) [*]const u32 {
    return handle.indices.ptr;
}

/// Free argsort result
export fn galleon_chunked_argsort_destroy(handle: *ChunkedSeriesArgsortResult) void {
    handle.allocator.free(handle.indices);
    std.heap.c_allocator.destroy(handle);
}

/// Sort - returns new sorted chunked column
export fn galleon_chunked_f64_sort(col: *chunked.ChunkedColumn(f64)) ?*ChunkedColumnF64Handle {
    return chunked.OpsF64.sort(col, std.heap.c_allocator) catch null;
}

// ============================================================================
// Arrow C Data Interface Operations (Zig-Managed)
// ============================================================================
// Go sends raw data to Zig. Zig creates and manages Arrow arrays.
// This ensures all Arrow operations happen in Zig, not Go.

/// Re-export Arrow types for CGO
pub const ArrowSchema = arrow.ArrowSchema;
pub const ArrowArray = arrow.ArrowArray;
pub const ManagedArrowArray = arrow.ManagedArrowArray;

// --- Managed Arrow Array Creation (Go sends raw data, Zig creates Arrow array) ---

/// Create a Float64 Arrow array from raw data
/// Go passes a pointer to float64 data, Zig copies it into an Arrow-managed buffer
export fn galleon_series_create_f64(data: [*]const f64, len: usize) ?*ManagedArrowArray {
    return ManagedArrowArray.createF64(std.heap.c_allocator, data[0..len]) catch null;
}

/// Create an Int64 Arrow array from raw data
export fn galleon_series_create_i64(data: [*]const i64, len: usize) ?*ManagedArrowArray {
    return ManagedArrowArray.createI64(std.heap.c_allocator, data[0..len]) catch null;
}

/// Create a Float64 Arrow array with null values
/// valid_bitmap: packed bits where 1=valid, 0=null (LSB first, Arrow format)
export fn galleon_series_create_f64_with_nulls(
    data: [*]const f64,
    len: usize,
    valid_bitmap: [*]const u8,
    bitmap_len: usize,
    null_count: i64,
) ?*ManagedArrowArray {
    return ManagedArrowArray.createF64WithNulls(
        std.heap.c_allocator,
        data[0..len],
        valid_bitmap[0..bitmap_len],
        null_count,
    ) catch null;
}

/// Create an Int64 Arrow array with null values
export fn galleon_series_create_i64_with_nulls(
    data: [*]const i64,
    len: usize,
    valid_bitmap: [*]const u8,
    bitmap_len: usize,
    null_count: i64,
) ?*ManagedArrowArray {
    return ManagedArrowArray.createI64WithNulls(
        std.heap.c_allocator,
        data[0..len],
        valid_bitmap[0..bitmap_len],
        null_count,
    ) catch null;
}

/// Destroy a managed Arrow array (frees all memory)
export fn galleon_series_destroy(arr: *ManagedArrowArray) void {
    arr.deinit();
}

// --- Managed Arrow Array Properties ---

/// Get length of managed Arrow array
export fn galleon_series_len(arr: *const ManagedArrowArray) usize {
    return arr.len();
}

/// Get null count of managed Arrow array
export fn galleon_series_null_count(arr: *const ManagedArrowArray) i64 {
    return arr.nullCount();
}

/// Check if managed Arrow array has nulls
export fn galleon_series_has_nulls(arr: *const ManagedArrowArray) bool {
    return arr.hasNulls();
}

// --- Managed Arrow Array SIMD Operations ---

/// Sum Float64 managed Arrow array using SIMD
export fn galleon_series_sum_f64(arr: *const ManagedArrowArray) f64 {
    return arr.sumF64();
}

/// Min Float64 managed Arrow array using SIMD
export fn galleon_series_min_f64(arr: *const ManagedArrowArray) f64 {
    return arr.minF64();
}

/// Max Float64 managed Arrow array using SIMD
export fn galleon_series_max_f64(arr: *const ManagedArrowArray) f64 {
    return arr.maxF64();
}

/// Mean Float64 managed Arrow array
export fn galleon_series_mean_f64(arr: *const ManagedArrowArray) f64 {
    return arr.meanF64();
}

/// Sum Int64 managed Arrow array using SIMD
export fn galleon_series_sum_i64(arr: *const ManagedArrowArray) i64 {
    return arr.sumI64();
}

/// Min Int64 managed Arrow array
export fn galleon_series_min_i64(arr: *const ManagedArrowArray) i64 {
    return arr.minI64();
}

/// Max Int64 managed Arrow array
export fn galleon_series_max_i64(arr: *const ManagedArrowArray) i64 {
    return arr.maxI64();
}

// --- Arrow Sort Operations ---

/// Argsort result handle - owns the indices array
pub const SeriesArgsortResult = struct {
    indices: []u32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SeriesArgsortResult) void {
        if (self.indices.len > 0) {
            self.allocator.free(self.indices);
        }
        self.allocator.destroy(self);
    }
};

/// Argsort Float64 Arrow array - returns indices that would sort the array
export fn galleon_series_argsort_f64(arr: *const ManagedArrowArray, ascending: bool) ?*SeriesArgsortResult {
    const result = std.heap.c_allocator.create(SeriesArgsortResult) catch return null;
    result.allocator = std.heap.c_allocator;
    result.indices = arr.argsortF64(ascending) catch {
        std.heap.c_allocator.destroy(result);
        return null;
    };
    return result;
}

/// Argsort Int64 Arrow array
export fn galleon_series_argsort_i64(arr: *const ManagedArrowArray, ascending: bool) ?*SeriesArgsortResult {
    const result = std.heap.c_allocator.create(SeriesArgsortResult) catch return null;
    result.allocator = std.heap.c_allocator;
    result.indices = arr.argsortI64(ascending) catch {
        std.heap.c_allocator.destroy(result);
        return null;
    };
    return result;
}

/// Get argsort result length
export fn galleon_series_argsort_len(result: *const SeriesArgsortResult) usize {
    return result.indices.len;
}

/// Get argsort result indices pointer
export fn galleon_series_argsort_indices(result: *const SeriesArgsortResult) [*]const u32 {
    return result.indices.ptr;
}

/// Free argsort result
export fn galleon_series_argsort_destroy(result: *SeriesArgsortResult) void {
    result.deinit();
}

/// Sort Float64 Arrow array - returns new sorted ManagedArrowArray
export fn galleon_series_sort_f64(arr: *const ManagedArrowArray, ascending: bool) ?*ManagedArrowArray {
    return arr.sortF64(ascending) catch null;
}

/// Sort Int64 Arrow array - returns new sorted ManagedArrowArray
export fn galleon_series_sort_i64(arr: *const ManagedArrowArray, ascending: bool) ?*ManagedArrowArray {
    return arr.sortI64(ascending) catch null;
}

// --- Arrow Data Access ---

/// Check if value at index is valid (not null)
export fn galleon_series_is_valid(arr: *const ManagedArrowArray, index: usize) bool {
    return arr.isValidAt(index);
}

/// Get Float64 value at index, returns NaN if null or out of bounds
export fn galleon_series_get_f64(arr: *const ManagedArrowArray, index: usize) f64 {
    return arr.getF64(index) orelse std.math.nan(f64);
}

/// Get Int64 value at index, returns 0 if null or out of bounds
/// Use galleon_series_is_valid to check validity first
export fn galleon_series_get_i64(arr: *const ManagedArrowArray, index: usize) i64 {
    return arr.getI64(index) orelse 0;
}

/// Get raw pointer to Float64 data buffer (for zero-copy access)
export fn galleon_series_data_ptr_f64(arr: *const ManagedArrowArray) ?[*]const f64 {
    return arr.getDataPtrF64();
}

/// Get raw pointer to Int64 data buffer
export fn galleon_series_data_ptr_i64(arr: *const ManagedArrowArray) ?[*]const i64 {
    return arr.getDataPtrI64();
}

/// Create a slice of Float64 array [start, end)
export fn galleon_series_slice_f64(arr: *const ManagedArrowArray, start: usize, end: usize) ?*ManagedArrowArray {
    return arr.sliceF64(start, end) catch null;
}

/// Create a slice of Int64 array [start, end)
export fn galleon_series_slice_i64(arr: *const ManagedArrowArray, start: usize, end: usize) ?*ManagedArrowArray {
    return arr.sliceI64(start, end) catch null;
}

/// Copy Float64 data to provided buffer, returns number of elements copied
export fn galleon_series_copy_f64(arr: *const ManagedArrowArray, dest: [*]f64, dest_len: usize) usize {
    return arr.copyToF64(dest[0..dest_len]);
}

/// Copy Int64 data to provided buffer, returns number of elements copied
export fn galleon_series_copy_i64(arr: *const ManagedArrowArray, dest: [*]i64, dest_len: usize) usize {
    return arr.copyToI64(dest[0..dest_len]);
}

// ============================================================================
// Float32 Series Operations
// ============================================================================

/// Create a Float32 Arrow array from raw data
export fn galleon_series_create_f32(data: [*]const f32, len: usize) ?*ManagedArrowArray {
    return ManagedArrowArray.createF32(std.heap.c_allocator, data[0..len]) catch null;
}

/// Create a Float32 Arrow array with null values
export fn galleon_series_create_f32_with_nulls(
    data: [*]const f32,
    len: usize,
    valid_bitmap: [*]const u8,
    bitmap_len: usize,
    null_count: i64,
) ?*ManagedArrowArray {
    return ManagedArrowArray.createF32WithNulls(
        std.heap.c_allocator,
        data[0..len],
        valid_bitmap[0..bitmap_len],
        null_count,
    ) catch null;
}

/// Sum Float32 managed Arrow array
export fn galleon_series_sum_f32(arr: *const ManagedArrowArray) f32 {
    return arr.sumF32();
}

/// Min Float32 managed Arrow array
export fn galleon_series_min_f32(arr: *const ManagedArrowArray) f32 {
    return arr.minF32();
}

/// Max Float32 managed Arrow array
export fn galleon_series_max_f32(arr: *const ManagedArrowArray) f32 {
    return arr.maxF32();
}

/// Mean Float32 managed Arrow array
export fn galleon_series_mean_f32(arr: *const ManagedArrowArray) f32 {
    return arr.meanF32();
}

/// Argsort Float32 Arrow array
export fn galleon_series_argsort_f32(arr: *const ManagedArrowArray, ascending: bool) ?*SeriesArgsortResult {
    const result = std.heap.c_allocator.create(SeriesArgsortResult) catch return null;
    result.allocator = std.heap.c_allocator;
    result.indices = arr.argsortF32(ascending) catch {
        std.heap.c_allocator.destroy(result);
        return null;
    };
    return result;
}

/// Sort Float32 Arrow array
export fn galleon_series_sort_f32(arr: *const ManagedArrowArray, ascending: bool) ?*ManagedArrowArray {
    return arr.sortF32(ascending) catch null;
}

/// Get Float32 value at index
export fn galleon_series_get_f32(arr: *const ManagedArrowArray, index: usize) f32 {
    return arr.getF32(index) orelse std.math.nan(f32);
}

/// Get raw pointer to Float32 data buffer
export fn galleon_series_data_ptr_f32(arr: *const ManagedArrowArray) ?[*]const f32 {
    return arr.getDataPtrF32();
}

/// Create a slice of Float32 array
export fn galleon_series_slice_f32(arr: *const ManagedArrowArray, start: usize, end: usize) ?*ManagedArrowArray {
    return arr.sliceF32(start, end) catch null;
}

/// Copy Float32 data to provided buffer
export fn galleon_series_copy_f32(arr: *const ManagedArrowArray, dest: [*]f32, dest_len: usize) usize {
    return arr.copyToF32(dest[0..dest_len]);
}

// ============================================================================
// Int32 Series Operations
// ============================================================================

/// Create an Int32 Arrow array from raw data
export fn galleon_series_create_i32(data: [*]const i32, len: usize) ?*ManagedArrowArray {
    return ManagedArrowArray.createI32(std.heap.c_allocator, data[0..len]) catch null;
}

/// Create an Int32 Arrow array with null values
export fn galleon_series_create_i32_with_nulls(
    data: [*]const i32,
    len: usize,
    valid_bitmap: [*]const u8,
    bitmap_len: usize,
    null_count: i64,
) ?*ManagedArrowArray {
    return ManagedArrowArray.createI32WithNulls(
        std.heap.c_allocator,
        data[0..len],
        valid_bitmap[0..bitmap_len],
        null_count,
    ) catch null;
}

/// Sum Int32 managed Arrow array
export fn galleon_series_sum_i32(arr: *const ManagedArrowArray) i32 {
    return arr.sumI32();
}

/// Min Int32 managed Arrow array
export fn galleon_series_min_i32(arr: *const ManagedArrowArray) i32 {
    return arr.minI32();
}

/// Max Int32 managed Arrow array
export fn galleon_series_max_i32(arr: *const ManagedArrowArray) i32 {
    return arr.maxI32();
}

/// Argsort Int32 Arrow array
export fn galleon_series_argsort_i32(arr: *const ManagedArrowArray, ascending: bool) ?*SeriesArgsortResult {
    const result = std.heap.c_allocator.create(SeriesArgsortResult) catch return null;
    result.allocator = std.heap.c_allocator;
    result.indices = arr.argsortI32(ascending) catch {
        std.heap.c_allocator.destroy(result);
        return null;
    };
    return result;
}

/// Sort Int32 Arrow array
export fn galleon_series_sort_i32(arr: *const ManagedArrowArray, ascending: bool) ?*ManagedArrowArray {
    return arr.sortI32(ascending) catch null;
}

/// Get Int32 value at index
export fn galleon_series_get_i32(arr: *const ManagedArrowArray, index: usize) i32 {
    return arr.getI32(index) orelse 0;
}

/// Get raw pointer to Int32 data buffer
export fn galleon_series_data_ptr_i32(arr: *const ManagedArrowArray) ?[*]const i32 {
    return arr.getDataPtrI32();
}

/// Create a slice of Int32 array
export fn galleon_series_slice_i32(arr: *const ManagedArrowArray, start: usize, end: usize) ?*ManagedArrowArray {
    return arr.sliceI32(start, end) catch null;
}

/// Copy Int32 data to provided buffer
export fn galleon_series_copy_i32(arr: *const ManagedArrowArray, dest: [*]i32, dest_len: usize) usize {
    return arr.copyToI32(dest[0..dest_len]);
}

// ============================================================================
// UInt64 Series Operations
// ============================================================================

/// Create a UInt64 Arrow array from raw data
export fn galleon_series_create_u64(data: [*]const u64, len: usize) ?*ManagedArrowArray {
    return ManagedArrowArray.createU64(std.heap.c_allocator, data[0..len]) catch null;
}

/// Create a UInt64 Arrow array with null values
export fn galleon_series_create_u64_with_nulls(
    data: [*]const u64,
    len: usize,
    valid_bitmap: [*]const u8,
    bitmap_len: usize,
    null_count: i64,
) ?*ManagedArrowArray {
    return ManagedArrowArray.createU64WithNulls(
        std.heap.c_allocator,
        data[0..len],
        valid_bitmap[0..bitmap_len],
        null_count,
    ) catch null;
}

/// Sum UInt64 managed Arrow array
export fn galleon_series_sum_u64(arr: *const ManagedArrowArray) u64 {
    return arr.sumU64();
}

/// Min UInt64 managed Arrow array
export fn galleon_series_min_u64(arr: *const ManagedArrowArray) u64 {
    return arr.minU64();
}

/// Max UInt64 managed Arrow array
export fn galleon_series_max_u64(arr: *const ManagedArrowArray) u64 {
    return arr.maxU64();
}

/// Argsort UInt64 Arrow array
export fn galleon_series_argsort_u64(arr: *const ManagedArrowArray, ascending: bool) ?*SeriesArgsortResult {
    const result = std.heap.c_allocator.create(SeriesArgsortResult) catch return null;
    result.allocator = std.heap.c_allocator;
    result.indices = arr.argsortU64(ascending) catch {
        std.heap.c_allocator.destroy(result);
        return null;
    };
    return result;
}

/// Sort UInt64 Arrow array
export fn galleon_series_sort_u64(arr: *const ManagedArrowArray, ascending: bool) ?*ManagedArrowArray {
    return arr.sortU64(ascending) catch null;
}

/// Get UInt64 value at index
export fn galleon_series_get_u64(arr: *const ManagedArrowArray, index: usize) u64 {
    return arr.getU64(index) orelse 0;
}

/// Get raw pointer to UInt64 data buffer
export fn galleon_series_data_ptr_u64(arr: *const ManagedArrowArray) ?[*]const u64 {
    return arr.getDataPtrU64();
}

/// Create a slice of UInt64 array
export fn galleon_series_slice_u64(arr: *const ManagedArrowArray, start: usize, end: usize) ?*ManagedArrowArray {
    return arr.sliceU64(start, end) catch null;
}

/// Copy UInt64 data to provided buffer
export fn galleon_series_copy_u64(arr: *const ManagedArrowArray, dest: [*]u64, dest_len: usize) usize {
    return arr.copyToU64(dest[0..dest_len]);
}

// ============================================================================
// UInt32 Series Operations
// ============================================================================

/// Create a UInt32 Arrow array from raw data
export fn galleon_series_create_u32(data: [*]const u32, len: usize) ?*ManagedArrowArray {
    return ManagedArrowArray.createU32(std.heap.c_allocator, data[0..len]) catch null;
}

/// Create a UInt32 Arrow array with null values
export fn galleon_series_create_u32_with_nulls(
    data: [*]const u32,
    len: usize,
    valid_bitmap: [*]const u8,
    bitmap_len: usize,
    null_count: i64,
) ?*ManagedArrowArray {
    return ManagedArrowArray.createU32WithNulls(
        std.heap.c_allocator,
        data[0..len],
        valid_bitmap[0..bitmap_len],
        null_count,
    ) catch null;
}

/// Sum UInt32 managed Arrow array
export fn galleon_series_sum_u32(arr: *const ManagedArrowArray) u32 {
    return arr.sumU32();
}

/// Min UInt32 managed Arrow array
export fn galleon_series_min_u32(arr: *const ManagedArrowArray) u32 {
    return arr.minU32();
}

/// Max UInt32 managed Arrow array
export fn galleon_series_max_u32(arr: *const ManagedArrowArray) u32 {
    return arr.maxU32();
}

/// Argsort UInt32 Arrow array
export fn galleon_series_argsort_u32(arr: *const ManagedArrowArray, ascending: bool) ?*SeriesArgsortResult {
    const result = std.heap.c_allocator.create(SeriesArgsortResult) catch return null;
    result.allocator = std.heap.c_allocator;
    result.indices = arr.argsortU32(ascending) catch {
        std.heap.c_allocator.destroy(result);
        return null;
    };
    return result;
}

/// Sort UInt32 Arrow array
export fn galleon_series_sort_u32(arr: *const ManagedArrowArray, ascending: bool) ?*ManagedArrowArray {
    return arr.sortU32(ascending) catch null;
}

/// Get UInt32 value at index
export fn galleon_series_get_u32(arr: *const ManagedArrowArray, index: usize) u32 {
    return arr.getU32(index) orelse 0;
}

/// Get raw pointer to UInt32 data buffer
export fn galleon_series_data_ptr_u32(arr: *const ManagedArrowArray) ?[*]const u32 {
    return arr.getDataPtrU32();
}

/// Create a slice of UInt32 array
export fn galleon_series_slice_u32(arr: *const ManagedArrowArray, start: usize, end: usize) ?*ManagedArrowArray {
    return arr.sliceU32(start, end) catch null;
}

/// Copy UInt32 data to provided buffer
export fn galleon_series_copy_u32(arr: *const ManagedArrowArray, dest: [*]u32, dest_len: usize) usize {
    return arr.copyToU32(dest[0..dest_len]);
}

// --- Arrow Filter Operations ---

/// Result for comparison operations - holds boolean mask
pub const SeriesFilterResult = struct {
    mask: []bool,
    allocator: std.mem.Allocator,
};

/// Create filter result from mask
fn createSeriesFilterResult(mask: []bool) ?*SeriesFilterResult {
    const result = std.heap.c_allocator.create(SeriesFilterResult) catch return null;
    result.mask = mask;
    result.allocator = std.heap.c_allocator;
    return result;
}

/// Get filter result length
export fn galleon_series_filter_result_len(result: *const SeriesFilterResult) usize {
    return result.mask.len;
}

/// Get filter result mask pointer
export fn galleon_series_filter_result_mask(result: *const SeriesFilterResult) [*]const bool {
    return result.mask.ptr;
}

/// Destroy filter result
export fn galleon_series_filter_result_destroy(result: *SeriesFilterResult) void {
    result.allocator.free(result.mask);
    std.heap.c_allocator.destroy(result);
}

/// Float64 greater than comparison
export fn galleon_series_gt_f64(arr: *const ManagedArrowArray, value: f64) ?*SeriesFilterResult {
    const mask = arr.gtF64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Float64 greater than or equal comparison
export fn galleon_series_ge_f64(arr: *const ManagedArrowArray, value: f64) ?*SeriesFilterResult {
    const mask = arr.geF64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Float64 less than comparison
export fn galleon_series_lt_f64(arr: *const ManagedArrowArray, value: f64) ?*SeriesFilterResult {
    const mask = arr.ltF64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Float64 less than or equal comparison
export fn galleon_series_le_f64(arr: *const ManagedArrowArray, value: f64) ?*SeriesFilterResult {
    const mask = arr.leF64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Float64 equal comparison
export fn galleon_series_eq_f64(arr: *const ManagedArrowArray, value: f64) ?*SeriesFilterResult {
    const mask = arr.eqF64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Float64 not equal comparison
export fn galleon_series_ne_f64(arr: *const ManagedArrowArray, value: f64) ?*SeriesFilterResult {
    const mask = arr.neF64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Int64 greater than comparison
export fn galleon_series_gt_i64(arr: *const ManagedArrowArray, value: i64) ?*SeriesFilterResult {
    const mask = arr.gtI64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Int64 greater than or equal comparison
export fn galleon_series_ge_i64(arr: *const ManagedArrowArray, value: i64) ?*SeriesFilterResult {
    const mask = arr.geI64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Int64 less than comparison
export fn galleon_series_lt_i64(arr: *const ManagedArrowArray, value: i64) ?*SeriesFilterResult {
    const mask = arr.ltI64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Int64 less than or equal comparison
export fn galleon_series_le_i64(arr: *const ManagedArrowArray, value: i64) ?*SeriesFilterResult {
    const mask = arr.leI64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Int64 equal comparison
export fn galleon_series_eq_i64(arr: *const ManagedArrowArray, value: i64) ?*SeriesFilterResult {
    const mask = arr.eqI64(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Int64 not equal comparison
export fn galleon_series_ne_i64(arr: *const ManagedArrowArray, value: i64) ?*SeriesFilterResult {
    const mask = arr.neI64(value) catch return null;
    return createSeriesFilterResult(mask);
}

// Float32 Comparisons
export fn galleon_series_gt_f32(arr: *const ManagedArrowArray, value: f32) ?*SeriesFilterResult {
    const mask = arr.gtF32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ge_f32(arr: *const ManagedArrowArray, value: f32) ?*SeriesFilterResult {
    const mask = arr.geF32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_lt_f32(arr: *const ManagedArrowArray, value: f32) ?*SeriesFilterResult {
    const mask = arr.ltF32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_le_f32(arr: *const ManagedArrowArray, value: f32) ?*SeriesFilterResult {
    const mask = arr.leF32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_eq_f32(arr: *const ManagedArrowArray, value: f32) ?*SeriesFilterResult {
    const mask = arr.eqF32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ne_f32(arr: *const ManagedArrowArray, value: f32) ?*SeriesFilterResult {
    const mask = arr.neF32(value) catch return null;
    return createSeriesFilterResult(mask);
}

// Int32 Comparisons
export fn galleon_series_gt_i32(arr: *const ManagedArrowArray, value: i32) ?*SeriesFilterResult {
    const mask = arr.gtI32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ge_i32(arr: *const ManagedArrowArray, value: i32) ?*SeriesFilterResult {
    const mask = arr.geI32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_lt_i32(arr: *const ManagedArrowArray, value: i32) ?*SeriesFilterResult {
    const mask = arr.ltI32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_le_i32(arr: *const ManagedArrowArray, value: i32) ?*SeriesFilterResult {
    const mask = arr.leI32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_eq_i32(arr: *const ManagedArrowArray, value: i32) ?*SeriesFilterResult {
    const mask = arr.eqI32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ne_i32(arr: *const ManagedArrowArray, value: i32) ?*SeriesFilterResult {
    const mask = arr.neI32(value) catch return null;
    return createSeriesFilterResult(mask);
}

// UInt64 Comparisons
export fn galleon_series_gt_u64(arr: *const ManagedArrowArray, value: u64) ?*SeriesFilterResult {
    const mask = arr.gtU64(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ge_u64(arr: *const ManagedArrowArray, value: u64) ?*SeriesFilterResult {
    const mask = arr.geU64(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_lt_u64(arr: *const ManagedArrowArray, value: u64) ?*SeriesFilterResult {
    const mask = arr.ltU64(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_le_u64(arr: *const ManagedArrowArray, value: u64) ?*SeriesFilterResult {
    const mask = arr.leU64(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_eq_u64(arr: *const ManagedArrowArray, value: u64) ?*SeriesFilterResult {
    const mask = arr.eqU64(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ne_u64(arr: *const ManagedArrowArray, value: u64) ?*SeriesFilterResult {
    const mask = arr.neU64(value) catch return null;
    return createSeriesFilterResult(mask);
}

// UInt32 Comparisons
export fn galleon_series_gt_u32(arr: *const ManagedArrowArray, value: u32) ?*SeriesFilterResult {
    const mask = arr.gtU32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ge_u32(arr: *const ManagedArrowArray, value: u32) ?*SeriesFilterResult {
    const mask = arr.geU32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_lt_u32(arr: *const ManagedArrowArray, value: u32) ?*SeriesFilterResult {
    const mask = arr.ltU32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_le_u32(arr: *const ManagedArrowArray, value: u32) ?*SeriesFilterResult {
    const mask = arr.leU32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_eq_u32(arr: *const ManagedArrowArray, value: u32) ?*SeriesFilterResult {
    const mask = arr.eqU32(value) catch return null;
    return createSeriesFilterResult(mask);
}

export fn galleon_series_ne_u32(arr: *const ManagedArrowArray, value: u32) ?*SeriesFilterResult {
    const mask = arr.neU32(value) catch return null;
    return createSeriesFilterResult(mask);
}

/// Filter Float64 array by boolean mask
export fn galleon_series_filter_f64(arr: *const ManagedArrowArray, mask: [*]const bool, mask_len: usize) ?*ManagedArrowArray {
    return arr.filterF64(mask[0..mask_len]) catch null;
}

/// Filter Int64 array by boolean mask
export fn galleon_series_filter_i64(arr: *const ManagedArrowArray, mask: [*]const bool, mask_len: usize) ?*ManagedArrowArray {
    return arr.filterI64(mask[0..mask_len]) catch null;
}

// ============================================================================
// Arrow Arithmetic Operations
// ============================================================================

/// Add two Float64 arrays element-wise
export fn galleon_series_add_f64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.addF64(arr2) catch null;
}

/// Subtract two Float64 arrays element-wise
export fn galleon_series_sub_f64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.subF64(arr2) catch null;
}

/// Multiply two Float64 arrays element-wise
export fn galleon_series_mul_f64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.mulF64(arr2) catch null;
}

/// Divide two Float64 arrays element-wise
export fn galleon_series_div_f64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.divF64(arr2) catch null;
}

/// Add scalar to Float64 array
export fn galleon_series_add_scalar_f64(arr: *const ManagedArrowArray, value: f64) ?*ManagedArrowArray {
    return arr.addScalarF64(value) catch null;
}

/// Subtract scalar from Float64 array
export fn galleon_series_sub_scalar_f64(arr: *const ManagedArrowArray, value: f64) ?*ManagedArrowArray {
    return arr.subScalarF64(value) catch null;
}

/// Multiply Float64 array by scalar
export fn galleon_series_mul_scalar_f64(arr: *const ManagedArrowArray, value: f64) ?*ManagedArrowArray {
    return arr.mulScalarF64(value) catch null;
}

/// Divide Float64 array by scalar
export fn galleon_series_div_scalar_f64(arr: *const ManagedArrowArray, value: f64) ?*ManagedArrowArray {
    return arr.divScalarF64(value) catch null;
}

/// Add two Int64 arrays element-wise
export fn galleon_series_add_i64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.addI64(arr2) catch null;
}

/// Subtract two Int64 arrays element-wise
export fn galleon_series_sub_i64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.subI64(arr2) catch null;
}

/// Multiply two Int64 arrays element-wise
export fn galleon_series_mul_i64(arr1: *const ManagedArrowArray, arr2: *const ManagedArrowArray) ?*ManagedArrowArray {
    return arr1.mulI64(arr2) catch null;
}

/// Add scalar to Int64 array
export fn galleon_series_add_scalar_i64(arr: *const ManagedArrowArray, value: i64) ?*ManagedArrowArray {
    return arr.addScalarI64(value) catch null;
}

/// Multiply Int64 array by scalar
export fn galleon_series_mul_scalar_i64(arr: *const ManagedArrowArray, value: i64) ?*ManagedArrowArray {
    return arr.mulScalarI64(value) catch null;
}

// ============================================================================
// Full Join Operations (Join + Materialize in one call)
// ============================================================================

const FullJoinResult = arrow.FullJoinResult;

/// Perform complete inner join: join + materialize all columns in one call
/// This minimizes CGO overhead by doing everything in Zig
export fn galleon_inner_join_full(
    left_key: *const ManagedArrowArray,
    right_key: *const ManagedArrowArray,
    left_columns: [*]const *const ManagedArrowArray,
    left_col_count: usize,
    right_columns: [*]const *const ManagedArrowArray,
    right_col_count: usize,
) ?*FullJoinResult {
    return arrow.arrowInnerJoinFull(
        std.heap.c_allocator,
        left_key,
        right_key,
        left_columns,
        left_col_count,
        right_columns,
        right_col_count,
    ) catch null;
}

/// Perform complete left join: join + materialize all columns in one call
export fn galleon_left_join_full(
    left_key: *const ManagedArrowArray,
    right_key: *const ManagedArrowArray,
    left_columns: [*]const *const ManagedArrowArray,
    left_col_count: usize,
    right_columns: [*]const *const ManagedArrowArray,
    right_col_count: usize,
) ?*FullJoinResult {
    return arrow.arrowLeftJoinFull(
        std.heap.c_allocator,
        left_key,
        right_key,
        left_columns,
        left_col_count,
        right_columns,
        right_col_count,
    ) catch null;
}

/// Get the number of result columns
export fn galleon_full_join_result_num_columns(result: *const FullJoinResult) usize {
    return result.num_columns;
}

/// Get the number of result rows
export fn galleon_full_join_result_num_rows(result: *const FullJoinResult) usize {
    return result.num_rows;
}

/// Get a result column by index (returns ManagedArrowArray pointer)
/// Note: This just returns the pointer, ownership is NOT transferred
export fn galleon_full_join_result_column(result: *const FullJoinResult, index: usize) ?*ManagedArrowArray {
    if (index >= result.num_columns) return null;
    return result.result_columns[index];
}

/// Take ownership of a result column by index (transfers ownership to caller)
/// The column is set to null in the result so destroy won't free it
export fn galleon_full_join_result_take_column(result: *FullJoinResult, index: usize) ?*ManagedArrowArray {
    if (index >= result.num_columns) return null;
    const col = result.result_columns[index];
    result.result_columns[index] = undefined; // Clear the pointer
    return col;
}

/// Free full join result structure only (use after taking all columns)
export fn galleon_full_join_result_destroy_struct(result: *FullJoinResult) void {
    result.allocator.free(result.result_columns);
    result.allocator.destroy(result);
}

/// Free full join result (also frees all result columns that haven't been taken)
export fn galleon_full_join_result_destroy(result: *FullJoinResult) void {
    result.deinit();
}

// ============================================================================
// Full DataFrame Sort (Sort + Gather all columns in one call)
// ============================================================================

const SortResult = arrow.SortResult;

/// Complete DataFrame sort: argsort + gather all columns in one call
/// This minimizes CGO overhead by doing everything in Zig
export fn galleon_sort_dataframe_full(
    sort_column: *const ManagedArrowArray,
    columns: [*]const *const ManagedArrowArray,
    col_count: usize,
    ascending: bool,
) ?*SortResult {
    return arrow.arrowSortDataFrameFull(
        std.heap.c_allocator,
        sort_column,
        columns,
        col_count,
        ascending,
    ) catch null;
}

/// Get number of columns in sort result
export fn galleon_sort_result_num_columns(result: *const SortResult) usize {
    return result.num_columns;
}

/// Get number of rows in sort result
export fn galleon_sort_result_num_rows(result: *const SortResult) usize {
    return result.num_rows;
}

/// Take ownership of a column from sort result (transfers ownership to caller)
/// The column at this index becomes null in the result
export fn galleon_sort_result_take_column(result: *SortResult, index: usize) ?*ManagedArrowArray {
    if (index >= result.num_columns) return null;
    const col = result.result_columns[index];
    result.result_columns[index] = undefined;
    return col;
}

/// Free sort result struct only (use after taking all columns)
export fn galleon_sort_result_destroy_struct(result: *SortResult) void {
    result.allocator.free(result.result_columns);
    result.allocator.destroy(result);
}

/// Free sort result (also frees all result columns that haven't been taken)
export fn galleon_sort_result_destroy(result: *SortResult) void {
    result.deinit();
}

// ============================================================================
// Arrow GroupBy Operations
// ============================================================================

const GroupBySumResult = arrow.GroupBySumResult;
const GroupByMultiAggResult = arrow.GroupByMultiAggResult;

/// GroupBy Sum: groups by Int64 key, sums Float64 values
export fn galleon_series_groupby_sum_i64_f64(
    keys: *const ManagedArrowArray,
    values: *const ManagedArrowArray,
) ?*GroupBySumResult {
    return arrow.arrowGroupBySumI64KeyF64Value(std.heap.c_allocator, keys, values) catch null;
}

/// GroupBy Count: groups by Int64 key, counts occurrences
export fn galleon_series_groupby_count_i64(
    keys: *const ManagedArrowArray,
) ?*GroupBySumResult {
    return arrow.arrowGroupByCountI64Key(std.heap.c_allocator, keys) catch null;
}

/// GroupBy Mean: groups by Int64 key, computes mean of Float64 values
export fn galleon_series_groupby_mean_i64_f64(
    keys: *const ManagedArrowArray,
    values: *const ManagedArrowArray,
) ?*GroupBySumResult {
    return arrow.arrowGroupByMeanI64KeyF64Value(std.heap.c_allocator, keys, values) catch null;
}

/// GroupBy Multi-Agg: groups by Int64 key, computes sum/min/max/count of Float64 values
export fn galleon_series_groupby_multi_agg_i64_f64(
    keys: *const ManagedArrowArray,
    values: *const ManagedArrowArray,
) ?*GroupByMultiAggResult {
    return arrow.arrowGroupByMultiAggI64KeyF64Value(std.heap.c_allocator, keys, values) catch null;
}

/// Get number of groups in groupby sum result
export fn galleon_series_groupby_sum_result_num_groups(result: *const GroupBySumResult) u32 {
    return result.num_groups;
}

/// Get keys array from groupby sum result
export fn galleon_series_groupby_sum_result_keys(result: *const GroupBySumResult) *const ManagedArrowArray {
    return result.keys;
}

/// Get values array from groupby sum result (sums, means, or counts depending on operation)
export fn galleon_series_groupby_sum_result_values(result: *const GroupBySumResult) *const ManagedArrowArray {
    return result.sums;
}

/// Free groupby sum result
export fn galleon_series_groupby_sum_result_destroy(result: *GroupBySumResult) void {
    result.deinit();
}

/// Get number of groups in groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_num_groups(result: *const GroupByMultiAggResult) u32 {
    return result.num_groups;
}

/// Get keys array from groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_keys(result: *const GroupByMultiAggResult) *const ManagedArrowArray {
    return result.keys;
}

/// Get sums array from groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_sums(result: *const GroupByMultiAggResult) *const ManagedArrowArray {
    return result.sums;
}

/// Get mins array from groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_mins(result: *const GroupByMultiAggResult) *const ManagedArrowArray {
    return result.mins;
}

/// Get maxs array from groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_maxs(result: *const GroupByMultiAggResult) *const ManagedArrowArray {
    return result.maxs;
}

/// Get counts array from groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_counts(result: *const GroupByMultiAggResult) *const ManagedArrowArray {
    return result.counts;
}

/// Free groupby multi-agg result
export fn galleon_series_groupby_multi_agg_result_destroy(result: *GroupByMultiAggResult) void {
    result.deinit();
}

// ============================================================================
// Tests
// ============================================================================

test "sum" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    const result = simd.sum(f64, &data);
    try std.testing.expectApproxEqAbs(@as(f64, 36.0), result, 0.0001);
}

test "mean" {
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    const result = simd.mean(f64, &data);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), result.?, 0.0001);
}

test "column creation and access" {
    const allocator = std.testing.allocator;
    const col = try column.ColumnF64.create(allocator, 100);
    defer col.deinit();

    try std.testing.expectEqual(@as(usize, 100), col.len());
}
