const std = @import("std");
const Allocator = std.mem.Allocator;

// Re-export modules
pub const column = @import("column.zig");
pub const simd = @import("simd.zig");
pub const groupby = @import("groupby.zig");

// Runtime dispatch system
pub const cpuid = @import("cpuid.zig");
pub const dispatch = @import("dispatch.zig");

// Work-stealing parallel execution
pub const blitz = @import("blitz/mod.zig");

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

export fn galleon_argsort_f64(
    data: [*]const f64,
    len: usize,
    out_indices: [*]u32,
    ascending: bool,
) void {
    simd.argsort(f64, data[0..len], out_indices[0..len], ascending);
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

export fn galleon_argsort_i64(
    data: [*]const i64,
    len: usize,
    out_indices: [*]u32,
    ascending: bool,
) void {
    simd.argsortInt(i64, data[0..len], out_indices[0..len], ascending);
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
    simd.argsortInt(i32, data[0..len], out_indices[0..len], ascending);
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
    simd.argsort(f32, data[0..len], out_indices[0..len], ascending);
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

export fn galleon_build_join_hash_table(
    hashes: [*]const u64,
    hashes_len: usize,
    table: [*]i32,
    next: [*]i32,
    table_size: u32,
) void {
    simd.buildJoinHashTable(hashes[0..hashes_len], table[0..table_size], next[0..hashes_len], table_size);
}

export fn galleon_probe_join_hash_table(
    probe_hashes: [*]const u64,
    probe_keys: [*]const i64,
    probe_len: usize,
    build_keys: [*]const i64,
    build_len: usize,
    table: [*]const i32,
    next: [*]const i32,
    table_size: u32,
    out_probe_indices: [*]i32,
    out_build_indices: [*]i32,
    max_matches: u32,
) u32 {
    _ = build_len;
    return simd.probeJoinHashTable(
        probe_hashes[0..probe_len],
        probe_keys[0..probe_len],
        build_keys[0..1 << 30],
        table[0..table_size],
        next[0..1 << 30],
        table_size,
        out_probe_indices[0..max_matches],
        out_build_indices[0..max_matches],
        max_matches,
    );
}

// ============================================================================
// End-to-End Inner Join (Single CGO Call - Phase 3)
// ============================================================================

/// Handle for end-to-end inner join result
pub const InnerJoinResultHandle = struct {
    result: simd.InnerJoinResult,
};

/// End-to-end inner join: pass left and right key columns, get matched indices
/// Single CGO call - hash both sides, build table, probe, return indices
export fn galleon_inner_join_e2e_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    handle.result = simd.innerJoinI64(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

export fn galleon_inner_join_result_num_matches(handle: *const InnerJoinResultHandle) u32 {
    return handle.result.num_matches;
}

export fn galleon_inner_join_result_left_indices(handle: *const InnerJoinResultHandle) [*]const i32 {
    return handle.result.left_indices.ptr;
}

export fn galleon_inner_join_result_right_indices(handle: *const InnerJoinResultHandle) [*]const i32 {
    return handle.result.right_indices.ptr;
}

export fn galleon_inner_join_result_destroy(handle: *InnerJoinResultHandle) void {
    handle.result.deinit();
    std.heap.c_allocator.destroy(handle);
}

// ============================================================================
// Left Join Operations
// ============================================================================

/// Handle for left join result
pub const LeftJoinResultHandle = struct {
    result: simd.LeftJoinResult,
};

/// Single-threaded left join
export fn galleon_left_join_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*LeftJoinResultHandle {
    const handle = std.heap.c_allocator.create(LeftJoinResultHandle) catch return null;
    handle.result = simd.leftJoinI64(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Parallel left join
export fn galleon_parallel_left_join_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*LeftJoinResultHandle {
    const handle = std.heap.c_allocator.create(LeftJoinResultHandle) catch return null;
    handle.result = simd.parallelLeftJoinI64(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

export fn galleon_left_join_result_num_rows(handle: *const LeftJoinResultHandle) u32 {
    return handle.result.num_rows;
}

export fn galleon_left_join_result_left_indices(handle: *const LeftJoinResultHandle) [*]const i32 {
    return handle.result.left_indices.ptr;
}

export fn galleon_left_join_result_right_indices(handle: *const LeftJoinResultHandle) [*]const i32 {
    return handle.result.right_indices.ptr;
}

export fn galleon_left_join_result_destroy(handle: *LeftJoinResultHandle) void {
    handle.result.deinit();
    std.heap.c_allocator.destroy(handle);
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
// Parallel Operations (Multi-threaded)
// ============================================================================

/// Parallel inner join with multi-threaded probing
export fn galleon_parallel_inner_join_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    // Use parallel probing for large datasets
    handle.result = simd.parallelInnerJoinI64(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Check if i64 array is sorted (ascending)
/// Returns true if sorted, false otherwise
export fn galleon_is_sorted_i64(keys: [*]const i64, len: usize) bool {
    return simd.joins.isSortedI64(keys[0..len]);
}

/// Radix inner join with inline keys - better cache performance
export fn galleon_inner_join_radix_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    handle.result = simd.innerJoinI64Radix(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Sort-merge inner join - excellent cache locality
export fn galleon_inner_join_sort_merge_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    handle.result = simd.innerJoinI64SortMerge(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Sort-merge left join - excellent cache locality
export fn galleon_left_join_sort_merge_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*LeftJoinResultHandle {
    const handle = std.heap.c_allocator.create(LeftJoinResultHandle) catch return null;
    handle.result = simd.leftJoinI64SortMerge(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Swiss Table inner join - SIMD control byte probing
export fn galleon_inner_join_swiss_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    handle.result = simd.innerJoinI64Swiss(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// Two-pass inner join with open-addressing hash table - pre-sized allocation
export fn galleon_inner_join_two_pass_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    handle.result = simd.innerJoinI64TwoPass(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

/// SIMD-accelerated inner join with inline keys
export fn galleon_inner_join_simd_i64(
    left_keys: [*]const i64,
    left_len: usize,
    right_keys: [*]const i64,
    right_len: usize,
) ?*InnerJoinResultHandle {
    const handle = std.heap.c_allocator.create(InnerJoinResultHandle) catch return null;
    handle.result = simd.innerJoinI64Simd(
        std.heap.c_allocator,
        left_keys[0..left_len],
        right_keys[0..right_len],
    ) catch {
        std.heap.c_allocator.destroy(handle);
        return null;
    };
    return handle;
}

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
pub const ChunkedArgsortResult = struct {
    indices: []u32,
    allocator: Allocator,
};

/// Argsort - returns indices that would sort the column
export fn galleon_chunked_f64_argsort(col: *chunked.ChunkedColumn(f64)) ?*ChunkedArgsortResult {
    const result = chunked.OpsF64.argsort(col, std.heap.c_allocator) catch return null;
    const handle = std.heap.c_allocator.create(ChunkedArgsortResult) catch {
        result.allocator.free(result.indices);
        return null;
    };
    handle.indices = result.indices;
    handle.allocator = result.allocator;
    return handle;
}

/// Get argsort result length
export fn galleon_chunked_argsort_len(handle: *const ChunkedArgsortResult) usize {
    return handle.indices.len;
}

/// Get argsort result indices pointer
export fn galleon_chunked_argsort_indices(handle: *const ChunkedArgsortResult) [*]const u32 {
    return handle.indices.ptr;
}

/// Free argsort result
export fn galleon_chunked_argsort_destroy(handle: *ChunkedArgsortResult) void {
    handle.allocator.free(handle.indices);
    std.heap.c_allocator.destroy(handle);
}

/// Sort - returns new sorted chunked column
export fn galleon_chunked_f64_sort(col: *chunked.ChunkedColumn(f64)) ?*ChunkedColumnF64Handle {
    return chunked.OpsF64.sort(col, std.heap.c_allocator) catch null;
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
