#ifndef GALLEON_H
#define GALLEON_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Opaque Column Types
// ============================================================================

typedef struct ColumnF64 ColumnF64;
typedef struct ColumnF32 ColumnF32;
typedef struct ColumnI64 ColumnI64;
typedef struct ColumnI32 ColumnI32;
typedef struct ColumnBool ColumnBool;

// ============================================================================
// Float64 Column Operations
// ============================================================================

ColumnF64* galleon_column_f64_create(const double* data, size_t len);
void galleon_column_f64_destroy(ColumnF64* col);
size_t galleon_column_f64_len(const ColumnF64* col);
double galleon_column_f64_get(const ColumnF64* col, size_t index);
const double* galleon_column_f64_data(const ColumnF64* col);

// Float64 Aggregations (auto-parallelized for large data via Blitz)
double galleon_sum_f64(const double* data, size_t len);
double galleon_min_f64(const double* data, size_t len);
double galleon_max_f64(const double* data, size_t len);
double galleon_mean_f64(const double* data, size_t len);

// ============================================================================
// SIMD Configuration (Runtime CPU Feature Detection)
// ============================================================================

// SIMD levels:
// 0 = Scalar (no SIMD or fallback)
// 1 = SSE4 (128-bit vectors) / ARM NEON
// 2 = AVX2 (256-bit vectors)
// 3 = AVX-512 (512-bit vectors)

// Get the detected SIMD level being used
uint8_t galleon_get_simd_level(void);

// Override the SIMD level (for testing or compatibility)
void galleon_set_simd_level(uint8_t level);

// Get the SIMD level name as a string ("Scalar", "SSE4", "AVX2", "AVX-512")
const char* galleon_get_simd_level_name(void);

// Get the vector width in bytes for the current SIMD level
size_t galleon_get_simd_vector_bytes(void);

// ============================================================================
// Thread Configuration
// ============================================================================

// Set the maximum number of threads to use (0 = auto-detect from CPU count)
void galleon_set_max_threads(size_t max_threads);

// Get the current effective max threads
size_t galleon_get_max_threads(void);

// Check if thread count was auto-detected
bool galleon_is_threads_auto_detected(void);

// ============================================================================
// Blitz Work-Stealing Thread Pool (Diagnostic Functions)
// ============================================================================
// Note: Blitz auto-initializes on first use. These functions are for diagnostics
// and explicit lifecycle management. The regular galleon_* functions automatically
// use Blitz for large data (>100K elements).

// Initialize the Blitz work-stealing thread pool (optional - auto-initializes)
// Returns true on success, false on failure
bool blitz_init(void);

// Shutdown the Blitz thread pool and free resources
void blitz_deinit(void);

// Check if Blitz pool is initialized
bool blitz_is_initialized(void);

// Get the number of worker threads
uint32_t blitz_num_workers(void);

// ============================================================================
// Float64 Vectorized Operations
// ============================================================================

// Float64 Vectorized Operations
void galleon_add_scalar_f64(double* data, size_t len, double scalar);
void galleon_mul_scalar_f64(double* data, size_t len, double scalar);
void galleon_add_arrays_f64(double* dst, const double* src, size_t len);

// Float64 Filter Operations
void galleon_filter_gt_f64(const double* data, size_t len, double threshold,
                           uint32_t* out_indices, size_t* out_count);
void galleon_filter_mask_gt_f64(const double* data, size_t len, double threshold,
                                bool* out_mask);
void galleon_filter_mask_u8_gt_f64(const double* data, size_t len, double threshold,
                                   uint8_t* out_mask);

// Float64 Sort Operations
void galleon_argsort_f64(const double* data, size_t len, uint32_t* out_indices, bool ascending);

// ============================================================================
// Float32 Column Operations
// ============================================================================

ColumnF32* galleon_column_f32_create(const float* data, size_t len);
void galleon_column_f32_destroy(ColumnF32* col);
size_t galleon_column_f32_len(const ColumnF32* col);
float galleon_column_f32_get(const ColumnF32* col, size_t index);
const float* galleon_column_f32_data(const ColumnF32* col);

// Float32 Aggregations
float galleon_sum_f32(const float* data, size_t len);
float galleon_min_f32(const float* data, size_t len);
float galleon_max_f32(const float* data, size_t len);
float galleon_mean_f32(const float* data, size_t len);

// Float32 Vectorized Operations
void galleon_add_scalar_f32(float* data, size_t len, float scalar);
void galleon_mul_scalar_f32(float* data, size_t len, float scalar);

// Float32 Filter Operations
void galleon_filter_gt_f32(const float* data, size_t len, float threshold,
                           uint32_t* out_indices, size_t* out_count);
void galleon_filter_mask_u8_gt_f32(const float* data, size_t len, float threshold,
                                   uint8_t* out_mask);

// Float32 Sort Operations
void galleon_argsort_f32(const float* data, size_t len, uint32_t* out_indices, bool ascending);

// ============================================================================
// Int64 Column Operations
// ============================================================================

ColumnI64* galleon_column_i64_create(const int64_t* data, size_t len);
void galleon_column_i64_destroy(ColumnI64* col);
size_t galleon_column_i64_len(const ColumnI64* col);
int64_t galleon_column_i64_get(const ColumnI64* col, size_t index);
const int64_t* galleon_column_i64_data(const ColumnI64* col);

// Int64 Aggregations
int64_t galleon_sum_i64(const int64_t* data, size_t len);
int64_t galleon_min_i64(const int64_t* data, size_t len);
int64_t galleon_max_i64(const int64_t* data, size_t len);

// Int64 Vectorized Operations
void galleon_add_scalar_i64(int64_t* data, size_t len, int64_t scalar);
void galleon_mul_scalar_i64(int64_t* data, size_t len, int64_t scalar);

// Int64 Filter Operations
void galleon_filter_gt_i64(const int64_t* data, size_t len, int64_t threshold,
                           uint32_t* out_indices, size_t* out_count);
void galleon_filter_mask_u8_gt_i64(const int64_t* data, size_t len, int64_t threshold,
                                   uint8_t* out_mask);

// Int64 Sort Operations
void galleon_argsort_i64(const int64_t* data, size_t len, uint32_t* out_indices, bool ascending);

// ============================================================================
// Int32 Column Operations
// ============================================================================

ColumnI32* galleon_column_i32_create(const int32_t* data, size_t len);
void galleon_column_i32_destroy(ColumnI32* col);
size_t galleon_column_i32_len(const ColumnI32* col);
int32_t galleon_column_i32_get(const ColumnI32* col, size_t index);
const int32_t* galleon_column_i32_data(const ColumnI32* col);

// Int32 Aggregations
int32_t galleon_sum_i32(const int32_t* data, size_t len);
int32_t galleon_min_i32(const int32_t* data, size_t len);
int32_t galleon_max_i32(const int32_t* data, size_t len);

// Int32 Vectorized Operations
void galleon_add_scalar_i32(int32_t* data, size_t len, int32_t scalar);
void galleon_mul_scalar_i32(int32_t* data, size_t len, int32_t scalar);

// Int32 Filter Operations
void galleon_filter_gt_i32(const int32_t* data, size_t len, int32_t threshold,
                           uint32_t* out_indices, size_t* out_count);
void galleon_filter_mask_u8_gt_i32(const int32_t* data, size_t len, int32_t threshold,
                                   uint8_t* out_mask);

// Int32 Sort Operations
void galleon_argsort_i32(const int32_t* data, size_t len, uint32_t* out_indices, bool ascending);

// ============================================================================
// Bool Column Operations
// ============================================================================

ColumnBool* galleon_column_bool_create(const bool* data, size_t len);
void galleon_column_bool_destroy(ColumnBool* col);
size_t galleon_column_bool_len(const ColumnBool* col);
bool galleon_column_bool_get(const ColumnBool* col, size_t index);
const bool* galleon_column_bool_data(const ColumnBool* col);

// Bool Operations
size_t galleon_count_true(const bool* data, size_t len);
size_t galleon_count_false(const bool* data, size_t len);

// ============================================================================
// GroupBy Aggregation Functions
// ============================================================================

void galleon_aggregate_sum_f64_by_group(const double* data, const uint32_t* group_ids,
                                         double* out_sums, size_t data_len, size_t num_groups);
void galleon_aggregate_sum_i64_by_group(const int64_t* data, const uint32_t* group_ids,
                                         int64_t* out_sums, size_t data_len, size_t num_groups);

void galleon_aggregate_min_f64_by_group(const double* data, const uint32_t* group_ids,
                                         double* out_mins, size_t data_len, size_t num_groups);
void galleon_aggregate_min_i64_by_group(const int64_t* data, const uint32_t* group_ids,
                                         int64_t* out_mins, size_t data_len, size_t num_groups);

void galleon_aggregate_max_f64_by_group(const double* data, const uint32_t* group_ids,
                                         double* out_maxs, size_t data_len, size_t num_groups);
void galleon_aggregate_max_i64_by_group(const int64_t* data, const uint32_t* group_ids,
                                         int64_t* out_maxs, size_t data_len, size_t num_groups);

void galleon_count_by_group(const uint32_t* group_ids, uint64_t* out_counts,
                            size_t data_len, size_t num_groups);

void galleon_hash_i64_column(const int64_t* data, uint64_t* out_hashes, size_t len);
void galleon_hash_i32_column(const int32_t* data, uint64_t* out_hashes, size_t len);
void galleon_hash_f64_column(const double* data, uint64_t* out_hashes, size_t len);
void galleon_hash_f32_column(const float* data, uint64_t* out_hashes, size_t len);
void galleon_combine_hashes(const uint64_t* hash1, const uint64_t* hash2, uint64_t* out_hashes, size_t len);

// ============================================================================
// Join Helper Functions
// ============================================================================

void galleon_gather_f64(const double* src, size_t src_len, const int32_t* indices,
                        double* dst, size_t dst_len);
void galleon_gather_i64(const int64_t* src, size_t src_len, const int32_t* indices,
                        int64_t* dst, size_t dst_len);
void galleon_gather_i32(const int32_t* src, size_t src_len, const int32_t* indices,
                        int32_t* dst, size_t dst_len);
void galleon_gather_f32(const float* src, size_t src_len, const int32_t* indices,
                        float* dst, size_t dst_len);

void galleon_build_join_hash_table(const uint64_t* hashes, size_t hashes_len,
                                   int32_t* table, int32_t* next, uint32_t table_size);

uint32_t galleon_probe_join_hash_table(const uint64_t* probe_hashes, const int64_t* probe_keys,
                                       size_t probe_len, const int64_t* build_keys, size_t build_len,
                                       const int32_t* table, const int32_t* next, uint32_t table_size,
                                       int32_t* out_probe_indices, int32_t* out_build_indices,
                                       uint32_t max_matches);

// ============================================================================
// End-to-End Inner Join
// ============================================================================

typedef struct InnerJoinResultHandle InnerJoinResultHandle;

InnerJoinResultHandle* galleon_inner_join_e2e_i64(const int64_t* left_keys, size_t left_len,
                                                   const int64_t* right_keys, size_t right_len);
InnerJoinResultHandle* galleon_parallel_inner_join_i64(const int64_t* left_keys, size_t left_len,
                                                        const int64_t* right_keys, size_t right_len);
uint32_t galleon_inner_join_result_num_matches(const InnerJoinResultHandle* handle);
const int32_t* galleon_inner_join_result_left_indices(const InnerJoinResultHandle* handle);
const int32_t* galleon_inner_join_result_right_indices(const InnerJoinResultHandle* handle);
void galleon_inner_join_result_destroy(InnerJoinResultHandle* handle);

// ============================================================================
// End-to-End Left Join
// ============================================================================

typedef struct LeftJoinResultHandle LeftJoinResultHandle;

LeftJoinResultHandle* galleon_left_join_i64(const int64_t* left_keys, size_t left_len,
                                             const int64_t* right_keys, size_t right_len);
LeftJoinResultHandle* galleon_parallel_left_join_i64(const int64_t* left_keys, size_t left_len,
                                                      const int64_t* right_keys, size_t right_len);
uint32_t galleon_left_join_result_num_rows(const LeftJoinResultHandle* handle);
const int32_t* galleon_left_join_result_left_indices(const LeftJoinResultHandle* handle);
const int32_t* galleon_left_join_result_right_indices(const LeftJoinResultHandle* handle);
void galleon_left_join_result_destroy(LeftJoinResultHandle* handle);

// ============================================================================
// GroupBy Operations
// ============================================================================

typedef struct GroupByResultHandle GroupByResultHandle;
typedef struct GroupByResultExtHandle GroupByResultExtHandle;
typedef struct GroupBySumResultHandle GroupBySumResultHandle;
typedef struct GroupByMultiAggResultHandle GroupByMultiAggResultHandle;

GroupByResultHandle* galleon_groupby_compute(const uint64_t* hashes, size_t len);
uint32_t galleon_groupby_result_num_groups(const GroupByResultHandle* handle);
const uint32_t* galleon_groupby_result_group_ids(const GroupByResultHandle* handle);
void galleon_groupby_result_destroy(GroupByResultHandle* handle);

GroupByResultHandle* galleon_groupby_compute_with_keys_i64(const uint64_t* hashes, const int64_t* keys, size_t len);

GroupByResultExtHandle* galleon_groupby_compute_ext(const uint64_t* hashes, size_t len);
uint32_t galleon_groupby_result_ext_num_groups(const GroupByResultExtHandle* handle);
const uint32_t* galleon_groupby_result_ext_group_ids(const GroupByResultExtHandle* handle);
const uint32_t* galleon_groupby_result_ext_first_row_idx(const GroupByResultExtHandle* handle);
const uint32_t* galleon_groupby_result_ext_group_counts(const GroupByResultExtHandle* handle);
void galleon_groupby_result_ext_destroy(GroupByResultExtHandle* handle);

GroupBySumResultHandle* galleon_groupby_sum_e2e_i64_f64(const int64_t* keys, const double* values, size_t len);
uint32_t galleon_groupby_sum_result_num_groups(const GroupBySumResultHandle* handle);
const int64_t* galleon_groupby_sum_result_keys(const GroupBySumResultHandle* handle);
const double* galleon_groupby_sum_result_sums(const GroupBySumResultHandle* handle);
void galleon_groupby_sum_result_destroy(GroupBySumResultHandle* handle);

GroupByMultiAggResultHandle* galleon_groupby_multi_agg_e2e_i64_f64(const int64_t* keys, const double* values, size_t len);
uint32_t galleon_groupby_multi_agg_result_num_groups(const GroupByMultiAggResultHandle* handle);
const int64_t* galleon_groupby_multi_agg_result_keys(const GroupByMultiAggResultHandle* handle);
const double* galleon_groupby_multi_agg_result_sums(const GroupByMultiAggResultHandle* handle);
const double* galleon_groupby_multi_agg_result_mins(const GroupByMultiAggResultHandle* handle);
const double* galleon_groupby_multi_agg_result_maxs(const GroupByMultiAggResultHandle* handle);
const uint64_t* galleon_groupby_multi_agg_result_counts(const GroupByMultiAggResultHandle* handle);
void galleon_groupby_multi_agg_result_destroy(GroupByMultiAggResultHandle* handle);

// GroupBy aggregation by group
void galleon_groupby_sum_f64(const double* data, const uint32_t* group_ids, size_t len, double* out, size_t num_groups);
void galleon_groupby_sum_i64(const int64_t* data, const uint32_t* group_ids, size_t len, int64_t* out, size_t num_groups);
void galleon_groupby_min_f64(const double* data, const uint32_t* group_ids, size_t len, double* out, size_t num_groups);
void galleon_groupby_min_i64(const int64_t* data, const uint32_t* group_ids, size_t len, int64_t* out, size_t num_groups);
void galleon_groupby_max_f64(const double* data, const uint32_t* group_ids, size_t len, double* out, size_t num_groups);
void galleon_groupby_max_i64(const int64_t* data, const uint32_t* group_ids, size_t len, int64_t* out, size_t num_groups);
void galleon_groupby_mean_f64(const double* data, const uint32_t* group_ids, size_t len, double* out, const uint64_t* counts, size_t num_groups);
void galleon_groupby_count(const uint32_t* group_ids, size_t len, uint64_t* out, size_t num_groups);

// ============================================================================
// Array Operations
// ============================================================================

void galleon_add_f64(const double* a, const double* b, double* out, size_t len);
void galleon_sub_f64(const double* a, const double* b, double* out, size_t len);
void galleon_mul_f64(const double* a, const double* b, double* out, size_t len);
void galleon_div_f64(const double* a, const double* b, double* out, size_t len);
void galleon_add_i64(const int64_t* a, const int64_t* b, int64_t* out, size_t len);
void galleon_sub_i64(const int64_t* a, const int64_t* b, int64_t* out, size_t len);
void galleon_mul_i64(const int64_t* a, const int64_t* b, int64_t* out, size_t len);
void galleon_cmp_gt_f64(const double* a, const double* b, uint8_t* out, size_t len);
void galleon_cmp_ge_f64(const double* a, const double* b, uint8_t* out, size_t len);
void galleon_cmp_lt_f64(const double* a, const double* b, uint8_t* out, size_t len);
void galleon_cmp_le_f64(const double* a, const double* b, uint8_t* out, size_t len);
void galleon_cmp_eq_f64(const double* a, const double* b, uint8_t* out, size_t len);
void galleon_cmp_ne_f64(const double* a, const double* b, uint8_t* out, size_t len);

// Mask operations
size_t galleon_count_mask_true(const uint8_t* mask, size_t len);
size_t galleon_indices_from_mask(const uint8_t* mask, size_t len, uint32_t* out_indices, size_t max_indices);

// ============================================================================
// Conditional Operations
// ============================================================================

// Select (when/then/otherwise)
void galleon_select_f64(const uint8_t* mask, const double* then_val,
                        const double* else_val, double* out, size_t len);
void galleon_select_i64(const uint8_t* mask, const int64_t* then_val,
                        const int64_t* else_val, int64_t* out, size_t len);
void galleon_select_scalar_f64(const uint8_t* mask, const double* then_val,
                               double else_scalar, double* out, size_t len);

// Null detection (NaN for floats)
void galleon_is_null_f64(const double* data, uint8_t* out, size_t len);
void galleon_is_not_null_f64(const double* data, uint8_t* out, size_t len);

// Fill null
void galleon_fill_null_f64(const double* data, double fill_value, double* out, size_t len);
void galleon_fill_null_forward_f64(const double* data, double* out, size_t len);
void galleon_fill_null_backward_f64(const double* data, double* out, size_t len);

// Coalesce
void galleon_coalesce2_f64(const double* a, const double* b, double* out, size_t len);

// Null counting
size_t galleon_count_null_f64(const double* data, size_t len);
size_t galleon_count_not_null_f64(const double* data, size_t len);

// ============================================================================
// Advanced Statistics Operations
// ============================================================================

// Median (returns result through pointer, out_valid indicates success)
double galleon_median_f64(const double* data, size_t len, bool* out_valid);

// Quantile (q should be in [0, 1])
double galleon_quantile_f64(const double* data, size_t len, double q, bool* out_valid);

// Skewness (3rd standardized moment)
double galleon_skewness_f64(const double* data, size_t len, bool* out_valid);

// Kurtosis (excess kurtosis, 4th standardized moment - 3)
double galleon_kurtosis_f64(const double* data, size_t len, bool* out_valid);

// Pearson correlation coefficient between two arrays
double galleon_correlation_f64(const double* x, const double* y, size_t len, bool* out_valid);

// Variance (sample variance with n-1 denominator)
double galleon_variance_f64(const double* data, size_t len, bool* out_valid);

// Standard deviation (square root of variance)
double galleon_stddev_f64(const double* data, size_t len, bool* out_valid);

// ============================================================================
// Window Operations
// ============================================================================

// Lag/Lead shift operations
void galleon_lag_f64(const double* data, size_t len, size_t offset, double default_val, double* out);
void galleon_lead_f64(const double* data, size_t len, size_t offset, double default_val, double* out);
void galleon_lag_i64(const int64_t* data, size_t len, size_t offset, int64_t default_val, int64_t* out);
void galleon_lead_i64(const int64_t* data, size_t len, size_t offset, int64_t default_val, int64_t* out);

// Ranking functions
void galleon_row_number(uint32_t* out, size_t len);
void galleon_row_number_partitioned(const uint32_t* partition_ids, uint32_t* out, size_t len);
void galleon_rank_f64(const double* data, uint32_t* out, size_t len);
void galleon_dense_rank_f64(const double* data, uint32_t* out, size_t len);

// Cumulative functions
void galleon_cumsum_f64(const double* data, double* out, size_t len);
void galleon_cumsum_i64(const int64_t* data, int64_t* out, size_t len);
void galleon_cumsum_partitioned_f64(const double* data, const uint32_t* partition_ids, double* out, size_t len);
void galleon_cummin_f64(const double* data, double* out, size_t len);
void galleon_cummax_f64(const double* data, double* out, size_t len);

// Rolling aggregations
void galleon_rolling_sum_f64(const double* data, size_t len, size_t window_size, size_t min_periods, double* out);
void galleon_rolling_mean_f64(const double* data, size_t len, size_t window_size, size_t min_periods, double* out);
void galleon_rolling_min_f64(const double* data, size_t len, size_t window_size, size_t min_periods, double* out);
void galleon_rolling_max_f64(const double* data, size_t len, size_t window_size, size_t min_periods, double* out);
void galleon_rolling_std_f64(const double* data, size_t len, size_t window_size, size_t min_periods, double* out);

// Diff and percent change
void galleon_diff_f64(const double* data, double* out, size_t len, double default_val);
void galleon_diff_n_f64(const double* data, double* out, size_t len, size_t n, double default_val);
void galleon_pct_change_f64(const double* data, double* out, size_t len);

// ============================================================================
// Fold/Horizontal Aggregation Operations
// ============================================================================

// Sum across columns (row-wise)
void galleon_sum_horizontal2_f64(const double* a, const double* b, double* out, size_t len);
void galleon_sum_horizontal3_f64(const double* a, const double* b, const double* c, double* out, size_t len);

// Min across columns (row-wise)
void galleon_min_horizontal2_f64(const double* a, const double* b, double* out, size_t len);
void galleon_min_horizontal3_f64(const double* a, const double* b, const double* c, double* out, size_t len);

// Max across columns (row-wise)
void galleon_max_horizontal2_f64(const double* a, const double* b, double* out, size_t len);
void galleon_max_horizontal3_f64(const double* a, const double* b, const double* c, double* out, size_t len);

// Product across columns (row-wise)
void galleon_product_horizontal2_f64(const double* a, const double* b, double* out, size_t len);
void galleon_product_horizontal3_f64(const double* a, const double* b, const double* c, double* out, size_t len);

// Boolean horizontal operations
void galleon_any_horizontal2(const uint8_t* a, const uint8_t* b, uint8_t* out, size_t len);
void galleon_all_horizontal2(const uint8_t* a, const uint8_t* b, uint8_t* out, size_t len);

// Count non-null values across columns
void galleon_count_non_null_horizontal2_f64(const double* a, const double* b, uint32_t* out, size_t len);
void galleon_count_non_null_horizontal3_f64(const double* a, const double* b, const double* c, uint32_t* out, size_t len);

// ============================================================================
// ChunkedColumn V2 Operations (Cache-Friendly Chunk-Based Storage)
// ============================================================================
// ChunkedColumn stores data in L2-cache-sized chunks for optimal performance.
// Operations process chunk-by-chunk, always cache-warm.

typedef struct ChunkedColumnF64Handle ChunkedColumnF64Handle;
typedef struct ChunkedArgsortResult ChunkedArgsortResult;

// Creation and destruction
ChunkedColumnF64Handle* galleon_chunked_f64_create(const double* data, size_t len);
void galleon_chunked_f64_destroy(ChunkedColumnF64Handle* col);

// Basic accessors
size_t galleon_chunked_f64_len(const ChunkedColumnF64Handle* col);
size_t galleon_chunked_f64_num_chunks(const ChunkedColumnF64Handle* col);
double galleon_chunked_f64_get(const ChunkedColumnF64Handle* col, size_t index);
void galleon_chunked_f64_copy_to_slice(const ChunkedColumnF64Handle* col, double* out);

// Aggregations (parallel over chunks)
double galleon_chunked_f64_sum(const ChunkedColumnF64Handle* col);
double galleon_chunked_f64_min(const ChunkedColumnF64Handle* col);
double galleon_chunked_f64_max(const ChunkedColumnF64Handle* col);
double galleon_chunked_f64_mean(const ChunkedColumnF64Handle* col);

// Filtering (returns new chunked column)
ChunkedColumnF64Handle* galleon_chunked_f64_filter_gt(const ChunkedColumnF64Handle* col, double threshold);
ChunkedColumnF64Handle* galleon_chunked_f64_filter_lt(const ChunkedColumnF64Handle* col, double threshold);

// Sorting
ChunkedArgsortResult* galleon_chunked_f64_argsort(ChunkedColumnF64Handle* col);
size_t galleon_chunked_argsort_len(const ChunkedArgsortResult* result);
const uint32_t* galleon_chunked_argsort_indices(const ChunkedArgsortResult* result);
void galleon_chunked_argsort_destroy(ChunkedArgsortResult* result);

ChunkedColumnF64Handle* galleon_chunked_f64_sort(ChunkedColumnF64Handle* col);

#ifdef __cplusplus
}
#endif

#endif // GALLEON_H
