const std = @import("std");
const core = @import("core.zig");
const blitz = @import("../blitz/mod.zig");

// Import core constants
const MAX_THREADS = core.MAX_THREADS;
const getMaxThreads = core.getMaxThreads;

// ============================================================================
// Radix Sort Configuration
// ============================================================================

/// Radix sort bits per pass
pub const RADIX_BITS: u6 = 8;

/// Number of buckets per radix sort pass (256 buckets)
pub const RADIX_SIZE: usize = 1 << RADIX_BITS;

/// Mask for extracting radix digit
pub const RADIX_MASK: u64 = RADIX_SIZE - 1;

/// Threshold for using parallel sample sort
pub const PARALLEL_SORT_THRESHOLD: usize = 50000;

// ============================================================================
// Sort Pair Structures
// ============================================================================

/// Pair structure for cache-friendly sorting
/// Packs value and index together so comparisons don't cause cache misses
pub const SortPair = packed struct {
    key: u64, // sortable representation of value
    idx: u32, // original index
};

/// ValueIndex pair for cache-friendly sorting
pub const ValueIndexPair = struct {
    value: f64,
    idx: u32,
};

// ============================================================================
// Float to Sortable Conversion
// ============================================================================

/// Convert f64 to sortable u64 representation
/// This maps floats to integers that sort in the same order:
/// - Positive floats: flip sign bit
/// - Negative floats: flip all bits
pub inline fn f64ToSortable(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    // If negative (sign bit set), flip all bits; otherwise flip just sign bit
    const mask: u64 = @bitCast(-@as(i64, @bitCast(bits >> 63)));
    return bits ^ (mask | (1 << 63));
}

/// Convert f64 to sortable u64 representation (alternate implementation)
/// This ensures: -inf < negative < -0 < +0 < positive < +inf
pub inline fn floatToSortable(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    // If negative (sign bit set), flip all bits
    // If positive, flip only the sign bit
    // Use wrapping subtraction to handle the sign extension safely
    const sign_bit = bits >> 63;
    const mask: u64 = (0 -% sign_bit) | (@as(u64, 1) << 63);
    return bits ^ mask;
}

/// Convert sortable u64 back to f64
pub inline fn sortableToFloat(bits: u64) f64 {
    // Reverse the transformation
    // Use wrapping addition to avoid overflow
    const sign_bit = bits >> 63;
    const mask: u64 = ((~sign_bit) +% 1) | (@as(u64, 1) << 63);
    return @bitCast(bits ^ mask);
}

// ============================================================================
// Partitioning Functions
// ============================================================================

/// Simple partition for small arrays
fn simplePartitionPairs(pairs: []ValueIndexPair, ascending: bool) usize {
    if (pairs.len <= 1) return 0;

    const last = pairs.len - 1;
    const pivot = pairs[last].value;
    var i: usize = 0;

    for (0..last) |j| {
        const cmp = if (ascending) pairs[j].value < pivot else pairs[j].value > pivot;
        if (cmp) {
            std.mem.swap(ValueIndexPair, &pairs[i], &pairs[j]);
            i += 1;
        }
    }
    std.mem.swap(ValueIndexPair, &pairs[i], &pairs[last]);
    return i;
}

/// SIMD-accelerated partition for quicksort on pairs
/// Returns the partition index where all elements < pivot are on the left
fn simdPartitionPairs(pairs: []ValueIndexPair, ascending: bool) usize {
    if (pairs.len <= 1) return 0;
    if (pairs.len <= 16) {
        // Small array: use simple partition
        return simplePartitionPairs(pairs, ascending);
    }

    // Median-of-three pivot selection
    const mid = pairs.len / 2;
    const last = pairs.len - 1;

    if (ascending) {
        if (pairs[0].value > pairs[mid].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[mid]);
        if (pairs[0].value > pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[last]);
        if (pairs[mid].value > pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[mid], &pairs[last]);
    } else {
        if (pairs[0].value < pairs[mid].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[mid]);
        if (pairs[0].value < pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[0], &pairs[last]);
        if (pairs[mid].value < pairs[last].value) std.mem.swap(ValueIndexPair, &pairs[mid], &pairs[last]);
    }

    std.mem.swap(ValueIndexPair, &pairs[mid], &pairs[last - 1]);
    const pivot = pairs[last - 1].value;

    // SIMD partition using vector comparisons
    const Vec = @Vector(4, f64);
    const pivot_vec: Vec = @splat(pivot);

    var left: usize = 0;
    var right: usize = last - 1;

    // Process 4 elements at a time from left
    while (left + 4 <= right) {
        // Load 4 values
        var vals: Vec = undefined;
        inline for (0..4) |i| {
            vals[i] = pairs[left + i].value;
        }

        // Compare with pivot
        const cmp = if (ascending) vals < pivot_vec else vals > pivot_vec;

        // Count elements that should stay on left
        var stay_count: usize = 0;
        inline for (0..4) |i| {
            if (cmp[i]) stay_count += 1;
        }

        if (stay_count == 4) {
            // All stay on left
            left += 4;
        } else if (stay_count == 0) {
            // All go to right - swap with right side
            inline for (0..4) |i| {
                right -= 1;
                std.mem.swap(ValueIndexPair, &pairs[left + i], &pairs[right]);
            }
        } else {
            // Mixed - fall back to scalar
            break;
        }
    }

    // Scalar cleanup
    while (left < right) {
        const cmp = if (ascending) pairs[left].value < pivot else pairs[left].value > pivot;
        if (cmp) {
            left += 1;
        } else {
            right -= 1;
            std.mem.swap(ValueIndexPair, &pairs[left], &pairs[right]);
        }
    }

    // Move pivot to final position
    std.mem.swap(ValueIndexPair, &pairs[left], &pairs[last - 1]);
    return left;
}

// ============================================================================
// Insertion Sort
// ============================================================================

/// Insertion sort for small arrays
fn insertionSortPairs(pairs: []ValueIndexPair, ascending: bool) void {
    if (pairs.len <= 1) return;
    for (1..pairs.len) |i| {
        const key = pairs[i];
        var j: usize = i;
        while (j > 0) {
            const cmp = if (ascending) key.value < pairs[j - 1].value else key.value > pairs[j - 1].value;
            if (!cmp) break;
            pairs[j] = pairs[j - 1];
            j -= 1;
        }
        pairs[j] = key;
    }
}

// ============================================================================
// SIMD Quicksort
// ============================================================================

/// SIMD-accelerated quicksort on pairs
fn simdQuicksortPairs(pairs: []ValueIndexPair, ascending: bool) void {
    if (pairs.len <= 24) {
        insertionSortPairs(pairs, ascending);
        return;
    }

    const pivot_idx = simdPartitionPairs(pairs, ascending);

    // Recursively sort partitions
    if (pivot_idx > 0) simdQuicksortPairs(pairs[0..pivot_idx], ascending);
    if (pivot_idx + 1 < pairs.len) simdQuicksortPairs(pairs[pivot_idx + 1 ..], ascending);
}

// ============================================================================
// Parallel Sample Sort
// ============================================================================

/// Find the bucket for a value based on splitters
fn findBucket(value: f64, splitters: []const f64, ascending: bool) usize {
    for (splitters, 0..) |s, i| {
        if (ascending) {
            if (value < s) return i;
        } else {
            if (value > s) return i;
        }
    }
    return splitters.len;
}

/// Parallel sample sort for large arrays
fn parallelSampleSortPairs(pairs: []ValueIndexPair, ascending: bool) void {
    const len = pairs.len;
    const num_threads: usize = @min(getMaxThreads(), 8); // Cap at 8 threads for sort
    const num_buckets: usize = num_threads;

    if (len < PARALLEL_SORT_THRESHOLD or num_threads <= 1) {
        simdQuicksortPairs(pairs, ascending);
        return;
    }

    const allocator = std.heap.page_allocator;

    // Sample to find partition boundaries
    const sample_size = @min(num_buckets * 100, len / 10);
    var samples = allocator.alloc(f64, sample_size) catch {
        simdQuicksortPairs(pairs, ascending);
        return;
    };
    defer allocator.free(samples);

    // Take evenly spaced samples
    const step = len / sample_size;
    for (0..sample_size) |i| {
        samples[i] = pairs[i * step].value;
    }

    // Sort samples to find splitters
    if (ascending) {
        std.sort.pdq(f64, samples, {}, struct {
            fn lt(_: void, a: f64, b: f64) bool {
                return a < b;
            }
        }.lt);
    } else {
        std.sort.pdq(f64, samples, {}, struct {
            fn lt(_: void, a: f64, b: f64) bool {
                return a > b;
            }
        }.lt);
    }

    // Extract splitters (bucket boundaries)
    var splitters: [8]f64 = undefined;
    for (0..num_buckets - 1) |i| {
        splitters[i] = samples[(i + 1) * sample_size / num_buckets];
    }

    // Count elements per bucket
    var bucket_counts: [8]usize = [_]usize{0} ** 8;
    for (pairs) |pair| {
        const bucket = findBucket(pair.value, splitters[0 .. num_buckets - 1], ascending);
        bucket_counts[bucket] += 1;
    }

    // Calculate bucket offsets
    var bucket_offsets: [9]usize = undefined;
    bucket_offsets[0] = 0;
    for (0..num_buckets) |i| {
        bucket_offsets[i + 1] = bucket_offsets[i] + bucket_counts[i];
    }

    // Allocate temp array and distribute to buckets
    const temp = allocator.alloc(ValueIndexPair, len) catch {
        simdQuicksortPairs(pairs, ascending);
        return;
    };
    defer allocator.free(temp);

    var bucket_cursors: [8]usize = undefined;
    for (0..num_buckets) |i| {
        bucket_cursors[i] = bucket_offsets[i];
    }

    for (pairs) |pair| {
        const bucket = findBucket(pair.value, splitters[0 .. num_buckets - 1], ascending);
        temp[bucket_cursors[bucket]] = pair;
        bucket_cursors[bucket] += 1;
    }

    // Sort each bucket in parallel using Blitz
    const BucketSortCtx = struct {
        temp: []ValueIndexPair,
        bucket_offsets: *const [9]usize,
        num_buckets: usize,
        ascending: bool,
    };

    const ctx = BucketSortCtx{
        .temp = temp,
        .bucket_offsets = &bucket_offsets,
        .num_buckets = num_buckets,
        .ascending = ascending,
    };

    // Use Blitz to sort buckets in parallel
    blitz.parallelForWithGrain(num_buckets, BucketSortCtx, ctx, struct {
        fn sortBuckets(c: BucketSortCtx, start_bucket: usize, end_bucket: usize) void {
            for (start_bucket..end_bucket) |i| {
                const start = c.bucket_offsets[i];
                const end = c.bucket_offsets[i + 1];
                if (start < end) {
                    simdQuicksortPairs(c.temp[start..end], c.ascending);
                }
            }
        }
    }.sortBuckets, 1); // Grain size 1 means each bucket can be stolen

    // Copy back
    @memcpy(pairs, temp);
}

// ============================================================================
// Fallback Sort
// ============================================================================

/// Fallback simple argsort for when allocation fails
fn argsortFallback(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }
    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lt(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] < ctx[b];
            }
        }.lt);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lt(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] > ctx[b];
            }
        }.lt);
    }
}

/// Simple argsort for small arrays using comparison sort
fn argsortSmall(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);

    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] < ctx[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] > ctx[b];
            }
        }.lessThan);
    }
}

// ============================================================================
// Public Argsort Functions
// ============================================================================

/// High-performance radix sort for f64 argsort
/// Uses optimized single-threaded LSD radix sort
pub fn argsortPairRadix(data: []const f64, out_indices: []u32, ascending: bool) void {
    argsortRadixF64(data, out_indices, ascending);
}

/// Radix sort for f64 argsort - uses c_allocator for better performance
pub fn argsortRadixF64(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use simple sort
    if (len < 256) {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    }

    const allocator = std.heap.c_allocator;

    // Allocate working buffers
    const keys = allocator.alloc(u64, len) catch {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(keys);

    const temp_indices = allocator.alloc(u32, len) catch {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp_indices);

    const temp_keys = allocator.alloc(u64, len) catch {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp_keys);

    // Convert floats to sortable integers and initialize indices
    for (0..len) |i| {
        keys[i] = floatToSortable(data[i]);
        out_indices[i] = @intCast(i);
    }

    var src_keys = keys;
    var dst_keys = temp_keys;
    var src_indices = out_indices;
    var dst_indices = temp_indices;

    // LSD radix sort: 8 passes for 64-bit keys
    var pass: u8 = 0;
    while (pass < 8) : (pass += 1) {
        const shift: u6 = @intCast(pass * 8);
        // Count occurrences for this digit
        var counts: [256]usize = [_]usize{0} ** 256;
        for (src_keys[0..len]) |key| {
            const digit: usize = @intCast((key >> shift) & 0xFF);
            counts[digit] += 1;
        }

        // Compute prefix sums
        var offsets: [256]usize = undefined;
        var total: usize = 0;
        for (0..256) |i| {
            offsets[i] = total;
            total += counts[i];
        }

        // Distribute elements
        for (0..len) |i| {
            const key = src_keys[i];
            const digit: usize = @intCast((key >> shift) & 0xFF);
            const dst_pos = offsets[digit];
            offsets[digit] += 1;

            dst_keys[dst_pos] = key;
            dst_indices[dst_pos] = src_indices[i];
        }

        // Swap buffers
        const tmp_keys = src_keys;
        src_keys = dst_keys;
        dst_keys = tmp_keys;

        const tmp_indices = src_indices;
        src_indices = dst_indices;
        dst_indices = tmp_indices;
    }

    // After 8 passes, result is in original buffers

    // If descending, reverse
    if (!ascending) {
        var left: usize = 0;
        var right: usize = len - 1;
        while (left < right) {
            const tmp = out_indices[left];
            out_indices[left] = out_indices[right];
            out_indices[right] = tmp;
            left += 1;
            right -= 1;
        }
    }
}

/// Parallel sort using divide-and-conquer with parallel merge (using Blitz)
/// Each chunk is sorted in parallel, then merged using parallel pairwise merge
pub fn argsortParallelMerge(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use radix sort
    if (len < 16384) {
        argsortRadixF64(data, out_indices, ascending);
        return;
    }

    const num_threads = blitz.numWorkers();
    if (num_threads <= 1) {
        argsortRadixF64(data, out_indices, ascending);
        return;
    }

    const allocator = std.heap.page_allocator;
    const chunk_size = (len + num_threads - 1) / num_threads;

    // Allocate temp buffer for merging
    const temp_indices = allocator.alloc(u32, len) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp_indices);

    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    // Phase 1: Sort chunks in parallel using Blitz
    const ChunkSortCtx = struct {
        data: []const f64,
        indices: []u32,
        chunk_size: usize,
        len: usize,
        ascending: bool,
    };

    const sort_ctx = ChunkSortCtx{
        .data = data,
        .indices = out_indices,
        .chunk_size = chunk_size,
        .len = len,
        .ascending = ascending,
    };

    blitz.parallelForWithGrain(num_threads, ChunkSortCtx, sort_ctx, struct {
        fn sortChunks(ctx: ChunkSortCtx, start_chunk: usize, end_chunk: usize) void {
            for (start_chunk..end_chunk) |t| {
                const start = t * ctx.chunk_size;
                const end = @min(start + ctx.chunk_size, ctx.len);
                if (start >= ctx.len) continue;

                const chunk = ctx.indices[start..end];
                if (ctx.ascending) {
                    std.sort.pdq(u32, chunk, ctx.data, struct {
                        fn lt(d: []const f64, a: u32, b: u32) bool {
                            return d[a] < d[b];
                        }
                    }.lt);
                } else {
                    std.sort.pdq(u32, chunk, ctx.data, struct {
                        fn lt(d: []const f64, a: u32, b: u32) bool {
                            return d[a] > d[b];
                        }
                    }.lt);
                }
            }
        }
    }.sortChunks, 1);

    // Phase 2: Parallel pairwise merge
    var current_chunk_size = chunk_size;
    var src = out_indices;
    var dst = temp_indices;

    while (current_chunk_size < len) {
        const num_merges = (len + 2 * current_chunk_size - 1) / (2 * current_chunk_size);

        const MergeCtx = struct {
            data: []const f64,
            src: []const u32,
            dst: []u32,
            current_chunk_size: usize,
            len: usize,
            ascending: bool,
        };

        const merge_ctx = MergeCtx{
            .data = data,
            .src = src,
            .dst = dst,
            .current_chunk_size = current_chunk_size,
            .len = len,
            .ascending = ascending,
        };

        blitz.parallelForWithGrain(num_merges, MergeCtx, merge_ctx, struct {
            fn doMerges(ctx: MergeCtx, start_merge: usize, end_merge: usize) void {
                for (start_merge..end_merge) |t| {
                    const left = t * 2 * ctx.current_chunk_size;
                    if (left >= ctx.len) continue;

                    const mid = @min(left + ctx.current_chunk_size, ctx.len);
                    const right = @min(left + 2 * ctx.current_chunk_size, ctx.len);

                    if (mid >= right) {
                        // Only one chunk, just copy
                        @memcpy(ctx.dst[left..right], ctx.src[left..right]);
                        continue;
                    }

                    // Merge [left..mid] and [mid..right]
                    var i = left;
                    var j = mid;
                    var k = left;

                    while (i < mid and j < right) {
                        const cmp = if (ctx.ascending)
                            ctx.data[ctx.src[i]] <= ctx.data[ctx.src[j]]
                        else
                            ctx.data[ctx.src[i]] >= ctx.data[ctx.src[j]];

                        if (cmp) {
                            ctx.dst[k] = ctx.src[i];
                            i += 1;
                        } else {
                            ctx.dst[k] = ctx.src[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    while (i < mid) : (i += 1) {
                        ctx.dst[k] = ctx.src[i];
                        k += 1;
                    }
                    while (j < right) : (j += 1) {
                        ctx.dst[k] = ctx.src[j];
                        k += 1;
                    }
                }
            }
        }.doMerges, 1);

        // Swap buffers
        const tmp = src;
        src = dst;
        dst = tmp;

        current_chunk_size *= 2;
    }

    // If result is in temp buffer, copy back
    if (src.ptr != out_indices.ptr) {
        @memcpy(out_indices[0..len], src[0..len]);
    }
}

/// Parallel argsort using divide-and-conquer with Blitz
/// Divides data into chunks, sorts each in parallel, then merges
pub fn argsortParallel(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use simple sort
    if (len < 32768) {
        argsortSmall(T, data, out_indices, ascending);
        return;
    }

    const num_workers = blitz.numWorkers();
    const chunk_size = (len + num_workers - 1) / num_workers;
    const num_chunks = (len + chunk_size - 1) / chunk_size;
    const allocator = std.heap.page_allocator;

    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    // Temp buffer for merging
    const temp = allocator.alloc(u32, len) catch {
        argsortSmall(T, data, out_indices, ascending);
        return;
    };
    defer allocator.free(temp);

    // Sort chunks in parallel using Blitz
    const ChunkSortCtx = struct {
        data: []const T,
        indices: []u32,
        chunk_size: usize,
        len: usize,
        ascending: bool,
    };

    const sort_ctx = ChunkSortCtx{
        .data = data,
        .indices = out_indices,
        .chunk_size = chunk_size,
        .len = len,
        .ascending = ascending,
    };

    blitz.parallelForWithGrain(num_chunks, ChunkSortCtx, sort_ctx, struct {
        fn sortChunks(ctx: ChunkSortCtx, start_chunk: usize, end_chunk: usize) void {
            for (start_chunk..end_chunk) |c| {
                const start = c * ctx.chunk_size;
                const end = @min(start + ctx.chunk_size, ctx.len);
                if (start >= ctx.len) break;

                const chunk = ctx.indices[start..end];
                if (ctx.ascending) {
                    std.mem.sort(u32, chunk, ctx.data, struct {
                        fn lt(d: []const T, a: u32, b: u32) bool {
                            return d[a] < d[b];
                        }
                    }.lt);
                } else {
                    std.mem.sort(u32, chunk, ctx.data, struct {
                        fn lt(d: []const T, a: u32, b: u32) bool {
                            return d[a] > d[b];
                        }
                    }.lt);
                }
            }
        }
    }.sortChunks, 1);

    // Merge sorted chunks (log(num_chunks) levels)
    var current_size = chunk_size;
    var src = out_indices;
    var dst = temp;

    while (current_size < len) {
        const num_merges = (len + 2 * current_size - 1) / (2 * current_size);

        // Parallel merge using Blitz
        const MergeCtx = struct {
            data: []const T,
            src: []const u32,
            dst: []u32,
            current_size: usize,
            len: usize,
            ascending: bool,
        };

        const merge_ctx = MergeCtx{
            .data = data,
            .src = src,
            .dst = dst,
            .current_size = current_size,
            .len = len,
            .ascending = ascending,
        };

        blitz.parallelForWithGrain(num_merges, MergeCtx, merge_ctx, struct {
            fn doMerges(ctx: MergeCtx, start_merge: usize, end_merge: usize) void {
                for (start_merge..end_merge) |m| {
                    const left = m * 2 * ctx.current_size;
                    if (left >= ctx.len) break;

                    const mid = @min(left + ctx.current_size, ctx.len);
                    const right = @min(left + 2 * ctx.current_size, ctx.len);

                    if (mid >= right) {
                        @memcpy(ctx.dst[left..right], ctx.src[left..right]);
                        continue;
                    }

                    // Merge [left..mid] and [mid..right]
                    var i = left;
                    var j = mid;
                    var k = left;

                    while (i < mid and j < right) {
                        const cmp = if (ctx.ascending)
                            ctx.data[ctx.src[i]] <= ctx.data[ctx.src[j]]
                        else
                            ctx.data[ctx.src[i]] >= ctx.data[ctx.src[j]];

                        if (cmp) {
                            ctx.dst[k] = ctx.src[i];
                            i += 1;
                        } else {
                            ctx.dst[k] = ctx.src[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    @memcpy(ctx.dst[k .. k + (mid - i)], ctx.src[i..mid]);
                    k += mid - i;
                    @memcpy(ctx.dst[k .. k + (right - j)], ctx.src[j..right]);
                }
            }
        }.doMerges, 1);

        // Swap buffers
        const tmp = src;
        src = dst;
        dst = tmp;

        current_size *= 2;
    }

    // Copy result back if needed
    if (src.ptr != out_indices.ptr) {
        @memcpy(out_indices[0..len], src[0..len]);
    }
}

/// In-place parallel sort using Blitz (modifies out_indices directly)
fn argsortParallelInPlace(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = out_indices.len;
    const num_workers = blitz.numWorkers();
    const chunk_size = (len + num_workers - 1) / num_workers;
    const num_chunks = (len + chunk_size - 1) / chunk_size;
    const allocator = std.heap.page_allocator;

    // Sort chunks in parallel using Blitz
    const ChunkSortCtx = struct {
        data: []const T,
        indices: []u32,
        chunk_size: usize,
        len: usize,
        ascending: bool,
    };

    const sort_ctx = ChunkSortCtx{
        .data = data,
        .indices = out_indices,
        .chunk_size = chunk_size,
        .len = len,
        .ascending = ascending,
    };

    blitz.parallelForWithGrain(num_chunks, ChunkSortCtx, sort_ctx, struct {
        fn sortChunks(ctx: ChunkSortCtx, start_chunk: usize, end_chunk: usize) void {
            for (start_chunk..end_chunk) |c| {
                const start = c * ctx.chunk_size;
                const end = @min(start + ctx.chunk_size, ctx.len);
                if (start >= ctx.len) break;

                const chunk = ctx.indices[start..end];
                if (ctx.ascending) {
                    std.mem.sort(u32, chunk, ctx.data, struct {
                        fn lt(d: []const T, a: u32, b: u32) bool {
                            return d[a] < d[b];
                        }
                    }.lt);
                } else {
                    std.mem.sort(u32, chunk, ctx.data, struct {
                        fn lt(d: []const T, a: u32, b: u32) bool {
                            return d[a] > d[b];
                        }
                    }.lt);
                }
            }
        }
    }.sortChunks, 1);

    // K-way merge using heap
    const temp_buf = allocator.alloc(u32, len) catch {
        // Fallback: just return partially sorted (chunks are sorted)
        return;
    };
    defer allocator.free(temp_buf);

    // Merge pairs iteratively using Blitz
    var current_size = chunk_size;
    var src = out_indices;
    var dst = temp_buf;

    while (current_size < len) {
        const num_merges = (len + 2 * current_size - 1) / (2 * current_size);

        const MergeCtx = struct {
            data: []const T,
            src: []const u32,
            dst: []u32,
            current_size: usize,
            len: usize,
            ascending: bool,
        };

        const merge_ctx = MergeCtx{
            .data = data,
            .src = src,
            .dst = dst,
            .current_size = current_size,
            .len = len,
            .ascending = ascending,
        };

        blitz.parallelForWithGrain(num_merges, MergeCtx, merge_ctx, struct {
            fn doMerges(ctx: MergeCtx, start_merge: usize, end_merge: usize) void {
                for (start_merge..end_merge) |m| {
                    const left = m * 2 * ctx.current_size;
                    if (left >= ctx.len) break;

                    const mid = @min(left + ctx.current_size, ctx.len);
                    const right = @min(left + 2 * ctx.current_size, ctx.len);

                    if (mid >= right) {
                        @memcpy(ctx.dst[left..right], ctx.src[left..right]);
                        continue;
                    }

                    // Merge [left..mid] and [mid..right]
                    var i = left;
                    var j = mid;
                    var k = left;

                    while (i < mid and j < right) {
                        const cmp = if (ctx.ascending)
                            ctx.data[ctx.src[i]] <= ctx.data[ctx.src[j]]
                        else
                            ctx.data[ctx.src[i]] >= ctx.data[ctx.src[j]];

                        if (cmp) {
                            ctx.dst[k] = ctx.src[i];
                            i += 1;
                        } else {
                            ctx.dst[k] = ctx.src[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    @memcpy(ctx.dst[k .. k + (mid - i)], ctx.src[i..mid]);
                    k += mid - i;
                    @memcpy(ctx.dst[k .. k + (right - j)], ctx.src[j..right]);
                }
            }
        }.doMerges, 1);

        const tmp = src;
        src = dst;
        dst = tmp;
        current_size *= 2;
    }

    // Copy back if needed
    if (src.ptr != out_indices.ptr) {
        @memcpy(out_indices, src);
    }
}

/// Argsort - return indices that would sort the array
/// Uses pair-based radix sort for f64 (fastest), fallback to comparison sort for others
pub fn argsort(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For f64, use the new pair-based radix sort (O(n) and cache-friendly)
    if (T == f64) {
        argsortPairRadix(data, out_indices, ascending);
        return;
    }

    // For other types, use comparison-based sort
    // Initialize indices
    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    // Use parallel sort for large arrays
    if (len >= 100000) {
        argsortParallelInPlace(T, data, out_indices[0..len], ascending);
    } else {
        // Use block sort for smaller arrays
        if (ascending) {
            std.mem.sort(u32, out_indices[0..len], data, struct {
                fn lt(ctx: []const T, a: u32, b: u32) bool {
                    return ctx[a] < ctx[b];
                }
            }.lt);
        } else {
            std.mem.sort(u32, out_indices[0..len], data, struct {
                fn lt(ctx: []const T, a: u32, b: u32) bool {
                    return ctx[a] > ctx[b];
                }
            }.lt);
        }
    }
}

/// Argsort for integer types
pub fn argsortInt(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);

    for (out_indices[0..len], 0..) |*idx, i| {
        idx.* = @intCast(i);
    }

    if (ascending) {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] < ctx[b];
            }
        }.lessThan);
    } else {
        std.mem.sort(u32, out_indices[0..len], data, struct {
            fn lessThan(ctx: []const T, a: u32, b: u32) bool {
                return ctx[a] > ctx[b];
            }
        }.lessThan);
    }
}

// ============================================================================
// Tests
// ============================================================================

test "sorting - f64ToSortable preserves order" {
    const a = f64ToSortable(-10.0);
    const b = f64ToSortable(-1.0);
    const c = f64ToSortable(0.0);
    const d = f64ToSortable(1.0);
    const e = f64ToSortable(10.0);

    try std.testing.expect(a < b);
    try std.testing.expect(b < c);
    try std.testing.expect(c < d);
    try std.testing.expect(d < e);
}

test "sorting - floatToSortable preserves sort order" {
    // Test that floatToSortable preserves relative ordering
    const values = [_]f64{ -100.5, -1.0, 0.0, 1.0, 100.5 };

    for (0..values.len - 1) |i| {
        const a = floatToSortable(values[i]);
        const b = floatToSortable(values[i + 1]);
        try std.testing.expect(a < b);
    }
}

test "sorting - argsort f64 ascending" {
    const data = [_]f64{ 3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0 };
    var indices: [8]u32 = undefined;

    argsort(f64, &data, &indices, true);

    // Verify sorted order
    for (0..7) |i| {
        try std.testing.expect(data[indices[i]] <= data[indices[i + 1]]);
    }
}

test "sorting - argsort f64 descending" {
    const data = [_]f64{ 3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0 };
    var indices: [8]u32 = undefined;

    argsort(f64, &data, &indices, false);

    // Verify sorted order (descending)
    for (0..7) |i| {
        try std.testing.expect(data[indices[i]] >= data[indices[i + 1]]);
    }
}

test "sorting - argsortInt i64" {
    const data = [_]i64{ 5, 2, 8, 1, 9, 3 };
    var indices: [6]u32 = undefined;

    argsortInt(i64, &data, &indices, true);

    // Expected order: 1, 2, 3, 5, 8, 9
    try std.testing.expectEqual(@as(u32, 3), indices[0]); // index of 1
    try std.testing.expectEqual(@as(u32, 1), indices[1]); // index of 2
    try std.testing.expectEqual(@as(u32, 5), indices[2]); // index of 3
}

test "sorting - argsortRadixF64 small array" {
    const data = [_]f64{ 5.5, 2.2, 8.8, 1.1 };
    var indices: [4]u32 = undefined;

    argsortRadixF64(&data, &indices, true);

    // Verify sorted order
    try std.testing.expect(data[indices[0]] <= data[indices[1]]);
    try std.testing.expect(data[indices[1]] <= data[indices[2]]);
    try std.testing.expect(data[indices[2]] <= data[indices[3]]);
}

test "sorting - empty array" {
    const data: []const f64 = &[_]f64{};
    var indices: [0]u32 = undefined;

    argsort(f64, data, &indices, true);
    // Should not crash
}

test "sorting - single element" {
    const data = [_]f64{42.0};
    var indices: [1]u32 = undefined;

    argsort(f64, &data, &indices, true);
    try std.testing.expectEqual(@as(u32, 0), indices[0]);
}
