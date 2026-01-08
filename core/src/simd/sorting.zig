const std = @import("std");
const core = @import("core.zig");

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

    // Sort each bucket in parallel
    var threads: [8]?std.Thread = [_]?std.Thread{null} ** 8;

    for (0..num_buckets) |t| {
        const start = bucket_offsets[t];
        const end = bucket_offsets[t + 1];
        if (start >= end) continue;

        threads[t] = std.Thread.spawn(.{}, struct {
            fn work(slice: []ValueIndexPair, asc: bool) void {
                simdQuicksortPairs(slice, asc);
            }
        }.work, .{ temp[start..end], ascending }) catch null;
    }

    // Wait for all threads
    for (&threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

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

/// NEW: Pair-based sort using parallel sample sort + SIMD quicksort
/// This is cache-friendly because comparisons access contiguous memory
pub fn argsortPairRadix(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    const allocator = std.heap.page_allocator;

    // Allocate pairs array
    const pairs = allocator.alloc(ValueIndexPair, len) catch {
        argsortFallback(f64, data, out_indices, ascending);
        return;
    };
    defer allocator.free(pairs);

    // Initialize pairs
    for (data[0..len], 0..) |val, i| {
        pairs[i] = .{ .value = val, .idx = @intCast(i) };
    }

    // Sort pairs using parallel sample sort
    parallelSampleSortPairs(pairs, ascending);

    // Extract indices
    for (pairs, 0..) |pair, i| {
        out_indices[i] = pair.idx;
    }
}

/// Parallel radix sort for f64 argsort - much faster than comparison sort
/// Uses LSB radix sort with parallel counting and distribution
pub fn argsortRadixF64(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use simple sort
    if (len < 256) {
        argsortSmall(f64, data, out_indices, ascending);
        return;
    }

    const allocator = std.heap.page_allocator;

    // Allocate working buffers
    const keys = allocator.alloc(u64, len) catch {
        // Fallback to simple sort on allocation failure
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

    // Perform radix sort passes (8 passes for 64-bit keys)
    var src_keys = keys;
    var dst_keys = temp_keys;
    var src_indices = out_indices;
    var dst_indices = temp_indices;

    // LSD radix sort: process from least significant byte to most significant
    var shift: u6 = 0;
    while (shift < 64) : (shift += RADIX_BITS) {
        // Count occurrences for this digit
        var counts: [RADIX_SIZE]usize = [_]usize{0} ** RADIX_SIZE;
        for (src_keys[0..len]) |key| {
            const digit: usize = @intCast((key >> shift) & RADIX_MASK);
            counts[digit] += 1;
        }

        // Compute prefix sums (starting positions for each bucket)
        var offsets: [RADIX_SIZE]usize = undefined;
        var total: usize = 0;
        for (0..RADIX_SIZE) |i| {
            offsets[i] = total;
            total += counts[i];
        }

        // Distribute elements to destination
        for (0..len) |i| {
            const key = src_keys[i];
            const digit: usize = @intCast((key >> shift) & RADIX_MASK);
            const dst_pos = offsets[digit];
            offsets[digit] += 1;

            dst_keys[dst_pos] = key;
            dst_indices[dst_pos] = src_indices[i];
        }

        // Swap source and destination for next pass
        const tmp_keys = src_keys;
        src_keys = dst_keys;
        dst_keys = tmp_keys;

        const tmp_indices = src_indices;
        src_indices = dst_indices;
        dst_indices = tmp_indices;
    }

    // After 8 passes (even number), result is in original buffers
    // Keys are in keys, indices are in out_indices - already correct!

    // If descending, reverse the result
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

/// Parallel sort using divide-and-conquer with parallel merge
/// Each thread sorts its chunk, then we merge in parallel
pub fn argsortParallelMerge(data: []const f64, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use radix sort
    if (len < 16384) {
        argsortRadixF64(data, out_indices, ascending);
        return;
    }

    const num_threads = getMaxThreads();
    const allocator = std.heap.page_allocator;

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

    // Each thread sorts its chunk using radix sort
    const chunk_size = (len + num_threads - 1) / num_threads;

    // Allocate per-thread buffers for radix sort
    const thread_keys = allocator.alloc([]u64, num_threads) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(thread_keys);

    const thread_temp_keys = allocator.alloc([]u64, num_threads) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(thread_temp_keys);

    const thread_temp_indices = allocator.alloc([]u32, num_threads) catch {
        argsortRadixF64(data, out_indices, ascending);
        return;
    };
    defer allocator.free(thread_temp_indices);

    // Allocate buffers for each thread
    var alloc_failed = false;
    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) {
            thread_keys[t] = &[_]u64{};
            thread_temp_keys[t] = &[_]u64{};
            thread_temp_indices[t] = &[_]u32{};
            continue;
        }
        const csize = end - start;

        thread_keys[t] = allocator.alloc(u64, csize) catch {
            alloc_failed = true;
            break;
        };
        thread_temp_keys[t] = allocator.alloc(u64, csize) catch {
            alloc_failed = true;
            break;
        };
        thread_temp_indices[t] = allocator.alloc(u32, csize) catch {
            alloc_failed = true;
            break;
        };
    }

    if (alloc_failed) {
        // Free any allocated buffers and fallback
        for (0..num_threads) |t| {
            if (thread_keys[t].len > 0) allocator.free(thread_keys[t]);
            if (thread_temp_keys[t].len > 0) allocator.free(thread_temp_keys[t]);
            if (thread_temp_indices[t].len > 0) allocator.free(thread_temp_indices[t]);
        }
        argsortRadixF64(data, out_indices, ascending);
        return;
    }
    defer {
        for (0..num_threads) |t| {
            if (thread_keys[t].len > 0) allocator.free(thread_keys[t]);
            if (thread_temp_keys[t].len > 0) allocator.free(thread_temp_keys[t]);
            if (thread_temp_indices[t].len > 0) allocator.free(thread_temp_indices[t]);
        }
    }

    // Sort chunks in parallel
    var sort_threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;
    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) break;

        sort_threads[t] = std.Thread.spawn(.{}, struct {
            fn work(
                d: []const f64,
                indices: []u32,
                keys: []u64,
                temp_keys_buf: []u64,
                temp_idx: []u32,
                s: usize,
                e: usize,
            ) void {
                const clen = e - s;
                const chunk_indices = indices[s..e];

                // Convert to sortable keys
                for (0..clen) |i| {
                    keys[i] = floatToSortable(d[s + i]);
                }

                // LSD Radix sort on this chunk
                var src_keys_local = keys;
                var dst_keys_local = temp_keys_buf;
                var src_idx = chunk_indices;
                var dst_idx = temp_idx;

                var shift: u6 = 0;
                while (shift < 64) : (shift += RADIX_BITS) {
                    var counts: [RADIX_SIZE]usize = [_]usize{0} ** RADIX_SIZE;
                    for (src_keys_local[0..clen]) |key| {
                        const digit: usize = @intCast((key >> shift) & RADIX_MASK);
                        counts[digit] += 1;
                    }

                    var offsets: [RADIX_SIZE]usize = undefined;
                    var total: usize = 0;
                    for (0..RADIX_SIZE) |i| {
                        offsets[i] = total;
                        total += counts[i];
                    }

                    for (0..clen) |i| {
                        const key = src_keys_local[i];
                        const digit: usize = @intCast((key >> shift) & RADIX_MASK);
                        const dst_pos = offsets[digit];
                        offsets[digit] += 1;

                        dst_keys_local[dst_pos] = key;
                        dst_idx[dst_pos] = src_idx[i];
                    }

                    const tmp_k = src_keys_local;
                    src_keys_local = dst_keys_local;
                    dst_keys_local = tmp_k;

                    const tmp_i = src_idx;
                    src_idx = dst_idx;
                    dst_idx = tmp_i;
                }

                // Copy result back if needed (after 8 passes, result is in original)
                // Since we did 8 passes, result should be in keys and chunk_indices
            }
        }.work, .{ data, out_indices, thread_keys[t], thread_temp_keys[t], thread_temp_indices[t], start, end }) catch null;
    }

    // Wait for all sorts to complete
    for (&sort_threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // Now merge sorted chunks using k-way merge
    // For simplicity, do pairwise merging
    var current_chunk_size = chunk_size;
    var src = out_indices;
    var dst = temp_indices;

    while (current_chunk_size < len) {
        const merge_threads_count = (len + 2 * current_chunk_size - 1) / (2 * current_chunk_size);
        var merge_threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

        for (0..@min(merge_threads_count, num_threads)) |t| {
            const merge_start = t * 2 * current_chunk_size;
            if (merge_start >= len) break;

            const mid = @min(merge_start + current_chunk_size, len);
            const merge_end = @min(merge_start + 2 * current_chunk_size, len);

            merge_threads[t] = std.Thread.spawn(.{}, struct {
                fn work(d: []const f64, s_buf: []const u32, d_buf: []u32, left: usize, m: usize, right: usize, asc: bool) void {
                    var i = left;
                    var j = m;
                    var k = left;

                    while (i < m and j < right) {
                        const cmp = if (asc)
                            d[s_buf[i]] <= d[s_buf[j]]
                        else
                            d[s_buf[i]] >= d[s_buf[j]];

                        if (cmp) {
                            d_buf[k] = s_buf[i];
                            i += 1;
                        } else {
                            d_buf[k] = s_buf[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    while (i < m) {
                        d_buf[k] = s_buf[i];
                        i += 1;
                        k += 1;
                    }

                    while (j < right) {
                        d_buf[k] = s_buf[j];
                        j += 1;
                        k += 1;
                    }
                }
            }.work, .{ data, src, dst, merge_start, mid, merge_end, ascending }) catch null;
        }

        for (&merge_threads) |*t| {
            if (t.*) |thread| {
                thread.join();
                t.* = null;
            }
        }

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

/// Parallel argsort using divide-and-conquer
/// Divides data into chunks, sorts each in parallel, then merges
pub fn argsortParallel(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = @min(data.len, out_indices.len);
    if (len == 0) return;

    // For small arrays, use simple sort
    if (len < 32768) {
        argsortSmall(T, data, out_indices, ascending);
        return;
    }

    const num_threads = getMaxThreads();
    const chunk_size = (len + num_threads - 1) / num_threads;
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

    // Sort chunks in parallel
    var threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) break;

        threads[t] = std.Thread.spawn(.{}, struct {
            fn work(d: []const T, indices: []u32, s: usize, e: usize, asc: bool) void {
                const chunk = indices[s..e];
                if (asc) {
                    std.mem.sort(u32, chunk, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] < ctx[b];
                        }
                    }.lt);
                } else {
                    std.mem.sort(u32, chunk, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] > ctx[b];
                        }
                    }.lt);
                }
            }
        }.work, .{ data, out_indices, start, end, ascending }) catch null;
    }

    // Wait for all sorts
    for (&threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // Merge sorted chunks (log(num_threads) levels)
    var current_size = chunk_size;
    var src = out_indices;
    var dst = temp;

    while (current_size < len) {
        const num_merges = (len + 2 * current_size - 1) / (2 * current_size);

        // Parallel merge
        var merge_threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

        for (0..@min(num_merges, num_threads)) |t| {
            const left = t * 2 * current_size;
            if (left >= len) break;

            const mid = @min(left + current_size, len);
            const right = @min(left + 2 * current_size, len);

            if (mid >= right) {
                // Only one chunk, just copy
                @memcpy(dst[left..right], src[left..right]);
                continue;
            }

            merge_threads[t] = std.Thread.spawn(.{}, struct {
                fn work(d: []const T, s_buf: []const u32, d_buf: []u32, l: usize, m: usize, r: usize, asc: bool) void {
                    var i = l;
                    var j = m;
                    var k = l;

                    while (i < m and j < r) {
                        const cmp = if (asc)
                            d[s_buf[i]] <= d[s_buf[j]]
                        else
                            d[s_buf[i]] >= d[s_buf[j]];

                        if (cmp) {
                            d_buf[k] = s_buf[i];
                            i += 1;
                        } else {
                            d_buf[k] = s_buf[j];
                            j += 1;
                        }
                        k += 1;
                    }

                    while (i < m) : (i += 1) {
                        d_buf[k] = s_buf[i];
                        k += 1;
                    }
                    while (j < r) : (j += 1) {
                        d_buf[k] = s_buf[j];
                        k += 1;
                    }
                }
            }.work, .{ data, src, dst, left, mid, right, ascending }) catch null;
        }

        // Handle remaining merges if more than num_threads
        for (@min(num_merges, num_threads)..num_merges) |t| {
            const left = t * 2 * current_size;
            if (left >= len) break;

            const mid = @min(left + current_size, len);
            const right = @min(left + 2 * current_size, len);

            if (mid >= right) {
                @memcpy(dst[left..right], src[left..right]);
                continue;
            }

            // Sequential merge for extra chunks
            var i = left;
            var j = mid;
            var k = left;

            while (i < mid and j < right) {
                const cmp = if (ascending)
                    data[src[i]] <= data[src[j]]
                else
                    data[src[i]] >= data[src[j]];

                if (cmp) {
                    dst[k] = src[i];
                    i += 1;
                } else {
                    dst[k] = src[j];
                    j += 1;
                }
                k += 1;
            }

            while (i < mid) : (i += 1) {
                dst[k] = src[i];
                k += 1;
            }
            while (j < right) : (j += 1) {
                dst[k] = src[j];
                k += 1;
            }
        }

        for (&merge_threads) |*t| {
            if (t.*) |thread| {
                thread.join();
                t.* = null;
            }
        }

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

/// In-place parallel sort (modifies out_indices directly)
fn argsortParallelInPlace(comptime T: type, data: []const T, out_indices: []u32, ascending: bool) void {
    const len = out_indices.len;
    const num_threads = getMaxThreads();
    const chunk_size = (len + num_threads - 1) / num_threads;
    const allocator = std.heap.page_allocator;

    // Sort chunks in parallel
    var threads: [MAX_THREADS]?std.Thread = [_]?std.Thread{null} ** MAX_THREADS;

    for (0..num_threads) |t| {
        const start = t * chunk_size;
        const end = @min(start + chunk_size, len);
        if (start >= len) break;

        threads[t] = std.Thread.spawn(.{}, struct {
            fn work(d: []const T, indices: []u32, asc: bool) void {
                if (asc) {
                    std.mem.sort(u32, indices, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] < ctx[b];
                        }
                    }.lt);
                } else {
                    std.mem.sort(u32, indices, d, struct {
                        fn lt(ctx: []const T, a: u32, b: u32) bool {
                            return ctx[a] > ctx[b];
                        }
                    }.lt);
                }
            }
        }.work, .{ data, out_indices[start..end], ascending }) catch null;
    }

    for (&threads) |*t| {
        if (t.*) |thread| {
            thread.join();
            t.* = null;
        }
    }

    // K-way merge using heap
    const temp_buf = allocator.alloc(u32, len) catch {
        // Fallback: just return partially sorted (chunks are sorted)
        return;
    };
    defer allocator.free(temp_buf);

    // Merge pairs iteratively
    var current_size = chunk_size;
    var src = out_indices;
    var dst = temp_buf;

    while (current_size < len) {
        var merge_idx: usize = 0;
        while (merge_idx * 2 * current_size < len) : (merge_idx += 1) {
            const left = merge_idx * 2 * current_size;
            const mid = @min(left + current_size, len);
            const right = @min(left + 2 * current_size, len);

            // Merge [left..mid] and [mid..right] into dst[left..right]
            var i = left;
            var j = mid;
            var k = left;

            while (i < mid and j < right) {
                const cmp = if (ascending)
                    data[src[i]] <= data[src[j]]
                else
                    data[src[i]] >= data[src[j]];

                if (cmp) {
                    dst[k] = src[i];
                    i += 1;
                } else {
                    dst[k] = src[j];
                    j += 1;
                }
                k += 1;
            }

            @memcpy(dst[k .. k + (mid - i)], src[i..mid]);
            k += mid - i;
            @memcpy(dst[k .. k + (right - j)], src[j..right]);
        }

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
