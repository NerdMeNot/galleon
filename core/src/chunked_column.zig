const std = @import("std");
const Allocator = std.mem.Allocator;
const blitz = @import("blitz/mod.zig");

// ============================================================================
// Chunked Column - Cache-Friendly Columnar Storage
// ============================================================================
//
// Key Design Principles:
// 1. Data is split into fixed-size chunks that fit in L2 cache
// 2. Operations process chunk-by-chunk, always cache-warm
// 3. Scratch space is pre-allocated and reused across operations
// 4. All parallelism goes through Blitz
//
// ============================================================================

/// Number of elements per chunk - chosen to fit in L2 cache
/// 64K elements * 8 bytes = 512KB for f64 (typical L2 is 512KB-1MB per core)
pub const CHUNK_SIZE: usize = 65536;

/// Cache line size for alignment
pub const CACHE_LINE_SIZE: usize = 64;
const CACHE_LINE_ALIGN: std.mem.Alignment = .@"64";

/// Minimum chunks to parallelize (avoid overhead for tiny data)
pub const MIN_PARALLEL_CHUNKS: usize = 2;

// ============================================================================
// Scratch Space - Reusable Operation Buffers
// ============================================================================

/// Per-column scratch space for operations
/// Allocated lazily on first use, reused forever
pub fn ColumnScratch(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        num_chunks: usize,

        // For sorting - one buffer per chunk
        sort_keys: ?[][]align(CACHE_LINE_SIZE) u64,
        sort_indices: ?[][]align(CACHE_LINE_SIZE) u32,
        sort_temp_keys: ?[][]align(CACHE_LINE_SIZE) u64,
        sort_temp_indices: ?[][]align(CACHE_LINE_SIZE) u32,

        // Single chunk-sized temp buffer for general use
        temp_chunk: ?[]align(CACHE_LINE_SIZE) T,

        pub fn init(allocator: Allocator, num_chunks: usize) Self {
            return Self{
                .allocator = allocator,
                .num_chunks = num_chunks,
                .sort_keys = null,
                .sort_indices = null,
                .sort_temp_keys = null,
                .sort_temp_indices = null,
                .temp_chunk = null,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.sort_keys) |keys| {
                for (keys) |chunk| {
                    self.allocator.free(chunk);
                }
                self.allocator.free(keys);
            }
            if (self.sort_indices) |indices| {
                for (indices) |chunk| {
                    self.allocator.free(chunk);
                }
                self.allocator.free(indices);
            }
            if (self.sort_temp_keys) |keys| {
                for (keys) |chunk| {
                    self.allocator.free(chunk);
                }
                self.allocator.free(keys);
            }
            if (self.sort_temp_indices) |indices| {
                for (indices) |chunk| {
                    self.allocator.free(chunk);
                }
                self.allocator.free(indices);
            }
            if (self.temp_chunk) |chunk| {
                self.allocator.free(chunk);
            }
        }

        /// Ensure sort buffers are allocated
        pub fn ensureSortBuffers(self: *Self, chunk_sizes: []const usize) !void {
            if (self.sort_keys != null) return; // Already allocated

            const n = self.num_chunks;

            // Allocate arrays of chunk pointers
            self.sort_keys = try self.allocator.alloc([]align(CACHE_LINE_SIZE) u64, n);
            self.sort_indices = try self.allocator.alloc([]align(CACHE_LINE_SIZE) u32, n);
            self.sort_temp_keys = try self.allocator.alloc([]align(CACHE_LINE_SIZE) u64, n);
            self.sort_temp_indices = try self.allocator.alloc([]align(CACHE_LINE_SIZE) u32, n);

            // Allocate each chunk's buffers
            for (0..n) |i| {
                const size = chunk_sizes[i];
                self.sort_keys.?[i] = try self.allocator.alignedAlloc(u64, CACHE_LINE_ALIGN, size);
                self.sort_indices.?[i] = try self.allocator.alignedAlloc(u32, CACHE_LINE_ALIGN, size);
                self.sort_temp_keys.?[i] = try self.allocator.alignedAlloc(u64, CACHE_LINE_ALIGN, size);
                self.sort_temp_indices.?[i] = try self.allocator.alignedAlloc(u32, CACHE_LINE_ALIGN, size);
            }
        }

        /// Ensure temp chunk buffer is allocated
        pub fn ensureTempChunk(self: *Self) ![]align(CACHE_LINE_SIZE) T {
            if (self.temp_chunk) |chunk| return chunk;

            self.temp_chunk = try self.allocator.alignedAlloc(T, CACHE_LINE_ALIGN, CHUNK_SIZE);
            return self.temp_chunk.?;
        }
    };
}

// ============================================================================
// Chunked Column
// ============================================================================

/// A column stored as an array of fixed-size chunks
/// This is the core data structure for Galleon V2
pub fn ChunkedColumn(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,

        /// Array of chunk pointers - each chunk is cache-aligned
        chunks: [][]align(CACHE_LINE_SIZE) T,

        /// Number of chunks (including partial last chunk)
        num_chunks: usize,

        /// Total number of elements across all chunks
        total_length: usize,

        /// Size of each chunk (last chunk may be smaller)
        chunk_sizes: []usize,

        /// Reusable scratch space for operations
        scratch: ?*ColumnScratch(T),

        // ====================================================================
        // Creation and Destruction
        // ====================================================================

        /// Create a chunked column from a contiguous slice
        /// Data is copied into cache-aligned chunks
        pub fn createFromSlice(allocator: Allocator, data: []const T) !*Self {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            self.allocator = allocator;
            self.total_length = data.len;
            self.scratch = null;

            if (data.len == 0) {
                self.chunks = &[_][]align(CACHE_LINE_SIZE) T{};
                self.chunk_sizes = &[_]usize{};
                self.num_chunks = 0;
                return self;
            }

            // Calculate number of chunks needed
            self.num_chunks = (data.len + CHUNK_SIZE - 1) / CHUNK_SIZE;

            // Allocate chunk pointer array
            self.chunks = try allocator.alloc([]align(CACHE_LINE_SIZE) T, self.num_chunks);
            errdefer allocator.free(self.chunks);

            // Allocate chunk sizes array
            self.chunk_sizes = try allocator.alloc(usize, self.num_chunks);
            errdefer allocator.free(self.chunk_sizes);

            // Allocate and fill each chunk
            var offset: usize = 0;
            for (0..self.num_chunks) |i| {
                const chunk_len = @min(CHUNK_SIZE, data.len - offset);
                self.chunk_sizes[i] = chunk_len;

                self.chunks[i] = try allocator.alignedAlloc(T, CACHE_LINE_ALIGN, chunk_len);
                @memcpy(self.chunks[i], data[offset..][0..chunk_len]);

                offset += chunk_len;
            }

            return self;
        }

        /// Create an empty chunked column with pre-allocated capacity
        pub fn createWithCapacity(allocator: Allocator, capacity: usize) !*Self {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            self.allocator = allocator;
            self.total_length = 0;
            self.scratch = null;

            if (capacity == 0) {
                self.chunks = &[_][]align(CACHE_LINE_SIZE) T{};
                self.chunk_sizes = &[_]usize{};
                self.num_chunks = 0;
                return self;
            }

            // Pre-allocate chunks for capacity
            self.num_chunks = (capacity + CHUNK_SIZE - 1) / CHUNK_SIZE;

            self.chunks = try allocator.alloc([]align(CACHE_LINE_SIZE) T, self.num_chunks);
            errdefer allocator.free(self.chunks);

            self.chunk_sizes = try allocator.alloc(usize, self.num_chunks);
            errdefer allocator.free(self.chunk_sizes);

            for (0..self.num_chunks) |i| {
                const chunk_cap = if (i == self.num_chunks - 1)
                    capacity - (i * CHUNK_SIZE)
                else
                    CHUNK_SIZE;

                self.chunks[i] = try allocator.alignedAlloc(T, CACHE_LINE_ALIGN, chunk_cap);
                self.chunk_sizes[i] = 0; // Empty initially
            }

            return self;
        }

        /// Destroy the column and free all memory
        pub fn destroy(self: *Self) void {
            // Free scratch space if allocated
            if (self.scratch) |scratch| {
                scratch.deinit();
                self.allocator.destroy(scratch);
            }

            // Free each chunk
            for (self.chunks) |chunk| {
                self.allocator.free(chunk);
            }

            // Free chunk arrays
            if (self.chunks.len > 0) {
                self.allocator.free(self.chunks);
                self.allocator.free(self.chunk_sizes);
            }

            // Free self
            self.allocator.destroy(self);
        }

        // ====================================================================
        // Scratch Space Management
        // ====================================================================

        /// Ensure scratch space is allocated
        pub fn ensureScratch(self: *Self) !*ColumnScratch(T) {
            if (self.scratch) |s| return s;

            const scratch = try self.allocator.create(ColumnScratch(T));
            scratch.* = ColumnScratch(T).init(self.allocator, self.num_chunks);
            self.scratch = scratch;
            return scratch;
        }

        // ====================================================================
        // Data Access
        // ====================================================================

        /// Get element at index (bounds-checked)
        pub fn get(self: *const Self, index: usize) ?T {
            if (index >= self.total_length) return null;

            const chunk_idx = index / CHUNK_SIZE;
            const local_idx = index % CHUNK_SIZE;

            return self.chunks[chunk_idx][local_idx];
        }

        /// Set element at index (bounds-checked)
        pub fn set(self: *Self, index: usize, value: T) bool {
            if (index >= self.total_length) return false;

            const chunk_idx = index / CHUNK_SIZE;
            const local_idx = index % CHUNK_SIZE;

            self.chunks[chunk_idx][local_idx] = value;
            return true;
        }

        /// Get a specific chunk by index
        pub fn getChunk(self: *const Self, chunk_idx: usize) ?[]const T {
            if (chunk_idx >= self.num_chunks) return null;
            return self.chunks[chunk_idx][0..self.chunk_sizes[chunk_idx]];
        }

        /// Copy all data to a contiguous slice
        /// Caller must provide a slice of at least total_length elements
        pub fn copyToSlice(self: *const Self, dest: []T) void {
            var offset: usize = 0;
            for (0..self.num_chunks) |i| {
                const chunk_len = self.chunk_sizes[i];
                @memcpy(dest[offset..][0..chunk_len], self.chunks[i][0..chunk_len]);
                offset += chunk_len;
            }
        }

        // ====================================================================
        // Iteration
        // ====================================================================

        /// Iterator over all elements
        pub const Iterator = struct {
            column: *const Self,
            chunk_idx: usize,
            local_idx: usize,

            pub fn next(self: *Iterator) ?T {
                while (self.chunk_idx < self.column.num_chunks) {
                    const chunk_size = self.column.chunk_sizes[self.chunk_idx];
                    if (self.local_idx < chunk_size) {
                        const value = self.column.chunks[self.chunk_idx][self.local_idx];
                        self.local_idx += 1;
                        return value;
                    }
                    self.chunk_idx += 1;
                    self.local_idx = 0;
                }
                return null;
            }
        };

        pub fn iterator(self: *const Self) Iterator {
            return Iterator{
                .column = self,
                .chunk_idx = 0,
                .local_idx = 0,
            };
        }
    };
}

// ============================================================================
// Chunk-Parallel Operations
// ============================================================================

/// Operations on ChunkedColumn using Blitz for parallelism.
/// All operations are cache-friendly - each chunk fits in L2.
pub fn ChunkedOps(comptime T: type) type {
    return struct {
        const simd = @import("simd.zig");

        // ====================================================================
        // Aggregations
        // ====================================================================

        /// Parallel sum over all chunks
        pub fn sum(col: *const ChunkedColumn(T)) T {
            if (col.total_length == 0) return 0;

            // For small data, sequential
            if (col.num_chunks < MIN_PARALLEL_CHUNKS) {
                var total: T = 0;
                for (0..col.num_chunks) |i| {
                    total += simd.sum(T, col.chunks[i][0..col.chunk_sizes[i]]);
                }
                return total;
            }

            // Parallel: sum each chunk, then combine
            const Context = struct {};
            const ctx = Context{};

            const chunk_sums = blitz.parallelChunkReduce(
                T,
                T,
                @constCast(@ptrCast(col.chunks)),
                col.chunk_sizes,
                std.heap.c_allocator,
                0,
                Context,
                ctx,
                struct {
                    fn process(_: Context, chunk: []const T) T {
                        return simd.sum(T, chunk);
                    }
                }.process,
                struct {
                    fn combine(a: T, b: T) T {
                        return a + b;
                    }
                }.combine,
            ) catch {
                // Fallback to sequential on allocation failure
                var total: T = 0;
                for (0..col.num_chunks) |i| {
                    total += simd.sum(T, col.chunks[i][0..col.chunk_sizes[i]]);
                }
                return total;
            };

            return chunk_sums;
        }

        /// Parallel min over all chunks
        pub fn min(col: *const ChunkedColumn(T)) ?T {
            if (col.total_length == 0) return null;

            // For small data, sequential
            if (col.num_chunks < MIN_PARALLEL_CHUNKS) {
                var result: ?T = null;
                for (0..col.num_chunks) |i| {
                    const chunk_min = simd.min(T, col.chunks[i][0..col.chunk_sizes[i]]);
                    if (chunk_min) |cm| {
                        if (result) |r| {
                            result = @min(r, cm);
                        } else {
                            result = cm;
                        }
                    }
                }
                return result;
            }

            // Parallel: min each chunk, then combine
            const identity = std.math.floatMax(T);
            const Context = struct {};
            const ctx = Context{};

            const chunk_mins = blitz.parallelChunkReduce(
                T,
                T,
                @constCast(@ptrCast(col.chunks)),
                col.chunk_sizes,
                std.heap.c_allocator,
                identity,
                Context,
                ctx,
                struct {
                    fn process(_: Context, chunk: []const T) T {
                        return simd.min(T, chunk) orelse std.math.floatMax(T);
                    }
                }.process,
                struct {
                    fn combine(a: T, b: T) T {
                        return @min(a, b);
                    }
                }.combine,
            ) catch {
                // Fallback to sequential
                var result: T = identity;
                for (0..col.num_chunks) |i| {
                    const chunk_min = simd.min(T, col.chunks[i][0..col.chunk_sizes[i]]) orelse identity;
                    result = @min(result, chunk_min);
                }
                return if (result == identity) null else result;
            };

            return if (chunk_mins == identity) null else chunk_mins;
        }

        /// Parallel max over all chunks
        pub fn max(col: *const ChunkedColumn(T)) ?T {
            if (col.total_length == 0) return null;

            // For small data, sequential
            if (col.num_chunks < MIN_PARALLEL_CHUNKS) {
                var result: ?T = null;
                for (0..col.num_chunks) |i| {
                    const chunk_max = simd.max(T, col.chunks[i][0..col.chunk_sizes[i]]);
                    if (chunk_max) |cm| {
                        if (result) |r| {
                            result = @max(r, cm);
                        } else {
                            result = cm;
                        }
                    }
                }
                return result;
            }

            // Parallel: max each chunk, then combine
            const identity = std.math.floatMin(T);
            const Context = struct {};
            const ctx = Context{};

            const chunk_maxs = blitz.parallelChunkReduce(
                T,
                T,
                @constCast(@ptrCast(col.chunks)),
                col.chunk_sizes,
                std.heap.c_allocator,
                identity,
                Context,
                ctx,
                struct {
                    fn process(_: Context, chunk: []const T) T {
                        return simd.max(T, chunk) orelse std.math.floatMin(T);
                    }
                }.process,
                struct {
                    fn combine(a: T, b: T) T {
                        return @max(a, b);
                    }
                }.combine,
            ) catch {
                // Fallback to sequential
                var result: T = identity;
                for (0..col.num_chunks) |i| {
                    const chunk_max = simd.max(T, col.chunks[i][0..col.chunk_sizes[i]]) orelse identity;
                    result = @max(result, chunk_max);
                }
                return if (result == identity) null else result;
            };

            return if (chunk_maxs == identity) null else chunk_maxs;
        }

        /// Parallel mean over all chunks
        pub fn mean(col: *const ChunkedColumn(T)) ?T {
            if (col.total_length == 0) return null;
            const s = sum(col);
            return s / @as(T, @floatFromInt(col.total_length));
        }

        // ====================================================================
        // Filtering
        // ====================================================================

        /// Filter result holding counts and mask for each chunk
        pub const FilterResult = struct {
            /// Number of matching elements in each chunk
            chunk_counts: []usize,
            /// Total matching elements
            total_count: usize,
            /// Allocator for cleanup
            allocator: Allocator,

            pub fn deinit(self: *FilterResult) void {
                self.allocator.free(self.chunk_counts);
            }
        };

        /// Count elements matching predicate per chunk (parallel)
        /// Returns counts needed to allocate filter output
        pub fn filterCount(
            col: *const ChunkedColumn(T),
            allocator: Allocator,
            comptime pred_fn: fn (T) bool,
        ) !FilterResult {
            if (col.num_chunks == 0) {
                return FilterResult{
                    .chunk_counts = &[_]usize{},
                    .total_count = 0,
                    .allocator = allocator,
                };
            }

            const counts = try allocator.alloc(usize, col.num_chunks);
            errdefer allocator.free(counts);

            // Count matches in each chunk (can parallelize)
            for (0..col.num_chunks) |i| {
                var count: usize = 0;
                for (col.chunks[i][0..col.chunk_sizes[i]]) |val| {
                    if (pred_fn(val)) count += 1;
                }
                counts[i] = count;
            }

            // Sum total
            var total: usize = 0;
            for (counts) |c| total += c;

            return FilterResult{
                .chunk_counts = counts,
                .total_count = total,
                .allocator = allocator,
            };
        }

        /// Apply filter and create new chunked column with matching elements
        pub fn filter(
            col: *const ChunkedColumn(T),
            allocator: Allocator,
            comptime pred_fn: fn (T) bool,
        ) !*ChunkedColumn(T) {
            // First pass: count matches per chunk
            var filter_result = try filterCount(col, allocator, pred_fn);
            defer filter_result.deinit();

            if (filter_result.total_count == 0) {
                return ChunkedColumn(T).createFromSlice(allocator, &[_]T{});
            }

            // Allocate output data
            const out_data = try allocator.alloc(T, filter_result.total_count);
            defer allocator.free(out_data);

            // Second pass: gather matching elements
            var out_idx: usize = 0;
            for (0..col.num_chunks) |i| {
                for (col.chunks[i][0..col.chunk_sizes[i]]) |val| {
                    if (pred_fn(val)) {
                        out_data[out_idx] = val;
                        out_idx += 1;
                    }
                }
            }

            // Create new chunked column from filtered data
            return ChunkedColumn(T).createFromSlice(allocator, out_data);
        }

        /// Filter by comparison with a scalar value
        pub fn filterGt(col: *const ChunkedColumn(T), allocator: Allocator, threshold: T) !*ChunkedColumn(T) {
            // Count matches
            var total_count: usize = 0;
            const counts = try allocator.alloc(usize, col.num_chunks);
            defer allocator.free(counts);

            for (0..col.num_chunks) |i| {
                var count: usize = 0;
                const chunk = col.chunks[i][0..col.chunk_sizes[i]];
                for (chunk) |val| {
                    if (val > threshold) count += 1;
                }
                counts[i] = count;
                total_count += count;
            }

            if (total_count == 0) {
                return ChunkedColumn(T).createFromSlice(allocator, &[_]T{});
            }

            // Allocate and gather
            const out_data = try allocator.alloc(T, total_count);
            defer allocator.free(out_data);

            var out_idx: usize = 0;
            for (0..col.num_chunks) |i| {
                const chunk = col.chunks[i][0..col.chunk_sizes[i]];
                for (chunk) |val| {
                    if (val > threshold) {
                        out_data[out_idx] = val;
                        out_idx += 1;
                    }
                }
            }

            return ChunkedColumn(T).createFromSlice(allocator, out_data);
        }

        /// Filter by comparison: less than
        pub fn filterLt(col: *const ChunkedColumn(T), allocator: Allocator, threshold: T) !*ChunkedColumn(T) {
            var total_count: usize = 0;
            const counts = try allocator.alloc(usize, col.num_chunks);
            defer allocator.free(counts);

            for (0..col.num_chunks) |i| {
                var count: usize = 0;
                const chunk = col.chunks[i][0..col.chunk_sizes[i]];
                for (chunk) |val| {
                    if (val < threshold) count += 1;
                }
                counts[i] = count;
                total_count += count;
            }

            if (total_count == 0) {
                return ChunkedColumn(T).createFromSlice(allocator, &[_]T{});
            }

            const out_data = try allocator.alloc(T, total_count);
            defer allocator.free(out_data);

            var out_idx: usize = 0;
            for (0..col.num_chunks) |i| {
                const chunk = col.chunks[i][0..col.chunk_sizes[i]];
                for (chunk) |val| {
                    if (val < threshold) {
                        out_data[out_idx] = val;
                        out_idx += 1;
                    }
                }
            }

            return ChunkedColumn(T).createFromSlice(allocator, out_data);
        }

        // ====================================================================
        // Sorting
        // ====================================================================

        /// Argsort result with global indices
        pub const ArgsortResult = struct {
            indices: []u32,
            allocator: Allocator,

            pub fn deinit(self: *ArgsortResult) void {
                self.allocator.free(self.indices);
            }
        };

        /// Argsort: return indices that would sort the column
        /// Uses chunk-local sorting followed by k-way merge
        /// Each chunk's sort is cache-warm (fits in L2)
        pub fn argsort(col: *ChunkedColumn(T), allocator: Allocator) !ArgsortResult {
            if (col.total_length == 0) {
                return ArgsortResult{
                    .indices = &[_]u32{},
                    .allocator = allocator,
                };
            }

            // Single chunk: just sort it directly
            if (col.num_chunks == 1) {
                const indices = try allocator.alloc(u32, col.total_length);
                errdefer allocator.free(indices);

                // Initialize indices
                for (indices, 0..) |*idx, i| {
                    idx.* = @intCast(i);
                }

                // Sort using insertion sort for small, or SIMD sort for larger
                const chunk = col.chunks[0][0..col.chunk_sizes[0]];
                sortIndicesByValue(chunk, indices);

                return ArgsortResult{
                    .indices = indices,
                    .allocator = allocator,
                };
            }

            // Multi-chunk: sort each chunk, then k-way merge
            return argsortMultiChunk(col, allocator);
        }

        /// Sort a slice of indices by corresponding values
        /// Uses pdq sort for thread-safe operation (radix sort has allocation issues in threads)
        fn sortIndicesByValue(values: []const T, indices: []u32) void {
            std.sort.pdq(u32, indices, values, struct {
                fn lessThan(vals: []const T, a: u32, b: u32) bool {
                    return vals[a] < vals[b];
                }
            }.lessThan);
        }

        /// Multi-chunk argsort with k-way merge
        fn argsortMultiChunk(col: *ChunkedColumn(T), allocator: Allocator) !ArgsortResult {
            const n_chunks = col.num_chunks;

            // Allocate per-chunk index arrays
            const chunk_indices = try allocator.alloc([]u32, n_chunks);
            defer {
                for (chunk_indices) |ci| allocator.free(ci);
                allocator.free(chunk_indices);
            }

            // Sort each chunk sequentially
            for (0..n_chunks) |i| {
                const chunk_size = col.chunk_sizes[i];
                chunk_indices[i] = try allocator.alloc(u32, chunk_size);

                // Initialize chunk-local indices
                for (chunk_indices[i], 0..) |*idx, j| {
                    idx.* = @intCast(j);
                }

                // Sort this chunk
                sortIndicesByValue(col.chunks[i][0..chunk_size], chunk_indices[i]);
            }

            // K-way merge using min-heap
            const result = try allocator.alloc(u32, col.total_length);
            errdefer allocator.free(result);

            // Heap entry: (chunk_idx, position_in_chunk)
            const HeapEntry = struct {
                chunk_idx: usize,
                pos: usize,
            };

            // Initialize heap with first element from each chunk
            var heap = try std.ArrayList(HeapEntry).initCapacity(allocator, n_chunks);
            defer heap.deinit(allocator);

            for (0..n_chunks) |i| {
                if (col.chunk_sizes[i] > 0) {
                    heap.appendAssumeCapacity(HeapEntry{ .chunk_idx = i, .pos = 0 });
                }
            }

            // Heapify - create min-heap
            const Context = struct {
                col: *ChunkedColumn(T),
                chunk_indices: [][]u32,

                fn getValue(self: @This(), entry: HeapEntry) T {
                    const local_idx = self.chunk_indices[entry.chunk_idx][entry.pos];
                    return self.col.chunks[entry.chunk_idx][local_idx];
                }

                fn lessThan(self: @This(), a: HeapEntry, b: HeapEntry) bool {
                    return self.getValue(a) < self.getValue(b);
                }
            };

            const ctx = Context{ .col = col, .chunk_indices = chunk_indices };

            // Build min-heap
            heapify(HeapEntry, heap.items, ctx);

            // Extract elements in sorted order
            var out_idx: usize = 0;
            while (heap.items.len > 0) {
                // Pop minimum
                const min_entry = heap.items[0];
                const local_idx = chunk_indices[min_entry.chunk_idx][min_entry.pos];

                // Convert to global index
                var global_idx: u32 = local_idx;
                for (0..min_entry.chunk_idx) |c| {
                    global_idx += @intCast(col.chunk_sizes[c]);
                }
                result[out_idx] = global_idx;
                out_idx += 1;

                // Advance this chunk's position
                const new_pos = min_entry.pos + 1;
                if (new_pos < col.chunk_sizes[min_entry.chunk_idx]) {
                    heap.items[0] = HeapEntry{ .chunk_idx = min_entry.chunk_idx, .pos = new_pos };
                    siftDown(HeapEntry, heap.items, 0, ctx);
                } else {
                    // Remove this chunk from heap
                    heap.items[0] = heap.items[heap.items.len - 1];
                    _ = heap.pop();
                    if (heap.items.len > 0) {
                        siftDown(HeapEntry, heap.items, 0, ctx);
                    }
                }
            }

            return ArgsortResult{
                .indices = result,
                .allocator = allocator,
            };
        }

        /// Build min-heap from array
        fn heapify(comptime E: type, items: []E, ctx: anytype) void {
            if (items.len <= 1) return;
            var i = items.len / 2;
            while (i > 0) {
                i -= 1;
                siftDown(E, items, i, ctx);
            }
        }

        /// Sift down element at position i
        fn siftDown(comptime E: type, items: []E, i: usize, ctx: anytype) void {
            var idx = i;
            while (true) {
                var smallest = idx;
                const left = 2 * idx + 1;
                const right = 2 * idx + 2;

                if (left < items.len and ctx.lessThan(items[left], items[smallest])) {
                    smallest = left;
                }
                if (right < items.len and ctx.lessThan(items[right], items[smallest])) {
                    smallest = right;
                }

                if (smallest == idx) break;

                std.mem.swap(E, &items[idx], &items[smallest]);
                idx = smallest;
            }
        }

        /// Sort the column in place, returning a new column with sorted values
        pub fn sort(col: *ChunkedColumn(T), allocator: Allocator) !*ChunkedColumn(T) {
            var indices = try argsort(col, allocator);
            defer indices.deinit();

            // Gather values by sorted indices
            const sorted_data = try allocator.alloc(T, col.total_length);
            defer allocator.free(sorted_data);

            for (indices.indices, 0..) |idx, i| {
                sorted_data[i] = col.get(idx).?;
            }

            return ChunkedColumn(T).createFromSlice(allocator, sorted_data);
        }
    };
}

// Pre-instantiated ops for common types
pub const OpsF64 = ChunkedOps(f64);
pub const OpsF32 = ChunkedOps(f32);

// ============================================================================
// Type Aliases
// ============================================================================

pub const ChunkedColumnF64 = ChunkedColumn(f64);
pub const ChunkedColumnF32 = ChunkedColumn(f32);
pub const ChunkedColumnI64 = ChunkedColumn(i64);
pub const ChunkedColumnI32 = ChunkedColumn(i32);
pub const ChunkedColumnU64 = ChunkedColumn(u64);
pub const ChunkedColumnU32 = ChunkedColumn(u32);
pub const ChunkedColumnBool = ChunkedColumn(bool);

// ============================================================================
// Tests
// ============================================================================

test "ChunkedColumn - create from empty slice" {
    const allocator = std.testing.allocator;
    const data: []const f64 = &[_]f64{};

    const col = try ChunkedColumnF64.createFromSlice(allocator, data);
    defer col.destroy();

    try std.testing.expectEqual(@as(usize, 0), col.total_length);
    try std.testing.expectEqual(@as(usize, 0), col.num_chunks);
}

test "ChunkedColumn - create from small slice" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    try std.testing.expectEqual(@as(usize, 5), col.total_length);
    try std.testing.expectEqual(@as(usize, 1), col.num_chunks); // All fits in one chunk
    try std.testing.expectEqual(@as(usize, 5), col.chunk_sizes[0]);

    // Verify data
    try std.testing.expectEqual(@as(f64, 1.0), col.get(0).?);
    try std.testing.expectEqual(@as(f64, 5.0), col.get(4).?);
    try std.testing.expectEqual(@as(?f64, null), col.get(5)); // Out of bounds
}

test "ChunkedColumn - create spanning multiple chunks" {
    const allocator = std.testing.allocator;

    // Create data larger than one chunk
    const size = CHUNK_SIZE + 100;
    const data = try allocator.alloc(f64, size);
    defer allocator.free(data);

    for (0..size) |i| {
        data[i] = @floatFromInt(i);
    }

    const col = try ChunkedColumnF64.createFromSlice(allocator, data);
    defer col.destroy();

    try std.testing.expectEqual(size, col.total_length);
    try std.testing.expectEqual(@as(usize, 2), col.num_chunks);
    try std.testing.expectEqual(CHUNK_SIZE, col.chunk_sizes[0]);
    try std.testing.expectEqual(@as(usize, 100), col.chunk_sizes[1]);

    // Verify data across chunk boundary
    try std.testing.expectEqual(@as(f64, 0.0), col.get(0).?);
    try std.testing.expectEqual(@as(f64, @floatFromInt(CHUNK_SIZE - 1)), col.get(CHUNK_SIZE - 1).?);
    try std.testing.expectEqual(@as(f64, @floatFromInt(CHUNK_SIZE)), col.get(CHUNK_SIZE).?);
    try std.testing.expectEqual(@as(f64, @floatFromInt(size - 1)), col.get(size - 1).?);
}

test "ChunkedColumn - set element" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    try std.testing.expect(col.set(1, 42.0));
    try std.testing.expectEqual(@as(f64, 42.0), col.get(1).?);

    try std.testing.expect(!col.set(100, 0.0)); // Out of bounds
}

test "ChunkedColumn - iterator" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    var iter = col.iterator();
    var sum: f64 = 0;
    var count: usize = 0;

    while (iter.next()) |val| {
        sum += val;
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 5), count);
    try std.testing.expectEqual(@as(f64, 15.0), sum);
}

test "ChunkedColumn - copyToSlice" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    var dest: [5]f64 = undefined;
    col.copyToSlice(&dest);

    try std.testing.expectEqualSlices(f64, &data, &dest);
}

test "ChunkedColumn - scratch space allocation" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    // Ensure scratch is created
    const scratch = try col.ensureScratch();
    try std.testing.expect(scratch.sort_keys == null); // Lazy - not allocated yet

    // Allocate sort buffers
    try scratch.ensureSortBuffers(col.chunk_sizes);
    try std.testing.expect(scratch.sort_keys != null);
    try std.testing.expectEqual(@as(usize, 1), scratch.sort_keys.?.len);
}

test "ChunkedColumn - getChunk" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const chunk = col.getChunk(0).?;
    try std.testing.expectEqual(@as(usize, 5), chunk.len);
    try std.testing.expectEqual(@as(f64, 1.0), chunk[0]);
    try std.testing.expectEqual(@as(f64, 5.0), chunk[4]);

    try std.testing.expectEqual(@as(?[]const f64, null), col.getChunk(1)); // No second chunk
}

test "ChunkedOps - sum small data" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const result = OpsF64.sum(col);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), result, 0.001);
}

test "ChunkedOps - sum large data across chunks" {
    const allocator = std.testing.allocator;

    // Create data larger than one chunk
    const size = CHUNK_SIZE * 2 + 100;
    const data = try allocator.alloc(f64, size);
    defer allocator.free(data);

    var expected_sum: f64 = 0;
    for (0..size) |i| {
        data[i] = @floatFromInt(i % 100);
        expected_sum += data[i];
    }

    const col = try ChunkedColumnF64.createFromSlice(allocator, data);
    defer col.destroy();

    try std.testing.expectEqual(@as(usize, 3), col.num_chunks);

    const result = OpsF64.sum(col);
    try std.testing.expectApproxEqAbs(expected_sum, result, 0.01);
}

test "ChunkedOps - min/max" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0, 3.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    try std.testing.expectEqual(@as(f64, 1.0), OpsF64.min(col).?);
    try std.testing.expectEqual(@as(f64, 9.0), OpsF64.max(col).?);
}

test "ChunkedOps - mean" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 2.0, 4.0, 6.0, 8.0, 10.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    try std.testing.expectApproxEqAbs(@as(f64, 6.0), OpsF64.mean(col).?, 0.001);
}

test "ChunkedOps - empty column" {
    const allocator = std.testing.allocator;
    const data: []const f64 = &[_]f64{};

    const col = try ChunkedColumnF64.createFromSlice(allocator, data);
    defer col.destroy();

    try std.testing.expectEqual(@as(f64, 0.0), OpsF64.sum(col));
    try std.testing.expectEqual(@as(?f64, null), OpsF64.min(col));
    try std.testing.expectEqual(@as(?f64, null), OpsF64.max(col));
    try std.testing.expectEqual(@as(?f64, null), OpsF64.mean(col));
}

test "ChunkedOps - filterGt" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 5.0, 2.0, 8.0, 3.0, 9.0, 4.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const filtered = try OpsF64.filterGt(col, allocator, 4.0);
    defer filtered.destroy();

    try std.testing.expectEqual(@as(usize, 3), filtered.total_length);
    try std.testing.expectEqual(@as(f64, 5.0), filtered.get(0).?);
    try std.testing.expectEqual(@as(f64, 8.0), filtered.get(1).?);
    try std.testing.expectEqual(@as(f64, 9.0), filtered.get(2).?);
}

test "ChunkedOps - filterLt" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 5.0, 2.0, 8.0, 3.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const filtered = try OpsF64.filterLt(col, allocator, 4.0);
    defer filtered.destroy();

    try std.testing.expectEqual(@as(usize, 3), filtered.total_length);
    try std.testing.expectEqual(@as(f64, 1.0), filtered.get(0).?);
    try std.testing.expectEqual(@as(f64, 2.0), filtered.get(1).?);
    try std.testing.expectEqual(@as(f64, 3.0), filtered.get(2).?);
}

test "ChunkedOps - filter with predicate" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    // Filter even numbers (val % 2 == 0)
    const filtered = try OpsF64.filter(col, allocator, struct {
        fn isEven(val: f64) bool {
            return @rem(val, 2.0) == 0.0;
        }
    }.isEven);
    defer filtered.destroy();

    try std.testing.expectEqual(@as(usize, 3), filtered.total_length);
    try std.testing.expectEqual(@as(f64, 2.0), filtered.get(0).?);
    try std.testing.expectEqual(@as(f64, 4.0), filtered.get(1).?);
    try std.testing.expectEqual(@as(f64, 6.0), filtered.get(2).?);
}

test "ChunkedOps - filter no matches" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const filtered = try OpsF64.filterGt(col, allocator, 100.0);
    defer filtered.destroy();

    try std.testing.expectEqual(@as(usize, 0), filtered.total_length);
}

test "ChunkedOps - argsort small" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    var result = try OpsF64.argsort(col, allocator);
    defer result.deinit();

    // Expected order: indices pointing to [1.0, 2.0, 5.0, 8.0, 9.0]
    // Original: [5.0, 2.0, 8.0, 1.0, 9.0]
    // Sorted indices: [3, 1, 0, 2, 4]
    try std.testing.expectEqual(@as(usize, 5), result.indices.len);
    try std.testing.expectEqual(@as(u32, 3), result.indices[0]); // 1.0 at index 3
    try std.testing.expectEqual(@as(u32, 1), result.indices[1]); // 2.0 at index 1
    try std.testing.expectEqual(@as(u32, 0), result.indices[2]); // 5.0 at index 0
    try std.testing.expectEqual(@as(u32, 2), result.indices[3]); // 8.0 at index 2
    try std.testing.expectEqual(@as(u32, 4), result.indices[4]); // 9.0 at index 4
}

test "ChunkedOps - sort small" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const sorted = try OpsF64.sort(col, allocator);
    defer sorted.destroy();

    try std.testing.expectEqual(@as(usize, 5), sorted.total_length);
    try std.testing.expectEqual(@as(f64, 1.0), sorted.get(0).?);
    try std.testing.expectEqual(@as(f64, 2.0), sorted.get(1).?);
    try std.testing.expectEqual(@as(f64, 5.0), sorted.get(2).?);
    try std.testing.expectEqual(@as(f64, 8.0), sorted.get(3).?);
    try std.testing.expectEqual(@as(f64, 9.0), sorted.get(4).?);
}

test "ChunkedOps - sort empty" {
    const allocator = std.testing.allocator;
    const data: []const f64 = &[_]f64{};

    const col = try ChunkedColumnF64.createFromSlice(allocator, data);
    defer col.destroy();

    const sorted = try OpsF64.sort(col, allocator);
    defer sorted.destroy();

    try std.testing.expectEqual(@as(usize, 0), sorted.total_length);
}

test "ChunkedOps - sort already sorted" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const sorted = try OpsF64.sort(col, allocator);
    defer sorted.destroy();

    try std.testing.expectEqual(@as(usize, 5), sorted.total_length);
    for (0..5) |i| {
        try std.testing.expectEqual(@as(f64, @floatFromInt(i + 1)), sorted.get(i).?);
    }
}

test "ChunkedOps - sort reverse sorted" {
    const allocator = std.testing.allocator;
    const data = [_]f64{ 5.0, 4.0, 3.0, 2.0, 1.0 };

    const col = try ChunkedColumnF64.createFromSlice(allocator, &data);
    defer col.destroy();

    const sorted = try OpsF64.sort(col, allocator);
    defer sorted.destroy();

    try std.testing.expectEqual(@as(usize, 5), sorted.total_length);
    for (0..5) |i| {
        try std.testing.expectEqual(@as(f64, @floatFromInt(i + 1)), sorted.get(i).?);
    }
}
