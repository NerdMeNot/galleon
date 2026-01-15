# Galleon V2 Architecture: Chunk-Based Columnar Storage

## Design Goals

1. **Cache-Friendly**: All operations work on L2-cache-sized chunks
2. **Unified Parallelism**: Blitz handles ALL parallelism - no exceptions
3. **Zero Allocation Operations**: Scratch space is reused, not reallocated
4. **Simple Mental Model**: Chunk → Process → Combine

---

## Core Data Structure: ChunkedColumn

```zig
/// Configuration
pub const CHUNK_SIZE: usize = 65536;  // 64K elements per chunk
pub const CHUNK_BYTES_F64: usize = CHUNK_SIZE * 8;  // 512KB - fits in L2

/// A column is an array of fixed-size chunks
pub fn ChunkedColumn(comptime T: type) type {
    return struct {
        const Self = @This();

        chunks: [][]align(64) T,      // Array of chunk pointers
        num_chunks: usize,
        total_length: usize,
        last_chunk_len: usize,        // Last chunk may be partial

        // Attached scratch space (allocated once, reused)
        scratch: ?*ColumnScratch(T),

        allocator: Allocator,
    };
}

/// Per-column scratch space for operations
pub fn ColumnScratch(comptime T: type) type {
    return struct {
        // For sorting
        sort_keys: [][]u64,           // One per chunk
        sort_indices: [][]u32,        // One per chunk

        // For filtering
        filter_mask: [][]bool,        // One per chunk

        // General temp buffer
        temp_chunk: []T,              // Single chunk-sized buffer
    };
}
```

---

## Memory Layout

```
ChunkedColumn<f64> with 1M elements:
├── chunks[0]  → [64K f64] (512KB, cache-aligned)
├── chunks[1]  → [64K f64] (512KB, cache-aligned)
├── ...
├── chunks[15] → [16K f64] (128KB, partial chunk)
└── scratch
    ├── sort_keys[16]    → pre-allocated for sorting
    ├── sort_indices[16] → pre-allocated for sorting
    └── temp_chunk       → single 512KB buffer
```

**Key properties:**
- 1M elements = 16 chunks (15 full + 1 partial)
- Each chunk is independently processable
- Scratch space allocated on first use, reused forever

---

## Operation Model

### Pattern: Map-Reduce over Chunks

Every operation follows this pattern:

```zig
/// Generic chunk-parallel operation
pub fn parallelChunkOp(
    column: *ChunkedColumn(T),
    comptime processChunk: fn(chunk: []T, ctx: *Context) ChunkResult,
    comptime combineResults: fn(results: []ChunkResult) FinalResult,
    ctx: *Context,
) FinalResult {
    // Blitz processes chunks in parallel
    const chunk_results = blitz.parallelMap(
        column.chunks,
        processChunk,
        ctx,
    );

    // Combine results (often trivial - sum, min, max, concat)
    return combineResults(chunk_results);
}
```

### Example: Sum

```zig
pub fn sum(column: *ChunkedColumn(f64)) f64 {
    // Process each chunk (parallel via Blitz)
    const chunk_sums = blitz.parallelMap(
        column.chunks,
        struct {
            fn sumChunk(chunk: []f64) f64 {
                return simd.sum(chunk);  // SIMD sum of 64K elements
            }
        }.sumChunk,
    );

    // Combine: sum the chunk sums (trivial, single-threaded)
    var total: f64 = 0;
    for (chunk_sums) |cs| total += cs;
    return total;
}
```

### Example: Sort (the hard one)

```zig
pub fn argsort(column: *ChunkedColumn(f64)) []u32 {
    // Ensure scratch space is allocated
    column.ensureScratch();

    // Phase 1: Sort each chunk independently (parallel)
    blitz.parallelFor(column.num_chunks, struct {
        fn sortChunk(column: *ChunkedColumn(f64), chunk_idx: usize) void {
            const chunk = column.chunks[chunk_idx];
            const keys = column.scratch.sort_keys[chunk_idx];
            const indices = column.scratch.sort_indices[chunk_idx];

            // Convert to sortable keys (in pre-allocated buffer)
            for (chunk, 0..) |val, i| {
                keys[i] = floatToSortable(val);
                indices[i] = @intCast(i);
            }

            // Radix sort this chunk (512KB, fits in L2)
            radixSortInPlace(keys, indices);
        }
    }.sortChunk, column);

    // Phase 2: K-way merge of sorted chunks
    // Uses tournament tree, can also be parallelized
    return kWayMerge(column);
}
```

**Why this is fast:**
1. Each chunk's sort is entirely within L2 cache (512KB)
2. Scratch buffers are pre-allocated (no malloc during sort)
3. Chunks are sorted in parallel via Blitz
4. Only the merge phase touches multiple chunks

---

## Blitz Integration

### Principle: Blitz is the ONLY parallelism mechanism

```zig
// In blitz/mod.zig

/// Process chunks in parallel, return array of results
pub fn parallelMap(
    comptime T: type,
    comptime R: type,
    chunks: [][]T,
    comptime process: fn([]T) R,
) []R {
    const results = allocator.alloc(R, chunks.len);

    parallelFor(chunks.len, struct {
        fn work(idx: usize) void {
            results[idx] = process(chunks[idx]);
        }
    }.work);

    return results;
}

/// Parallel for with automatic threshold
pub fn parallelFor(
    count: usize,
    comptime Context: type,
    ctx: Context,
    comptime body: fn(Context, usize, usize) void,
) void {
    // Automatic threshold: only parallelize if worth it
    if (count < MIN_PARALLEL_CHUNKS or num_workers <= 1) {
        body(ctx, 0, count);  // Sequential
        return;
    }

    // Work-stealing parallel execution
    // ... existing Blitz implementation ...
}
```

### Operations using Blitz

| Operation | Parallelism Pattern |
|-----------|---------------------|
| Sum/Min/Max/Mean | Map chunks → Reduce results |
| Filter | Map chunks → Concatenate results |
| Sort | Map (sort chunks) → Merge |
| GroupBy | Map (local hash tables) → Merge tables |
| Join | Build (parallel) → Probe (parallel) |

---

## Buffer Management

### Arena Allocator for Operations

```zig
/// Per-operation arena - cleared after each high-level operation
pub const OperationArena = struct {
    buffer: []u8,
    offset: usize,

    pub fn alloc(self: *OperationArena, comptime T: type, n: usize) []T {
        // Bump allocation - very fast
        const bytes = n * @sizeOf(T);
        const result = self.buffer[self.offset..][0..bytes];
        self.offset += bytes;
        return @ptrCast(result);
    }

    pub fn reset(self: *OperationArena) void {
        self.offset = 0;  // "Free" everything instantly
    }
};
```

### Usage Pattern

```zig
// High-level operation (e.g., DataFrame.sort())
pub fn sortDataFrame(df: *DataFrame, column: []const u8) *DataFrame {
    // Get thread-local arena
    const arena = blitz.getArena();
    defer arena.reset();  // Free all temp allocations at once

    // All intermediate allocations use arena
    const indices = arena.alloc(u32, df.height);
    // ... sort logic ...

    return result;
}
```

---

## Migration Path

### Phase 1: Core Infrastructure
1. Implement `ChunkedColumn(T)` in Zig
2. Add `ColumnScratch(T)` for reusable buffers
3. Update Blitz with `parallelMap` for chunks

### Phase 2: Migrate Operations
1. Sum/Min/Max/Mean - simplest, good test
2. Filter - moderate complexity
3. Sort - most complex, biggest payoff
4. GroupBy/Join - after sort works

### Phase 3: Go Integration
1. Update CGO bindings for chunked columns
2. Maintain backward compatibility where possible
3. Update Series/DataFrame to use new storage

### Phase 4: Cleanup
1. Remove old monolithic column code
2. Remove non-Blitz parallelism
3. Optimize threshold constants

---

## Expected Performance Improvements

| Operation | Current | Expected | Reason |
|-----------|---------|----------|--------|
| Sort 1M | 29ms | 5-8ms | Cache-warm chunks + parallel |
| Filter 1M | 0.3ms | 0.2ms | Already good, minor gains |
| Sum 1M | 0.1ms | 0.1ms | Already optimal |
| GroupBy 1M | 6ms | 4ms | Better cache utilization |

---

## Open Questions

1. **Chunk size**: 64K elements (512KB for f64) seems right for L2, but should benchmark
2. **Scratch allocation**: Lazy vs eager? Currently proposing lazy (first use)
3. **String handling**: Keep strings in Go or move to chunked Zig storage?
4. **Null handling**: Validity bitmap per chunk or global?

---

## Summary

The key insight is: **operations should never touch more memory than fits in L2 cache**.

By chunking data and processing chunk-by-chunk:
- Every operation is cache-friendly
- Parallelism is natural (chunks are independent)
- Scratch space is bounded and reusable
- Blitz handles all parallelism uniformly

This is how Polars/Arrow achieve their performance. Galleon V2 adopts the same principles.
