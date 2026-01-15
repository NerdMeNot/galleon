const std = @import("std");
const blitz = @import("../blitz/mod.zig");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Rapidhash - Fast, high-quality hash function (evolution of wyhash)
// ============================================================================

/// Rapidhash secrets - carefully chosen constants for good mixing
pub const RAPID_SECRET0: u64 = 0x2d358dccaa6c78a5;
pub const RAPID_SECRET1: u64 = 0x8bb84b93962eacc9;
pub const RAPID_SECRET2: u64 = 0x4b33a62ed433d4a3;

/// Core mixing function: 64x64->128 multiply, return xor of high and low
pub inline fn rapidMix(a: u64, b: u64) u64 {
    const full: u128 = @as(u128, a) *% @as(u128, b);
    return @as(u64, @truncate(full)) ^ @as(u64, @truncate(full >> 64));
}

/// Hash a single 64-bit value using rapidhash
pub inline fn rapidHash64(val: u64) u64 {
    // Mix with secrets and combine
    const a = val ^ RAPID_SECRET0;
    const b = val ^ RAPID_SECRET1;
    return rapidMix(a, b) ^ RAPID_SECRET2;
}

/// Hash a single 32-bit value using rapidhash
pub inline fn rapidHash32(val: u32) u64 {
    // Extend to 64-bit and mix
    const extended: u64 = @as(u64, val) | (@as(u64, val) << 32);
    return rapidHash64(extended);
}

/// Fast integer hash using triple multiply-shift mixing
/// Better distribution than single multiply-xorshift for join operations
/// Uses Murmur3-style finalization for excellent bit avalanche
pub inline fn fastIntHash(val: i64) u64 {
    var x = @as(u64, @bitCast(val));
    // Murmur3 64-bit finalization mix
    x ^= x >> 33;
    x *%= 0xFF51AFD7ED558CCD;
    x ^= x >> 33;
    x *%= 0xC4CEB9FE1A85EC53;
    x ^= x >> 33;
    return x;
}

// ============================================================================
// Column Hashing Functions
// ============================================================================

// ============================================================================
// SIMD-Accelerated Hash Functions
// Uses vectorized Murmur3 finalization for high throughput and excellent distribution
// ============================================================================

/// SIMD constants for vectorized hashing (Murmur3 finalization constants)
const HASH_MULT: u64 = 0xFF51AFD7ED558CCD;
const HASH_MIX: u64 = 0xC4CEB9FE1A85EC53;

/// SIMD hash for int64 column using vectorized multiply-xorshift
/// Processes 4 u64 values at a time using SIMD vectors
pub fn hashInt64ColumnSIMD(data: []const i64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    // For 64-bit values, we can process 4 at a time efficiently
    const Vec4 = @Vector(4, u64);
    const mult_vec: Vec4 = @splat(HASH_MULT);
    const mix_vec: Vec4 = @splat(HASH_MIX);

    const aligned_len = len - (len % 4);
    var i: usize = 0;

    // Process 4 elements at a time with SIMD (Murmur3 finalization)
    while (i < aligned_len) : (i += 4) {
        // Load 4 values (reinterpret i64 as u64)
        var vals: Vec4 = @bitCast(data[i..][0..4].*);

        // Murmur3 64-bit finalization mix (3 rounds)
        vals ^= vals >> @splat(33);
        vals *%= mult_vec;
        vals ^= vals >> @splat(33);
        vals *%= mix_vec;
        vals ^= vals >> @splat(33);

        // Store result
        out_hashes[i..][0..4].* = vals;
    }

    // Handle remainder with same mixing
    while (i < len) : (i += 1) {
        var h: u64 = @bitCast(data[i]);
        h ^= h >> 33;
        h *%= HASH_MULT;
        h ^= h >> 33;
        h *%= HASH_MIX;
        out_hashes[i] = h ^ (h >> 33);
    }
}

/// SIMD hash for float64 column
pub fn hashFloat64ColumnSIMD(data: []const f64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    const Vec4 = @Vector(4, u64);
    const mult_vec: Vec4 = @splat(HASH_MULT);
    const mix_vec: Vec4 = @splat(HASH_MIX);

    const aligned_len = len - (len % 4);
    var i: usize = 0;

    while (i < aligned_len) : (i += 4) {
        // Load and bitcast f64 to u64
        const vals: Vec4 = @bitCast(data[i..][0..4].*);

        const h1 = vals *% mult_vec;
        const h2 = h1 ^ (h1 >> @splat(32));
        const h3 = h2 *% mix_vec;
        const result = h3 ^ (h3 >> @splat(32));

        out_hashes[i..][0..4].* = result;
    }

    while (i < len) : (i += 1) {
        const val: u64 = @bitCast(data[i]);
        var h = val *% HASH_MULT;
        h ^= h >> 32;
        h *%= HASH_MIX;
        out_hashes[i] = h ^ (h >> 32);
    }
}

/// SIMD hash for int32 column - processes 8 at a time
pub fn hashInt32ColumnSIMD(data: []const i32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    // Process 4 at a time (32->64 bit expansion limits parallelism)
    const Vec4 = @Vector(4, u64);
    const mult_vec: Vec4 = @splat(HASH_MULT);
    const mix_vec: Vec4 = @splat(HASH_MIX);

    const aligned_len = len - (len % 4);
    var i: usize = 0;

    while (i < aligned_len) : (i += 4) {
        // Load and expand i32 to u64
        const vals_i32: @Vector(4, i32) = data[i..][0..4].*;
        const vals_u32: @Vector(4, u32) = @bitCast(vals_i32);
        const vals: Vec4 = vals_u32;

        const h1 = vals *% mult_vec;
        const h2 = h1 ^ (h1 >> @splat(32));
        const h3 = h2 *% mix_vec;
        const result = h3 ^ (h3 >> @splat(32));

        out_hashes[i..][0..4].* = result;
    }

    while (i < len) : (i += 1) {
        const val: u64 = @as(u32, @bitCast(data[i]));
        var h = val *% HASH_MULT;
        h ^= h >> 32;
        h *%= HASH_MIX;
        out_hashes[i] = h ^ (h >> 32);
    }
}

/// SIMD hash for float32 column
pub fn hashFloat32ColumnSIMD(data: []const f32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    const Vec4 = @Vector(4, u64);
    const mult_vec: Vec4 = @splat(HASH_MULT);
    const mix_vec: Vec4 = @splat(HASH_MIX);

    const aligned_len = len - (len % 4);
    var i: usize = 0;

    while (i < aligned_len) : (i += 4) {
        const vals_f32: @Vector(4, f32) = data[i..][0..4].*;
        const vals_u32: @Vector(4, u32) = @bitCast(vals_f32);
        const vals: Vec4 = vals_u32;

        const h1 = vals *% mult_vec;
        const h2 = h1 ^ (h1 >> @splat(32));
        const h3 = h2 *% mix_vec;
        const result = h3 ^ (h3 >> @splat(32));

        out_hashes[i..][0..4].* = result;
    }

    while (i < len) : (i += 1) {
        const val: u64 = @as(u32, @bitCast(data[i]));
        var h = val *% HASH_MULT;
        h ^= h >> 32;
        h *%= HASH_MIX;
        out_hashes[i] = h ^ (h >> 32);
    }
}

/// SIMD combine hashes - processes 4 at a time
pub fn combineHashesSIMD(hash1: []const u64, hash2: []const u64, out_hashes: []u64) void {
    const len = @min(@min(hash1.len, hash2.len), out_hashes.len);
    if (len == 0) return;

    const Vec4 = @Vector(4, u64);
    const mix_vec: Vec4 = @splat(HASH_MIX);

    const aligned_len = len - (len % 4);
    var i: usize = 0;

    while (i < aligned_len) : (i += 4) {
        const h1: Vec4 = hash1[i..][0..4].*;
        const h2: Vec4 = hash2[i..][0..4].*;

        // Combine using XOR and mixing
        const combined = h1 ^ h2;
        const h3 = combined *% mix_vec;
        const result = h3 ^ (h3 >> @splat(32));

        out_hashes[i..][0..4].* = result;
    }

    while (i < len) : (i += 1) {
        const combined = hash1[i] ^ hash2[i];
        const h = combined *% HASH_MIX;
        out_hashes[i] = h ^ (h >> 32);
    }
}

// ============================================================================
// Original Column Hashing Functions (Rapidhash)
// Kept for compatibility and when high-quality hashing is needed
// ============================================================================

/// Hash int64 column for groupby/join using rapidhash
/// Outputs hash values that can be used for grouping
pub fn hashInt64Column(data: []const i64, out_hashes: []u64) void {
    // Use SIMD version by default for better performance
    hashInt64ColumnSIMD(data, out_hashes);
}

/// Hash int32 column
pub fn hashInt32Column(data: []const i32, out_hashes: []u64) void {
    // Use SIMD version by default
    hashInt32ColumnSIMD(data, out_hashes);
}

/// Hash float64 column
pub fn hashFloat64Column(data: []const f64, out_hashes: []u64) void {
    // Use SIMD version by default
    hashFloat64ColumnSIMD(data, out_hashes);
}

/// Hash float32 column
pub fn hashFloat32Column(data: []const f32, out_hashes: []u64) void {
    // Use SIMD version by default
    hashFloat32ColumnSIMD(data, out_hashes);
}

// ============================================================================
// Hash Combination Functions
// ============================================================================

/// Combine two hash columns (for multi-key groupby/join) using SIMD mixing
pub fn combineHashes(hash1: []const u64, hash2: []const u64, out_hashes: []u64) void {
    // Use SIMD version by default
    combineHashesSIMD(hash1, hash2, out_hashes);
}

/// Hash multiple int64 columns together (for multi-key groupby/join)
/// Combines hashes from multiple columns using rapidhash
pub fn hashInt64Columns(columns: []const []const i64, out_hashes: []u64) void {
    if (columns.len == 0) return;

    const len = out_hashes.len;

    for (0..len) |row| {
        var hash: u64 = RAPID_SECRET0;

        for (columns) |col| {
            if (row < col.len) {
                const val: u64 = @bitCast(col[row]);
                // Mix each column value into the running hash
                hash = rapidMix(hash ^ val, RAPID_SECRET1);
            }
        }

        out_hashes[row] = hash ^ RAPID_SECRET2;
    }
}

// ============================================================================
// Parallel Hashing (using Blitz work-stealing)
// ============================================================================

/// Chunk size for parallel hashing (~64KB of i64 values)
const HASH_CHUNK_SIZE: usize = 8192;

/// Parallel hash int64 column using Blitz work-stealing
pub fn parallelHashInt64Column(data: []const i64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    const Context = struct {
        data: []const i64,
        out: []u64,
    };
    const ctx = Context{ .data = data[0..len], .out = out_hashes[0..len] };

    blitz.parallelForWithGrain(
        len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                // Hash this chunk
                const chunk_data = c.data[start..end];
                const chunk_out = c.out[start..end];
                const chunk_len = end - start;

                // Process 4 at a time for ILP
                const unrolled = chunk_len - (chunk_len % 4);
                var i: usize = 0;

                while (i < unrolled) : (i += 4) {
                    chunk_out[i] = rapidHash64(@bitCast(chunk_data[i]));
                    chunk_out[i + 1] = rapidHash64(@bitCast(chunk_data[i + 1]));
                    chunk_out[i + 2] = rapidHash64(@bitCast(chunk_data[i + 2]));
                    chunk_out[i + 3] = rapidHash64(@bitCast(chunk_data[i + 3]));
                }

                while (i < chunk_len) : (i += 1) {
                    chunk_out[i] = rapidHash64(@bitCast(chunk_data[i]));
                }
            }
        }.body,
        HASH_CHUNK_SIZE,
    );
}

/// Parallel hash int32 column using Blitz work-stealing
pub fn parallelHashInt32Column(data: []const i32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    const Context = struct {
        data: []const i32,
        out: []u64,
    };
    const ctx = Context{ .data = data[0..len], .out = out_hashes[0..len] };

    blitz.parallelForWithGrain(
        len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                const chunk_data = c.data[start..end];
                const chunk_out = c.out[start..end];
                const chunk_len = end - start;

                const unrolled = chunk_len - (chunk_len % 4);
                var i: usize = 0;

                while (i < unrolled) : (i += 4) {
                    chunk_out[i] = rapidHash32(@bitCast(chunk_data[i]));
                    chunk_out[i + 1] = rapidHash32(@bitCast(chunk_data[i + 1]));
                    chunk_out[i + 2] = rapidHash32(@bitCast(chunk_data[i + 2]));
                    chunk_out[i + 3] = rapidHash32(@bitCast(chunk_data[i + 3]));
                }

                while (i < chunk_len) : (i += 1) {
                    chunk_out[i] = rapidHash32(@bitCast(chunk_data[i]));
                }
            }
        }.body,
        HASH_CHUNK_SIZE,
    );
}

/// Parallel hash float64 column using Blitz work-stealing
pub fn parallelHashFloat64Column(data: []const f64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    const Context = struct {
        data: []const f64,
        out: []u64,
    };
    const ctx = Context{ .data = data[0..len], .out = out_hashes[0..len] };

    blitz.parallelForWithGrain(
        len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                const chunk_data = c.data[start..end];
                const chunk_out = c.out[start..end];
                const chunk_len = end - start;

                const unrolled = chunk_len - (chunk_len % 4);
                var i: usize = 0;

                while (i < unrolled) : (i += 4) {
                    chunk_out[i] = rapidHash64(@bitCast(chunk_data[i]));
                    chunk_out[i + 1] = rapidHash64(@bitCast(chunk_data[i + 1]));
                    chunk_out[i + 2] = rapidHash64(@bitCast(chunk_data[i + 2]));
                    chunk_out[i + 3] = rapidHash64(@bitCast(chunk_data[i + 3]));
                }

                while (i < chunk_len) : (i += 1) {
                    chunk_out[i] = rapidHash64(@bitCast(chunk_data[i]));
                }
            }
        }.body,
        HASH_CHUNK_SIZE,
    );
}

/// Parallel hash float32 column using Blitz work-stealing
pub fn parallelHashFloat32Column(data: []const f32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);
    if (len == 0) return;

    const Context = struct {
        data: []const f32,
        out: []u64,
    };
    const ctx = Context{ .data = data[0..len], .out = out_hashes[0..len] };

    blitz.parallelForWithGrain(
        len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                const chunk_data = c.data[start..end];
                const chunk_out = c.out[start..end];
                const chunk_len = end - start;

                const unrolled = chunk_len - (chunk_len % 4);
                var i: usize = 0;

                while (i < unrolled) : (i += 4) {
                    chunk_out[i] = rapidHash32(@bitCast(chunk_data[i]));
                    chunk_out[i + 1] = rapidHash32(@bitCast(chunk_data[i + 1]));
                    chunk_out[i + 2] = rapidHash32(@bitCast(chunk_data[i + 2]));
                    chunk_out[i + 3] = rapidHash32(@bitCast(chunk_data[i + 3]));
                }

                while (i < chunk_len) : (i += 1) {
                    chunk_out[i] = rapidHash32(@bitCast(chunk_data[i]));
                }
            }
        }.body,
        HASH_CHUNK_SIZE,
    );
}

/// Parallel combine hashes using Blitz work-stealing
pub fn parallelCombineHashes(hash1: []const u64, hash2: []const u64, out_hashes: []u64) void {
    const len = @min(@min(hash1.len, hash2.len), out_hashes.len);
    if (len == 0) return;

    const Context = struct {
        h1: []const u64,
        h2: []const u64,
        out: []u64,
    };
    const ctx = Context{ .h1 = hash1[0..len], .h2 = hash2[0..len], .out = out_hashes[0..len] };

    blitz.parallelForWithGrain(
        len,
        Context,
        ctx,
        struct {
            fn body(c: Context, start: usize, end: usize) void {
                for (start..end) |i| {
                    c.out[i] = rapidMix(c.h1[i], c.h2[i]);
                }
            }
        }.body,
        HASH_CHUNK_SIZE,
    );
}

// ============================================================================
// Tests
// ============================================================================

test "hashing - rapidMix is deterministic" {
    const a: u64 = 0x123456789ABCDEF0;
    const b: u64 = 0xFEDCBA9876543210;

    const result1 = rapidMix(a, b);
    const result2 = rapidMix(a, b);
    try std.testing.expectEqual(result1, result2);
}

test "hashing - rapidHash64 produces different hashes for different values" {
    const hash1 = rapidHash64(1);
    const hash2 = rapidHash64(2);
    const hash3 = rapidHash64(1000);

    try std.testing.expect(hash1 != hash2);
    try std.testing.expect(hash2 != hash3);
    try std.testing.expect(hash1 != hash3);
}

test "hashing - fastIntHash produces different hashes" {
    const hash1 = fastIntHash(1);
    const hash2 = fastIntHash(2);
    const hash3 = fastIntHash(-1);

    try std.testing.expect(hash1 != hash2);
    try std.testing.expect(hash2 != hash3);
    try std.testing.expect(hash1 != hash3);
}

test "hashing - hashInt64Column" {
    const data = [_]i64{ 100, 200, 300, 400 };
    var hashes: [4]u64 = undefined;

    hashInt64Column(&data, &hashes);

    // Verify all hashes are computed and different
    try std.testing.expect(hashes[0] != hashes[1]);
    try std.testing.expect(hashes[1] != hashes[2]);
    try std.testing.expect(hashes[2] != hashes[3]);

    // Verify determinism
    var hashes2: [4]u64 = undefined;
    hashInt64Column(&data, &hashes2);
    try std.testing.expectEqualSlices(u64, &hashes, &hashes2);
}

test "hashing - hashInt32Column" {
    const data = [_]i32{ 100, 200, 300, 400 };
    var hashes: [4]u64 = undefined;

    hashInt32Column(&data, &hashes);
    try std.testing.expect(hashes[0] != hashes[1]);
    try std.testing.expect(hashes[1] != hashes[2]);
}

test "hashing - hashFloat64Column" {
    const data = [_]f64{ 1.5, 2.5, 3.5, 4.5 };
    var hashes: [4]u64 = undefined;

    hashFloat64Column(&data, &hashes);
    try std.testing.expect(hashes[0] != hashes[1]);
    try std.testing.expect(hashes[1] != hashes[2]);
}

test "hashing - combineHashes" {
    const hash1 = [_]u64{ 0x1111, 0x2222, 0x3333 };
    const hash2 = [_]u64{ 0xAAAA, 0xBBBB, 0xCCCC };
    var combined: [3]u64 = undefined;

    combineHashes(&hash1, &hash2, &combined);

    // Verify combination produces different result than inputs
    try std.testing.expect(combined[0] != hash1[0]);
    try std.testing.expect(combined[0] != hash2[0]);
    // Verify all combined hashes are different
    try std.testing.expect(combined[0] != combined[1]);
    try std.testing.expect(combined[1] != combined[2]);
}

test "hashing - hashInt64Columns multi-key" {
    const col1 = [_]i64{ 1, 2, 3 };
    const col2 = [_]i64{ 10, 20, 30 };
    const columns = [_][]const i64{ &col1, &col2 };
    var hashes: [3]u64 = undefined;

    hashInt64Columns(&columns, &hashes);

    // All hashes should be different for different row combinations
    try std.testing.expect(hashes[0] != hashes[1]);
    try std.testing.expect(hashes[1] != hashes[2]);
}
