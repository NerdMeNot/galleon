const std = @import("std");

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

/// Fast integer hash using multiply-shift
/// Faster than rapidHash64 for simple integer keys
/// Uses a prime multiplier for good bit mixing
pub inline fn fastIntHash(val: i64) u64 {
    const x = @as(u64, @bitCast(val));
    // Multiply by golden ratio prime, then mix high/low bits
    const h = x *% 0x9E3779B97F4A7C15;
    return h ^ (h >> 32);
}

// ============================================================================
// Column Hashing Functions
// ============================================================================

/// Hash int64 column for groupby/join using rapidhash
/// Outputs hash values that can be used for grouping
pub fn hashInt64Column(data: []const i64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    // Process 4 at a time for better ILP
    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash64(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash64(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash64(@bitCast(data[i + 3]));
    }

    // Handle remainder
    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
    }
}

/// Hash int32 column
pub fn hashInt32Column(data: []const i32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash32(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash32(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash32(@bitCast(data[i + 3]));
    }

    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
    }
}

/// Hash float64 column
pub fn hashFloat64Column(data: []const f64, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash64(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash64(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash64(@bitCast(data[i + 3]));
    }

    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash64(@bitCast(data[i]));
    }
}

/// Hash float32 column
pub fn hashFloat32Column(data: []const f32, out_hashes: []u64) void {
    const len = @min(data.len, out_hashes.len);

    const unrolled = len - (len % 4);
    var i: usize = 0;

    while (i < unrolled) : (i += 4) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
        out_hashes[i + 1] = rapidHash32(@bitCast(data[i + 1]));
        out_hashes[i + 2] = rapidHash32(@bitCast(data[i + 2]));
        out_hashes[i + 3] = rapidHash32(@bitCast(data[i + 3]));
    }

    while (i < len) : (i += 1) {
        out_hashes[i] = rapidHash32(@bitCast(data[i]));
    }
}

// ============================================================================
// Hash Combination Functions
// ============================================================================

/// Combine two hash columns (for multi-key groupby/join) using rapidhash mixing
pub fn combineHashes(hash1: []const u64, hash2: []const u64, out_hashes: []u64) void {
    const len = @min(@min(hash1.len, hash2.len), out_hashes.len);

    var i: usize = 0;
    while (i < len) : (i += 1) {
        // Combine using rapidhash mixing
        out_hashes[i] = rapidMix(hash1[i], hash2[i]);
    }
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
