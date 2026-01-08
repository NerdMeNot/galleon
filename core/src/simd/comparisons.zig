const std = @import("std");
const core = @import("core.zig");

const VECTOR_WIDTH = core.VECTOR_WIDTH;
const CHUNK_SIZE = core.CHUNK_SIZE;

// ============================================================================
// Array Comparison Operations (a cmp b -> u8 mask)
// ============================================================================

/// Compare two arrays: out[i] = 1 if a[i] > b[i], else 0
pub fn cmpGt(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const m0: BoolVec = a0 > b0;
        const m1: BoolVec = a1 > b1;
        const m2: BoolVec = a2 > b2;
        const m3: BoolVec = a3 > b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av > bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] > b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] >= b[i], else 0
pub fn cmpGe(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const m0: BoolVec = a0 >= b0;
        const m1: BoolVec = a1 >= b1;
        const m2: BoolVec = a2 >= b2;
        const m3: BoolVec = a3 >= b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av >= bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] >= b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] < b[i], else 0
pub fn cmpLt(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const m0: BoolVec = a0 < b0;
        const m1: BoolVec = a1 < b1;
        const m2: BoolVec = a2 < b2;
        const m3: BoolVec = a3 < b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av < bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] < b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] <= b[i], else 0
pub fn cmpLe(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const m0: BoolVec = a0 <= b0;
        const m1: BoolVec = a1 <= b1;
        const m2: BoolVec = a2 <= b2;
        const m3: BoolVec = a3 <= b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av <= bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] <= b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] == b[i], else 0
pub fn cmpEq(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const m0: BoolVec = a0 == b0;
        const m1: BoolVec = a1 == b1;
        const m2: BoolVec = a2 == b2;
        const m3: BoolVec = a3 == b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av == bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] == b[i]) 1 else 0;
    }
}

/// Compare two arrays: out[i] = 1 if a[i] != b[i], else 0
pub fn cmpNe(comptime T: type, a: []const T, b: []const T, out: []u8) void {
    const len = @min(@min(a.len, b.len), out.len);
    if (len == 0) return;

    const Vec = @Vector(VECTOR_WIDTH, T);
    const BoolVec = @Vector(VECTOR_WIDTH, bool);

    const unrolled_len = len - (len % CHUNK_SIZE);
    var i: usize = 0;

    while (i < unrolled_len) : (i += CHUNK_SIZE) {
        const a0: Vec = a[i..][0..VECTOR_WIDTH].*;
        const a1: Vec = a[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const a2: Vec = a[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const a3: Vec = a[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const b0: Vec = b[i..][0..VECTOR_WIDTH].*;
        const b1: Vec = b[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].*;
        const b2: Vec = b[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].*;
        const b3: Vec = b[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].*;

        const m0: BoolVec = a0 != b0;
        const m1: BoolVec = a1 != b1;
        const m2: BoolVec = a2 != b2;
        const m3: BoolVec = a3 != b3;

        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m0);
        out[i + VECTOR_WIDTH ..][0..VECTOR_WIDTH].* = @intFromBool(m1);
        out[i + VECTOR_WIDTH * 2 ..][0..VECTOR_WIDTH].* = @intFromBool(m2);
        out[i + VECTOR_WIDTH * 3 ..][0..VECTOR_WIDTH].* = @intFromBool(m3);
    }

    const aligned_len = len - (len % VECTOR_WIDTH);
    while (i < aligned_len) : (i += VECTOR_WIDTH) {
        const av: Vec = a[i..][0..VECTOR_WIDTH].*;
        const bv: Vec = b[i..][0..VECTOR_WIDTH].*;
        const m: BoolVec = av != bv;
        out[i..][0..VECTOR_WIDTH].* = @intFromBool(m);
    }

    while (i < len) : (i += 1) {
        out[i] = if (a[i] != b[i]) 1 else 0;
    }
}

// ============================================================================
// Tests
// ============================================================================

test "comparison - cmpGt f64 - basic" {
    const a = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0, 8.0 };
    const b = [_]f64{ 2.0, 4.0, 3.0, 6.0, 3.0, 8.0 };
    var out: [6]u8 = undefined;
    cmpGt(f64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 0), out[0]);
    try std.testing.expectEqual(@as(u8, 1), out[1]);
    try std.testing.expectEqual(@as(u8, 0), out[2]);
    try std.testing.expectEqual(@as(u8, 1), out[3]);
    try std.testing.expectEqual(@as(u8, 0), out[4]);
    try std.testing.expectEqual(@as(u8, 0), out[5]);
}

test "comparison - cmpGt i64 - basic" {
    const a = [_]i64{ 1, 5, 3, 7, -1, 0 };
    const b = [_]i64{ 2, 4, 3, 6, 0, -1 };
    var out: [6]u8 = undefined;
    cmpGt(i64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 0), out[0]);
    try std.testing.expectEqual(@as(u8, 1), out[1]);
    try std.testing.expectEqual(@as(u8, 0), out[2]);
    try std.testing.expectEqual(@as(u8, 1), out[3]);
    try std.testing.expectEqual(@as(u8, 0), out[4]);
    try std.testing.expectEqual(@as(u8, 1), out[5]);
}

test "comparison - cmpGe f64 - basic" {
    const a = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0 };
    const b = [_]f64{ 2.0, 4.0, 3.0, 6.0, 3.0 };
    var out: [5]u8 = undefined;
    cmpGe(f64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 0), out[0]);
    try std.testing.expectEqual(@as(u8, 1), out[1]);
    try std.testing.expectEqual(@as(u8, 1), out[2]);
    try std.testing.expectEqual(@as(u8, 1), out[3]);
    try std.testing.expectEqual(@as(u8, 0), out[4]);
}

test "comparison - cmpLt f64 - basic" {
    const a = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0 };
    const b = [_]f64{ 2.0, 4.0, 3.0, 6.0, 3.0 };
    var out: [5]u8 = undefined;
    cmpLt(f64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 1), out[0]);
    try std.testing.expectEqual(@as(u8, 0), out[1]);
    try std.testing.expectEqual(@as(u8, 0), out[2]);
    try std.testing.expectEqual(@as(u8, 0), out[3]);
    try std.testing.expectEqual(@as(u8, 1), out[4]);
}

test "comparison - cmpLe f64 - basic" {
    const a = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0 };
    const b = [_]f64{ 2.0, 4.0, 3.0, 6.0, 3.0 };
    var out: [5]u8 = undefined;
    cmpLe(f64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 1), out[0]);
    try std.testing.expectEqual(@as(u8, 0), out[1]);
    try std.testing.expectEqual(@as(u8, 1), out[2]);
    try std.testing.expectEqual(@as(u8, 0), out[3]);
    try std.testing.expectEqual(@as(u8, 1), out[4]);
}

test "comparison - cmpEq f64 - basic" {
    const a = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0 };
    const b = [_]f64{ 1.0, 4.0, 3.0, 6.0, 2.0 };
    var out: [5]u8 = undefined;
    cmpEq(f64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 1), out[0]);
    try std.testing.expectEqual(@as(u8, 0), out[1]);
    try std.testing.expectEqual(@as(u8, 1), out[2]);
    try std.testing.expectEqual(@as(u8, 0), out[3]);
    try std.testing.expectEqual(@as(u8, 1), out[4]);
}

test "comparison - cmpNe f64 - basic" {
    const a = [_]f64{ 1.0, 5.0, 3.0, 7.0, 2.0 };
    const b = [_]f64{ 1.0, 4.0, 3.0, 6.0, 2.0 };
    var out: [5]u8 = undefined;
    cmpNe(f64, &a, &b, &out);
    try std.testing.expectEqual(@as(u8, 0), out[0]);
    try std.testing.expectEqual(@as(u8, 1), out[1]);
    try std.testing.expectEqual(@as(u8, 0), out[2]);
    try std.testing.expectEqual(@as(u8, 1), out[3]);
    try std.testing.expectEqual(@as(u8, 0), out[4]);
}

test "comparison - cmpGt f64 - empty array" {
    const a = [_]f64{};
    const b = [_]f64{};
    var out: [0]u8 = undefined;
    cmpGt(f64, &a, &b, &out);
}

test "comparison - cmpGt f64 - large array SIMD path" {
    var a: [100]f64 = undefined;
    var b: [100]f64 = undefined;
    var out: [100]u8 = undefined;

    for (0..100) |i| {
        a[i] = @floatFromInt(i);
        b[i] = 50.0;
    }

    cmpGt(f64, &a, &b, &out);

    for (0..100) |i| {
        const expected: u8 = if (i > 50) 1 else 0;
        try std.testing.expectEqual(expected, out[i]);
    }
}

test "comparison - cmpEq i64 - large array SIMD path" {
    var a: [100]i64 = undefined;
    var b: [100]i64 = undefined;
    var out: [100]u8 = undefined;

    for (0..100) |i| {
        a[i] = @intCast(i % 10);
        b[i] = 5;
    }

    cmpEq(i64, &a, &b, &out);

    for (0..100) |i| {
        const expected: u8 = if (i % 10 == 5) 1 else 0;
        try std.testing.expectEqual(expected, out[i]);
    }
}
