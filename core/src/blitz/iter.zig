//! Parallel Iterators for Blitz
//!
//! Provides Rayon-style parallel iterators with composable operations.
//! Unlike Rayon's trait-based approach, we use Zig's comptime for zero-cost abstractions.
//!
//! Usage:
//! ```zig
//! const blitz = @import("blitz");
//!
//! // Parallel sum
//! const sum = blitz.iter(i64, data).sum();
//!
//! // Parallel map and collect
//! var result = try blitz.iter(i64, data)
//!     .map(square)
//!     .collect(allocator);
//! ```

const std = @import("std");
const api = @import("api.zig");
const simd = @import("simd.zig");

/// Create a parallel iterator from a slice.
pub fn iter(comptime T: type, data: []const T) ParIter(T) {
    return ParIter(T).init(data);
}

/// Create a mutable parallel iterator from a slice.
pub fn iterMut(comptime T: type, data: []T) ParIterMut(T) {
    return ParIterMut(T).init(data);
}

/// Create a parallel iterator over a range [start, end).
pub fn range(start: usize, end: usize) RangeIter {
    return RangeIter{ .start = start, .end = end };
}

/// Get the number of worker threads.
fn getWorkerCount() usize {
    return @intCast(api.numWorkers());
}

/// Parallel iterator over a slice.
pub fn ParIter(comptime T: type) type {
    return struct {
        const Self = @This();

        data: []const T,

        pub fn init(data: []const T) Self {
            return Self{ .data = data };
        }

        /// Reduce all elements to a single value using parallel reduction.
        pub fn reduce(self: Self, identity: T, comptime reducer: fn (T, T) T) T {
            const Context = struct { slice: []const T };
            const ctx = Context{ .slice = self.data };

            return api.parallelReduce(
                T,
                self.data.len,
                identity,
                Context,
                ctx,
                struct {
                    fn mapFn(c: Context, i: usize) T {
                        return c.slice[i];
                    }
                }.mapFn,
                reducer,
            );
        }

        /// Sum all elements using SIMD-optimized parallel reduction.
        pub fn sum(self: Self) T {
            return simd.parallelSum(T, self.data);
        }

        /// Find the minimum element using SIMD-optimized parallel reduction.
        pub fn min(self: Self) ?T {
            return simd.parallelMin(T, self.data);
        }

        /// Find the maximum element using SIMD-optimized parallel reduction.
        pub fn max(self: Self) ?T {
            return simd.parallelMax(T, self.data);
        }

        /// Count elements.
        pub fn count(self: Self) usize {
            return self.data.len;
        }

        /// Map each element through a function.
        /// Returns a MappedIter for chaining.
        pub fn map(self: Self, comptime func: fn (T) T) MappedIter(T) {
            return MappedIter(T).init(self.data, func);
        }

        /// Execute a function for each element (parallel for-each).
        pub fn forEach(self: Self, comptime func: fn (T) void) void {
            const Context = struct { slice: []const T };
            const ctx = Context{ .slice = self.data };

            api.parallelFor(self.data.len, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (c.slice[start..end]) |item| {
                        func(item);
                    }
                }
            }.body);
        }

        /// Check if any element satisfies a predicate.
        pub fn any(self: Self, comptime pred: fn (T) bool) bool {
            for (self.data) |item| {
                if (pred(item)) return true;
            }
            return false;
        }

        /// Check if all elements satisfy a predicate.
        pub fn all(self: Self, comptime pred: fn (T) bool) bool {
            for (self.data) |item| {
                if (!pred(item)) return false;
            }
            return true;
        }

        /// Collect into a new array (identity map).
        pub fn collect(self: Self, allocator: std.mem.Allocator) ![]T {
            const result = try allocator.alloc(T, self.data.len);
            @memcpy(result, self.data);
            return result;
        }
    };
}

/// Mapped parallel iterator.
pub fn MappedIter(comptime T: type) type {
    return struct {
        const Self = @This();

        data: []const T,
        mapFn: *const fn (T) T,

        pub fn init(data: []const T, comptime func: fn (T) T) Self {
            return Self{ .data = data, .mapFn = func };
        }

        /// Collect mapped results into a new array.
        pub fn collect(self: Self, allocator: std.mem.Allocator) ![]T {
            const result = try allocator.alloc(T, self.data.len);
            errdefer allocator.free(result);

            const Context = struct {
                src: []const T,
                dst: []T,
                func: *const fn (T) T,
            };
            const ctx = Context{ .src = self.data, .dst = result, .func = self.mapFn };

            api.parallelFor(self.data.len, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (start..end) |i| {
                        c.dst[i] = c.func(c.src[i]);
                    }
                }
            }.body);

            return result;
        }

        /// Reduce mapped elements.
        pub fn reduce(self: Self, identity: T, comptime reducer: fn (T, T) T) T {
            const Context = struct {
                slice: []const T,
                func: *const fn (T) T,
            };
            const ctx = Context{ .slice = self.data, .func = self.mapFn };

            return api.parallelReduce(
                T,
                self.data.len,
                identity,
                Context,
                ctx,
                struct {
                    fn mapFn(c: Context, i: usize) T {
                        return c.func(c.slice[i]);
                    }
                }.mapFn,
                reducer,
            );
        }

        /// Sum mapped elements.
        pub fn sum(self: Self) T {
            return self.reduce(0, struct {
                fn add(a: T, b: T) T {
                    return a + b;
                }
            }.add);
        }
    };
}

/// Mutable parallel iterator for in-place operations.
pub fn ParIterMut(comptime T: type) type {
    return struct {
        const Self = @This();

        data: []T,

        pub fn init(data: []T) Self {
            return Self{ .data = data };
        }

        /// Apply a function to each element in-place.
        pub fn mapInPlace(self: Self, comptime func: fn (T) T) void {
            const Context = struct { slice: []T };
            const ctx = Context{ .slice = self.data };

            api.parallelFor(self.data.len, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (c.slice[start..end]) |*val| {
                        val.* = func(val.*);
                    }
                }
            }.body);
        }

        /// Fill all elements with a value.
        pub fn fill(self: Self, value: T) void {
            const Context = struct { slice: []T, val: T };
            const ctx = Context{ .slice = self.data, .val = value };

            api.parallelFor(self.data.len, Context, ctx, struct {
                fn body(c: Context, start: usize, end: usize) void {
                    for (c.slice[start..end]) |*v| {
                        v.* = c.val;
                    }
                }
            }.body);
        }
    };
}

/// Range iterator for parallel index ranges.
pub const RangeIter = struct {
    start: usize,
    end: usize,

    /// Execute a function for each index in the range.
    pub fn forEach(self: RangeIter, comptime func: fn (usize) void) void {
        const len = self.end - self.start;
        if (len == 0) return;

        const Context = struct { base: usize };
        const ctx = Context{ .base = self.start };

        api.parallelFor(len, Context, ctx, struct {
            fn body(c: Context, start_offset: usize, end_offset: usize) void {
                var i = c.base + start_offset;
                const limit = c.base + end_offset;
                while (i < limit) : (i += 1) {
                    func(i);
                }
            }
        }.body);
    }

    /// Reduce over a range.
    pub fn reduce(self: RangeIter, comptime T: type, identity: T, comptime mapper: fn (usize) T, comptime reducer: fn (T, T) T) T {
        const len = self.end - self.start;
        if (len == 0) return identity;

        const Context = struct { base: usize };
        const ctx = Context{ .base = self.start };

        return api.parallelReduce(
            T,
            len,
            identity,
            Context,
            ctx,
            struct {
                fn mapFn(c: Context, i: usize) T {
                    return mapper(c.base + i);
                }
            }.mapFn,
            reducer,
        );
    }

    /// Sum the range using a mapper function.
    pub fn sum(self: RangeIter, comptime T: type, comptime mapper: fn (usize) T) T {
        return self.reduce(T, 0, mapper, struct {
            fn add(a: T, b: T) T {
                return a + b;
            }
        }.add);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "ParIter - sum" {
    const data = [_]i64{ 1, 2, 3, 4, 5 };
    const result = iter(i64, &data).sum();
    try std.testing.expectEqual(@as(i64, 15), result);
}

test "ParIter - min/max" {
    const data = [_]i64{ 3, 1, 4, 1, 5, 9, 2, 6 };
    try std.testing.expectEqual(@as(?i64, 1), iter(i64, &data).min());
    try std.testing.expectEqual(@as(?i64, 9), iter(i64, &data).max());
}

test "ParIter - empty" {
    const data: []const i64 = &.{};
    try std.testing.expectEqual(@as(?i64, null), iter(i64, data).min());
    try std.testing.expectEqual(@as(?i64, null), iter(i64, data).max());
}

test "ParIterMut - fill" {
    var data = [_]i64{ 1, 2, 3, 4, 5 };
    iterMut(i64, &data).fill(42);

    for (data) |v| {
        try std.testing.expectEqual(@as(i64, 42), v);
    }
}

test "RangeIter - sum" {
    const result = range(0, 10).sum(i64, struct {
        fn identity(i: usize) i64 {
            return @intCast(i);
        }
    }.identity);
    try std.testing.expectEqual(@as(i64, 45), result);
}
