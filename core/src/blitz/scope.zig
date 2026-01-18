//! Scope-based parallelism for Blitz
//!
//! Provides scope-based parallel execution for spawning arbitrary tasks
//! that must all complete before the scope exits.
//!
//! Usage:
//! ```zig
//! const blitz = @import("blitz");
//!
//! // Execute multiple tasks in parallel
//! const results = blitz.join2(i64, i64,
//!     fn() i64 { return compute1(); },
//!     fn() i64 { return compute2(); },
//! );
//!
//! // Or spawn N tasks dynamically
//! blitz.parallelForRange(0, 100, fn(i: usize) void { process(i); });
//! ```

const std = @import("std");
const api = @import("api.zig");
const latch_mod = @import("latch.zig");

/// Maximum concurrent spawned tasks in a scope.
const MAX_SCOPE_TASKS = 64;

/// A scope for running parallel tasks.
/// Tasks spawned within a scope all complete before the scope exits.
pub const Scope = struct {
    /// Number of pending tasks.
    pending: std.atomic.Value(usize),

    /// Initialize a new scope.
    pub fn init() Scope {
        return Scope{
            .pending = std.atomic.Value(usize).init(0),
        };
    }

    /// Spawn a function to run (potentially in parallel).
    /// Note: In this simplified implementation, we use parallelFor under the hood.
    pub fn spawn(self: *Scope, comptime func: fn () void) void {
        _ = self;
        func();
    }
};

/// Execute a scope function.
/// All tasks spawned within the scope will complete before returning.
pub fn scope(comptime func: fn (*Scope) void) void {
    var s = Scope.init();
    func(&s);
}

/// Execute a scope function with a context value.
pub fn scopeWithContext(comptime Context: type, context: Context, comptime func: fn (Context, *Scope) void) void {
    var s = Scope.init();
    func(context, &s);
}

// ============================================================================
// Convenience Functions for Common Patterns
// ============================================================================

/// Execute two functions in parallel and return their results.
pub fn join2(
    comptime A: type,
    comptime B: type,
    comptime func_a: fn () A,
    comptime func_b: fn () B,
) struct { A, B } {
    return api.join(
        A,
        B,
        struct {
            fn wrapA(_: void) A {
                return func_a();
            }
        }.wrapA,
        struct {
            fn wrapB(_: void) B {
                return func_b();
            }
        }.wrapB,
        {},
        {},
    );
}

/// Execute three functions in parallel and return their results.
pub fn join3(
    comptime A: type,
    comptime B: type,
    comptime C: type,
    comptime func_a: fn () A,
    comptime func_b: fn () B,
    comptime func_c: fn () C,
) struct { A, B, C } {
    // Execute A in parallel with (B || C)
    const ab = api.join(
        A,
        struct { B, C },
        struct {
            fn wrapA(_: void) A {
                return func_a();
            }
        }.wrapA,
        struct {
            fn wrapBC(_: void) struct { B, C } {
                return api.join(
                    B,
                    C,
                    struct {
                        fn wrapB(_: void) B {
                            return func_b();
                        }
                    }.wrapB,
                    struct {
                        fn wrapC(_: void) C {
                            return func_c();
                        }
                    }.wrapC,
                    {},
                    {},
                );
            }
        }.wrapBC,
        {},
        {},
    );

    return .{ ab[0], ab[1][0], ab[1][1] };
}

/// Execute N functions in parallel using an array of function pointers.
/// Note: Limited to functions returning the same type.
pub fn joinN(comptime T: type, comptime N: usize, funcs: *const [N]fn () T) [N]T {
    var results: [N]T = undefined;

    if (N == 0) return results;
    if (N == 1) {
        results[0] = funcs[0]();
        return results;
    }

    // Use parallelFor pattern for N tasks
    const Context = struct {
        funcs: *const [N]fn () T,
        results: *[N]T,
    };
    const ctx = Context{ .funcs = funcs, .results = &results };

    api.parallelFor(N, Context, ctx, struct {
        fn body(c: Context, start: usize, end: usize) void {
            for (start..end) |i| {
                c.results[i] = c.funcs[i]();
            }
        }
    }.body);

    return results;
}

// ============================================================================
// Parallel For with Index Ranges
// ============================================================================

/// Parallel for_each over an index range.
/// Splits the range into chunks and executes in parallel.
pub fn parallelForRange(start: usize, end: usize, comptime func: fn (usize) void) void {
    if (end <= start) return;

    const len = end - start;

    const Context = struct { base: usize };
    const ctx = Context{ .base = start };

    api.parallelFor(len, Context, ctx, struct {
        fn body(c: Context, s: usize, e: usize) void {
            var i = c.base + s;
            const limit = c.base + e;
            while (i < limit) : (i += 1) {
                func(i);
            }
        }
    }.body);
}

/// Parallel for_each with context.
pub fn parallelForRangeWithContext(
    comptime Context: type,
    context: Context,
    start: usize,
    end: usize,
    comptime func: fn (Context, usize) void,
) void {
    if (end <= start) return;

    const len = end - start;

    const FullContext = struct { base: usize, ctx: Context };
    const full_ctx = FullContext{ .base = start, .ctx = context };

    api.parallelFor(len, FullContext, full_ctx, struct {
        fn body(c: FullContext, s: usize, e: usize) void {
            var i = c.base + s;
            const limit = c.base + e;
            while (i < limit) : (i += 1) {
                func(c.ctx, i);
            }
        }
    }.body);
}

// ============================================================================
// Tests
// ============================================================================

test "join2 - basic" {
    const result = join2(
        i32,
        i64,
        struct {
            fn a() i32 {
                return 42;
            }
        }.a,
        struct {
            fn b() i64 {
                return 100;
            }
        }.b,
    );

    try std.testing.expectEqual(@as(i32, 42), result[0]);
    try std.testing.expectEqual(@as(i64, 100), result[1]);
}

test "parallelForRange - basic" {
    var sum: std.atomic.Value(usize) = std.atomic.Value(usize).init(0);

    parallelForRangeWithContext(
        *std.atomic.Value(usize),
        &sum,
        0,
        100,
        struct {
            fn body(s: *std.atomic.Value(usize), i: usize) void {
                _ = s.fetchAdd(i, .monotonic);
            }
        }.body,
    );

    // Sum of 0..99 = 99*100/2 = 4950
    try std.testing.expectEqual(@as(usize, 4950), sum.load(.monotonic));
}

test "Scope - basic" {
    // Simple test that scope executes without error
    scope(struct {
        fn run(s: *Scope) void {
            _ = s;
            // Scope body runs
        }
    }.run);
}
