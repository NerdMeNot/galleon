//! Swiss Set - High Performance Hash Set
//!
//! A hash set implementation based on Table, matching hashbrown's HashSet.
//! This is a thin wrapper around Table(T, void).
//!
//! Usage:
//! ```zig
//! const swisstable = @import("swisstable");
//! var set = swisstable.Set(i64).init(allocator);
//! defer set.deinit();
//!
//! try set.insert(42);
//! if (set.contains(42)) {
//!     // ...
//! }
//! ```

const std = @import("std");
const lib = @import("lib.zig");

/// High-performance hash set using Swiss Table design
pub fn Set(comptime T: type) type {
    return struct {
        const Self = @This();

        // Use a zero-sized type as value
        const Table = lib.Table(T, void);

        table: Table,

        // ====================================================================
        // Initialization
        // ====================================================================

        /// Create an empty set
        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .table = Table.init(allocator) };
        }

        /// Create with pre-allocated capacity
        pub fn initCapacity(allocator: std.mem.Allocator, initial_capacity: usize) !Self {
            return .{ .table = try Table.initCapacity(allocator, initial_capacity) };
        }

        /// Free all memory
        pub fn deinit(self: *Self) void {
            self.table.deinit();
        }

        // ====================================================================
        // Core Operations
        // ====================================================================

        /// Adds a value to the set.
        /// Returns true if the value was newly inserted, false if it already existed.
        pub fn insert(self: *Self, value: T) !bool {
            const old = try self.table.put(value, {});
            return old == null;
        }

        /// Removes a value from the set.
        /// Returns true if the value was present, false otherwise.
        pub fn remove(self: *Self, value: T) bool {
            return self.table.remove(value) != null;
        }

        /// Returns true if the set contains the value.
        pub fn contains(self: *const Self, value: T) bool {
            return self.table.contains(value);
        }

        /// Number of elements in the set
        pub fn count(self: *const Self) usize {
            return self.table.count();
        }

        /// Returns true if the set is empty
        pub fn isEmpty(self: *const Self) bool {
            return self.table.isEmpty();
        }

        /// Clears the set, removing all values.
        /// Keeps the allocated memory for reuse.
        pub fn clear(self: *Self) void {
            self.table.clear();
        }

        /// Clears the set and deallocates memory.
        pub fn clearAndFree(self: *Self) void {
            self.table.clearAndFree();
        }

        /// Current capacity
        pub fn capacity(self: *const Self) usize {
            return self.table.capacity();
        }

        /// Reserve space for at least `additional` more elements
        pub fn reserve(self: *Self, additional: usize) !void {
            try self.table.reserve(additional);
        }

        /// Shrinks the capacity as much as possible
        pub fn shrinkToFit(self: *Self) !void {
            try self.table.shrinkToFit();
        }

        /// Shrinks the capacity with a lower limit
        pub fn shrinkTo(self: *Self, min_capacity: usize) !void {
            try self.table.shrinkTo(min_capacity);
        }

        /// Creates a copy of this set
        pub fn clone(self: *const Self) !Self {
            return .{ .table = try self.table.clone() };
        }

        // ====================================================================
        // Iteration
        // ====================================================================

        /// Iterator over set elements
        pub const Iterator = struct {
            inner: Table.KeyIterator,

            pub fn next(self: *Iterator) ?T {
                return self.inner.next();
            }
        };

        /// Returns an iterator over the set elements
        pub fn iterator(self: *const Self) Iterator {
            return .{ .inner = self.table.keys() };
        }

        // ====================================================================
        // Set Operations
        // ====================================================================

        /// Returns true if self has no elements in common with other.
        pub fn isDisjoint(self: *const Self, other: *const Self) bool {
            // Iterate over smaller set
            if (self.count() <= other.count()) {
                var it = self.iterator();
                while (it.next()) |value| {
                    if (other.contains(value)) return false;
                }
            } else {
                var it = other.iterator();
                while (it.next()) |value| {
                    if (self.contains(value)) return false;
                }
            }
            return true;
        }

        /// Returns true if the set is a subset of another.
        /// i.e., other contains at least all the values in self.
        pub fn isSubset(self: *const Self, other: *const Self) bool {
            if (self.count() > other.count()) return false;
            var it = self.iterator();
            while (it.next()) |value| {
                if (!other.contains(value)) return false;
            }
            return true;
        }

        /// Returns true if the set is a superset of another.
        /// i.e., self contains at least all the values in other.
        pub fn isSuperset(self: *const Self, other: *const Self) bool {
            return other.isSubset(self);
        }

        /// Visits the values representing the union.
        /// Creates a new set containing all elements from both sets.
        pub fn unionWith(self: *const Self, other: *const Self) !Self {
            var result = try self.clone();
            errdefer result.deinit();

            var it = other.iterator();
            while (it.next()) |value| {
                _ = try result.insert(value);
            }
            return result;
        }

        /// Visits the values representing the intersection.
        /// Creates a new set containing only elements present in both sets.
        pub fn intersection(self: *const Self, other: *const Self) !Self {
            var result = Self.init(self.table.allocator);
            errdefer result.deinit();

            // Iterate over smaller set
            const smaller = if (self.count() <= other.count()) self else other;
            const larger = if (self.count() <= other.count()) other else self;

            var it = smaller.iterator();
            while (it.next()) |value| {
                if (larger.contains(value)) {
                    _ = try result.insert(value);
                }
            }
            return result;
        }

        /// Visits the values representing the difference.
        /// Creates a new set containing elements in self but not in other.
        pub fn difference(self: *const Self, other: *const Self) !Self {
            var result = Self.init(self.table.allocator);
            errdefer result.deinit();

            var it = self.iterator();
            while (it.next()) |value| {
                if (!other.contains(value)) {
                    _ = try result.insert(value);
                }
            }
            return result;
        }

        /// Visits the values representing the symmetric difference.
        /// Creates a new set containing elements in either self or other but not both.
        pub fn symmetricDifference(self: *const Self, other: *const Self) !Self {
            var result = Self.init(self.table.allocator);
            errdefer result.deinit();

            var it = self.iterator();
            while (it.next()) |value| {
                if (!other.contains(value)) {
                    _ = try result.insert(value);
                }
            }

            var it2 = other.iterator();
            while (it2.next()) |value| {
                if (!self.contains(value)) {
                    _ = try result.insert(value);
                }
            }
            return result;
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

test "basic operations" {
    var set = Set(i64).init(std.testing.allocator);
    defer set.deinit();

    // Insert
    try std.testing.expect(try set.insert(1));
    try std.testing.expect(try set.insert(2));
    try std.testing.expect(try set.insert(3));
    try std.testing.expect(!try set.insert(1)); // Already exists

    // Contains
    try std.testing.expect(set.contains(1));
    try std.testing.expect(set.contains(2));
    try std.testing.expect(!set.contains(4));

    // Count
    try std.testing.expectEqual(@as(usize, 3), set.count());

    // Remove
    try std.testing.expect(set.remove(2));
    try std.testing.expect(!set.remove(4)); // Doesn't exist
    try std.testing.expectEqual(@as(usize, 2), set.count());
}

test "iteration" {
    var set = Set(i64).init(std.testing.allocator);
    defer set.deinit();

    _ = try set.insert(1);
    _ = try set.insert(2);
    _ = try set.insert(3);

    var sum: i64 = 0;
    var it = set.iterator();
    while (it.next()) |value| {
        sum += value;
    }

    try std.testing.expectEqual(@as(i64, 6), sum);
}

test "set operations - isSubset" {
    var a = Set(i64).init(std.testing.allocator);
    defer a.deinit();
    var b = Set(i64).init(std.testing.allocator);
    defer b.deinit();

    _ = try a.insert(1);
    _ = try a.insert(2);

    _ = try b.insert(1);
    _ = try b.insert(2);
    _ = try b.insert(3);

    try std.testing.expect(a.isSubset(&b));
    try std.testing.expect(!b.isSubset(&a));
    try std.testing.expect(b.isSuperset(&a));
}

test "set operations - intersection" {
    var a = Set(i64).init(std.testing.allocator);
    defer a.deinit();
    var b = Set(i64).init(std.testing.allocator);
    defer b.deinit();

    _ = try a.insert(1);
    _ = try a.insert(2);
    _ = try a.insert(3);

    _ = try b.insert(2);
    _ = try b.insert(3);
    _ = try b.insert(4);

    var result = try a.intersection(&b);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.count());
    try std.testing.expect(result.contains(2));
    try std.testing.expect(result.contains(3));
}

test "set operations - difference" {
    var a = Set(i64).init(std.testing.allocator);
    defer a.deinit();
    var b = Set(i64).init(std.testing.allocator);
    defer b.deinit();

    _ = try a.insert(1);
    _ = try a.insert(2);
    _ = try a.insert(3);

    _ = try b.insert(2);
    _ = try b.insert(3);
    _ = try b.insert(4);

    var result = try a.difference(&b);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.count());
    try std.testing.expect(result.contains(1));
}

test "set operations - union" {
    var a = Set(i64).init(std.testing.allocator);
    defer a.deinit();
    var b = Set(i64).init(std.testing.allocator);
    defer b.deinit();

    _ = try a.insert(1);
    _ = try a.insert(2);

    _ = try b.insert(2);
    _ = try b.insert(3);

    var result = try a.unionWith(&b);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.count());
    try std.testing.expect(result.contains(1));
    try std.testing.expect(result.contains(2));
    try std.testing.expect(result.contains(3));
}

test "clone" {
    var set = Set(i64).init(std.testing.allocator);
    defer set.deinit();

    _ = try set.insert(1);
    _ = try set.insert(2);
    _ = try set.insert(3);

    var cloned = try set.clone();
    defer cloned.deinit();

    try std.testing.expectEqual(@as(usize, 3), cloned.count());
    try std.testing.expect(cloned.contains(1));
    try std.testing.expect(cloned.contains(2));
    try std.testing.expect(cloned.contains(3));

    // Modify original, clone should be unaffected
    _ = try set.insert(4);
    try std.testing.expect(!cloned.contains(4));
}
