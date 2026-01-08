const std = @import("std");
const Allocator = std.mem.Allocator;

/// Cache line alignment (64 bytes)
const CACHE_LINE_SIZE = 64;
const CACHE_LINE_ALIGN: std.mem.Alignment = .@"64";

/// Generic column storage with cache-aligned data
pub fn Column(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        buffer: []align(CACHE_LINE_SIZE) T,
        length: usize,
        null_bitmap: ?[]u64, // Each bit represents null status

        /// Create a new column with the given capacity
        pub fn create(allocator: Allocator, capacity: usize) !*Self {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            const buffer = try allocator.alignedAlloc(T, CACHE_LINE_ALIGN, capacity);
            @memset(buffer, std.mem.zeroes(T));

            self.* = Self{
                .allocator = allocator,
                .buffer = buffer,
                .length = capacity,
                .null_bitmap = null,
            };

            return self;
        }

        /// Create a column from an existing slice (copies data)
        pub fn createFromSlice(allocator: Allocator, src_data: []const T) !*Self {
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);

            const buffer = try allocator.alignedAlloc(T, CACHE_LINE_ALIGN, src_data.len);
            @memcpy(buffer, src_data);

            self.* = Self{
                .allocator = allocator,
                .buffer = buffer,
                .length = src_data.len,
                .null_bitmap = null,
            };

            return self;
        }

        /// Free column resources
        pub fn deinit(self: *Self) void {
            if (self.null_bitmap) |bitmap| {
                self.allocator.free(bitmap);
            }
            self.allocator.free(self.buffer);
            self.allocator.destroy(self);
        }

        /// Get the length of the column
        pub fn len(self: *const Self) usize {
            return self.length;
        }

        /// Get the underlying data slice
        pub fn data(self: *const Self) []const T {
            return self.buffer[0..self.length];
        }

        /// Get mutable data slice
        pub fn dataMut(self: *Self) []T {
            return self.buffer[0..self.length];
        }

        /// Get value at index (returns null if index is out of bounds or value is null)
        pub fn get(self: *const Self, index: usize) ?T {
            if (index >= self.length) return null;
            if (self.isNull(index)) return null;
            return self.buffer[index];
        }

        /// Set value at index
        pub fn set(self: *Self, index: usize, value: T) void {
            if (index < self.length) {
                self.buffer[index] = value;
                if (self.null_bitmap) |bitmap| {
                    // Clear null bit
                    const word_idx = index / 64;
                    const bit_idx: u6 = @intCast(index % 64);
                    bitmap[word_idx] &= ~(@as(u64, 1) << bit_idx);
                }
            }
        }

        /// Check if value at index is null
        pub fn isNull(self: *const Self, index: usize) bool {
            if (self.null_bitmap) |bitmap| {
                const word_idx = index / 64;
                const bit_idx: u6 = @intCast(index % 64);
                return (bitmap[word_idx] & (@as(u64, 1) << bit_idx)) != 0;
            }
            return false;
        }

        /// Set value at index to null
        pub fn setNull(self: *Self, index: usize) !void {
            if (index >= self.length) return;

            // Lazily allocate null bitmap
            if (self.null_bitmap == null) {
                const bitmap_len = (self.length + 63) / 64;
                self.null_bitmap = try self.allocator.alloc(u64, bitmap_len);
                @memset(self.null_bitmap.?, 0);
            }

            const word_idx = index / 64;
            const bit_idx: u6 = @intCast(index % 64);
            self.null_bitmap.?[word_idx] |= (@as(u64, 1) << bit_idx);
        }

        /// Count non-null values
        pub fn countNonNull(self: *const Self) usize {
            if (self.null_bitmap == null) return self.length;

            var null_count: usize = 0;
            for (self.null_bitmap.?) |word| {
                null_count += @popCount(word);
            }
            return self.length - null_count;
        }

        /// Get raw pointer for FFI
        pub fn rawPtr(self: *const Self) [*]const T {
            return self.buffer.ptr;
        }
    };
}

// Type aliases for common column types
pub const ColumnF64 = Column(f64);
pub const ColumnF32 = Column(f32);
pub const ColumnI64 = Column(i64);
pub const ColumnI32 = Column(i32);
pub const ColumnU64 = Column(u64);
pub const ColumnU32 = Column(u32);

// ============================================================================
// Tests
// ============================================================================

test "column create and access" {
    const allocator = std.testing.allocator;
    const col = try ColumnF64.create(allocator, 10);
    defer col.deinit();

    try std.testing.expectEqual(@as(usize, 10), col.len());

    col.set(0, 42.0);
    try std.testing.expectEqual(@as(f64, 42.0), col.get(0).?);
}

test "column from slice" {
    const allocator = std.testing.allocator;
    const src = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };

    var col = try ColumnF64.createFromSlice(allocator, &src);
    defer col.deinit();

    try std.testing.expectEqual(@as(usize, 5), col.len());
    try std.testing.expectEqual(@as(f64, 3.0), col.get(2).?);
}

test "column null handling" {
    const allocator = std.testing.allocator;
    const col = try ColumnF64.create(allocator, 10);
    defer col.deinit();

    col.set(0, 42.0);
    try col.setNull(1);

    try std.testing.expect(!col.isNull(0));
    try std.testing.expect(col.isNull(1));
    try std.testing.expectEqual(@as(?f64, 42.0), col.get(0));
    try std.testing.expectEqual(@as(?f64, null), col.get(1));
}

test "column alignment" {
    const allocator = std.testing.allocator;
    const col = try ColumnF64.create(allocator, 1000);
    defer col.deinit();

    // Check that buffer is cache-line aligned (64 bytes)
    const addr = @intFromPtr(col.buffer.ptr);
    try std.testing.expect(addr % 64 == 0);
}
