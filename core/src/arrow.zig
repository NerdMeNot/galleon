//! Arrow C Data Interface types and utilities for Zig.
//!
//! This module provides:
//! - ArrowSchema and ArrowArray types matching the Arrow C Data Interface spec
//! - Helper functions to extract typed buffers from ArrowArray
//! - Integration with existing SIMD operations
//!
//! Reference: https://arrow.apache.org/docs/format/CDataInterface.html

const std = @import("std");
const simd = @import("simd.zig");

// ============================================================================
// Arrow C Data Interface Types
// ============================================================================

/// Arrow Schema - describes the type of an array
/// This matches the ArrowSchema struct from the C Data Interface
pub const ArrowSchema = extern struct {
    /// Format string (e.g., "l" for int64, "g" for float64)
    format: ?[*:0]const u8,
    /// Optional name
    name: ?[*:0]const u8,
    /// Optional metadata (key-value pairs)
    metadata: ?[*]const u8,
    /// Flags (e.g., nullable)
    flags: i64,
    /// Number of children (for nested types)
    n_children: i64,
    /// Child schemas (for nested types)
    children: ?[*]*ArrowSchema,
    /// Dictionary schema (for dictionary-encoded arrays)
    dictionary: ?*ArrowSchema,
    /// Release callback - must be called to free resources
    release: ?*const fn (*ArrowSchema) callconv(.c) void,
    /// Private data for the producer
    private_data: ?*anyopaque,

    /// Check if this schema has been released
    pub fn isReleased(self: *const ArrowSchema) bool {
        return self.release == null;
    }
};

/// Arrow Array - holds the actual data
/// This matches the ArrowArray struct from the C Data Interface
pub const ArrowArray = extern struct {
    /// Number of elements in the array
    length: i64,
    /// Number of null values
    null_count: i64,
    /// Offset into the buffers (for sliced arrays)
    offset: i64,
    /// Number of buffers
    n_buffers: i64,
    /// Number of children (for nested types)
    n_children: i64,
    /// Array of buffer pointers
    /// - buffers[0]: validity bitmap (may be null if no nulls)
    /// - buffers[1]: data buffer (for primitive types)
    /// - buffers[1]: offsets, buffers[2]: data (for variable-length types)
    buffers: ?[*]?*anyopaque,
    /// Child arrays (for nested types)
    children: ?[*]*ArrowArray,
    /// Dictionary array (for dictionary-encoded arrays)
    dictionary: ?*ArrowArray,
    /// Release callback - must be called to free resources
    release: ?*const fn (*ArrowArray) callconv(.c) void,
    /// Private data for the producer
    private_data: ?*anyopaque,

    /// Check if this array has been released
    pub fn isReleased(self: *const ArrowArray) bool {
        return self.release == null;
    }

    /// Get the length as usize
    pub fn len(self: *const ArrowArray) usize {
        return @intCast(self.length);
    }

    /// Check if array has nulls
    pub fn hasNulls(self: *const ArrowArray) bool {
        return self.null_count > 0;
    }
};

// ============================================================================
// Buffer Extraction Helpers
// ============================================================================

/// Extract Float64 data buffer from ArrowArray
/// Returns null if buffers is null or buffer[1] is null
pub fn getFloat64Buffer(arr: *const ArrowArray) ?[]const f64 {
    const buffers = arr.buffers orelse return null;
    const data_ptr = buffers[1] orelse return null;
    const ptr: [*]const f64 = @ptrCast(@alignCast(data_ptr));
    const offset: usize = @intCast(arr.offset);
    return ptr[offset..][0..arr.len()];
}

/// Extract Float32 data buffer from ArrowArray
pub fn getFloat32Buffer(arr: *const ArrowArray) ?[]const f32 {
    const buffers = arr.buffers orelse return null;
    const data_ptr = buffers[1] orelse return null;
    const ptr: [*]const f32 = @ptrCast(@alignCast(data_ptr));
    const offset: usize = @intCast(arr.offset);
    return ptr[offset..][0..arr.len()];
}

/// Extract Int64 data buffer from ArrowArray
pub fn getInt64Buffer(arr: *const ArrowArray) ?[]const i64 {
    const buffers = arr.buffers orelse return null;
    const data_ptr = buffers[1] orelse return null;
    const ptr: [*]const i64 = @ptrCast(@alignCast(data_ptr));
    const offset: usize = @intCast(arr.offset);
    return ptr[offset..][0..arr.len()];
}

/// Extract Int32 data buffer from ArrowArray
pub fn getInt32Buffer(arr: *const ArrowArray) ?[]const i32 {
    const buffers = arr.buffers orelse return null;
    const data_ptr = buffers[1] orelse return null;
    const ptr: [*]const i32 = @ptrCast(@alignCast(data_ptr));
    const offset: usize = @intCast(arr.offset);
    return ptr[offset..][0..arr.len()];
}

/// Extract UInt32 data buffer from ArrowArray
pub fn getUInt32Buffer(arr: *const ArrowArray) ?[]const u32 {
    const buffers = arr.buffers orelse return null;
    const data_ptr = buffers[1] orelse return null;
    const ptr: [*]const u32 = @ptrCast(@alignCast(data_ptr));
    const offset: usize = @intCast(arr.offset);
    return ptr[offset..][0..arr.len()];
}

/// Extract UInt64 data buffer from ArrowArray
pub fn getUInt64Buffer(arr: *const ArrowArray) ?[]const u64 {
    const buffers = arr.buffers orelse return null;
    const data_ptr = buffers[1] orelse return null;
    const ptr: [*]const u64 = @ptrCast(@alignCast(data_ptr));
    const offset: usize = @intCast(arr.offset);
    return ptr[offset..][0..arr.len()];
}

/// Extract validity bitmap from ArrowArray
/// Returns null if no validity bitmap (all values are valid)
pub fn getValidityBitmap(arr: *const ArrowArray) ?[]const u8 {
    const buffers = arr.buffers orelse return null;
    const validity_ptr = buffers[0] orelse return null;
    const ptr: [*]const u8 = @ptrCast(validity_ptr);
    // Bitmap size in bytes: ceil(length / 8)
    const bitmap_len = (arr.len() + 7) / 8;
    return ptr[0..bitmap_len];
}

/// Check if a specific index is valid (not null) in the validity bitmap
pub fn isValid(validity: ?[]const u8, index: usize) bool {
    const bitmap = validity orelse return true; // No bitmap = all valid
    const byte_index = index / 8;
    const bit_index: u3 = @intCast(index % 8);
    return (bitmap[byte_index] & (@as(u8, 1) << bit_index)) != 0;
}

// ============================================================================
// SIMD Operations on Arrow Arrays
// ============================================================================

/// Sum Float64 Arrow array using SIMD
/// Ignores null values if validity bitmap is present
pub fn sumFloat64(arr: *const ArrowArray) f64 {
    const data = getFloat64Buffer(arr) orelse return 0.0;

    if (!arr.hasNulls()) {
        // Fast path: no nulls, use existing SIMD sum
        return simd.sum(f64, data);
    }

    // Slow path: handle nulls
    const validity = getValidityBitmap(arr);
    var total: f64 = 0.0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
        }
    }
    return total;
}

/// Min Float64 Arrow array using SIMD
pub fn minFloat64(arr: *const ArrowArray) f64 {
    const data = getFloat64Buffer(arr) orelse return std.math.nan(f64);

    if (!arr.hasNulls()) {
        return simd.min(f64, data) orelse std.math.nan(f64);
    }

    // Handle nulls
    const validity = getValidityBitmap(arr);
    var result: f64 = std.math.inf(f64);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val < result) {
            result = val;
        }
    }
    return result;
}

/// Max Float64 Arrow array using SIMD
pub fn maxFloat64(arr: *const ArrowArray) f64 {
    const data = getFloat64Buffer(arr) orelse return std.math.nan(f64);

    if (!arr.hasNulls()) {
        return simd.max(f64, data) orelse std.math.nan(f64);
    }

    // Handle nulls
    const validity = getValidityBitmap(arr);
    var result: f64 = -std.math.inf(f64);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val > result) {
            result = val;
        }
    }
    return result;
}

/// Mean Float64 Arrow array
pub fn meanFloat64(arr: *const ArrowArray) f64 {
    const data = getFloat64Buffer(arr) orelse return std.math.nan(f64);

    if (!arr.hasNulls()) {
        return simd.mean(f64, data) orelse std.math.nan(f64);
    }

    // Handle nulls
    const validity = getValidityBitmap(arr);
    var total: f64 = 0.0;
    var count: usize = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
            count += 1;
        }
    }
    return if (count > 0) total / @as(f64, @floatFromInt(count)) else std.math.nan(f64);
}

/// Sum Int64 Arrow array using SIMD
pub fn sumInt64(arr: *const ArrowArray) i64 {
    const data = getInt64Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        return simd.sumInt(i64, data);
    }

    // Handle nulls
    const validity = getValidityBitmap(arr);
    var total: i64 = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
        }
    }
    return total;
}

/// Min Int64 Arrow array
pub fn minInt64(arr: *const ArrowArray) i64 {
    const data = getInt64Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        return simd.minInt(i64, data) orelse 0;
    }

    // Handle nulls
    const validity = getValidityBitmap(arr);
    var result: i64 = std.math.maxInt(i64);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val < result) {
            result = val;
        }
    }
    return result;
}

/// Max Int64 Arrow array
pub fn maxInt64(arr: *const ArrowArray) i64 {
    const data = getInt64Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        return simd.maxInt(i64, data) orelse 0;
    }

    // Handle nulls
    const validity = getValidityBitmap(arr);
    var result: i64 = std.math.minInt(i64);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val > result) {
            result = val;
        }
    }
    return result;
}

// ============================================================================
// Float32 Aggregation Operations
// ============================================================================

/// Sum Float32 Arrow array using SIMD
pub fn sumFloat32(arr: *const ArrowArray) f32 {
    const data = getFloat32Buffer(arr) orelse return 0.0;

    if (!arr.hasNulls()) {
        return simd.sum(f32, data);
    }

    // Slow path: handle nulls
    const validity = getValidityBitmap(arr);
    var total: f32 = 0.0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
        }
    }
    return total;
}

/// Min Float32 Arrow array using SIMD
pub fn minFloat32(arr: *const ArrowArray) f32 {
    const data = getFloat32Buffer(arr) orelse return std.math.nan(f32);

    if (!arr.hasNulls()) {
        return simd.min(f32, data) orelse std.math.nan(f32);
    }

    const validity = getValidityBitmap(arr);
    var result: f32 = std.math.inf(f32);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val < result) {
            result = val;
        }
    }
    return result;
}

/// Max Float32 Arrow array using SIMD
pub fn maxFloat32(arr: *const ArrowArray) f32 {
    const data = getFloat32Buffer(arr) orelse return std.math.nan(f32);

    if (!arr.hasNulls()) {
        return simd.max(f32, data) orelse std.math.nan(f32);
    }

    const validity = getValidityBitmap(arr);
    var result: f32 = -std.math.inf(f32);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val > result) {
            result = val;
        }
    }
    return result;
}

/// Mean Float32 Arrow array
pub fn meanFloat32(arr: *const ArrowArray) f32 {
    const data = getFloat32Buffer(arr) orelse return std.math.nan(f32);

    if (!arr.hasNulls()) {
        return simd.mean(f32, data) orelse std.math.nan(f32);
    }

    const validity = getValidityBitmap(arr);
    var total: f32 = 0.0;
    var count: usize = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
            count += 1;
        }
    }
    return if (count > 0) total / @as(f32, @floatFromInt(count)) else std.math.nan(f32);
}

// ============================================================================
// Int32 Aggregation Operations
// ============================================================================

/// Sum Int32 Arrow array using SIMD
pub fn sumInt32(arr: *const ArrowArray) i32 {
    const data = getInt32Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        return simd.sumInt(i32, data);
    }

    const validity = getValidityBitmap(arr);
    var total: i32 = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
        }
    }
    return total;
}

/// Min Int32 Arrow array
pub fn minInt32(arr: *const ArrowArray) i32 {
    const data = getInt32Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        return simd.minInt(i32, data) orelse 0;
    }

    const validity = getValidityBitmap(arr);
    var result: i32 = std.math.maxInt(i32);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val < result) {
            result = val;
        }
    }
    return result;
}

/// Max Int32 Arrow array
pub fn maxInt32(arr: *const ArrowArray) i32 {
    const data = getInt32Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        return simd.maxInt(i32, data) orelse 0;
    }

    const validity = getValidityBitmap(arr);
    var result: i32 = std.math.minInt(i32);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val > result) {
            result = val;
        }
    }
    return result;
}

// ============================================================================
// UInt64 Aggregation Operations
// ============================================================================

/// Sum UInt64 Arrow array
pub fn sumUInt64(arr: *const ArrowArray) u64 {
    const data = getUInt64Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        var total: u64 = 0;
        for (data) |val| {
            total += val;
        }
        return total;
    }

    const validity = getValidityBitmap(arr);
    var total: u64 = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
        }
    }
    return total;
}

/// Min UInt64 Arrow array
pub fn minUInt64(arr: *const ArrowArray) u64 {
    const data = getUInt64Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        var result: u64 = std.math.maxInt(u64);
        for (data) |val| {
            if (val < result) result = val;
        }
        return result;
    }

    const validity = getValidityBitmap(arr);
    var result: u64 = std.math.maxInt(u64);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val < result) {
            result = val;
        }
    }
    return result;
}

/// Max UInt64 Arrow array
pub fn maxUInt64(arr: *const ArrowArray) u64 {
    const data = getUInt64Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        var result: u64 = 0;
        for (data) |val| {
            if (val > result) result = val;
        }
        return result;
    }

    const validity = getValidityBitmap(arr);
    var result: u64 = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val > result) {
            result = val;
        }
    }
    return result;
}

// ============================================================================
// UInt32 Aggregation Operations
// ============================================================================

/// Sum UInt32 Arrow array
pub fn sumUInt32(arr: *const ArrowArray) u32 {
    const data = getUInt32Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        var total: u32 = 0;
        for (data) |val| {
            total += val;
        }
        return total;
    }

    const validity = getValidityBitmap(arr);
    var total: u32 = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i)) {
            total += val;
        }
    }
    return total;
}

/// Min UInt32 Arrow array
pub fn minUInt32(arr: *const ArrowArray) u32 {
    const data = getUInt32Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        var result: u32 = std.math.maxInt(u32);
        for (data) |val| {
            if (val < result) result = val;
        }
        return result;
    }

    const validity = getValidityBitmap(arr);
    var result: u32 = std.math.maxInt(u32);
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val < result) {
            result = val;
        }
    }
    return result;
}

/// Max UInt32 Arrow array
pub fn maxUInt32(arr: *const ArrowArray) u32 {
    const data = getUInt32Buffer(arr) orelse return 0;

    if (!arr.hasNulls()) {
        var result: u32 = 0;
        for (data) |val| {
            if (val > result) result = val;
        }
        return result;
    }

    const validity = getValidityBitmap(arr);
    var result: u32 = 0;
    for (data, 0..) |val, i| {
        if (isValid(validity, i) and val > result) {
            result = val;
        }
    }
    return result;
}

// ============================================================================
// Managed Arrow Arrays (Zig-owned, created from raw data)
// ============================================================================

/// A Zig-managed Arrow array that owns its memory.
/// This is used when Go sends raw data to Zig for Arrow-based operations.
pub const ManagedArrowArray = struct {
    /// The Arrow array structure (compatible with C Data Interface)
    array: ArrowArray,
    /// Allocator used for memory management
    allocator: std.mem.Allocator,
    /// Owned data buffer
    data_buffer: []u8,
    /// Owned validity bitmap (null if no nulls)
    validity_buffer: ?[]u8,
    /// Buffer pointers array
    buffer_ptrs: [2]?*anyopaque,

    const Self = @This();

    /// Create a Float64 Arrow array from raw data (copies the data)
    pub fn createF64(allocator: std.mem.Allocator, data: []const f64) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Allocate and copy data buffer
        const data_bytes = data.len * @sizeOf(f64);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        // No validity bitmap (all values valid)
        self.validity_buffer = null;

        // Set up buffer pointers
        self.buffer_ptrs[0] = null; // No validity bitmap
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        // Initialize Arrow array structure
        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null, // Managed by Zig, not C release callback
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create an Int64 Arrow array from raw data (copies the data)
    pub fn createI64(allocator: std.mem.Allocator, data: []const i64) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Allocate and copy data buffer
        const data_bytes = data.len * @sizeOf(i64);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        // No validity bitmap
        self.validity_buffer = null;

        // Set up buffer pointers
        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        // Initialize Arrow array structure
        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create a Float64 Arrow array with validity bitmap
    /// valid_bitmap: each byte contains 8 validity bits (LSB first)
    pub fn createF64WithNulls(
        allocator: std.mem.Allocator,
        data: []const f64,
        valid_bitmap: []const u8,
        null_count: i64,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Allocate and copy data buffer
        const data_bytes = data.len * @sizeOf(f64);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        // Allocate and copy validity bitmap
        const bitmap_len = (data.len + 7) / 8;
        self.validity_buffer = try allocator.alloc(u8, bitmap_len);
        errdefer allocator.free(self.validity_buffer.?);

        if (valid_bitmap.len >= bitmap_len) {
            @memcpy(self.validity_buffer.?, valid_bitmap[0..bitmap_len]);
        } else {
            @memcpy(self.validity_buffer.?[0..valid_bitmap.len], valid_bitmap);
            @memset(self.validity_buffer.?[valid_bitmap.len..], 0xFF);
        }

        // Set up buffer pointers
        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        // Initialize Arrow array structure
        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = null_count,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create an Int64 Arrow array with validity bitmap
    pub fn createI64WithNulls(
        allocator: std.mem.Allocator,
        data: []const i64,
        valid_bitmap: []const u8,
        null_count: i64,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Allocate and copy data buffer
        const data_bytes = data.len * @sizeOf(i64);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        // Allocate and copy validity bitmap
        const bitmap_len = (data.len + 7) / 8;
        self.validity_buffer = try allocator.alloc(u8, bitmap_len);
        errdefer allocator.free(self.validity_buffer.?);

        if (valid_bitmap.len >= bitmap_len) {
            @memcpy(self.validity_buffer.?, valid_bitmap[0..bitmap_len]);
        } else {
            @memcpy(self.validity_buffer.?[0..valid_bitmap.len], valid_bitmap);
            @memset(self.validity_buffer.?[valid_bitmap.len..], 0xFF);
        }

        // Set up buffer pointers
        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        // Initialize Arrow array structure
        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = null_count,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    // =========================================================================
    // Zero-Copy Creation Methods (takes ownership of allocated data)
    // =========================================================================

    /// Create a Float64 Arrow array taking ownership of already-allocated data (no copy)
    pub fn createF64FromOwned(allocator: std.mem.Allocator, data: []f64) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Take ownership of the data buffer (reinterpret as bytes)
        self.data_buffer = std.mem.sliceAsBytes(data);
        self.validity_buffer = null;

        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create an Int64 Arrow array taking ownership of already-allocated data (no copy)
    pub fn createI64FromOwned(allocator: std.mem.Allocator, data: []i64) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Take ownership of the data buffer (reinterpret as bytes)
        self.data_buffer = std.mem.sliceAsBytes(data);
        self.validity_buffer = null;

        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create a Float64 Arrow array with validity bitmap, taking ownership (no copy)
    pub fn createF64WithValidityFromOwned(
        allocator: std.mem.Allocator,
        data: []f64,
        validity: []u8,
        null_count: usize,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.data_buffer = std.mem.sliceAsBytes(data);
        self.validity_buffer = validity;

        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = @intCast(null_count),
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create an Int64 Arrow array with validity bitmap, taking ownership (no copy)
    pub fn createI64WithValidityFromOwned(
        allocator: std.mem.Allocator,
        data: []i64,
        validity: []u8,
        null_count: usize,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.data_buffer = std.mem.sliceAsBytes(data);
        self.validity_buffer = validity;

        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = @intCast(null_count),
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    // =========================================================================
    // Float32 Creation Methods
    // =========================================================================

    /// Create a Float32 Arrow array from raw data (copies the data)
    pub fn createF32(allocator: std.mem.Allocator, data: []const f32) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(f32);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        self.validity_buffer = null;
        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create a Float32 Arrow array with validity bitmap
    pub fn createF32WithNulls(
        allocator: std.mem.Allocator,
        data: []const f32,
        valid_bitmap: []const u8,
        null_count: i64,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(f32);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        const bitmap_len = (data.len + 7) / 8;
        self.validity_buffer = try allocator.alloc(u8, bitmap_len);
        errdefer allocator.free(self.validity_buffer.?);

        if (valid_bitmap.len >= bitmap_len) {
            @memcpy(self.validity_buffer.?, valid_bitmap[0..bitmap_len]);
        } else {
            @memcpy(self.validity_buffer.?[0..valid_bitmap.len], valid_bitmap);
            @memset(self.validity_buffer.?[valid_bitmap.len..], 0xFF);
        }

        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = null_count,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    // =========================================================================
    // Int32 Creation Methods
    // =========================================================================

    /// Create an Int32 Arrow array from raw data (copies the data)
    pub fn createI32(allocator: std.mem.Allocator, data: []const i32) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(i32);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        self.validity_buffer = null;
        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create an Int32 Arrow array with validity bitmap
    pub fn createI32WithNulls(
        allocator: std.mem.Allocator,
        data: []const i32,
        valid_bitmap: []const u8,
        null_count: i64,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(i32);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        const bitmap_len = (data.len + 7) / 8;
        self.validity_buffer = try allocator.alloc(u8, bitmap_len);
        errdefer allocator.free(self.validity_buffer.?);

        if (valid_bitmap.len >= bitmap_len) {
            @memcpy(self.validity_buffer.?, valid_bitmap[0..bitmap_len]);
        } else {
            @memcpy(self.validity_buffer.?[0..valid_bitmap.len], valid_bitmap);
            @memset(self.validity_buffer.?[valid_bitmap.len..], 0xFF);
        }

        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = null_count,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    // =========================================================================
    // UInt64 Creation Methods
    // =========================================================================

    /// Create a UInt64 Arrow array from raw data (copies the data)
    pub fn createU64(allocator: std.mem.Allocator, data: []const u64) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(u64);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        self.validity_buffer = null;
        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create a UInt64 Arrow array with validity bitmap
    pub fn createU64WithNulls(
        allocator: std.mem.Allocator,
        data: []const u64,
        valid_bitmap: []const u8,
        null_count: i64,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(u64);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        const bitmap_len = (data.len + 7) / 8;
        self.validity_buffer = try allocator.alloc(u8, bitmap_len);
        errdefer allocator.free(self.validity_buffer.?);

        if (valid_bitmap.len >= bitmap_len) {
            @memcpy(self.validity_buffer.?, valid_bitmap[0..bitmap_len]);
        } else {
            @memcpy(self.validity_buffer.?[0..valid_bitmap.len], valid_bitmap);
            @memset(self.validity_buffer.?[valid_bitmap.len..], 0xFF);
        }

        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = null_count,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    // =========================================================================
    // UInt32 Creation Methods
    // =========================================================================

    /// Create a UInt32 Arrow array from raw data (copies the data)
    pub fn createU32(allocator: std.mem.Allocator, data: []const u32) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(u32);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        self.validity_buffer = null;
        self.buffer_ptrs[0] = null;
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = 0,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Create a UInt32 Arrow array with validity bitmap
    pub fn createU32WithNulls(
        allocator: std.mem.Allocator,
        data: []const u32,
        valid_bitmap: []const u8,
        null_count: i64,
    ) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        const data_bytes = data.len * @sizeOf(u32);
        self.data_buffer = try allocator.alloc(u8, data_bytes);
        errdefer allocator.free(self.data_buffer);

        const src_bytes: [*]const u8 = @ptrCast(data.ptr);
        @memcpy(self.data_buffer, src_bytes[0..data_bytes]);

        const bitmap_len = (data.len + 7) / 8;
        self.validity_buffer = try allocator.alloc(u8, bitmap_len);
        errdefer allocator.free(self.validity_buffer.?);

        if (valid_bitmap.len >= bitmap_len) {
            @memcpy(self.validity_buffer.?, valid_bitmap[0..bitmap_len]);
        } else {
            @memcpy(self.validity_buffer.?[0..valid_bitmap.len], valid_bitmap);
            @memset(self.validity_buffer.?[valid_bitmap.len..], 0xFF);
        }

        self.buffer_ptrs[0] = @ptrCast(self.validity_buffer.?.ptr);
        self.buffer_ptrs[1] = @ptrCast(self.data_buffer.ptr);

        self.array = ArrowArray{
            .length = @intCast(data.len),
            .null_count = null_count,
            .offset = 0,
            .n_buffers = 2,
            .n_children = 0,
            .buffers = &self.buffer_ptrs,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };

        self.allocator = allocator;
        return self;
    }

    /// Get the underlying ArrowArray pointer for SIMD operations
    pub fn getArray(self: *Self) *const ArrowArray {
        return &self.array;
    }

    /// Get length
    pub fn len(self: *const Self) usize {
        return @intCast(self.array.length);
    }

    /// Get null count
    pub fn nullCount(self: *const Self) i64 {
        return self.array.null_count;
    }

    /// Check if has nulls
    pub fn hasNulls(self: *const Self) bool {
        return self.array.null_count > 0;
    }

    /// Sum (for Float64 arrays)
    pub fn sumF64(self: *const Self) f64 {
        return sumFloat64(&self.array);
    }

    /// Min (for Float64 arrays)
    pub fn minF64(self: *const Self) f64 {
        return minFloat64(&self.array);
    }

    /// Max (for Float64 arrays)
    pub fn maxF64(self: *const Self) f64 {
        return maxFloat64(&self.array);
    }

    /// Mean (for Float64 arrays)
    pub fn meanF64(self: *const Self) f64 {
        return meanFloat64(&self.array);
    }

    /// Sum (for Int64 arrays)
    pub fn sumI64(self: *const Self) i64 {
        return sumInt64(&self.array);
    }

    /// Min (for Int64 arrays)
    pub fn minI64(self: *const Self) i64 {
        return minInt64(&self.array);
    }

    /// Max (for Int64 arrays)
    pub fn maxI64(self: *const Self) i64 {
        return maxInt64(&self.array);
    }

    // --- Float32 Aggregations ---

    /// Sum (for Float32 arrays)
    pub fn sumF32(self: *const Self) f32 {
        return sumFloat32(&self.array);
    }

    /// Min (for Float32 arrays)
    pub fn minF32(self: *const Self) f32 {
        return minFloat32(&self.array);
    }

    /// Max (for Float32 arrays)
    pub fn maxF32(self: *const Self) f32 {
        return maxFloat32(&self.array);
    }

    /// Mean (for Float32 arrays)
    pub fn meanF32(self: *const Self) f32 {
        return meanFloat32(&self.array);
    }

    // --- Int32 Aggregations ---

    /// Sum (for Int32 arrays)
    pub fn sumI32(self: *const Self) i32 {
        return sumInt32(&self.array);
    }

    /// Min (for Int32 arrays)
    pub fn minI32(self: *const Self) i32 {
        return minInt32(&self.array);
    }

    /// Max (for Int32 arrays)
    pub fn maxI32(self: *const Self) i32 {
        return maxInt32(&self.array);
    }

    // --- UInt64 Aggregations ---

    /// Sum (for UInt64 arrays)
    pub fn sumU64(self: *const Self) u64 {
        return sumUInt64(&self.array);
    }

    /// Min (for UInt64 arrays)
    pub fn minU64(self: *const Self) u64 {
        return minUInt64(&self.array);
    }

    /// Max (for UInt64 arrays)
    pub fn maxU64(self: *const Self) u64 {
        return maxUInt64(&self.array);
    }

    // --- UInt32 Aggregations ---

    /// Sum (for UInt32 arrays)
    pub fn sumU32(self: *const Self) u32 {
        return sumUInt32(&self.array);
    }

    /// Min (for UInt32 arrays)
    pub fn minU32(self: *const Self) u32 {
        return minUInt32(&self.array);
    }

    /// Max (for UInt32 arrays)
    pub fn maxU32(self: *const Self) u32 {
        return maxUInt32(&self.array);
    }

    // --- Sort Operations ---

    /// Argsort Float64 array - returns indices that would sort the array
    /// Caller owns the returned slice and must free it
    pub fn argsortF64(self: *const Self, ascending: bool) ![]u32 {
        const length = self.len();
        if (length == 0) return &[_]u32{};

        const indices = try self.allocator.alloc(u32, length);
        errdefer self.allocator.free(indices);

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        simd.argsortF64(data, indices, ascending);

        return indices;
    }

    /// Argsort Int64 array - returns indices that would sort the array
    pub fn argsortI64(self: *const Self, ascending: bool) ![]u32 {
        const length = self.len();
        if (length == 0) return &[_]u32{};

        const indices = try self.allocator.alloc(u32, length);
        errdefer self.allocator.free(indices);

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        simd.argsortI64(data, indices, ascending);

        return indices;
    }

    /// Sort Float64 array - returns new sorted ManagedArrowArray
    /// Note: Does not preserve null positions (nulls are treated as values)
    /// Uses direct radix sort for optimal performance (no index indirection)
    pub fn sortF64(self: *const Self, ascending: bool) !*Self {
        const length = self.len();
        if (length == 0) {
            return createF64(self.allocator, &[_]f64{});
        }

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;

        // Use direct sort - much faster than argsort + gather
        const sorted_data = try self.allocator.alloc(f64, length);
        defer self.allocator.free(sorted_data);
        simd.sortF64(data, sorted_data, ascending);

        // Create new ManagedArrowArray with sorted data
        return createF64(self.allocator, sorted_data);
    }

    /// Sort Int64 array - returns new sorted ManagedArrowArray
    /// Uses direct radix sort for optimal performance (no index indirection)
    pub fn sortI64(self: *const Self, ascending: bool) !*Self {
        const length = self.len();
        if (length == 0) {
            return createI64(self.allocator, &[_]i64{});
        }

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;

        // Use direct sort - much faster than argsort + gather
        const sorted_data = try self.allocator.alloc(i64, length);
        defer self.allocator.free(sorted_data);
        simd.sortI64(data, sorted_data, ascending);

        // Create new ManagedArrowArray with sorted data
        return createI64(self.allocator, sorted_data);
    }

    /// Argsort Float32 array - returns indices that would sort the array
    pub fn argsortF32(self: *const Self, ascending: bool) ![]u32 {
        const length = self.len();
        if (length == 0) return &[_]u32{};

        const indices = try self.allocator.alloc(u32, length);
        errdefer self.allocator.free(indices);

        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        simd.argsortF32(data, indices, ascending);

        return indices;
    }

    /// Argsort Int32 array
    pub fn argsortI32(self: *const Self, ascending: bool) ![]u32 {
        const length = self.len();
        if (length == 0) return &[_]u32{};

        const indices = try self.allocator.alloc(u32, length);
        errdefer self.allocator.free(indices);

        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        simd.argsortI32(data, indices, ascending);

        return indices;
    }

    /// Argsort UInt64 array
    pub fn argsortU64(self: *const Self, ascending: bool) ![]u32 {
        const length = self.len();
        if (length == 0) return &[_]u32{};

        const indices = try self.allocator.alloc(u32, length);
        errdefer self.allocator.free(indices);

        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        simd.argsortU64(data, indices, ascending);

        return indices;
    }

    /// Argsort UInt32 array
    pub fn argsortU32(self: *const Self, ascending: bool) ![]u32 {
        const length = self.len();
        if (length == 0) return &[_]u32{};

        const indices = try self.allocator.alloc(u32, length);
        errdefer self.allocator.free(indices);

        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        simd.argsortU32(data, indices, ascending);

        return indices;
    }

    /// Sort Float32 array - returns new sorted ManagedArrowArray
    pub fn sortF32(self: *const Self, ascending: bool) !*Self {
        const length = self.len();
        if (length == 0) {
            return createF32(self.allocator, &[_]f32{});
        }

        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;

        const indices = try self.allocator.alloc(u32, length);
        defer self.allocator.free(indices);
        simd.argsortF32(data, indices, ascending);

        const sorted_data = try self.allocator.alloc(f32, length);
        defer self.allocator.free(sorted_data);

        for (indices, 0..) |idx, i| {
            sorted_data[i] = data[idx];
        }

        return createF32(self.allocator, sorted_data);
    }

    /// Sort Int32 array - returns new sorted ManagedArrowArray
    pub fn sortI32(self: *const Self, ascending: bool) !*Self {
        const length = self.len();
        if (length == 0) {
            return createI32(self.allocator, &[_]i32{});
        }

        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;

        const indices = try self.allocator.alloc(u32, length);
        defer self.allocator.free(indices);
        simd.argsortI32(data, indices, ascending);

        const sorted_data = try self.allocator.alloc(i32, length);
        defer self.allocator.free(sorted_data);

        for (indices, 0..) |idx, i| {
            sorted_data[i] = data[idx];
        }

        return createI32(self.allocator, sorted_data);
    }

    /// Sort UInt64 array - returns new sorted ManagedArrowArray
    pub fn sortU64(self: *const Self, ascending: bool) !*Self {
        const length = self.len();
        if (length == 0) {
            return createU64(self.allocator, &[_]u64{});
        }

        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;

        const indices = try self.allocator.alloc(u32, length);
        defer self.allocator.free(indices);
        simd.argsortU64(data, indices, ascending);

        const sorted_data = try self.allocator.alloc(u64, length);
        defer self.allocator.free(sorted_data);

        for (indices, 0..) |idx, i| {
            sorted_data[i] = data[idx];
        }

        return createU64(self.allocator, sorted_data);
    }

    /// Sort UInt32 array - returns new sorted ManagedArrowArray
    pub fn sortU32(self: *const Self, ascending: bool) !*Self {
        const length = self.len();
        if (length == 0) {
            return createU32(self.allocator, &[_]u32{});
        }

        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;

        const indices = try self.allocator.alloc(u32, length);
        defer self.allocator.free(indices);
        simd.argsortU32(data, indices, ascending);

        const sorted_data = try self.allocator.alloc(u32, length);
        defer self.allocator.free(sorted_data);

        for (indices, 0..) |idx, i| {
            sorted_data[i] = data[idx];
        }

        return createU32(self.allocator, sorted_data);
    }

    // --- Data Access Operations ---

    /// Check if value at index is valid (not null)
    pub fn isValidAt(self: *const Self, index: usize) bool {
        if (index >= self.len()) return false;
        if (!self.hasNulls()) return true;
        return isValid(self.validity_buffer, index);
    }

    /// Get Float64 value at index
    /// Returns null if index is out of bounds or value is null
    pub fn getF64(self: *const Self, index: usize) ?f64 {
        if (index >= self.len()) return null;
        if (!self.isValidAt(index)) return null;

        const data = getFloat64Buffer(&self.array) orelse return null;
        return data[index];
    }

    /// Get Int64 value at index
    pub fn getI64(self: *const Self, index: usize) ?i64 {
        if (index >= self.len()) return null;
        if (!self.isValidAt(index)) return null;

        const data = getInt64Buffer(&self.array) orelse return null;
        return data[index];
    }

    /// Get raw pointer to Float64 data buffer
    /// Returns null if not a float64 array or buffer is empty
    pub fn getDataPtrF64(self: *const Self) ?[*]const f64 {
        const data = getFloat64Buffer(&self.array) orelse return null;
        return data.ptr;
    }

    /// Get raw pointer to Int64 data buffer
    pub fn getDataPtrI64(self: *const Self) ?[*]const i64 {
        const data = getInt64Buffer(&self.array) orelse return null;
        return data.ptr;
    }

    /// Get Float32 value at index
    pub fn getF32(self: *const Self, index: usize) ?f32 {
        if (index >= self.len()) return null;
        if (!self.isValidAt(index)) return null;

        const data = getFloat32Buffer(&self.array) orelse return null;
        return data[index];
    }

    /// Get Int32 value at index
    pub fn getI32(self: *const Self, index: usize) ?i32 {
        if (index >= self.len()) return null;
        if (!self.isValidAt(index)) return null;

        const data = getInt32Buffer(&self.array) orelse return null;
        return data[index];
    }

    /// Get UInt64 value at index
    pub fn getU64(self: *const Self, index: usize) ?u64 {
        if (index >= self.len()) return null;
        if (!self.isValidAt(index)) return null;

        const data = getUInt64Buffer(&self.array) orelse return null;
        return data[index];
    }

    /// Get UInt32 value at index
    pub fn getU32(self: *const Self, index: usize) ?u32 {
        if (index >= self.len()) return null;
        if (!self.isValidAt(index)) return null;

        const data = getUInt32Buffer(&self.array) orelse return null;
        return data[index];
    }

    /// Get raw pointer to Float32 data buffer
    pub fn getDataPtrF32(self: *const Self) ?[*]const f32 {
        const data = getFloat32Buffer(&self.array) orelse return null;
        return data.ptr;
    }

    /// Get raw pointer to Int32 data buffer
    pub fn getDataPtrI32(self: *const Self) ?[*]const i32 {
        const data = getInt32Buffer(&self.array) orelse return null;
        return data.ptr;
    }

    /// Get raw pointer to UInt64 data buffer
    pub fn getDataPtrU64(self: *const Self) ?[*]const u64 {
        const data = getUInt64Buffer(&self.array) orelse return null;
        return data.ptr;
    }

    /// Get raw pointer to UInt32 data buffer
    pub fn getDataPtrU32(self: *const Self) ?[*]const u32 {
        const data = getUInt32Buffer(&self.array) orelse return null;
        return data.ptr;
    }

    /// Create a slice of Float64 array [start, end)
    /// Returns a new ManagedArrowArray with copied data
    pub fn sliceF64(self: *const Self, start: usize, end: usize) !*Self {
        const length = self.len();
        const actual_start = @min(start, length);
        const actual_end = @min(end, length);

        if (actual_start >= actual_end) {
            return createF64(self.allocator, &[_]f64{});
        }

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const slice_data = data[actual_start..actual_end];

        // Handle validity bitmap if present
        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const slice_len = actual_end - actual_start;
            const bitmap_len = (slice_len + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);

            // Copy and shift validity bits
            var null_count: i64 = 0;
            for (0..slice_len) |i| {
                const src_idx = actual_start + i;
                const is_valid_bit = isValid(validity, src_idx);
                if (is_valid_bit) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }

            return createF64WithNulls(self.allocator, slice_data, new_bitmap, null_count);
        }

        return createF64(self.allocator, slice_data);
    }

    /// Create a slice of Int64 array [start, end)
    pub fn sliceI64(self: *const Self, start: usize, end: usize) !*Self {
        const length = self.len();
        const actual_start = @min(start, length);
        const actual_end = @min(end, length);

        if (actual_start >= actual_end) {
            return createI64(self.allocator, &[_]i64{});
        }

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const slice_data = data[actual_start..actual_end];

        // Handle validity bitmap if present
        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const slice_len = actual_end - actual_start;
            const bitmap_len = (slice_len + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);

            // Copy and shift validity bits
            var null_count: i64 = 0;
            for (0..slice_len) |i| {
                const src_idx = actual_start + i;
                const is_valid_bit = isValid(validity, src_idx);
                if (is_valid_bit) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }

            return createI64WithNulls(self.allocator, slice_data, new_bitmap, null_count);
        }

        return createI64(self.allocator, slice_data);
    }

    /// Copy Float64 data to caller-provided buffer
    /// Returns number of elements copied
    pub fn copyToF64(self: *const Self, dest: []f64) usize {
        const data = getFloat64Buffer(&self.array) orelse return 0;
        const copy_len = @min(data.len, dest.len);
        @memcpy(dest[0..copy_len], data[0..copy_len]);
        return copy_len;
    }

    /// Copy Int64 data to caller-provided buffer
    pub fn copyToI64(self: *const Self, dest: []i64) usize {
        const data = getInt64Buffer(&self.array) orelse return 0;
        const copy_len = @min(data.len, dest.len);
        @memcpy(dest[0..copy_len], data[0..copy_len]);
        return copy_len;
    }

    /// Create a slice of Float32 array [start, end)
    pub fn sliceF32(self: *const Self, start: usize, end: usize) !*Self {
        const length = self.len();
        const actual_start = @min(start, length);
        const actual_end = @min(end, length);

        if (actual_start >= actual_end) {
            return createF32(self.allocator, &[_]f32{});
        }

        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const slice_data = data[actual_start..actual_end];

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const slice_len = actual_end - actual_start;
            const bitmap_len = (slice_len + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);

            var null_count: i64 = 0;
            for (0..slice_len) |i| {
                const src_idx = actual_start + i;
                const is_valid_bit = isValid(validity, src_idx);
                if (is_valid_bit) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }

            return createF32WithNulls(self.allocator, slice_data, new_bitmap, null_count);
        }

        return createF32(self.allocator, slice_data);
    }

    /// Create a slice of Int32 array [start, end)
    pub fn sliceI32(self: *const Self, start: usize, end: usize) !*Self {
        const length = self.len();
        const actual_start = @min(start, length);
        const actual_end = @min(end, length);

        if (actual_start >= actual_end) {
            return createI32(self.allocator, &[_]i32{});
        }

        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const slice_data = data[actual_start..actual_end];

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const slice_len = actual_end - actual_start;
            const bitmap_len = (slice_len + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);

            var null_count: i64 = 0;
            for (0..slice_len) |i| {
                const src_idx = actual_start + i;
                const is_valid_bit = isValid(validity, src_idx);
                if (is_valid_bit) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }

            return createI32WithNulls(self.allocator, slice_data, new_bitmap, null_count);
        }

        return createI32(self.allocator, slice_data);
    }

    /// Create a slice of UInt64 array [start, end)
    pub fn sliceU64(self: *const Self, start: usize, end: usize) !*Self {
        const length = self.len();
        const actual_start = @min(start, length);
        const actual_end = @min(end, length);

        if (actual_start >= actual_end) {
            return createU64(self.allocator, &[_]u64{});
        }

        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const slice_data = data[actual_start..actual_end];

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const slice_len = actual_end - actual_start;
            const bitmap_len = (slice_len + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);

            var null_count: i64 = 0;
            for (0..slice_len) |i| {
                const src_idx = actual_start + i;
                const is_valid_bit = isValid(validity, src_idx);
                if (is_valid_bit) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }

            return createU64WithNulls(self.allocator, slice_data, new_bitmap, null_count);
        }

        return createU64(self.allocator, slice_data);
    }

    /// Create a slice of UInt32 array [start, end)
    pub fn sliceU32(self: *const Self, start: usize, end: usize) !*Self {
        const length = self.len();
        const actual_start = @min(start, length);
        const actual_end = @min(end, length);

        if (actual_start >= actual_end) {
            return createU32(self.allocator, &[_]u32{});
        }

        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const slice_data = data[actual_start..actual_end];

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const slice_len = actual_end - actual_start;
            const bitmap_len = (slice_len + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);

            var null_count: i64 = 0;
            for (0..slice_len) |i| {
                const src_idx = actual_start + i;
                const is_valid_bit = isValid(validity, src_idx);
                if (is_valid_bit) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }

            return createU32WithNulls(self.allocator, slice_data, new_bitmap, null_count);
        }

        return createU32(self.allocator, slice_data);
    }

    /// Copy Float32 data to caller-provided buffer
    pub fn copyToF32(self: *const Self, dest: []f32) usize {
        const data = getFloat32Buffer(&self.array) orelse return 0;
        const copy_len = @min(data.len, dest.len);
        @memcpy(dest[0..copy_len], data[0..copy_len]);
        return copy_len;
    }

    /// Copy Int32 data to caller-provided buffer
    pub fn copyToI32(self: *const Self, dest: []i32) usize {
        const data = getInt32Buffer(&self.array) orelse return 0;
        const copy_len = @min(data.len, dest.len);
        @memcpy(dest[0..copy_len], data[0..copy_len]);
        return copy_len;
    }

    /// Copy UInt64 data to caller-provided buffer
    pub fn copyToU64(self: *const Self, dest: []u64) usize {
        const data = getUInt64Buffer(&self.array) orelse return 0;
        const copy_len = @min(data.len, dest.len);
        @memcpy(dest[0..copy_len], data[0..copy_len]);
        return copy_len;
    }

    /// Copy UInt32 data to caller-provided buffer
    pub fn copyToU32(self: *const Self, dest: []u32) usize {
        const data = getUInt32Buffer(&self.array) orelse return 0;
        const copy_len = @min(data.len, dest.len);
        @memcpy(dest[0..copy_len], data[0..copy_len]);
        return copy_len;
    }

    // --- Filter Operations ---

    /// Compare Float64 values greater than scalar, returns boolean mask
    /// Caller owns the returned slice
    pub fn gtF64(self: *const Self, value: f64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v > value;
        }

        return mask;
    }

    /// Compare Float64 values greater than or equal to scalar
    pub fn geF64(self: *const Self, value: f64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v >= value;
        }

        return mask;
    }

    /// Compare Float64 values less than scalar
    pub fn ltF64(self: *const Self, value: f64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v < value;
        }

        return mask;
    }

    /// Compare Float64 values less than or equal to scalar
    pub fn leF64(self: *const Self, value: f64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v <= value;
        }

        return mask;
    }

    /// Compare Float64 values equal to scalar
    pub fn eqF64(self: *const Self, value: f64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v == value;
        }

        return mask;
    }

    /// Compare Float64 values not equal to scalar
    pub fn neF64(self: *const Self, value: f64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v != value;
        }

        return mask;
    }

    /// Compare Int64 values greater than scalar
    pub fn gtI64(self: *const Self, value: i64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v > value;
        }

        return mask;
    }

    /// Compare Int64 values greater than or equal to scalar
    pub fn geI64(self: *const Self, value: i64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v >= value;
        }

        return mask;
    }

    /// Compare Int64 values less than scalar
    pub fn ltI64(self: *const Self, value: i64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v < value;
        }

        return mask;
    }

    /// Compare Int64 values less than or equal to scalar
    pub fn leI64(self: *const Self, value: i64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v <= value;
        }

        return mask;
    }

    /// Compare Int64 values equal to scalar
    pub fn eqI64(self: *const Self, value: i64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v == value;
        }

        return mask;
    }

    /// Compare Int64 values not equal to scalar
    pub fn neI64(self: *const Self, value: i64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);

        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v != value;
        }

        return mask;
    }

    // --- Float32 Comparisons ---

    pub fn gtF32(self: *const Self, value: f32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v > value;
        }
        return mask;
    }

    pub fn geF32(self: *const Self, value: f32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v >= value;
        }
        return mask;
    }

    pub fn ltF32(self: *const Self, value: f32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v < value;
        }
        return mask;
    }

    pub fn leF32(self: *const Self, value: f32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v <= value;
        }
        return mask;
    }

    pub fn eqF32(self: *const Self, value: f32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v == value;
        }
        return mask;
    }

    pub fn neF32(self: *const Self, value: f32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getFloat32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v != value;
        }
        return mask;
    }

    // --- Int32 Comparisons ---

    pub fn gtI32(self: *const Self, value: i32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v > value;
        }
        return mask;
    }

    pub fn geI32(self: *const Self, value: i32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v >= value;
        }
        return mask;
    }

    pub fn ltI32(self: *const Self, value: i32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v < value;
        }
        return mask;
    }

    pub fn leI32(self: *const Self, value: i32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v <= value;
        }
        return mask;
    }

    pub fn eqI32(self: *const Self, value: i32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v == value;
        }
        return mask;
    }

    pub fn neI32(self: *const Self, value: i32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v != value;
        }
        return mask;
    }

    // --- UInt64 Comparisons ---

    pub fn gtU64(self: *const Self, value: u64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v > value;
        }
        return mask;
    }

    pub fn geU64(self: *const Self, value: u64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v >= value;
        }
        return mask;
    }

    pub fn ltU64(self: *const Self, value: u64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v < value;
        }
        return mask;
    }

    pub fn leU64(self: *const Self, value: u64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v <= value;
        }
        return mask;
    }

    pub fn eqU64(self: *const Self, value: u64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v == value;
        }
        return mask;
    }

    pub fn neU64(self: *const Self, value: u64) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v != value;
        }
        return mask;
    }

    // --- UInt32 Comparisons ---

    pub fn gtU32(self: *const Self, value: u32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v > value;
        }
        return mask;
    }

    pub fn geU32(self: *const Self, value: u32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v >= value;
        }
        return mask;
    }

    pub fn ltU32(self: *const Self, value: u32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v < value;
        }
        return mask;
    }

    pub fn leU32(self: *const Self, value: u32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v <= value;
        }
        return mask;
    }

    pub fn eqU32(self: *const Self, value: u32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v == value;
        }
        return mask;
    }

    pub fn neU32(self: *const Self, value: u32) ![]bool {
        const length = self.len();
        if (length == 0) return &[_]bool{};
        const data = getUInt32Buffer(&self.array) orelse return error.InvalidData;
        const mask = try self.allocator.alloc(bool, length);
        errdefer self.allocator.free(mask);
        const validity = self.validity_buffer;
        for (data, 0..) |v, i| {
            mask[i] = isValid(validity, i) and v != value;
        }
        return mask;
    }

    /// Filter Float64 array by boolean mask
    /// Returns a new ManagedArrowArray with only elements where mask is true
    pub fn filterF64(self: *const Self, mask: []const bool) !*Self {
        const length = self.len();
        if (length == 0 or mask.len == 0) {
            return createF64(self.allocator, &[_]f64{});
        }

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const mask_len = @min(length, mask.len);

        // Count true values
        var count: usize = 0;
        for (mask[0..mask_len]) |m| {
            if (m) count += 1;
        }

        if (count == 0) {
            return createF64(self.allocator, &[_]f64{});
        }

        // Allocate result
        const result_data = try self.allocator.alloc(f64, count);
        defer self.allocator.free(result_data);

        // Copy filtered values
        var j: usize = 0;
        for (0..mask_len) |i| {
            if (mask[i]) {
                result_data[j] = data[i];
                j += 1;
            }
        }

        // Handle validity bitmap if present
        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const new_bitmap_len = (count + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, new_bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            j = 0;
            for (0..mask_len) |i| {
                if (mask[i]) {
                    if (isValid(validity, i)) {
                        const byte_idx = j / 8;
                        const bit_idx: u3 = @intCast(j % 8);
                        new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                    } else {
                        null_count += 1;
                    }
                    j += 1;
                }
            }

            return createF64WithNulls(self.allocator, result_data, new_bitmap, null_count);
        }

        return createF64(self.allocator, result_data);
    }

    /// Filter Int64 array by boolean mask
    pub fn filterI64(self: *const Self, mask: []const bool) !*Self {
        const length = self.len();
        if (length == 0 or mask.len == 0) {
            return createI64(self.allocator, &[_]i64{});
        }

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const mask_len = @min(length, mask.len);

        // Count true values
        var count: usize = 0;
        for (mask[0..mask_len]) |m| {
            if (m) count += 1;
        }

        if (count == 0) {
            return createI64(self.allocator, &[_]i64{});
        }

        // Allocate result
        const result_data = try self.allocator.alloc(i64, count);
        defer self.allocator.free(result_data);

        // Copy filtered values
        var j: usize = 0;
        for (0..mask_len) |i| {
            if (mask[i]) {
                result_data[j] = data[i];
                j += 1;
            }
        }

        // Handle validity bitmap if present
        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const new_bitmap_len = (count + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, new_bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            j = 0;
            for (0..mask_len) |i| {
                if (mask[i]) {
                    if (isValid(validity, i)) {
                        const byte_idx = j / 8;
                        const bit_idx: u3 = @intCast(j % 8);
                        new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                    } else {
                        null_count += 1;
                    }
                    j += 1;
                }
            }

            return createI64WithNulls(self.allocator, result_data, new_bitmap, null_count);
        }

        return createI64(self.allocator, result_data);
    }

    /// Count elements where mask is true
    pub fn countMask(mask: []const bool) usize {
        var count: usize = 0;
        for (mask) |m| {
            if (m) count += 1;
        }
        return count;
    }

    // --- Arithmetic Operations ---

    /// Add two Float64 arrays element-wise
    /// Arrays must have the same length
    pub fn addF64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data1 = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getFloat64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] + data2[i];
        }

        // Handle nulls: result is null if either operand is null
        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createF64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Subtract two Float64 arrays element-wise (self - other)
    pub fn subF64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data1 = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getFloat64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] - data2[i];
        }

        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createF64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Multiply two Float64 arrays element-wise
    pub fn mulF64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data1 = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getFloat64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] * data2[i];
        }

        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createF64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Divide two Float64 arrays element-wise (self / other)
    pub fn divF64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data1 = getFloat64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getFloat64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] / data2[i]; // Division by zero produces inf/nan
        }

        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createF64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Add scalar to Float64 array
    pub fn addScalarF64(self: *const Self, value: f64) !*Self {
        const length = self.len();
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data[i] + value;
        }

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memcpy(new_bitmap, validity[0..bitmap_len]);
            return createF64WithNulls(self.allocator, result, new_bitmap, self.array.null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Subtract scalar from Float64 array (self - value)
    pub fn subScalarF64(self: *const Self, value: f64) !*Self {
        return self.addScalarF64(-value);
    }

    /// Multiply Float64 array by scalar
    pub fn mulScalarF64(self: *const Self, value: f64) !*Self {
        const length = self.len();
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data[i] * value;
        }

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memcpy(new_bitmap, validity[0..bitmap_len]);
            return createF64WithNulls(self.allocator, result, new_bitmap, self.array.null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Divide Float64 array by scalar (self / value)
    pub fn divScalarF64(self: *const Self, value: f64) !*Self {
        const length = self.len();
        if (length == 0) return createF64(self.allocator, &[_]f64{});

        const data = getFloat64Buffer(&self.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(f64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data[i] / value;
        }

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memcpy(new_bitmap, validity[0..bitmap_len]);
            return createF64WithNulls(self.allocator, result, new_bitmap, self.array.null_count);
        }

        return createF64(self.allocator, result);
    }

    /// Add two Int64 arrays element-wise
    pub fn addI64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createI64(self.allocator, &[_]i64{});

        const data1 = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getInt64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(i64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] + data2[i];
        }

        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createI64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createI64(self.allocator, result);
    }

    /// Subtract two Int64 arrays element-wise (self - other)
    pub fn subI64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createI64(self.allocator, &[_]i64{});

        const data1 = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getInt64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(i64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] - data2[i];
        }

        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createI64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createI64(self.allocator, result);
    }

    /// Multiply two Int64 arrays element-wise
    pub fn mulI64(self: *const Self, other: *const Self) !*Self {
        const length = self.len();
        if (length != other.len()) return error.LengthMismatch;
        if (length == 0) return createI64(self.allocator, &[_]i64{});

        const data1 = getInt64Buffer(&self.array) orelse return error.InvalidData;
        const data2 = getInt64Buffer(&other.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(i64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data1[i] * data2[i];
        }

        if (self.hasNulls() or other.hasNulls()) {
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memset(new_bitmap, 0);

            var null_count: i64 = 0;
            for (0..length) |i| {
                const v1 = isValid(self.validity_buffer, i);
                const v2 = isValid(other.validity_buffer, i);
                if (v1 and v2) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = @intCast(i % 8);
                    new_bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
                } else {
                    null_count += 1;
                }
            }
            return createI64WithNulls(self.allocator, result, new_bitmap, null_count);
        }

        return createI64(self.allocator, result);
    }

    /// Add scalar to Int64 array
    pub fn addScalarI64(self: *const Self, value: i64) !*Self {
        const length = self.len();
        if (length == 0) return createI64(self.allocator, &[_]i64{});

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(i64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data[i] + value;
        }

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memcpy(new_bitmap, validity[0..bitmap_len]);
            return createI64WithNulls(self.allocator, result, new_bitmap, self.array.null_count);
        }

        return createI64(self.allocator, result);
    }

    /// Multiply Int64 array by scalar
    pub fn mulScalarI64(self: *const Self, value: i64) !*Self {
        const length = self.len();
        if (length == 0) return createI64(self.allocator, &[_]i64{});

        const data = getInt64Buffer(&self.array) orelse return error.InvalidData;

        const result = try self.allocator.alloc(i64, length);
        defer self.allocator.free(result);

        for (0..length) |i| {
            result[i] = data[i] * value;
        }

        if (self.hasNulls()) {
            const validity = self.validity_buffer orelse return error.InvalidData;
            const bitmap_len = (length + 7) / 8;
            const new_bitmap = try self.allocator.alloc(u8, bitmap_len);
            defer self.allocator.free(new_bitmap);
            @memcpy(new_bitmap, validity[0..bitmap_len]);
            return createI64WithNulls(self.allocator, result, new_bitmap, self.array.null_count);
        }

        return createI64(self.allocator, result);
    }

    /// Free all owned memory
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.data_buffer);
        if (self.validity_buffer) |vb| {
            self.allocator.free(vb);
        }
        self.allocator.destroy(self);
    }
};

// ============================================================================
// Arrow GroupBy Results
// ============================================================================

const groupby = @import("groupby.zig");

/// Result of a groupby sum operation on Arrow arrays
pub const GroupBySumResult = struct {
    /// Unique key values (one per group) as ManagedArrowArray
    keys: *ManagedArrowArray,
    /// Sum values for each group as ManagedArrowArray
    sums: *ManagedArrowArray,
    /// Number of groups
    num_groups: u32,
    /// Allocator for cleanup
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GroupBySumResult) void {
        self.keys.deinit();
        self.sums.deinit();
        self.allocator.destroy(self);
    }
};

/// Result of a groupby multi-aggregation operation on Arrow arrays
pub const GroupByMultiAggResult = struct {
    /// Unique key values (one per group) as ManagedArrowArray
    keys: *ManagedArrowArray,
    /// Sum values for each group
    sums: *ManagedArrowArray,
    /// Min values for each group
    mins: *ManagedArrowArray,
    /// Max values for each group
    maxs: *ManagedArrowArray,
    /// Count values for each group (as Int64)
    counts: *ManagedArrowArray,
    /// Number of groups
    num_groups: u32,
    /// Allocator for cleanup
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GroupByMultiAggResult) void {
        self.keys.deinit();
        self.sums.deinit();
        self.mins.deinit();
        self.maxs.deinit();
        self.counts.deinit();
        self.allocator.destroy(self);
    }
};

/// Perform groupby sum aggregation on Arrow arrays
/// Keys: Int64 array, Values: Float64 array
pub fn arrowGroupBySumI64KeyF64Value(
    allocator: std.mem.Allocator,
    keys: *const ManagedArrowArray,
    values: *const ManagedArrowArray,
) !*GroupBySumResult {
    // Get the data buffers
    const key_data = getInt64Buffer(&keys.array) orelse return error.InvalidData;
    const value_data = getFloat64Buffer(&values.array) orelse return error.InvalidData;

    if (key_data.len != value_data.len) return error.LengthMismatch;

    // Perform the groupby using existing implementation
    const gb_result = try groupby.groupbySumI64KeyF64Value(allocator, key_data, value_data);
    defer {
        allocator.free(gb_result.keys);
        allocator.free(gb_result.sums);
    }

    // Create ManagedArrowArrays for the results
    const keys_arr = try ManagedArrowArray.createI64(allocator, gb_result.keys);
    errdefer keys_arr.deinit();

    const sums_arr = try ManagedArrowArray.createF64(allocator, gb_result.sums);
    errdefer sums_arr.deinit();

    // Create the result struct
    const result = try allocator.create(GroupBySumResult);
    result.* = GroupBySumResult{
        .keys = keys_arr,
        .sums = sums_arr,
        .num_groups = gb_result.num_groups,
        .allocator = allocator,
    };

    return result;
}

/// Perform groupby with multiple aggregations on Arrow arrays
/// Keys: Int64 array, Values: Float64 array
/// Returns sum, min, max, count for each group
pub fn arrowGroupByMultiAggI64KeyF64Value(
    allocator: std.mem.Allocator,
    keys: *const ManagedArrowArray,
    values: *const ManagedArrowArray,
) !*GroupByMultiAggResult {
    // Get the data buffers
    const key_data = getInt64Buffer(&keys.array) orelse return error.InvalidData;
    const value_data = getFloat64Buffer(&values.array) orelse return error.InvalidData;

    if (key_data.len != value_data.len) return error.LengthMismatch;

    // Perform the groupby using existing implementation
    var gb_result = try groupby.groupbyMultiAggI64KeyF64Value(allocator, key_data, value_data);
    defer {
        allocator.free(gb_result.keys);
        allocator.free(gb_result.sums);
        allocator.free(gb_result.mins);
        allocator.free(gb_result.maxs);
        allocator.free(gb_result.counts);
    }

    // Convert u64 counts to i64 for Arrow compatibility
    const counts_i64 = try allocator.alloc(i64, gb_result.num_groups);
    defer allocator.free(counts_i64);
    for (gb_result.counts[0..gb_result.num_groups], 0..) |c, i| {
        counts_i64[i] = @intCast(c);
    }

    // Create ManagedArrowArrays for the results
    const keys_arr = try ManagedArrowArray.createI64(allocator, gb_result.keys);
    errdefer keys_arr.deinit();

    const sums_arr = try ManagedArrowArray.createF64(allocator, gb_result.sums);
    errdefer sums_arr.deinit();

    const mins_arr = try ManagedArrowArray.createF64(allocator, gb_result.mins);
    errdefer mins_arr.deinit();

    const maxs_arr = try ManagedArrowArray.createF64(allocator, gb_result.maxs);
    errdefer maxs_arr.deinit();

    const counts_arr = try ManagedArrowArray.createI64(allocator, counts_i64);
    errdefer counts_arr.deinit();

    // Create the result struct
    const result = try allocator.create(GroupByMultiAggResult);
    result.* = GroupByMultiAggResult{
        .keys = keys_arr,
        .sums = sums_arr,
        .mins = mins_arr,
        .maxs = maxs_arr,
        .counts = counts_arr,
        .num_groups = gb_result.num_groups,
        .allocator = allocator,
    };

    return result;
}

/// Perform groupby count operation on Arrow arrays
/// Keys: Int64 array
/// Returns unique keys and their counts
pub fn arrowGroupByCountI64Key(
    allocator: std.mem.Allocator,
    keys: *const ManagedArrowArray,
) !*GroupBySumResult {
    // Get the key data
    const key_data = getInt64Buffer(&keys.array) orelse return error.InvalidData;

    // Create dummy values of 1.0 for count (sum of 1s = count)
    const ones = try allocator.alloc(f64, key_data.len);
    defer allocator.free(ones);
    @memset(ones, 1.0);

    // Perform the groupby sum
    const gb_result = try groupby.groupbySumI64KeyF64Value(allocator, key_data, ones);
    defer {
        allocator.free(gb_result.keys);
        allocator.free(gb_result.sums);
    }

    // Create ManagedArrowArrays for the results
    const keys_arr = try ManagedArrowArray.createI64(allocator, gb_result.keys);
    errdefer keys_arr.deinit();

    // Convert sums to counts (they should be integer values)
    const sums_arr = try ManagedArrowArray.createF64(allocator, gb_result.sums);
    errdefer sums_arr.deinit();

    // Create the result struct (using GroupBySumResult for counts)
    const result = try allocator.create(GroupBySumResult);
    result.* = GroupBySumResult{
        .keys = keys_arr,
        .sums = sums_arr, // sums contains counts as f64
        .num_groups = gb_result.num_groups,
        .allocator = allocator,
    };

    return result;
}

/// Perform groupby mean operation on Arrow arrays
/// Keys: Int64 array, Values: Float64 array
/// Returns unique keys and their mean values
pub fn arrowGroupByMeanI64KeyF64Value(
    allocator: std.mem.Allocator,
    keys: *const ManagedArrowArray,
    values: *const ManagedArrowArray,
) !*GroupBySumResult {
    // Get the data buffers
    const key_data = getInt64Buffer(&keys.array) orelse return error.InvalidData;
    const value_data = getFloat64Buffer(&values.array) orelse return error.InvalidData;

    if (key_data.len != value_data.len) return error.LengthMismatch;

    // Use multi-agg to get sum and count, then compute mean
    const gb_result = try groupby.groupbyMultiAggI64KeyF64Value(allocator, key_data, value_data);
    defer {
        allocator.free(gb_result.keys);
        allocator.free(gb_result.sums);
        allocator.free(gb_result.mins);
        allocator.free(gb_result.maxs);
        allocator.free(gb_result.counts);
    }

    // Compute means from sums and counts
    const means = try allocator.alloc(f64, gb_result.num_groups);
    defer allocator.free(means);
    for (0..gb_result.num_groups) |i| {
        means[i] = gb_result.sums[i] / @as(f64, @floatFromInt(gb_result.counts[i]));
    }

    // Create ManagedArrowArrays for the results
    const keys_arr = try ManagedArrowArray.createI64(allocator, gb_result.keys);
    errdefer keys_arr.deinit();

    const means_arr = try ManagedArrowArray.createF64(allocator, means);
    errdefer means_arr.deinit();

    // Create the result struct
    const result = try allocator.create(GroupBySumResult);
    result.* = GroupBySumResult{
        .keys = keys_arr,
        .sums = means_arr, // reusing sums field for means
        .num_groups = gb_result.num_groups,
        .allocator = allocator,
    };

    return result;
}

// ============================================================================
// Full Join Operations (Join + Materialize in one call)
// ============================================================================

/// Result of a full join operation - contains all materialized result columns
pub const FullJoinResult = struct {
    /// Result columns (caller takes ownership)
    result_columns: []*ManagedArrowArray,
    /// Number of result columns
    num_columns: usize,
    /// Number of result rows
    num_rows: usize,
    /// Allocator for cleanup
    allocator: std.mem.Allocator,

    pub fn deinit(self: *FullJoinResult) void {
        for (self.result_columns[0..self.num_columns]) |col| {
            col.deinit();
        }
        self.allocator.free(self.result_columns);
        self.allocator.destroy(self);
    }
};

// Profiling disabled for production
const JOIN_PROFILING = false;

fn writeProfile(comptime fmt: []const u8, args: anytype) void {
    _ = fmt;
    _ = args;
}

/// Perform a complete inner join: join + materialize all columns
/// This does everything in Zig to minimize CGO overhead
pub fn arrowInnerJoinFull(
    allocator: std.mem.Allocator,
    left_key: *const ManagedArrowArray,
    right_key: *const ManagedArrowArray,
    left_columns: [*]const *const ManagedArrowArray,
    left_col_count: usize,
    right_columns: [*]const *const ManagedArrowArray,
    right_col_count: usize,
) !*FullJoinResult {
    var timer = std.time.Timer.start() catch unreachable;

    // Get key data
    const left_keys = getInt64Buffer(&left_key.array) orelse return error.InvalidData;
    const right_keys = getInt64Buffer(&right_key.array) orelse return error.InvalidData;

    // Perform the join using single-pass parallel join (thread-local buffers)
    var join_result = try simd.singlePassParallelInnerJoin(allocator, left_keys, right_keys);
    defer {
        if (join_result.owns_memory) {
            if (join_result.left_indices.len > 0) allocator.free(join_result.left_indices);
            if (join_result.right_indices.len > 0) allocator.free(join_result.right_indices);
        }
    }

    const join_time_us = timer.read() / 1000;
    timer.reset();

    const num_rows = join_result.num_matches;
    const total_columns = left_col_count + right_col_count;

    // Allocate result structure
    const result = try allocator.create(FullJoinResult);
    errdefer allocator.destroy(result);

    result.result_columns = try allocator.alloc(*ManagedArrowArray, total_columns);
    errdefer allocator.free(result.result_columns);

    result.num_columns = total_columns;
    result.num_rows = num_rows;
    result.allocator = allocator;

    var col_idx: usize = 0;

    // Gather left columns
    for (left_columns[0..left_col_count]) |src_col| {
        result.result_columns[col_idx] = try gatherColumn(allocator, src_col, join_result.left_indices[0..num_rows]);
        col_idx += 1;
    }

    const left_gather_time_us = timer.read() / 1000;
    timer.reset();

    // Gather right columns
    for (right_columns[0..right_col_count]) |src_col| {
        result.result_columns[col_idx] = try gatherColumn(allocator, src_col, join_result.right_indices[0..num_rows]);
        col_idx += 1;
    }

    const right_gather_time_us = timer.read() / 1000;

    if (JOIN_PROFILING) {
        writeProfile("[INNER JOIN] rows={d} join={d}us left_gather={d}us right_gather={d}us total={d}us\n", .{
            num_rows,
            join_time_us,
            left_gather_time_us,
            right_gather_time_us,
            join_time_us + left_gather_time_us + right_gather_time_us,
        });
    }

    return result;
}

/// Perform a complete left join: join + materialize all columns
/// Right columns will have nulls for unmatched left rows
pub fn arrowLeftJoinFull(
    allocator: std.mem.Allocator,
    left_key: *const ManagedArrowArray,
    right_key: *const ManagedArrowArray,
    left_columns: [*]const *const ManagedArrowArray,
    left_col_count: usize,
    right_columns: [*]const *const ManagedArrowArray,
    right_col_count: usize,
) !*FullJoinResult {
    var timer = std.time.Timer.start() catch unreachable;

    // Get key data
    const left_keys = getInt64Buffer(&left_key.array) orelse return error.InvalidData;
    const right_keys = getInt64Buffer(&right_key.array) orelse return error.InvalidData;

    // Perform the join using single-pass parallel join (thread-local buffers)
    var join_result = try simd.singlePassParallelLeftJoin(allocator, left_keys, right_keys);
    defer {
        if (join_result.owns_memory) {
            if (join_result.left_indices.len > 0) allocator.free(join_result.left_indices);
            if (join_result.right_indices.len > 0) allocator.free(join_result.right_indices);
        }
    }

    const join_time_us = timer.read() / 1000;
    timer.reset();

    const num_rows: usize = join_result.num_rows;
    const total_columns = left_col_count + right_col_count;

    // Allocate result structure
    const result = try allocator.create(FullJoinResult);
    errdefer allocator.destroy(result);

    result.result_columns = try allocator.alloc(*ManagedArrowArray, total_columns);
    errdefer allocator.free(result.result_columns);

    result.num_columns = total_columns;
    result.num_rows = num_rows;
    result.allocator = allocator;

    var col_idx: usize = 0;

    // Gather left columns (no nulls needed - all left rows are in output)
    for (left_columns[0..left_col_count]) |src_col| {
        result.result_columns[col_idx] = try gatherColumn(allocator, src_col, join_result.left_indices[0..num_rows]);
        col_idx += 1;
    }

    const left_gather_time_us = timer.read() / 1000;
    timer.reset();

    // Gather right columns (with null handling for unmatched rows)
    for (right_columns[0..right_col_count]) |src_col| {
        result.result_columns[col_idx] = try gatherColumnWithNulls(allocator, src_col, join_result.right_indices[0..num_rows]);
        col_idx += 1;
    }

    const right_gather_time_us = timer.read() / 1000;

    if (JOIN_PROFILING) {
        writeProfile("[LEFT JOIN] rows={d} join={d}us left_gather={d}us right_gather={d}us total={d}us\n", .{
            num_rows,
            join_time_us,
            left_gather_time_us,
            right_gather_time_us,
            join_time_us + left_gather_time_us + right_gather_time_us,
        });
    }

    return result;
}

/// Gather values from a column by indices, creating a new column
fn gatherColumn(
    allocator: std.mem.Allocator,
    src: *const ManagedArrowArray,
    indices: []const i32,
) !*ManagedArrowArray {
    const len = indices.len;

    // Determine type and gather
    if (getFloat64Buffer(&src.array)) |src_data| {
        const result_data = try allocator.alloc(f64, len);
        errdefer allocator.free(result_data);

        // Use SIMD gather
        simd.gather.gatherF64(src_data, indices, result_data);

        return ManagedArrowArray.createF64FromOwned(allocator, result_data);
    } else if (getInt64Buffer(&src.array)) |src_data| {
        const result_data = try allocator.alloc(i64, len);
        errdefer allocator.free(result_data);

        // Use SIMD gather
        simd.gather.gatherI64(src_data, indices, result_data);

        return ManagedArrowArray.createI64FromOwned(allocator, result_data);
    } else {
        return error.UnsupportedType;
    }
}

/// Gather values from a column by indices, with null handling for -1 indices
fn gatherColumnWithNulls(
    allocator: std.mem.Allocator,
    src: *const ManagedArrowArray,
    indices: []const i32,
) !*ManagedArrowArray {
    const len = indices.len;

    // Create validity bitmap and count nulls in one SIMD pass
    const bitmap_result = try createValidityBitmapAndCountNulls(allocator, indices);
    const validity = bitmap_result.validity;
    const null_count = bitmap_result.null_count;
    errdefer allocator.free(validity);

    // Determine type and gather
    if (getFloat64Buffer(&src.array)) |src_data| {
        const result_data = try allocator.alloc(f64, len);
        errdefer allocator.free(result_data);

        // Use SIMD gather (handles -1 as 0)
        simd.gather.gatherF64(src_data, indices, result_data);

        if (null_count > 0) {
            return ManagedArrowArray.createF64WithValidityFromOwned(allocator, result_data, validity, null_count);
        } else {
            allocator.free(validity);
            return ManagedArrowArray.createF64FromOwned(allocator, result_data);
        }
    } else if (getInt64Buffer(&src.array)) |src_data| {
        const result_data = try allocator.alloc(i64, len);
        errdefer allocator.free(result_data);

        // Use SIMD gather (handles -1 as 0)
        simd.gather.gatherI64(src_data, indices, result_data);

        if (null_count > 0) {
            return ManagedArrowArray.createI64WithValidityFromOwned(allocator, result_data, validity, null_count);
        } else {
            allocator.free(validity);
            return ManagedArrowArray.createI64FromOwned(allocator, result_data);
        }
    } else {
        allocator.free(validity);
        return error.UnsupportedType;
    }
}

/// Create a validity bitmap from indices AND count nulls in one pass using SIMD
/// Returns the validity bitmap and null count
fn createValidityBitmapAndCountNulls(allocator: std.mem.Allocator, indices: []const i32) !struct { validity: []u8, null_count: usize } {
    const len = indices.len;
    const num_bytes = (len + 7) / 8;
    const validity = try allocator.alloc(u8, num_bytes);

    var total_valid: usize = 0;
    const VecSize = 8;

    // Process 8 indices at a time (produces 1 validity byte)
    var byte_idx: usize = 0;
    var i: usize = 0;
    while (i + VecSize <= len) : ({
        i += VecSize;
        byte_idx += 1;
    }) {
        const chunk: @Vector(VecSize, i32) = indices[i..][0..VecSize].*;
        // valid if idx >= 0 (i.e., idx != -1 for our case, but >= 0 is more correct)
        const valid_mask = chunk >= @as(@Vector(VecSize, i32), @splat(0));
        // Convert bool vector to u8 - each bit represents validity
        const byte: u8 = @bitCast(valid_mask);
        validity[byte_idx] = byte;
        total_valid += @popCount(byte);
    }

    // Handle remaining elements
    if (i < len) {
        var byte: u8 = 0;
        var bit: u3 = 0;
        while (i < len) : ({
            i += 1;
            bit +%= 1;
        }) {
            if (indices[i] >= 0) {
                byte |= @as(u8, 1) << bit;
                total_valid += 1;
            }
        }
        validity[byte_idx] = byte;
    }

    return .{ .validity = validity, .null_count = len - total_valid };
}

// ============================================================================
// Full DataFrame Sort (Sort + Gather all columns in one call)
// ============================================================================

/// Result of a full sort operation - contains all reordered columns
pub const SortResult = struct {
    result_columns: []*ManagedArrowArray,
    num_columns: usize,
    num_rows: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SortResult) void {
        for (self.result_columns[0..self.num_columns]) |col| {
            col.deinit();
        }
        self.allocator.free(self.result_columns);
        self.allocator.destroy(self);
    }
};

/// Perform a complete DataFrame sort: argsort + gather all columns in one call
/// This minimizes CGO overhead by doing everything in Zig
pub fn arrowSortDataFrameFull(
    allocator: std.mem.Allocator,
    sort_column: *const ManagedArrowArray,
    columns: [*]const *const ManagedArrowArray,
    col_count: usize,
    ascending: bool,
) !*SortResult {
    const num_rows = sort_column.array.length;

    // Handle empty case
    if (num_rows == 0) {
        const result = try allocator.create(SortResult);
        result.result_columns = try allocator.alloc(*ManagedArrowArray, col_count);
        result.num_columns = col_count;
        result.num_rows = 0;
        result.allocator = allocator;

        // Create empty columns
        for (0..col_count) |i| {
            const src = columns[i];
            if (getFloat64Buffer(&src.array) != null) {
                result.result_columns[i] = try ManagedArrowArray.createF64(allocator, &[_]f64{});
            } else if (getInt64Buffer(&src.array) != null) {
                result.result_columns[i] = try ManagedArrowArray.createI64(allocator, &[_]i64{});
            } else {
                return error.UnsupportedType;
            }
        }
        return result;
    }

    // Get sort indices based on type
    const indices = try allocator.alloc(u32, @intCast(num_rows));
    defer allocator.free(indices);

    if (getFloat64Buffer(&sort_column.array)) |data| {
        simd.argsortF64(data, indices, ascending);
    } else if (getInt64Buffer(&sort_column.array)) |data| {
        simd.argsortI64(data, indices, ascending);
    } else if (getFloat32Buffer(&sort_column.array)) |data| {
        simd.argsortF32(data, indices, ascending);
    } else if (getInt32Buffer(&sort_column.array)) |data| {
        simd.argsortI32(data, indices, ascending);
    } else {
        return error.UnsupportedType;
    }

    // Convert u32 indices to i32 for gatherColumn
    const indices_i32 = try allocator.alloc(i32, indices.len);
    defer allocator.free(indices_i32);
    for (indices, 0..) |idx, i| {
        indices_i32[i] = @intCast(idx);
    }

    // Allocate result structure
    const result = try allocator.create(SortResult);
    errdefer allocator.destroy(result);

    result.result_columns = try allocator.alloc(*ManagedArrowArray, col_count);
    errdefer allocator.free(result.result_columns);

    result.num_columns = col_count;
    result.num_rows = @intCast(num_rows);
    result.allocator = allocator;

    // Gather all columns using the sort indices
    for (0..col_count) |i| {
        result.result_columns[i] = try gatherColumn(allocator, columns[i], indices_i32);
    }

    return result;
}

// ============================================================================
// Tests
// ============================================================================

test "arrow - buffer extraction" {
    // Create a mock ArrowArray for testing
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var buffers = [_]?*anyopaque{ null, @ptrCast(&data) };

    var arr = ArrowArray{
        .length = 5,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = &buffers,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    const extracted = getFloat64Buffer(&arr);
    try std.testing.expect(extracted != null);
    try std.testing.expectEqual(@as(usize, 5), extracted.?.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), extracted.?[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), extracted.?[4], 0.001);
}

test "arrow - sum without nulls" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var buffers = [_]?*anyopaque{ null, @ptrCast(&data) };

    var arr = ArrowArray{
        .length = 5,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = &buffers,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    const result = sumFloat64(&arr);
    try std.testing.expectApproxEqAbs(@as(f64, 15.0), result, 0.001);
}

test "arrow - sum with nulls" {
    var data = [_]f64{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    // Validity bitmap: 0b11011 = values 0,1,3,4 are valid; value 2 is null
    var validity = [_]u8{0b00011011};
    var buffers = [_]?*anyopaque{ @ptrCast(&validity), @ptrCast(&data) };

    var arr = ArrowArray{
        .length = 5,
        .null_count = 1,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = &buffers,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    const result = sumFloat64(&arr);
    // Sum of 1 + 2 + 4 + 5 = 12 (skipping index 2 which is null)
    try std.testing.expectApproxEqAbs(@as(f64, 12.0), result, 0.001);
}

test "arrow - validity bitmap check" {
    const bitmap = [_]u8{0b00011011}; // bits 0,1,3,4 set

    try std.testing.expect(isValid(&bitmap, 0) == true);
    try std.testing.expect(isValid(&bitmap, 1) == true);
    try std.testing.expect(isValid(&bitmap, 2) == false);
    try std.testing.expect(isValid(&bitmap, 3) == true);
    try std.testing.expect(isValid(&bitmap, 4) == true);
    try std.testing.expect(isValid(&bitmap, 5) == false);
}
