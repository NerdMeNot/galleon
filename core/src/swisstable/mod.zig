//! Swiss Table Module
//!
//! A high-performance hash table implementation based on Google's Swiss Table design.
//! This is a standalone module with no external dependencies.
//!
//! Key components:
//! - Table: Hash map with SIMD control byte scanning for fast lookups
//! - Set: Hash set wrapping Table (like hashbrown's HashSet)
//! - hashToPartition: Fast partition routing using 128-bit multiply
//! - dirtyHash: Fast multiplicative hash for integers
//! - fastIntHash: Fast integer hash using fibonacci multiply
//!
//! Usage:
//! ```zig
//! const swisstable = @import("swisstable");
//!
//! // Hash Map
//! var table = swisstable.Table(i64, MyValue).init(allocator);
//! defer table.deinit();
//!
//! _ = try table.put(42, myValue);
//! if (table.get(42)) |value| {
//!     // Found it
//! }
//!
//! // Hash Set
//! var set = swisstable.Set(i64).init(allocator);
//! defer set.deinit();
//!
//! _ = try set.insert(42);
//! if (set.contains(42)) {
//!     // Found it
//! }
//! ```

const std = @import("std");

// Core Swiss Table implementation
pub const lib = @import("lib.zig");
const set_mod = @import("set.zig");

// Main types
pub const Table = lib.Table;
pub const Set = set_mod.Set;

// Hash functions
pub const hashToPartition = lib.hashToPartition;
pub const dirtyHash = lib.dirtyHash;
pub const fastIntHash = lib.fastIntHash;

// Constants
pub const GROUP_WIDTH = lib.GROUP_WIDTH;
pub const EMPTY = lib.EMPTY;
pub const DELETED = lib.DELETED;

// ============================================================================
// Tests
// ============================================================================

test {
    _ = lib;
    _ = set_mod;
}
