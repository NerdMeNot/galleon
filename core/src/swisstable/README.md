# Swiss Table

A high-performance hash table implementation in Zig, based on Google's Swiss Table design (abseil) and Rust's hashbrown. Provides full feature parity with hashbrown's HashMap and HashSet.

## Features

- **SIMD-accelerated lookups**: Uses vectorized control byte scanning (16 slots per instruction on x86, 8 on ARM)
- **87.5% max load factor**: Memory efficient with fast probing
- **Cache-friendly layout**: Control bytes and entries stored contiguously
- **Triangular probing**: Guarantees visiting all slots in power-of-2 tables
- **Zero external dependencies**: Only requires Zig standard library
- **Entry API**: hashbrown-style entry API for efficient conditional operations
- **HashSet**: Full-featured set implementation with set operations

## Architecture-Specific Optimizations

| Architecture | Group Width | SIMD | Notes |
|--------------|-------------|------|-------|
| x86-64 | 16 bytes | SSE2 | Standard Swiss Table layout |
| ARM64/aarch64 | 8 bytes | NEON | Smaller groups for lower NEON latency |

## Usage

### Table (Hash Map)

```zig
const swisstable = @import("swisstable");

var table = swisstable.Table(i64, MyValue).init(allocator);
defer table.deinit();

// Insert
_ = try table.put(42, myValue);

// Lookup
if (table.get(42)) |value| {
    // Found it - value is *MyValue
}

// Check existence
if (table.contains(42)) {
    // Key exists
}

// Get mutable pointer
if (table.getPtr(42)) |ptr| {
    ptr.* = newValue;
}

// Remove
if (table.remove(42)) |old_value| {
    // Key was present, old_value is the removed value
}

// Entry API (hashbrown-style)
const e = try table.entry(key);
switch (e) {
    .occupied => |o| {
        // Key exists
        const val = o.get();      // Get *V
        const k = o.key();        // Get K
        _ = o.insert(newValue);   // Replace, returns old value
        _ = o.remove();           // Remove entry, returns value
    },
    .vacant => |v| {
        // Key doesn't exist
        const k = v.getKey();     // Get the key
        _ = v.insert(value);      // Insert, returns *V
    },
}

// Convenience methods
const ptr = try table.getOrInsert(key, defaultValue);
const ptr2 = try table.getOrInsertWith(key, defaultFn);
const ptr3 = try table.getOrInsertDefault(key, defaultValue);

// Fast insert for known-new keys (skips existence check)
try table.putNew(uniqueKey, value);

// Iterate over entries
var it = table.iterator();
while (it.next()) |entry| {
    // entry.key, entry.value
}

// Iterate over keys only
var keys_it = table.keys();
while (keys_it.next()) |key| {
    // ...
}

// Iterate over values (immutable)
var vals_it = table.values();
while (vals_it.next()) |value| {
    // value is *const V
}

// Iterate over values (mutable)
var vals_mut_it = table.valuesMut();
while (vals_mut_it.next()) |value| {
    value.* += 1;  // value is *V
}

// Retain only matching entries
table.retain(myPredicate);  // fn(K, *V) bool

// Capacity management
const n = table.count();           // Number of entries
const cap = table.capacity();      // Current bucket count
const empty = table.isEmpty();     // True if count == 0
try table.reserve(100);            // Reserve for 100 more entries
try table.shrinkToFit();           // Shrink to minimum capacity
try table.shrinkTo(minCap);        // Shrink with minimum
table.clear();                     // Remove all, keep memory
table.clearAndFree();              // Remove all, free memory

// Clone
var cloned = try table.clone();
defer cloned.deinit();
```

### Set (Hash Set)

```zig
const swisstable = @import("swisstable");

var set = swisstable.Set(i64).init(allocator);
defer set.deinit();

// Insert (returns true if newly inserted)
const was_new = try set.insert(42);

// Check membership
if (set.contains(42)) {
    // ...
}

// Remove (returns true if was present)
const was_present = set.remove(42);

// Iterate
var it = set.iterator();
while (it.next()) |value| {
    // ...
}

// Set operations (return new sets)
var other = swisstable.Set(i64).init(allocator);
defer other.deinit();
_ = try other.insert(1);
_ = try other.insert(2);

// Union: elements in either set
var union_set = try set.unionWith(&other);
defer union_set.deinit();

// Intersection: elements in both sets
var inter_set = try set.intersection(&other);
defer inter_set.deinit();

// Difference: elements in self but not other
var diff_set = try set.difference(&other);
defer diff_set.deinit();

// Symmetric difference: elements in either but not both
var sym_diff = try set.symmetricDifference(&other);
defer sym_diff.deinit();

// Set predicates
if (set.isSubset(&other)) { ... }    // All elements of set are in other
if (set.isSuperset(&other)) { ... }  // Set contains all elements of other
if (set.isDisjoint(&other)) { ... }  // No common elements

// Capacity management (same as Table)
const n = set.count();
const cap = set.capacity();
const empty = set.isEmpty();
try set.reserve(100);
try set.shrinkToFit();
try set.shrinkTo(minCap);
set.clear();
set.clearAndFree();

// Clone
var cloned = try set.clone();
defer cloned.deinit();
```

### Partition Routing (for parallel hash joins)

```zig
const swisstable = @import("swisstable");

// Hash a key
const hash = swisstable.dirtyHash(key);

// Route to partition (0 to n_partitions-1)
const partition = swisstable.hashToPartition(hash, n_partitions);
```

## Building

```bash
# Run tests
zig build test

# Run benchmarks
zig build bench

# Build static library
zig build
```

## API Reference

### Table(K, V)

Generic hash map with key type `K` and value type `V`.

#### Initialization

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn(Allocator) Self` | Create empty table |
| `initCapacity` | `fn(Allocator, usize) !Self` | Create with pre-allocated capacity |
| `deinit` | `fn(*Self) void` | Free all memory |

#### Core Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `put` | `fn(*Self, K, V) !?V` | Insert or update, returns old value if existed |
| `putNew` | `fn(*Self, K, V) !void` | Fast insert for known-new keys (skips existence check) |
| `get` | `fn(*const Self, K) ?*V` | Get pointer to value, or null |
| `getPtr` | `fn(*Self, K) ?*V` | Get mutable pointer to value, or null |
| `getOrInsertDefault` | `fn(*Self, K, V) !*V` | Get existing or insert default, return pointer |
| `remove` | `fn(*Self, K) ?V` | Remove key, returns value if existed |
| `contains` | `fn(*const Self, K) bool` | Check if key exists |

#### Entry API

| Method | Signature | Description |
|--------|-----------|-------------|
| `entry` | `fn(*Self, K) !EntryResult` | Get entry for in-place manipulation |
| `getOrInsert` | `fn(*Self, K, V) !*V` | Get existing or insert, return pointer |
| `getOrInsertWith` | `fn(*Self, K, *const fn() V) !*V` | Get existing or insert with function |

##### EntryResult (union)

- `.occupied` → `OccupiedEntry`
- `.vacant` → `VacantEntry`

##### OccupiedEntry

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `fn(OccupiedEntry) *V` | Get pointer to value |
| `key` | `fn(OccupiedEntry) K` | Get the key |
| `insert` | `fn(OccupiedEntry, V) V` | Replace value, return old |
| `remove` | `fn(OccupiedEntry) V` | Remove entry, return value |

##### VacantEntry

| Method | Signature | Description |
|--------|-----------|-------------|
| `getKey` | `fn(VacantEntry) K` | Get the key |
| `insert` | `fn(VacantEntry, V) *V` | Insert value, return pointer |

#### Capacity Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `count` | `fn(*const Self) usize` | Number of entries |
| `capacity` | `fn(*const Self) usize` | Current capacity (buckets) |
| `isEmpty` | `fn(*const Self) bool` | Returns true if no entries |
| `reserve` | `fn(*Self, usize) !void` | Reserve space for additional entries |
| `shrinkToFit` | `fn(*Self) !void` | Shrink capacity to minimum needed |
| `shrinkTo` | `fn(*Self, usize) !void` | Shrink with minimum capacity |
| `clear` | `fn(*Self) void` | Remove all entries, keep memory |
| `clearAndFree` | `fn(*Self) void` | Remove all entries, free memory |
| `clone` | `fn(*const Self) !Self` | Create a copy of the table |

#### Iteration

| Method | Signature | Description |
|--------|-----------|-------------|
| `iterator` | `fn(*const Self) Iterator` | Iterate over all entries |
| `keys` | `fn(*const Self) KeyIterator` | Iterate over keys only |
| `values` | `fn(*const Self) ValueIterator` | Iterate over values (immutable) |
| `valuesMut` | `fn(*Self) ValueMutIterator` | Iterate over values (mutable) |
| `retain` | `fn(*Self, *const fn(K, *V) bool) void` | Keep only entries matching predicate |

##### Iterator Types

| Type | `next()` Returns |
|------|------------------|
| `Iterator` | `?*Entry` (struct with `.key: K`, `.value: V`) |
| `KeyIterator` | `?K` |
| `ValueIterator` | `?*const V` |
| `ValueMutIterator` | `?*V` |

---

### Set(T)

Generic hash set with element type `T`.

#### Initialization

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn(Allocator) Self` | Create empty set |
| `initCapacity` | `fn(Allocator, usize) !Self` | Create with pre-allocated capacity |
| `deinit` | `fn(*Self) void` | Free all memory |

#### Core Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `insert` | `fn(*Self, T) !bool` | Add value, returns true if new |
| `remove` | `fn(*Self, T) bool` | Remove value, returns true if existed |
| `contains` | `fn(*const Self, T) bool` | Check if value exists |

#### Capacity Management

| Method | Signature | Description |
|--------|-----------|-------------|
| `count` | `fn(*const Self) usize` | Number of elements |
| `capacity` | `fn(*const Self) usize` | Current capacity |
| `isEmpty` | `fn(*const Self) bool` | Returns true if empty |
| `reserve` | `fn(*Self, usize) !void` | Reserve space |
| `shrinkToFit` | `fn(*Self) !void` | Shrink to minimum |
| `shrinkTo` | `fn(*Self, usize) !void` | Shrink with minimum |
| `clear` | `fn(*Self) void` | Remove all, keep memory |
| `clearAndFree` | `fn(*Self) void` | Remove all, free memory |
| `clone` | `fn(*const Self) !Self` | Create a copy |

#### Iteration

| Method | Signature | Description |
|--------|-----------|-------------|
| `iterator` | `fn(*const Self) Iterator` | Iterate over elements |

##### Iterator

| Method | Returns |
|--------|---------|
| `next` | `?T` |

#### Set Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `unionWith` | `fn(*const Self, *const Self) !Self` | Elements in either set |
| `intersection` | `fn(*const Self, *const Self) !Self` | Elements in both sets |
| `difference` | `fn(*const Self, *const Self) !Self` | Elements in self but not other |
| `symmetricDifference` | `fn(*const Self, *const Self) !Self` | Elements in either but not both |
| `isSubset` | `fn(*const Self, *const Self) bool` | True if all elements in other |
| `isSuperset` | `fn(*const Self, *const Self) bool` | True if contains all of other |
| `isDisjoint` | `fn(*const Self, *const Self) bool` | True if no common elements |

---

### Utility Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `hashToPartition` | `fn(u64, usize) usize` | Route hash to partition index [0, n) |
| `dirtyHash` | `fn(u64) u64` | Fast hash for partition routing |
| `fastIntHash` | `fn(u64) u64` | Fast integer hash (fibonacci multiply) |

---

### Constants

| Constant | Type | Value | Description |
|----------|------|-------|-------------|
| `GROUP_WIDTH` | `usize` | 8 or 16 | Control bytes per SIMD group |
| `EMPTY` | `u8` | 0xFF | Empty slot marker |
| `DELETED` | `u8` | 0x80 | Tombstone marker |

## Performance

### Running Benchmarks

```bash
# Zig swisstable benchmarks
cd core/src/swisstable
zig build bench

# Rust hashbrown benchmarks (for comparison)
cd core/src/swisstable/rust_bench
cargo run --release
```

### Benchmark Results (Apple M1, aarch64)

Results at N=100,000 entries - the sweet spot where cache effects become visible:

#### Zig swisstable vs std.AutoHashMap

| Operation | swisstable | std.AutoHashMap | Speedup |
|-----------|------------|-----------------|---------|
| Insert (seq) | 5.3 ns/op | 23.3 ns/op | **4.4x** |
| Insert (rnd) | 11.8 ns/op | 20.7 ns/op | 1.8x |
| Insert (pre) | 2.3 ns/op | 9.0 ns/op | **3.9x** |
| PutNew | 1.5 ns/op | 8.6 ns/op | **5.8x** |
| Lookup (hit) | 2.3 ns/op | 7.5 ns/op | 3.2x |
| Lookup (miss) | 1.4 ns/op | 15.1 ns/op | **11.0x** |
| Lookup (rnd) | 6.5 ns/op | 15.3 ns/op | 2.4x |
| Remove | 1.9 ns/op | 8.3 ns/op | **4.4x** |
| Remove+Reinsert | 2.3 ns/op | 28.1 ns/op | **12.1x** |
| Iterate | 0.7 ns/op | 2.3 ns/op | 3.3x |
| Keys iter | 0.7 ns/op | 1.7 ns/op | 2.6x |
| GetOrInsert | 3.8 ns/op | 14.4 ns/op | **3.8x** |
| Memory | 22.3 B/entry | 26.2 B/entry | 15% smaller |

#### Rust hashbrown vs std::collections::HashMap

| Operation | hashbrown | std HashMap | Speedup |
|-----------|-----------|-------------|---------|
| Insert (seq) | 11.7 ns/op | 22.0 ns/op | 1.9x |
| Insert (rnd) | 11.0 ns/op | 21.1 ns/op | 1.9x |
| Insert (pre) | 3.4 ns/op | 9.9 ns/op | 2.9x |
| InsertUnique | 2.5 ns/op | 9.7 ns/op | **3.8x** |
| Lookup (hit) | 2.5 ns/op | 7.1 ns/op | 2.8x |
| Lookup (miss) | 6.1 ns/op | 11.8 ns/op | 1.9x |
| Lookup (rnd) | 10.5 ns/op | 17.2 ns/op | 1.6x |
| Remove | 9.0 ns/op | 15.9 ns/op | 1.8x |
| Remove+Reinsert | 11.1 ns/op | 19.0 ns/op | 1.7x |
| Iterate | 1.9 ns/op | 1.9 ns/op | 1.0x |
| Entry API | 8.9 ns/op | 19.5 ns/op | 2.2x |
| Memory | 19.4 B/entry | 26.7 B/entry | 27% smaller |

#### Cross-Language Comparison (Swiss Table implementations)

| Operation | Zig swisstable | Rust hashbrown | Notes |
|-----------|---------------|----------------|-------|
| Insert (pre) | 2.3 ns/op | 3.4 ns/op | Zig 1.5x faster |
| PutNew/Unique | 1.5 ns/op | 2.5 ns/op | Zig 1.7x faster |
| Lookup (hit) | 2.3 ns/op | 2.5 ns/op | ~same |
| Lookup (miss) | 1.4 ns/op | 6.1 ns/op | Zig 4.4x faster |
| Remove | 1.9 ns/op | 9.0 ns/op | Zig 4.7x faster |
| Iterate | 0.7 ns/op | 1.9 ns/op | Zig 2.7x faster |

**Key observations:**
- SIMD control byte scanning provides massive speedups for cache-miss scenarios
- Lookup miss is 11x faster than std in Zig due to early termination on EMPTY
- Remove+Reinsert shows 12x speedup due to efficient tombstone handling
- Memory overhead is 15-27% smaller due to 1-byte control bytes vs stored hashes

## Implementation Details

### Control Byte Encoding

Each slot has a 1-byte control byte:
- `0x00-0x7F`: FULL - contains H2 (top 7 bits of hash)
- `0x80`: DELETED - tombstone marker
- `0xFF`: EMPTY - never used

### Memory Layout

```
[ctrl bytes: n + GROUP_WIDTH] [entries: n]
```

Extra `GROUP_WIDTH` control bytes at the end enable unaligned SIMD loads at table boundaries.

### Probing Sequence

Triangular probing: positions `pos`, `pos+1`, `pos+3`, `pos+6`, `pos+10`, ...

This guarantees visiting all groups exactly once in a power-of-2 table.

## References

- [Abseil Swiss Tables](https://abseil.io/about/design/swisstables)
- [hashbrown (Rust)](https://github.com/rust-lang/hashbrown)
- [CppCon 2017: Designing a Fast, Efficient, Cache-friendly Hash Table](https://www.youtube.com/watch?v=ncHmEUmJZf4)

## License

Same license as the parent project.
