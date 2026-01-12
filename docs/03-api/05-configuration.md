# Configuration API Reference

Galleon provides runtime configuration options for tuning performance characteristics.

## Thread Configuration

### SetMaxThreads

Sets the maximum number of threads for parallel operations.

```go
func SetMaxThreads(maxThreads int)
```

**Parameters:**
- `maxThreads`: Maximum threads to use. Set to 0 for auto-detection.

**Example:**
```go
// Use exactly 4 threads
galleon.SetMaxThreads(4)

// Use auto-detection (based on CPU cores)
galleon.SetMaxThreads(0)
```

### GetMaxThreads

Returns the current maximum thread setting.

```go
func GetMaxThreads() int
```

**Example:**
```go
threads := galleon.GetMaxThreads()
fmt.Printf("Using %d threads\n", threads)
```

### GetThreadConfig

Returns detailed thread configuration.

```go
func GetThreadConfig() ThreadConfig

type ThreadConfig struct {
    MaxThreads   int
    AutoDetected bool
}
```

**Example:**
```go
config := galleon.GetThreadConfig()
if config.AutoDetected {
    fmt.Printf("Auto-detected %d threads\n", config.MaxThreads)
} else {
    fmt.Printf("Configured for %d threads\n", config.MaxThreads)
}
```

## Thread Configuration Details

### Default Behavior

By default, Galleon auto-detects the optimal thread count based on available CPU cores:

```go
// Default: auto-detect based on CPU cores
config := galleon.GetThreadConfig()
// config.AutoDetected == true
// config.MaxThreads == number of CPU cores (up to 32)
```

### Maximum Thread Limit

There is a compile-time maximum of 32 threads. Any value set above this will be capped:

```go
galleon.SetMaxThreads(64)  // Will be capped to 32
actual := galleon.GetMaxThreads()  // Returns 32
```

### Thread Scaling

Thread usage scales with data size:
- Small datasets (< 10,000 rows): May use fewer threads due to overhead
- Medium datasets: Scales proportionally
- Large datasets: Uses all available threads

## Configuration Best Practices

### For Server Applications

```go
// On a dedicated server, use all available cores
galleon.SetMaxThreads(0)  // Auto-detect
```

### For Shared Environments

```go
// Leave some cores for other processes
config := galleon.GetThreadConfig()
galleon.SetMaxThreads(config.MaxThreads / 2)
```

### For Batch Processing

```go
// Maximize throughput
galleon.SetMaxThreads(0)  // Use all cores

// Process multiple files
for _, file := range files {
    df, _ := galleon.ReadCSV(file, galleon.DefaultCSVReadOptions())
    // Process df...
}
```

### For Interactive Applications

```go
// Balance responsiveness and performance
galleon.SetMaxThreads(4)  // Limit parallel work
```

## Performance Considerations

### Thread Overhead

Each parallel operation has some overhead for:
- Thread spawning and coordination
- Memory allocation for per-thread buffers
- Result aggregation

For very small datasets, this overhead may exceed the benefit of parallelism.

### Memory Usage

Each thread may allocate temporary buffers:
- Aggregation operations: O(groups) per thread
- Sort operations: O(n/threads) per thread
- Filter operations: O(n/threads) per thread

### Cache Efficiency

Galleon's parallel algorithms are designed for cache efficiency:
- Data is partitioned to minimize cache conflicts
- SIMD operations process cache-line-aligned data
- Results are written to avoid false sharing

## Example: Benchmarking Thread Settings

```go
package main

import (
    "fmt"
    "time"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    // Generate test data
    n := 10_000_000
    values := make([]float64, n)
    for i := range values {
        values[i] = float64(i)
    }

    series := galleon.NewSeriesFloat64("values", values)

    // Benchmark different thread counts
    for threads := 1; threads <= 16; threads *= 2 {
        galleon.SetMaxThreads(threads)

        start := time.Now()
        for i := 0; i < 100; i++ {
            _ = series.Sum()
        }
        elapsed := time.Since(start)

        fmt.Printf("%2d threads: %v\n", threads, elapsed/100)
    }
}
```

## Zig Backend Configuration

For advanced users, thread configuration can also be accessed through the C API:

```c
// C API (used internally by CGO)
void galleon_set_max_threads(size_t max_threads);
size_t galleon_get_max_threads();

typedef struct {
    size_t max_threads;
    bool auto_detected;
} ThreadConfig;

ThreadConfig galleon_get_thread_config();
```

These functions directly configure the Zig SIMD backend's thread pool behavior.

## Display Configuration

Control how DataFrames are formatted when printed.

### DisplayConfig

Configuration structure for DataFrame display:

```go
type DisplayConfig struct {
    MaxRows        int    // Max rows to display (default: 10)
    MaxCols        int    // Max columns to display (default: 10)
    MaxColWidth    int    // Max width per column (default: 25)
    MinColWidth    int    // Min width per column (default: 8)
    FloatPrecision int    // Decimal places for floats (default: 4)
    ShowDTypes     bool   // Show data types row (default: true)
    ShowShape      bool   // Show shape header (default: true)
    TableStyle     string // Border style (default: "rounded")
}
```

### DefaultDisplayConfig

Returns the default display configuration.

```go
func DefaultDisplayConfig() DisplayConfig
```

### GetDisplayConfig

Returns the current global display configuration.

```go
func GetDisplayConfig() DisplayConfig
```

### SetDisplayConfig

Sets the global display configuration.

```go
func SetDisplayConfig(cfg DisplayConfig)
```

### SetMaxDisplayRows

Sets maximum rows to display (split between head and tail).

```go
func SetMaxDisplayRows(n int)
```

**Example:**
```go
galleon.SetMaxDisplayRows(20)  // Show 10 head + 10 tail rows
```

### SetMaxDisplayCols

Sets maximum columns to display (split between first and last).

```go
func SetMaxDisplayCols(n int)
```

**Example:**
```go
galleon.SetMaxDisplayCols(8)  // Show 4 first + 4 last columns
```

### SetFloatPrecision

Sets decimal places for floating point display.

```go
func SetFloatPrecision(n int)
```

**Example:**
```go
galleon.SetFloatPrecision(2)  // Display as 3.14 instead of 3.1416
```

### SetTableStyle

Sets the table border style.

```go
func SetTableStyle(style string)
```

**Available styles:**
- `"rounded"` - Rounded corners: ╭╮╰╯ (default)
- `"sharp"` - Sharp corners: ┌┐└┘
- `"ascii"` - ASCII characters: +-|
- `"minimal"` - Minimal borders

**Example:**
```go
galleon.SetTableStyle("ascii")  // For non-Unicode terminals
```

### DataFrame.StringWithConfig

Format DataFrame with custom configuration.

```go
func (df *DataFrame) StringWithConfig(cfg DisplayConfig) string
```

**Example:**
```go
cfg := galleon.DefaultDisplayConfig()
cfg.FloatPrecision = 2
cfg.TableStyle = "minimal"
fmt.Println(df.StringWithConfig(cfg))
```

### Series.StringWithConfig

Format Series with custom configuration.

```go
func (s *Series) StringWithConfig(cfg DisplayConfig) string
```

**Example:**
```go
cfg := galleon.DefaultDisplayConfig()
cfg.FloatPrecision = 2
cfg.TableStyle = "ascii"
fmt.Println(series.StringWithConfig(cfg))
```

## Display Configuration Best Practices

### For Log Files

```go
galleon.SetTableStyle("ascii")  // Ensure compatibility
galleon.SetMaxDisplayRows(10)   // Keep logs concise
```

### For Wide DataFrames

```go
galleon.SetMaxDisplayCols(6)    // Prevent horizontal scrolling
galleon.SetMaxColWidth(15)      // Narrow columns
```

### For Financial Data

```go
galleon.SetFloatPrecision(2)    // Currency display
```

### For Scientific Data

```go
galleon.SetFloatPrecision(8)    // Higher precision
```
