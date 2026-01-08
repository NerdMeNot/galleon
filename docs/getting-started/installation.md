# Installation Guide

Complete instructions for installing and building Galleon.

## System Requirements

### Required

- **Go**: 1.21 or later
- **Zig**: 0.13 or later
- **Operating System**: macOS, Linux, or Windows with WSL

### Optional

- **C Compiler**: For CGO (usually included with OS)
- **Git**: For cloning the repository

## Installation Steps

### 1. Install Prerequisites

#### macOS

```bash
# Install Zig
brew install zig

# Verify Go (usually pre-installed or via brew)
go version

# Verify Zig
zig version
```

#### Linux (Ubuntu/Debian)

```bash
# Install Zig
snap install zig --classic --beta
# Or download from https://ziglang.org/download/

# Install Go
sudo apt-get update
sudo apt-get install golang-go

# Verify versions
go version
zig version
```

#### Windows (WSL)

```bash
# In WSL, follow Linux instructions
sudo apt-get update
sudo apt-get install golang-go

# Install Zig
wget https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz
tar xf zig-linux-x86_64-0.13.0.tar.xz
export PATH=$PATH:$PWD/zig-linux-x86_64-0.13.0
```

### 2. Clone the Repository

```bash
git clone https://github.com/NerdMeNot/galleon.git
cd galleon
```

### 3. Build the Zig Core Library

```bash
cd core

# Development build (faster compilation)
zig build

# Release build (optimized for performance)
zig build -Doptimize=ReleaseFast

cd ..
```

This creates:
- `core/zig-out/lib/libgalleon.a` (or `.dylib`/`.so`)
- `core/zig-out/include/galleon.h`

### 4. Verify the Go Module

```bash
cd go

# Download dependencies
go mod download

# Run tests
go test ./...

# Run a specific test
go test -v -run TestSum

cd ..
```

### 5. Run Examples

```bash
cd examples/01_basic_usage
go run main.go
cd ../..
```

## Project Structure

```
galleon/
├── core/                  # Zig SIMD core library
│   ├── src/
│   │   ├── main.zig      # CGO exports
│   │   └── simd.zig      # SIMD operations
│   ├── include/
│   │   └── galleon.h     # C header
│   └── build.zig         # Build configuration
├── go/                    # Go package
│   ├── galleon.go        # Main Go API
│   ├── series.go         # Series type
│   ├── dataframe.go      # DataFrame type
│   ├── expr.go           # Expression system
│   ├── lazy.go           # Lazy evaluation
│   └── go.mod            # Go module
├── examples/              # Usage examples
├── docs/                  # Documentation
└── benchmarks/            # Benchmark code
```

## Using Galleon in Your Project

### As a Local Dependency

```go
// go.mod
module myproject

go 1.21

require github.com/NerdMeNot/galleon/go v0.1.0

replace github.com/NerdMeNot/galleon/go => /path/to/galleon/go
```

### Import and Use

```go
package main

import (
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    series := galleon.NewSeriesFloat64("values", []float64{1, 2, 3})
    println(series.Sum())
}
```

## Build Options

### Zig Build Modes

```bash
# Debug (default) - includes debug info, slower
zig build

# ReleaseFast - optimized for speed
zig build -Doptimize=ReleaseFast

# ReleaseSafe - optimized with safety checks
zig build -Doptimize=ReleaseSafe

# ReleaseSmall - optimized for size
zig build -Doptimize=ReleaseSmall
```

### CGO Configuration

The Go package uses CGO to call the Zig library. The paths are configured in `go/galleon.go`:

```go
/*
#cgo CFLAGS: -I${SRCDIR}/../core/zig-out/include
#cgo LDFLAGS: -L${SRCDIR}/../core/zig-out/lib -lgalleon
*/
```

### Custom Build Location

If you build the Zig library in a different location:

```bash
# Set custom library path
export CGO_LDFLAGS="-L/custom/path/lib -lgalleon"
export CGO_CFLAGS="-I/custom/path/include"
```

## Troubleshooting

### Build Errors

#### "zig: command not found"

Ensure Zig is in your PATH:
```bash
export PATH=$PATH:/path/to/zig
```

#### "cannot find -lgalleon"

The Zig library hasn't been built:
```bash
cd core
zig build -Doptimize=ReleaseFast
```

#### "undefined: C.xxx"

CGO is not enabled or paths are wrong:
```bash
# Verify CGO is enabled
go env CGO_ENABLED  # Should be "1"

# Enable if needed
export CGO_ENABLED=1
```

### Runtime Errors

#### "dyld: Library not loaded: libgalleon.dylib"

On macOS, set the library path:
```bash
export DYLD_LIBRARY_PATH=/path/to/galleon/core/zig-out/lib:$DYLD_LIBRARY_PATH
```

#### "error while loading shared libraries"

On Linux, set the library path:
```bash
export LD_LIBRARY_PATH=/path/to/galleon/core/zig-out/lib:$LD_LIBRARY_PATH
```

### Static Linking

For deployment without shared libraries:

```bash
# Build static library
cd core
zig build -Doptimize=ReleaseFast

# The default build produces a static library (.a)
# CGO will link it statically
```

## Platform-Specific Notes

### macOS

- Works on both Intel and Apple Silicon (M1/M2/M3)
- Zig cross-compiles automatically for the current architecture
- No additional dependencies needed

### Linux

- Tested on Ubuntu 20.04+, Debian 11+
- Works on x86_64 and ARM64
- May need `build-essential` for GCC:
  ```bash
  sudo apt-get install build-essential
  ```

### Windows

- Use WSL (Windows Subsystem for Linux) for best experience
- Native Windows support is experimental
- Ensure paths use forward slashes in configuration

## Verifying Installation

Run the verification script:

```bash
cd go
go test -v -run TestBasic
```

Expected output:
```
=== RUN   TestBasic
--- PASS: TestBasic (0.00s)
PASS
ok      github.com/NerdMeNot/galleon/go    0.005s
```

## Next Steps

- [Quick Start](quickstart.md) - Get started with Galleon
- [API Reference](api-dataframe.md) - Full API documentation
- [Examples](../examples/README.md) - Code examples
