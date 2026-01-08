# Installation Guide

Complete instructions for installing and using Galleon.

## For Users

### Quick Install

Galleon comes with prebuilt libraries for all major platforms. Just install with `go get`:

```bash
go get github.com/NerdMeNot/galleon/go
```

That's it! No Zig installation required.

### Supported Platforms

| Platform | Architecture | Status |
|----------|--------------|--------|
| macOS | Apple Silicon (M1/M2/M3) | ✅ |
| macOS | Intel x86_64 | ✅ |
| Linux | x86_64 | ✅ |
| Linux | ARM64 | ✅ |
| Windows | x86_64 | ✅ |

### Requirements

- **Go**: 1.21 or later
- **C Compiler**: Required for CGO
  - **macOS**: Xcode Command Line Tools (`xcode-select --install`)
  - **Linux**: GCC (usually pre-installed, or `apt install build-essential`)
  - **Windows**: MinGW-w64 or MSVC

### Verify Installation

```go
package main

import (
    "fmt"
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    s := galleon.NewSeriesFloat64("test", []float64{1, 2, 3, 4, 5})
    fmt.Printf("Sum: %.0f\n", s.Sum()) // Output: Sum: 15
}
```

---

## For Developers

If you're contributing to Galleon or modifying the Zig core library, follow these instructions.

### Prerequisites

- **Go**: 1.21 or later
- **Zig**: 0.15.2 or later
- **Git**: For cloning the repository

#### Install Zig

**macOS:**
```bash
brew install zig
```

**Linux:**
```bash
# Download from https://ziglang.org/download/
wget https://ziglang.org/builds/zig-linux-x86_64-0.15.2.tar.xz
tar xf zig-linux-x86_64-0.15.2.tar.xz
export PATH=$PATH:$PWD/zig-linux-x86_64-0.15.2
```

**Verify:**
```bash
zig version  # Should show 0.15.2 or later
```

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/NerdMeNot/galleon.git
cd galleon

# Build the Zig core library
cd core
zig build -Doptimize=ReleaseFast
cd ..

# Test with the dev build tag
cd go
go test -tags dev ./...
```

### Development Workflow

When modifying Zig code, use the `-tags dev` flag to use your local build:

```bash
# Build Zig library
cd core && zig build -Doptimize=ReleaseFast && cd ..

# Build/test Go with dev tag
cd go
go build -tags dev ./...
go test -tags dev ./...
```

### Rebuilding Prebuilt Libraries

After making changes to the Zig code, rebuild all platform libraries:

```bash
./scripts/build-all-platforms.sh
```

This cross-compiles for all 5 supported platforms from your machine.

Then commit:
```bash
git add go/lib/
git commit -m "Rebuild prebuilt libraries"
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
│   ├── lib/              # Prebuilt libraries
│   │   ├── darwin_arm64/
│   │   ├── darwin_amd64/
│   │   ├── linux_amd64/
│   │   ├── linux_arm64/
│   │   └── windows_amd64/
│   └── go.mod            # Go module
├── scripts/               # Build scripts
│   └── build-all-platforms.sh
├── examples/              # Usage examples
├── docs/                  # Documentation
└── benchmarks/            # Benchmark code
```

## Using Galleon in Your Project

### Standard Installation

```bash
go get github.com/NerdMeNot/galleon/go
```

### Import and Use

```go
package main

import (
    galleon "github.com/NerdMeNot/galleon/go"
)

func main() {
    df, _ := galleon.NewDataFrame(
        galleon.NewSeriesFloat64("values", []float64{1, 2, 3}),
    )
    println(df.Height()) // 3
}
```

## Build Options

### Zig Build Modes

```bash
# Debug (default) - includes debug info
zig build

# ReleaseFast - optimized for speed (recommended)
zig build -Doptimize=ReleaseFast

# ReleaseSafe - optimized with safety checks
zig build -Doptimize=ReleaseSafe

# ReleaseSmall - optimized for binary size
zig build -Doptimize=ReleaseSmall
```

### Cross-Compilation Targets

```bash
# macOS ARM64
zig build -Dtarget=aarch64-macos -Doptimize=ReleaseFast

# macOS Intel
zig build -Dtarget=x86_64-macos -Doptimize=ReleaseFast

# Linux x86_64
zig build -Dtarget=x86_64-linux-gnu -Doptimize=ReleaseFast

# Linux ARM64
zig build -Dtarget=aarch64-linux-gnu -Doptimize=ReleaseFast

# Windows x86_64
zig build -Dtarget=x86_64-windows-gnu -Doptimize=ReleaseFast
```

## Troubleshooting

### "cannot find -lgalleon"

**For users:** Ensure you're on a supported platform. The prebuilt libraries should be included automatically.

**For developers:** Build the Zig library first:
```bash
cd core && zig build -Doptimize=ReleaseFast
```

Then use `-tags dev`:
```bash
go build -tags dev ./...
```

### "CGO_ENABLED=0"

CGO must be enabled:
```bash
export CGO_ENABLED=1
go build ./...
```

### "zig: command not found"

Add Zig to your PATH:
```bash
export PATH=$PATH:/path/to/zig
```

### Platform Not Supported

If you're on an unsupported platform, you can build from source:
```bash
cd core
zig build -Doptimize=ReleaseFast
cd ../go
go build -tags dev ./...
```

## Next Steps

- [Quick Start](02-quickstart.md) - Get started with Galleon
- [API Reference](../03-api/01-dataframe.md) - Full API documentation
- [Examples](../../go/examples/README.md) - Code examples
