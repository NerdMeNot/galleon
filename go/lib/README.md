# Prebuilt Libraries

This directory contains prebuilt static libraries for each supported platform, enabling users to install Galleon via `go get` without needing Zig installed.

## Supported Platforms

| Directory | Platform | Architecture |
|-----------|----------|--------------|
| `darwin_arm64` | macOS | Apple Silicon (M1/M2/M3) |
| `darwin_amd64` | macOS | Intel x86_64 |
| `linux_amd64` | Linux | x86_64 |
| `linux_arm64` | Linux | ARM64 (aarch64) |
| `windows_amd64` | Windows | x86_64 |

## For Users

Just install normally - prebuilt libraries are used automatically:

```bash
go get github.com/NerdMeNot/galleon/go
```

**Requirements:** A C compiler is still needed for CGO:
- **macOS**: Xcode Command Line Tools (`xcode-select --install`)
- **Linux**: GCC (usually pre-installed)
- **Windows**: MinGW-w64 or MSVC

## For Developers

### Building from Source

If you're modifying the Zig code, build locally with the `dev` tag:

```bash
# 1. Build the Zig library
cd core
zig build -Doptimize=ReleaseFast

# 2. Build/test Go with dev tag
cd ../go
go build -tags dev ./...
go test -tags dev ./...
```

### Rebuilding Prebuilt Libraries

When Zig code changes, rebuild all platforms from macOS (Zig cross-compiles everything):

```bash
cd core

# macOS ARM64
rm -rf zig-out && zig build -Dtarget=aarch64-macos -Doptimize=ReleaseFast
cp zig-out/lib/libgalleon.a ../go/lib/darwin_arm64/
cp include/galleon.h ../go/lib/darwin_arm64/

# macOS Intel
rm -rf zig-out && zig build -Dtarget=x86_64-macos -Doptimize=ReleaseFast
cp zig-out/lib/libgalleon.a ../go/lib/darwin_amd64/
cp include/galleon.h ../go/lib/darwin_amd64/

# Linux x86_64
rm -rf zig-out && zig build -Dtarget=x86_64-linux-gnu -Doptimize=ReleaseFast
cp zig-out/lib/libgalleon.a ../go/lib/linux_amd64/
cp include/galleon.h ../go/lib/linux_amd64/

# Linux ARM64
rm -rf zig-out && zig build -Dtarget=aarch64-linux-gnu -Doptimize=ReleaseFast
cp zig-out/lib/libgalleon.a ../go/lib/linux_arm64/
cp include/galleon.h ../go/lib/linux_arm64/

# Windows x86_64 (note: outputs galleon.lib)
rm -rf zig-out && zig build -Dtarget=x86_64-windows-gnu -Doptimize=ReleaseFast
cp zig-out/lib/galleon.lib ../go/lib/windows_amd64/libgalleon.a
cp include/galleon.h ../go/lib/windows_amd64/
```

Then commit the updated libraries:

```bash
git add go/lib/
git commit -m "Rebuild prebuilt libraries"
```

### Requirements

- **Zig 0.15.2+** for building
- Cross-compilation works from any platform, but macOS is recommended

## How It Works

The Go package uses build tags to select the correct library:

- `go/cgo_darwin_arm64.go` - macOS Apple Silicon
- `go/cgo_darwin_amd64.go` - macOS Intel
- `go/cgo_linux_amd64.go` - Linux x86_64
- `go/cgo_linux_arm64.go` - Linux ARM64
- `go/cgo_windows_amd64.go` - Windows x86_64
- `go/cgo_dev.go` - Local development (requires `-tags dev`)

When building without `-tags dev`, the platform-specific file is selected automatically based on `GOOS` and `GOARCH`.
