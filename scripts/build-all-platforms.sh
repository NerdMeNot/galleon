#!/bin/bash
#
# Build prebuilt libraries for all supported platforms.
# Run from the repository root directory.
#
# Usage: ./scripts/build-all-platforms.sh
#
# Requirements: Zig 0.15.2+

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CORE_DIR="$REPO_ROOT/core"
LIB_DIR="$REPO_ROOT/go/lib"

echo "Building Galleon for all platforms..."
echo "Core directory: $CORE_DIR"
echo "Output directory: $LIB_DIR"
echo ""

cd "$CORE_DIR"

# Platform configurations: target, output_dir, lib_name
PLATFORMS=(
    "aarch64-macos:darwin_arm64:libgalleon.a"
    "x86_64-macos:darwin_amd64:libgalleon.a"
    "x86_64-linux-gnu:linux_amd64:libgalleon.a"
    "aarch64-linux-gnu:linux_arm64:libgalleon.a"
    "x86_64-windows-gnu:windows_amd64:galleon.lib"
)

for platform in "${PLATFORMS[@]}"; do
    IFS=':' read -r target output_dir lib_name <<< "$platform"

    echo "Building $target..."

    # Clean and build
    rm -rf zig-out
    zig build -Dtarget="$target" -Doptimize=ReleaseFast

    # Create output directory if needed
    mkdir -p "$LIB_DIR/$output_dir"

    # Copy library (always as libgalleon.a for consistency)
    cp "zig-out/lib/$lib_name" "$LIB_DIR/$output_dir/libgalleon.a"

    # Copy header
    cp include/galleon.h "$LIB_DIR/$output_dir/"

    # Show result
    size=$(ls -lh "$LIB_DIR/$output_dir/libgalleon.a" | awk '{print $5}')
    echo "  ✓ $output_dir ($size)"
done

echo ""
echo "All platforms built successfully!"
echo ""
echo "To commit:"
echo "  git add go/lib/"
echo "  git commit -m 'Rebuild prebuilt libraries'"
