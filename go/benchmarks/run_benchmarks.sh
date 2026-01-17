#!/bin/bash
# Run Galleon comprehensive benchmarks using Podman (or Docker)
#
# Usage:
#   ./run_benchmarks.sh              # Run with default sizes (100K, 1M)
#   ./run_benchmarks.sh --sizes 1000000,10000000  # Custom sizes
#   ./run_benchmarks.sh --docker     # Use Docker instead of Podman

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
CONTAINER_TOOL="podman"
IMAGE_NAME="galleon-benchmark"
EXTRA_ARGS=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --docker)
            CONTAINER_TOOL="docker"
            shift
            ;;
        --sizes)
            EXTRA_ARGS="--sizes $2"
            shift 2
            ;;
        *)
            EXTRA_ARGS="$EXTRA_ARGS $1"
            shift
            ;;
    esac
done

echo "========================================================================"
echo "GALLEON BENCHMARK RUNNER"
echo "========================================================================"
echo ""
echo "Container tool: $CONTAINER_TOOL"
echo "Project root: $PROJECT_ROOT"
echo ""

# Check if container tool is available
if ! command -v $CONTAINER_TOOL &> /dev/null; then
    echo "Error: $CONTAINER_TOOL is not installed"
    if [ "$CONTAINER_TOOL" = "podman" ]; then
        echo "Install with: brew install podman (macOS) or apt install podman (Linux)"
        echo "Or use --docker flag to use Docker instead"
    fi
    exit 1
fi

# Always rebuild to pick up code changes (builds are cached so this is fast)
# We can't use volume mounts because the host's compiled Zig library (macOS)
# is incompatible with the container's Linux environment
echo "Building benchmark container..."
echo ""
cd "$PROJECT_ROOT"
$CONTAINER_TOOL build -t $IMAGE_NAME -f go/benchmarks/Dockerfile .
echo ""
echo "Build complete."
echo ""

# Run benchmarks (no volume mount - use code from image)
echo "========================================================================"
echo "RUNNING BENCHMARKS"
echo "========================================================================"
echo ""

$CONTAINER_TOOL run --rm \
    $IMAGE_NAME \
    python3 /galleon/go/benchmarks/benchmark_comparison.py $EXTRA_ARGS

echo ""
echo "========================================================================"
echo "BENCHMARK COMPLETE"
echo "========================================================================"
