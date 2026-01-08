# Galleon - High-performance DataFrame library
# Run `just` to see available commands

set shell := ["bash", "-cu"]

# Default recipe: show help
default:
    @just --list

# === Build Commands ===

# Build Zig core library (release)
build-zig:
    cd core && zig build -Doptimize=ReleaseFast

# Build Zig core library (debug)
build-zig-debug:
    cd core && zig build

# Build Go package
build-go:
    cd go && go build ./...

# Build everything
build: build-zig build-go

# === Test Commands ===

# Run Go tests
test:
    cd go && go test ./...

# Run Go tests with verbose output
test-v:
    cd go && go test -v ./...

# Run specific Go test (usage: just test-run TestName)
test-run TEST:
    cd go && go test -v -run {{TEST}} ./...

# Run Zig tests
test-zig:
    cd core && zig build test

# Run all tests (Zig + Go)
test-all: test-zig test

# === Coverage Commands ===

# Run Go tests with coverage
coverage:
    cd go && go test -coverprofile=coverage.out ./...
    @cd go && go tool cover -func=coverage.out | tail -1

# Generate HTML coverage report
coverage-html: coverage
    cd go && go tool cover -html=coverage.out -o coverage.html
    @echo "Coverage report: go/coverage.html"

# Show detailed coverage by function
coverage-func:
    cd go && go test -coverprofile=coverage.out ./...
    cd go && go tool cover -func=coverage.out

# Show coverage for specific file (usage: just coverage-file lazy_executor)
coverage-file FILE:
    cd go && go test -coverprofile=coverage.out ./...
    cd go && go tool cover -func=coverage.out | grep {{FILE}}

# === Benchmark Commands ===

# Run Go benchmarks
bench:
    cd go && go test -bench=. ./...

# Run specific benchmark (usage: just bench-run BenchmarkName)
bench-run BENCH:
    cd go && go test -bench={{BENCH}} -benchmem ./...

# Run benchmarks with CPU profiling
bench-profile:
    cd go && go test -bench=. -cpuprofile=cpu.prof ./benchmarks/
    @echo "Profile: go/cpu.prof (use 'go tool pprof cpu.prof')"

# === Development Commands ===

# Format Go code
fmt:
    cd go && go fmt ./...

# Run Go linter
lint:
    cd go && go vet ./...

# Tidy Go modules
tidy:
    cd go && go mod tidy

# Clean build artifacts
clean:
    rm -rf core/zig-out core/zig-cache
    rm -f go/coverage.out go/coverage.html go/*.prof

# === Quick Checks ===

# Quick check: format, lint, test
check: fmt lint test

# Full CI check: all tests with coverage
ci: test-zig coverage
    @echo "All checks passed!"

# === Examples ===

# Run a specific example (usage: just example 01_basic_usage)
example NAME:
    cd go/examples/{{NAME}} && go run main.go
