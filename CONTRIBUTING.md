# Contributing to Galleon

Thank you for your interest in contributing to Galleon! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- **Go 1.21+**: [Download Go](https://golang.org/dl/)
- **Zig 0.11+**: [Download Zig](https://ziglang.org/download/)
- **Git**: For version control

### Building from Source

```bash
# Clone the repository
git clone https://github.com/NerdMeNot/galleon.git
cd galleon

# Build the Zig library
cd core
zig build -Doptimize=ReleaseFast

# Run Go tests
cd ../go
go test ./...

# Run benchmarks
go test -bench=. ./benchmarks/
```

## Project Structure

```
galleon/
├── core/           # Zig SIMD backend
│   ├── src/
│   │   ├── simd.zig      # SIMD operations, joins
│   │   ├── groupby.zig   # GroupBy hash tables
│   │   ├── column.zig    # Column storage types
│   │   └── main.zig      # CGO exports
│   ├── include/
│   │   └── galleon.h     # C header for CGO
│   └── build.zig
└── go/             # Go package
    ├── galleon.go        # CGO bindings, low-level wrappers
    ├── series.go         # Series type and operations
    ├── dataframe.go      # DataFrame type and operations
    ├── dtype.go          # Type system
    ├── groupby.go        # GroupBy implementation
    ├── join.go           # Join implementations
    ├── expr.go           # Expression system
    ├── lazyframe.go      # Lazy evaluation API
    ├── lazy_executor.go  # Plan execution
    ├── lazy_optimizer.go # Query optimization
    ├── parallel.go       # Parallel execution
    ├── pool.go           # Memory pooling
    ├── io_*.go           # I/O operations
    └── *_test.go         # Test files
```

## Code Style

### Go Code

- Follow standard Go conventions (`gofmt`, `go vet`)
- Use meaningful variable and function names
- Add comments for exported functions
- Keep functions focused and reasonably sized

### Zig Code

- Follow Zig style guide
- Use `snake_case` for functions and variables
- Add doc comments for public functions
- Prefer explicit error handling

## Making Changes

### 1. Fork and Clone

```bash
git clone https://github.com/YOUR_USERNAME/galleon.git
cd galleon
git remote add upstream https://github.com/NerdMeNot/galleon.git
```

### 2. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

### 3. Make Changes

- Write clean, well-documented code
- Add tests for new functionality
- Update documentation if needed

### 4. Test Your Changes

```bash
# Build Zig library
cd core && zig build -Doptimize=ReleaseFast && cd ..

# Run all tests
cd go && go test ./...

# Run specific test
go test -v -run TestYourFunction

# Run benchmarks
go test -bench=. ./benchmarks/
```

### 5. Commit and Push

```bash
git add .
git commit -m "feat: Add your feature description"
git push origin feature/your-feature-name
```

### 6. Create Pull Request

- Go to the repository on GitHub
- Click "New Pull Request"
- Select your branch
- Fill in the PR template

## Commit Messages

Follow conventional commits format:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Adding or updating tests
- `perf:` Performance improvements
- `refactor:` Code refactoring
- `chore:` Maintenance tasks

Examples:
```
feat: Add parallel left join implementation
fix: Handle null values in groupby aggregation
docs: Update API documentation for Series
perf: Optimize hash table probing with prefetch
```

## Pull Request Guidelines

1. **One feature/fix per PR**: Keep PRs focused
2. **Tests required**: Add tests for new functionality
3. **Documentation**: Update docs if changing public API
4. **Benchmarks**: Include benchmarks for performance changes
5. **Clean history**: Squash commits if needed

## Areas for Contribution

### Good First Issues

- Documentation improvements
- Additional test coverage
- Error message improvements
- Code cleanup and refactoring

### Intermediate

- New aggregation functions
- Additional I/O format support
- Query optimization rules
- Memory pool improvements

### Advanced

- SIMD optimizations
- New join algorithms
- Parallel execution improvements
- GPU acceleration

## Testing

### Unit Tests

```bash
cd go
go test -v ./...
```

### Benchmarks

```bash
cd go
go test -bench=. -benchmem ./benchmarks/
```

### Coverage

```bash
cd go
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Performance Guidelines

When contributing performance-sensitive code:

1. **Benchmark before and after**: Measure the impact
2. **Consider memory**: Track allocations with `-benchmem`
3. **Test at scale**: Use realistic data sizes (1M+ rows)
4. **Profile hotspots**: Use `go tool pprof`

Example benchmark:
```go
func BenchmarkYourOperation(b *testing.B) {
    data := generateTestData(1_000_000)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        YourOperation(data)
    }
}
```

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Check existing issues before creating new ones

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow

Thank you for contributing to Galleon!
