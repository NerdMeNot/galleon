//go:build dev

// This file is used for local development when building from source.
// Build with: go build -tags dev
//
// Before building, ensure you have built the Zig library:
//   cd core && zig build -Doptimize=ReleaseFast

package galleon

/*
#cgo CFLAGS: -I${SRCDIR}/../core/include
#cgo LDFLAGS: -L${SRCDIR}/../core/zig-out/lib -lgalleon
*/
import "C"
