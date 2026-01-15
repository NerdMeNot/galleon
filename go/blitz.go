package galleon

/*
#include "galleon.h"
*/
import "C"

import (
	"sync"
)

// ============================================================================
// Blitz - Parallel Execution Engine (Diagnostic Functions)
// ============================================================================
//
// Blitz is Galleon's work-stealing parallel execution engine. It automatically
// parallelizes operations on large datasets (>100K elements).
//
// You don't need to call these functions directly - Blitz auto-initializes
// on first use and the regular Galleon functions (SumF64, MinF64, etc.)
// automatically use parallel execution for large data.
//
// These functions are provided for diagnostics and explicit lifecycle control.

var (
	blitzOnce sync.Once
	blitzInit bool
)

// BlitzInit initializes the Blitz parallel execution engine.
// This is called automatically on first use of parallel operations.
// Returns true if initialization succeeded, false otherwise.
func BlitzInit() bool {
	blitzOnce.Do(func() {
		blitzInit = bool(C.blitz_init())
	})
	return blitzInit
}

// BlitzDeinit shuts down the Blitz parallel execution engine.
// This should be called when done with parallel operations to clean up resources.
func BlitzDeinit() {
	C.blitz_deinit()
}

// BlitzIsInitialized returns true if Blitz has been initialized.
func BlitzIsInitialized() bool {
	return bool(C.blitz_is_initialized())
}

// BlitzNumWorkers returns the number of worker threads in the Blitz pool.
func BlitzNumWorkers() int {
	return int(C.blitz_num_workers())
}
