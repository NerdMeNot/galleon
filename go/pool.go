package galleon

import (
	"sync"
)

// BoolMask is a pooled boolean slice for filter operations
// Call Release() when done to return it to the pool
type BoolMask struct {
	Data []bool
	pool *sync.Pool
}

// Release returns the mask to the pool for reuse
func (m *BoolMask) Release() {
	if m.pool != nil && m.Data != nil {
		// Clear the slice but keep capacity
		for i := range m.Data {
			m.Data[i] = false
		}
		m.pool.Put(m)
	}
}

// Uint32Slice is a pooled uint32 slice for index operations
type Uint32Slice struct {
	Data []uint32
	pool *sync.Pool
}

// Release returns the slice to the pool for reuse
func (s *Uint32Slice) Release() {
	if s.pool != nil && s.Data != nil {
		s.pool.Put(s)
	}
}

// Pool sizes - we use power-of-2 buckets for efficiency
var (
	boolPools   [32]*sync.Pool // pools for sizes 2^0 to 2^31
	uint32Pools [32]*sync.Pool
	poolInit    sync.Once
)

func initPools() {
	poolInit.Do(func() {
		for i := range boolPools {
			size := 1 << i
			boolPools[i] = &sync.Pool{
				New: func() interface{} {
					return &BoolMask{
						Data: make([]bool, size),
					}
				},
			}
			uint32Pools[i] = &sync.Pool{
				New: func() interface{} {
					return &Uint32Slice{
						Data: make([]uint32, size),
					}
				},
			}
		}
	})
}

// getBucket returns the pool bucket index for a given size
func getBucket(size int) int {
	if size <= 0 {
		return 0
	}
	// Find the smallest power of 2 >= size
	bucket := 0
	n := size - 1
	for n > 0 {
		n >>= 1
		bucket++
	}
	if bucket >= 32 {
		bucket = 31
	}
	return bucket
}

// getBoolMask gets a bool mask from the pool with at least 'size' capacity
func getBoolMask(size int) *BoolMask {
	initPools()
	bucket := getBucket(size)
	pool := boolPools[bucket]
	mask := pool.Get().(*BoolMask)
	mask.pool = pool

	// Ensure correct size (pool may have larger capacity)
	poolSize := 1 << bucket
	if len(mask.Data) != size {
		mask.Data = mask.Data[:size]
	}
	// If we need more than pool size, allocate new
	if size > poolSize {
		mask.Data = make([]bool, size)
	}

	return mask
}

// getUint32Slice gets a uint32 slice from the pool with at least 'size' capacity
func getUint32Slice(size int) *Uint32Slice {
	initPools()
	bucket := getBucket(size)
	pool := uint32Pools[bucket]
	slice := pool.Get().(*Uint32Slice)
	slice.pool = pool

	poolSize := 1 << bucket
	if len(slice.Data) != size {
		slice.Data = slice.Data[:size]
	}
	if size > poolSize {
		slice.Data = make([]uint32, size)
	}

	return slice
}
