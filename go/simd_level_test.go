package galleon

import (
	"testing"
)

func TestGetSimdLevel(t *testing.T) {
	level := GetSimdLevel()
	// Level should be between Scalar (0) and AVX512 (3)
	if level > SimdAVX512 {
		t.Errorf("Invalid SIMD level: %d", level)
	}
	t.Logf("Detected SIMD level: %v (level %d)", level.String(), level)
}

func TestGetSimdLevelName(t *testing.T) {
	name := GetSimdLevelName()
	if name == "" {
		t.Error("SIMD level name should not be empty")
	}
	t.Logf("SIMD level name: %s", name)
}

func TestGetSimdVectorBytes(t *testing.T) {
	bytes := GetSimdVectorBytes()
	// Valid vector bytes: 8 (scalar), 16 (SSE4), 32 (AVX2), 64 (AVX512)
	validBytes := map[int]bool{8: true, 16: true, 32: true, 64: true}
	if !validBytes[bytes] {
		t.Errorf("Invalid vector bytes: %d", bytes)
	}
	t.Logf("Vector bytes: %d", bytes)
}

func TestGetSimdConfig(t *testing.T) {
	config := GetSimdConfig()
	t.Logf("SIMD Config: Level=%s, Name=%s, VectorBytes=%d",
		config.Level.String(), config.LevelName, config.VectorBytes)

	// Verify consistency
	if config.Level.String() != config.LevelName {
		t.Errorf("Level string mismatch: %s vs %s", config.Level.String(), config.LevelName)
	}
}

func TestSimdLevelString(t *testing.T) {
	tests := []struct {
		level    SimdLevel
		expected string
	}{
		{SimdScalar, "Scalar"},
		{SimdSSE4, "SSE4"},
		{SimdAVX2, "AVX2"},
		{SimdAVX512, "AVX-512"},
	}

	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.expected {
			t.Errorf("SimdLevel(%d).String() = %s, want %s", tt.level, got, tt.expected)
		}
	}
}

func TestSimdLevelVectorBytes(t *testing.T) {
	tests := []struct {
		level    SimdLevel
		expected int
	}{
		{SimdScalar, 8},
		{SimdSSE4, 16},
		{SimdAVX2, 32},
		{SimdAVX512, 64},
	}

	for _, tt := range tests {
		got := tt.level.VectorBytes()
		if got != tt.expected {
			t.Errorf("SimdLevel(%d).VectorBytes() = %d, want %d", tt.level, got, tt.expected)
		}
	}
}
