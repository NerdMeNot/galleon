package galleon

import (
	"math"
	"testing"
)

func TestSumHorizontal2F64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	b := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	out := make([]float64, len(a))

	SumHorizontal2F64(a, b, out)

	expected := []float64{11.0, 22.0, 33.0, 44.0, 55.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("SumHorizontal2F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestSumHorizontal3F64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	b := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	c := []float64{100.0, 200.0, 300.0, 400.0, 500.0}
	out := make([]float64, len(a))

	SumHorizontal3F64(a, b, c, out)

	expected := []float64{111.0, 222.0, 333.0, 444.0, 555.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("SumHorizontal3F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestMinHorizontal2F64(t *testing.T) {
	a := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	b := []float64{3.0, 7.0, 4.0, 6.0, 2.0}
	out := make([]float64, len(a))

	MinHorizontal2F64(a, b, out)

	expected := []float64{3.0, 2.0, 4.0, 1.0, 2.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("MinHorizontal2F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestMinHorizontal3F64(t *testing.T) {
	a := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	b := []float64{3.0, 7.0, 4.0, 6.0, 2.0}
	c := []float64{4.0, 1.0, 9.0, 3.0, 5.0}
	out := make([]float64, len(a))

	MinHorizontal3F64(a, b, c, out)

	expected := []float64{3.0, 1.0, 4.0, 1.0, 2.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("MinHorizontal3F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestMaxHorizontal2F64(t *testing.T) {
	a := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	b := []float64{3.0, 7.0, 4.0, 6.0, 2.0}
	out := make([]float64, len(a))

	MaxHorizontal2F64(a, b, out)

	expected := []float64{5.0, 7.0, 8.0, 6.0, 9.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("MaxHorizontal2F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestMaxHorizontal3F64(t *testing.T) {
	a := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	b := []float64{3.0, 7.0, 4.0, 6.0, 2.0}
	c := []float64{4.0, 1.0, 10.0, 3.0, 5.0}
	out := make([]float64, len(a))

	MaxHorizontal3F64(a, b, c, out)

	expected := []float64{5.0, 7.0, 10.0, 6.0, 9.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("MaxHorizontal3F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestProductHorizontal2F64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	b := []float64{2.0, 3.0, 4.0, 5.0, 6.0}
	out := make([]float64, len(a))

	ProductHorizontal2F64(a, b, out)

	expected := []float64{2.0, 6.0, 12.0, 20.0, 30.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("ProductHorizontal2F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestProductHorizontal3F64(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	b := []float64{2.0, 2.0, 2.0, 2.0, 2.0}
	c := []float64{3.0, 3.0, 3.0, 3.0, 3.0}
	out := make([]float64, len(a))

	ProductHorizontal3F64(a, b, c, out)

	expected := []float64{6.0, 12.0, 18.0, 24.0, 30.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("ProductHorizontal3F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestAnyHorizontal2(t *testing.T) {
	a := []uint8{0, 1, 0, 1, 0}
	b := []uint8{0, 0, 1, 1, 0}
	out := make([]uint8, len(a))

	AnyHorizontal2(a, b, out)

	expected := []uint8{0, 1, 1, 1, 0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("AnyHorizontal2 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestAllHorizontal2(t *testing.T) {
	a := []uint8{0, 1, 0, 1, 0}
	b := []uint8{0, 0, 1, 1, 0}
	out := make([]uint8, len(a))

	AllHorizontal2(a, b, out)

	expected := []uint8{0, 0, 0, 1, 0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("AllHorizontal2 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCountNonNullHorizontal2F64(t *testing.T) {
	nan := math.NaN()
	a := []float64{1.0, nan, 3.0, nan, 5.0}
	b := []float64{nan, 2.0, nan, 4.0, 5.0}
	out := make([]uint32, len(a))

	CountNonNullHorizontal2F64(a, b, out)

	expected := []uint32{1, 1, 1, 1, 2}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("CountNonNullHorizontal2F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCountNonNullHorizontal3F64(t *testing.T) {
	nan := math.NaN()
	a := []float64{1.0, nan, 3.0, nan, 5.0}
	b := []float64{nan, 2.0, nan, 4.0, 5.0}
	c := []float64{1.0, 2.0, nan, nan, 5.0}
	out := make([]uint32, len(a))

	CountNonNullHorizontal3F64(a, b, c, out)

	expected := []uint32{2, 2, 1, 1, 3}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("CountNonNullHorizontal3F64 out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

// Test with larger data to exercise SIMD paths
func TestSumHorizontal2F64Large(t *testing.T) {
	n := 1000
	a := make([]float64, n)
	b := make([]float64, n)
	out := make([]float64, n)

	for i := 0; i < n; i++ {
		a[i] = float64(i)
		b[i] = float64(i * 2)
	}

	SumHorizontal2F64(a, b, out)

	for i := 0; i < n; i++ {
		expected := float64(i) + float64(i*2)
		if out[i] != expected {
			t.Errorf("SumHorizontal2F64 (large) out[%d] = %v, want %v", i, out[i], expected)
		}
	}
}

func TestMinHorizontal2F64Large(t *testing.T) {
	n := 1000
	a := make([]float64, n)
	b := make([]float64, n)
	out := make([]float64, n)

	for i := 0; i < n; i++ {
		a[i] = float64(i)
		b[i] = float64(n - i)
	}

	MinHorizontal2F64(a, b, out)

	for i := 0; i < n; i++ {
		expected := math.Min(float64(i), float64(n-i))
		if out[i] != expected {
			t.Errorf("MinHorizontal2F64 (large) out[%d] = %v, want %v", i, out[i], expected)
		}
	}
}

func TestMaxHorizontal2F64Large(t *testing.T) {
	n := 1000
	a := make([]float64, n)
	b := make([]float64, n)
	out := make([]float64, n)

	for i := 0; i < n; i++ {
		a[i] = float64(i)
		b[i] = float64(n - i)
	}

	MaxHorizontal2F64(a, b, out)

	for i := 0; i < n; i++ {
		expected := math.Max(float64(i), float64(n-i))
		if out[i] != expected {
			t.Errorf("MaxHorizontal2F64 (large) out[%d] = %v, want %v", i, out[i], expected)
		}
	}
}
