package galleon

import (
	"math"
	"testing"
)

func TestSelectF64(t *testing.T) {
	mask := []byte{1, 0, 1, 0, 1, 0}
	thenVal := []float64{10.0, 20.0, 30.0, 40.0, 50.0, 60.0}
	elseVal := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}
	out := make([]float64, 6)

	SelectF64(mask, thenVal, elseVal, out)

	expected := []float64{10.0, 2.0, 30.0, 4.0, 50.0, 6.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestIsNullF64(t *testing.T) {
	nan := math.NaN()
	data := []float64{1.0, nan, 3.0, nan, 5.0}
	out := make([]byte, len(data))

	IsNullF64(data, out)

	expected := []byte{0, 1, 0, 1, 0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestFillNullF64(t *testing.T) {
	nan := math.NaN()
	data := []float64{1.0, nan, 3.0, nan, 5.0}
	out := make([]float64, len(data))

	FillNullF64(data, 0.0, out)

	expected := []float64{1.0, 0.0, 3.0, 0.0, 5.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCoalesce2F64(t *testing.T) {
	nan := math.NaN()
	a := []float64{1.0, nan, 3.0, nan}
	b := []float64{10.0, 20.0, nan, 40.0}
	out := make([]float64, len(a))

	Coalesce2F64(a, b, out)

	// First non-NaN: a[0]=1, a[1]=NaN->b[1]=20, a[2]=3, a[3]=NaN->b[3]=40
	expected := []float64{1.0, 20.0, 3.0, 40.0}
	for i, exp := range expected {
		if out[i] != exp {
			t.Errorf("out[%d] = %v, want %v", i, out[i], exp)
		}
	}
}

func TestCountNullF64(t *testing.T) {
	nan := math.NaN()
	data := []float64{1.0, nan, 3.0, nan, nan, 6.0}

	nullCount := CountNullF64(data)
	if nullCount != 3 {
		t.Errorf("CountNullF64 = %d, want 3", nullCount)
	}

	notNullCount := CountNotNullF64(data)
	if notNullCount != 3 {
		t.Errorf("CountNotNullF64 = %d, want 3", notNullCount)
	}
}

func TestWhenExprBuilder(t *testing.T) {
	// Test the expression builder API
	expr := When(Col("x").Gt(Lit(0))).Then(Lit(1)).Otherwise(Lit(0))
	
	if expr == nil {
		t.Fatal("WhenExpr should not be nil")
	}
	
	expected := "when((col(\"x\") > lit(0))).then(lit(1)).otherwise(lit(0))"
	if expr.String() != expected {
		t.Errorf("WhenExpr.String() = %q, want %q", expr.String(), expected)
	}
}

func TestIsNullExprBuilder(t *testing.T) {
	expr := Col("x").IsNull()
	
	if expr == nil {
		t.Fatal("IsNullExpr should not be nil")
	}
	
	expected := "col(\"x\").is_null()"
	if expr.String() != expected {
		t.Errorf("IsNullExpr.String() = %q, want %q", expr.String(), expected)
	}
}

func TestFillNullExprBuilder(t *testing.T) {
	expr := Col("x").FillNullLit(0)
	
	if expr == nil {
		t.Fatal("FillNullExpr should not be nil")
	}
	
	expected := "col(\"x\").fill_null(lit(0))"
	if expr.String() != expected {
		t.Errorf("FillNullExpr.String() = %q, want %q", expr.String(), expected)
	}
}

func TestCoalesceExprBuilder(t *testing.T) {
	expr := Coalesce(Col("a"), Col("b"), Lit(0))
	
	if expr == nil {
		t.Fatal("CoalesceExpr should not be nil")
	}
	
	expected := "coalesce(col(\"a\"), col(\"b\"), lit(0))"
	if expr.String() != expected {
		t.Errorf("CoalesceExpr.String() = %q, want %q", expr.String(), expected)
	}
}
