package galleon

import (
	"strings"
	"testing"
)

// ============================================================================
// DType String Tests
// ============================================================================

func TestDType_String(t *testing.T) {
	tests := []struct {
		dtype    DType
		expected string
	}{
		{Float64, "Float64"},
		{Float32, "Float32"},
		{Int64, "Int64"},
		{Int32, "Int32"},
		{UInt64, "UInt64"},
		{UInt32, "UInt32"},
		{Bool, "Bool"},
		{String, "String"},
		{DateTime, "DateTime"},
		{Duration, "Duration"},
		{Null, "Null"},
	}

	for _, tc := range tests {
		result := tc.dtype.String()
		if result != tc.expected {
			t.Errorf("DType(%d).String() = %q, want %q", tc.dtype, result, tc.expected)
		}
	}

	// Test unknown type
	unknown := DType(255)
	result := unknown.String()
	if !strings.HasPrefix(result, "Unknown") {
		t.Errorf("Unknown DType.String() = %q, want prefix 'Unknown'", result)
	}
}

// ============================================================================
// DType IsNumeric Tests
// ============================================================================

func TestDType_IsNumeric(t *testing.T) {
	numericTypes := []DType{Float64, Float32, Int64, Int32, UInt64, UInt32}
	nonNumericTypes := []DType{Bool, String, DateTime, Duration, Null}

	for _, dt := range numericTypes {
		if !dt.IsNumeric() {
			t.Errorf("%s.IsNumeric() = false, want true", dt)
		}
	}

	for _, dt := range nonNumericTypes {
		if dt.IsNumeric() {
			t.Errorf("%s.IsNumeric() = true, want false", dt)
		}
	}
}

// ============================================================================
// DType IsFloat Tests
// ============================================================================

func TestDType_IsFloat(t *testing.T) {
	floatTypes := []DType{Float64, Float32}
	nonFloatTypes := []DType{Int64, Int32, UInt64, UInt32, Bool, String, DateTime, Duration, Null}

	for _, dt := range floatTypes {
		if !dt.IsFloat() {
			t.Errorf("%s.IsFloat() = false, want true", dt)
		}
	}

	for _, dt := range nonFloatTypes {
		if dt.IsFloat() {
			t.Errorf("%s.IsFloat() = true, want false", dt)
		}
	}
}

// ============================================================================
// DType IsInteger Tests
// ============================================================================

func TestDType_IsInteger(t *testing.T) {
	intTypes := []DType{Int64, Int32, UInt64, UInt32}
	nonIntTypes := []DType{Float64, Float32, Bool, String, DateTime, Duration, Null}

	for _, dt := range intTypes {
		if !dt.IsInteger() {
			t.Errorf("%s.IsInteger() = false, want true", dt)
		}
	}

	for _, dt := range nonIntTypes {
		if dt.IsInteger() {
			t.Errorf("%s.IsInteger() = true, want false", dt)
		}
	}
}

// ============================================================================
// DType IsSigned Tests
// ============================================================================

func TestDType_IsSigned(t *testing.T) {
	signedTypes := []DType{Float64, Float32, Int64, Int32}
	unsignedTypes := []DType{UInt64, UInt32, Bool, String, DateTime, Duration, Null}

	for _, dt := range signedTypes {
		if !dt.IsSigned() {
			t.Errorf("%s.IsSigned() = false, want true", dt)
		}
	}

	for _, dt := range unsignedTypes {
		if dt.IsSigned() {
			t.Errorf("%s.IsSigned() = true, want false", dt)
		}
	}
}

// ============================================================================
// DType Size Tests
// ============================================================================

func TestDType_Size(t *testing.T) {
	tests := []struct {
		dtype    DType
		expected int
	}{
		{Float64, 8},
		{Int64, 8},
		{UInt64, 8},
		{DateTime, 8},
		{Duration, 8},
		{Float32, 4},
		{Int32, 4},
		{UInt32, 4},
		{Bool, 1},
		{String, -1}, // Variable size
		{Null, 0},
	}

	for _, tc := range tests {
		result := tc.dtype.Size()
		if result != tc.expected {
			t.Errorf("%s.Size() = %d, want %d", tc.dtype, result, tc.expected)
		}
	}

	// Test unknown type
	unknown := DType(255)
	if unknown.Size() != 0 {
		t.Errorf("Unknown DType.Size() = %d, want 0", unknown.Size())
	}
}

// ============================================================================
// Schema Tests
// ============================================================================

func TestNewSchema(t *testing.T) {
	names := []string{"a", "b", "c"}
	dtypes := []DType{Float64, Int64, String}

	schema, err := NewSchema(names, dtypes)
	if err != nil {
		t.Fatalf("NewSchema failed: %v", err)
	}

	if schema.Len() != 3 {
		t.Errorf("Schema.Len() = %d, want 3", schema.Len())
	}
}

func TestNewSchema_LengthMismatch(t *testing.T) {
	names := []string{"a", "b"}
	dtypes := []DType{Float64, Int64, String}

	_, err := NewSchema(names, dtypes)
	if err == nil {
		t.Error("Expected error for length mismatch")
	}
}

func TestNewSchema_DuplicateNames(t *testing.T) {
	names := []string{"a", "b", "a"} // Duplicate "a"
	dtypes := []DType{Float64, Int64, String}

	_, err := NewSchema(names, dtypes)
	if err == nil {
		t.Error("Expected error for duplicate names")
	}
}

func TestSchema_Len(t *testing.T) {
	schema, _ := NewSchema([]string{"a", "b"}, []DType{Float64, Int64})
	if schema.Len() != 2 {
		t.Errorf("Len() = %d, want 2", schema.Len())
	}
}

func TestSchema_Names(t *testing.T) {
	schema, _ := NewSchema([]string{"a", "b", "c"}, []DType{Float64, Int64, String})

	names := schema.Names()
	if len(names) != 3 {
		t.Errorf("Names() length = %d, want 3", len(names))
	}

	expected := []string{"a", "b", "c"}
	for i, name := range names {
		if name != expected[i] {
			t.Errorf("Names()[%d] = %q, want %q", i, name, expected[i])
		}
	}

	// Verify it returns a copy (modifying doesn't affect original)
	names[0] = "modified"
	origNames := schema.Names()
	if origNames[0] != "a" {
		t.Error("Names() should return a copy")
	}
}

func TestSchema_DTypes(t *testing.T) {
	schema, _ := NewSchema([]string{"a", "b", "c"}, []DType{Float64, Int64, String})

	dtypes := schema.DTypes()
	if len(dtypes) != 3 {
		t.Errorf("DTypes() length = %d, want 3", len(dtypes))
	}

	expected := []DType{Float64, Int64, String}
	for i, dt := range dtypes {
		if dt != expected[i] {
			t.Errorf("DTypes()[%d] = %v, want %v", i, dt, expected[i])
		}
	}

	// Verify it returns a copy
	dtypes[0] = Bool
	origDTypes := schema.DTypes()
	if origDTypes[0] != Float64 {
		t.Error("DTypes() should return a copy")
	}
}

func TestSchema_GetDType(t *testing.T) {
	schema, _ := NewSchema([]string{"a", "b", "c"}, []DType{Float64, Int64, String})

	// Existing column
	dt, ok := schema.GetDType("b")
	if !ok || dt != Int64 {
		t.Errorf("GetDType('b') = (%v, %v), want (Int64, true)", dt, ok)
	}

	// Non-existing column
	dt, ok = schema.GetDType("x")
	if ok {
		t.Errorf("GetDType('x') should return false, got (%v, %v)", dt, ok)
	}
	if dt != Null {
		t.Errorf("GetDType('x') should return Null dtype, got %v", dt)
	}
}

func TestSchema_GetIndex(t *testing.T) {
	schema, _ := NewSchema([]string{"a", "b", "c"}, []DType{Float64, Int64, String})

	// Existing column
	idx, ok := schema.GetIndex("b")
	if !ok || idx != 1 {
		t.Errorf("GetIndex('b') = (%d, %v), want (1, true)", idx, ok)
	}

	idx, ok = schema.GetIndex("c")
	if !ok || idx != 2 {
		t.Errorf("GetIndex('c') = (%d, %v), want (2, true)", idx, ok)
	}

	// Non-existing column
	idx, ok = schema.GetIndex("x")
	if ok {
		t.Errorf("GetIndex('x') should return false")
	}
	if idx != -1 {
		t.Errorf("GetIndex('x') should return -1, got %d", idx)
	}
}

func TestSchema_String(t *testing.T) {
	schema, _ := NewSchema([]string{"a", "b"}, []DType{Float64, Int64})

	str := schema.String()

	// Verify it contains expected content
	if !strings.Contains(str, "Schema{") {
		t.Error("Schema.String() should contain 'Schema{'")
	}
	if !strings.Contains(str, "a: Float64") {
		t.Error("Schema.String() should contain 'a: Float64'")
	}
	if !strings.Contains(str, "b: Int64") {
		t.Error("Schema.String() should contain 'b: Int64'")
	}
}

// ============================================================================
// DType Combination Tests
// ============================================================================

func TestDType_Combinations(t *testing.T) {
	// Test that numeric types are correctly identified
	allTypes := []DType{Float64, Float32, Int64, Int32, UInt64, UInt32, Bool, String, DateTime, Duration, Null}

	for _, dt := range allTypes {
		// IsFloat should imply IsNumeric
		if dt.IsFloat() && !dt.IsNumeric() {
			t.Errorf("%s.IsFloat() but not IsNumeric()", dt)
		}

		// IsInteger should imply IsNumeric
		if dt.IsInteger() && !dt.IsNumeric() {
			t.Errorf("%s.IsInteger() but not IsNumeric()", dt)
		}

		// Float and Integer should be mutually exclusive
		if dt.IsFloat() && dt.IsInteger() {
			t.Errorf("%s is both Float and Integer", dt)
		}
	}
}
