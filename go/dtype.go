package galleon

import "fmt"

// DType represents the data type of a Series
type DType uint8

const (
	// Numeric types
	Float64 DType = iota
	Float32
	Int64
	Int32
	UInt64
	UInt32

	// Other types
	Bool
	String
	DateTime
	Duration

	// Null type
	Null
)

// String returns the string representation of the DType
func (d DType) String() string {
	switch d {
	case Float64:
		return "Float64"
	case Float32:
		return "Float32"
	case Int64:
		return "Int64"
	case Int32:
		return "Int32"
	case UInt64:
		return "UInt64"
	case UInt32:
		return "UInt32"
	case Bool:
		return "Bool"
	case String:
		return "String"
	case DateTime:
		return "DateTime"
	case Duration:
		return "Duration"
	case Null:
		return "Null"
	default:
		return fmt.Sprintf("Unknown(%d)", d)
	}
}

// IsNumeric returns true if the dtype is a numeric type
func (d DType) IsNumeric() bool {
	switch d {
	case Float64, Float32, Int64, Int32, UInt64, UInt32:
		return true
	default:
		return false
	}
}

// IsFloat returns true if the dtype is a floating point type
func (d DType) IsFloat() bool {
	return d == Float64 || d == Float32
}

// IsInteger returns true if the dtype is an integer type
func (d DType) IsInteger() bool {
	switch d {
	case Int64, Int32, UInt64, UInt32:
		return true
	default:
		return false
	}
}

// IsSigned returns true if the dtype is a signed numeric type
func (d DType) IsSigned() bool {
	switch d {
	case Float64, Float32, Int64, Int32:
		return true
	default:
		return false
	}
}

// Size returns the size in bytes of the dtype
func (d DType) Size() int {
	switch d {
	case Float64, Int64, UInt64, DateTime, Duration:
		return 8
	case Float32, Int32, UInt32:
		return 4
	case Bool:
		return 1
	case String:
		return -1 // Variable size
	case Null:
		return 0
	default:
		return 0
	}
}

// Schema represents the schema of a DataFrame
type Schema struct {
	names  []string
	dtypes []DType
}

// NewSchema creates a new schema from column names and types
func NewSchema(names []string, dtypes []DType) (*Schema, error) {
	if len(names) != len(dtypes) {
		return nil, fmt.Errorf("names and dtypes must have same length: %d != %d", len(names), len(dtypes))
	}

	// Check for duplicate names
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			return nil, fmt.Errorf("duplicate column name: %s", name)
		}
		seen[name] = true
	}

	return &Schema{
		names:  append([]string{}, names...),
		dtypes: append([]DType{}, dtypes...),
	}, nil
}

// Len returns the number of columns in the schema
func (s *Schema) Len() int {
	return len(s.names)
}

// Names returns the column names
func (s *Schema) Names() []string {
	return append([]string{}, s.names...)
}

// DTypes returns the column data types
func (s *Schema) DTypes() []DType {
	return append([]DType{}, s.dtypes...)
}

// GetDType returns the dtype for a column name
func (s *Schema) GetDType(name string) (DType, bool) {
	for i, n := range s.names {
		if n == name {
			return s.dtypes[i], true
		}
	}
	return Null, false
}

// GetIndex returns the index of a column name
func (s *Schema) GetIndex(name string) (int, bool) {
	for i, n := range s.names {
		if n == name {
			return i, true
		}
	}
	return -1, false
}

// String returns a string representation of the schema
func (s *Schema) String() string {
	result := "Schema{\n"
	for i, name := range s.names {
		result += fmt.Sprintf("  %s: %s\n", name, s.dtypes[i])
	}
	result += "}"
	return result
}
