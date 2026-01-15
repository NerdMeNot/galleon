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

	// Nested types
	Struct // Struct with named fields
	List   // Variable-length list of elements
	Array  // Fixed-length array of elements

	// Categorical type (dictionary-encoded strings)
	Categorical // String stored as integer indices into a dictionary
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
	case Struct:
		return "Struct"
	case List:
		return "List"
	case Array:
		return "Array"
	case Categorical:
		return "Categorical"
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

// IsNested returns true if the dtype is a nested type (Struct, List, or Array)
func (d DType) IsNested() bool {
	switch d {
	case Struct, List, Array:
		return true
	default:
		return false
	}
}

// IsCategorical returns true if the dtype is Categorical
func (d DType) IsCategorical() bool {
	return d == Categorical
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
	case String, List, Struct, Array, Categorical:
		return -1 // Variable size
	case Null:
		return 0
	default:
		return 0
	}
}

// ============================================================================
// Nested Type Metadata
// ============================================================================

// StructField represents a field in a Struct type
type StructField struct {
	Name  string
	DType DType
	// For nested structs/lists, this holds the inner type info
	Inner interface{} // *StructType or *ListType
}

// StructType describes the structure of a Struct dtype
type StructType struct {
	Fields []StructField
}

// NewStructType creates a new StructType from field definitions
func NewStructType(fields []StructField) *StructType {
	return &StructType{
		Fields: append([]StructField{}, fields...),
	}
}

// GetField returns a field by name
func (s *StructType) GetField(name string) (*StructField, bool) {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return &s.Fields[i], true
		}
	}
	return nil, false
}

// GetFieldIndex returns the index of a field by name
func (s *StructType) GetFieldIndex(name string) (int, bool) {
	for i := range s.Fields {
		if s.Fields[i].Name == name {
			return i, true
		}
	}
	return -1, false
}

// NumFields returns the number of fields
func (s *StructType) NumFields() int {
	return len(s.Fields)
}

// String returns a string representation of the struct type
func (s *StructType) String() string {
	result := "Struct{"
	for i, f := range s.Fields {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%s: %s", f.Name, f.DType)
	}
	result += "}"
	return result
}

// ListType describes the element type of a List dtype
type ListType struct {
	ElementType DType
	// For nested lists/structs, this holds the inner type info
	Inner interface{} // *StructType or *ListType
}

// NewListType creates a new ListType
func NewListType(elemType DType) *ListType {
	return &ListType{ElementType: elemType}
}

// NewListTypeNested creates a ListType with nested type info
func NewListTypeNested(elemType DType, inner interface{}) *ListType {
	return &ListType{ElementType: elemType, Inner: inner}
}

// String returns a string representation of the list type
func (l *ListType) String() string {
	return fmt.Sprintf("List[%s]", l.ElementType)
}

// ArrayType describes a fixed-size array
type ArrayType struct {
	ElementType DType
	Size        int
	Inner       interface{} // For nested types
}

// NewArrayType creates a new ArrayType
func NewArrayType(elemType DType, size int) *ArrayType {
	return &ArrayType{ElementType: elemType, Size: size}
}

// String returns a string representation of the array type
func (a *ArrayType) String() string {
	return fmt.Sprintf("Array[%s; %d]", a.ElementType, a.Size)
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
