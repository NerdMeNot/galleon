package galleon

import (
	"fmt"
)

// ============================================================================
// StructSeries - A series containing struct values (row of named fields)
// ============================================================================

// StructSeries represents a column of struct values where each row has
// the same set of named fields. Internally, it stores each field as a
// separate Series (columnar storage).
type StructSeries struct {
	name       string
	structType *StructType
	fields     map[string]*Series
	length     int
}

// NewStructSeries creates a new StructSeries from a map of field Series.
// All field Series must have the same length.
func NewStructSeries(name string, fields map[string]*Series) (*StructSeries, error) {
	if len(fields) == 0 {
		return &StructSeries{
			name:       name,
			structType: &StructType{},
			fields:     make(map[string]*Series),
			length:     0,
		}, nil
	}

	// Verify all fields have the same length
	var length int = -1
	structFields := make([]StructField, 0, len(fields))

	for fieldName, series := range fields {
		if length == -1 {
			length = series.Len()
		} else if series.Len() != length {
			return nil, fmt.Errorf("field %s has length %d, expected %d",
				fieldName, series.Len(), length)
		}
		structFields = append(structFields, StructField{
			Name:  fieldName,
			DType: series.DType(),
		})
	}

	return &StructSeries{
		name:       name,
		structType: NewStructType(structFields),
		fields:     fields,
		length:     length,
	}, nil
}

// NewStructSeriesFromSeries creates a StructSeries from ordered field names and series
func NewStructSeriesFromSeries(name string, fieldNames []string, series []*Series) (*StructSeries, error) {
	if len(fieldNames) != len(series) {
		return nil, fmt.Errorf("field names count %d doesn't match series count %d",
			len(fieldNames), len(series))
	}

	fields := make(map[string]*Series, len(fieldNames))
	for i, fname := range fieldNames {
		fields[fname] = series[i]
	}

	return NewStructSeries(name, fields)
}

// Name returns the series name
func (s *StructSeries) Name() string {
	return s.name
}

// DType returns Struct
func (s *StructSeries) DType() DType {
	return Struct
}

// Len returns the number of rows
func (s *StructSeries) Len() int {
	return s.length
}

// StructType returns the struct type metadata
func (s *StructSeries) StructType() *StructType {
	return s.structType
}

// Field returns a specific field by name
func (s *StructSeries) Field(name string) *Series {
	return s.fields[name]
}

// Fields returns all field Series
func (s *StructSeries) Fields() map[string]*Series {
	return s.fields
}

// FieldNames returns the names of all fields
func (s *StructSeries) FieldNames() []string {
	names := make([]string, 0, len(s.structType.Fields))
	for _, f := range s.structType.Fields {
		names = append(names, f.Name)
	}
	return names
}

// GetRow returns all field values at a given row index as a map
func (s *StructSeries) GetRow(index int) map[string]interface{} {
	if index < 0 || index >= s.length {
		return nil
	}

	result := make(map[string]interface{}, len(s.fields))
	for name, series := range s.fields {
		result[name] = series.Get(index)
	}
	return result
}

// Unnest expands the struct into separate columns (returns a map of Series)
func (s *StructSeries) Unnest() map[string]*Series {
	result := make(map[string]*Series, len(s.fields))
	for name, series := range s.fields {
		// Optionally prefix with struct name
		result[name] = series
	}
	return result
}

// UnnestPrefixed expands the struct with column name prefixes
func (s *StructSeries) UnnestPrefixed() map[string]*Series {
	result := make(map[string]*Series, len(s.fields))
	for name, series := range s.fields {
		prefixedName := s.name + "." + name
		result[prefixedName] = series.Rename(prefixedName)
	}
	return result
}

// String returns a string representation
func (s *StructSeries) String() string {
	return fmt.Sprintf("StructSeries('%s', %s, len=%d)", s.name, s.structType, s.length)
}

// ============================================================================
// ListSeries - A series containing list values (variable-length arrays)
// ============================================================================

// ListSeries represents a column of list values where each row contains
// a variable-length list of elements of the same type.
// Uses offset-based storage: offsets[i] is the start index in values for row i.
type ListSeries struct {
	name     string
	listType *ListType
	offsets  []int32  // Length = rows + 1, offsets[i] to offsets[i+1] is range for row i
	values   *Series  // Flattened values
	length   int      // Number of rows
}

// NewListSeries creates a new ListSeries from offsets and flattened values.
// offsets should have length = numRows + 1, where offsets[i] to offsets[i+1]
// defines the range in values for row i.
func NewListSeries(name string, offsets []int32, values *Series) (*ListSeries, error) {
	if len(offsets) < 1 {
		return nil, fmt.Errorf("offsets must have at least 1 element")
	}

	numRows := len(offsets) - 1

	// Validate offsets
	for i := 0; i < numRows; i++ {
		if offsets[i] > offsets[i+1] {
			return nil, fmt.Errorf("invalid offsets at row %d: %d > %d",
				i, offsets[i], offsets[i+1])
		}
	}

	// Validate last offset matches values length
	if int(offsets[numRows]) != values.Len() {
		return nil, fmt.Errorf("last offset %d doesn't match values length %d",
			offsets[numRows], values.Len())
	}

	return &ListSeries{
		name:     name,
		listType: NewListType(values.DType()),
		offsets:  offsets,
		values:   values,
		length:   numRows,
	}, nil
}

// NewListSeriesFromSlices creates a ListSeries from a slice of slices.
// This is a convenience constructor for common use cases.
func NewListSeriesFromSlicesF64(name string, data [][]float64) *ListSeries {
	if len(data) == 0 {
		return &ListSeries{
			name:     name,
			listType: NewListType(Float64),
			offsets:  []int32{0},
			values:   NewSeriesFloat64("values", nil),
			length:   0,
		}
	}

	// Calculate total length and build offsets
	offsets := make([]int32, len(data)+1)
	totalLen := 0
	for i, row := range data {
		offsets[i] = int32(totalLen)
		totalLen += len(row)
	}
	offsets[len(data)] = int32(totalLen)

	// Flatten values
	flatValues := make([]float64, totalLen)
	idx := 0
	for _, row := range data {
		for _, v := range row {
			flatValues[idx] = v
			idx++
		}
	}

	return &ListSeries{
		name:     name,
		listType: NewListType(Float64),
		offsets:  offsets,
		values:   NewSeriesFloat64("values", flatValues),
		length:   len(data),
	}
}

// NewListSeriesFromSlicesI64 creates a ListSeries from int64 slices
func NewListSeriesFromSlicesI64(name string, data [][]int64) *ListSeries {
	if len(data) == 0 {
		return &ListSeries{
			name:     name,
			listType: NewListType(Int64),
			offsets:  []int32{0},
			values:   NewSeriesInt64("values", nil),
			length:   0,
		}
	}

	offsets := make([]int32, len(data)+1)
	totalLen := 0
	for i, row := range data {
		offsets[i] = int32(totalLen)
		totalLen += len(row)
	}
	offsets[len(data)] = int32(totalLen)

	flatValues := make([]int64, totalLen)
	idx := 0
	for _, row := range data {
		for _, v := range row {
			flatValues[idx] = v
			idx++
		}
	}

	return &ListSeries{
		name:     name,
		listType: NewListType(Int64),
		offsets:  offsets,
		values:   NewSeriesInt64("values", flatValues),
		length:   len(data),
	}
}

// NewListSeriesFromSlicesString creates a ListSeries from string slices
func NewListSeriesFromSlicesString(name string, data [][]string) *ListSeries {
	if len(data) == 0 {
		return &ListSeries{
			name:     name,
			listType: NewListType(String),
			offsets:  []int32{0},
			values:   NewSeriesString("values", nil),
			length:   0,
		}
	}

	offsets := make([]int32, len(data)+1)
	totalLen := 0
	for i, row := range data {
		offsets[i] = int32(totalLen)
		totalLen += len(row)
	}
	offsets[len(data)] = int32(totalLen)

	flatValues := make([]string, totalLen)
	idx := 0
	for _, row := range data {
		for _, v := range row {
			flatValues[idx] = v
			idx++
		}
	}

	return &ListSeries{
		name:     name,
		listType: NewListType(String),
		offsets:  offsets,
		values:   NewSeriesString("values", flatValues),
		length:   len(data),
	}
}

// Name returns the series name
func (l *ListSeries) Name() string {
	return l.name
}

// DType returns List
func (l *ListSeries) DType() DType {
	return List
}

// Len returns the number of rows
func (l *ListSeries) Len() int {
	return l.length
}

// ListType returns the list type metadata
func (l *ListSeries) ListType() *ListType {
	return l.listType
}

// ElementType returns the type of elements in the list
func (l *ListSeries) ElementType() DType {
	return l.listType.ElementType
}

// Values returns the underlying flattened values Series
func (l *ListSeries) Values() *Series {
	return l.values
}

// Offsets returns the offset array
func (l *ListSeries) Offsets() []int32 {
	return l.offsets
}

// GetListLen returns the length of the list at row index
func (l *ListSeries) GetListLen(index int) int {
	if index < 0 || index >= l.length {
		return 0
	}
	return int(l.offsets[index+1] - l.offsets[index])
}

// GetList returns the list values at row index as a slice
func (l *ListSeries) GetList(index int) interface{} {
	if index < 0 || index >= l.length {
		return nil
	}

	start := int(l.offsets[index])
	end := int(l.offsets[index+1])

	switch l.listType.ElementType {
	case Float64:
		data := l.values.Float64()
		if data == nil {
			return nil
		}
		result := make([]float64, end-start)
		copy(result, data[start:end])
		return result
	case Int64:
		data := l.values.Int64()
		if data == nil {
			return nil
		}
		result := make([]int64, end-start)
		copy(result, data[start:end])
		return result
	case Int32:
		data := l.values.Int32()
		if data == nil {
			return nil
		}
		result := make([]int32, end-start)
		copy(result, data[start:end])
		return result
	case String:
		data := l.values.Strings()
		if data == nil {
			return nil
		}
		result := make([]string, end-start)
		copy(result, data[start:end])
		return result
	}
	return nil
}

// GetListF64 returns the list at index as []float64
func (l *ListSeries) GetListF64(index int) []float64 {
	if index < 0 || index >= l.length || l.listType.ElementType != Float64 {
		return nil
	}
	start := int(l.offsets[index])
	end := int(l.offsets[index+1])
	data := l.values.Float64()
	if data == nil {
		return nil
	}
	result := make([]float64, end-start)
	copy(result, data[start:end])
	return result
}

// GetListI64 returns the list at index as []int64
func (l *ListSeries) GetListI64(index int) []int64 {
	if index < 0 || index >= l.length || l.listType.ElementType != Int64 {
		return nil
	}
	start := int(l.offsets[index])
	end := int(l.offsets[index+1])
	data := l.values.Int64()
	if data == nil {
		return nil
	}
	result := make([]int64, end-start)
	copy(result, data[start:end])
	return result
}

// GetElement returns a specific element from a list
// index is the row, elemIndex is the index within that row's list
func (l *ListSeries) GetElement(index, elemIndex int) interface{} {
	if index < 0 || index >= l.length {
		return nil
	}
	start := int(l.offsets[index])
	end := int(l.offsets[index+1])
	listLen := end - start

	if elemIndex < 0 || elemIndex >= listLen {
		return nil
	}

	return l.values.Get(start + elemIndex)
}

// Explode expands the list series into a flat Series with one row per element.
// Also returns indices mapping each output row to its source row.
func (l *ListSeries) Explode() (*Series, []int32) {
	if l.length == 0 {
		return l.values, nil
	}

	// Build row indices
	totalLen := int(l.offsets[l.length])
	rowIndices := make([]int32, totalLen)

	idx := 0
	for row := 0; row < l.length; row++ {
		listLen := int(l.offsets[row+1] - l.offsets[row])
		for j := 0; j < listLen; j++ {
			rowIndices[idx] = int32(row)
			idx++
		}
	}

	// Values are already flat
	return l.values.Rename(l.name), rowIndices
}

// ListLengths returns a Series containing the length of each list
func (l *ListSeries) ListLengths() *Series {
	lengths := make([]int32, l.length)
	for i := 0; i < l.length; i++ {
		lengths[i] = l.offsets[i+1] - l.offsets[i]
	}
	return NewSeriesInt32(l.name+"_len", lengths)
}

// ListSum returns the sum of elements in each list (for numeric types)
func (l *ListSeries) ListSum() *Series {
	if !l.listType.ElementType.IsNumeric() {
		return nil
	}

	sums := make([]float64, l.length)

	switch l.listType.ElementType {
	case Float64:
		data := l.values.Float64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			var sum float64
			for j := start; j < end; j++ {
				sum += data[j]
			}
			sums[i] = sum
		}
	case Int64:
		data := l.values.Int64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			var sum int64
			for j := start; j < end; j++ {
				sum += data[j]
			}
			sums[i] = float64(sum)
		}
	case Int32:
		data := l.values.Int32()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			var sum int32
			for j := start; j < end; j++ {
				sum += data[j]
			}
			sums[i] = float64(sum)
		}
	}

	return NewSeriesFloat64(l.name+"_sum", sums)
}

// ListMean returns the mean of elements in each list
func (l *ListSeries) ListMean() *Series {
	if !l.listType.ElementType.IsNumeric() {
		return nil
	}

	means := make([]float64, l.length)

	switch l.listType.ElementType {
	case Float64:
		data := l.values.Float64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			if start == end {
				means[i] = 0
				continue
			}
			var sum float64
			for j := start; j < end; j++ {
				sum += data[j]
			}
			means[i] = sum / float64(end-start)
		}
	case Int64:
		data := l.values.Int64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			if start == end {
				means[i] = 0
				continue
			}
			var sum int64
			for j := start; j < end; j++ {
				sum += data[j]
			}
			means[i] = float64(sum) / float64(end-start)
		}
	}

	return NewSeriesFloat64(l.name+"_mean", means)
}

// ListMin returns the minimum element in each list
func (l *ListSeries) ListMin() *Series {
	if !l.listType.ElementType.IsNumeric() {
		return nil
	}

	mins := make([]float64, l.length)

	switch l.listType.ElementType {
	case Float64:
		data := l.values.Float64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			if start == end {
				mins[i] = 0
				continue
			}
			minVal := data[start]
			for j := start + 1; j < end; j++ {
				if data[j] < minVal {
					minVal = data[j]
				}
			}
			mins[i] = minVal
		}
	case Int64:
		data := l.values.Int64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			if start == end {
				mins[i] = 0
				continue
			}
			minVal := data[start]
			for j := start + 1; j < end; j++ {
				if data[j] < minVal {
					minVal = data[j]
				}
			}
			mins[i] = float64(minVal)
		}
	}

	return NewSeriesFloat64(l.name+"_min", mins)
}

// ListMax returns the maximum element in each list
func (l *ListSeries) ListMax() *Series {
	if !l.listType.ElementType.IsNumeric() {
		return nil
	}

	maxs := make([]float64, l.length)

	switch l.listType.ElementType {
	case Float64:
		data := l.values.Float64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			if start == end {
				maxs[i] = 0
				continue
			}
			maxVal := data[start]
			for j := start + 1; j < end; j++ {
				if data[j] > maxVal {
					maxVal = data[j]
				}
			}
			maxs[i] = maxVal
		}
	case Int64:
		data := l.values.Int64()
		for i := 0; i < l.length; i++ {
			start := int(l.offsets[i])
			end := int(l.offsets[i+1])
			if start == end {
				maxs[i] = 0
				continue
			}
			maxVal := data[start]
			for j := start + 1; j < end; j++ {
				if data[j] > maxVal {
					maxVal = data[j]
				}
			}
			maxs[i] = float64(maxVal)
		}
	}

	return NewSeriesFloat64(l.name+"_max", maxs)
}

// String returns a string representation
func (l *ListSeries) String() string {
	return fmt.Sprintf("ListSeries('%s', %s, len=%d)", l.name, l.listType, l.length)
}
