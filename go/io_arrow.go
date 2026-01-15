package galleon

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// Arrow Export
// ============================================================================

// ToArrow exports a DataFrame to an Arrow Record.
// The caller is responsible for calling Release() on the returned Record.
func (df *DataFrame) ToArrow(mem memory.Allocator) (arrow.Record, error) {
	if mem == nil {
		mem = memory.DefaultAllocator
	}

	// Build Arrow schema
	fields := make([]arrow.Field, df.Width())
	for i, col := range df.columns {
		arrowType, err := dtypeToArrowType(col.DType())
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name(), err)
		}
		fields[i] = arrow.Field{Name: col.Name(), Type: arrowType, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)

	// Convert each column to Arrow array
	arrays := make([]arrow.Array, df.Width())
	for i, col := range df.columns {
		arr, err := seriesToArrowArray(col, mem)
		if err != nil {
			// Clean up already created arrays
			for j := 0; j < i; j++ {
				arrays[j].Release()
			}
			return nil, fmt.Errorf("column %s: %w", col.Name(), err)
		}
		arrays[i] = arr
	}

	// Create Record
	record := array.NewRecord(schema, arrays, int64(df.Height()))

	// Release arrays (Record retains them)
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// ToArrowTable exports a DataFrame to an Arrow Table.
// The caller is responsible for calling Release() on the returned Table.
func (df *DataFrame) ToArrowTable(mem memory.Allocator) (arrow.Table, error) {
	record, err := df.ToArrow(mem)
	if err != nil {
		return nil, err
	}
	defer record.Release()

	return array.NewTableFromRecords(record.Schema(), []arrow.Record{record}), nil
}

// dtypeToArrowType converts Galleon DType to Arrow DataType
func dtypeToArrowType(dtype DType) (arrow.DataType, error) {
	switch dtype {
	case Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case Float32:
		return arrow.PrimitiveTypes.Float32, nil
	case Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case UInt64:
		return arrow.PrimitiveTypes.Uint64, nil
	case UInt32:
		return arrow.PrimitiveTypes.Uint32, nil
	case Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case String:
		return arrow.BinaryTypes.String, nil
	case Categorical:
		// Dictionary encoded strings
		return &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}
}

// seriesToArrowArray converts a Series to an Arrow Array
func seriesToArrowArray(s *Series, mem memory.Allocator) (arrow.Array, error) {
	switch s.DType() {
	case Float64:
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()
		builder.AppendValues(s.Float64(), nil)
		return builder.NewArray(), nil

	case Float32:
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()
		builder.AppendValues(s.Float32(), nil)
		return builder.NewArray(), nil

	case Int64:
		builder := array.NewInt64Builder(mem)
		defer builder.Release()
		builder.AppendValues(s.Int64(), nil)
		return builder.NewArray(), nil

	case Int32:
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		builder.AppendValues(s.Int32(), nil)
		return builder.NewArray(), nil

	case Bool:
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()
		data := s.Bool()
		for _, v := range data {
			builder.Append(v)
		}
		return builder.NewArray(), nil

	case String:
		builder := array.NewStringBuilder(mem)
		defer builder.Release()
		builder.AppendValues(s.Strings(), nil)
		return builder.NewArray(), nil

	case Categorical:
		// Build dictionary-encoded array
		dictType := &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Int32,
			ValueType: arrow.BinaryTypes.String,
		}
		builder := array.NewDictionaryBuilder(mem, dictType)
		defer builder.Release()

		categories := s.Categories()
		indices := s.CategoricalIndices()

		// Build dictionary from categories
		dictBuilder := builder.(*array.BinaryDictionaryBuilder)
		for _, idx := range indices {
			if idx >= 0 && int(idx) < len(categories) {
				if err := dictBuilder.AppendString(categories[idx]); err != nil {
					return nil, err
				}
			} else {
				dictBuilder.AppendNull()
			}
		}
		return builder.NewArray(), nil

	default:
		return nil, fmt.Errorf("unsupported dtype for Arrow export: %s", s.DType())
	}
}

// ============================================================================
// Arrow Import
// ============================================================================

// NewDataFrameFromArrow creates a DataFrame from an Arrow Record.
func NewDataFrameFromArrow(record arrow.Record) (*DataFrame, error) {
	if record == nil {
		return nil, fmt.Errorf("record is nil")
	}

	schema := record.Schema()
	numCols := int(record.NumCols())
	series := make([]*Series, numCols)

	for i := 0; i < numCols; i++ {
		field := schema.Field(i)
		col := record.Column(i)

		s, err := arrowArrayToSeries(field.Name, col)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", field.Name, err)
		}
		series[i] = s
	}

	return NewDataFrame(series...)
}

// NewDataFrameFromArrowTable creates a DataFrame from an Arrow Table.
func NewDataFrameFromArrowTable(table arrow.Table) (*DataFrame, error) {
	if table == nil {
		return nil, fmt.Errorf("table is nil")
	}

	schema := table.Schema()
	numCols := int(table.NumCols())
	series := make([]*Series, numCols)

	for i := 0; i < numCols; i++ {
		field := schema.Field(i)
		chunkedCol := table.Column(i)
		data := chunkedCol.Data()

		// Combine chunks into single array
		var allData interface{}
		for j := 0; j < data.Len(); j++ {
			chunk := data.Chunk(j)
			chunkData, err := extractArrowArrayData(chunk)
			if err != nil {
				return nil, fmt.Errorf("column %s chunk %d: %w", field.Name, j, err)
			}
			if allData == nil {
				allData = chunkData
			} else {
				allData = appendData(allData, chunkData)
			}
		}

		s, err := createSeriesFromData(field.Name, allData, field.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", field.Name, err)
		}
		series[i] = s
	}

	return NewDataFrame(series...)
}

// arrowArrayToSeries converts an Arrow Array to a Series
func arrowArrayToSeries(name string, arr arrow.Array) (*Series, error) {
	switch a := arr.(type) {
	case *array.Float64:
		data := make([]float64, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return NewSeriesFloat64(name, data), nil

	case *array.Float32:
		data := make([]float32, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return NewSeriesFloat32(name, data), nil

	case *array.Int64:
		data := make([]int64, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return NewSeriesInt64(name, data), nil

	case *array.Int32:
		data := make([]int32, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return NewSeriesInt32(name, data), nil

	case *array.Boolean:
		data := make([]bool, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return NewSeriesBool(name, data), nil

	case *array.String:
		data := make([]string, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return NewSeriesString(name, data), nil

	case *array.Dictionary:
		// Dictionary encoded -> Categorical
		indices := a.Indices()
		dict := a.Dictionary()

		// Extract categories from dictionary
		var categories []string
		switch d := dict.(type) {
		case *array.String:
			categories = make([]string, d.Len())
			for i := 0; i < d.Len(); i++ {
				categories[i] = d.Value(i)
			}
		default:
			return nil, fmt.Errorf("unsupported dictionary value type: %T", dict)
		}

		// Extract indices and convert to strings
		data := make([]string, a.Len())
		switch idx := indices.(type) {
		case *array.Int32:
			for i := 0; i < idx.Len(); i++ {
				idxVal := idx.Value(i)
				if idxVal >= 0 && int(idxVal) < len(categories) {
					data[i] = categories[idxVal]
				}
			}
		case *array.Int64:
			for i := 0; i < idx.Len(); i++ {
				idxVal := idx.Value(i)
				if idxVal >= 0 && int(idxVal) < len(categories) {
					data[i] = categories[idxVal]
				}
			}
		default:
			return nil, fmt.Errorf("unsupported dictionary index type: %T", indices)
		}

		return NewSeriesCategoricalWithCategories(name, data, categories)

	default:
		return nil, fmt.Errorf("unsupported Arrow array type: %T", arr)
	}
}

// extractArrowArrayData extracts raw data from an Arrow Array
func extractArrowArrayData(arr arrow.Array) (interface{}, error) {
	switch a := arr.(type) {
	case *array.Float64:
		data := make([]float64, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return data, nil
	case *array.Float32:
		data := make([]float32, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return data, nil
	case *array.Int64:
		data := make([]int64, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return data, nil
	case *array.Int32:
		data := make([]int32, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return data, nil
	case *array.Boolean:
		data := make([]bool, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return data, nil
	case *array.String:
		data := make([]string, a.Len())
		for i := 0; i < a.Len(); i++ {
			data[i] = a.Value(i)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported Arrow array type: %T", arr)
	}
}

// appendData appends data slices of the same type
func appendData(existing, new interface{}) interface{} {
	switch e := existing.(type) {
	case []float64:
		return append(e, new.([]float64)...)
	case []float32:
		return append(e, new.([]float32)...)
	case []int64:
		return append(e, new.([]int64)...)
	case []int32:
		return append(e, new.([]int32)...)
	case []bool:
		return append(e, new.([]bool)...)
	case []string:
		return append(e, new.([]string)...)
	default:
		return existing
	}
}

// createSeriesFromData creates a Series from extracted data
func createSeriesFromData(name string, data interface{}, arrowType arrow.DataType) (*Series, error) {
	switch d := data.(type) {
	case []float64:
		return NewSeriesFloat64(name, d), nil
	case []float32:
		return NewSeriesFloat32(name, d), nil
	case []int64:
		return NewSeriesInt64(name, d), nil
	case []int32:
		return NewSeriesInt32(name, d), nil
	case []bool:
		return NewSeriesBool(name, d), nil
	case []string:
		return NewSeriesString(name, d), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %T", data)
	}
}
