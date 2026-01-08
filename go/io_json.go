package galleon

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// JSONFormat specifies the JSON output format
type JSONFormat int

const (
	// JSONRecords outputs as array of row objects: [{"a":1,"b":2}, {"a":3,"b":4}]
	JSONRecords JSONFormat = iota
	// JSONColumns outputs as object of column arrays: {"a":[1,3],"b":[2,4]}
	JSONColumns
)

// JSONReadOptions configures JSON reading behavior
type JSONReadOptions struct {
	Format      JSONFormat        // Expected format
	ColumnTypes map[string]DType  // Force column types
}

// DefaultJSONReadOptions returns default JSON reading options
func DefaultJSONReadOptions() JSONReadOptions {
	return JSONReadOptions{
		Format: JSONRecords,
	}
}

// ReadJSON reads a JSON file into a DataFrame
func ReadJSON(path string, opts ...JSONReadOptions) (*DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	return ReadJSONFromReader(f, opts...)
}

// ReadJSONFromReader reads JSON data from an io.Reader into a DataFrame
func ReadJSONFromReader(r io.Reader, opts ...JSONReadOptions) (*DataFrame, error) {
	opt := DefaultJSONReadOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	switch opt.Format {
	case JSONRecords:
		return readJSONRecords(data, opt)
	case JSONColumns:
		return readJSONColumns(data, opt)
	default:
		return nil, fmt.Errorf("unknown JSON format: %d", opt.Format)
	}
}

func readJSONRecords(data []byte, opt JSONReadOptions) (*DataFrame, error) {
	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(records) == 0 {
		return NewDataFrame()
	}

	// Collect all column names
	colNames := make([]string, 0)
	colSet := make(map[string]bool)
	for _, record := range records {
		for key := range record {
			if !colSet[key] {
				colNames = append(colNames, key)
				colSet[key] = true
			}
		}
	}

	// Infer types from first non-null value
	colTypes := make(map[string]DType)
	for _, name := range colNames {
		if dtype, ok := opt.ColumnTypes[name]; ok {
			colTypes[name] = dtype
		} else {
			colTypes[name] = inferJSONType(records, name)
		}
	}

	// Build columns (parallel for large datasets)
	columns := make([]*Series, len(colNames))
	errors := make([]error, len(colNames))
	cfg := globalConfig

	if cfg.shouldParallelize(len(records)) && len(colNames) > 1 {
		var wg sync.WaitGroup
		for i, name := range colNames {
			wg.Add(1)
			go func(idx int, colName string) {
				defer wg.Done()
				columns[idx], errors[idx] = buildJSONColumn(colName, colTypes[colName], records)
			}(i, name)
		}
		wg.Wait()
	} else {
		for i, name := range colNames {
			columns[i], errors[i] = buildJSONColumn(name, colTypes[name], records)
		}
	}

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("failed to build column '%s': %w", colNames[i], err)
		}
	}

	return NewDataFrame(columns...)
}

func inferJSONType(records []map[string]interface{}, name string) DType {
	for _, record := range records {
		val, ok := record[name]
		if !ok || val == nil {
			continue
		}

		switch v := val.(type) {
		case bool:
			return Bool
		case float64:
			// Check if it's actually an integer
			if v == float64(int64(v)) {
				return Int64
			}
			return Float64
		case string:
			return String
		default:
			return String
		}
	}
	return String
}

func buildJSONColumn(name string, dtype DType, records []map[string]interface{}) (*Series, error) {
	n := len(records)

	switch dtype {
	case Float64:
		data := make([]float64, n)
		for i, record := range records {
			val, ok := record[name]
			if !ok || val == nil {
				data[i] = 0
				continue
			}
			switch v := val.(type) {
			case float64:
				data[i] = v
			case int64:
				data[i] = float64(v)
			default:
				data[i] = 0
			}
		}
		return NewSeriesFloat64(name, data), nil

	case Int64:
		data := make([]int64, n)
		for i, record := range records {
			val, ok := record[name]
			if !ok || val == nil {
				data[i] = 0
				continue
			}
			switch v := val.(type) {
			case float64:
				data[i] = int64(v)
			case int64:
				data[i] = v
			default:
				data[i] = 0
			}
		}
		return NewSeriesInt64(name, data), nil

	case Bool:
		data := make([]bool, n)
		for i, record := range records {
			val, ok := record[name]
			if !ok || val == nil {
				data[i] = false
				continue
			}
			if v, ok := val.(bool); ok {
				data[i] = v
			}
		}
		return NewSeriesBool(name, data), nil

	case String:
		data := make([]string, n)
		for i, record := range records {
			val, ok := record[name]
			if !ok || val == nil {
				data[i] = ""
				continue
			}
			data[i] = fmt.Sprintf("%v", val)
		}
		return NewSeriesString(name, data), nil

	default:
		return nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}
}

func readJSONColumns(data []byte, opt JSONReadOptions) (*DataFrame, error) {
	var colData map[string][]interface{}
	if err := json.Unmarshal(data, &colData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(colData) == 0 {
		return NewDataFrame()
	}

	// Get column names and determine length
	var height int
	colNames := make([]string, 0, len(colData))
	for name, values := range colData {
		colNames = append(colNames, name)
		if len(values) > height {
			height = len(values)
		}
	}

	// Build columns
	columns := make([]*Series, len(colNames))
	for i, name := range colNames {
		values := colData[name]
		dtype := String
		if forcedType, ok := opt.ColumnTypes[name]; ok {
			dtype = forcedType
		} else if len(values) > 0 {
			dtype = inferJSONArrayType(values)
		}

		col, err := buildJSONArrayColumn(name, dtype, values, height)
		if err != nil {
			return nil, fmt.Errorf("failed to build column '%s': %w", name, err)
		}
		columns[i] = col
	}

	return NewDataFrame(columns...)
}

func inferJSONArrayType(values []interface{}) DType {
	for _, val := range values {
		if val == nil {
			continue
		}
		switch v := val.(type) {
		case bool:
			return Bool
		case float64:
			if v == float64(int64(v)) {
				return Int64
			}
			return Float64
		case string:
			return String
		}
	}
	return String
}

func buildJSONArrayColumn(name string, dtype DType, values []interface{}, height int) (*Series, error) {
	switch dtype {
	case Float64:
		data := make([]float64, height)
		for i := 0; i < height; i++ {
			if i < len(values) && values[i] != nil {
				if v, ok := values[i].(float64); ok {
					data[i] = v
				}
			}
		}
		return NewSeriesFloat64(name, data), nil

	case Int64:
		data := make([]int64, height)
		for i := 0; i < height; i++ {
			if i < len(values) && values[i] != nil {
				if v, ok := values[i].(float64); ok {
					data[i] = int64(v)
				}
			}
		}
		return NewSeriesInt64(name, data), nil

	case Bool:
		data := make([]bool, height)
		for i := 0; i < height; i++ {
			if i < len(values) && values[i] != nil {
				if v, ok := values[i].(bool); ok {
					data[i] = v
				}
			}
		}
		return NewSeriesBool(name, data), nil

	case String:
		data := make([]string, height)
		for i := 0; i < height; i++ {
			if i < len(values) && values[i] != nil {
				data[i] = fmt.Sprintf("%v", values[i])
			}
		}
		return NewSeriesString(name, data), nil

	default:
		return nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}
}

// JSONWriteOptions configures JSON writing behavior
type JSONWriteOptions struct {
	Format JSONFormat // Output format
	Indent string     // Indent string (default "", no indent)
}

// DefaultJSONWriteOptions returns default JSON writing options
func DefaultJSONWriteOptions() JSONWriteOptions {
	return JSONWriteOptions{
		Format: JSONRecords,
		Indent: "",
	}
}

// WriteJSON writes a DataFrame to a JSON file
func (df *DataFrame) WriteJSON(path string, opts ...JSONWriteOptions) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	return df.WriteJSONToWriter(f, opts...)
}

// WriteJSONToWriter writes a DataFrame to an io.Writer
func (df *DataFrame) WriteJSONToWriter(w io.Writer, opts ...JSONWriteOptions) error {
	opt := DefaultJSONWriteOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	var data interface{}

	cfg := globalConfig
	height := df.Height()

	switch opt.Format {
	case JSONRecords:
		records := make([]map[string]interface{}, height)

		// Parallel record building for large datasets
		if cfg.shouldParallelize(height) {
			var wg sync.WaitGroup
			numWorkers := cfg.numWorkers()
			chunkSize := (height + numWorkers - 1) / numWorkers

			for workerID := 0; workerID < numWorkers; workerID++ {
				start := workerID * chunkSize
				end := start + chunkSize
				if end > height {
					end = height
				}
				if start >= height {
					break
				}

				wg.Add(1)
				go func(startRow, endRow int) {
					defer wg.Done()
					for i := startRow; i < endRow; i++ {
						record := make(map[string]interface{})
						for _, col := range df.columns {
							record[col.Name()] = col.Get(i)
						}
						records[i] = record
					}
				}(start, end)
			}
			wg.Wait()
		} else {
			for i := 0; i < height; i++ {
				record := make(map[string]interface{})
				for _, col := range df.columns {
					record[col.Name()] = col.Get(i)
				}
				records[i] = record
			}
		}
		data = records

	case JSONColumns:
		colData := make(map[string]interface{})
		for _, col := range df.columns {
			switch col.DType() {
			case Float64:
				colData[col.Name()] = col.Float64()
			case Float32:
				colData[col.Name()] = col.Float32()
			case Int64:
				colData[col.Name()] = col.Int64()
			case Int32:
				colData[col.Name()] = col.Int32()
			case Bool:
				colData[col.Name()] = col.Bool()
			case String:
				colData[col.Name()] = col.Strings()
			default:
				// Build slice from Get
				vals := make([]interface{}, col.Len())
				for i := 0; i < col.Len(); i++ {
					vals[i] = col.Get(i)
				}
				colData[col.Name()] = vals
			}
		}
		data = colData

	default:
		return fmt.Errorf("unknown JSON format: %d", opt.Format)
	}

	encoder := json.NewEncoder(w)
	if opt.Indent != "" {
		encoder.SetIndent("", opt.Indent)
	}

	return encoder.Encode(data)
}
