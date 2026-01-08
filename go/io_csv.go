package galleon

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// CSVReadOptions configures CSV reading behavior
type CSVReadOptions struct {
	Delimiter    rune              // Field delimiter (default ',')
	HasHeader    bool              // First row is header (default true)
	ColumnNames  []string          // Override column names
	ColumnTypes  map[string]DType  // Force column types
	InferTypes   bool              // Auto-detect types (default true)
	NullValues   []string          // Strings to treat as null
	SkipRows     int               // Skip first N rows
	MaxRows      int               // Max rows to read (0 = unlimited)
	TrimSpace    bool              // Trim whitespace from values
	Comment      rune              // Comment character (skip lines starting with this)
}

// DefaultCSVReadOptions returns default CSV reading options
func DefaultCSVReadOptions() CSVReadOptions {
	return CSVReadOptions{
		Delimiter:  ',',
		HasHeader:  true,
		InferTypes: true,
		NullValues: []string{"", "null", "NULL", "NA", "N/A", "nan", "NaN"},
		TrimSpace:  true,
	}
}

// ReadCSV reads a CSV file into a DataFrame
func ReadCSV(path string, opts ...CSVReadOptions) (*DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	return ReadCSVFromReader(f, opts...)
}

// ReadCSVFromReader reads CSV data from an io.Reader into a DataFrame
func ReadCSVFromReader(r io.Reader, opts ...CSVReadOptions) (*DataFrame, error) {
	opt := DefaultCSVReadOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	reader := csv.NewReader(r)
	reader.Comma = opt.Delimiter
	if opt.Comment != 0 {
		reader.Comment = opt.Comment
	}
	reader.TrimLeadingSpace = opt.TrimSpace

	// Skip rows
	for i := 0; i < opt.SkipRows; i++ {
		if _, err := reader.Read(); err != nil {
			return nil, fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	// Read header
	var headers []string
	if opt.HasHeader {
		var err error
		headers, err = reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
	} else if len(opt.ColumnNames) > 0 {
		headers = opt.ColumnNames
	}

	// Read all data
	var records [][]string
	rowCount := 0
	for {
		if opt.MaxRows > 0 && rowCount >= opt.MaxRows {
			break
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row %d: %w", rowCount, err)
		}

		// Generate headers if needed
		if headers == nil {
			headers = make([]string, len(record))
			for i := range record {
				headers[i] = fmt.Sprintf("column_%d", i)
			}
		}

		records = append(records, record)
		rowCount++
	}

	if len(records) == 0 {
		return NewDataFrame()
	}

	// Infer or use specified types (in parallel for large datasets)
	colTypes := make([]DType, len(headers))
	cfg := globalConfig

	if opt.InferTypes {
		if cfg.shouldParallelize(len(records)) && len(headers) > 1 {
			// Parallel type inference
			var wg sync.WaitGroup
			for i := range headers {
				wg.Add(1)
				go func(colIdx int) {
					defer wg.Done()
					colTypes[colIdx] = inferColumnType(records, colIdx, opt.NullValues)
				}(i)
			}
			wg.Wait()
		} else {
			// Sequential type inference
			for i := range headers {
				colTypes[i] = inferColumnType(records, i, opt.NullValues)
			}
		}
	}

	// Override with specified types
	for name, dtype := range opt.ColumnTypes {
		for i, h := range headers {
			if h == name {
				colTypes[i] = dtype
				break
			}
		}
	}

	// Build columns (in parallel for large datasets)
	columns := make([]*Series, len(headers))
	errors := make([]error, len(headers))

	if cfg.shouldParallelize(len(records)) && len(headers) > 1 {
		// Parallel column building
		var wg sync.WaitGroup
		for i, name := range headers {
			wg.Add(1)
			go func(colIdx int, colName string) {
				defer wg.Done()
				columns[colIdx], errors[colIdx] = buildColumn(colName, colTypes[colIdx], records, colIdx, opt.NullValues)
			}(i, name)
		}
		wg.Wait()
	} else {
		// Sequential column building
		for i, name := range headers {
			columns[i], errors[i] = buildColumn(name, colTypes[i], records, i, opt.NullValues)
		}
	}

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("failed to build column '%s': %w", headers[i], err)
		}
	}

	return NewDataFrame(columns...)
}

func inferColumnType(records [][]string, colIdx int, nullValues []string) DType {
	hasInt := false
	hasFloat := false
	hasBool := false
	hasString := false

	for _, record := range records {
		if colIdx >= len(record) {
			continue
		}
		val := strings.TrimSpace(record[colIdx])

		// Check if null
		isNull := false
		for _, nv := range nullValues {
			if val == nv {
				isNull = true
				break
			}
		}
		if isNull {
			continue
		}

		// Try bool
		lower := strings.ToLower(val)
		if lower == "true" || lower == "false" {
			hasBool = true
			continue
		}

		// Try int
		if _, err := strconv.ParseInt(val, 10, 64); err == nil {
			hasInt = true
			continue
		}

		// Try float
		if _, err := strconv.ParseFloat(val, 64); err == nil {
			hasFloat = true
			continue
		}

		// It's a string
		hasString = true
	}

	// Priority: string > float > int > bool
	if hasString {
		return String
	}
	if hasFloat {
		return Float64
	}
	if hasInt {
		return Int64
	}
	if hasBool {
		return Bool
	}

	// Default to string if we can't determine
	return String
}

func buildColumn(name string, dtype DType, records [][]string, colIdx int, nullValues []string) (*Series, error) {
	n := len(records)

	switch dtype {
	case Float64:
		data := make([]float64, n)
		for i, record := range records {
			if colIdx >= len(record) {
				data[i] = 0
				continue
			}
			val := strings.TrimSpace(record[colIdx])
			if isNull(val, nullValues) {
				data[i] = 0 // TODO: Handle nulls properly
				continue
			}
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("row %d: cannot parse '%s' as float64", i, val)
			}
			data[i] = f
		}
		return NewSeriesFloat64(name, data), nil

	case Int64:
		data := make([]int64, n)
		for i, record := range records {
			if colIdx >= len(record) {
				data[i] = 0
				continue
			}
			val := strings.TrimSpace(record[colIdx])
			if isNull(val, nullValues) {
				data[i] = 0
				continue
			}
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("row %d: cannot parse '%s' as int64", i, val)
			}
			data[i] = v
		}
		return NewSeriesInt64(name, data), nil

	case Bool:
		data := make([]bool, n)
		for i, record := range records {
			if colIdx >= len(record) {
				data[i] = false
				continue
			}
			val := strings.TrimSpace(record[colIdx])
			if isNull(val, nullValues) {
				data[i] = false
				continue
			}
			lower := strings.ToLower(val)
			data[i] = lower == "true" || lower == "1" || lower == "yes"
		}
		return NewSeriesBool(name, data), nil

	case String:
		data := make([]string, n)
		for i, record := range records {
			if colIdx >= len(record) {
				data[i] = ""
				continue
			}
			data[i] = strings.TrimSpace(record[colIdx])
		}
		return NewSeriesString(name, data), nil

	default:
		return nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}
}

func isNull(val string, nullValues []string) bool {
	for _, nv := range nullValues {
		if val == nv {
			return true
		}
	}
	return false
}

// CSVWriteOptions configures CSV writing behavior
type CSVWriteOptions struct {
	Delimiter   rune // Field delimiter (default ',')
	WriteHeader bool // Write header row (default true)
	NullString  string // String to write for null values (default "")
}

// DefaultCSVWriteOptions returns default CSV writing options
func DefaultCSVWriteOptions() CSVWriteOptions {
	return CSVWriteOptions{
		Delimiter:   ',',
		WriteHeader: true,
		NullString:  "",
	}
}

// WriteCSV writes a DataFrame to a CSV file
func (df *DataFrame) WriteCSV(path string, opts ...CSVWriteOptions) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	return df.WriteCSVToWriter(w, opts...)
}

// WriteCSVToWriter writes a DataFrame to an io.Writer
func (df *DataFrame) WriteCSVToWriter(w io.Writer, opts ...CSVWriteOptions) error {
	opt := DefaultCSVWriteOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	writer := csv.NewWriter(w)
	writer.Comma = opt.Delimiter

	// Write header
	if opt.WriteHeader {
		if err := writer.Write(df.Columns()); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	cfg := globalConfig
	height := df.Height()
	width := df.Width()

	// For large datasets, format rows in parallel then write sequentially
	if cfg.shouldParallelize(height) {
		// Pre-format all rows in parallel
		allRows := make([][]string, height)

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
					row := make([]string, width)
					for j, col := range df.columns {
						val := col.Get(i)
						if val == nil {
							row[j] = opt.NullString
						} else {
							row[j] = formatValue(val)
						}
					}
					allRows[i] = row
				}
			}(start, end)
		}
		wg.Wait()

		// Write all rows sequentially (I/O must be sequential)
		for i := 0; i < height; i++ {
			if err := writer.Write(allRows[i]); err != nil {
				return fmt.Errorf("failed to write row %d: %w", i, err)
			}
		}
	} else {
		// Sequential path for small datasets
		row := make([]string, width)
		for i := 0; i < height; i++ {
			for j, col := range df.columns {
				val := col.Get(i)
				if val == nil {
					row[j] = opt.NullString
				} else {
					row[j] = formatValue(val)
				}
			}
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("failed to write row %d: %w", i, err)
			}
		}
	}

	writer.Flush()
	return writer.Error()
}

func formatValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case bool:
		return strconv.FormatBool(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
