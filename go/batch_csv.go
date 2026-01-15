package galleon

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// CSVBatchReader reads CSV data in batches.
type CSVBatchReader struct {
	reader    *csv.Reader
	closer    io.Closer
	headers   []string
	dtypes    []DType
	batchSize int
	schema    *Schema
	done      bool
}

// NewCSVBatchReader creates a new CSV batch reader.
// The reader will automatically detect column types from the first batch.
func NewCSVBatchReader(r io.Reader, opts ...CSVBatchReaderOptions) (*CSVBatchReader, error) {
	var opt CSVBatchReaderOptions
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		opt = DefaultCSVBatchReaderOptions()
	}

	csvReader := csv.NewReader(r)
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	// Read header row
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Clean headers
	for i := range headers {
		headers[i] = strings.TrimSpace(headers[i])
	}

	// If types are specified, use them; otherwise, will infer later
	var dtypes []DType
	if len(opt.DTypes) > 0 {
		dtypes = opt.DTypes
	}

	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}

	return &CSVBatchReader{
		reader:    csvReader,
		closer:    closer,
		headers:   headers,
		dtypes:    dtypes,
		batchSize: opt.BatchSize,
		done:      false,
	}, nil
}

// CSVBatchReaderOptions configures CSV batch reading.
type CSVBatchReaderOptions struct {
	// BatchSize is the number of rows per batch
	BatchSize int

	// DTypes specifies column types. If nil, types are inferred.
	DTypes []DType

	// Delimiter is the field delimiter (default ',')
	Delimiter rune
}

// DefaultCSVBatchReaderOptions returns default options.
func DefaultCSVBatchReaderOptions() CSVBatchReaderOptions {
	return CSVBatchReaderOptions{
		BatchSize: 65536,
		Delimiter: ',',
	}
}

// Next reads the next batch of data.
func (r *CSVBatchReader) Next(ctx context.Context) (*DataFrame, error) {
	if r.done {
		return nil, io.EOF
	}

	// Read up to batchSize rows
	records := make([][]string, 0, r.batchSize)
	for i := 0; i < r.batchSize; i++ {
		// Check context
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		record, err := r.reader.Read()
		if err == io.EOF {
			r.done = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		r.done = true
		return nil, io.EOF
	}

	// Infer types if not specified
	if r.dtypes == nil {
		r.dtypes = inferCSVTypes(records, len(r.headers))
		// Build schema
		r.schema, _ = NewSchema(r.headers, r.dtypes)
	}

	// Convert to DataFrame
	return recordsToDataFrame(r.headers, r.dtypes, records)
}

// Schema returns the schema of the data.
func (r *CSVBatchReader) Schema() *Schema {
	return r.schema
}

// Close releases resources.
func (r *CSVBatchReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

// inferCSVTypes infers column types from sample records
func inferCSVTypes(records [][]string, numCols int) []DType {
	dtypes := make([]DType, numCols)

	// Default to String, upgrade to numeric if possible
	for i := range dtypes {
		dtypes[i] = String
	}

	// Check each column
	for col := 0; col < numCols; col++ {
		canBeInt := true
		canBeFloat := true
		canBeBool := true

		for _, record := range records {
			if col >= len(record) {
				continue
			}
			val := strings.TrimSpace(record[col])
			if val == "" {
				continue
			}

			// Try int
			if canBeInt {
				if _, err := strconv.ParseInt(val, 10, 64); err != nil {
					canBeInt = false
				}
			}

			// Try float
			if canBeFloat {
				if _, err := strconv.ParseFloat(val, 64); err != nil {
					canBeFloat = false
				}
			}

			// Try bool
			if canBeBool {
				lower := strings.ToLower(val)
				if lower != "true" && lower != "false" && lower != "0" && lower != "1" {
					canBeBool = false
				}
			}
		}

		// Set type based on inference
		if canBeInt {
			dtypes[col] = Int64
		} else if canBeFloat {
			dtypes[col] = Float64
		} else if canBeBool {
			dtypes[col] = Bool
		} else {
			dtypes[col] = String
		}
	}

	return dtypes
}

// recordsToDataFrame converts CSV records to a DataFrame
func recordsToDataFrame(headers []string, dtypes []DType, records [][]string) (*DataFrame, error) {
	n := len(records)
	series := make([]*Series, len(headers))

	for col, name := range headers {
		dtype := dtypes[col]

		switch dtype {
		case Int64:
			data := make([]int64, n)
			for i, record := range records {
				if col >= len(record) {
					continue
				}
				val := strings.TrimSpace(record[col])
				if val == "" {
					continue
				}
				v, _ := strconv.ParseInt(val, 10, 64)
				data[i] = v
			}
			series[col] = NewSeriesInt64(name, data)

		case Float64:
			data := make([]float64, n)
			for i, record := range records {
				if col >= len(record) {
					continue
				}
				val := strings.TrimSpace(record[col])
				if val == "" {
					continue
				}
				v, _ := strconv.ParseFloat(val, 64)
				data[i] = v
			}
			series[col] = NewSeriesFloat64(name, data)

		case Bool:
			data := make([]bool, n)
			for i, record := range records {
				if col >= len(record) {
					continue
				}
				val := strings.TrimSpace(record[col])
				lower := strings.ToLower(val)
				data[i] = lower == "true" || lower == "1"
			}
			series[col] = NewSeriesBool(name, data)

		default: // String
			data := make([]string, n)
			for i, record := range records {
				if col >= len(record) {
					data[i] = ""
					continue
				}
				data[i] = strings.TrimSpace(record[col])
			}
			series[col] = NewSeriesString(name, data)
		}
	}

	return NewDataFrame(series...)
}
