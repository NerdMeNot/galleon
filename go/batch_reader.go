package galleon

import (
	"context"
	"io"
)

// BatchReader is an interface for reading data in batches.
// This enables processing of datasets larger than RAM.
type BatchReader interface {
	// Next reads the next batch of data.
	// Returns io.EOF when there are no more batches.
	Next(ctx context.Context) (*DataFrame, error)

	// Schema returns the schema of the data.
	// May return nil if schema is unknown until first read.
	Schema() *Schema

	// Close releases any resources held by the reader.
	Close() error
}

// BatchOptions configures batch reading behavior.
type BatchOptions struct {
	// BatchSize is the number of rows per batch.
	// Default: 65536 (matches typical columnar chunk size)
	BatchSize int
}

// DefaultBatchOptions returns default batch reading options.
func DefaultBatchOptions() BatchOptions {
	return BatchOptions{
		BatchSize: 65536,
	}
}

// ============================================================================
// Pipeline API for Streaming Processing
// ============================================================================

// Pipeline represents a streaming data processing pipeline.
type Pipeline struct {
	reader     BatchReader
	transforms []func(*DataFrame) (*DataFrame, error)
	filters    []Expr
	limit      int
	hasLimit   bool
}

// NewPipeline creates a new streaming pipeline from a batch reader.
func NewPipeline(reader BatchReader) *Pipeline {
	return &Pipeline{
		reader:     reader,
		transforms: make([]func(*DataFrame) (*DataFrame, error), 0),
		filters:    make([]Expr, 0),
	}
}

// Filter adds a filter predicate to the pipeline.
func (p *Pipeline) Filter(pred Expr) *Pipeline {
	p.filters = append(p.filters, pred)
	return p
}

// Transform adds a transformation function to the pipeline.
func (p *Pipeline) Transform(fn func(*DataFrame) (*DataFrame, error)) *Pipeline {
	p.transforms = append(p.transforms, fn)
	return p
}

// Limit sets a maximum number of rows to process.
func (p *Pipeline) Limit(n int) *Pipeline {
	p.limit = n
	p.hasLimit = true
	return p
}

// Collect processes all batches and combines the results into a single DataFrame.
func (p *Pipeline) Collect(ctx context.Context) (*DataFrame, error) {
	var results []*DataFrame
	totalRows := 0

	for {
		// Check context
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Read next batch
		batch, err := p.reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Apply filters
		for _, pred := range p.filters {
			batch, err = applyFilterToBatch(batch, pred)
			if err != nil {
				return nil, err
			}
		}

		// Apply transforms
		for _, transform := range p.transforms {
			batch, err = transform(batch)
			if err != nil {
				return nil, err
			}
		}

		// Check limit
		if p.hasLimit {
			remaining := p.limit - totalRows
			if remaining <= 0 {
				break
			}
			if batch.Height() > remaining {
				batch = batch.Head(remaining)
			}
		}

		results = append(results, batch)
		totalRows += batch.Height()

		if p.hasLimit && totalRows >= p.limit {
			break
		}
	}

	// Combine all batches
	if len(results) == 0 {
		// Return empty DataFrame with schema if available
		schema := p.reader.Schema()
		if schema != nil {
			return emptyDataFrameFromSchema(schema), nil
		}
		return NewDataFrame()
	}

	return ConcatDataFrames(results...)
}

// ForEach processes each batch without combining results.
// Useful for aggregations or side effects.
func (p *Pipeline) ForEach(ctx context.Context, fn func(*DataFrame) error) error {
	for {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next batch
		batch, err := p.reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Apply filters
		for _, pred := range p.filters {
			batch, err = applyFilterToBatch(batch, pred)
			if err != nil {
				return err
			}
		}

		// Apply transforms
		for _, transform := range p.transforms {
			batch, err = transform(batch)
			if err != nil {
				return err
			}
		}

		// Call user function
		if err := fn(batch); err != nil {
			return err
		}
	}

	return nil
}

// applyFilterToBatch applies a filter expression to a DataFrame
// Uses the lazy executor to evaluate the expression and filter
func applyFilterToBatch(df *DataFrame, pred Expr) (*DataFrame, error) {
	// Use lazy frame to evaluate the filter
	lf := df.Lazy().Filter(pred)
	return lf.Collect()
}

// emptyDataFrameFromSchema creates an empty DataFrame with the given schema
func emptyDataFrameFromSchema(schema *Schema) *DataFrame {
	series := make([]*Series, schema.Len())
	for i, name := range schema.Names() {
		series[i] = newEmptySeries(name, schema.dtypes[i])
	}
	df, _ := NewDataFrame(series...)
	return df
}

// ConcatDataFrames concatenates multiple DataFrames vertically.
func ConcatDataFrames(dfs ...*DataFrame) (*DataFrame, error) {
	if len(dfs) == 0 {
		return NewDataFrame()
	}
	if len(dfs) == 1 {
		return dfs[0], nil
	}

	// Use first DataFrame as reference for schema
	ref := dfs[0]
	numCols := ref.Width()
	colNames := ref.Columns()

	// Verify all DataFrames have same schema
	for i, df := range dfs[1:] {
		if df.Width() != numCols {
			return nil, &SchemaError{Message: "DataFrames have different number of columns", Index: i + 1}
		}
	}

	// Concatenate each column
	resultCols := make([]*Series, numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		colName := colNames[colIdx]
		dtype := ref.Column(colIdx).DType()

		var concatenated *Series
		for _, df := range dfs {
			col := df.ColumnByName(colName)
			if col == nil {
				return nil, &ColumnNotFoundError{Name: colName}
			}
			if concatenated == nil {
				// Copy the first column
				concatenated = col
			} else {
				appended, err := concatenateSeries(concatenated, col)
				if err != nil {
					return nil, err
				}
				concatenated = appended
			}
		}
		// Rename to original name
		concatenated = concatenated.Rename(colName)
		_ = dtype // preserve dtype info
		resultCols[colIdx] = concatenated
	}

	return NewDataFrame(resultCols...)
}

// concatenateSeries appends two series of the same type
func concatenateSeries(a, b *Series) (*Series, error) {
	if a.DType() != b.DType() {
		return nil, &SchemaError{Message: "series have different dtypes"}
	}

	switch a.DType() {
	case Float64:
		data := append(a.Float64(), b.Float64()...)
		return NewSeriesFloat64(a.Name(), data), nil
	case Float32:
		data := append(a.Float32(), b.Float32()...)
		return NewSeriesFloat32(a.Name(), data), nil
	case Int64:
		data := append(a.Int64(), b.Int64()...)
		return NewSeriesInt64(a.Name(), data), nil
	case Int32:
		data := append(a.Int32(), b.Int32()...)
		return NewSeriesInt32(a.Name(), data), nil
	case Bool:
		data := append(a.Bool(), b.Bool()...)
		return NewSeriesBool(a.Name(), data), nil
	case String:
		data := append(a.Strings(), b.Strings()...)
		return NewSeriesString(a.Name(), data), nil
	case Categorical:
		// Convert both to strings, concatenate, convert back to categorical
		aStrings := make([]string, a.Len())
		bStrings := make([]string, b.Len())
		for i := 0; i < a.Len(); i++ {
			aStrings[i] = a.Get(i).(string)
		}
		for i := 0; i < b.Len(); i++ {
			bStrings[i] = b.Get(i).(string)
		}
		data := append(aStrings, bStrings...)
		return NewSeriesCategorical(a.Name(), data), nil
	default:
		return nil, &ExprError{Message: "unsupported dtype for concatenation"}
	}
}

// ExprError represents an expression evaluation error
type ExprError struct {
	Expr    Expr
	Message string
}

func (e *ExprError) Error() string {
	if e.Expr != nil {
		return e.Message + ": " + e.Expr.String()
	}
	return e.Message
}

// SchemaError represents a schema mismatch error
type SchemaError struct {
	Message string
	Index   int
}

func (e *SchemaError) Error() string {
	return e.Message
}

// ColumnNotFoundError represents a column not found error
type ColumnNotFoundError struct {
	Name string
}

func (e *ColumnNotFoundError) Error() string {
	return "column not found: " + e.Name
}
