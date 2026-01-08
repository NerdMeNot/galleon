package galleon

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// ParquetReadOptions configures Parquet reading behavior
type ParquetReadOptions struct {
	Columns []string // Only read these columns (nil = all)
	MaxRows int      // Max rows to read (0 = unlimited)
}

// DefaultParquetReadOptions returns default Parquet reading options
func DefaultParquetReadOptions() ParquetReadOptions {
	return ParquetReadOptions{}
}

// ReadParquet reads a Parquet file into a DataFrame
func ReadParquet(path string, opts ...ParquetReadOptions) (*DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return ReadParquetFromReader(f, stat.Size(), opts...)
}

// colBuilder holds data while reading parquet columns
type colBuilder struct {
	dtype    DType
	f64Data  []float64
	f32Data  []float32
	i64Data  []int64
	i32Data  []int32
	boolData []bool
	strData  []string
}

// ReadParquetFromReader reads Parquet data from an io.ReaderAt into a DataFrame
func ReadParquetFromReader(r io.ReaderAt, size int64, opts ...ParquetReadOptions) (*DataFrame, error) {
	opt := DefaultParquetReadOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	pf, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	schema := pf.Schema()

	// Determine columns to read
	var colNames []string

	if len(opt.Columns) > 0 {
		colNames = opt.Columns
	} else {
		fields := schema.Fields()
		colNames = make([]string, len(fields))
		for i, f := range fields {
			colNames[i] = f.Name()
		}
	}

	// Build column index map
	colIndexMap := make(map[string]int)
	for i, col := range schema.Columns() {
		if len(col) > 0 {
			colIndexMap[col[0]] = i
		}
	}

	// Initialize builders based on schema
	builders := make([]colBuilder, len(colNames))
	colIndices := make([]int, len(colNames))

	for i, name := range colNames {
		idx, ok := colIndexMap[name]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found in parquet file", name)
		}
		colIndices[i] = idx

		leaf := schema.Columns()[idx]
		builders[i].dtype = parquetLeafToDType(schema, leaf)
	}

	// Get row groups
	rowGroups := pf.RowGroups()
	cfg := globalConfig

	// For parallel reading, we read each row group separately then combine
	if cfg.shouldParallelize(int(pf.NumRows())) && len(rowGroups) > 1 {
		return readParquetParallel(rowGroups, colNames, colIndices, builders, opt)
	}

	// Sequential read
	rowCount := 0
	for _, rg := range rowGroups {
		if opt.MaxRows > 0 && rowCount >= opt.MaxRows {
			break
		}

		rows := rg.Rows()
		rowBuf := make([]parquet.Row, 1000)
		for {
			n, err := rows.ReadRows(rowBuf)
			if err != nil && err != io.EOF {
				rows.Close()
				return nil, fmt.Errorf("failed to read rows: %w", err)
			}
			if n == 0 {
				break
			}

			for _, row := range rowBuf[:n] {
				if opt.MaxRows > 0 && rowCount >= opt.MaxRows {
					break
				}

				for i, colIdx := range colIndices {
					if colIdx < len(row) {
						appendValue(&builders[i], row[colIdx])
					} else {
						appendNullValue(&builders[i])
					}
				}
				rowCount++
			}

			if opt.MaxRows > 0 && rowCount >= opt.MaxRows {
				break
			}
		}
		rows.Close()
	}

	// Build Series from builders
	columns := make([]*Series, len(colNames))
	for i, name := range colNames {
		b := &builders[i]
		switch b.dtype {
		case Float64:
			columns[i] = NewSeriesFloat64(name, b.f64Data)
		case Float32:
			columns[i] = NewSeriesFloat32(name, b.f32Data)
		case Int64:
			columns[i] = NewSeriesInt64(name, b.i64Data)
		case Int32:
			columns[i] = NewSeriesInt32(name, b.i32Data)
		case Bool:
			columns[i] = NewSeriesBool(name, b.boolData)
		case String:
			columns[i] = NewSeriesString(name, b.strData)
		default:
			columns[i] = NewSeriesString(name, b.strData)
		}
	}

	return NewDataFrame(columns...)
}

// readParquetParallel reads row groups in parallel
func readParquetParallel(rowGroups []parquet.RowGroup, colNames []string, colIndices []int, templateBuilders []colBuilder, opt ParquetReadOptions) (*DataFrame, error) {
	numRGs := len(rowGroups)

	// Each row group gets its own set of builders
	rgBuilders := make([][]colBuilder, numRGs)
	rgErrors := make([]error, numRGs)

	var wg sync.WaitGroup
	for rgIdx := range rowGroups {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Create builders for this row group
			builders := make([]colBuilder, len(colNames))
			for i := range builders {
				builders[i].dtype = templateBuilders[i].dtype
			}

			rg := rowGroups[idx]
			rows := rg.Rows()
			defer rows.Close()

			rowBuf := make([]parquet.Row, 1000)
			for {
				n, err := rows.ReadRows(rowBuf)
				if err != nil && err != io.EOF {
					rgErrors[idx] = fmt.Errorf("failed to read row group %d: %w", idx, err)
					return
				}
				if n == 0 {
					break
				}

				for _, row := range rowBuf[:n] {
					for i, colIdx := range colIndices {
						if colIdx < len(row) {
							appendValue(&builders[i], row[colIdx])
						} else {
							appendNullValue(&builders[i])
						}
					}
				}
			}

			rgBuilders[idx] = builders
		}(rgIdx)
	}
	wg.Wait()

	// Check for errors
	for i, err := range rgErrors {
		if err != nil {
			return nil, fmt.Errorf("row group %d: %w", i, err)
		}
	}

	// Merge builders from all row groups
	mergedBuilders := make([]colBuilder, len(colNames))
	for i := range mergedBuilders {
		mergedBuilders[i].dtype = templateBuilders[i].dtype
	}

	totalRows := 0
	for _, rgB := range rgBuilders {
		if len(rgB) > 0 {
			switch rgB[0].dtype {
			case Float64:
				totalRows += len(rgB[0].f64Data)
			case Float32:
				totalRows += len(rgB[0].f32Data)
			case Int64:
				totalRows += len(rgB[0].i64Data)
			case Int32:
				totalRows += len(rgB[0].i32Data)
			case Bool:
				totalRows += len(rgB[0].boolData)
			case String:
				totalRows += len(rgB[0].strData)
			}
		}
	}

	// Pre-allocate and merge
	for i := range mergedBuilders {
		switch mergedBuilders[i].dtype {
		case Float64:
			mergedBuilders[i].f64Data = make([]float64, 0, totalRows)
			for _, rgB := range rgBuilders {
				mergedBuilders[i].f64Data = append(mergedBuilders[i].f64Data, rgB[i].f64Data...)
			}
		case Float32:
			mergedBuilders[i].f32Data = make([]float32, 0, totalRows)
			for _, rgB := range rgBuilders {
				mergedBuilders[i].f32Data = append(mergedBuilders[i].f32Data, rgB[i].f32Data...)
			}
		case Int64:
			mergedBuilders[i].i64Data = make([]int64, 0, totalRows)
			for _, rgB := range rgBuilders {
				mergedBuilders[i].i64Data = append(mergedBuilders[i].i64Data, rgB[i].i64Data...)
			}
		case Int32:
			mergedBuilders[i].i32Data = make([]int32, 0, totalRows)
			for _, rgB := range rgBuilders {
				mergedBuilders[i].i32Data = append(mergedBuilders[i].i32Data, rgB[i].i32Data...)
			}
		case Bool:
			mergedBuilders[i].boolData = make([]bool, 0, totalRows)
			for _, rgB := range rgBuilders {
				mergedBuilders[i].boolData = append(mergedBuilders[i].boolData, rgB[i].boolData...)
			}
		case String:
			mergedBuilders[i].strData = make([]string, 0, totalRows)
			for _, rgB := range rgBuilders {
				mergedBuilders[i].strData = append(mergedBuilders[i].strData, rgB[i].strData...)
			}
		}
	}

	// Apply MaxRows limit if needed
	if opt.MaxRows > 0 && totalRows > opt.MaxRows {
		for i := range mergedBuilders {
			switch mergedBuilders[i].dtype {
			case Float64:
				mergedBuilders[i].f64Data = mergedBuilders[i].f64Data[:opt.MaxRows]
			case Float32:
				mergedBuilders[i].f32Data = mergedBuilders[i].f32Data[:opt.MaxRows]
			case Int64:
				mergedBuilders[i].i64Data = mergedBuilders[i].i64Data[:opt.MaxRows]
			case Int32:
				mergedBuilders[i].i32Data = mergedBuilders[i].i32Data[:opt.MaxRows]
			case Bool:
				mergedBuilders[i].boolData = mergedBuilders[i].boolData[:opt.MaxRows]
			case String:
				mergedBuilders[i].strData = mergedBuilders[i].strData[:opt.MaxRows]
			}
		}
	}

	// Build final columns
	columns := make([]*Series, len(colNames))
	for i, name := range colNames {
		b := &mergedBuilders[i]
		switch b.dtype {
		case Float64:
			columns[i] = NewSeriesFloat64(name, b.f64Data)
		case Float32:
			columns[i] = NewSeriesFloat32(name, b.f32Data)
		case Int64:
			columns[i] = NewSeriesInt64(name, b.i64Data)
		case Int32:
			columns[i] = NewSeriesInt32(name, b.i32Data)
		case Bool:
			columns[i] = NewSeriesBool(name, b.boolData)
		case String:
			columns[i] = NewSeriesString(name, b.strData)
		default:
			columns[i] = NewSeriesString(name, b.strData)
		}
	}

	return NewDataFrame(columns...)
}

func parquetLeafToDType(schema *parquet.Schema, leaf []string) DType {
	if len(leaf) == 0 {
		return String
	}

	// Find the column definition
	for _, col := range schema.Fields() {
		if col.Name() == leaf[0] {
			t := col.Type()
			if t == nil {
				return String
			}
			kind := t.Kind()
			switch kind {
			case parquet.Boolean:
				return Bool
			case parquet.Int32:
				return Int32
			case parquet.Int64:
				return Int64
			case parquet.Float:
				return Float32
			case parquet.Double:
				return Float64
			case parquet.ByteArray, parquet.FixedLenByteArray:
				return String
			default:
				return String
			}
		}
	}
	return String
}

func appendNullValue(b *colBuilder) {
	switch b.dtype {
	case Float64:
		b.f64Data = append(b.f64Data, 0)
	case Float32:
		b.f32Data = append(b.f32Data, 0)
	case Int64:
		b.i64Data = append(b.i64Data, 0)
	case Int32:
		b.i32Data = append(b.i32Data, 0)
	case Bool:
		b.boolData = append(b.boolData, false)
	case String:
		b.strData = append(b.strData, "")
	}
}

func appendValue(b *colBuilder, val parquet.Value) {
	if val.IsNull() {
		appendNullValue(b)
		return
	}

	switch b.dtype {
	case Float64:
		b.f64Data = append(b.f64Data, val.Double())
	case Float32:
		b.f32Data = append(b.f32Data, val.Float())
	case Int64:
		b.i64Data = append(b.i64Data, val.Int64())
	case Int32:
		b.i32Data = append(b.i32Data, val.Int32())
	case Bool:
		b.boolData = append(b.boolData, val.Boolean())
	case String:
		b.strData = append(b.strData, string(val.ByteArray()))
	}
}

// ParquetWriteOptions configures Parquet writing behavior
type ParquetWriteOptions struct {
	Compression  string // "snappy", "gzip", "zstd", "none" (default "snappy")
	RowGroupSize int    // Rows per row group (default 1000000)
}

// DefaultParquetWriteOptions returns default Parquet writing options
func DefaultParquetWriteOptions() ParquetWriteOptions {
	return ParquetWriteOptions{
		Compression:  "snappy",
		RowGroupSize: 1000000,
	}
}

// WriteParquet writes a DataFrame to a Parquet file
func (df *DataFrame) WriteParquet(path string, opts ...ParquetWriteOptions) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	return df.WriteParquetToWriter(f, opts...)
}

// parquetGenericRow is used for parquet writing
type parquetGenericRow struct {
	values []parquet.Value
}

// WriteParquetToWriter writes a DataFrame to an io.Writer
func (df *DataFrame) WriteParquetToWriter(w io.Writer, opts ...ParquetWriteOptions) error {
	opt := DefaultParquetWriteOptions()
	if len(opts) > 0 {
		opt = opts[0]
	}

	if df.Width() == 0 || df.Height() == 0 {
		return nil
	}

	// Build schema as a group of named columns
	group := make(parquet.Group)
	for _, col := range df.columns {
		group[col.Name()] = dtypeToParquetNode(col.DType())
	}

	schema := parquet.NewSchema("dataframe", group)

	// Determine compression
	var writerOpts []parquet.WriterOption
	writerOpts = append(writerOpts, schema)
	switch opt.Compression {
	case "snappy":
		writerOpts = append(writerOpts, parquet.Compression(&parquet.Snappy))
	case "gzip":
		writerOpts = append(writerOpts, parquet.Compression(&parquet.Gzip))
	case "zstd":
		writerOpts = append(writerOpts, parquet.Compression(&parquet.Zstd))
	}

	pw := parquet.NewWriter(w, writerOpts...)
	defer pw.Close()

	height := df.Height()
	width := df.Width()

	// Use smaller batches to avoid memory issues with large string columns
	batchSize := 1000

	// Write in batches for better performance
	rows := make([]parquet.Row, 0, batchSize)
	for i := 0; i < height; i++ {
		row := make(parquet.Row, width)
		for j, col := range df.columns {
			row[j] = toParquetValue(col.Get(i), col.DType())
		}
		rows = append(rows, row)

		// Flush batch when full
		if len(rows) >= batchSize {
			if _, err := pw.WriteRows(rows); err != nil {
				return fmt.Errorf("failed to write rows at %d: %w", i-len(rows)+1, err)
			}
			rows = rows[:0]
		}
	}

	// Write remaining rows
	if len(rows) > 0 {
		if _, err := pw.WriteRows(rows); err != nil {
			return fmt.Errorf("failed to write final rows: %w", err)
		}
	}

	return pw.Close()
}

func dtypeToParquetNode(dtype DType) parquet.Node {
	switch dtype {
	case Float64:
		return parquet.Leaf(parquet.DoubleType)
	case Float32:
		return parquet.Leaf(parquet.FloatType)
	case Int64:
		return parquet.Leaf(parquet.Int64Type)
	case Int32:
		return parquet.Leaf(parquet.Int32Type)
	case Bool:
		return parquet.Leaf(parquet.BooleanType)
	case String:
		return parquet.Leaf(parquet.ByteArrayType)
	default:
		return parquet.Leaf(parquet.ByteArrayType)
	}
}

func toParquetValue(v interface{}, dtype DType) parquet.Value {
	if v == nil {
		return parquet.NullValue()
	}

	switch dtype {
	case Float64:
		if f, ok := v.(float64); ok {
			return parquet.DoubleValue(f)
		}
	case Float32:
		if f, ok := v.(float32); ok {
			return parquet.FloatValue(f)
		}
	case Int64:
		if i, ok := v.(int64); ok {
			return parquet.Int64Value(i)
		}
	case Int32:
		if i, ok := v.(int32); ok {
			return parquet.Int32Value(i)
		}
	case Bool:
		if b, ok := v.(bool); ok {
			return parquet.BooleanValue(b)
		}
	case String:
		if s, ok := v.(string); ok {
			return parquet.ByteArrayValue([]byte(s))
		}
	}

	// Fallback to string representation
	return parquet.ByteArrayValue([]byte(fmt.Sprintf("%v", v)))
}
