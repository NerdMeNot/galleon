package galleon

import (
	"fmt"
	"strings"
	"sync"
)

// DisplayConfig controls how DataFrames are formatted when printed.
type DisplayConfig struct {
	// MaxRows is the maximum number of rows to display.
	// If the DataFrame has more rows, it shows head and tail rows with "..." in between.
	// Default: 10 (5 head + 5 tail)
	MaxRows int

	// MaxCols is the maximum number of columns to display.
	// If the DataFrame has more columns, middle columns are replaced with "...".
	// Default: 10
	MaxCols int

	// MaxColWidth is the maximum width for column content.
	// Values longer than this are truncated with "...".
	// Default: 25
	MaxColWidth int

	// MinColWidth is the minimum column width for alignment.
	// Default: 8
	MinColWidth int

	// FloatPrecision is the number of decimal places for float values.
	// Default: 4
	FloatPrecision int

	// ShowDTypes controls whether to display data types under column names.
	// Default: true
	ShowDTypes bool

	// ShowShape controls whether to display the shape (rows × columns) header.
	// Default: true
	ShowShape bool

	// TableStyle controls the table border style.
	// Options: "rounded", "sharp", "ascii", "minimal"
	// Default: "rounded"
	TableStyle string
}

// Table style characters
type tableChars struct {
	topLeft, topRight, bottomLeft, bottomRight string
	horizontal, vertical                       string
	topT, bottomT, leftT, rightT, cross        string
}

var tableStyles = map[string]tableChars{
	"rounded": {
		topLeft: "╭", topRight: "╮", bottomLeft: "╰", bottomRight: "╯",
		horizontal: "─", vertical: "│",
		topT: "┬", bottomT: "┴", leftT: "├", rightT: "┤", cross: "┼",
	},
	"sharp": {
		topLeft: "┌", topRight: "┐", bottomLeft: "└", bottomRight: "┘",
		horizontal: "─", vertical: "│",
		topT: "┬", bottomT: "┴", leftT: "├", rightT: "┤", cross: "┼",
	},
	"ascii": {
		topLeft: "+", topRight: "+", bottomLeft: "+", bottomRight: "+",
		horizontal: "-", vertical: "|",
		topT: "+", bottomT: "+", leftT: "+", rightT: "+", cross: "+",
	},
	"minimal": {
		topLeft: " ", topRight: " ", bottomLeft: " ", bottomRight: " ",
		horizontal: "─", vertical: " ",
		topT: " ", bottomT: " ", leftT: " ", rightT: " ", cross: " ",
	},
}

// DefaultDisplayConfig returns the default display configuration.
func DefaultDisplayConfig() DisplayConfig {
	return DisplayConfig{
		MaxRows:        10,
		MaxCols:        10,
		MaxColWidth:    25,
		MinColWidth:    8,
		FloatPrecision: 4,
		ShowDTypes:     true,
		ShowShape:      true,
		TableStyle:     "rounded",
	}
}

// Global display configuration with mutex for thread safety
var (
	globalDisplayConfig = DefaultDisplayConfig()
	displayConfigMu     sync.RWMutex
)

// SetDisplayConfig sets the global display configuration.
func SetDisplayConfig(cfg DisplayConfig) {
	displayConfigMu.Lock()
	defer displayConfigMu.Unlock()
	globalDisplayConfig = cfg
}

// GetDisplayConfig returns the current global display configuration.
func GetDisplayConfig() DisplayConfig {
	displayConfigMu.RLock()
	defer displayConfigMu.RUnlock()
	return globalDisplayConfig
}

// SetMaxDisplayRows sets the maximum number of rows to display.
func SetMaxDisplayRows(n int) {
	displayConfigMu.Lock()
	defer displayConfigMu.Unlock()
	globalDisplayConfig.MaxRows = n
}

// SetMaxDisplayCols sets the maximum number of columns to display.
func SetMaxDisplayCols(n int) {
	displayConfigMu.Lock()
	defer displayConfigMu.Unlock()
	globalDisplayConfig.MaxCols = n
}

// SetFloatPrecision sets the decimal precision for float display.
func SetFloatPrecision(n int) {
	displayConfigMu.Lock()
	defer displayConfigMu.Unlock()
	globalDisplayConfig.FloatPrecision = n
}

// SetTableStyle sets the table border style.
// Options: "rounded", "sharp", "ascii", "minimal"
func SetTableStyle(style string) {
	displayConfigMu.Lock()
	defer displayConfigMu.Unlock()
	if _, ok := tableStyles[style]; ok {
		globalDisplayConfig.TableStyle = style
	}
}

// formatDisplayValue formats a value for display with the given configuration.
func formatDisplayValue(val interface{}, cfg DisplayConfig) string {
	var s string
	switch v := val.(type) {
	case nil:
		s = "null"
	case float64:
		format := fmt.Sprintf("%%.%df", cfg.FloatPrecision)
		s = fmt.Sprintf(format, v)
	case float32:
		format := fmt.Sprintf("%%.%df", cfg.FloatPrecision)
		s = fmt.Sprintf(format, v)
	case string:
		s = v
	case bool:
		if v {
			s = "true"
		} else {
			s = "false"
		}
	default:
		s = fmt.Sprintf("%v", v)
	}

	// Truncate if too long
	if len(s) > cfg.MaxColWidth {
		s = s[:cfg.MaxColWidth-3] + "..."
	}
	return s
}

// calculateColumnWidths computes optimal width for each column.
func calculateColumnWidths(df *DataFrame, cfg DisplayConfig, rowIndices []int) []int {
	widths := make([]int, len(df.columns))

	for i, col := range df.columns {
		// Start with column name width
		widths[i] = len(col.Name())

		// Check data type width
		if cfg.ShowDTypes {
			dtypeLen := len(col.DType().String())
			if dtypeLen > widths[i] {
				widths[i] = dtypeLen
			}
		}

		// Check sample values
		for _, rowIdx := range rowIndices {
			valStr := formatDisplayValue(col.Get(rowIdx), cfg)
			if len(valStr) > widths[i] {
				widths[i] = len(valStr)
			}
		}

		// Apply min/max constraints
		if widths[i] < cfg.MinColWidth {
			widths[i] = cfg.MinColWidth
		}
		if widths[i] > cfg.MaxColWidth {
			widths[i] = cfg.MaxColWidth
		}
	}

	return widths
}

// StringWithConfig formats the DataFrame using the provided configuration.
func (df *DataFrame) StringWithConfig(cfg DisplayConfig) string {
	if df.height == 0 || len(df.columns) == 0 {
		return "DataFrame(empty)"
	}

	chars, ok := tableStyles[cfg.TableStyle]
	if !ok {
		chars = tableStyles["rounded"]
	}

	var sb strings.Builder

	// Shape header
	if cfg.ShowShape {
		sb.WriteString(fmt.Sprintf("shape: (%d, %d)\n", df.height, len(df.columns)))
	}

	// Determine which columns to show
	numCols := len(df.columns)
	showAllCols := numCols <= cfg.MaxCols
	var colIndices []int
	if showAllCols {
		colIndices = make([]int, numCols)
		for i := range colIndices {
			colIndices[i] = i
		}
	} else {
		// Show first half and last half with "..." in middle
		headCols := cfg.MaxCols / 2
		tailCols := cfg.MaxCols - headCols
		colIndices = make([]int, 0, cfg.MaxCols)
		for i := 0; i < headCols; i++ {
			colIndices = append(colIndices, i)
		}
		colIndices = append(colIndices, -1) // marker for "..."
		for i := numCols - tailCols; i < numCols; i++ {
			colIndices = append(colIndices, i)
		}
	}

	// Determine which rows to show
	showAllRows := df.height <= cfg.MaxRows
	var rowIndices []int
	if showAllRows {
		rowIndices = make([]int, df.height)
		for i := range rowIndices {
			rowIndices[i] = i
		}
	} else {
		// Show head and tail with "..." in middle
		headRows := cfg.MaxRows / 2
		tailRows := cfg.MaxRows - headRows
		rowIndices = make([]int, 0, cfg.MaxRows)
		for i := 0; i < headRows; i++ {
			rowIndices = append(rowIndices, i)
		}
		rowIndices = append(rowIndices, -1) // marker for "..."
		for i := df.height - tailRows; i < df.height; i++ {
			rowIndices = append(rowIndices, i)
		}
	}

	// Calculate column widths (only for visible columns)
	allWidths := calculateColumnWidths(df, cfg, filterPositive(rowIndices))
	colWidths := make([]int, len(colIndices))
	for i, colIdx := range colIndices {
		if colIdx == -1 {
			colWidths[i] = 3 // "..."
		} else {
			colWidths[i] = allWidths[colIdx]
		}
	}

	// Build the table
	// Top border
	sb.WriteString(chars.topLeft)
	for i, w := range colWidths {
		if i > 0 {
			sb.WriteString(chars.topT)
		}
		sb.WriteString(strings.Repeat(chars.horizontal, w+2))
	}
	sb.WriteString(chars.topRight)
	sb.WriteString("\n")

	// Column names
	sb.WriteString(chars.vertical)
	for i, colIdx := range colIndices {
		if colIdx == -1 {
			sb.WriteString(fmt.Sprintf(" %*s ", colWidths[i], "…"))
		} else {
			name := df.columns[colIdx].Name()
			if len(name) > colWidths[i] {
				name = name[:colWidths[i]-3] + "..."
			}
			sb.WriteString(fmt.Sprintf(" %-*s ", colWidths[i], name))
		}
		sb.WriteString(chars.vertical)
	}
	sb.WriteString("\n")

	// Data types row
	if cfg.ShowDTypes {
		sb.WriteString(chars.vertical)
		for i, colIdx := range colIndices {
			if colIdx == -1 {
				sb.WriteString(fmt.Sprintf(" %*s ", colWidths[i], "---"))
			} else {
				dtype := df.columns[colIdx].DType().String()
				if len(dtype) > colWidths[i] {
					dtype = dtype[:colWidths[i]-3] + "..."
				}
				sb.WriteString(fmt.Sprintf(" %-*s ", colWidths[i], dtype))
			}
			sb.WriteString(chars.vertical)
		}
		sb.WriteString("\n")
	}

	// Separator after header
	sb.WriteString(chars.leftT)
	for i, w := range colWidths {
		if i > 0 {
			sb.WriteString(chars.cross)
		}
		sb.WriteString(strings.Repeat(chars.horizontal, w+2))
	}
	sb.WriteString(chars.rightT)
	sb.WriteString("\n")

	// Data rows
	for _, rowIdx := range rowIndices {
		sb.WriteString(chars.vertical)
		if rowIdx == -1 {
			// Ellipsis row
			for i, w := range colWidths {
				sb.WriteString(fmt.Sprintf(" %*s ", w, "…"))
				sb.WriteString(chars.vertical)
				_ = i
			}
		} else {
			for i, colIdx := range colIndices {
				if colIdx == -1 {
					sb.WriteString(fmt.Sprintf(" %*s ", colWidths[i], "…"))
				} else {
					val := df.columns[colIdx].Get(rowIdx)
					valStr := formatDisplayValue(val, cfg)
					sb.WriteString(fmt.Sprintf(" %*s ", colWidths[i], valStr))
				}
				sb.WriteString(chars.vertical)
			}
		}
		sb.WriteString("\n")
	}

	// Bottom border
	sb.WriteString(chars.bottomLeft)
	for i, w := range colWidths {
		if i > 0 {
			sb.WriteString(chars.bottomT)
		}
		sb.WriteString(strings.Repeat(chars.horizontal, w+2))
	}
	sb.WriteString(chars.bottomRight)

	return sb.String()
}

// filterPositive returns only positive indices (filters out -1 markers).
func filterPositive(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if idx >= 0 {
			result = append(result, idx)
		}
	}
	return result
}

// SeriesStringWithConfig formats the Series using the provided configuration.
func SeriesStringWithConfig(s *Series, cfg DisplayConfig) string {
	if s.Len() == 0 {
		return fmt.Sprintf("Series: '%s' (%s)\nlength: 0\n[]", s.Name(), s.DType())
	}

	chars, ok := tableStyles[cfg.TableStyle]
	if !ok {
		chars = tableStyles["rounded"]
	}

	var sb strings.Builder

	// Header
	sb.WriteString(fmt.Sprintf("Series: '%s' (%s)\n", s.Name(), s.DType()))
	sb.WriteString(fmt.Sprintf("length: %d\n", s.Len()))

	// Determine which rows to show
	showAllRows := s.Len() <= cfg.MaxRows
	var rowIndices []int
	if showAllRows {
		rowIndices = make([]int, s.Len())
		for i := range rowIndices {
			rowIndices[i] = i
		}
	} else {
		headRows := cfg.MaxRows / 2
		tailRows := cfg.MaxRows - headRows
		rowIndices = make([]int, 0, cfg.MaxRows+1)
		for i := 0; i < headRows; i++ {
			rowIndices = append(rowIndices, i)
		}
		rowIndices = append(rowIndices, -1) // marker for "..."
		for i := s.Len() - tailRows; i < s.Len(); i++ {
			rowIndices = append(rowIndices, i)
		}
	}

	// Calculate column widths
	indexWidth := len(fmt.Sprintf("%d", s.Len()-1))
	if indexWidth < 3 {
		indexWidth = 3
	}

	valueWidth := cfg.MinColWidth
	for _, idx := range rowIndices {
		if idx >= 0 {
			valStr := formatDisplayValue(s.Get(idx), cfg)
			if len(valStr) > valueWidth {
				valueWidth = len(valStr)
			}
		}
	}
	if valueWidth > cfg.MaxColWidth {
		valueWidth = cfg.MaxColWidth
	}

	// Top border
	sb.WriteString(chars.topLeft)
	sb.WriteString(strings.Repeat(chars.horizontal, indexWidth+2))
	sb.WriteString(chars.topT)
	sb.WriteString(strings.Repeat(chars.horizontal, valueWidth+2))
	sb.WriteString(chars.topRight)
	sb.WriteString("\n")

	// Data rows
	for _, idx := range rowIndices {
		sb.WriteString(chars.vertical)
		if idx == -1 {
			sb.WriteString(fmt.Sprintf(" %*s ", indexWidth, "…"))
			sb.WriteString(chars.vertical)
			sb.WriteString(fmt.Sprintf(" %*s ", valueWidth, "…"))
		} else {
			sb.WriteString(fmt.Sprintf(" %*d ", indexWidth, idx))
			sb.WriteString(chars.vertical)
			valStr := formatDisplayValue(s.Get(idx), cfg)
			if len(valStr) > valueWidth {
				valStr = valStr[:valueWidth-3] + "..."
			}
			sb.WriteString(fmt.Sprintf(" %*s ", valueWidth, valStr))
		}
		sb.WriteString(chars.vertical)
		sb.WriteString("\n")
	}

	// Bottom border
	sb.WriteString(chars.bottomLeft)
	sb.WriteString(strings.Repeat(chars.horizontal, indexWidth+2))
	sb.WriteString(chars.bottomT)
	sb.WriteString(strings.Repeat(chars.horizontal, valueWidth+2))
	sb.WriteString(chars.bottomRight)

	return sb.String()
}
