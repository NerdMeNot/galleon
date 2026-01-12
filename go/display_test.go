package galleon

import (
	"strings"
	"testing"
)

func TestDefaultDisplayConfig(t *testing.T) {
	cfg := DefaultDisplayConfig()

	if cfg.MaxRows != 10 {
		t.Errorf("expected MaxRows=10, got %d", cfg.MaxRows)
	}
	if cfg.MaxCols != 10 {
		t.Errorf("expected MaxCols=10, got %d", cfg.MaxCols)
	}
	if cfg.FloatPrecision != 4 {
		t.Errorf("expected FloatPrecision=4, got %d", cfg.FloatPrecision)
	}
	if cfg.TableStyle != "rounded" {
		t.Errorf("expected TableStyle=rounded, got %s", cfg.TableStyle)
	}
}

func TestSetGetDisplayConfig(t *testing.T) {
	// Save original
	original := GetDisplayConfig()
	defer SetDisplayConfig(original)

	// Modify
	SetMaxDisplayRows(20)
	cfg := GetDisplayConfig()
	if cfg.MaxRows != 20 {
		t.Errorf("expected MaxRows=20 after SetMaxDisplayRows, got %d", cfg.MaxRows)
	}

	SetMaxDisplayCols(5)
	cfg = GetDisplayConfig()
	if cfg.MaxCols != 5 {
		t.Errorf("expected MaxCols=5 after SetMaxDisplayCols, got %d", cfg.MaxCols)
	}

	SetFloatPrecision(2)
	cfg = GetDisplayConfig()
	if cfg.FloatPrecision != 2 {
		t.Errorf("expected FloatPrecision=2 after SetFloatPrecision, got %d", cfg.FloatPrecision)
	}

	SetTableStyle("ascii")
	cfg = GetDisplayConfig()
	if cfg.TableStyle != "ascii" {
		t.Errorf("expected TableStyle=ascii after SetTableStyle, got %s", cfg.TableStyle)
	}
}

func TestDataFrameStringSmall(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("id", []int64{1, 2, 3}),
		NewSeriesString("name", []string{"Alice", "Bob", "Charlie"}),
		NewSeriesFloat64("score", []float64{95.5, 87.25, 92.0}),
	)
	if err != nil {
		t.Fatal(err)
	}

	s := df.String()

	// Should contain shape
	if !strings.Contains(s, "shape: (3, 3)") {
		t.Error("expected shape header in output")
	}

	// Should contain column names
	if !strings.Contains(s, "id") {
		t.Error("expected 'id' column in output")
	}
	if !strings.Contains(s, "name") {
		t.Error("expected 'name' column in output")
	}
	if !strings.Contains(s, "score") {
		t.Error("expected 'score' column in output")
	}

	// Should contain values
	if !strings.Contains(s, "Alice") {
		t.Error("expected 'Alice' in output")
	}
	if !strings.Contains(s, "Charlie") {
		t.Error("expected 'Charlie' in output")
	}
}

func TestDataFrameStringLargeRows(t *testing.T) {
	// Create DataFrame with 100 rows
	ids := make([]int64, 100)
	values := make([]float64, 100)
	for i := range ids {
		ids[i] = int64(i + 1)
		values[i] = float64(i) * 1.5
	}

	df, err := NewDataFrame(
		NewSeriesInt64("id", ids),
		NewSeriesFloat64("value", values),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Set to show 10 rows (5 head + 5 tail)
	original := GetDisplayConfig()
	defer SetDisplayConfig(original)
	SetMaxDisplayRows(10)

	s := df.String()

	// Should show shape
	if !strings.Contains(s, "shape: (100, 2)") {
		t.Error("expected shape (100, 2) in output")
	}

	// Should show first row (id=1)
	if !strings.Contains(s, "1") {
		t.Error("expected first row in output")
	}

	// Should show last row (id=100)
	if !strings.Contains(s, "100") {
		t.Error("expected last row in output")
	}

	// Should show ellipsis
	if !strings.Contains(s, "…") {
		t.Error("expected ellipsis for truncated rows")
	}
}

func TestDataFrameStringManyColumns(t *testing.T) {
	// Create DataFrame with 15 columns
	cols := make([]*Series, 15)
	for i := range cols {
		cols[i] = NewSeriesInt64("col"+string(rune('a'+i)), []int64{int64(i + 1)})
	}

	df, err := NewDataFrame(cols...)
	if err != nil {
		t.Fatal(err)
	}

	// Set to show only 6 columns
	original := GetDisplayConfig()
	defer SetDisplayConfig(original)
	SetMaxDisplayCols(6)

	s := df.String()

	// Should show shape
	if !strings.Contains(s, "shape: (1, 15)") {
		t.Error("expected shape (1, 15) in output")
	}

	// Should show ellipsis for columns
	if !strings.Contains(s, "…") {
		t.Error("expected ellipsis for truncated columns")
	}
}

func TestDataFrameStringWithConfig(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesFloat64("value", []float64{3.14159265358979}),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Test with different precision
	cfg := DefaultDisplayConfig()
	cfg.FloatPrecision = 2

	s := df.StringWithConfig(cfg)
	if !strings.Contains(s, "3.14") {
		t.Error("expected 2 decimal precision")
	}

	// Test with higher precision
	cfg.FloatPrecision = 6
	s = df.StringWithConfig(cfg)
	if !strings.Contains(s, "3.141593") {
		t.Error("expected 6 decimal precision")
	}
}

func TestTableStyles(t *testing.T) {
	df, err := NewDataFrame(
		NewSeriesInt64("x", []int64{1, 2}),
	)
	if err != nil {
		t.Fatal(err)
	}

	styles := []string{"rounded", "sharp", "ascii", "minimal"}
	for _, style := range styles {
		cfg := DefaultDisplayConfig()
		cfg.TableStyle = style
		s := df.StringWithConfig(cfg)
		if len(s) == 0 {
			t.Errorf("empty output for style %s", style)
		}
	}

	// Check specific characters for each style
	cfg := DefaultDisplayConfig()

	cfg.TableStyle = "rounded"
	s := df.StringWithConfig(cfg)
	if !strings.Contains(s, "╭") {
		t.Error("rounded style should use ╭")
	}

	cfg.TableStyle = "sharp"
	s = df.StringWithConfig(cfg)
	if !strings.Contains(s, "┌") {
		t.Error("sharp style should use ┌")
	}

	cfg.TableStyle = "ascii"
	s = df.StringWithConfig(cfg)
	if !strings.Contains(s, "+") {
		t.Error("ascii style should use +")
	}
}

func TestEmptyDataFrameString(t *testing.T) {
	df, _ := NewDataFrame()
	s := df.String()
	if s != "DataFrame(empty)" {
		t.Errorf("expected 'DataFrame(empty)', got %s", s)
	}
}
