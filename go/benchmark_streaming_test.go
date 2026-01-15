package galleon

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// ============================================================================
// Streaming/Batch Processing Benchmarks
// Run with: go test -tags dev -bench=BenchmarkStreaming -benchmem
// ============================================================================

func makeStreamingCSV(nRows int) string {
	var sb strings.Builder
	sb.WriteString("id,name,value,score\n")
	for i := 0; i < nRows; i++ {
		sb.WriteString(fmt.Sprintf("%d,name_%d,%d,%.2f\n",
			i, i%100, i*10, float64(i)*0.5))
	}
	return sb.String()
}

// ============================================================================
// CSV Batch Reader Benchmarks
// ============================================================================

func BenchmarkStreaming_CSVReader_10K(b *testing.B) {
	csv := makeStreamingCSV(10_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		for {
			_, err := reader.Next(ctx)
			if err != nil {
				break
			}
		}
		reader.Close()
	}
}

func BenchmarkStreaming_CSVReader_100K(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		for {
			_, err := reader.Next(ctx)
			if err != nil {
				break
			}
		}
		reader.Close()
	}
}

func BenchmarkStreaming_CSVReader_100K_SmallBatch(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()
	opts := CSVBatchReaderOptions{BatchSize: 1000}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv), opts)
		for {
			_, err := reader.Next(ctx)
			if err != nil {
				break
			}
		}
		reader.Close()
	}
}

// ============================================================================
// Pipeline Benchmarks
// ============================================================================

func BenchmarkStreaming_Pipeline_Collect_10K(b *testing.B) {
	csv := makeStreamingCSV(10_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader)
		_, _ = pipeline.Collect(ctx)
	}
}

func BenchmarkStreaming_Pipeline_Collect_100K(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader)
		_, _ = pipeline.Collect(ctx)
	}
}

func BenchmarkStreaming_Pipeline_Filter_10K(b *testing.B) {
	csv := makeStreamingCSV(10_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader).
			Filter(Col("value").Gt(Lit(50000)))
		_, _ = pipeline.Collect(ctx)
	}
}

func BenchmarkStreaming_Pipeline_Filter_100K(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader).
			Filter(Col("value").Gt(Lit(500000)))
		_, _ = pipeline.Collect(ctx)
	}
}

func BenchmarkStreaming_Pipeline_Limit_100K(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader).Limit(1000)
		_, _ = pipeline.Collect(ctx)
	}
}

func BenchmarkStreaming_Pipeline_Transform_10K(b *testing.B) {
	csv := makeStreamingCSV(10_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader).
			Transform(func(df *DataFrame) (*DataFrame, error) {
				return df.Lazy().
					WithColumn("doubled", Col("value").Mul(Lit(2))).
					Collect()
			})
		_, _ = pipeline.Collect(ctx)
	}
}

func BenchmarkStreaming_Pipeline_Transform_100K(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader).
			Transform(func(df *DataFrame) (*DataFrame, error) {
				return df.Lazy().
					WithColumn("doubled", Col("value").Mul(Lit(2))).
					Collect()
			})
		_, _ = pipeline.Collect(ctx)
	}
}

// ============================================================================
// ForEach Benchmarks
// ============================================================================

func BenchmarkStreaming_ForEach_10K(b *testing.B) {
	csv := makeStreamingCSV(10_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader)
		_ = pipeline.ForEach(ctx, func(df *DataFrame) error {
			_ = df.Height()
			return nil
		})
	}
}

func BenchmarkStreaming_ForEach_100K(b *testing.B) {
	csv := makeStreamingCSV(100_000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewCSVBatchReader(strings.NewReader(csv))
		pipeline := NewPipeline(reader)
		_ = pipeline.ForEach(ctx, func(df *DataFrame) error {
			_ = df.Height()
			return nil
		})
	}
}

// ============================================================================
// ConcatDataFrames Benchmarks
// ============================================================================

func BenchmarkStreaming_Concat_10x1K(b *testing.B) {
	// Create 10 DataFrames of 1K rows each
	dfs := make([]*DataFrame, 10)
	for i := 0; i < 10; i++ {
		values := make([]float64, 1000)
		for j := range values {
			values[j] = float64(i*1000 + j)
		}
		dfs[i], _ = NewDataFrame(
			NewSeriesFloat64("value", values),
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ConcatDataFrames(dfs...)
	}
}

func BenchmarkStreaming_Concat_100x1K(b *testing.B) {
	// Create 100 DataFrames of 1K rows each
	dfs := make([]*DataFrame, 100)
	for i := 0; i < 100; i++ {
		values := make([]float64, 1000)
		for j := range values {
			values[j] = float64(i*1000 + j)
		}
		dfs[i], _ = NewDataFrame(
			NewSeriesFloat64("value", values),
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ConcatDataFrames(dfs...)
	}
}

func BenchmarkStreaming_Concat_10x10K(b *testing.B) {
	// Create 10 DataFrames of 10K rows each
	dfs := make([]*DataFrame, 10)
	for i := 0; i < 10; i++ {
		values := make([]float64, 10_000)
		for j := range values {
			values[j] = float64(i*10000 + j)
		}
		dfs[i], _ = NewDataFrame(
			NewSeriesFloat64("value", values),
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ConcatDataFrames(dfs...)
	}
}
