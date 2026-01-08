# Benchmark Results

Generated: 2026-01-08T10:44:36.420883


### 10,000 Rows

| Operation | Polars | Pandas | Polars Speedup |
|-----------|--------|--------|----------------|
| Sum | 1.3µs | 10.4µs | 8.1x |
| Min | 1.3µs | 12.3µs | 9.3x |
| Max | 1.3µs | 12.0µs | 9.3x |
| Mean | 1.1µs | 12.9µs | 11.5x |
| Filter | 83.4µs | 97.5µs | 1.2x |
| Sort | 307.2µs | 403.7µs | 1.3x |
| GroupBy Sum | 240.8µs | 154.4µs | 0.6x |
| GroupBy Mean | 281.5µs | 135.8µs | 0.5x |
| Inner Join | 381.6µs | 1.16ms | 3.0x |
| Left Join | 514.4µs | 1.11ms | 2.2x |

### 100,000 Rows

| Operation | Polars | Pandas | Polars Speedup |
|-----------|--------|--------|----------------|
| Sum | 9.0µs | 30.0µs | 3.3x |
| Min | 8.8µs | 43.4µs | 4.9x |
| Max | 8.8µs | 45.4µs | 5.2x |
| Mean | 9.1µs | 49.6µs | 5.5x |
| Filter | 102.0µs | 366.8µs | 3.6x |
| Sort | 1.32ms | 7.15ms | 5.4x |
| GroupBy Sum | 904.1µs | 1.80ms | 2.0x |
| GroupBy Mean | 820.5µs | 1.69ms | 2.1x |
| Inner Join | 1.80ms | 9.83ms | 5.4x |
| Left Join | 2.20ms | 9.47ms | 4.3x |

### 1,000,000 Rows

| Operation | Polars | Pandas | Polars Speedup |
|-----------|--------|--------|----------------|
| Sum | 83.1µs | 251.4µs | 3.0x |
| Min | 79.4µs | 453.8µs | 5.7x |
| Max | 81.9µs | 418.7µs | 5.1x |
| Mean | 81.7µs | 438.0µs | 5.4x |
| Filter | 425.5µs | 3.34ms | 7.9x |
| Sort | 16.68ms | 86.25ms | 5.2x |
| GroupBy Sum | 6.82ms | 22.49ms | 3.3x |
| GroupBy Mean | 6.51ms | 22.66ms | 3.5x |
| Inner Join | 28.68ms | 152.97ms | 5.3x |
| Left Join | 31.78ms | 165.71ms | 5.2x |