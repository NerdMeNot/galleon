# Window Functions Guide

Window functions perform calculations across rows related to the current row without collapsing the result set. They're essential for time series analysis, ranking, and cumulative calculations.

## Overview

### What are Window Functions?

Window functions operate on a "window" of rows and return a value for each row based on that window. Unlike aggregate functions with GROUP BY, window functions maintain all rows in the result.

**Regular aggregation (GROUP BY):**
```
Input:  5 rows
Output: 2 rows (grouped)
```

**Window function:**
```
Input:  5 rows
Output: 5 rows (all preserved)
```

## Shift Operations

### Lag

Access values from previous rows.

```go
// Previous day's closing price
df.Lazy().
    WithColumn("prev_close", galleon.Col("close").Lag(1, 0.0)).
    Collect()
```

**Example:**
```
| date     | close | prev_close |
|----------|-------|------------|
| 2024-01  | 100   | 0          |  ← default value
| 2024-02  | 105   | 100        |  ← previous row
| 2024-03  | 103   | 105        |
```

**Parameters:**
- `offset`: Number of rows to look back (positive integer)
- `defaultValue`: Value for rows without previous data

### Lead

Access values from following rows.

```go
// Next day's opening price
df.Lazy().
    WithColumn("next_open", galleon.Col("open").Lead(1, 0.0)).
    Collect()
```

**Example:**
```
| date     | open | next_open |
|----------|------|-----------|
| 2024-01  | 98   | 104       |  ← next row
| 2024-02  | 104  | 102       |
| 2024-03  | 102  | 0         |  ← default value
```

## Difference Operations

### Diff

Calculate difference from previous row (1 period).

```go
// Daily price change
df.Lazy().
    WithColumn("price_change", galleon.Col("close").Diff()).
    Collect()
```

**Example:**
```
| date     | close | price_change |
|----------|-------|--------------|
| 2024-01  | 100   | NaN          |  ← no previous value
| 2024-02  | 105   | 5            |  ← 105 - 100
| 2024-03  | 103   | -2           |  ← 103 - 105
```

### DiffN

Calculate difference with n periods offset.

```go
// Week-over-week change (7 days)
df.Lazy().
    WithColumn("wow_change", galleon.Col("sales").DiffN(7)).
    Collect()
```

### PctChange

Calculate percentage change from previous row.

```go
// Daily return percentage
df.Lazy().
    WithColumn("daily_return", galleon.Col("close").PctChange()).
    Collect()
```

**Example:**
```
| date     | close | daily_return |
|----------|-------|--------------|
| 2024-01  | 100   | NaN          |
| 2024-02  | 105   | 0.05         |  ← (105-100)/100 = 5%
| 2024-03  | 103   | -0.019       |  ← (103-105)/105 = -1.9%
```

## Cumulative Functions

Cumulative functions compute running aggregates from the start of the dataset.

### CumSum

Cumulative sum (running total).

```go
// Running total of sales
df.Lazy().
    WithColumn("running_total", galleon.Col("sales").CumSum()).
    Collect()
```

**Example:**
```
| date     | sales | running_total |
|----------|-------|---------------|
| 2024-01  | 100   | 100           |
| 2024-02  | 150   | 250           |  ← 100 + 150
| 2024-03  | 120   | 370           |  ← 100 + 150 + 120
```

### CumMin

Cumulative minimum.

```go
// Lowest price seen so far
df.Lazy().
    WithColumn("min_so_far", galleon.Col("price").CumMin()).
    Collect()
```

**Example:**
```
| date     | price | min_so_far |
|----------|-------|------------|
| 2024-01  | 105   | 105        |
| 2024-02  | 103   | 103        |  ← min(105, 103)
| 2024-03  | 107   | 103        |  ← min(105, 103, 107)
```

### CumMax

Cumulative maximum.

```go
// Highest price seen so far
df.Lazy().
    WithColumn("max_so_far", galleon.Col("price").CumMax()).
    Collect()
```

## Rolling Window Functions

Rolling functions compute aggregates over a sliding window of fixed size.

### RollingSum

Rolling window sum.

```go
// 7-day rolling sum
df.Lazy().
    WithColumn("rolling_7d_sum",
        galleon.Col("sales").RollingSum(7, 1),
    ).
    Collect()
```

**Parameters:**
- `windowSize`: Number of rows in the window
- `minPeriods`: Minimum number of non-null values required

**Example (window=3):**
```
| date     | sales | rolling_sum |
|----------|-------|-------------|
| 2024-01  | 10    | 10          |  ← only 1 value
| 2024-02  | 20    | 30          |  ← sum of 2 values
| 2024-03  | 30    | 60          |  ← sum of 3 values
| 2024-04  | 40    | 90          |  ← sum of [20,30,40]
| 2024-05  | 50    | 120         |  ← sum of [30,40,50]
```

### RollingMean

Rolling window average.

```go
// 30-day moving average
df.Lazy().
    WithColumn("ma30",
        galleon.Col("price").RollingMean(30, 20),
    ).
    Collect()
```

**Common use: Moving averages for trend analysis**
```go
// Simple Moving Average (SMA) strategy
result, _ := df.Lazy().
    WithColumn("sma20", galleon.Col("close").RollingMean(20, 15)).
    WithColumn("sma50", galleon.Col("close").RollingMean(50, 40)).
    // Buy signal: short-term MA crosses above long-term MA
    WithColumn("signal",
        galleon.Col("sma20").Gt(galleon.Col("sma50")).Cast(galleon.Int64),
    ).
    Collect()
```

## Practical Examples

### Time Series Analysis

```go
// Comprehensive time series features
result, _ := df.Lazy().
    Sort(galleon.Col("date"), true).
    // Price changes
    WithColumn("price_change", galleon.Col("close").Diff()).
    WithColumn("price_pct_change", galleon.Col("close").PctChange()).
    // Moving averages
    WithColumn("sma10", galleon.Col("close").RollingMean(10, 7)).
    WithColumn("sma30", galleon.Col("close").RollingMean(30, 20)).
    // Volatility (rolling std)
    WithColumn("volatility", galleon.Col("close").RollingStd(20, 15)).
    // Running statistics
    WithColumn("cumulative_return",
        galleon.Col("close").Div(galleon.Col("close").First()).Sub(galleon.Lit(1.0)),
    ).
    WithColumn("max_price_so_far", galleon.Col("close").CumMax()).
    // Drawdown
    WithColumn("drawdown",
        galleon.Col("close").Sub(galleon.Col("max_price_so_far")).
            Div(galleon.Col("max_price_so_far")),
    ).
    Collect()
```

### Sales Analytics

```go
// Sales performance metrics
result, _ := df.Lazy().
    Sort(galleon.Col("date"), true).
    // Running totals
    WithColumn("ytd_sales", galleon.Col("daily_sales").CumSum()).
    // Moving averages
    WithColumn("trailing_7d_avg",
        galleon.Col("daily_sales").RollingMean(7, 1),
    ).
    WithColumn("trailing_30d_avg",
        galleon.Col("daily_sales").RollingMean(30, 7),
    ).
    // Growth rates
    WithColumn("dow_growth",  // Day-over-day
        galleon.Col("daily_sales").PctChange(),
    ).
    WithColumn("wow_growth",  // Week-over-week
        galleon.Col("daily_sales").DiffN(7).
            Div(galleon.Col("daily_sales").Lag(7, 1.0)),
    ).
    Collect()
```

### Customer Behavior

```go
// Customer engagement metrics
result, _ := df.Lazy().
    Sort(galleon.Col("user_id"), galleon.Col("timestamp"), true).
    // Days since last purchase
    WithColumn("days_since_last",
        galleon.Col("timestamp").Diff(),
    ).
    // Purchase frequency (rolling 30-day count)
    WithColumn("purchase_freq_30d",
        galleon.Lit(1).RollingSum(30, 1),
    ).
    // Cumulative spend
    WithColumn("total_spend", galleon.Col("amount").CumSum()).
    // Average order value (rolling)
    WithColumn("aov_rolling_10",
        galleon.Col("amount").RollingMean(10, 1),
    ).
    Collect()
```

### Anomaly Detection

```go
// Detect anomalies using rolling statistics
result, _ := df.Lazy().
    // Calculate rolling mean and std
    WithColumn("rolling_mean",
        galleon.Col("value").RollingMean(30, 20),
    ).
    WithColumn("rolling_std",
        galleon.Col("value").RollingStd(30, 20),
    ).
    // Calculate z-score
    WithColumn("z_score",
        galleon.Col("value").Sub(galleon.Col("rolling_mean")).
            Div(galleon.Col("rolling_std")),
    ).
    // Flag anomalies (|z| > 3)
    WithColumn("is_anomaly",
        galleon.Col("z_score").Abs().Gt(galleon.Lit(3.0)),
    ).
    Collect()
```

### Financial Indicators

```go
// Technical indicators for trading
result, _ := df.Lazy().
    Sort(galleon.Col("date"), true).
    // Exponential Moving Averages (approximation using SMA)
    WithColumn("ema12", galleon.Col("close").RollingMean(12, 8)).
    WithColumn("ema26", galleon.Col("close").RollingMean(26, 18)).
    // MACD
    WithColumn("macd",
        galleon.Col("ema12").Sub(galleon.Col("ema26")),
    ).
    // Signal line (9-period MA of MACD)
    WithColumn("signal",
        galleon.Col("macd").RollingMean(9, 6),
    ).
    // RSI (Relative Strength Index) components
    WithColumn("gain",
        galleon.Col("close").Diff().Max(galleon.Lit(0.0)),
    ).
    WithColumn("loss",
        galleon.Col("close").Diff().Min(galleon.Lit(0.0)).Abs(),
    ).
    WithColumn("avg_gain",
        galleon.Col("gain").RollingMean(14, 10),
    ).
    WithColumn("avg_loss",
        galleon.Col("loss").RollingMean(14, 10),
    ).
    WithColumn("rsi",
        galleon.Lit(100).Sub(
            galleon.Lit(100).Div(
                galleon.Lit(1).Add(
                    galleon.Col("avg_gain").Div(galleon.Col("avg_loss")),
                ),
            ),
        ),
    ).
    Collect()
```

## Performance Considerations

### 1. Sort Before Window Functions

Window functions work sequentially, so sorting is important:

```go
// Always sort before applying window functions
df.Lazy().
    Sort(galleon.Col("date"), true).  // Ascending order
    WithColumn("rolling_avg", galleon.Col("value").RollingMean(7, 1)).
    Collect()
```

### 2. Choose Appropriate Window Sizes

```go
// Smaller windows = faster computation
// For daily data:
// - Short-term: 7-14 days
// - Medium-term: 20-30 days
// - Long-term: 50-200 days

df.Lazy().
    WithColumn("short_term", galleon.Col("price").RollingMean(10, 7)).
    WithColumn("long_term", galleon.Col("price").RollingMean(50, 40)).
    Collect()
```

### 3. Use minPeriods Wisely

```go
// minPeriods controls when results start appearing
// Higher minPeriods = fewer early nulls, but loses data

// Strict: require full window
df.Lazy().
    WithColumn("ma30_strict", galleon.Col("value").RollingMean(30, 30)).
    Collect()

// Lenient: start computing from first value
df.Lazy().
    WithColumn("ma30_lenient", galleon.Col("value").RollingMean(30, 1)).
    Collect()
```

## Comparison with SQL

### SQL Window Functions

```sql
SELECT
    date,
    close,
    LAG(close, 1) OVER (ORDER BY date) as prev_close,
    SUM(sales) OVER (ORDER BY date) as running_total,
    AVG(price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma7
FROM trades
```

### Galleon Equivalent

```go
df.Lazy().
    Sort(galleon.Col("date"), true).
    WithColumn("prev_close", galleon.Col("close").Lag(1, 0.0)).
    WithColumn("running_total", galleon.Col("sales").CumSum()).
    WithColumn("ma7", galleon.Col("price").RollingMean(7, 1)).
    Collect()
```

## Best Practices

### 1. Order Matters

```go
// Always sort by the dimension you're windowing over
df.Lazy().
    Sort(galleon.Col("timestamp"), true).  // Essential for time-based windows
    WithColumn("rolling_sum", galleon.Col("value").RollingSum(7, 1)).
    Collect()
```

### 2. Handle Nulls

```go
// Use minPeriods to handle nulls gracefully
df.Lazy().
    WithColumn("rolling_mean",
        galleon.Col("value").RollingMean(
            30,   // window size
            20,   // require at least 20 non-null values
        ),
    ).
    Collect()
```

### 3. Combine with Filtering

```go
// Filter after window calculations to keep all context
df.Lazy().
    Sort(galleon.Col("date"), true).
    WithColumn("ma50", galleon.Col("close").RollingMean(50, 40)).
    // Filter AFTER calculating MA (needs full history)
    Filter(galleon.Col("date").Gte(galleon.Lit("2024-01-01"))).
    Collect()
```

## API Reference

### Shift Functions

```go
func (e Expr) Lag(offset int, defaultValue interface{}) Expr
func (e Expr) Lead(offset int, defaultValue interface{}) Expr
```

### Difference Functions

```go
func (e Expr) Diff() Expr
func (e Expr) DiffN(n int) Expr
func (e Expr) PctChange() Expr
```

### Cumulative Functions

```go
func (e Expr) CumSum() Expr
func (e Expr) CumMin() Expr
func (e Expr) CumMax() Expr
```

### Rolling Functions

```go
func (e Expr) RollingSum(windowSize int, minPeriods int) Expr
func (e Expr) RollingMean(windowSize int, minPeriods int) Expr
```
