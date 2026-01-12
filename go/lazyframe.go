package galleon

import (
	"fmt"
)

// LazyFrame represents a lazy DataFrame that builds a query plan
// Operations on LazyFrame don't execute immediately - they build a plan
// that gets optimized and executed when Collect() is called
type LazyFrame struct {
	plan *LogicalPlan
}

// ============================================================================
// LazyFrame Creation
// ============================================================================

// Lazy converts a DataFrame to a LazyFrame
func (df *DataFrame) Lazy() *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:   PlanScan,
			Data: df,
		},
	}
}

// ScanCSV creates a LazyFrame that will read a CSV file when collected
func ScanCSV(path string, opts ...CSVReadOptions) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:         PlanScanCSV,
			SourcePath: path,
			CSVOpts:    opts,
		},
	}
}

// ScanParquet creates a LazyFrame that will read a Parquet file when collected
func ScanParquet(path string, opts ...ParquetReadOptions) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:          PlanScanParquet,
			SourcePath:  path,
			ParquetOpts: opts,
		},
	}
}

// ScanJSON creates a LazyFrame that will read a JSON file when collected
func ScanJSON(path string, opts ...JSONReadOptions) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:         PlanScanJSON,
			SourcePath: path,
			JSONOpts:   opts,
		},
	}
}

// ============================================================================
// LazyFrame Operations
// ============================================================================

// Select projects specific columns or expressions
func (lf *LazyFrame) Select(exprs ...Expr) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:          PlanProject,
			Input:       lf.plan,
			Projections: exprs,
		},
	}
}

// Filter applies a filter predicate
func (lf *LazyFrame) Filter(predicate Expr) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:        PlanFilter,
			Input:     lf.plan,
			Predicate: predicate,
		},
	}
}

// WithColumn adds or replaces a column
func (lf *LazyFrame) WithColumn(name string, expr Expr) *LazyFrame {
	aliased := &AliasExpr{Inner: expr, AliasName: name}
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:         PlanWithColumn,
			Input:      lf.plan,
			NewColName: name,
			NewColExpr: aliased,
		},
	}
}

// GroupBy starts a lazy group by operation
func (lf *LazyFrame) GroupBy(keys ...string) *LazyGroupBy {
	keyExprs := make([]Expr, len(keys))
	for i, k := range keys {
		keyExprs[i] = Col(k)
	}
	return &LazyGroupBy{
		input:    lf,
		keyExprs: keyExprs,
	}
}

// Join performs a lazy join with another LazyFrame
func (lf *LazyFrame) Join(other *LazyFrame, opts JoinOptions) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:       PlanJoin,
			Input:    lf.plan,
			Right:    other.plan,
			JoinType: InnerJoin,
			JoinOpts: opts,
		},
	}
}

// LeftJoin performs a lazy left join
func (lf *LazyFrame) LeftJoin(other *LazyFrame, opts JoinOptions) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:       PlanJoin,
			Input:    lf.plan,
			Right:    other.plan,
			JoinType: LeftJoin,
			JoinOpts: opts,
		},
	}
}

// Sort sorts by the specified columns
func (lf *LazyFrame) Sort(column string, ascending bool) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:            PlanSort,
			Input:         lf.plan,
			SortColumn:    column,
			SortAscending: ascending,
		},
	}
}

// Head limits to first n rows
func (lf *LazyFrame) Head(n int) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:    PlanLimit,
			Input: lf.plan,
			Limit: n,
		},
	}
}

// Tail limits to last n rows
func (lf *LazyFrame) Tail(n int) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:       PlanTail,
			Input:    lf.plan,
			TailRows: n,
		},
	}
}

// Distinct removes duplicate rows
func (lf *LazyFrame) Distinct() *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:    PlanDistinct,
			Input: lf.plan,
		},
	}
}

// ============================================================================
// Pivot/Melt Operations
// ============================================================================

// PivotOptions configures a pivot operation
type PivotOptions struct {
	Index  string  // Column to use as row identifier
	Column string  // Column whose values become new column names
	Values string  // Column whose values populate the new columns
	AggFn  AggType // Aggregation function for duplicate values (default: AggTypeFirst)
}

// Pivot reshapes data from long to wide format.
// It creates new columns based on unique values in the specified column.
// Example:
//
//	df.Lazy().Pivot(PivotOptions{
//	    Index:  "date",
//	    Column: "metric",
//	    Values: "value",
//	    AggFn:  AggTypeSum,
//	})
func (lf *LazyFrame) Pivot(opts PivotOptions) *LazyFrame {
	aggFn := opts.AggFn
	if aggFn == 0 {
		aggFn = AggTypeFirst // Default to first
	}
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:          PlanPivot,
			Input:       lf.plan,
			PivotIndex:  opts.Index,
			PivotColumn: opts.Column,
			PivotValues: opts.Values,
			PivotAggFn:  aggFn,
		},
	}
}

// MeltOptions configures a melt operation
type MeltOptions struct {
	IDVars    []string // Columns to keep as identifier variables
	ValueVars []string // Columns to unpivot (if empty, uses all non-ID columns)
	VarName   string   // Name for the variable column (default: "variable")
	ValueName string   // Name for the value column (default: "value")
}

// Melt reshapes data from wide to long format.
// It unpivots columns into rows.
// Example:
//
//	df.Lazy().Melt(MeltOptions{
//	    IDVars:    []string{"id", "date"},
//	    ValueVars: []string{"temp", "humidity", "pressure"},
//	    VarName:   "metric",
//	    ValueName: "reading",
//	})
func (lf *LazyFrame) Melt(opts MeltOptions) *LazyFrame {
	varName := opts.VarName
	if varName == "" {
		varName = "variable"
	}
	valueName := opts.ValueName
	if valueName == "" {
		valueName = "value"
	}
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:            PlanMelt,
			Input:         lf.plan,
			MeltIDVars:    opts.IDVars,
			MeltValueVars: opts.ValueVars,
			MeltVarName:   varName,
			MeltValueName: valueName,
		},
	}
}

// ============================================================================
// Cache Operation
// ============================================================================

// Cache materializes the intermediate result for reuse.
// This is useful when the same intermediate result is used multiple times.
// The cached result is computed once and reused in subsequent operations.
func (lf *LazyFrame) Cache() *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:       PlanCache,
			Input:    lf.plan,
			IsCached: true,
		},
	}
}

// ============================================================================
// Apply (UDF) Operation
// ============================================================================

// Apply applies a user-defined function to a column.
// The function receives a Series and should return a new Series.
// Example:
//
//	df.Lazy().Apply("price", func(s *Series) (*Series, error) {
//	    // Custom transformation logic
//	    data := s.Float64()
//	    result := make([]float64, len(data))
//	    for i, v := range data {
//	        result[i] = v * 1.1  // 10% markup
//	    }
//	    return NewSeriesFloat64(s.Name(), result), nil
//	})
func (lf *LazyFrame) Apply(column string, fn func(*Series) (*Series, error)) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:        PlanApply,
			Input:     lf.plan,
			ApplyCol:  column,
			ApplyFunc: fn,
		},
	}
}

// ApplyAll applies a user-defined function to the entire DataFrame.
// This is useful for transformations that need access to multiple columns.
func (lf *LazyFrame) ApplyAll(fn func(*DataFrame) (*DataFrame, error)) *LazyFrame {
	// Wrap the DataFrame function as a column-level function that processes all
	// We store this as a special case with empty column name
	wrappedFn := func(s *Series) (*Series, error) {
		// This will be handled specially in the executor
		return s, nil
	}
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:        PlanApply,
			Input:     lf.plan,
			ApplyCol:  "", // Empty indicates full DataFrame apply
			ApplyFunc: wrappedFn,
		},
	}
}

// ============================================================================
// Collect - Execute the plan
// ============================================================================

// Collect executes the query plan and returns a DataFrame
func (lf *LazyFrame) Collect() (*DataFrame, error) {
	// Optimize the plan first
	optimized := optimizePlan(lf.plan)

	// Execute the optimized plan
	return executePlan(optimized)
}

// Describe shows the query plan (for debugging)
func (lf *LazyFrame) Describe() string {
	return describePlan(lf.plan, 0)
}

// Explain shows the optimized query plan
func (lf *LazyFrame) Explain() string {
	optimized := optimizePlan(lf.plan)
	return describePlan(optimized, 0)
}

// ============================================================================
// LazyGroupBy
// ============================================================================

// LazyGroupBy represents a lazy group by operation
type LazyGroupBy struct {
	input    *LazyFrame
	keyExprs []Expr
}

// Agg applies aggregations to the grouped data
func (lgb *LazyGroupBy) Agg(aggs ...Expr) *LazyFrame {
	return &LazyFrame{
		plan: &LogicalPlan{
			Op:           PlanGroupBy,
			Input:        lgb.input.plan,
			GroupByKeys:  lgb.keyExprs,
			Aggregations: aggs,
		},
	}
}

// Sum aggregates with sum
func (lgb *LazyGroupBy) Sum(column string) *LazyFrame {
	return lgb.Agg(Col(column).Sum().Alias(column))
}

// Mean aggregates with mean
func (lgb *LazyGroupBy) Mean(column string) *LazyFrame {
	return lgb.Agg(Col(column).Mean().Alias(column))
}

// Min aggregates with min
func (lgb *LazyGroupBy) Min(column string) *LazyFrame {
	return lgb.Agg(Col(column).Min().Alias(column))
}

// Max aggregates with max
func (lgb *LazyGroupBy) Max(column string) *LazyFrame {
	return lgb.Agg(Col(column).Max().Alias(column))
}

// Count aggregates with count
func (lgb *LazyGroupBy) Count() *LazyFrame {
	return lgb.Agg(ExprCount().Alias("count"))
}

// ============================================================================
// Logical Plan
// ============================================================================

// PlanOp represents the type of logical plan operation
type PlanOp int

const (
	PlanScan PlanOp = iota
	PlanScanCSV
	PlanScanParquet
	PlanScanJSON
	PlanProject
	PlanFilter
	PlanWithColumn
	PlanGroupBy
	PlanJoin
	PlanSort
	PlanLimit
	PlanTail
	PlanDistinct
	PlanPivot
	PlanMelt
	PlanCache
	PlanApply
)

func (op PlanOp) String() string {
	switch op {
	case PlanScan:
		return "Scan"
	case PlanScanCSV:
		return "ScanCSV"
	case PlanScanParquet:
		return "ScanParquet"
	case PlanScanJSON:
		return "ScanJSON"
	case PlanProject:
		return "Project"
	case PlanFilter:
		return "Filter"
	case PlanWithColumn:
		return "WithColumn"
	case PlanGroupBy:
		return "GroupBy"
	case PlanJoin:
		return "Join"
	case PlanSort:
		return "Sort"
	case PlanLimit:
		return "Limit"
	case PlanTail:
		return "Tail"
	case PlanDistinct:
		return "Distinct"
	case PlanPivot:
		return "Pivot"
	case PlanMelt:
		return "Melt"
	case PlanCache:
		return "Cache"
	case PlanApply:
		return "Apply"
	default:
		return "Unknown"
	}
}

// LogicalPlan represents a node in the query plan tree
type LogicalPlan struct {
	Op    PlanOp
	Input *LogicalPlan // Parent plan (for unary operations)
	Right *LogicalPlan // Right input (for joins)

	// Scan source
	Data        *DataFrame
	SourcePath  string
	CSVOpts     []CSVReadOptions
	ParquetOpts []ParquetReadOptions
	JSONOpts    []JSONReadOptions

	// Projection
	Projections []Expr

	// Filter
	Predicate Expr

	// WithColumn
	NewColName string
	NewColExpr Expr

	// GroupBy
	GroupByKeys  []Expr
	Aggregations []Expr

	// Join
	JoinType JoinType
	JoinOpts JoinOptions

	// Sort
	SortColumn    string
	SortAscending bool

	// Limit/Tail
	Limit    int
	TailRows int

	// Pivot configuration
	PivotIndex  string   // Column to use as row identifier
	PivotColumn string   // Column whose values become new column names
	PivotValues string   // Column whose values populate the new columns
	PivotAggFn  AggType  // Aggregation function for duplicate values

	// Melt configuration
	MeltIDVars    []string // Columns to keep as identifier variables
	MeltValueVars []string // Columns to unpivot
	MeltVarName   string   // Name for the variable column (default: "variable")
	MeltValueName string   // Name for the value column (default: "value")

	// Apply (UDF) configuration
	ApplyFunc func(*Series) (*Series, error) // User-defined function
	ApplyCol  string                          // Column to apply function to

	// Cache flag
	IsCached bool
}

// describePlan returns a string representation of the plan
func describePlan(plan *LogicalPlan, indent int) string {
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	var result string

	switch plan.Op {
	case PlanScan:
		h, w := 0, 0
		if plan.Data != nil {
			h, w = plan.Data.Height(), plan.Data.Width()
		}
		result = fmt.Sprintf("%s%s [%d rows × %d cols]\n", prefix, plan.Op, h, w)

	case PlanScanCSV, PlanScanParquet, PlanScanJSON:
		result = fmt.Sprintf("%s%s path=%q\n", prefix, plan.Op, plan.SourcePath)

	case PlanProject:
		result = fmt.Sprintf("%s%s %v\n", prefix, plan.Op, plan.Projections)

	case PlanFilter:
		result = fmt.Sprintf("%s%s %s\n", prefix, plan.Op, plan.Predicate)

	case PlanWithColumn:
		result = fmt.Sprintf("%s%s %s = %s\n", prefix, plan.Op, plan.NewColName, plan.NewColExpr)

	case PlanGroupBy:
		result = fmt.Sprintf("%s%s keys=%v aggs=%v\n", prefix, plan.Op, plan.GroupByKeys, plan.Aggregations)

	case PlanJoin:
		result = fmt.Sprintf("%s%s type=%v on=%v\n", prefix, plan.Op, plan.JoinType, plan.JoinOpts.on)

	case PlanSort:
		result = fmt.Sprintf("%s%s col=%q asc=%v\n", prefix, plan.Op, plan.SortColumn, plan.SortAscending)

	case PlanLimit:
		result = fmt.Sprintf("%s%s n=%d\n", prefix, plan.Op, plan.Limit)

	case PlanTail:
		result = fmt.Sprintf("%s%s n=%d\n", prefix, plan.Op, plan.TailRows)

	case PlanDistinct:
		result = fmt.Sprintf("%s%s\n", prefix, plan.Op)

	case PlanPivot:
		result = fmt.Sprintf("%s%s index=%q column=%q values=%q agg=%s\n",
			prefix, plan.Op, plan.PivotIndex, plan.PivotColumn, plan.PivotValues, plan.PivotAggFn)

	case PlanMelt:
		result = fmt.Sprintf("%s%s id_vars=%v value_vars=%v var_name=%q value_name=%q\n",
			prefix, plan.Op, plan.MeltIDVars, plan.MeltValueVars, plan.MeltVarName, plan.MeltValueName)

	case PlanCache:
		result = fmt.Sprintf("%s%s\n", prefix, plan.Op)

	case PlanApply:
		result = fmt.Sprintf("%s%s col=%q\n", prefix, plan.Op, plan.ApplyCol)

	default:
		result = fmt.Sprintf("%s%s\n", prefix, plan.Op)
	}

	if plan.Input != nil {
		result += describePlan(plan.Input, indent+1)
	}
	if plan.Right != nil {
		result += describePlan(plan.Right, indent+1)
	}

	return result
}
