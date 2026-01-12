package galleon

import (
	"fmt"
)

// ============================================================================
// Plan Executor
// ============================================================================

// executePlan executes a logical plan and returns the resulting DataFrame
func executePlan(plan *LogicalPlan) (*DataFrame, error) {
	if plan == nil {
		return NewDataFrame()
	}

	switch plan.Op {
	case PlanScan:
		return executeScan(plan)

	case PlanScanCSV:
		return executeScanCSV(plan)

	case PlanScanParquet:
		return executeScanParquet(plan)

	case PlanScanJSON:
		return executeScanJSON(plan)

	case PlanProject:
		return executeProject(plan)

	case PlanFilter:
		return executeFilter(plan)

	case PlanWithColumn:
		return executeWithColumn(plan)

	case PlanGroupBy:
		return executeGroupBy(plan)

	case PlanJoin:
		return executeJoin(plan)

	case PlanSort:
		return executeSort(plan)

	case PlanLimit:
		return executeLimit(plan)

	case PlanTail:
		return executeTail(plan)

	case PlanDistinct:
		return executeDistinct(plan)

	case PlanPivot:
		return executePivot(plan)

	case PlanMelt:
		return executeMelt(plan)

	case PlanCache:
		return executeCache(plan)

	case PlanApply:
		return executeApply(plan)

	default:
		return nil, fmt.Errorf("unknown plan operation: %v", plan.Op)
	}
}

// ============================================================================
// Scan Operations
// ============================================================================

func executeScan(plan *LogicalPlan) (*DataFrame, error) {
	if plan.Data == nil {
		return NewDataFrame()
	}
	return plan.Data, nil
}

func executeScanCSV(plan *LogicalPlan) (*DataFrame, error) {
	var opts []CSVReadOptions
	if len(plan.CSVOpts) > 0 {
		opts = plan.CSVOpts
	}
	return ReadCSV(plan.SourcePath, opts...)
}

func executeScanParquet(plan *LogicalPlan) (*DataFrame, error) {
	var opts []ParquetReadOptions
	if len(plan.ParquetOpts) > 0 {
		opts = plan.ParquetOpts
	}
	return ReadParquet(plan.SourcePath, opts...)
}

func executeScanJSON(plan *LogicalPlan) (*DataFrame, error) {
	var opts []JSONReadOptions
	if len(plan.JSONOpts) > 0 {
		opts = plan.JSONOpts
	}
	return ReadJSON(plan.SourcePath, opts...)
}

// ============================================================================
// Projection
// ============================================================================

func executeProject(plan *LogicalPlan) (*DataFrame, error) {
	// Execute input
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Evaluate projections
	columns := make([]*Series, 0, len(plan.Projections))
	for _, expr := range plan.Projections {
		col, err := evaluateExpr(expr, df)
		if err != nil {
			return nil, fmt.Errorf("projection error: %w", err)
		}
		columns = append(columns, col)
	}

	return NewDataFrame(columns...)
}

// ============================================================================
// Filter
// ============================================================================

func executeFilter(plan *LogicalPlan) (*DataFrame, error) {
	// Execute input
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Evaluate predicate to get mask
	mask, err := evaluatePredicate(plan.Predicate, df)
	if err != nil {
		return nil, fmt.Errorf("filter error: %w", err)
	}

	return df.FilterByMask(mask)
}

// ============================================================================
// WithColumn
// ============================================================================

func executeWithColumn(plan *LogicalPlan) (*DataFrame, error) {
	// Execute input
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Evaluate expression
	col, err := evaluateExpr(plan.NewColExpr, df)
	if err != nil {
		return nil, fmt.Errorf("with_column error: %w", err)
	}

	return df.WithColumnSeries(col)
}

// ============================================================================
// GroupBy
// ============================================================================

func executeGroupBy(plan *LogicalPlan) (*DataFrame, error) {
	// Execute input
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Extract key column names
	keyNames := make([]string, len(plan.GroupByKeys))
	for i, expr := range plan.GroupByKeys {
		if colExpr, ok := expr.(*ColExpr); ok {
			keyNames[i] = colExpr.Name
		} else {
			return nil, fmt.Errorf("groupby keys must be column references")
		}
	}

	// Convert lazy aggregations to eager aggregations
	aggs := make([]Aggregation, 0, len(plan.Aggregations))
	for _, expr := range plan.Aggregations {
		agg, err := exprToAggregation(expr)
		if err != nil {
			return nil, fmt.Errorf("aggregation error: %w", err)
		}
		aggs = append(aggs, agg)
	}

	// Execute groupby
	gb := df.GroupBy(keyNames...)
	return gb.Agg(aggs...)
}

// exprToAggregation converts an expression to an eager Aggregation
func exprToAggregation(expr Expr) (Aggregation, error) {
	switch e := expr.(type) {
	case *AliasExpr:
		inner, err := exprToAggregation(e.Inner)
		if err != nil {
			return Aggregation{}, err
		}
		return inner.Alias(e.AliasName), nil

	case *AggExpr:
		// Get the column name
		var colName string
		if colExpr, ok := e.Input.(*ColExpr); ok {
			colName = colExpr.Name
		} else if _, ok := e.Input.(*LitExpr); ok && e.AggType == AggTypeCount {
			// count(*) case
			colName = ""
		} else {
			return Aggregation{}, fmt.Errorf("aggregation input must be a column")
		}

		// Use the existing aggregation constructors
		switch e.AggType {
		case AggTypeSum:
			return AggSum(colName), nil
		case AggTypeMean:
			return AggMean(colName), nil
		case AggTypeMin:
			return AggMin(colName), nil
		case AggTypeMax:
			return AggMax(colName), nil
		case AggTypeCount:
			return AggCount(), nil
		case AggTypeFirst:
			return AggFirst(colName), nil
		case AggTypeLast:
			return AggLast(colName), nil
		case AggTypeStd:
			return AggStd(colName), nil
		case AggTypeVar:
			return AggVar(colName), nil
		default:
			return Aggregation{}, fmt.Errorf("unknown aggregation type")
		}

	default:
		return Aggregation{}, fmt.Errorf("expected aggregation expression, got %T", expr)
	}
}

// ============================================================================
// Join
// ============================================================================

func executeJoin(plan *LogicalPlan) (*DataFrame, error) {
	// Execute both inputs
	left, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	right, err := executePlan(plan.Right)
	if err != nil {
		return nil, err
	}

	// Execute appropriate join type
	switch plan.JoinType {
	case InnerJoin:
		return left.Join(right, plan.JoinOpts)
	case LeftJoin:
		return left.LeftJoin(right, plan.JoinOpts)
	case RightJoin:
		return left.RightJoin(right, plan.JoinOpts)
	case OuterJoin:
		return left.OuterJoin(right, plan.JoinOpts)
	default:
		return nil, fmt.Errorf("unsupported join type: %v", plan.JoinType)
	}
}

// ============================================================================
// Sort
// ============================================================================

func executeSort(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	return df.SortBy(plan.SortColumn, plan.SortAscending)
}

// ============================================================================
// Limit / Tail
// ============================================================================

func executeLimit(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	return df.Head(plan.Limit), nil
}

func executeTail(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	return df.Tail(plan.TailRows), nil
}

// ============================================================================
// Distinct
// ============================================================================

func executeDistinct(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Simple distinct by using groupby on all columns
	cols := df.Columns()
	if len(cols) == 0 {
		return df, nil
	}

	gb := df.GroupBy(cols...)
	return gb.First(cols[0])
}

// ============================================================================
// Pivot
// ============================================================================

func executePivot(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Get the columns
	indexCol := df.ColumnByName(plan.PivotIndex)
	if indexCol == nil {
		return nil, fmt.Errorf("pivot index column '%s' not found", plan.PivotIndex)
	}

	pivotCol := df.ColumnByName(plan.PivotColumn)
	if pivotCol == nil {
		return nil, fmt.Errorf("pivot column '%s' not found", plan.PivotColumn)
	}

	valuesCol := df.ColumnByName(plan.PivotValues)
	if valuesCol == nil {
		return nil, fmt.Errorf("pivot values column '%s' not found", plan.PivotValues)
	}

	// Get unique values in the pivot column to create new column names
	uniquePivotValues := make(map[string]bool)
	pivotStrings := make([]string, pivotCol.Len())
	for i := 0; i < pivotCol.Len(); i++ {
		s := fmt.Sprintf("%v", pivotCol.Get(i))
		pivotStrings[i] = s
		uniquePivotValues[s] = true
	}

	// Sort unique values for consistent column order
	sortedPivotVals := make([]string, 0, len(uniquePivotValues))
	for v := range uniquePivotValues {
		sortedPivotVals = append(sortedPivotVals, v)
	}

	// Get unique index values and their mapping
	uniqueIndices := make(map[string]int)
	indexStrings := make([]string, indexCol.Len())
	for i := 0; i < indexCol.Len(); i++ {
		s := fmt.Sprintf("%v", indexCol.Get(i))
		indexStrings[i] = s
		if _, exists := uniqueIndices[s]; !exists {
			uniqueIndices[s] = len(uniqueIndices)
		}
	}

	// Create output columns
	numRows := len(uniqueIndices)
	numCols := len(sortedPivotVals)

	// Initialize result data - one column per unique pivot value
	resultData := make([][]float64, numCols)
	resultCounts := make([][]int, numCols) // For aggregation
	for i := range resultData {
		resultData[i] = make([]float64, numRows)
		resultCounts[i] = make([]int, numRows)
	}

	// Build index column for result
	indexData := make([]string, numRows)
	for s, idx := range uniqueIndices {
		indexData[idx] = s
	}

	// Create column name to index mapping
	pivotColIdx := make(map[string]int)
	for i, v := range sortedPivotVals {
		pivotColIdx[v] = i
	}

	// Populate the pivot table
	for i := 0; i < df.Height(); i++ {
		rowIdx := uniqueIndices[indexStrings[i]]
		colIdx := pivotColIdx[pivotStrings[i]]

		var val float64
		switch valuesCol.DType() {
		case Float64:
			val = valuesCol.Float64()[i]
		case Int64:
			val = float64(valuesCol.Int64()[i])
		case Int32:
			val = float64(valuesCol.Int32()[i])
		case Float32:
			val = float64(valuesCol.Float32()[i])
		default:
			val = 0
		}

		// Apply aggregation
		switch plan.PivotAggFn {
		case AggTypeSum:
			resultData[colIdx][rowIdx] += val
		case AggTypeMean:
			resultData[colIdx][rowIdx] += val
			resultCounts[colIdx][rowIdx]++
		case AggTypeMin:
			if resultCounts[colIdx][rowIdx] == 0 || val < resultData[colIdx][rowIdx] {
				resultData[colIdx][rowIdx] = val
			}
			resultCounts[colIdx][rowIdx]++
		case AggTypeMax:
			if resultCounts[colIdx][rowIdx] == 0 || val > resultData[colIdx][rowIdx] {
				resultData[colIdx][rowIdx] = val
			}
			resultCounts[colIdx][rowIdx]++
		case AggTypeFirst:
			if resultCounts[colIdx][rowIdx] == 0 {
				resultData[colIdx][rowIdx] = val
			}
			resultCounts[colIdx][rowIdx]++
		case AggTypeLast:
			resultData[colIdx][rowIdx] = val
			resultCounts[colIdx][rowIdx]++
		case AggTypeCount:
			resultData[colIdx][rowIdx]++
		default:
			// Default to first
			if resultCounts[colIdx][rowIdx] == 0 {
				resultData[colIdx][rowIdx] = val
			}
			resultCounts[colIdx][rowIdx]++
		}
	}

	// Finalize mean aggregation
	if plan.PivotAggFn == AggTypeMean {
		for col := range resultData {
			for row := range resultData[col] {
				if resultCounts[col][row] > 0 {
					resultData[col][row] /= float64(resultCounts[col][row])
				}
			}
		}
	}

	// Build result DataFrame
	columns := make([]*Series, 1+numCols)
	columns[0] = NewSeriesString(plan.PivotIndex, indexData)
	for i, pivotVal := range sortedPivotVals {
		columns[i+1] = NewSeriesFloat64(pivotVal, resultData[i])
	}

	return NewDataFrame(columns...)
}

// ============================================================================
// Melt
// ============================================================================

func executeMelt(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	// Determine value vars if not specified
	valueVars := plan.MeltValueVars
	if len(valueVars) == 0 {
		// Use all columns except ID vars
		idSet := make(map[string]bool)
		for _, id := range plan.MeltIDVars {
			idSet[id] = true
		}
		for _, col := range df.Columns() {
			if !idSet[col] {
				valueVars = append(valueVars, col)
			}
		}
	}

	if len(valueVars) == 0 {
		return nil, fmt.Errorf("no columns to melt")
	}

	// Calculate result size
	numRows := df.Height() * len(valueVars)

	// Create ID columns for result (repeated for each value var)
	idColumns := make([]*Series, len(plan.MeltIDVars))
	for i, idVar := range plan.MeltIDVars {
		srcCol := df.ColumnByName(idVar)
		if srcCol == nil {
			return nil, fmt.Errorf("id column '%s' not found", idVar)
		}

		// Repeat each ID value for each value var
		switch srcCol.DType() {
		case String:
			data := make([]string, numRows)
			srcData := srcCol.Strings()
			for j := 0; j < len(valueVars); j++ {
				for k, v := range srcData {
					data[j*df.Height()+k] = v
				}
			}
			idColumns[i] = NewSeriesString(idVar, data)
		case Float64:
			data := make([]float64, numRows)
			srcData := srcCol.Float64()
			for j := 0; j < len(valueVars); j++ {
				for k, v := range srcData {
					data[j*df.Height()+k] = v
				}
			}
			idColumns[i] = NewSeriesFloat64(idVar, data)
		case Int64:
			data := make([]int64, numRows)
			srcData := srcCol.Int64()
			for j := 0; j < len(valueVars); j++ {
				for k, v := range srcData {
					data[j*df.Height()+k] = v
				}
			}
			idColumns[i] = NewSeriesInt64(idVar, data)
		default:
			// Generic handling via string conversion
			data := make([]string, numRows)
			for j := 0; j < len(valueVars); j++ {
				for k := 0; k < srcCol.Len(); k++ {
					data[j*df.Height()+k] = fmt.Sprintf("%v", srcCol.Get(k))
				}
			}
			idColumns[i] = NewSeriesString(idVar, data)
		}
	}

	// Create variable column
	varData := make([]string, numRows)
	for i, varName := range valueVars {
		for j := 0; j < df.Height(); j++ {
			varData[i*df.Height()+j] = varName
		}
	}
	varColumn := NewSeriesString(plan.MeltVarName, varData)

	// Create value column
	valueData := make([]float64, numRows)
	for i, varName := range valueVars {
		srcCol := df.ColumnByName(varName)
		if srcCol == nil {
			return nil, fmt.Errorf("value column '%s' not found", varName)
		}
		srcData := toFloat64Slice(srcCol)
		for j, v := range srcData {
			valueData[i*df.Height()+j] = v
		}
	}
	valueColumn := NewSeriesFloat64(plan.MeltValueName, valueData)

	// Build result DataFrame
	columns := make([]*Series, len(idColumns)+2)
	copy(columns, idColumns)
	columns[len(idColumns)] = varColumn
	columns[len(idColumns)+1] = valueColumn

	return NewDataFrame(columns...)
}

// ============================================================================
// Cache
// ============================================================================

// Global cache for cached intermediate results
var planCache = make(map[*LogicalPlan]*DataFrame)

func executeCache(plan *LogicalPlan) (*DataFrame, error) {
	// Check if already cached
	if cached, ok := planCache[plan.Input]; ok {
		return cached, nil
	}

	// Execute input and cache result
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	planCache[plan.Input] = df
	return df, nil
}

// ClearCache clears the plan cache
func ClearCache() {
	planCache = make(map[*LogicalPlan]*DataFrame)
}

// ============================================================================
// Apply (UDF)
// ============================================================================

func executeApply(plan *LogicalPlan) (*DataFrame, error) {
	df, err := executePlan(plan.Input)
	if err != nil {
		return nil, err
	}

	if plan.ApplyFunc == nil {
		return nil, fmt.Errorf("apply function is nil")
	}

	// If column name is empty, this is a full DataFrame apply (currently not supported)
	if plan.ApplyCol == "" {
		return nil, fmt.Errorf("full DataFrame apply not yet supported; use Apply with a column name")
	}

	// Get the column to apply the function to
	col := df.ColumnByName(plan.ApplyCol)
	if col == nil {
		return nil, fmt.Errorf("column '%s' not found", plan.ApplyCol)
	}

	// Apply the function
	resultCol, err := plan.ApplyFunc(col)
	if err != nil {
		return nil, fmt.Errorf("apply function error: %w", err)
	}

	// Replace the column in the DataFrame
	return df.WithColumnSeries(resultCol.Rename(plan.ApplyCol))
}

// ============================================================================
// Expression Evaluation
// ============================================================================

// evaluateExpr evaluates an expression on a DataFrame and returns a Series
func evaluateExpr(expr Expr, df *DataFrame) (*Series, error) {
	switch e := expr.(type) {
	case *ColExpr:
		col := df.ColumnByName(e.Name)
		if col == nil {
			return nil, fmt.Errorf("column '%s' not found", e.Name)
		}
		return col, nil

	case *LitExpr:
		// Create a series with the literal value repeated
		return createLiteralSeries("literal", e.Value, df.Height())

	case *AliasExpr:
		col, err := evaluateExpr(e.Inner, df)
		if err != nil {
			return nil, err
		}
		return col.Rename(e.AliasName), nil

	case *BinaryOpExpr:
		return evaluateBinaryOp(e, df)

	case *CastExpr:
		col, err := evaluateExpr(e.Inner, df)
		if err != nil {
			return nil, err
		}
		return castSeries(col, e.TargetType)

	case *allColsExpr:
		return nil, fmt.Errorf("cannot evaluate * directly; use in Select")

	default:
		return nil, fmt.Errorf("cannot evaluate expression type: %T", expr)
	}
}

// evaluateBinaryOp evaluates a binary operation
func evaluateBinaryOp(expr *BinaryOpExpr, df *DataFrame) (*Series, error) {
	left, err := evaluateExpr(expr.Left, df)
	if err != nil {
		return nil, err
	}

	// Check if right is a literal for scalar operations
	if lit, ok := expr.Right.(*LitExpr); ok {
		return evaluateScalarOp(left, expr.Op, lit.Value)
	}

	right, err := evaluateExpr(expr.Right, df)
	if err != nil {
		return nil, err
	}

	return evaluateVectorOp(left, expr.Op, right)
}

// evaluateScalarOp evaluates a scalar operation
func evaluateScalarOp(col *Series, op BinaryOp, scalar interface{}) (*Series, error) {
	scalarFloat, ok := lazyToFloat64(scalar)
	if !ok {
		return nil, fmt.Errorf("cannot convert scalar to float64")
	}

	switch op {
	case OpAdd:
		return col.Add(scalarFloat), nil
	case OpSub:
		return col.Add(-scalarFloat), nil
	case OpMul:
		return col.Mul(scalarFloat), nil
	case OpDiv:
		return col.Mul(1.0 / scalarFloat), nil
	case OpGt, OpLt, OpGte, OpLte, OpEq, OpNeq:
		// Return a comparison result as bool series
		return evaluateComparison(col, op, scalarFloat)
	default:
		return nil, fmt.Errorf("unsupported scalar operation: %s", op)
	}
}

// evaluateComparison evaluates a comparison and returns a bool series
func evaluateComparison(col *Series, op BinaryOp, threshold float64) (*Series, error) {
	n := col.Len()
	result := make([]bool, n)

	// Get the comparison function
	switch col.DType() {
	case Float64:
		data := col.Float64()
		for i, v := range data {
			result[i] = compareFloat64(v, op, threshold)
		}
	case Float32:
		data := col.Float32()
		for i, v := range data {
			result[i] = compareFloat64(float64(v), op, threshold)
		}
	case Int64:
		data := col.Int64()
		for i, v := range data {
			result[i] = compareFloat64(float64(v), op, threshold)
		}
	case Int32:
		data := col.Int32()
		for i, v := range data {
			result[i] = compareFloat64(float64(v), op, threshold)
		}
	default:
		return nil, fmt.Errorf("comparison not supported for type %s", col.DType())
	}

	return NewSeriesBool(col.Name(), result), nil
}

func compareFloat64(a float64, op BinaryOp, b float64) bool {
	switch op {
	case OpGt:
		return a > b
	case OpLt:
		return a < b
	case OpGte:
		return a >= b
	case OpLte:
		return a <= b
	case OpEq:
		return a == b
	case OpNeq:
		return a != b
	default:
		return false
	}
}

// evaluateVectorOp evaluates an element-wise operation between two series
func evaluateVectorOp(left *Series, op BinaryOp, right *Series) (*Series, error) {
	if left.Len() != right.Len() {
		return nil, fmt.Errorf("series length mismatch: %d vs %d", left.Len(), right.Len())
	}

	// For now, only support numeric types
	if !left.DType().IsNumeric() || !right.DType().IsNumeric() {
		return nil, fmt.Errorf("vector operations require numeric types")
	}

	n := left.Len()

	// Convert both to float64 for simplicity
	leftData := toFloat64Slice(left)
	rightData := toFloat64Slice(right)

	switch op {
	case OpAdd:
		result := make([]float64, n)
		AddF64(leftData, rightData, result)
		return NewSeriesFloat64(left.Name(), result), nil

	case OpSub:
		result := make([]float64, n)
		SubF64(leftData, rightData, result)
		return NewSeriesFloat64(left.Name(), result), nil

	case OpMul:
		result := make([]float64, n)
		MulF64(leftData, rightData, result)
		return NewSeriesFloat64(left.Name(), result), nil

	case OpDiv:
		result := make([]float64, n)
		DivF64(leftData, rightData, result)
		return NewSeriesFloat64(left.Name(), result), nil

	case OpGt:
		mask := make([]byte, n)
		CmpGtF64(leftData, rightData, mask)
		return maskToBoolSeries(left.Name(), mask), nil

	case OpGte:
		mask := make([]byte, n)
		CmpGeF64(leftData, rightData, mask)
		return maskToBoolSeries(left.Name(), mask), nil

	case OpLt:
		mask := make([]byte, n)
		CmpLtF64(leftData, rightData, mask)
		return maskToBoolSeries(left.Name(), mask), nil

	case OpLte:
		mask := make([]byte, n)
		CmpLeF64(leftData, rightData, mask)
		return maskToBoolSeries(left.Name(), mask), nil

	case OpEq:
		mask := make([]byte, n)
		CmpEqF64(leftData, rightData, mask)
		return maskToBoolSeries(left.Name(), mask), nil

	case OpNeq:
		mask := make([]byte, n)
		CmpNeF64(leftData, rightData, mask)
		return maskToBoolSeries(left.Name(), mask), nil

	case OpAnd:
		result := make([]bool, n)
		for i := range result {
			result[i] = leftData[i] != 0 && rightData[i] != 0
		}
		return NewSeriesBool(left.Name(), result), nil

	case OpOr:
		result := make([]bool, n)
		for i := range result {
			result[i] = leftData[i] != 0 || rightData[i] != 0
		}
		return NewSeriesBool(left.Name(), result), nil

	default:
		return nil, fmt.Errorf("unsupported vector operation: %s", op)
	}
}

// evaluatePredicate evaluates a predicate expression and returns a byte mask
func evaluatePredicate(expr Expr, df *DataFrame) ([]byte, error) {
	// Evaluate to a bool series
	col, err := evaluateExpr(expr, df)
	if err != nil {
		return nil, err
	}

	if col.DType() != Bool {
		return nil, fmt.Errorf("predicate must evaluate to bool, got %s", col.DType())
	}

	// Convert bool slice to byte mask
	boolData := col.Bool()
	mask := make([]byte, len(boolData))
	for i, v := range boolData {
		if v {
			mask[i] = 1
		}
	}

	return mask, nil
}

// ============================================================================
// Helper Functions
// ============================================================================

func createLiteralSeries(name string, value interface{}, length int) (*Series, error) {
	switch v := value.(type) {
	case float64:
		data := make([]float64, length)
		for i := range data {
			data[i] = v
		}
		return NewSeriesFloat64(name, data), nil
	case int64:
		data := make([]int64, length)
		for i := range data {
			data[i] = v
		}
		return NewSeriesInt64(name, data), nil
	case int:
		data := make([]int64, length)
		for i := range data {
			data[i] = int64(v)
		}
		return NewSeriesInt64(name, data), nil
	case bool:
		data := make([]bool, length)
		for i := range data {
			data[i] = v
		}
		return NewSeriesBool(name, data), nil
	case string:
		data := make([]string, length)
		for i := range data {
			data[i] = v
		}
		return NewSeriesString(name, data), nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", value)
	}
}

func lazyToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int:
		return float64(val), true
	default:
		return 0, false
	}
}

func toFloat64Slice(s *Series) []float64 {
	switch s.DType() {
	case Float64:
		return s.Float64()
	case Float32:
		data := s.Float32()
		result := make([]float64, len(data))
		for i, v := range data {
			result[i] = float64(v)
		}
		return result
	case Int64:
		data := s.Int64()
		result := make([]float64, len(data))
		for i, v := range data {
			result[i] = float64(v)
		}
		return result
	case Int32:
		data := s.Int32()
		result := make([]float64, len(data))
		for i, v := range data {
			result[i] = float64(v)
		}
		return result
	case Bool:
		data := s.Bool()
		result := make([]float64, len(data))
		for i, v := range data {
			if v {
				result[i] = 1
			}
		}
		return result
	default:
		return nil
	}
}

// maskToBoolSeries converts a byte mask (0/1 values) to a bool series
func maskToBoolSeries(name string, mask []byte) *Series {
	result := make([]bool, len(mask))
	for i, v := range mask {
		result[i] = v != 0
	}
	return NewSeriesBool(name, result)
}

func castSeries(s *Series, targetType DType) (*Series, error) {
	name := s.Name()
	n := s.Len()

	switch targetType {
	case Float64:
		data := toFloat64Slice(s)
		return NewSeriesFloat64(name, data), nil

	case Int64:
		floats := toFloat64Slice(s)
		data := make([]int64, n)
		for i, v := range floats {
			data[i] = int64(v)
		}
		return NewSeriesInt64(name, data), nil

	case Bool:
		floats := toFloat64Slice(s)
		data := make([]bool, n)
		for i, v := range floats {
			data[i] = v != 0
		}
		return NewSeriesBool(name, data), nil

	case String:
		data := make([]string, n)
		for i := 0; i < n; i++ {
			data[i] = fmt.Sprintf("%v", s.Get(i))
		}
		return NewSeriesString(name, data), nil

	default:
		return nil, fmt.Errorf("unsupported cast to %s", targetType)
	}
}
