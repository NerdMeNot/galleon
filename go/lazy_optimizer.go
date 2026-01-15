package galleon

import "fmt"

// ============================================================================
// Query Optimizer
// ============================================================================

// optimizePlan applies optimization passes to the logical plan
func optimizePlan(plan *LogicalPlan) *LogicalPlan {
	// Apply optimization passes in order
	result := plan

	// Pass 1: Common Subexpression Elimination - compute duplicates once
	result = eliminateCommonSubexpressions(result)

	// Pass 2: Predicate pushdown - push filters closer to data sources
	result = pushdownPredicates(result)

	// Pass 3: Projection pushdown - only read needed columns
	result = pushdownProjections(result, nil)

	// Pass 4: Combine consecutive filters
	result = combineFilters(result)

	return result
}

// ============================================================================
// Predicate Pushdown
// ============================================================================

// pushdownPredicates pushes filter predicates down through the plan
func pushdownPredicates(plan *LogicalPlan) *LogicalPlan {
	if plan == nil {
		return nil
	}

	// First, recursively optimize children
	newPlan := &LogicalPlan{}
	*newPlan = *plan
	newPlan.Input = pushdownPredicates(plan.Input)
	newPlan.Right = pushdownPredicates(plan.Right)

	// Now try to push filters down
	if newPlan.Op == PlanFilter && newPlan.Input != nil {
		return tryPushFilter(newPlan)
	}

	return newPlan
}

// tryPushFilter attempts to push a filter through its input
func tryPushFilter(filterPlan *LogicalPlan) *LogicalPlan {
	child := filterPlan.Input
	predicate := filterPlan.Predicate

	switch child.Op {
	case PlanProject:
		// Can push filter below projection if it only uses projected columns
		// For now, keep filter above projection
		return filterPlan

	case PlanFilter:
		// Combine consecutive filters with AND
		combined := &BinaryOpExpr{
			Left:  child.Predicate,
			Op:    OpAnd,
			Right: predicate,
		}
		return &LogicalPlan{
			Op:        PlanFilter,
			Input:     child.Input,
			Predicate: combined,
		}

	case PlanJoin:
		// Push filter to appropriate side of join if possible
		return tryPushFilterThroughJoin(filterPlan, child, predicate)

	default:
		// Can't push through this operation
		return filterPlan
	}
}

// tryPushFilterThroughJoin attempts to push a filter to the left or right side of a join
func tryPushFilterThroughJoin(filterPlan *LogicalPlan, joinPlan *LogicalPlan, predicate Expr) *LogicalPlan {
	// Get column sets for left and right sides
	leftCols := collectPlanColumns(joinPlan.Input)
	rightCols := collectPlanColumns(joinPlan.Right)

	// Get columns used by the predicate
	predCols := predicate.columns()
	predColSet := make(map[string]bool)
	for _, col := range predCols {
		predColSet[col] = true
	}

	// Check if predicate uses only left-side columns
	usesOnlyLeft := true
	usesOnlyRight := true
	for col := range predColSet {
		if !leftCols[col] {
			usesOnlyLeft = false
		}
		if !rightCols[col] {
			usesOnlyRight = false
		}
	}

	// If predicate uses only left-side columns, push to left
	if usesOnlyLeft && len(predColSet) > 0 {
		newJoin := &LogicalPlan{}
		*newJoin = *joinPlan
		newJoin.Input = &LogicalPlan{
			Op:        PlanFilter,
			Input:     joinPlan.Input,
			Predicate: predicate,
		}
		return newJoin
	}

	// If predicate uses only right-side columns, push to right
	if usesOnlyRight && len(predColSet) > 0 {
		newJoin := &LogicalPlan{}
		*newJoin = *joinPlan
		newJoin.Right = &LogicalPlan{
			Op:        PlanFilter,
			Input:     joinPlan.Right,
			Predicate: predicate,
		}
		return newJoin
	}

	// Try to decompose AND predicates
	if binExpr, ok := predicate.(*BinaryOpExpr); ok && binExpr.Op == OpAnd {
		leftPred := binExpr.Left
		rightPred := binExpr.Right

		// Recursively try to push each part
		result := joinPlan
		leftFilter := &LogicalPlan{
			Op:        PlanFilter,
			Input:     result,
			Predicate: leftPred,
		}
		result = tryPushFilterThroughJoin(leftFilter, result, leftPred)

		rightFilter := &LogicalPlan{
			Op:        PlanFilter,
			Input:     result,
			Predicate: rightPred,
		}
		return tryPushFilterThroughJoin(rightFilter, result, rightPred)
	}

	// Can't push this filter - keep it above the join
	return filterPlan
}

// collectPlanColumns collects all column names available from a plan
func collectPlanColumns(plan *LogicalPlan) map[string]bool {
	cols := make(map[string]bool)
	if plan == nil {
		return cols
	}

	switch plan.Op {
	case PlanScan:
		// Get columns from DataFrame
		if plan.Data != nil {
			for _, name := range plan.Data.Columns() {
				cols[name] = true
			}
		}

	case PlanScanCSV, PlanScanParquet, PlanScanJSON:
		// For file scans, we don't know columns until execution
		// Return empty set (conservative approach)
		return cols

	case PlanWithColumn:
		// Inherit from input plus the new column
		cols = collectPlanColumns(plan.Input)
		if len(plan.Projections) > 0 {
			if alias, ok := plan.Projections[0].(*AliasExpr); ok {
				cols[alias.AliasName] = true
			}
		}

	case PlanProject:
		// Get columns from projections
		for _, expr := range plan.Projections {
			if alias, ok := expr.(*AliasExpr); ok {
				cols[alias.AliasName] = true
			} else if col, ok := expr.(*ColExpr); ok {
				cols[col.Name] = true
			}
		}

	case PlanGroupBy:
		// Group by produces key columns and aggregation results
		for _, key := range plan.GroupByKeys {
			if col, ok := key.(*ColExpr); ok {
				cols[col.Name] = true
			}
		}
		for _, agg := range plan.Aggregations {
			if alias, ok := agg.(*AliasExpr); ok {
				cols[alias.AliasName] = true
			}
		}

	case PlanJoin:
		// Combine columns from both sides
		for col := range collectPlanColumns(plan.Input) {
			cols[col] = true
		}
		for col := range collectPlanColumns(plan.Right) {
			// Handle suffix for duplicate columns
			if cols[col] {
				cols[col+plan.JoinOpts.suffix] = true
			} else {
				cols[col] = true
			}
		}

	default:
		// Inherit from input
		cols = collectPlanColumns(plan.Input)
	}

	return cols
}

// ============================================================================
// Projection Pushdown
// ============================================================================

// pushdownProjections ensures we only read needed columns
func pushdownProjections(plan *LogicalPlan, neededCols map[string]bool) *LogicalPlan {
	if plan == nil {
		return nil
	}

	newPlan := &LogicalPlan{}
	*newPlan = *plan

	switch plan.Op {
	case PlanScan:
		// At the source - apply column pruning if we know which columns are needed
		// For in-memory scans, this doesn't help much
		return newPlan

	case PlanScanCSV, PlanScanParquet, PlanScanJSON:
		// For file scans, we could add column selection here
		// This would require modifying the read options
		return newPlan

	case PlanProject:
		// Track which columns the projections need
		needed := make(map[string]bool)
		for _, expr := range plan.Projections {
			for _, col := range expr.columns() {
				needed[col] = true
			}
		}
		newPlan.Input = pushdownProjections(plan.Input, needed)
		return newPlan

	case PlanFilter:
		// Add filter columns to needed set
		needed := make(map[string]bool)
		for k, v := range neededCols {
			needed[k] = v
		}
		for _, col := range plan.Predicate.columns() {
			needed[col] = true
		}
		newPlan.Input = pushdownProjections(plan.Input, needed)
		return newPlan

	case PlanGroupBy:
		// Need all key columns and aggregation input columns
		needed := make(map[string]bool)
		for _, key := range plan.GroupByKeys {
			for _, col := range key.columns() {
				needed[col] = true
			}
		}
		for _, agg := range plan.Aggregations {
			for _, col := range agg.columns() {
				needed[col] = true
			}
		}
		newPlan.Input = pushdownProjections(plan.Input, needed)
		return newPlan

	case PlanJoin:
		// Need join key columns from both sides
		newPlan.Input = pushdownProjections(plan.Input, neededCols)
		newPlan.Right = pushdownProjections(plan.Right, neededCols)
		return newPlan

	default:
		newPlan.Input = pushdownProjections(plan.Input, neededCols)
		newPlan.Right = pushdownProjections(plan.Right, neededCols)
		return newPlan
	}
}

// ============================================================================
// Filter Combination
// ============================================================================

// combineFilters combines consecutive filter operations
func combineFilters(plan *LogicalPlan) *LogicalPlan {
	if plan == nil {
		return nil
	}

	newPlan := &LogicalPlan{}
	*newPlan = *plan
	newPlan.Input = combineFilters(plan.Input)
	newPlan.Right = combineFilters(plan.Right)

	// Check for consecutive filters
	if newPlan.Op == PlanFilter && newPlan.Input != nil && newPlan.Input.Op == PlanFilter {
		// Combine with AND
		combined := &BinaryOpExpr{
			Left:  newPlan.Input.Predicate,
			Op:    OpAnd,
			Right: newPlan.Predicate,
		}
		return &LogicalPlan{
			Op:        PlanFilter,
			Input:     newPlan.Input.Input,
			Predicate: combined,
		}
	}

	return newPlan
}

// ============================================================================
// Common Subexpression Elimination (CSE)
// ============================================================================

// cseInfo tracks information about a common subexpression
type cseInfo struct {
	expr      Expr   // The original expression
	exprStr   string // String representation (used as key)
	count     int    // Number of occurrences
	cseColName string // Name of the CSE column (e.g., "__cse_0")
}

// eliminateCommonSubexpressions detects duplicate expressions and computes them once
func eliminateCommonSubexpressions(plan *LogicalPlan) *LogicalPlan {
	if plan == nil {
		return nil
	}

	// Phase 1: Collect all non-trivial expressions with their occurrence counts
	exprCounts := make(map[string]*cseInfo)
	collectExpressions(plan, exprCounts)

	// Phase 2: Find expressions that appear more than once (excluding simple column refs)
	var duplicates []*cseInfo
	for _, info := range exprCounts {
		// Only consider non-trivial expressions that appear more than once
		if info.count > 1 && isNonTrivialExpr(info.expr) {
			duplicates = append(duplicates, info)
		}
	}

	// If no duplicates found, return plan unchanged
	if len(duplicates) == 0 {
		return plan
	}

	// Phase 3: Assign CSE column names
	for i, info := range duplicates {
		info.cseColName = fmt.Sprintf("__cse_%d", i)
	}

	// Phase 4: Rewrite plan to compute duplicates once and reference them
	// Insert WithColumn nodes for each CSE at the appropriate level
	result := rewritePlanWithCSE(plan, duplicates)

	return result
}

// collectExpressions traverses the plan and counts expression occurrences
func collectExpressions(plan *LogicalPlan, counts map[string]*cseInfo) {
	if plan == nil {
		return
	}

	// Collect from projections
	for _, expr := range plan.Projections {
		collectFromExpr(expr, counts)
	}

	// Collect from predicate
	if plan.Predicate != nil {
		collectFromExpr(plan.Predicate, counts)
	}

	// Collect from group by keys
	for _, expr := range plan.GroupByKeys {
		collectFromExpr(expr, counts)
	}

	// Collect from aggregations
	for _, expr := range plan.Aggregations {
		collectFromExpr(expr, counts)
	}

	// Recurse into children
	collectExpressions(plan.Input, counts)
	collectExpressions(plan.Right, counts)
}

// collectFromExpr recursively collects expressions and their subexpressions
func collectFromExpr(expr Expr, counts map[string]*cseInfo) {
	if expr == nil {
		return
	}

	// Get string representation as key
	key := expr.String()

	// Count this expression
	if info, exists := counts[key]; exists {
		info.count++
	} else {
		counts[key] = &cseInfo{
			expr:    expr,
			exprStr: key,
			count:   1,
		}
	}

	// Recursively collect from subexpressions
	switch e := expr.(type) {
	case *BinaryOpExpr:
		collectFromExpr(e.Left, counts)
		collectFromExpr(e.Right, counts)
	case *AliasExpr:
		collectFromExpr(e.Inner, counts)
	case *AggExpr:
		collectFromExpr(e.Input, counts)
	case *CastExpr:
		collectFromExpr(e.Inner, counts)
	case *IsNullExpr:
		collectFromExpr(e.Input, counts)
	case *IsNotNullExpr:
		collectFromExpr(e.Input, counts)
	case *FillNullExpr:
		collectFromExpr(e.Input, counts)
		collectFromExpr(e.FillValue, counts)
	case *CoalesceExpr:
		for _, ex := range e.Inputs {
			collectFromExpr(ex, counts)
		}
	case *QuantileExpr:
		collectFromExpr(e.Input, counts)
	case *CorrelationExpr:
		collectFromExpr(e.X, counts)
		collectFromExpr(e.Y, counts)
	// Add more cases as needed for other expression types
	}
}

// isNonTrivialExpr returns true if the expression is worth caching
// Simple column references and literals are not worth caching
func isNonTrivialExpr(expr Expr) bool {
	switch e := expr.(type) {
	case *ColExpr:
		return false // Column references are trivial
	case *LitExpr:
		return false // Literals are trivial
	case *AliasExpr:
		return isNonTrivialExpr(e.Inner) // Check inner expression
	default:
		return true // Everything else is non-trivial
	}
}

// rewritePlanWithCSE rewrites the plan to use CSE columns
func rewritePlanWithCSE(plan *LogicalPlan, duplicates []*cseInfo) *LogicalPlan {
	if plan == nil || len(duplicates) == 0 {
		return plan
	}

	// Create a map for quick lookup
	cseMap := make(map[string]string) // exprStr -> cseColName
	for _, info := range duplicates {
		cseMap[info.exprStr] = info.cseColName
	}

	// Recursively process children first
	newPlan := &LogicalPlan{}
	*newPlan = *plan
	newPlan.Input = rewritePlanWithCSE(plan.Input, duplicates)
	newPlan.Right = rewritePlanWithCSE(plan.Right, duplicates)

	// Rewrite expressions in this node
	newPlan.Projections = rewriteExprs(plan.Projections, cseMap)
	newPlan.Predicate = rewriteExpr(plan.Predicate, cseMap)
	newPlan.GroupByKeys = rewriteExprs(plan.GroupByKeys, cseMap)
	newPlan.Aggregations = rewriteExprs(plan.Aggregations, cseMap)

	// Find the earliest point where we should insert CSE WithColumn nodes
	// This is typically right after a scan or at the top of a subtree
	if shouldInsertCSE(newPlan) {
		// Insert WithColumn nodes for each CSE
		result := newPlan
		for _, info := range duplicates {
			// Only insert if this expression's columns are available at this point
			if exprColumnsAvailable(info.expr, result) {
				withCol := &LogicalPlan{
					Op:    PlanWithColumn,
					Input: result,
					Projections: []Expr{
						&AliasExpr{Inner: info.expr, AliasName: info.cseColName},
					},
				}
				result = withCol
			}
		}
		return result
	}

	return newPlan
}

// shouldInsertCSE determines if CSE columns should be inserted at this plan node
func shouldInsertCSE(plan *LogicalPlan) bool {
	if plan == nil {
		return false
	}
	// Insert CSE after scans (source of data)
	switch plan.Op {
	case PlanScan, PlanScanCSV, PlanScanParquet, PlanScanJSON:
		return true
	}
	return false
}

// exprColumnsAvailable checks if all columns needed by expr are available
func exprColumnsAvailable(expr Expr, plan *LogicalPlan) bool {
	// For now, assume columns are available after scan
	// A more complete implementation would check the schema
	return plan != nil
}

// rewriteExprs rewrites a slice of expressions
func rewriteExprs(exprs []Expr, cseMap map[string]string) []Expr {
	if exprs == nil {
		return nil
	}
	result := make([]Expr, len(exprs))
	for i, expr := range exprs {
		result[i] = rewriteExpr(expr, cseMap)
	}
	return result
}

// rewriteExpr replaces expressions with CSE column references
func rewriteExpr(expr Expr, cseMap map[string]string) Expr {
	if expr == nil {
		return nil
	}

	// Check if this exact expression should be replaced
	key := expr.String()
	if cseColName, exists := cseMap[key]; exists {
		// Replace with column reference
		return Col(cseColName)
	}

	// Recursively rewrite subexpressions
	switch e := expr.(type) {
	case *BinaryOpExpr:
		return &BinaryOpExpr{
			Left:  rewriteExpr(e.Left, cseMap),
			Op:    e.Op,
			Right: rewriteExpr(e.Right, cseMap),
		}
	case *AliasExpr:
		return &AliasExpr{
			Inner:     rewriteExpr(e.Inner, cseMap),
			AliasName: e.AliasName,
		}
	case *AggExpr:
		return &AggExpr{
			Input:   rewriteExpr(e.Input, cseMap),
			AggType: e.AggType,
		}
	case *CastExpr:
		return &CastExpr{
			Inner:      rewriteExpr(e.Inner, cseMap),
			TargetType: e.TargetType,
		}
	case *IsNullExpr:
		return &IsNullExpr{Input: rewriteExpr(e.Input, cseMap)}
	case *IsNotNullExpr:
		return &IsNotNullExpr{Input: rewriteExpr(e.Input, cseMap)}
	case *FillNullExpr:
		return &FillNullExpr{
			Input:     rewriteExpr(e.Input, cseMap),
			FillValue: rewriteExpr(e.FillValue, cseMap),
		}
	default:
		// Return as-is for expressions we don't handle
		return expr
	}
}

