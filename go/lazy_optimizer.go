package galleon

// ============================================================================
// Query Optimizer
// ============================================================================

// optimizePlan applies optimization passes to the logical plan
func optimizePlan(plan *LogicalPlan) *LogicalPlan {
	// Apply optimization passes in order
	result := plan

	// Pass 1: Predicate pushdown - push filters closer to data sources
	result = pushdownPredicates(result)

	// Pass 2: Projection pushdown - only read needed columns
	result = pushdownProjections(result, nil)

	// Pass 3: Combine consecutive filters
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
		// For simplicity, we don't push into joins for now
		// Future: check if predicate only uses left/right side columns
		_ = predicate.columns() // Analyze columns for future optimization
		return filterPlan

	default:
		// Can't push through this operation
		return filterPlan
	}
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
