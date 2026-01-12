package galleon

import (
	"fmt"
)

// Expr represents a lazy expression that can be evaluated on a DataFrame
type Expr interface {
	// String returns a string representation of the expression
	String() string

	// Clone creates a deep copy of the expression
	Clone() Expr

	// columns returns all column names referenced by this expression
	columns() []string

	// exprType returns the type of expression (for pattern matching)
	exprType() exprKind
}

type exprKind int

const (
	exprCol exprKind = iota
	exprLit
	exprAlias
	exprBinaryOp
	exprAgg
	exprCast
	exprWhen
	exprIsNull
	exprIsNotNull
	exprFillNull
	exprCoalesce
	exprQuantile
	exprCorrelation
	// Struct/List expression types
	exprStructField   // Access a field from a struct
	exprStructCreate  // Create a struct from expressions
	exprListGet       // Get element at index from list
	exprListLen       // Get length of list
	exprListSum       // Sum elements of list
	exprListMean      // Mean of list elements
	exprListMin       // Min of list elements
	exprListMax       // Max of list elements
	exprExplode       // Explode list into rows
	// String expression types
	exprStrUpper    // Convert to uppercase
	exprStrLower    // Convert to lowercase
	exprStrLen      // String length
	exprStrContains // Check if contains substring
	exprStrStartsWith
	exprStrEndsWith
	exprStrReplace
	exprStrTrim
	exprStrSplit
	// Window expression types
	exprLag
	exprLead
	exprRowNumber
	exprRank
	exprDenseRank
	exprCumSum
	exprCumMin
	exprCumMax
	exprDiff
	exprPctChange
	exprRollingSum
	exprRollingMean
	exprRollingMin
	exprRollingMax
)

// ============================================================================
// Column Expression
// ============================================================================

// ColExpr represents a column reference
type ColExpr struct {
	Name string
}

// Col creates a column reference expression
func Col(name string) *ColExpr {
	return &ColExpr{Name: name}
}

func (e *ColExpr) String() string    { return fmt.Sprintf("col(%q)", e.Name) }
func (e *ColExpr) Clone() Expr       { return &ColExpr{Name: e.Name} }
func (e *ColExpr) columns() []string { return []string{e.Name} }
func (e *ColExpr) exprType() exprKind { return exprCol }

// Arithmetic operations on columns
func (e *ColExpr) Add(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpAdd, Right: other} }
func (e *ColExpr) Sub(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpSub, Right: other} }
func (e *ColExpr) Mul(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpMul, Right: other} }
func (e *ColExpr) Div(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpDiv, Right: other} }

// Comparison operations
func (e *ColExpr) Gt(other Expr) *BinaryOpExpr  { return &BinaryOpExpr{Left: e, Op: OpGt, Right: other} }
func (e *ColExpr) Lt(other Expr) *BinaryOpExpr  { return &BinaryOpExpr{Left: e, Op: OpLt, Right: other} }
func (e *ColExpr) Eq(other Expr) *BinaryOpExpr  { return &BinaryOpExpr{Left: e, Op: OpEq, Right: other} }
func (e *ColExpr) Neq(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpNeq, Right: other} }
func (e *ColExpr) Gte(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpGte, Right: other} }
func (e *ColExpr) Lte(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpLte, Right: other} }

// Logical operations
func (e *ColExpr) And(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpAnd, Right: other} }
func (e *ColExpr) Or(other Expr) *BinaryOpExpr  { return &BinaryOpExpr{Left: e, Op: OpOr, Right: other} }

// Aggregation operations
func (e *ColExpr) Sum() *AggExpr   { return &AggExpr{Input: e, AggType: AggTypeSum} }
func (e *ColExpr) Mean() *AggExpr  { return &AggExpr{Input: e, AggType: AggTypeMean} }
func (e *ColExpr) Min() *AggExpr   { return &AggExpr{Input: e, AggType: AggTypeMin} }
func (e *ColExpr) Max() *AggExpr   { return &AggExpr{Input: e, AggType: AggTypeMax} }
func (e *ColExpr) Count() *AggExpr { return &AggExpr{Input: e, AggType: AggTypeCount} }
func (e *ColExpr) First() *AggExpr { return &AggExpr{Input: e, AggType: AggTypeFirst} }
func (e *ColExpr) Last() *AggExpr  { return &AggExpr{Input: e, AggType: AggTypeLast} }
func (e *ColExpr) Std() *AggExpr    { return &AggExpr{Input: e, AggType: AggTypeStd} }
func (e *ColExpr) Var() *AggExpr    { return &AggExpr{Input: e, AggType: AggTypeVar} }
func (e *ColExpr) Median() *AggExpr { return &AggExpr{Input: e, AggType: AggTypeMedian} }

// Skewness returns the skewness aggregation expression.
func (e *ColExpr) Skewness() *AggExpr { return &AggExpr{Input: e, AggType: AggTypeSkewness} }

// Kurtosis returns the kurtosis aggregation expression.
func (e *ColExpr) Kurtosis() *AggExpr { return &AggExpr{Input: e, AggType: AggTypeKurtosis} }

// Skew is deprecated. Use Skewness() instead.
func (e *ColExpr) Skew() *AggExpr { return e.Skewness() }

// Kurt is deprecated. Use Kurtosis() instead.
func (e *ColExpr) Kurt() *AggExpr { return e.Kurtosis() }
func (e *ColExpr) Quantile(q float64) *QuantileExpr {
	return &QuantileExpr{Input: e, Q: q}
}
func (e *ColExpr) Corr(other Expr) *CorrelationExpr {
	return &CorrelationExpr{X: e, Y: other}
}

// Alias renames the column
func (e *ColExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// Cast converts to a different type
func (e *ColExpr) Cast(dtype DType) *CastExpr {
	return &CastExpr{Inner: e, TargetType: dtype}
}

// Null handling operations
func (e *ColExpr) IsNull() *IsNullExpr       { return &IsNullExpr{Input: e} }
func (e *ColExpr) IsNotNull() *IsNotNullExpr { return &IsNotNullExpr{Input: e} }
func (e *ColExpr) FillNull(value Expr) *FillNullExpr {
	return &FillNullExpr{Input: e, FillValue: value}
}
func (e *ColExpr) FillNullLit(value interface{}) *FillNullExpr {
	return &FillNullExpr{Input: e, FillValue: Lit(value)}
}

// ============================================================================
// Literal Expression
// ============================================================================

// LitExpr represents a literal value
type LitExpr struct {
	Value interface{}
}

// Lit creates a literal value expression
func Lit(value interface{}) *LitExpr {
	return &LitExpr{Value: value}
}

func (e *LitExpr) String() string     { return fmt.Sprintf("lit(%v)", e.Value) }
func (e *LitExpr) Clone() Expr        { return &LitExpr{Value: e.Value} }
func (e *LitExpr) columns() []string  { return nil }
func (e *LitExpr) exprType() exprKind { return exprLit }

// ============================================================================
// Alias Expression
// ============================================================================

// AliasExpr wraps an expression with a new name
type AliasExpr struct {
	Inner     Expr
	AliasName string
}

func (e *AliasExpr) String() string     { return fmt.Sprintf("%s.alias(%q)", e.Inner, e.AliasName) }
func (e *AliasExpr) Clone() Expr        { return &AliasExpr{Inner: e.Inner.Clone(), AliasName: e.AliasName} }
func (e *AliasExpr) columns() []string  { return e.Inner.columns() }
func (e *AliasExpr) exprType() exprKind { return exprAlias }

// ============================================================================
// Binary Operation Expression
// ============================================================================

// BinaryOp represents binary operation types
type BinaryOp int

const (
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpGt
	OpLt
	OpEq
	OpNeq
	OpGte
	OpLte
	OpAnd
	OpOr
)

func (op BinaryOp) String() string {
	switch op {
	case OpAdd:
		return "+"
	case OpSub:
		return "-"
	case OpMul:
		return "*"
	case OpDiv:
		return "/"
	case OpGt:
		return ">"
	case OpLt:
		return "<"
	case OpEq:
		return "=="
	case OpNeq:
		return "!="
	case OpGte:
		return ">="
	case OpLte:
		return "<="
	case OpAnd:
		return "and"
	case OpOr:
		return "or"
	default:
		return "?"
	}
}

// BinaryOpExpr represents a binary operation between two expressions
type BinaryOpExpr struct {
	Left  Expr
	Op    BinaryOp
	Right Expr
}

func (e *BinaryOpExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

func (e *BinaryOpExpr) Clone() Expr {
	return &BinaryOpExpr{Left: e.Left.Clone(), Op: e.Op, Right: e.Right.Clone()}
}

func (e *BinaryOpExpr) columns() []string {
	cols := e.Left.columns()
	cols = append(cols, e.Right.columns()...)
	return cols
}

func (e *BinaryOpExpr) exprType() exprKind { return exprBinaryOp }

// Chainable operations on BinaryOpExpr
func (e *BinaryOpExpr) And(other Expr) *BinaryOpExpr { return &BinaryOpExpr{Left: e, Op: OpAnd, Right: other} }
func (e *BinaryOpExpr) Or(other Expr) *BinaryOpExpr  { return &BinaryOpExpr{Left: e, Op: OpOr, Right: other} }
func (e *BinaryOpExpr) Alias(name string) *AliasExpr { return &AliasExpr{Inner: e, AliasName: name} }

// ============================================================================
// Aggregation Expression
// ============================================================================

// AggType represents aggregation function types
type AggType int

const (
	AggTypeSum AggType = iota
	AggTypeMean
	AggTypeMin
	AggTypeMax
	AggTypeCount
	AggTypeFirst
	AggTypeLast
	AggTypeStd
	AggTypeVar
	AggTypeMedian
	AggTypeSkewness
	AggTypeKurtosis
)

func (t AggType) String() string {
	switch t {
	case AggTypeSum:
		return "sum"
	case AggTypeMean:
		return "mean"
	case AggTypeMin:
		return "min"
	case AggTypeMax:
		return "max"
	case AggTypeCount:
		return "count"
	case AggTypeFirst:
		return "first"
	case AggTypeLast:
		return "last"
	case AggTypeStd:
		return "std"
	case AggTypeVar:
		return "var"
	case AggTypeMedian:
		return "median"
	case AggTypeSkewness:
		return "skewness"
	case AggTypeKurtosis:
		return "kurtosis"
	default:
		return "?"
	}
}

// AggExpr represents an aggregation expression
type AggExpr struct {
	Input   Expr
	AggType AggType
}

func (e *AggExpr) String() string {
	return fmt.Sprintf("%s.%s()", e.Input, e.AggType)
}

func (e *AggExpr) Clone() Expr {
	return &AggExpr{Input: e.Input.Clone(), AggType: e.AggType}
}

func (e *AggExpr) columns() []string  { return e.Input.columns() }
func (e *AggExpr) exprType() exprKind { return exprAgg }

// Alias renames the aggregation result
func (e *AggExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ============================================================================
// Quantile Expression
// ============================================================================

// QuantileExpr represents a quantile aggregation with a specific q value
type QuantileExpr struct {
	Input Expr
	Q     float64 // Quantile value between 0 and 1 (0.5 = median)
}

func (e *QuantileExpr) String() string {
	return fmt.Sprintf("%s.quantile(%v)", e.Input, e.Q)
}

func (e *QuantileExpr) Clone() Expr {
	return &QuantileExpr{Input: e.Input.Clone(), Q: e.Q}
}

func (e *QuantileExpr) columns() []string  { return e.Input.columns() }
func (e *QuantileExpr) exprType() exprKind { return exprQuantile }

func (e *QuantileExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ============================================================================
// Correlation Expression
// ============================================================================

// CorrelationExpr represents a Pearson correlation between two columns
type CorrelationExpr struct {
	X Expr // First column
	Y Expr // Second column
}

func (e *CorrelationExpr) String() string {
	return fmt.Sprintf("corr(%s, %s)", e.X, e.Y)
}

func (e *CorrelationExpr) Clone() Expr {
	return &CorrelationExpr{X: e.X.Clone(), Y: e.Y.Clone()}
}

func (e *CorrelationExpr) columns() []string {
	cols := e.X.columns()
	cols = append(cols, e.Y.columns()...)
	return cols
}

func (e *CorrelationExpr) exprType() exprKind { return exprCorrelation }

func (e *CorrelationExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// Corr creates a correlation expression between two columns
func Corr(x, y Expr) *CorrelationExpr {
	return &CorrelationExpr{X: x, Y: y}
}

// ============================================================================
// Cast Expression
// ============================================================================

// CastExpr represents a type cast
type CastExpr struct {
	Inner      Expr
	TargetType DType
}

func (e *CastExpr) String() string {
	return fmt.Sprintf("%s.cast(%s)", e.Inner, e.TargetType)
}

func (e *CastExpr) Clone() Expr {
	return &CastExpr{Inner: e.Inner.Clone(), TargetType: e.TargetType}
}

func (e *CastExpr) columns() []string  { return e.Inner.columns() }
func (e *CastExpr) exprType() exprKind { return exprCast }

func (e *CastExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ============================================================================
// Helper Functions
// ============================================================================

// AllCols returns an expression that selects all columns (*)
func AllCols() *allColsExpr {
	return &allColsExpr{}
}

type allColsExpr struct{}

func (e *allColsExpr) String() string     { return "*" }
func (e *allColsExpr) Clone() Expr        { return &allColsExpr{} }
func (e *allColsExpr) columns() []string  { return nil } // means all
func (e *allColsExpr) exprType() exprKind { return exprCol }

// ExprCount creates a count(*) expression
func ExprCount() *AggExpr {
	return &AggExpr{Input: Lit(1), AggType: AggTypeCount}
}

// ============================================================================
// Conditional Expression - When/Then/Otherwise
// ============================================================================

// WhenExpr represents a conditional when/then/otherwise expression
type WhenExpr struct {
	Condition Expr // Boolean condition
	ThenExpr  Expr // Value if condition is true
	Otherwise Expr // Value if condition is false (can be nil for null)
}

func (e *WhenExpr) String() string {
	if e.Otherwise != nil {
		return fmt.Sprintf("when(%s).then(%s).otherwise(%s)", e.Condition, e.ThenExpr, e.Otherwise)
	}
	return fmt.Sprintf("when(%s).then(%s)", e.Condition, e.ThenExpr)
}

func (e *WhenExpr) Clone() Expr {
	clone := &WhenExpr{
		Condition: e.Condition.Clone(),
		ThenExpr:  e.ThenExpr.Clone(),
	}
	if e.Otherwise != nil {
		clone.Otherwise = e.Otherwise.Clone()
	}
	return clone
}

func (e *WhenExpr) columns() []string {
	cols := e.Condition.columns()
	cols = append(cols, e.ThenExpr.columns()...)
	if e.Otherwise != nil {
		cols = append(cols, e.Otherwise.columns()...)
	}
	return cols
}

func (e *WhenExpr) exprType() exprKind { return exprWhen }

// Alias renames the result
func (e *WhenExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// WhenBuilder helps construct when/then/otherwise expressions
type WhenBuilder struct {
	condition Expr
}

// When starts a conditional expression
func When(condition Expr) *WhenBuilder {
	return &WhenBuilder{condition: condition}
}

// Then specifies the value when condition is true
func (w *WhenBuilder) Then(value Expr) *ThenBuilder {
	return &ThenBuilder{condition: w.condition, thenValue: value}
}

// ThenBuilder helps construct the otherwise clause
type ThenBuilder struct {
	condition Expr
	thenValue Expr
}

// Otherwise specifies the value when condition is false
func (t *ThenBuilder) Otherwise(value Expr) *WhenExpr {
	return &WhenExpr{
		Condition: t.condition,
		ThenExpr:  t.thenValue,
		Otherwise: value,
	}
}

// OtherwiseLit specifies a literal value when condition is false
func (t *ThenBuilder) OtherwiseLit(value interface{}) *WhenExpr {
	return &WhenExpr{
		Condition: t.condition,
		ThenExpr:  t.thenValue,
		Otherwise: Lit(value),
	}
}

// OtherwiseNull returns null when condition is false
func (t *ThenBuilder) OtherwiseNull() *WhenExpr {
	return &WhenExpr{
		Condition: t.condition,
		ThenExpr:  t.thenValue,
		Otherwise: nil,
	}
}

// ============================================================================
// IsNull Expression
// ============================================================================

// IsNullExpr checks if values are null (NaN for floats)
type IsNullExpr struct {
	Input Expr
}

func (e *IsNullExpr) String() string     { return fmt.Sprintf("%s.is_null()", e.Input) }
func (e *IsNullExpr) Clone() Expr        { return &IsNullExpr{Input: e.Input.Clone()} }
func (e *IsNullExpr) columns() []string  { return e.Input.columns() }
func (e *IsNullExpr) exprType() exprKind { return exprIsNull }

func (e *IsNullExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ============================================================================
// IsNotNull Expression
// ============================================================================

// IsNotNullExpr checks if values are not null (not NaN for floats)
type IsNotNullExpr struct {
	Input Expr
}

func (e *IsNotNullExpr) String() string     { return fmt.Sprintf("%s.is_not_null()", e.Input) }
func (e *IsNotNullExpr) Clone() Expr        { return &IsNotNullExpr{Input: e.Input.Clone()} }
func (e *IsNotNullExpr) columns() []string  { return e.Input.columns() }
func (e *IsNotNullExpr) exprType() exprKind { return exprIsNotNull }

func (e *IsNotNullExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ============================================================================
// FillNull Expression
// ============================================================================

// FillNullExpr replaces null values with a fill value
type FillNullExpr struct {
	Input     Expr
	FillValue Expr
}

func (e *FillNullExpr) String() string {
	return fmt.Sprintf("%s.fill_null(%s)", e.Input, e.FillValue)
}

func (e *FillNullExpr) Clone() Expr {
	return &FillNullExpr{Input: e.Input.Clone(), FillValue: e.FillValue.Clone()}
}

func (e *FillNullExpr) columns() []string {
	cols := e.Input.columns()
	cols = append(cols, e.FillValue.columns()...)
	return cols
}

func (e *FillNullExpr) exprType() exprKind { return exprFillNull }

func (e *FillNullExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// FillNullStrategy specifies how to fill null values
type FillNullStrategy int

const (
	FillNullWithValue   FillNullStrategy = iota // Replace with a constant value
	FillNullForward                             // Forward fill (use previous non-null)
	FillNullBackward                            // Backward fill (use next non-null)
	FillNullWithMean                            // Replace with column mean
	FillNullWithMedian                          // Replace with column median
)

// FillNullStrategyExpr represents a fill null with strategy
type FillNullStrategyExpr struct {
	Input    Expr
	Strategy FillNullStrategy
}

func (e *FillNullStrategyExpr) String() string {
	strategies := []string{"value", "forward", "backward", "mean", "median"}
	return fmt.Sprintf("%s.fill_null_strategy(%s)", e.Input, strategies[e.Strategy])
}

func (e *FillNullStrategyExpr) Clone() Expr {
	return &FillNullStrategyExpr{Input: e.Input.Clone(), Strategy: e.Strategy}
}

func (e *FillNullStrategyExpr) columns() []string  { return e.Input.columns() }
func (e *FillNullStrategyExpr) exprType() exprKind { return exprFillNull }

// ============================================================================
// Coalesce Expression
// ============================================================================

// CoalesceExpr returns the first non-null value from a list of expressions
type CoalesceExpr struct {
	Inputs []Expr
}

func (e *CoalesceExpr) String() string {
	if len(e.Inputs) == 0 {
		return "coalesce()"
	}
	result := "coalesce("
	for i, input := range e.Inputs {
		if i > 0 {
			result += ", "
		}
		result += input.String()
	}
	return result + ")"
}

func (e *CoalesceExpr) Clone() Expr {
	inputs := make([]Expr, len(e.Inputs))
	for i, input := range e.Inputs {
		inputs[i] = input.Clone()
	}
	return &CoalesceExpr{Inputs: inputs}
}

func (e *CoalesceExpr) columns() []string {
	var cols []string
	for _, input := range e.Inputs {
		cols = append(cols, input.columns()...)
	}
	return cols
}

func (e *CoalesceExpr) exprType() exprKind { return exprCoalesce }

func (e *CoalesceExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// Coalesce creates an expression that returns the first non-null value
func Coalesce(exprs ...Expr) *CoalesceExpr {
	return &CoalesceExpr{Inputs: exprs}
}

// ============================================================================
// Struct Expressions
// ============================================================================

// StructFieldExpr accesses a field from a struct column
type StructFieldExpr struct {
	Input     Expr   // The struct column
	FieldName string // The field to access
}

func (e *StructFieldExpr) String() string {
	return fmt.Sprintf("%s.field(%q)", e.Input, e.FieldName)
}

func (e *StructFieldExpr) Clone() Expr {
	return &StructFieldExpr{Input: e.Input.Clone(), FieldName: e.FieldName}
}

func (e *StructFieldExpr) columns() []string { return e.Input.columns() }
func (e *StructFieldExpr) exprType() exprKind { return exprStructField }

func (e *StructFieldExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StructCreateExpr creates a struct from named expressions
type StructCreateExpr struct {
	Fields map[string]Expr
}

func (e *StructCreateExpr) String() string {
	result := "struct("
	first := true
	for name, expr := range e.Fields {
		if !first {
			result += ", "
		}
		result += fmt.Sprintf("%s=%s", name, expr)
		first = false
	}
	return result + ")"
}

func (e *StructCreateExpr) Clone() Expr {
	fields := make(map[string]Expr, len(e.Fields))
	for name, expr := range e.Fields {
		fields[name] = expr.Clone()
	}
	return &StructCreateExpr{Fields: fields}
}

func (e *StructCreateExpr) columns() []string {
	var cols []string
	for _, expr := range e.Fields {
		cols = append(cols, expr.columns()...)
	}
	return cols
}

func (e *StructCreateExpr) exprType() exprKind { return exprStructCreate }

func (e *StructCreateExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StructOf creates a struct expression from named fields
func StructOf(fields map[string]Expr) *StructCreateExpr {
	return &StructCreateExpr{Fields: fields}
}

// Field method on ColExpr to access struct fields
func (e *ColExpr) Field(name string) *StructFieldExpr {
	return &StructFieldExpr{Input: e, FieldName: name}
}

// ============================================================================
// List Expressions
// ============================================================================

// ListGetExpr gets an element at a specific index from a list column
type ListGetExpr struct {
	Input Expr // The list column
	Index int  // Index to access (negative for from end)
}

func (e *ListGetExpr) String() string {
	return fmt.Sprintf("%s.list.get(%d)", e.Input, e.Index)
}

func (e *ListGetExpr) Clone() Expr {
	return &ListGetExpr{Input: e.Input.Clone(), Index: e.Index}
}

func (e *ListGetExpr) columns() []string  { return e.Input.columns() }
func (e *ListGetExpr) exprType() exprKind { return exprListGet }

func (e *ListGetExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ListLenExpr gets the length of each list in a list column
type ListLenExpr struct {
	Input Expr
}

func (e *ListLenExpr) String() string     { return fmt.Sprintf("%s.list.len()", e.Input) }
func (e *ListLenExpr) Clone() Expr        { return &ListLenExpr{Input: e.Input.Clone()} }
func (e *ListLenExpr) columns() []string  { return e.Input.columns() }
func (e *ListLenExpr) exprType() exprKind { return exprListLen }

func (e *ListLenExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ListSumExpr sums the elements in each list
type ListSumExpr struct {
	Input Expr
}

func (e *ListSumExpr) String() string     { return fmt.Sprintf("%s.list.sum()", e.Input) }
func (e *ListSumExpr) Clone() Expr        { return &ListSumExpr{Input: e.Input.Clone()} }
func (e *ListSumExpr) columns() []string  { return e.Input.columns() }
func (e *ListSumExpr) exprType() exprKind { return exprListSum }

func (e *ListSumExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ListMeanExpr computes the mean of elements in each list
type ListMeanExpr struct {
	Input Expr
}

func (e *ListMeanExpr) String() string     { return fmt.Sprintf("%s.list.mean()", e.Input) }
func (e *ListMeanExpr) Clone() Expr        { return &ListMeanExpr{Input: e.Input.Clone()} }
func (e *ListMeanExpr) columns() []string  { return e.Input.columns() }
func (e *ListMeanExpr) exprType() exprKind { return exprListMean }

func (e *ListMeanExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ListMinExpr gets the minimum element in each list
type ListMinExpr struct {
	Input Expr
}

func (e *ListMinExpr) String() string     { return fmt.Sprintf("%s.list.min()", e.Input) }
func (e *ListMinExpr) Clone() Expr        { return &ListMinExpr{Input: e.Input.Clone()} }
func (e *ListMinExpr) columns() []string  { return e.Input.columns() }
func (e *ListMinExpr) exprType() exprKind { return exprListMin }

func (e *ListMinExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ListMaxExpr gets the maximum element in each list
type ListMaxExpr struct {
	Input Expr
}

func (e *ListMaxExpr) String() string     { return fmt.Sprintf("%s.list.max()", e.Input) }
func (e *ListMaxExpr) Clone() Expr        { return &ListMaxExpr{Input: e.Input.Clone()} }
func (e *ListMaxExpr) columns() []string  { return e.Input.columns() }
func (e *ListMaxExpr) exprType() exprKind { return exprListMax }

func (e *ListMaxExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ExplodeExpr expands list elements into separate rows
type ExplodeExpr struct {
	Input Expr
}

func (e *ExplodeExpr) String() string     { return fmt.Sprintf("%s.explode()", e.Input) }
func (e *ExplodeExpr) Clone() Expr        { return &ExplodeExpr{Input: e.Input.Clone()} }
func (e *ExplodeExpr) columns() []string  { return e.Input.columns() }
func (e *ExplodeExpr) exprType() exprKind { return exprExplode }

func (e *ExplodeExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// ListNamespace provides list operations on a column
type ListNamespace struct {
	col *ColExpr
}

// List returns a namespace for list operations on a column
func (e *ColExpr) List() *ListNamespace {
	return &ListNamespace{col: e}
}

// Get returns the element at the given index from each list
func (l *ListNamespace) Get(index int) *ListGetExpr {
	return &ListGetExpr{Input: l.col, Index: index}
}

// Len returns the length of each list
func (l *ListNamespace) Len() *ListLenExpr {
	return &ListLenExpr{Input: l.col}
}

// Sum returns the sum of elements in each list
func (l *ListNamespace) Sum() *ListSumExpr {
	return &ListSumExpr{Input: l.col}
}

// Mean returns the mean of elements in each list
func (l *ListNamespace) Mean() *ListMeanExpr {
	return &ListMeanExpr{Input: l.col}
}

// Min returns the minimum element in each list
func (l *ListNamespace) Min() *ListMinExpr {
	return &ListMinExpr{Input: l.col}
}

// Max returns the maximum element in each list
func (l *ListNamespace) Max() *ListMaxExpr {
	return &ListMaxExpr{Input: l.col}
}

// Explode expands the list into separate rows
func (e *ColExpr) Explode() *ExplodeExpr {
	return &ExplodeExpr{Input: e}
}

// ============================================================================
// String Expressions
// ============================================================================

// StrUpperExpr converts strings to uppercase
type StrUpperExpr struct {
	Input Expr
}

func (e *StrUpperExpr) String() string     { return fmt.Sprintf("%s.str.upper()", e.Input) }
func (e *StrUpperExpr) Clone() Expr        { return &StrUpperExpr{Input: e.Input.Clone()} }
func (e *StrUpperExpr) columns() []string  { return e.Input.columns() }
func (e *StrUpperExpr) exprType() exprKind { return exprStrUpper }

func (e *StrUpperExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrLowerExpr converts strings to lowercase
type StrLowerExpr struct {
	Input Expr
}

func (e *StrLowerExpr) String() string     { return fmt.Sprintf("%s.str.lower()", e.Input) }
func (e *StrLowerExpr) Clone() Expr        { return &StrLowerExpr{Input: e.Input.Clone()} }
func (e *StrLowerExpr) columns() []string  { return e.Input.columns() }
func (e *StrLowerExpr) exprType() exprKind { return exprStrLower }

func (e *StrLowerExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrLenExpr gets the length of each string
type StrLenExpr struct {
	Input Expr
}

func (e *StrLenExpr) String() string     { return fmt.Sprintf("%s.str.len()", e.Input) }
func (e *StrLenExpr) Clone() Expr        { return &StrLenExpr{Input: e.Input.Clone()} }
func (e *StrLenExpr) columns() []string  { return e.Input.columns() }
func (e *StrLenExpr) exprType() exprKind { return exprStrLen }

func (e *StrLenExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrContainsExpr checks if strings contain a substring
type StrContainsExpr struct {
	Input     Expr
	Substring string
}

func (e *StrContainsExpr) String() string {
	return fmt.Sprintf("%s.str.contains(%q)", e.Input, e.Substring)
}
func (e *StrContainsExpr) Clone() Expr {
	return &StrContainsExpr{Input: e.Input.Clone(), Substring: e.Substring}
}
func (e *StrContainsExpr) columns() []string  { return e.Input.columns() }
func (e *StrContainsExpr) exprType() exprKind { return exprStrContains }

func (e *StrContainsExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrStartsWithExpr checks if strings start with a prefix
type StrStartsWithExpr struct {
	Input  Expr
	Prefix string
}

func (e *StrStartsWithExpr) String() string {
	return fmt.Sprintf("%s.str.starts_with(%q)", e.Input, e.Prefix)
}
func (e *StrStartsWithExpr) Clone() Expr {
	return &StrStartsWithExpr{Input: e.Input.Clone(), Prefix: e.Prefix}
}
func (e *StrStartsWithExpr) columns() []string  { return e.Input.columns() }
func (e *StrStartsWithExpr) exprType() exprKind { return exprStrStartsWith }

func (e *StrStartsWithExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrEndsWithExpr checks if strings end with a suffix
type StrEndsWithExpr struct {
	Input  Expr
	Suffix string
}

func (e *StrEndsWithExpr) String() string {
	return fmt.Sprintf("%s.str.ends_with(%q)", e.Input, e.Suffix)
}
func (e *StrEndsWithExpr) Clone() Expr {
	return &StrEndsWithExpr{Input: e.Input.Clone(), Suffix: e.Suffix}
}
func (e *StrEndsWithExpr) columns() []string  { return e.Input.columns() }
func (e *StrEndsWithExpr) exprType() exprKind { return exprStrEndsWith }

func (e *StrEndsWithExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrReplaceExpr replaces occurrences of a pattern in strings
type StrReplaceExpr struct {
	Input   Expr
	Pattern string
	Replace string
}

func (e *StrReplaceExpr) String() string {
	return fmt.Sprintf("%s.str.replace(%q, %q)", e.Input, e.Pattern, e.Replace)
}
func (e *StrReplaceExpr) Clone() Expr {
	return &StrReplaceExpr{Input: e.Input.Clone(), Pattern: e.Pattern, Replace: e.Replace}
}
func (e *StrReplaceExpr) columns() []string  { return e.Input.columns() }
func (e *StrReplaceExpr) exprType() exprKind { return exprStrReplace }

func (e *StrReplaceExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StrTrimExpr trims whitespace from strings
type StrTrimExpr struct {
	Input Expr
}

func (e *StrTrimExpr) String() string     { return fmt.Sprintf("%s.str.trim()", e.Input) }
func (e *StrTrimExpr) Clone() Expr        { return &StrTrimExpr{Input: e.Input.Clone()} }
func (e *StrTrimExpr) columns() []string  { return e.Input.columns() }
func (e *StrTrimExpr) exprType() exprKind { return exprStrTrim }

func (e *StrTrimExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// StringNamespace provides string operations on a column
type StringNamespace struct {
	col *ColExpr
}

// Str returns a namespace for string operations on a column.
// Example: Col("name").Str().Upper()
func (e *ColExpr) Str() *StringNamespace {
	return &StringNamespace{col: e}
}

// Upper converts strings to uppercase
func (s *StringNamespace) Upper() *StrUpperExpr {
	return &StrUpperExpr{Input: s.col}
}

// Lower converts strings to lowercase
func (s *StringNamespace) Lower() *StrLowerExpr {
	return &StrLowerExpr{Input: s.col}
}

// Len returns the length of each string
func (s *StringNamespace) Len() *StrLenExpr {
	return &StrLenExpr{Input: s.col}
}

// Contains checks if strings contain the given substring
func (s *StringNamespace) Contains(substring string) *StrContainsExpr {
	return &StrContainsExpr{Input: s.col, Substring: substring}
}

// StartsWith checks if strings start with the given prefix
func (s *StringNamespace) StartsWith(prefix string) *StrStartsWithExpr {
	return &StrStartsWithExpr{Input: s.col, Prefix: prefix}
}

// EndsWith checks if strings end with the given suffix
func (s *StringNamespace) EndsWith(suffix string) *StrEndsWithExpr {
	return &StrEndsWithExpr{Input: s.col, Suffix: suffix}
}

// Replace replaces occurrences of pattern with replacement
func (s *StringNamespace) Replace(pattern, replacement string) *StrReplaceExpr {
	return &StrReplaceExpr{Input: s.col, Pattern: pattern, Replace: replacement}
}

// Trim removes leading and trailing whitespace
func (s *StringNamespace) Trim() *StrTrimExpr {
	return &StrTrimExpr{Input: s.col}
}

// ============================================================================
// Window Expressions
// ============================================================================

// LagExpr shifts values forward by offset positions
type LagExpr struct {
	Input   Expr
	Offset  int
	Default interface{} // Default value for positions without data
}

func (e *LagExpr) String() string {
	return fmt.Sprintf("%s.lag(%d)", e.Input, e.Offset)
}
func (e *LagExpr) Clone() Expr {
	return &LagExpr{Input: e.Input.Clone(), Offset: e.Offset, Default: e.Default}
}
func (e *LagExpr) columns() []string  { return e.Input.columns() }
func (e *LagExpr) exprType() exprKind { return exprLag }

func (e *LagExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// LeadExpr shifts values backward by offset positions
type LeadExpr struct {
	Input   Expr
	Offset  int
	Default interface{}
}

func (e *LeadExpr) String() string {
	return fmt.Sprintf("%s.lead(%d)", e.Input, e.Offset)
}
func (e *LeadExpr) Clone() Expr {
	return &LeadExpr{Input: e.Input.Clone(), Offset: e.Offset, Default: e.Default}
}
func (e *LeadExpr) columns() []string  { return e.Input.columns() }
func (e *LeadExpr) exprType() exprKind { return exprLead }

func (e *LeadExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// DiffExpr computes difference from previous value
type DiffExpr struct {
	Input  Expr
	Offset int // Number of rows to look back (default 1)
}

func (e *DiffExpr) String() string {
	return fmt.Sprintf("%s.diff(%d)", e.Input, e.Offset)
}
func (e *DiffExpr) Clone() Expr {
	return &DiffExpr{Input: e.Input.Clone(), Offset: e.Offset}
}
func (e *DiffExpr) columns() []string  { return e.Input.columns() }
func (e *DiffExpr) exprType() exprKind { return exprDiff }

func (e *DiffExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// PctChangeExpr computes percentage change from previous value
type PctChangeExpr struct {
	Input  Expr
	Offset int
}

func (e *PctChangeExpr) String() string {
	return fmt.Sprintf("%s.pct_change(%d)", e.Input, e.Offset)
}
func (e *PctChangeExpr) Clone() Expr {
	return &PctChangeExpr{Input: e.Input.Clone(), Offset: e.Offset}
}
func (e *PctChangeExpr) columns() []string  { return e.Input.columns() }
func (e *PctChangeExpr) exprType() exprKind { return exprPctChange }

func (e *PctChangeExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// CumSumExpr computes cumulative sum
type CumSumExpr struct {
	Input Expr
}

func (e *CumSumExpr) String() string     { return fmt.Sprintf("%s.cum_sum()", e.Input) }
func (e *CumSumExpr) Clone() Expr        { return &CumSumExpr{Input: e.Input.Clone()} }
func (e *CumSumExpr) columns() []string  { return e.Input.columns() }
func (e *CumSumExpr) exprType() exprKind { return exprCumSum }

func (e *CumSumExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// CumMinExpr computes cumulative minimum
type CumMinExpr struct {
	Input Expr
}

func (e *CumMinExpr) String() string     { return fmt.Sprintf("%s.cum_min()", e.Input) }
func (e *CumMinExpr) Clone() Expr        { return &CumMinExpr{Input: e.Input.Clone()} }
func (e *CumMinExpr) columns() []string  { return e.Input.columns() }
func (e *CumMinExpr) exprType() exprKind { return exprCumMin }

func (e *CumMinExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// CumMaxExpr computes cumulative maximum
type CumMaxExpr struct {
	Input Expr
}

func (e *CumMaxExpr) String() string     { return fmt.Sprintf("%s.cum_max()", e.Input) }
func (e *CumMaxExpr) Clone() Expr        { return &CumMaxExpr{Input: e.Input.Clone()} }
func (e *CumMaxExpr) columns() []string  { return e.Input.columns() }
func (e *CumMaxExpr) exprType() exprKind { return exprCumMax }

func (e *CumMaxExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// RollingSumExpr computes rolling sum over a window
type RollingSumExpr struct {
	Input      Expr
	WindowSize int
	MinPeriods int
}

func (e *RollingSumExpr) String() string {
	return fmt.Sprintf("%s.rolling_sum(%d)", e.Input, e.WindowSize)
}
func (e *RollingSumExpr) Clone() Expr {
	return &RollingSumExpr{Input: e.Input.Clone(), WindowSize: e.WindowSize, MinPeriods: e.MinPeriods}
}
func (e *RollingSumExpr) columns() []string  { return e.Input.columns() }
func (e *RollingSumExpr) exprType() exprKind { return exprRollingSum }

func (e *RollingSumExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// RollingMeanExpr computes rolling mean over a window
type RollingMeanExpr struct {
	Input      Expr
	WindowSize int
	MinPeriods int
}

func (e *RollingMeanExpr) String() string {
	return fmt.Sprintf("%s.rolling_mean(%d)", e.Input, e.WindowSize)
}
func (e *RollingMeanExpr) Clone() Expr {
	return &RollingMeanExpr{Input: e.Input.Clone(), WindowSize: e.WindowSize, MinPeriods: e.MinPeriods}
}
func (e *RollingMeanExpr) columns() []string  { return e.Input.columns() }
func (e *RollingMeanExpr) exprType() exprKind { return exprRollingMean }

func (e *RollingMeanExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// Window function methods on ColExpr

// Lag returns the value offset rows before the current row.
// Example: Col("price").Lag(1, 0.0) - previous day's price
func (e *ColExpr) Lag(offset int, defaultVal interface{}) *LagExpr {
	return &LagExpr{Input: e, Offset: offset, Default: defaultVal}
}

// Lead returns the value offset rows after the current row.
// Example: Col("price").Lead(1, 0.0) - next day's price
func (e *ColExpr) Lead(offset int, defaultVal interface{}) *LeadExpr {
	return &LeadExpr{Input: e, Offset: offset, Default: defaultVal}
}

// Diff computes the difference from the previous value.
// Example: Col("price").Diff() - daily price change
func (e *ColExpr) Diff() *DiffExpr {
	return &DiffExpr{Input: e, Offset: 1}
}

// DiffN computes the difference from N rows ago.
func (e *ColExpr) DiffN(offset int) *DiffExpr {
	return &DiffExpr{Input: e, Offset: offset}
}

// PctChange computes the percentage change from the previous value.
func (e *ColExpr) PctChange() *PctChangeExpr {
	return &PctChangeExpr{Input: e, Offset: 1}
}

// CumSum computes the cumulative sum.
func (e *ColExpr) CumSum() *CumSumExpr {
	return &CumSumExpr{Input: e}
}

// CumMin computes the cumulative minimum.
func (e *ColExpr) CumMin() *CumMinExpr {
	return &CumMinExpr{Input: e}
}

// CumMax computes the cumulative maximum.
func (e *ColExpr) CumMax() *CumMaxExpr {
	return &CumMaxExpr{Input: e}
}

// RollingSum computes the rolling sum over a window.
// Example: Col("value").RollingSum(7) - 7-period rolling sum with minPeriods=1
// Example: Col("value").RollingSum(7, 3) - 7-period rolling sum with minPeriods=3
func (e *ColExpr) RollingSum(windowSize int, minPeriods ...int) *RollingSumExpr {
	mp := 1
	if len(minPeriods) > 0 {
		mp = minPeriods[0]
	}
	return &RollingSumExpr{Input: e, WindowSize: windowSize, MinPeriods: mp}
}

// RollingMean computes the rolling mean over a window.
// Example: Col("price").RollingMean(20) - 20-period moving average with minPeriods=1
// Example: Col("price").RollingMean(20, 15) - 20-period moving average with minPeriods=15
func (e *ColExpr) RollingMean(windowSize int, minPeriods ...int) *RollingMeanExpr {
	mp := 1
	if len(minPeriods) > 0 {
		mp = minPeriods[0]
	}
	return &RollingMeanExpr{Input: e, WindowSize: windowSize, MinPeriods: mp}
}
