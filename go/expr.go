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
func (e *ColExpr) Std() *AggExpr   { return &AggExpr{Input: e, AggType: AggTypeStd} }
func (e *ColExpr) Var() *AggExpr   { return &AggExpr{Input: e, AggType: AggTypeVar} }

// Alias renames the column
func (e *ColExpr) Alias(name string) *AliasExpr {
	return &AliasExpr{Inner: e, AliasName: name}
}

// Cast converts to a different type
func (e *ColExpr) Cast(dtype DType) *CastExpr {
	return &CastExpr{Inner: e, TargetType: dtype}
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
