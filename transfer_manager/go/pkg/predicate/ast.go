package predicate

import (
	"fmt"
	"strconv"
	"strings"
)

// Node represents a node in the conditions abstract syntax tree.
type Node interface {
	node()
	String() string
}

func (*NumberLiteral) node()  {}
func (*StringLiteral) node()  {}
func (*BooleanLiteral) node() {}
func (*BinaryExpr) node()     {}
func (*ParenExpr) node()      {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
	Args() []string
}

func (*NumberLiteral) expr()  {}
func (*StringLiteral) expr()  {}
func (*BooleanLiteral) expr() {}
func (*BinaryExpr) expr()     {}
func (*ParenExpr) expr()      {}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

// String returns a string representation of the literal.
func (n *NumberLiteral) String() string { return strconv.FormatFloat(n.Val, 'f', 3, 64) }

func (n *NumberLiteral) Args() []string {
	args := []string{}
	return args
}

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

func (l *BooleanLiteral) Args() []string {
	args := []string{}
	return args
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return Quote(l.Val) }

func (l *StringLiteral) Args() []string {
	args := []string{}
	return args
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op, e.RHS.String())
}

func (e *BinaryExpr) Args() []string {
	var args []string

	args = append(e.LHS.Args(), args...)
	args = append(e.RHS.Args(), args...)

	return args
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr     Expr
	Inverted bool
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string {
	if e.Inverted {
		return fmt.Sprintf("NOT (%s)", e.Expr.String())
	}
	return fmt.Sprintf("(%s)", e.Expr.String())
}

func (e *ParenExpr) Args() []string {
	var args []string
	args = append(e.Expr.Args(), args...)

	return args
}

// Quote returns a quoted string.
func Quote(s string) string {
	return `"` + strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`).Replace(s) + `"`
}
