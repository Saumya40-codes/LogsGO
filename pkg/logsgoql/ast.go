package logsgoql

import "fmt"

type Expr interface {
	exprNode() string
}

type BinaryExpr struct {
	Left     Expr
	Operator TokenType
	Right    Expr
}

type ConditionExpr struct {
	Ident    string
	Operator TokenType
	Value    string
}

func tokenSymbol(t TokenType) string {
	switch t {
	case AND:
		return "AND"
	case OR:
		return "OR"
	case EQ:
		return "="
	case CONTAINS:
		return "CONTAINS"
	default:
		return fmt.Sprintf("<unknown:%v>", t)
	}
}

func precedence(t TokenType) int {
	switch t {
	case AND:
		return 2
	case OR:
		return 1
	default:
		return 0
	}
}

func (e *ConditionExpr) exprNode() string {
	return fmt.Sprintf("%s %s %q", e.Ident, tokenSymbol(e.Operator), e.Value)
}

func (e *BinaryExpr) exprNode() string {
	return binaryString(e, 0)
}

func binaryString(e *BinaryExpr, parentPrec int) string {
	myPrec := precedence(e.Operator)

	left := exprString(e.Left, myPrec)
	right := exprString(e.Right, myPrec)

	result := left + " " + tokenSymbol(e.Operator) + " " + right

	if myPrec < parentPrec {
		return "(" + result + ")"
	}
	return result
}

func exprString(expr Expr, parentPrec int) string {
	switch e := expr.(type) {
	case *BinaryExpr:
		return binaryString(e, parentPrec)
	case *ConditionExpr:
		return e.exprNode()
	default:
		return "<unknown>"
	}
}
