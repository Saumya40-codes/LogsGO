package logsgoql

type Expr interface {
	exprNode()
}

type BinaryExpr struct {
	Left     Expr
	Operator TokenType
	Right    Expr
}

func (exp *BinaryExpr) exprNode() {

}

type ConditionExpr struct {
	Ident    string
	Operator TokenType
	Value    string
}

func (exp *ConditionExpr) exprNode() {

}
