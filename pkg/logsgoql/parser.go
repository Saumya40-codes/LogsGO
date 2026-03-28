package logsgoql

import "fmt"

type Parser struct {
	l         *Lexer
	currToken Token
	peekToken Token
	err       []error
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{
		l: l,
	}

	// prefill the next and peek token
	p.NextToken()
	p.NextToken()

	return p
}

func (p *Parser) NextToken() {
	p.currToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) ParseExpression() Expr {
	return p.parseOr()
}

func (p *Parser) parseOr() Expr {
	left := p.parseAnd()

	for p.currToken.Type == OR {
		op := p.currToken.Type
		p.NextToken()
		right := p.parseAnd()

		return &BinaryExpr{
			Left:     left,
			Operator: op,
			Right:    right,
		}
	}

	return left
}

func (p *Parser) parseAnd() Expr {
	left := p.parsePrimary()

	for p.currToken.Type == AND {
		op := p.currToken.Type
		p.NextToken()
		right := p.parsePrimary()

		return &BinaryExpr{
			Left:     left,
			Operator: op,
			Right:    right,
		}
	}

	return left
}

func (p *Parser) parsePrimary() Expr {
	switch p.currToken.Type {
	case IDENT:
		field := p.currToken.Literal
		p.NextToken()

		op := p.currToken.Type
		p.NextToken()

		val := p.currToken.Literal
		p.NextToken()

		return &ConditionExpr{
			Ident:    field,
			Operator: op,
			Value:    val,
		}
	case LPAREN:
		p.NextToken()
		expr := p.ParseExpression()

		if p.currToken.Type != RPAREN {
			p.err = append(p.err, fmt.Errorf("expected RPAREN, got %s", p.currToken.Type))
			return nil
		}
		p.NextToken()

		return expr
	}

	return nil
}
