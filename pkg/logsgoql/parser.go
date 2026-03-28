package logsgoql

import "fmt"

type Parser struct {
	l         *Lexer
	currToken Token
	peekToken Token
	err       []error
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{l: l}

	// initialize curr + peek
	p.nextToken()
	p.nextToken()

	return p
}

func (p *Parser) nextToken() {
	p.currToken = p.peekToken
	p.peekToken = p.l.NextToken()

	if p.currToken.Type == ILLEGAL {
		p.err = append(p.err, fmt.Errorf("illegal token: %s", p.currToken.Literal))
	}
}

func (p *Parser) Errors() []error {
	return p.err
}

func (p *Parser) ParseExpression() Expr {
	expr := p.parseOr()

	if p.currToken.Type != EOF {
		p.err = append(p.err, fmt.Errorf("unexpected token at end: %s", p.currToken.Type))
	}

	return expr
}

func (p *Parser) parseOr() Expr {
	left := p.parseAnd()

	for p.currToken.Type == OR {
		op := p.currToken.Type
		p.nextToken()

		right := p.parseAnd()

		left = &BinaryExpr{
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
		p.nextToken()

		right := p.parsePrimary()

		left = &BinaryExpr{
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
		p.nextToken()

		// operator check
		if p.currToken.Type != EQ && p.currToken.Type != CONTAINS {
			p.err = append(p.err, fmt.Errorf("expected operator after IDENT, got %s", p.currToken.Type))
			return nil
		}
		op := p.currToken.Type

		// CONTAINS will require inverted indexes across our stores. TODO: add support for CONTAINS
		if op == CONTAINS {
			p.err = append(p.err, fmt.Errorf("CONTAINS not supported in v1"))
		}
		p.nextToken()

		// value check.... hmm notice that according to grammer we do allow ident on RHS of equals in case like level=warn (note: value isnt in quotation)
		// this might be false negative when something like level=level is used but again we assert that if RHS of EQUAL is right word expr then it might be the right value
		if p.currToken.Type != STRING && p.currToken.Type != IDENT {
			p.err = append(p.err, fmt.Errorf("expected value after operator, got %s", p.currToken.Type))
			return nil
		}
		val := p.currToken.Literal
		p.nextToken()

		return &ConditionExpr{
			Ident:    field,
			Operator: op,
			Value:    val,
		}

	case LPAREN:
		p.nextToken()

		expr := p.parseOr()

		if p.currToken.Type != RPAREN {
			p.err = append(p.err, fmt.Errorf("expected RPAREN, got %s", p.currToken.Type))
			return nil
		}
		p.nextToken()

		return expr

	case ILLEGAL:
		p.err = append(p.err, fmt.Errorf("illegal token: %s", p.currToken.Literal))
		return nil

	default:
		p.err = append(p.err, fmt.Errorf("unexpected token: %s", p.currToken.Type))
		return nil
	}
}
