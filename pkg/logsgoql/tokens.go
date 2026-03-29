package logsgoql

type TokenType string

const (
	AND      TokenType = "AND"
	OR       TokenType = "OR"
	LPAREN   TokenType = "("
	RPAREN   TokenType = ")"
	EQ       TokenType = "="
	CONTAINS TokenType = "CONTAINS"
	IDENT    TokenType = "IDENT"
	STRING   TokenType = "STRING"
	EOF      TokenType = "EOF"
	ILLEGAL  TokenType = "ILLEGAl"
)

type Token struct {
	Type    TokenType
	Literal string
}

func NewToken(tokenType TokenType, literal string) Token {
	return Token{
		Type:    tokenType,
		Literal: literal,
	}
}
