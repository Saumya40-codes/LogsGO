package logsgoql

import (
	"strings"
)

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

var keywords = map[string]TokenType{
	"AND":      AND,
	"OR":       OR,
	"CONTAINS": CONTAINS,
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) NextToken() Token {
	var tok Token
	l.skipWhitespace()

	switch l.ch {
	case '=':
		tok = NewToken(EQ, "=")
	case '(':
		tok = NewToken(LPAREN, "(")
	case ')':
		tok = NewToken(RPAREN, ")")
	case '"', '\'':
		return NewToken(STRING, l.readQuotedString())
	case 0:
		return NewToken(EOF, "")
	default:
		if isWordChar(l.ch) {
			lit := l.readWord()
			return Token{Type: lookupWord(lit), Literal: lit}
		}
		tok = NewToken(ILLEGAL, string(l.ch))
	}

	l.readChar()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func isWordChar(ch byte) bool {
	return ('a' <= ch && ch <= 'z') ||
		('A' <= ch && ch <= 'Z') ||
		('0' <= ch && ch <= '9') ||
		ch == '_' || ch == '-' || ch == '.' || ch == ':' || ch == '/'
}

func (l *Lexer) readWord() string {
	start := l.position
	for isWordChar(l.ch) {
		l.readChar()
	}
	return l.input[start:l.position]
}

func lookupWord(s string) TokenType {
	if tok, ok := keywords[strings.ToUpper(s)]; ok {
		return tok
	}
	return IDENT
}

func (l *Lexer) readQuotedString() string {
	quote := l.ch
	l.readChar() // skip opening quote
	start := l.position

	for l.ch != 0 && l.ch != quote {
		l.readChar()
	}
	val := l.input[start:l.position]
	if l.ch == quote {
		l.readChar() // skip closing quote
	}
	return val
}
