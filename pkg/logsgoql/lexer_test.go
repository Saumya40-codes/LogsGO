package logsgoql

import "testing"

func TestNextToken(t *testing.T) {
	inputQueries := []string{
		"level=\"a\"",
		"level='a'",
		"level=a",
	}

	expectedTokens := []struct {
		tokType           TokenType
		value             string
		exceptCase        bool
		exceptCaseTokType TokenType
	}{
		{
			tokType:    IDENT,
			value:      "level",
			exceptCase: false,
		},
		{
			tokType:    EQ,
			value:      "=",
			exceptCase: false,
		},
		{
			tokType:           STRING,
			value:             "a",
			exceptCase:        true,
			exceptCaseTokType: IDENT,
		},
	}
	for _, query := range inputQueries {
		l := NewLexer(query)

		for i, tt := range expectedTokens {
			tok := l.NextToken()

			if tok.Type != tt.tokType {
				if tt.exceptCase {
					if tok.Type != tt.exceptCaseTokType {
						t.Errorf("%d Wrong token received, expected %v, got %v\n", i, tt.exceptCaseTokType, tok.Type)
					}
				} else {
					t.Errorf("%d Wrong token received, expected %v, got %v\n", i, tt.tokType, tok.Type)
				}
			}

			if tok.Literal != tt.value {
				t.Errorf("%d Wrong literal received, expected %v, got %v\n", i, tt.value, tok.Literal)
			}
		}
	}
}

func TestILLEGALTokens(t *testing.T) {
	inputQueries := []string{
		"!",
		"?",
		"**",
		"!=\"a\"",
	}

	expectedTokens := []struct {
		tokType           TokenType
		value             string
		exceptCase        bool
		exceptCaseTokType TokenType
	}{
		{
			tokType: ILLEGAL,
		},
		{
			tokType: ILLEGAL,
		},
		{
			tokType: ILLEGAL,
		},
		{
			tokType: ILLEGAL,
		},
	}
	for i, query := range inputQueries {
		l := NewLexer(query)

		tok := l.NextToken()

		if tok.Type != expectedTokens[i].tokType {
			t.Errorf("%d Wrong token received, expected %v, got %v\n", i, expectedTokens[i].tokType, tok.Type)
		}
	}
}

func TestWordChars(t *testing.T) {
	inputQueries := []string{
		"service=\"ap-south1\"",
		"service='ap-south1'",
		"service=ap-south1",
	}

	expectedTokens := []struct {
		tokType           TokenType
		value             string
		exceptCase        bool
		exceptCaseTokType TokenType
	}{
		{
			tokType:    IDENT,
			value:      "service",
			exceptCase: false,
		},
		{
			tokType:    EQ,
			value:      "=",
			exceptCase: false,
		},
		{
			tokType:           STRING,
			value:             "ap-south1",
			exceptCase:        true,
			exceptCaseTokType: IDENT,
		},
	}
	for _, query := range inputQueries {
		l := NewLexer(query)

		for i, tt := range expectedTokens {
			tok := l.NextToken()

			if tok.Type != tt.tokType {
				if tt.exceptCase {
					if tok.Type != tt.exceptCaseTokType {
						t.Errorf("%d Wrong token received, expected %v, got %v\n", i, tt.exceptCaseTokType, tok.Type)
					}
				} else {
					t.Errorf("%d Wrong token received, expected %v, got %v\n", i, tt.tokType, tok.Type)
				}
			}

			if tok.Literal != tt.value {
				t.Errorf("%d Wrong literal received, expected %v, got %v\n", i, tt.value, tok.Literal)
			}
		}
	}
}

func TestIndents(t *testing.T) {
	inputQueries := []string{
		"service=\"ap-south1\" AND level=a",
		"service='ap-south1' AND level=\"a\"",
		"service=ap-south1 AND level='a'",
	}

	expectedTokens := []struct {
		tokType           TokenType
		value             string
		exceptCase        bool
		exceptCaseTokType TokenType
	}{
		{
			tokType:    IDENT,
			value:      "service",
			exceptCase: false,
		},
		{
			tokType:    EQ,
			value:      "=",
			exceptCase: false,
		},
		{
			tokType:           STRING,
			value:             "ap-south1",
			exceptCase:        true,
			exceptCaseTokType: IDENT,
		},
		{
			tokType: AND,
			value:   "AND",
		},
		{
			tokType: IDENT,
			value:   "level",
		},
		{
			tokType:    EQ,
			value:      "=",
			exceptCase: false,
		},
		{
			tokType:           STRING,
			value:             "a",
			exceptCase:        true,
			exceptCaseTokType: IDENT,
		},
	}
	for _, query := range inputQueries {
		l := NewLexer(query)

		for i, tt := range expectedTokens {
			tok := l.NextToken()

			if tok.Type != tt.tokType {
				if tt.exceptCase {
					if tok.Type != tt.exceptCaseTokType {
						t.Errorf("%d Wrong token received, expected %v, got %v\n", i, tt.exceptCaseTokType, tok.Type)
					}
				} else {
					t.Errorf("%d Wrong token received, expected %v, got %v\n", i, tt.tokType, tok.Type)
				}
			}

			if tok.Literal != tt.value {
				t.Errorf("%d Wrong literal received, expected %v, got %v\n", i, tt.value, tok.Literal)
			}
		}
	}
}
