package logsgoql

import (
	"reflect"
	"testing"
)

func TestParser(t *testing.T) {
	queries := []struct {
		input       string
		expected    Expr
		expectedErr bool
	}{
		{
			input: "level=\"error\" AND message = \"failed to connect\"",
			expected: &BinaryExpr{
				Left: &ConditionExpr{
					Ident:    "level",
					Operator: EQ,
					Value:    "error",
				},
				Operator: AND,
				Right: &ConditionExpr{
					Ident:    "message",
					Operator: EQ,
					Value:    "failed to connect",
				},
			},
		},
		{
			input: "level=\"error\" OR message = \"failed to connect\"",
			expected: &BinaryExpr{
				Left: &ConditionExpr{
					Ident:    "level",
					Operator: EQ,
					Value:    "error",
				},
				Operator: OR,
				Right: &ConditionExpr{
					Ident:    "message",
					Operator: EQ,
					Value:    "failed to connect",
				},
			},
		},
		{
			input: "service=\"ap-south1\" OR message = \"failed to connect\"",
			expected: &BinaryExpr{
				Left: &ConditionExpr{
					Ident:    "service",
					Operator: EQ,
					Value:    "ap-south1",
				},
				Operator: OR,
				Right: &ConditionExpr{
					Ident:    "message",
					Operator: EQ,
					Value:    "failed to connect",
				},
			},
		},
		{
			input: "service=\"ap-south1\" AND message = \"failed to connect\" OR service=\"us-west1\"",
			expected: &BinaryExpr{
				Left: &BinaryExpr{
					Left: &ConditionExpr{
						Ident:    "service",
						Operator: EQ,
						Value:    "ap-south1",
					},
					Operator: AND,
					Right: &ConditionExpr{
						Ident:    "message",
						Operator: EQ,
						Value:    "failed to connect",
					},
				},
				Operator: OR,
				Right: &ConditionExpr{
					Ident:    "service",
					Operator: EQ,
					Value:    "us-west1",
				},
			},
		},
		{
			input: "service=\"ap-south1\" OR message CONTAINS \"failed\"",
			expected: &BinaryExpr{
				Left: &ConditionExpr{
					Ident:    "service",
					Operator: EQ,
					Value:    "ap-south1",
				},
				Operator: OR,
				Right: &ConditionExpr{
					Ident:    "message",
					Operator: CONTAINS,
					Value:    "failed",
				},
			},
			expectedErr: true,
		},
		{
			input:       "service=\"ap-south1\" YOLO message CONTAINS \"failed\"",
			expected:    nil,
			expectedErr: true,
		},
		{
			input:       "service=\"ap-south1\" YOLO message CONTAINS \"failed\"",
			expected:    nil,
			expectedErr: true,
		},
		{
			// YOLO one
			input: "service=service AND message = message",
			expected: &BinaryExpr{
				Left: &ConditionExpr{
					Ident:    "service",
					Operator: EQ,
					Value:    "service",
				},
				Operator: AND,
				Right: &ConditionExpr{
					Ident:    "message",
					Operator: EQ,
					Value:    "message",
				},
			},
		},
	}

	for _, query := range queries {
		lexer := NewLexer(query.input)
		parser := NewParser(lexer)
		result := parser.ParseExpression()
		if !reflect.DeepEqual(result, query.expected) && !query.expectedErr {
			t.Errorf("For input '%s'\n, expected '%v'\n, but got '%v'", query.input, query.expected.exprNode(), result.exprNode())
		}
		if query.expectedErr && len(parser.Errors()) == 0 {
			t.Errorf("For input '%s', expected error but got none", query.input)
		}
	}
}
