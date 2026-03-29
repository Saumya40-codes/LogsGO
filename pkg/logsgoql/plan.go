package logsgoql

import (
	"fmt"
	"strings"
)

// Plan is a compiled, store-friendly representation of an AST Expr.
//
// Stores can evaluate this directly against candidate series keys, or push down
// parts of it to indexes/backends later.
type Plan struct {
	Root Node
}

type Node interface {
	planNode()
}

type LogicalOp uint8

const (
	OpAnd LogicalOp = iota + 1
	OpOr
)

type BinaryNode struct {
	Op    LogicalOp
	Left  Node
	Right Node
}

func (*BinaryNode) planNode() {}

type Field uint8

const (
	FieldService Field = iota + 1
	FieldLevel
	FieldMessage
)

type MatchOp uint8

const (
	MatchEq MatchOp = iota + 1
)

type MatchNode struct {
	Field Field
	Op    MatchOp
	Value string
}

func (*MatchNode) planNode() {}

// EntryLabels are the label-like dimensions used to identify a log series.
// This matches logsgoql.Series fields (Service, Level, Message).
type EntryLabels struct {
	Service string
	Level   string
	Message string
}

func BuildPlan(expr Expr) (*Plan, error) {
	root, err := buildNode(expr)
	if err != nil {
		return nil, err
	}
	return &Plan{Root: root}, nil
}

func buildNode(expr Expr) (Node, error) {
	if expr == nil {
		return nil, ErrInvalidQuery
	}

	switch e := expr.(type) {
	case *BinaryExpr:
		left, err := buildNode(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildNode(e.Right)
		if err != nil {
			return nil, err
		}

		var op LogicalOp
		switch e.Operator {
		case AND:
			op = OpAnd
		case OR:
			op = OpOr
		default:
			return nil, fmt.Errorf("%w: unsupported logical operator %q", ErrInvalidQuery, e.Operator)
		}

		return &BinaryNode{Op: op, Left: left, Right: right}, nil

	case *ConditionExpr:
		field, err := parseField(e.Ident)
		if err != nil {
			return nil, err
		}

		switch e.Operator {
		case EQ:
			return &MatchNode{Field: field, Op: MatchEq, Value: e.Value}, nil
		case CONTAINS:
			return nil, fmt.Errorf("%w: CONTAINS not supported", ErrNotImplemented)
		default:
			return nil, fmt.Errorf("%w: unsupported match operator %q", ErrInvalidQuery, e.Operator)
		}

	default:
		return nil, fmt.Errorf("%w: unsupported expression type %T", ErrInvalidQuery, expr)
	}
}

func parseField(ident string) (Field, error) {
	switch strings.ToLower(strings.TrimSpace(ident)) {
	case "service":
		return FieldService, nil
	case "level":
		return FieldLevel, nil
	case "message":
		return FieldMessage, nil
	default:
		return 0, fmt.Errorf("%w: unknown field %q", ErrInvalidLabel, ident)
	}
}

// Match evaluates the plan against a single series key (service/level/message).
func (p *Plan) Match(labels EntryLabels) (bool, error) {
	if p == nil || p.Root == nil {
		return false, ErrInvalidQuery
	}
	return matchNode(p.Root, labels)
}

func matchNode(n Node, labels EntryLabels) (bool, error) {
	switch x := n.(type) {
	case *BinaryNode:
		l, err := matchNode(x.Left, labels)
		if err != nil {
			return false, err
		}
		switch x.Op {
		case OpAnd:
			if !l {
				return false, nil
			}
			r, err := matchNode(x.Right, labels)
			if err != nil {
				return false, err
			}
			return l && r, nil
		case OpOr:
			if l {
				return true, nil
			}
			r, err := matchNode(x.Right, labels)
			if err != nil {
				return false, err
			}
			return l || r, nil
		default:
			return false, fmt.Errorf("%w: unknown logical op %d", ErrInternal, x.Op)
		}

	case *MatchNode:
		var got string
		switch x.Field {
		case FieldService:
			got = labels.Service
		case FieldLevel:
			got = labels.Level
		case FieldMessage:
			got = labels.Message
		default:
			return false, fmt.Errorf("%w: unknown field %d", ErrInternal, x.Field)
		}

		switch x.Op {
		case MatchEq:
			return got == x.Value, nil
		default:
			return false, fmt.Errorf("%w: unknown match op %d", ErrInternal, x.Op)
		}

	default:
		return false, fmt.Errorf("%w: unknown plan node %T", ErrInternal, n)
	}
}
