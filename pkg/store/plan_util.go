package store

import "github.com/Saumya40-codes/LogsGO/pkg/logsgoql"

func planUsesField(n logsgoql.Node, field logsgoql.Field) bool {
	switch x := n.(type) {
	case *logsgoql.MatchNode:
		return x.Field == field
	case *logsgoql.BinaryNode:
		return planUsesField(x.Left, field) || planUsesField(x.Right, field)
	}

	return false
}

func requiredEqSet(n logsgoql.Node, field logsgoql.Field) (set map[string]struct{}, ok bool) {
	switch x := n.(type) {
	case *logsgoql.MatchNode:
		if x.Field == field && x.Op == logsgoql.MatchEq {
			return map[string]struct{}{x.Value: {}}, true
		}
		return nil, false

	case *logsgoql.BinaryNode:
		left, leftOK := requiredEqSet(x.Left, field)
		right, rightOK := requiredEqSet(x.Right, field)

		switch x.Op {
		case logsgoql.OpAnd:
			switch {
			case leftOK && rightOK:
				out := make(map[string]struct{})
				for s := range left {
					if _, ok := right[s]; ok {
						out[s] = struct{}{}
					}
				}
				return out, true
			case leftOK:
				return left, true
			case rightOK:
				return right, true
			default:
				return nil, false
			}

		case logsgoql.OpOr:
			if !leftOK || !rightOK {
				return nil, false
			}
			out := make(map[string]struct{}, len(left)+len(right))
			for s := range left {
				out[s] = struct{}{}
			}
			for s := range right {
				out[s] = struct{}{}
			}
			return out, true

		default:
			return nil, false
		}

	default:
		return nil, false
	}
}
