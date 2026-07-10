package logsgoql

type atom struct {
	field     Field
	labelName string
	value     string
}

type term map[atom]struct{}

// planToDNF flattens a monotone AND/OR plan into disjunctive normal form: a
// slice of conjunctive terms, each a set of equality atoms. Returns nil for any
// node outside the supported fragment, signalling "cannot reason about this".
func planToDNF(n Node) []term {
	switch x := n.(type) {
	case *MatchNode:
		if x.Op != MatchEq {
			return nil
		}
		return []term{{atom{x.Field, x.LabelName, x.Value}: {}}}
	case *BinaryNode:
		l := planToDNF(x.Left)
		r := planToDNF(x.Right)
		if l == nil || r == nil {
			return nil
		}
		switch x.Op {
		case OpOr:
			return append(l, r...)
		case OpAnd:
			out := make([]term, 0, len(l)*len(r))
			for _, lt := range l {
				for _, rt := range r {
					out = append(out, mergeTerms(lt, rt))
				}
			}
			return out
		default:
			return nil
		}
	default:
		return nil
	}
}

func mergeTerms(a, b term) term {
	out := make(term, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

func subset(sub, super term) bool {
	if len(sub) > len(super) {
		return false
	}
	for k := range sub {
		if _, ok := super[k]; !ok {
			return false
		}
	}
	return true
}

// SubsetOf reports whether every log matching p also matches other, i.e. p ⊆
// other. It is sound but conservative: outside the pure AND/OR-of-equalities
// fragment it returns false, so callers can safely treat false as "unknown".
func (p *Plan) SubsetOf(other *Plan) bool {
	if p == nil || p.Root == nil || other == nil || other.Root == nil {
		return false
	}

	pDNF := planToDNF(p.Root)
	oDNF := planToDNF(other.Root)
	if pDNF == nil || oDNF == nil || len(pDNF) == 0 || len(oDNF) == 0 {
		return false
	}

	for _, pt := range pDNF {
		covered := false
		for _, ot := range oDNF {
			if subset(ot, pt) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}
