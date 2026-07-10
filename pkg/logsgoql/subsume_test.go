package logsgoql

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func mustPlan(t *testing.T, q string) *Plan {
	t.Helper()
	p := NewParser(NewLexer(q))
	expr := p.ParseExpression()
	testutil.Assert(t, len(p.Errors()) == 0, "parser errors: %v", p.Errors())
	plan, err := BuildPlan(expr)
	testutil.Ok(t, err)
	return plan
}

func TestPlanSubsetOf(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		policy string
		want   bool
	}{
		{"exact", `level=error`, `level=error`, true},
		{"query narrower via and", `level=error AND service=payments`, `level=error`, true},
		{"policy wider via or", `level=error`, `level=error OR level=warn`, true},
		{"query wider not covered", `level=error OR level=info`, `level=error`, false},
		{"disjoint", `service=auth`, `level=error`, false},
		{"multi-term all covered", `level=error OR service=payments`, `level=error OR service=payments OR level=warn`, true},
		{"multi-term one uncovered", `level=error OR service=billing`, `level=error OR service=payments`, false},
		{"and both sides in policy", `level=error AND service=payments`, `level=error OR service=payments`, true},
		{"policy and, query only one", `level=error`, `level=error AND service=payments`, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := mustPlan(t, tc.query)
			p := mustPlan(t, tc.policy)
			got := q.SubsetOf(p)
			testutil.Assert(t, got == tc.want, "SubsetOf(%q, %q) = %v, want %v", tc.query, tc.policy, got, tc.want)
		})
	}
}
