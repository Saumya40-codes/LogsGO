package logsgoql

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestBuildPlan_Match(t *testing.T) {
	l := NewLexer(`service="auth" OR level=error OR level=warn OR service="payment"`)
	p := NewParser(l)
	expr := p.ParseExpression()
	testutil.Assert(t, len(p.Errors()) == 0, "parser errors: %v", p.Errors())

	plan, err := BuildPlan(expr)
	testutil.Ok(t, err)

	ok, err := plan.Match(EntryLabels{Service: "warn", Level: "error", Message: "yolo"})
	testutil.Ok(t, err)
	testutil.Assert(t, ok, "expected match")

	ok, err = plan.Match(EntryLabels{Service: "auth", Level: "info", Message: "x"})
	testutil.Ok(t, err)
	testutil.Assert(t, ok, "expected match")

	ok, err = plan.Match(EntryLabels{Service: "notification", Level: "info", Message: "x"})
	testutil.Ok(t, err)
	testutil.Assert(t, !ok, "expected no match")
}

func TestBuildPlan_InvalidField(t *testing.T) {
	l := NewLexer(`unknown=foo`)
	p := NewParser(l)
	expr := p.ParseExpression()
	testutil.Assert(t, len(p.Errors()) == 0, "parser errors: %v", p.Errors())

	_, err := BuildPlan(expr)
	testutil.Assert(t, err != nil, "expected error")
}

func TestBuildPlan_ContainsNotSupported(t *testing.T) {
	l := NewLexer(`message CONTAINS "oops"`)
	p := NewParser(l)
	expr := p.ParseExpression()

	// Parser already flags CONTAINS as unsupported in v1, but still produces an Expr.
	testutil.Assert(t, len(p.Errors()) > 0, "expected parser errors")

	_, err := BuildPlan(expr)
	testutil.Assert(t, err != nil, "expected error")
}
