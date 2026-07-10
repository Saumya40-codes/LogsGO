package store

import (
	"testing"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
)

func buildStorePlan(t *testing.T, q string) *logsgoql.Plan {
	t.Helper()
	p := logsgoql.NewParser(logsgoql.NewLexer(q))
	expr := p.ParseExpression()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors for %q: %v", q, errs)
	}
	plan, err := logsgoql.BuildPlan(expr)
	if err != nil {
		t.Fatalf("build plan for %q: %v", q, err)
	}
	return plan
}

func newTestMemStore(t *testing.T, policy *CachePolicy) (*MemoryStore, *LocalStore) {
	t.Helper()
	index := NewShardedLogIndex()
	local, err := NewLocalStore(t.TempDir(), nil, "10m", false, index, metrics.NewMetrics())
	if err != nil {
		t.Fatalf("local store: %v", err)
	}
	var next Store = local
	mem := NewMemoryStore(&next, "1h", 0, policy, index, metrics.NewMetrics())
	t.Cleanup(func() { mem.Close() })
	return mem, local
}

func logEntry(ts int64, service, level, message string) *logapi.LogEntry {
	return &logapi.LogEntry{Timestamp: ts, Service: service, Level: level, Message: message}
}

func mustPolicy(t *testing.T, cfg CacheConfig) *CachePolicy {
	t.Helper()
	p, err := NewCachePolicy(cfg)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	return p
}

// Every log must reach the durable store, but only policy-matched logs are cached.
func TestWriteThroughAndSelectiveCache(t *testing.T) {
	policy := mustPolicy(t, CacheConfig{Enabled: true, Rules: []string{"level=error"}})
	mem, local := newTestMemStore(t, policy)

	ts := time.Now().Unix()
	logs := []*logapi.LogEntry{
		logEntry(ts, "svc", "error", "boom"),
		logEntry(ts, "svc", "info", "hello"),
	}
	if err := mem.Insert(logs, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if mem.totalLogs != 1 {
		t.Fatalf("expected 1 cached log, got %d", mem.totalLogs)
	}

	// durable store holds both series regardless of the cache policy
	got, err := local.getSeries(logsgoql.QueryContext{StartTs: ts, EndTs: ts}, buildStorePlan(t, "service=svc"))
	if err != nil {
		t.Fatalf("local query: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 series durably persisted, got %d", len(got))
	}
}

// A query subsumed by the policy is answered from the cache alone (next skipped),
// and matches what the merged path would return.
func TestShortCircuitCoverage(t *testing.T) {
	policy := mustPolicy(t, CacheConfig{Enabled: true, Rules: []string{"level=error"}})
	mem, _ := newTestMemStore(t, policy)

	ts := time.Now().Unix()
	if err := mem.Insert([]*logapi.LogEntry{
		logEntry(ts, "svc", "error", "boom"),
		logEntry(ts, "svc", "info", "hello"),
	}, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}

	queryCtx := logsgoql.QueryContext{StartTs: ts, EndTs: ts}

	if !mem.covers(queryCtx, buildStorePlan(t, "level=error")) {
		t.Fatal("expected level=error query to be covered by cache")
	}
	if mem.covers(queryCtx, buildStorePlan(t, "level=info")) {
		t.Fatal("level=info is outside the policy and must not be covered")
	}

	res, err := mem.Series(queryCtx, buildStorePlan(t, "level=error"))
	if err != nil {
		t.Fatalf("series: %v", err)
	}
	if len(res) != 1 || res[0].Level != "error" {
		t.Fatalf("expected single error series from cache, got %+v", res)
	}

	// uncovered query still resolves via the durable tier
	res, err = mem.Series(queryCtx, buildStorePlan(t, "level=info"))
	if err != nil {
		t.Fatalf("series: %v", err)
	}
	if len(res) != 1 || res[0].Level != "info" {
		t.Fatalf("expected info series from durable tier, got %+v", res)
	}
}

// Coverage must be denied when the query window predates the retained set.
func TestCoverageDeniedBelowFloor(t *testing.T) {
	policy := mustPolicy(t, CacheConfig{Enabled: true, Rules: []string{"level=error"}})
	mem, _ := newTestMemStore(t, policy)

	now := time.Now().Unix()
	if err := mem.Insert([]*logapi.LogEntry{logEntry(now, "svc", "error", "boom")}, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}

	below := logsgoql.QueryContext{StartTs: now - 3600, EndTs: now}
	if mem.covers(below, buildStorePlan(t, "level=error")) {
		t.Fatal("query starting before the cache floor must not be covered")
	}
}

// Oldest-first eviction keeps the retained set a contiguous suffix and raises the floor.
func TestSizeEvictionRaisesFloor(t *testing.T) {
	policy := mustPolicy(t, CacheConfig{Enabled: true, MaxEntries: 2, Rules: []string{"level=error"}})
	mem, _ := newTestMemStore(t, policy)

	if err := mem.Insert([]*logapi.LogEntry{
		logEntry(100, "svc", "error", "a"),
		logEntry(200, "svc", "error", "b"),
		logEntry(300, "svc", "error", "c"),
	}, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}

	mem.mu.Lock()
	total := mem.totalLogs
	floor := mem.oldestKeyLocked()
	mem.mu.Unlock()

	if total != 2 {
		t.Fatalf("expected 2 retained after eviction, got %d", total)
	}
	if floor != 200 {
		t.Fatalf("expected floor to rise to 200, got %d", floor)
	}
}

// A late log below the current floor is written through but not cached, so the
// retained set stays a gap-free suffix and coverage stays sound.
func TestLateLogBelowFloorNotCached(t *testing.T) {
	policy := mustPolicy(t, CacheConfig{Enabled: true, Rules: []string{"level=error"}})
	mem, local := newTestMemStore(t, policy)

	if err := mem.Insert([]*logapi.LogEntry{logEntry(200, "svc", "error", "b")}, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := mem.Insert([]*logapi.LogEntry{
		logEntry(100, "svc", "error", "a"),
		logEntry(300, "svc", "error", "c"),
	}, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}

	mem.mu.Lock()
	total := mem.totalLogs
	floor := mem.oldestKeyLocked()
	mem.mu.Unlock()

	if total != 2 || floor != 200 {
		t.Fatalf("expected 2 cached with floor 200, got total=%d floor=%d", total, floor)
	}

	// the late log is still durably queryable
	got, err := local.getSeries(logsgoql.QueryContext{StartTs: 100, EndTs: 100}, buildStorePlan(t, "level=error"))
	if err != nil {
		t.Fatalf("local query: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected late log persisted durably, got %d series", len(got))
	}
}

// A disabled policy caches nothing but still writes through.
func TestDisabledPolicyPassthrough(t *testing.T) {
	mem, local := newTestMemStore(t, mustPolicy(t, CacheConfig{Enabled: false}))

	ts := time.Now().Unix()
	if err := mem.Insert([]*logapi.LogEntry{logEntry(ts, "svc", "error", "boom")}, nil, ""); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if mem.totalLogs != 0 {
		t.Fatalf("disabled policy must cache nothing, got %d", mem.totalLogs)
	}
	got, err := local.getSeries(logsgoql.QueryContext{StartTs: ts, EndTs: ts}, buildStorePlan(t, "service=svc"))
	if err != nil {
		t.Fatalf("local query: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected log written through to durable store, got %d series", len(got))
	}
}
