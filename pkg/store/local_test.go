package store

import (
	"testing"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
)

func newTestLocalStore(t *testing.T) *LocalStore {
	t.Helper()
	l, err := NewLocalStore(t.TempDir(), nil, "10m", false, NewShardedLogIndex(), metrics.NewMetrics())
	if err != nil {
		t.Fatalf("failed to create local store: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func insertTestLogs(t *testing.T, l *LocalStore, ts int64) {
	t.Helper()
	logs := []*logapi.LogEntry{{Timestamp: ts, Level: "error", Service: "svc", Message: "boom"}}
	series := map[LogKey]map[int64]CounterValue{
		{Service: "svc", Level: "error", Message: "boom"}: {ts: {value: 3}},
	}
	if err := l.Insert(logs, series, ""); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
}

func TestFlushMarkerRoundTrip(t *testing.T) {
	l := newTestLocalStore(t)

	marker, err := l.readFlushMarker()
	if err != nil || marker != nil {
		t.Fatalf("expected no marker, got %v (err %v)", marker, err)
	}

	want := flushMarker{FlushID: "id-123", Cutoff: 42}
	if err := l.writeFlushMarker(want); err != nil {
		t.Fatalf("write marker failed: %v", err)
	}

	got, err := l.readFlushMarker()
	if err != nil {
		t.Fatalf("read marker failed: %v", err)
	}
	if got == nil || got.FlushID != want.FlushID || got.Cutoff != want.Cutoff {
		t.Fatalf("marker mismatch: got %+v want %+v", got, want)
	}
}

func TestMarkerKeyNotParsedAsLog(t *testing.T) {
	if _, _, _, _, _, ok := parseLocalStoreKey(flushMarkerKey); ok {
		t.Fatal("flush marker key must not parse as a log key")
	}
}

func TestFlushDeletesExpiredRangeAndMarker(t *testing.T) {
	l := newTestLocalStore(t)

	old := time.Now().Unix() - 3600
	insertTestLogs(t, l, old)

	if err := l.writeFlushMarker(flushMarker{FlushID: "crashed", Cutoff: time.Now().Unix() - 1800}); err != nil {
		t.Fatalf("write marker failed: %v", err)
	}

	if err := l.Flush(FlushConfig{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	marker, err := l.readFlushMarker()
	if err != nil {
		t.Fatalf("read marker failed: %v", err)
	}
	if marker != nil {
		t.Fatalf("marker should be cleared after flush, got %+v", marker)
	}

	it, err := l.db.Conn.NewIter(nil)
	if err != nil {
		t.Fatalf("iterator failed: %v", err)
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		t.Fatalf("expected empty db after flush, found key %x", it.Key())
	}
}

func TestFlushRespectsMarkerCutoff(t *testing.T) {
	l := newTestLocalStore(t)

	now := time.Now().Unix()
	oldTs := now - 3600
	newerTs := now - 600
	insertTestLogs(t, l, oldTs)
	insertTestLogs(t, l, newerTs)

	if err := l.writeFlushMarker(flushMarker{FlushID: "crashed", Cutoff: now - 1800}); err != nil {
		t.Fatalf("write marker failed: %v", err)
	}

	if err := l.Flush(FlushConfig{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	it, err := l.db.Conn.NewIter(nil)
	if err != nil {
		t.Fatalf("iterator failed: %v", err)
	}
	defer it.Close()

	var remaining []int64
	for it.First(); it.Valid(); it.Next() {
		ts, _, _, _, _, ok := parseLocalStoreKey(it.Key())
		if ok {
			remaining = append(remaining, ts)
		}
	}
	if len(remaining) != 1 || remaining[0] != newerTs {
		t.Fatalf("expected only the newer log to remain, got %v", remaining)
	}
}
