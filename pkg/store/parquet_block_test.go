package store

import (
	"bytes"
	"testing"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/parquet-go/parquet-go"
)

func TestParquetRoundTrip(t *testing.T) {
	ts := time.Now().Unix()
	in := []*logapi.Series{
		{Entry: &logapi.LogEntry{Timestamp: ts, Service: "svc", Level: "error", Message: "boom", Labels: map[string]string{"k": "v"}}, Count: 3},
		{Entry: &logapi.LogEntry{Timestamp: ts + 1, Service: "svc", Level: "info", Message: "ok"}, Count: 1},
	}

	data, err := encodeSeriesParquet(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty parquet payload")
	}

	got, err := loadAllSeriesFromParquet(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 series, got %d", len(got))
	}
	if got[0].Entry.Level != "error" || got[0].Count != 3 || got[0].Entry.Labels["k"] != "v" {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[1].Entry.Level != "info" {
		t.Fatalf("unexpected second row: %+v", got[1])
	}
}

func TestParquetRowGroupTimeSkip(t *testing.T) {
	const n = parquetRowsPerGroup*2 + 10
	entries := make([]*logapi.Series, 0, n)
	base := int64(1_700_000_000)
	for i := 0; i < n; i++ {
		entries = append(entries, &logapi.Series{
			Entry: &logapi.LogEntry{
				Timestamp: base + int64(i),
				Service:   "svc",
				Level:     "info",
				Message:   "m",
			},
			Count: 1,
		})
	}
	data, err := encodeSeriesParquet(entries)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	start := base + int64(n-5)
	end := base + int64(n)
	plan := buildStorePlan(t, `level=info`)
	got, err := seriesFromParquetReaderAt(bytes.NewReader(data), int64(len(data)), plan, start, end)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 rows in window, got %d", len(got))
	}
	for _, s := range got {
		if s.Entry.Timestamp < start || s.Entry.Timestamp > end {
			t.Fatalf("row outside window: %d", s.Entry.Timestamp)
		}
	}

	errPlan := buildStorePlan(t, `level=error`)
	got, err = seriesFromParquetReaderAt(bytes.NewReader(data), int64(len(data)), errPlan, base, end)
	if err != nil {
		t.Fatalf("scan error plan: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 error rows, got %d", len(got))
	}
}

func TestParquetPlanFilter(t *testing.T) {
	ts := time.Now().Unix()
	in := []*logapi.Series{
		{Entry: &logapi.LogEntry{Timestamp: ts, Service: "payments", Level: "error", Message: "x"}, Count: 1},
		{Entry: &logapi.LogEntry{Timestamp: ts, Service: "auth", Level: "info", Message: "y"}, Count: 1},
		{Entry: &logapi.LogEntry{Timestamp: ts, Service: "payments", Level: "info", Message: "z"}, Count: 2},
	}
	data, err := encodeSeriesParquet(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	plan := buildStorePlan(t, `service=payments`)
	got, err := seriesFromParquetReaderAt(bytes.NewReader(data), int64(len(data)), plan, ts, ts)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 payments rows, got %d", len(got))
	}
}

func TestParquetEncodingsApplied(t *testing.T) {
	n := parquetRowsPerGroup + 1
	entries := make([]*logapi.Series, 0, n)
	base := int64(1000)
	for i := 0; i < n; i++ {
		lvl := "info"
		if i%2 == 0 {
			lvl = "error"
		}
		entries = append(entries, &logapi.Series{
			Entry: &logapi.LogEntry{Timestamp: base + int64(i), Service: "svc", Level: lvl, Message: "m"},
			Count: uint64(i),
		})
	}
	data, err := encodeSeriesParquet(entries)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(f.RowGroups()) < 2 {
		t.Fatalf("expected >=2 row groups, got %d", len(f.RowGroups()))
	}
	rg := f.RowGroups()[0]
	idx := columnIndexByName(rg, "timestamp")
	if idx < 0 {
		t.Fatal("timestamp column missing")
	}
	b, ok := rg.ColumnChunks()[idx].(columnBounds)
	if !ok {
		t.Fatal("timestamp chunk missing Bounds")
	}
	minV, maxV, has := b.Bounds()
	if !has || maxV.Int64() < minV.Int64() {
		t.Fatalf("bad bounds min=%v max=%v has=%v", minV, maxV, has)
	}
}
