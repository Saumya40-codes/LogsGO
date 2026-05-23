package store

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	logapi "github.com/Saumya40-codes/LogsGO/api/grpc/pb"
	"github.com/Saumya40-codes/LogsGO/pkg/logsgoql"
	"github.com/Saumya40-codes/LogsGO/pkg/metrics"
	"github.com/dgraph-io/badger/v4"
)

type queryTimingCase struct {
	name       string
	query      string
	startTs    int64
	endTs      int64
	lookback   int64
	resolution int64
}

type queryTimingResult struct {
	name    string
	query   string
	series  int
	points  int
	elapsed time.Duration
}

func TestQueryTimingMemoryStore(t *testing.T) {
	if os.Getenv("LOGSGO_QUERY_TIMING") != "1" {
		t.Skip("set LOGSGO_QUERY_TIMING=1 to run query timing smoke test")
	}

	logCount := envInt("LOGSGO_QUERY_TIMING_LOGS", 100_000)
	baseTs := time.Now().Add(-1 * time.Hour).Unix()
	logs, _ := buildTimingLogs(logCount, baseTs)

	mem := NewMemoryStore(nil, "24h", int64(logCount*2), false, NewShardedLogIndex(), metrics.NewMetrics())
	defer func() {
		if err := mem.Close(); err != nil {
			t.Fatalf("close memory store: %v", err)
		}
	}()

	insertStart := time.Now()
	if err := mem.Insert(logs, nil); err != nil {
		t.Fatalf("insert synthetic logs: %v", err)
	}
	insertElapsed := time.Since(insertStart)

	results := runQueryTimingCases(t, mem, timingCases(baseTs, logCount))
	t.Log(formatQueryTimingReport("memory", logCount, insertElapsed, results))
}

func TestQueryTimingLocalStore(t *testing.T) {
	if os.Getenv("LOGSGO_QUERY_TIMING") != "1" {
		t.Skip("set LOGSGO_QUERY_TIMING=1 to run query timing smoke test")
	}

	logCount := envInt("LOGSGO_QUERY_TIMING_LOGS", 100_000)
	baseTs := time.Now().Add(-1 * time.Hour).Unix()
	logs, series := buildTimingLogs(logCount, baseTs)

	opts := badger.DefaultOptions(t.TempDir()).WithCompactL0OnClose(true).WithValueLogFileSize(16 << 20)
	opts.Logger = nil

	local, err := NewLocalStore(opts, nil, "24h", false, NewShardedLogIndex(), metrics.NewMetrics())
	if err != nil {
		t.Fatalf("create local store: %v", err)
	}
	defer func() {
		if err := local.Close(); err != nil {
			t.Fatalf("close local store: %v", err)
		}
	}()

	insertStart := time.Now()
	if err := local.Insert(logs, series); err != nil {
		t.Fatalf("insert synthetic logs: %v", err)
	}
	insertElapsed := time.Since(insertStart)

	results := runQueryTimingCases(t, local, timingCases(baseTs, logCount))
	t.Log(formatQueryTimingReport("local", logCount, insertElapsed, results))
}

func runQueryTimingCases(t *testing.T, s logsgoql.Store, cases []queryTimingCase) []queryTimingResult {
	t.Helper()

	results := make([]queryTimingResult, 0, len(cases))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := mustBuildTimingPlan(t, tc.query)
			queryCtx := logsgoql.QueryContext{
				StartTs:  tc.startTs,
				EndTs:    tc.endTs,
				Lookback: tc.lookback,
			}
			exec := logsgoql.NewExecutor(logsgoql.NewEngine(s), queryCtx)

			start := time.Now()
			var (
				series []logsgoql.Series
				err    error
			)
			if tc.resolution > 0 {
				series, err = exec.ExecuteRangeQuery(plan, tc.resolution)
			} else {
				series, err = exec.ExecuteQuery(plan)
			}
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			results = append(results, queryTimingResult{
				name:    tc.name,
				query:   tc.query,
				series:  len(series),
				points:  countTimingPoints(series),
				elapsed: elapsed,
			})
		})
	}
	return results
}

func buildTimingLogs(logCount int, baseTs int64) ([]*logapi.LogEntry, map[LogKey]map[int64]CounterValue) {
	logs := make([]*logapi.LogEntry, 0, logCount)
	series := make(map[LogKey]map[int64]CounterValue)
	counts := make(map[LogKey]uint64)

	for i := 0; i < logCount; i++ {
		service := fmt.Sprintf("service-%02d", i%32)
		level := "info"
		if i%10 == 0 {
			level = "error"
		} else if i%5 == 0 {
			level = "warn"
		}

		entry := &logapi.LogEntry{
			Service:   service,
			Level:     level,
			Message:   fmt.Sprintf("synthetic log message bucket-%03d", i%256),
			Timestamp: baseTs + int64(i),
		}
		logs = append(logs, entry)

		key := LogKey{Service: entry.Service, Level: entry.Level, Message: entry.Message}
		counts[key]++
		if series[key] == nil {
			series[key] = make(map[int64]CounterValue)
		}
		series[key][entry.Timestamp] = CounterValue{value: counts[key]}
	}

	return logs, series
}

func timingCases(baseTs int64, logCount int) []queryTimingCase {
	return []queryTimingCase{
		{
			name:     "instant-service-level",
			query:    `service=service-00&level=error`,
			startTs:  baseTs + int64(logCount-1),
			endTs:    baseTs + int64(logCount-1),
			lookback: int64(logCount),
		},
		{
			name:     "instant-or-services",
			query:    `service=service-00|service=service-01`,
			startTs:  baseTs + int64(logCount-1),
			endTs:    baseTs + int64(logCount-1),
			lookback: int64(logCount),
		},
		{
			name:       "range-service-level",
			query:      `service=service-00&level=error`,
			startTs:    baseTs,
			endTs:      baseTs + int64(logCount-1),
			lookback:   300,
			resolution: 60,
		},
	}
}

func mustBuildTimingPlan(t *testing.T, query string) *logsgoql.Plan {
	t.Helper()

	lexer := logsgoql.NewLexer(query)
	parser := logsgoql.NewParser(lexer)
	expr := parser.ParseExpression()
	if len(parser.Errors()) > 0 {
		t.Fatalf("parse query %q: %v", query, parser.Errors())
	}

	plan, err := logsgoql.BuildPlan(expr)
	if err != nil {
		t.Fatalf("build plan for %q: %v", query, err)
	}
	return plan
}

func countTimingPoints(series []logsgoql.Series) int {
	total := 0
	for _, s := range series {
		total += len(s.Points)
	}
	return total
}

func formatQueryTimingReport(storeName string, logCount int, insertElapsed time.Duration, results []queryTimingResult) string {
	var b strings.Builder

	fmt.Fprintf(&b, "\n\nQuery timing report\n")
	fmt.Fprintf(&b, "store:  %s\n", storeName)
	fmt.Fprintf(&b, "logs:   %d\n", logCount)
	fmt.Fprintf(&b, "insert: %s (%.2f logs/sec)\n\n", insertElapsed, float64(logCount)/insertElapsed.Seconds())

	fmt.Fprintf(&b, "%-24s %-38s %8s %8s %12s\n", "case", "query", "series", "points", "elapsed")
	fmt.Fprintf(&b, "%-24s %-38s %8s %8s %12s\n", strings.Repeat("-", 24), strings.Repeat("-", 38), strings.Repeat("-", 8), strings.Repeat("-", 8), strings.Repeat("-", 12))

	for _, r := range results {
		fmt.Fprintf(
			&b,
			"%-24s %-38s %8d %8d %12s\n",
			r.name,
			truncateTimingCell(r.query, 38),
			r.series,
			r.points,
			r.elapsed,
		)
	}

	return b.String()
}

func truncateTimingCell(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
