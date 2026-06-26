# Chaos testing LogsGO

Heavy-load scenarios against a local LogsGO + Prometheus stack, with pprof and PromQL observation.

## Stack

```bash
# Prometheus (host network scrapes :8080)
docker run -d --name logsgo-prom --network host \
  -v /tmp/logsgo-chaos/prometheus-host.yml:/etc/prometheus/prometheus.yml:ro \
  prom/prometheus:latest \
  --config.file=/etc/prometheus/prometheus.yml \
  --web.listen-address=0.0.0.0:9090 \
  --enable-feature=native-histograms,promql-experimental-functions

# LogsGO
./logsGo --data-dir=/tmp/logsgo-chaos/data \
  --http-listen-addr=0.0.0.0:8080 --grpc-listen-addr=0.0.0.0:50051 \
  --max-time-in-mem=30s --max-logs-in-mem=2000 --max-retention-time=5m --unlock-data-dir
```

Prometheus MCP (`pab1it0/prometheus-mcp-server`) is configured with `PROMETHEUS_URL=http://127.0.0.1:9090`.

## Load generator

```bash
go build -o chaos/loadgen ./chaos/loadgen.go
./chaos/loadgen -scenario all -duration 25s -workers 48 -batch 40 -services 30
```

Scenarios: `ingest`, `batch`, `cardinality`, `burst`, `query`, `mixed`.

Queries use LogsGO QL (`service="x" AND level="INFO"`), not PromQL brace selectors.

## Baseline findings (pre-fix)

| Signal | Observation |
| --- | --- |
| Throughput | ~30k–100k+ logs/s ingest depending on scenario |
| CPU (pprof) | ~30% in GC; hot paths `MemoryStore.Insert/Flush`, `LocalStore.Insert` |
| Heap | ~650MB; dominated by `MemoryStore.Insert`, `ShardedLogIndex.Inc` |
| Queries | 100% failures in loadgen used PromQL `{service=...}` (invalid grammar) |
| `/api/v1/labels` | ~1.2s under high cardinality |
| Metrics | `FlushDuration` registered under wrong name and never exposed; no memory gauge; no query metrics |
| Contended lock | single `sync.Mutex` on memory + local blocked readers during writes |

## Fixes on this branch

1. **Metrics**: `flush_duration_seconds` (was misnamed/unregistered), `query_duration_seconds`, `query_errors_total`; memory + local `current_logs_ingested` gauges kept accurate on insert/flush; `logs_flushed_total` on memory→local flush.
2. **RWMutex** on memory and local stores so `Series` / `LabelValues` can proceed under RLock.
3. **Local meta cache**: keep `meta.json` in memory; avoid full re-read on every insert batch.
4. **Flush robustness**: skip orphan skiplist entries instead of aborting the whole memory flush.
5. **pprof**: enable mutex/block profiles at process start for contention debugging.
6. **Chaos harness**: `chaos/loadgen.go` for multi-scenario load.

## Post-fix snapshot (mixed, 15s, 32 workers)

- `ok_logs=61864`, `fail_ops=0`, `queries_ok=1426`
- Labels endpoint ~7ms
- Flush histogram and memory gauge visible in Prometheus

## Useful PromQL

```promql
sum by (store) (rate(logs_ingested_total[1m]))
histogram_quantile(0.99, sum by (le, store) (rate(ingestion_duration_seconds_bucket[1m])))
sum by (from, to) (rate(flush_duration_seconds_count[1m]))
sum by (type) (rate(query_duration_seconds_count[1m]))
sum by (reason) (rate(query_errors_total[1m]))
current_logs_ingested
```

## pprof

```bash
go tool pprof -http=:0 http://127.0.0.1:8080/debug/pprof/profile?seconds=15
go tool pprof http://127.0.0.1:8080/debug/pprof/heap
go tool pprof http://127.0.0.1:8080/debug/pprof/mutex
```
