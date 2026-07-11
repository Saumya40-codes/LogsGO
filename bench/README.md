# LogsGo benchmarks

Reference numbers from project runs. No load harness is shipped in-tree.

Setup for these runs:

- Pebble local store; optional MinIO cold tier (parquet blocks)
- Cache policy: `service=bench` OR `level=error`, `ttl=4m`, `max_entries=100000`
- Traffic mix: **55%** `service=bench` (cache-eligible), **45%** `service=other`
- Batch size 2000, 8 ingest workers
- No message queue

## Concurrent ingest + query (local)

1M gRPC batch ingest while running 2k instant HTTP queries.

| | Ingest | Query |
|--|-------:|------:|
| Volume | 1,000,000 logs | 2,000 queries |
| Errors | 0 | 0 |
| Wall time | **5.868 s** | **5.981 s** |
| Throughput | **~170k logs/s** | **~334 qps** |
| Latency | avg batch 92 ms | avg **11 ms** (1–62 ms) |

## S3 / bucket query path

200k backdated logs → local→MinIO parquet flush → 1k range queries.

| | Ingest | Query (post-flush) |
|--|-------:|------:|
| Volume | 200,000 logs | 1,000 queries |
| Errors | 0 | 0 |
| Wall time | **1.065 s** | **4.676 s** |
| Throughput | **~188k logs/s** | **~214 qps** |
| Latency | avg batch 81 ms | avg **18 ms** (3–42 ms) |

## Earlier ingest-only (for comparison)

Direct gRPC 1M (55% hot / 45% cold), no concurrent queries:

| Metric | Value |
|--------|------:|
| Time | 15.520 s |
| Logs/s | ~64,432 |
| Errors | 0 |
