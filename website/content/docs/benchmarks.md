---
title: Benchmarks
description: Ingest, concurrent query, and S3 query reference numbers.
weight: 80
---

Numbers from project runs (see also [`/bench`](https://github.com/Saumya40-codes/LogsGO/tree/main/bench)). No in-tree load harness.

Setup:

- Pebble local store; MinIO for the S3 path (parquet blocks)
- Cache: `service=bench` \| `level=error`, `ttl=4m`, `max_entries=100000`
- **55%** hot (`service=bench`), **45%** cold (`service=other`)
- Batch 2000, 8 workers; no message queue

## Concurrent ingest + query (local)

| | Ingest | Query |
|--|-------:|------:|
| Volume | 1,000,000 logs | 2,000 queries |
| Errors | 0 | 0 |
| Wall time | **5.868 s** | **5.981 s** |
| Throughput | **~170k logs/s** | **~334 qps** |
| Latency | avg batch 92 ms | avg **11 ms** (1–62 ms) |

## S3 / bucket query path

| | Ingest | Query (post-flush) |
|--|-------:|------:|
| Volume | 200,000 logs | 1,000 queries |
| Errors | 0 | 0 |
| Wall time | **1.065 s** | **4.676 s** |
| Throughput | **~188k logs/s** | **~214 qps** |
| Latency | avg batch 81 ms | avg **18 ms** (3–42 ms) |

Hardware and flags change absolute numbers; treat tables as reference.
