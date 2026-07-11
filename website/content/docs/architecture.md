---
title: Architecture
description: End-to-end design — ingestion, tiered stores, chaining, and query path.
weight: 10
---

LogsGo is designed as a **single binary** service with three main surfaces:

| Surface | Default | Role |
|--------|---------|------|
| gRPC | `:50051` | Log ingestion (`UploadLog`, `UploadLogs`) |
| REST HTTP | `:8080` | Query API, label discovery, metrics, pprof |
| Web UI | `:19091` | Embedded dashboard for express queries and charts |

## High-level flow

![Architecture overview](/images/archv1.png)

*Architecture overview from the repository docs*

1. **Clients** push logs with the Go `logclient` (or any gRPC client implementing the proto).
2. The **ingestion server** accepts entries at the head of the chain — the **in-memory store**, which now behaves as a **write-through cache**.
3. Every log is **written through to the local [Pebble](https://github.com/cockroachdb/pebble) store** for durability. In addition, logs matching the **cache policy** (`--cache-config-path`) are retained in the skiplist-backed cache for low-latency hot queries. Logs that match no rule live only in Pebble.
4. If an S3-compatible **remote store** is configured, local blocks are further flushed to object storage for long retention.
5. **Queries** start at the head of the chain. When the request is fully covered by the cache, it is served from memory alone; otherwise it walks each store via `.next`, merging results with **deduplication** (parent tier preferred on conflicts). Either way the result is identical — Pebble always holds the full set.

## Store chain

Each store implements a common interface and optionally holds a pointer to the **next** tier. Flush and query operations are **transparent** along the chain:

```
MemoryStore (cache) → LocalStore (Pebble) → BucketStore (S3 / MinIO)
```

- **Memory**: write-through cache holding a policy-selected hot subset; evicted by TTL and size, never flushed downward (Pebble already has the data).
- **Local**: durable source of truth for every log; on-disk under `--data-dir`.
- **Bucket**: cold tier; Parquet time blocks on S3/MinIO with ranged reads and **compaction** for cheaper deep scans.

## Optional async path

When `--queue-config-path` is set, ingestion workers can consume from **RabbitMQ**. Producers may publish serialized batches to the queue for decoupling; workers pull in **round-robin** across `numWorkers`. See [Message queue]({{% ref "/docs/queueing" %}}).

## Query plane

REST exposes `/api/v1/query` and related endpoints. The engine builds an **execution plan** from LogsGoQL and runs it **concurrently** across stores. Instant queries use `--lookback-period` when you query “now”; range queries step by `resolution` from `start` to `end`. Details: [Query execution]({{% ref "/docs/query-execution" %}}).

## Process model

On startup (`logsgo` / `logsGo` binary), the process starts:

1. gRPC ingestion (optional queue consumers)
2. REST API (Gin) with optional JWT middleware
3. Static UI server for the Vite-built dashboard

Graceful shutdown listens for `SIGINT` / `SIGTERM`, cancels the root context, and waits on worker groups. Because logs are written through to Pebble on ingest, there is no in-memory-only window to lose; `--flush-on-exit` still governs the local → bucket flush for stronger cold-tier persistence.
