---
title: Storage tiers
description: Memory skiplist, BadgerDB local store, and S3-compatible bucket store.
weight: 30
---

LogsGo keeps writes simple and retention multi-tiered. Each tier optimizes for a different access pattern.

## 1. In-memory store

- Backed by a **skiplist** for O(log n) insertion and query.
- Ideal for **hot** data and low-latency instant queries.
- Eviction / flush driven by:
  - `--max-time-in-mem` (default `1h`)
  - `--max-logs-in-mem` (default `10000`)

## 2. Local store (BadgerDB)

- Persistent **on-disk** store under `--data-dir` (default `./data`).
- Receives flushed memory segments.
- Use `--unlock-data-dir` only if you understand multi-process access risks (not recommended for production).

## 3. Cloud / bucket store

Configure an S3-compatible backend (AWS S3, MinIO, etc.) via YAML:

```yaml
remote_store:
  provider: "minio"
  bucket: "bkt1"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  create_bucket_on_empty: true
  endpoint: "minio:9000"
```

Pass with `--store-config-path=store_config.yaml` or inline `--store-config=...` (mutually exclusive).

## Compaction

Object-store blocks can be **compacted** (e.g. several 2h blocks into one 12h block) on a schedule:

- `--compact-duration` (default `12h`) — how often compaction cycles run
- Internal compact configuration controls block windows / downsampling behavior

Compaction reduces object count and can improve **range / deep historical** query performance.

## Chaining semantics

Operations are delegated along `.next`:

- **Flush**: memory → local → bucket
- **Query**: fan-out / sequential chain with merge and dedupe

You can run **memory + local only** for simpler deployments, or full three-tier for maximum retention at lower cost.
