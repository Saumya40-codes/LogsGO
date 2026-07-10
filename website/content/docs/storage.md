---
title: Storage tiers
description: Memory skiplist, Pebble local store, and S3-compatible bucket store.
weight: 30
---

LogsGo keeps writes simple and retention multi-tiered. Each tier optimizes for a different access pattern.

## 1. In-memory store (cache)

- Backed by a **skiplist** for O(log n) insertion and query.
- A **write-through cache**: every log is persisted to Pebble first, and only logs matching the **cache policy** are additionally kept here to accelerate hot queries. See [Cache policy]({{% ref "/deployment/configuration" %}}).
- Retained data is bounded by eviction (oldest-first), never flushed downward:
  - `ttl` / `--max-time-in-mem` (default `1h`) — drop entries older than this
  - `max_entries` / `--max-logs-in-mem` (default `10000`) — cap on cached entries
- With no cache config, nothing is cached and all queries are served from Pebble.

## 2. Local store ([Pebble](https://github.com/cockroachdb/pebble))

- Persistent **on-disk** store under `--data-dir` (default `./data`).
- **Source of truth**: receives every log on ingest, cached or not.

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

- **Write**: memory writes through to local on ingest; local → bucket flush on schedule
- **Query**: served from the memory cache when it fully covers the request, otherwise fanned out down the chain with merge and dedupe

You can run **memory + local only** for simpler deployments, or full three-tier for maximum retention at lower cost.
