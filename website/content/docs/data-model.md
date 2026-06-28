---
title: Data model
description: Log entries, series, labels, and how data is represented across tiers.
weight: 20
---

## Log entry

A log is a structured record aligned with the gRPC `LogEntry` message:

| Field | Type | Description |
|-------|------|-------------|
| `service` | string | Service or source identifier (queryable) |
| `level` | string | Severity / level (e.g. `info`, `warn`, `error`) |
| `message` | string | Log body (supports `CONTAINS` in LogsGoQL) |
| `timestamp` | int64 | Unix timestamp (client may omit; server can set “now”) |
| `labels` | map[string]string | Custom key/value labels for richer filtering |

```protobuf
message LogEntry {
  string service = 1;
  string level = 2;
  string message = 3;
  int64 timestamp = 4;
  map<string, string> labels = 5;
}
```

## Series and aggregation

Query results are often returned as **series**: an entry shape plus a **count** of matching occurrences in a window. Instant queries aggregate within the lookback; range queries produce points along the timeline.

![Data model diagram](/images/data_model.png)

*Data model overview*

## Blocks and retention

On disk and in object storage, data is organized into **time blocks** / chunks. Retention is controlled by `--max-retention-time` (default `10d`) for how long block chunks remain. Memory residency is separate (`--max-time-in-mem`, `--max-logs-in-mem`).

## Deduplication across tiers

Because the same series can briefly exist in multiple tiers during flush windows, query merge prefers the **parent** tier when timestamps collide. Deduplication runs between local↔bucket and memory↔(local+bucket) results so counts stay consistent.

## Custom labels

Beyond `service` and `level`, arbitrary labels ride on each entry and participate in query planning and label-value discovery APIs used by the UI filters.
