---
title: CLI flags
description: Complete reference for logsgo / logsGo command-line flags.
weight: 30
---

Binary entrypoint: `cmd/logsGo` (Cobra). Run `./logsGo --help` for the build you have.

## Storage & retention

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `data` | Directory for local log data |
| `--max-retention-time` | `10d` | Max time block chunks remain on disk (`d`/`h`/`m`/`s`) |
| `--max-time-in-mem` | `1h` | Time logs remain in memory before flush |
| `--max-logs-in-mem` | `10000` | Max in-memory logs before flush |
| `--unlock-data-dir` | `false` | Allow other processes on data dir (not recommended) |
| `--flush-on-exit` | `false` | Flush to next store on any exit for stronger persistence |
| `--store-config-path` | _empty_ | Path to S3-compatible store YAML |
| `--store-config` | _empty_ | Inline store config (exclusive with path) |

## Listen addresses

| Flag | Default | Description |
|------|---------|-------------|
| `--http-listen-addr` | `:8080` | REST API |
| `--grpc-listen-addr` | `:50051` | gRPC ingestion |
| `--web-listen-addr` | `:19091` | Embedded web UI |

## Query

| Flag | Default | Description |
|------|---------|-------------|
| `--lookback-period` | `15m` | Instant query lookback from “now” |

## Security

| Flag | Default | Description |
|------|---------|-------------|
| `--public-key-path` | _empty_ | RSA public key for JWT on gRPC + HTTP |
| `--tls-config-path` | _empty_ | TLS cert/key config file |

## Queue

| Flag | Default | Description |
|------|---------|-------------|
| `--queue-config-path` | _empty_ | RabbitMQ config (`url`, `name`, optional `numWorkers`) |

## Compaction

| Flag | Default | Description |
|------|---------|-------------|
| `--compact-duration` | `12h` | Compaction cycle interval |
| `--compact-config` | _empty_ | Hidden/advanced compaction config |

## Duration format

Flags that take durations must end with **`d`**, **`h`**, **`m`**, or **`s`**, with a digit before the suffix (validated at startup).
