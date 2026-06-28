---
title: Benchmarks
description: 1M log load tests — direct gRPC vs RabbitMQ enqueue path.
weight: 80
---

Benchmarks live under [`/bench`](https://github.com/Saumya40-codes/LogsGO/tree/main/bench) in the repository (docker-compose driven load tests).

## Direct ingestion — 1,000,000 logs

| Metric | Value |
|--------|------:|
| Total logs sent | 1,000,000 |
| Errors | 0 |
| Success rate | 100% |
| Total time | **14.318 s** |
| Logs per second | **~69,839** |
| Batches success | 500 |
| Avg batch latency | 562 ms |
| Min / max batch latency | 49 ms / 963 ms |

## Via message queue — 1,000,000 logs (publish path)

| Metric | Value |
|--------|------:|
| Total logs sent | 1,000,000 |
| Errors | 0 |
| Success rate | 100% |
| Total time | **492 ms** |
| Logs per second | **~2,031,233** |
| Avg batch latency | 10 ms |
| Min / max batch latency | 1 ms / 47 ms |

The queue path optimizes **producer publish latency** and decoupling; LogsGo workers still perform durable ingestion on the consumer side. Choose based on whether your bottleneck is **app-side blocking** or **server-side persist**.

## Reproduce

```bash
cd bench
# follow bench/README.md and docker-compose.yml
docker compose up
```

Hardware, batch size, store config (memory-only vs full tier), and network all affect numbers — treat these as **reference** results from the project authors’ runs.
