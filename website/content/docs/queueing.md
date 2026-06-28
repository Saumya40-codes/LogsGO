---
title: Message queue ingestion
description: RabbitMQ opt-in path for high-throughput async log upload.
weight: 60
---

For peak throughput or producer/consumer decoupling, LogsGo can ingest from **RabbitMQ**. Producers enqueue batches; **N workers** on the LogsGo side consume in **round-robin**.

![Queueing model](/images/logs-queueing.png)

*Ideal queueing model*

## Server configuration

Create a queue config file and pass `--queue-config-path`:

```yaml
queue:
  url: amqp://guest:guest@rabbitmq:5672/
  name: logs
  numWorkers: 4   # optional, default 1
```

Workers distribute consumption; load shifts to the LogsGo process while producers return quickly after publish.

## Client configuration

The Go client supports queue-aware construction (`NewLogClientWithQueue`) with `QueueOpts` (`QueueName`, `Url`) plus TLS/insecure gRPC options. Batches can be published as protobuf-serialized payloads to AMQP.

## Example stack

See `examples/example-with-rabbitmq/` — MinIO + RabbitMQ + LogsGo + sample app:

```bash
cd examples/example-with-rabbitmq
docker compose up
```

Sample app uploads batches to the queue; LogsGo workers insert them shortly after.

## When to use it

| Prefer direct gRPC | Prefer queue |
|--------------------|--------------|
| Low latency ack to app | Burst absorption |
| Simple topology | Many producers, fewer consumers |
| Smaller deployments | Benchmarks needing multi-million logs/s publish rates |

Benchmarks in [Benchmarks]({{% ref "/docs/benchmarks" %}}) show ~**70k logs/s** direct vs ~**2M logs/s** enqueue path for 1M logs (publish-side timing; server still processes asynchronously).
