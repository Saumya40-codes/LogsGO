---
title: Configuration files
description: Store, queue, TLS, and Prometheus scrape examples.
weight: 30
---

## Remote store (`store_config.yaml`)

```yaml
remote_store:
  provider: "minio"
  bucket: "bkt1"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  create_bucket_on_empty: true
  endpoint: "minio:9000"
```

```bash
./logsGo --store-config-path=store_config.yaml
```

## Cache policy (`cache-config.yaml`)

All logs are always persisted durably to Pebble. This policy only decides which logs are *additionally* kept in the in-memory cache to speed up hot queries. Rules use the same query language as reads ([LogsGoQL]({{% ref "/docs/query-language" %}})); a log is cached if it matches **any** rule.

```yaml
cache:
  enabled: true
  ttl: 1h              # evict cached logs older than this
  max_entries: 100000  # cap on cached logs; oldest are evicted first
  rules:
    - 'level=error|level=warn'
    - 'service=payments'
```

```bash
./logsGo --cache-config-path=cache-config.yaml
```

A query is served from the cache alone (skipping Pebble) only when its predicate is fully covered by the policy rules **and** its time window falls within the retained set; otherwise it transparently falls back to the durable tier. Omit the flag to disable caching entirely — every query is then served from Pebble.

## Queue (`queue-config.yaml`)

```yaml
queue:
  url: amqp://guest:guest@rabbitmq:5672/
  name: logs
  numWorkers: 4
```

```bash
./logsGo --queue-config-path=queue-config.yaml
```

## TLS

See `examples/tls-config.yaml` for cert and key path fields. Enable with:

```bash
./logsGo --tls-config-path=tls-config.yaml
```

## Prometheus

`examples/prometheus.yml` shows scraping LogsGo metrics. Point Prometheus at the REST listen address / metrics path used by your build.

## Make targets

| Target | Purpose |
|--------|---------|
| `make proto` | Regenerate gRPC Go code |
| `make build` | Build `logsGo` binary |
| `make tests` | Run Go tests |
| `make start-react-app` | Vite dev server for UI |
| `make build-react-app` | Production UI into `pkg/ui/dist` |
| `make build-all` | UI + Go binary |

## Production checklist

1. Pin image / binary version  
2. Set retention and memory limits for your volume  
3. Enable TLS + JWT for multi-tenant or public exposure  
4. Persist `--data-dir` on a volume  
5. Configure remote store for long retention  
6. Restrict REST/pprof and metrics to private networks  
7. Use queue workers sized to CPU and AMQP throughput  
8. Schedule backups of Pebble data and/or bucket lifecycle policies  
