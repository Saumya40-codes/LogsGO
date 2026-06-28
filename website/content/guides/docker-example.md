---
title: Docker example
description: Run examples/ compose stacks with MinIO and optional RabbitMQ.
weight: 20
---

## Standard example (`examples/`)

```bash
cd examples
docker compose up
```

Typical services:

- **logsgo** — ingestion + API + UI
- **minio** — S3-compatible bucket backend
- **example_app** — sample producer (varies by compose file)
- Config bind-mounts such as `store_config.yaml`, `queue-config.yaml`, `prometheus.yml`

Inspect `examples/docker-compose.yml` and `examples/store_config.yaml` for exact wiring.

## RabbitMQ example

```bash
cd examples/example-with-rabbitmq
docker compose up
```

This variant adds **RabbitMQ**, configures LogsGo with `--queue-config-path`, and demonstrates async batch upload. Example logs show batches published by `example_app` and inserted by `logsgo` workers within seconds.

## Image

CI builds publish images (see `.github/workflows/build-docker-image.yml`). Helm defaults reference `saumyashah40/logsgo` with tag `dev` — pin versions for production.

## Local Dockerfile

Repository root `Dockerfile` multi-stage builds the Go service (and UI assets depending on stages). Build:

```bash
docker build -t logsgo:local .
```

Run with published ports `8080`, `50051`, `19091` and a volume for `--data-dir`.
