---
title: Docker
description: Container image, ports, volumes, and compose patterns.
weight: 10
---

## Build

```bash
docker build -t logsgo:local .
```

Root `Dockerfile` and `.dockerignore` define the image context.

## Run

```bash
docker run --rm -p 8080:8080 -p 50051:50051 -p 19091:19091 \
  -v logsgo-data:/data \
  logsgo:local \
  --data-dir=/data
```

Mount config files for store, TLS, and queue as needed:

```bash
docker run ... \
  -v $PWD/store_config.yaml:/config/store.yaml:ro \
  logsgo:local \
  --data-dir=/data \
  --store-config-path=/config/store.yaml
```

## Compose

Prefer `examples/docker-compose.yml` as a template: attach MinIO, optional Prometheus, and an example producer. Mirror environment-specific secrets via env files or Docker secrets — do not bake keys into images.

## Published image

Helm chart default repository: `saumyashah40/logsgo`. Confirm tags on Docker Hub / GHCR as published by project CI.
