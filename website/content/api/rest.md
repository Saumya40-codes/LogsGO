---
title: REST API
description: HTTP endpoints for query, labels, metrics, and health-related ops.
weight: 10
---

Base URL defaults to `http://localhost:8080`. Authenticated deployments require `Authorization: Bearer <jwt>` when `--public-key-path` is set.

## `GET /api/v1/query`

Run a LogsGoQL expression.

| Query param | Required | Description |
|-------------|----------|-------------|
| `expression` | yes | LogsGoQL string |
| `start` | for range | Start timestamp (API accepts formats parsed by the server) |
| `end` | for range | End timestamp |
| `resolution` | for range | Step duration with `s`/`m`/`h`/`d` suffix |

Invalid `resolution` returns **400** with an error payload.

Example:

```bash
curl -G 'http://localhost:8080/api/v1/query' \
  --data-urlencode 'expression=service=api&level=error' \
  --data-urlencode 'start=...' \
  --data-urlencode 'end=...' \
  --data-urlencode 'resolution=1m'
```

## Label discovery

The REST package defines responses for **services**, **levels**, and **custom label values** used by the dashboard suggestion filters (see `LabelValuesResponse` in `api/rest/v1.go`). Exact paths follow the `/api/v1` group implemented alongside `/query` — inspect the source or network tab of the UI for the full list as it evolves.

## Metrics

Prometheus metrics are registered and served from the REST process (Prometheus client_golang). Scrape the metrics endpoint exposed by the Gin server / promhttp handler in your deployment (commonly used with the `examples/prometheus.yml` scrape config).

## CORS

Allowed origins include Vite dev (`http://localhost:5173`) and the origin derived from `--web-listen-addr` so the embedded UI can call the API.

## Profiling

`net/http/pprof` is imported for diagnostics — restrict access in production.
