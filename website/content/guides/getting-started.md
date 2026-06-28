---
title: Getting started
description: Build from source, run the binary, open the dashboard, send your first logs.
weight: 10
---

## Prerequisites

- **Go** (see `go.mod` for the module version)
- **Node.js / npm** (only if you rebuild the UI)
- **Docker** (recommended for the full example stack)
- Optional: `protoc` if regenerating gRPC stubs (`make proto`)

## Option A — Interactive Docker example (fastest)

```bash
git clone https://github.com/Saumya40-codes/LogsGO.git
cd LogsGO/examples
docker compose up
```

Then open:

| Endpoint | URL |
|----------|-----|
| Dashboard | http://localhost:19091 |
| REST API | http://localhost:8080 |
| gRPC | `localhost:50051` |

The compose file typically wires LogsGo with MinIO (and related config). For RabbitMQ, use `examples/example-with-rabbitmq/`.

## Option B — Build from source

```bash
git clone https://github.com/Saumya40-codes/LogsGO.git
cd LogsGO

# Optional: rebuild embedded UI
make build-react-app

# Build Go binary (embeds UI dist when present)
make build
# produces ./logsGo

./logsGo --help
./logsGo
```

Useful flags on first run:

```bash
./logsGo \
  --data-dir=./data \
  --http-listen-addr=:8080 \
  --grpc-listen-addr=:50051 \
  --web-listen-addr=:19091 \
  --max-time-in-mem=1h \
  --max-logs-in-mem=10000 \
  --lookback-period=15m
```

## Send logs with the Go client

```go
package main

import (
    "context"
    "time"

    "github.com/Saumya40-codes/LogsGO/client/go/logclient"
)

func main() {
    ctx := context.Background()
    c, err := logclient.NewLogClient(ctx, "localhost:50051")
    if err != nil {
        panic(err)
    }
    defer c.Close() // if available on your client version

    _ = c.UploadLog(&logclient.LogOpts{
        Service:   "apsouth-1",
        Level:     "warn",
        Message:   "Disk usage high",
        Timestamp: time.Now().Unix(),
        Labels:    map[string]string{"region": "ap-south-1"},
    })
}
```

Adjust method names to match the client API (`UploadLog` / batch helpers) in `client/go/logclient`.

## Query from the UI

1. Open the dashboard on `--web-listen-addr`.
2. Enter an expression, e.g. `service=apsouth-1&level=warn`.
3. Run an **instant** or **range** query; inspect table and graph views.

## Next steps

- [Docker example deep dive]({{% ref "/guides/docker-example" %}})
- [Client library]({{% ref "/guides/client-library" %}})
- [CLI flags]({{% ref "/api/cli" %}})
- [Helm deployment]({{% ref "/deployment/helm" %}})
