---
title: Go client library
description: Embed logclient in your service for gRPC and optional queue upload.
weight: 30
---

Module path:

```text
github.com/Saumya40-codes/LogsGO/client/go/logclient
```

## Connect (insecure)

```go
c, err := logclient.NewLogClient(ctx, "localhost:50051")
```

Default gRPC address in the server is `:50051`.

## Connect with TLS

Use `NewLogClientWithTLS(ctx, addr, tlsConfig)` when the server is started with `--tls-config-path`.

## Queue-aware client

```go
c, err := logclient.NewLogClientWithQueue(ctx, grpcAddr, &logclient.QueueOpts{
    QueueName: "logs",
    Url:       "amqp://guest:guest@localhost:5672/",
}, true /* insecure gRPC */, nil)
```

Publishing to the queue decouples your app from LogsGo ingest latency.

## Log payload

```go
type LogOpts struct {
    Service   string
    Level     string
    Message   string
    Timestamp int64             // unix; may default to now if unset in helpers
    Labels    map[string]string // custom labels
}
```

Batch type wraps `[]*LogOpts` for multi-entry upload RPCs.

## Proto surface

The client is a thin wrapper over:

```protobuf
service LogIngestor {
    rpc UploadLog(LogEntry) returns (UploadResponse);
    rpc UploadLogs(LogBatch) returns (UploadResponse);
}
```

Generate stubs with `make proto` if you change `api/grpc/proto/logs.proto`.

## Errors and retries

Handle gRPC errors from the client; for queue mode, also handle AMQP publish failures. Prefer **batches** for throughput; tune batch size against memory flush thresholds on the server.
