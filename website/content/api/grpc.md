---
title: gRPC API
description: LogIngestor service — UploadLog and UploadLogs.
weight: 20
---

Package: `logapi`  
Proto: `api/grpc/proto/logs.proto`  
Generated Go: `api/grpc/pb/`

## Service

```protobuf
service LogIngestor {
    rpc UploadLog(LogEntry) returns (UploadResponse);
    rpc UploadLogs(LogBatch) returns (UploadResponse);
}
```

## Messages

**LogEntry** — `service`, `level`, `message`, `timestamp`, `labels` map.

**LogBatch** — `repeated LogEntry entries`.

**UploadResponse** — `bool success`.

Additional messages (`Series`, `SeriesBatch`) support query/result shapes in the broader API surface.

## Default listen address

`--grpc-listen-addr` default **`:50051`**.

## Auth & TLS

- JWT: enabled when public key path is configured; attach credentials via gRPC interceptors / metadata as implemented in `api/auth`.
- TLS: server credentials from `--tls-config-path`.

## Client

Prefer the maintained Go client:

```text
github.com/Saumya40-codes/LogsGO/client/go/logclient
```

Other languages can generate stubs from the same `.proto` file.
