---
title: Authentication & TLS
description: Optional JWT on REST/gRPC and TLS configuration for encrypted endpoints.
weight: 70
---

Security features are **opt-in** via CLI flags and small config files.

![Auth flow](/images/auth_in_logsGo.png)

*Authentication design notes*

## JWT authentication

If `--public-key-path` points at an **RSA public key** file, LogsGo enables JWT validation for **gRPC and HTTP** servers.

- Clients must send a valid bearer token (HTTP `Authorization` header; gRPC metadata per your client setup).
- REST routes under `/api/v1` use Gin JWT middleware when the key is loaded.
- Misconfigured or missing keys fail fast at startup when a path is set but unreadable.

```bash
./logsGo --public-key-path=/secrets/jwt.pub
```

## TLS

Provide a TLS config YAML with certificate and key paths via `--tls-config-path` (see `examples/tls-config.yaml` in the repo). When set, REST and gRPC can serve with TLS credentials.

```bash
./logsGo --tls-config-path=auth.yaml
```

Combine JWT + TLS for encrypted transport and authenticated API access.

## Operational notes

- Keep private keys **out of the image**; mount secrets in Kubernetes (Helm chart supports secret patterns).
- UI origin CORS is derived from `--web-listen-addr` plus local Vite dev (`localhost:5173`).
- Prometheus metrics and pprof are exposed on the REST process — protect the port in production networks.
