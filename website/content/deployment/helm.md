---
title: Helm / Kubernetes
description: Install the logsgo chart, services, ingress, and security context.
weight: 20
---

Chart path: [`charts/logsgo`](https://github.com/Saumya40-codes/LogsGO/tree/main/charts/logsgo)

| Field | Value |
|-------|-------|
| Chart name | `logsgo` |
| Chart version | `0.1.0` |
| App version | `0.0.1` (chart metadata; override image tag) |

## Install

```bash
helm install logsgo ./charts/logsgo -n logs --create-namespace
```

## Notable values

- **Image**: `saumyashah40/logsgo`, tag `dev` by default — set `image.tag` for releases.
- **Service ports**: HTTP `8080`, gRPC `50051`, web `19091`.
- **Ingress**: enabled by default (`nginx` class), host example `dashboard.myapp.local` → web UI port.
- **Security context**: non-root (`runAsUser: 1000`), drop all capabilities, no privilege escalation.
- **Service account**: created by default.
- **Probes / HPA / secrets**: templates included under `charts/logsgo/templates/`.

## Customize

```bash
helm install logsgo ./charts/logsgo \
  --set image.tag=latest \
  --set ingress.hosts[0].host=logs.example.com \
  --set replicaCount=2
```

Mount store, TLS, and JWT material via Secrets and extra volumes (extend `values.yaml` / deployment template as needed for your cluster).

## Verify

```bash
kubectl -n logs port-forward svc/logsgo 19091:19091
# open http://localhost:19091
```

Helm test hook: `charts/logsgo/templates/tests/test-connection.yaml`.
