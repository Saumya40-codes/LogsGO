---
title: Documentation
description: Architecture, storage, query language, security, and performance for LogsGo.
weight: 1
---

LogsGo is a **standalone log ingestion and querying service** aimed at long retention and operational simplicity. Use this section for design deep-dives; use **Guides** for runbooks and **API** / **Deploy** for interfaces and operations.

### What you can do

- Ingest structured logs over **gRPC** (single or batch), optionally via **RabbitMQ**
- Retain data across **memory → BadgerDB → S3-compatible** tiers
- Query with **LogsGoQL** (instant and range) from REST or the embedded dashboard
- Harden with **JWT** and **TLS** when needed
- Deploy with **Docker**, **docker compose** examples, or the **Helm** chart

Pick a topic below or start with [Getting started]({{% ref "/guides/getting-started" %}}).
