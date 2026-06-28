---
title: Query execution
description: Planning, concurrent store execution, deduplication, instant and range logic.
weight: 50
---

## Execution plan

The LogsGoQL engine **parses** the expression into an AST, then builds a **query plan**. Each store executes the plan (or relevant slices) and results are **merged**.

![Query execution plan](/images/query-execution.png)

*Query execution plan overview*

## Flow across the chain

Parent and child stores run with awareness of each other; **deduplication** prefers parent series when the same series exists at the same timestamp in two tiers.

![Query flow](/images/query-flow.png)

*Query flow across stores*

Two merge stages are important conceptually:

1. **Local ↔ bucket**  
2. **Memory ↔ (local + bucket)**

## Instant queries

Instant queries answer “what matches **now** (with lookback)?” — useful for dashboards and triage.

![Instant query logic](/images/query-instant_logic.png)

*Instant query logic*

Example UI output (aggregated counts with latest timestamps):

![Instant query in dashboard](/images/v0.2.0-instant-query.png)

*Instant query results in the LogsGo UI*

## Range queries

Range queries walk `[start, end]` in steps of `resolution`, counting matches per step. If a step has **zero** matches, that point may not appear on the graph.

![Range query logic](/images/query-range_logic.png)

*Range query stepping logic*

![Range query table](/images/v0.2.0-range-query.png)

*Range query tabular results*

![Range query graph](/images/range-query-graph-view.png)

*Range query graph view*

## Concurrency

Stores execute plans **concurrently** where safe, reducing latency when historical data spans multiple tiers. Always validate `resolution` duration format on the API (`s`/`m`/`h`/`d` suffixes).
