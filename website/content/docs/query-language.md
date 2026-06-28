---
title: Query language (LogsGoQL)
description: Grammar, operators, examples for AND/OR, equality, and CONTAINS.
weight: 40
---

LogsGo ships a small, focused query language for filtering log series. Expressions are used from the **dashboard** and the **REST** `expression` parameter.

## Grammar (EBNF)

```text
query      -> expr
expr       -> term (OR term)*
term       -> factor (AND factor)*
factor     -> "(" expr ")" | comparison
comparison -> ident operator value
ident      -> level | service | message | <custom label keys>
operator   -> "=" | CONTAINS
value      -> string | ident
```

In the UI / HTTP API, **AND** is often written as `&` and **OR** as `|` in compact form:

```text
service=ap-south-1&level=warn
service=ap-south-1|service="us-west-1"
```

## Operators

| Operator | Meaning | Example |
|----------|---------|---------|
| `=` | Exact match on field / label | `level=error` |
| `CONTAINS` | Substring match (especially useful on `message`) | `message CONTAINS timeout` |
| `&` / AND | Conjunction | `service=api&level=warn` |
| `\|` / OR | Disjunction | `level=error\|level=fatal` |
| `( )` | Grouping | `(service=a\|service=b)&level=info` |

## Fields

- **`service`** — source service name  
- **`level`** — log level  
- **`message`** — body text  
- **Custom labels** — any key supplied in `LogEntry.labels` / client `Labels` map  

## Instant vs range

- **Instant**: evaluate around “now” with `--lookback-period` (default `15m`) unless explicit window logic applies in the UI/API.
- **Range**: provide `start`, `end`, and `resolution` (duration suffix `s`/`m`/`h`/`d`). The engine steps from start to end by resolution and counts matches per bucket. Empty buckets may be omitted in the graph.

## Tips

- Prefer equality on **low-cardinality** labels (`service`, `level`) for efficient plans.
- Use `CONTAINS` sparingly on high-volume streams; combine with service/level filters first.
- Quote values with spaces or special characters as needed: `service="us-west-1"`.
