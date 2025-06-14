# LogsGo
<p align="center">
  <img src="docs/logsGo_logo.png" alt="LogsGo Gopher" width="300"/>
</p>

**LogsGo** is a standalone, scalable log ingestion and querying service designed for infinite log retention. It features a multi-tiered store architecture, pluggable backends, and a web-based dashboard. Logs are ingested via gRPC and flushed across configured stores based on a customizable time interval.

---

## Features

-  **Push-based log ingestion** using a lightweight gRPC client.
-  **Multi-tiered store architecture**:
    - **In-memory store** → for fast ingestion and short-term access.
    - **Local store** → persistent storage powered by [BadgerDB](https://github.com/dgraph-io/badger).
    - **Cloud store** → support for S3-compatible services like AWS S3 or MinIO.
-  **Chained store design**: Each store passes query and flush operations to its `.next` store for transparent fallbacks and deep queries.
-  **Custom query language**: Enables querying logs with `AND`/`OR` operators. Example:
  
    ```
    service=apsouth-1&level=warn
    ```
    
- **Web dashboard**: Simple UI to query and visualize logs.
- **Client library**: Embed the LogsGo client into your service and send logs using `UploadLogs()` over gRPC. Example:
  ```
    import "github.com/Saumya40-codes/LogsGO/client/go/logclient"
    
    c := logclient.NewLogClient(ctx, my_logsGo_endpoint_url)
    c.UploadLog(&logclient.Opts{
        {
            Service: "apsouth-1",
            Level: "warn",
            Message: "Disk usage high",
            Timestamp: time.Now(),
        },
    })
  ```

       
- **Configurable flush intervals** for controlling when logs are forwarded from one store to the next.

---

## Architecture Overview

![Architecture Diagram](docs/archv1.png)

1. Logs are received via the gRPC client.
2. They are stored first in an in-memory buffer.
3. At regular intervals, logs are flushed to:
 - Local store (BadgerDB)
 - Then to S3-compatible object store (e.g., AWS S3, MinIO)
4. Queries traverse through each store using a `.next` store in chain until results are found.

---
## DataModel Overview

![Data Model](docs/data_model.png)

---
## Query Execution Overview

![Querying Execution](docs/query-execution.png)

## Current State
![LogsGo Current State](docs/v0.1.0-status.png)

