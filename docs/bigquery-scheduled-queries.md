# BigQuery Scheduled Queries

BigQuery scheduled queries use the **BigQuery Data Transfer Service** to run SQL on a recurring schedule. The schedule is managed server-side — no external orchestrator needed.

## How It Works

1. You provide a GoogleSQL query (including DDL, DML, or `EXPORT DATA`).
2. BigQuery runs it on the specified cron-like schedule.
3. Two built-in parameters are available: `@run_time` (TIMESTAMP, UTC) and `@run_date` (DATE).
4. Results can be written to a destination table or exported via `EXPORT DATA`.

## Schedule Syntax

Uses a cron-like format from the App Engine scheduling syntax:

```
every 15 minutes
every 1 hours
every 24 hours
every mon 09:00
every day 23:30
```

Minimum interval: **5 minutes**.

## Built-in Parameters

| Parameter   | Type      | Description                                                        |
|-------------|-----------|--------------------------------------------------------------------|
| `@run_time` | TIMESTAMP | Intended execution time in UTC. Consistent between runs.           |
| `@run_date` | DATE      | Date portion of `@run_time`.                                       |

These are useful for incremental exports, e.g.:

```sql
EXPORT DATA OPTIONS(uri='gs://bucket/{run_time|"%Y%m%d_%H%M%S"}/*.parquet', ...)
AS SELECT * FROM table WHERE updated_at > TIMESTAMP_SUB(@run_time, INTERVAL 1 HOUR)
```

## Destination Table Templating

The destination table name supports runtime parameter templating:

| Example Template                        | Result (for 2026-04-15 00:00:00 UTC) |
|-----------------------------------------|--------------------------------------|
| `mytable`                               | `mytable`                            |
| `mytable_{run_time\|"%Y%m%d"}`          | `mytable_20260415`                   |
| `mytable_{run_time+25h\|"%Y%m%d"}`      | `mytable_20260416`                   |

## Write Preferences

- `WRITE_TRUNCATE` — Overwrite the destination table.
- `WRITE_APPEND` — Append to the destination table.

## Authentication

- **User credentials** (default): Query runs as the creating user. Access tokens expire after **2 days**.
- **Service account**: Recommended for production. Tokens last up to **150 days**.

## Python API (Data Transfer Service)

```python
from google.cloud import bigquery_datatransfer
from google.protobuf.struct_pb2 import Struct

client = bigquery_datatransfer.DataTransferServiceClient()

params = Struct()
params.update({"query": "SELECT ..."})

transfer_config = bigquery_datatransfer.TransferConfig(
    display_name="My scheduled export",
    data_source_id="scheduled_query",
    schedule="every 15 minutes",
    params=params,
)

result = client.create_transfer_config(
    request=bigquery_datatransfer.CreateTransferConfigRequest(
        parent="projects/my-project/locations/US",
        transfer_config=transfer_config,
        service_account_name="my-sa@project.iam.gserviceaccount.com",  # optional
    )
)
print(result.name)  # resource ID for later management
```

## Limitations

- Queries on the hour (e.g. 09:00) may trigger multiple times — use off-the-hour schedules.
- `@run_time` gives the *scheduled* time, not the actual execution time.
- Cannot modify the SQL of a running scheduled query — must delete and recreate.
- Cross-region queries not supported.

## References

- [Scheduling queries](https://cloud.google.com/bigquery/docs/scheduling-queries)
- [Python client for BigQuery Data Transfer](https://cloud.google.com/python/docs/reference/bigquerydatatransfer/latest)
