# BigQuery Continuous Queries

BigQuery continuous queries are SQL statements that run continuously, processing incoming data in real time.

## How It Works

1. A continuous query listens for new data written to a BigQuery table.
2. It processes each new row using the query logic.
3. Results are exported to a supported destination.

## Supported Data Sources

Data must be written via one of:
- BigQuery Storage Write API
- `tabledata.insertAll` method
- Batch load
- `INSERT` DML
- Pub/Sub BigQuery subscription
- Dataflow writes
- Results from other continuous queries

## Supported Destinations

| Destination    | Format             | Notes                                        |
|----------------|--------------------|----------------------------------------------|
| BigQuery table | INSERT statement   | Write results back to another BQ table       |
| **Pub/Sub**    | CLOUD_PUBSUB       | Publish to a Pub/Sub topic                   |
| **Bigtable**   | CLOUD_BIGTABLE     | Export to Bigtable tables                    |
| **Spanner**    | CLOUD_SPANNER      | Export to Spanner tables                     |

> **Cloud Storage and S3 are NOT supported** as continuous query destinations.

## Change History Functions

- `APPENDS(table)` — Process only newly appended rows from a specific point in time.
- `CHANGES(table)` — Process both appends and mutations (Pub/Sub export only).

## Example: Continuous Query to Pub/Sub

```sql
EXPORT DATA
OPTIONS (
  format = 'CLOUD_PUBSUB',
  uri = 'https://pubsub.googleapis.com/projects/myproject/topics/my-topic'
) AS (
  SELECT TO_JSON_STRING(STRUCT(id, name, updated_at)) AS message
  FROM `myproject.dataset.table`
  WHERE status = 'active'
);
```

## Stateful Operations (Preview)

- JOINs (stream-to-stream)
- Aggregations
- Window aggregations

These retain state across rows/time intervals.

## Pricing & Requirements

- Requires **Enterprise or Enterprise Plus** edition reservations.
- Uses capacity-based (slot) pricing — on-demand not supported.
- Minimum reservation can use slot autoscaling.
- A `CONTINUOUS` reservation assignment is required.

## Limitations

- **Cannot export to Cloud Storage or S3** — only Pub/Sub, Bigtable, Spanner.
- No UDFs, DISTINCT, recursive CTEs, or window function calls.
- No external tables, materialized views, or wildcard tables as sources.
- User account tokens expire after 2 days; service accounts last up to 150 days.
- If output watermark lag exceeds 48 hours, the query fails.
- Duplicate output rows possible during reprocessing.

## Why It's Not Used in This Tool

Continuous queries cannot export to Cloud Storage (GCS) or S3, which are the intermediate storage layer this tool uses to bridge BigQuery and ClickHouse. The only viable continuous export paths would require Pub/Sub as an intermediary, adding significant infrastructure complexity.

## References

- [Introduction to continuous queries](https://cloud.google.com/bigquery/docs/continuous-queries-introduction)
- [Create continuous queries](https://cloud.google.com/bigquery/docs/continuous-queries)
