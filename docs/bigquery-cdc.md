# BigQuery Change Data Capture (CDC)

BigQuery CDC ingestion updates BigQuery tables by processing streamed changes (upserts and deletes) via the **Storage Write API**.

> **Note:** BigQuery CDC is for streaming changes *into* BigQuery, not for exporting changes *out*. It is **not** directly useful for migrating data from BigQuery to other systems.

## How It Works

1. Define a table with **primary keys** (up to 16 columns).
2. Stream rows via the Storage Write API default stream, setting the `_CHANGE_TYPE` pseudocolumn.
3. BigQuery applies `UPSERT` and `DELETE` operations in the background.

## Change Types

| `_CHANGE_TYPE` | Behavior                                              |
|----------------|-------------------------------------------------------|
| `UPSERT`       | Insert or update the row based on primary key match   |
| `DELETE`       | Remove the row matching the primary key               |

## Example

Starting table:

| ID  | Name    | Salary |
|-----|---------|--------|
| 100 | Charlie | 2000   |
| 101 | Tal     | 3000   |
| 102 | Lee     | 5000   |

Streamed changes:

| ID  | Name  | Salary | _CHANGE_TYPE |
|-----|-------|--------|--------------|
| 100 |       |        | DELETE       |
| 101 | Tal   | 8000   | UPSERT       |
| 105 | Izumi | 6000   | UPSERT       |

Result:

| ID  | Name  | Salary |
|-----|-------|--------|
| 101 | Tal   | 8000   |
| 102 | Lee   | 5000   |
| 105 | Izumi | 6000   |

## `max_staleness` Option

Controls how often BigQuery applies pending modifications:

```sql
CREATE TABLE employees (
  id INT64 PRIMARY KEY NOT ENFORCED,
  name STRING
)
CLUSTER BY id
OPTIONS (max_staleness = INTERVAL 10 MINUTE);
```

- Lower values = fresher data, higher compute cost.
- Higher values = reduced cost, slightly stale query results.

## Custom Ordering (`_CHANGE_SEQUENCE_NUMBER`)

For use cases with frequent upserts to the same primary key, you can supply ordering via `_CHANGE_SEQUENCE_NUMBER` (hexadecimal string format, up to 4 sections separated by `/`).

## Limitations

- No key enforcement — primary keys must be unique by contract.
- Max 16 primary key columns, max 2000 top-level columns.
- CDC-enabled tables don't support: DML (`DELETE`/`UPDATE`/`MERGE`), wildcard tables, search indexes.
- `EXPORT` operations on CDC tables don't export unapplied modifications — use `EXPORT DATA` statement instead.
- Delete retention window is 2 days.

## Why It's Not Useful for BQ→CH Migration

BigQuery CDC is an *ingestion* mechanism (external systems → BigQuery). There is no built-in way to *subscribe* to changes in a BigQuery table and export them. For getting data *out* of BigQuery continuously, the options are:
- **Scheduled queries** with `EXPORT DATA`
- **Continuous queries** (to Pub/Sub, Bigtable, Spanner only — not Cloud Storage)
- **Polling** with watermark-based incremental exports

## References

- [Stream table updates with change data capture](https://cloud.google.com/bigquery/docs/change-data-capture)
