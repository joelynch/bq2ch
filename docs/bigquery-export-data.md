# BigQuery EXPORT DATA Statement

BigQuery's `EXPORT DATA` statement exports query results to external storage (Cloud Storage, S3, Azure Blob Storage).

## Syntax

```sql
EXPORT DATA
[WITH CONNECTION connection_name]
OPTIONS (export_option_list)
AS
query_statement
```

## Supported Formats

| Format  | Compression Options         | Notes                                      |
|---------|-----------------------------|--------------------------------------------|
| CSV     | GZIP                        | No nested/repeated data support            |
| JSON    | GZIP                        | INT64 encoded as strings for precision     |
| Avro    | DEFLATE, SNAPPY             | GZIP not supported                         |
| Parquet | SNAPPY, GZIP, ZSTD          | Best for ClickHouse interop                |

## Key Options

| Option      | Type   | Description                                                                 |
|-------------|--------|-----------------------------------------------------------------------------|
| `uri`       | STRING | **Required.** Destination URI with wildcard, e.g. `gs://bucket/path/*.parquet` |
| `format`    | STRING | **Required.** `AVRO`, `CSV`, `JSON`, or `PARQUET`                           |
| `compression` | STRING | Optional. `GZIP`, `SNAPPY`, `ZSTD`, etc.                                |
| `overwrite` | BOOL   | If true, overwrites existing files. Default: false                          |
| `header`    | BOOL   | CSV only. Default: false                                                    |

## GCS Export Example

```sql
EXPORT DATA OPTIONS(
  uri='gs://my-bucket/exports/*.parquet',
  format='PARQUET',
  compression='SNAPPY',
  overwrite=true
) AS
SELECT * FROM `project.dataset.table`
```

## S3 Export Example

Requires a pre-configured BigQuery Connection:

```sql
EXPORT DATA
  WITH CONNECTION `project.region.my-s3-connection`
  OPTIONS(
    uri='s3://my-bucket/exports/*.parquet',
    format='PARQUET',
    compression='SNAPPY',
    overwrite=true
  ) AS
SELECT * FROM `project.dataset.table`
```

## Limitations

- Max 1 GB per single file — use wildcard URIs (`*`) for larger exports, which shards automatically.
- Cannot export to local files, Google Sheets, or Google Drive.
- Exported file sizes vary and are not guaranteed.
- Nested/repeated data not supported in CSV format.
- Export order is not guaranteed unless `ORDER BY` is specified.

## Wildcard URI Behavior

BigQuery replaces `*` in the URI with zero-padded 12-digit numbers:
- `gs://bucket/file-*.parquet` → `file-000000000000.parquet`, `file-000000000001.parquet`, ...

## Parquet Export Type Mapping

| BigQuery Type | Parquet Type                                        |
|---------------|-----------------------------------------------------|
| INT64         | INT64                                               |
| FLOAT64       | FLOAT                                               |
| BOOL          | BOOLEAN                                             |
| STRING        | BYTE_ARRAY (STRING/UTF8)                            |
| BYTES         | BYTE_ARRAY                                          |
| DATE          | INT32 (DATE)                                        |
| DATETIME      | INT64 (TIMESTAMP, isAdjustedToUTC=false, MICROS)    |
| TIMESTAMP     | INT64 (TIMESTAMP, isAdjustedToUTC=false, MICROS)    |
| NUMERIC       | FIXED_LEN_BYTE_ARRAY (DECIMAL precision=38, scale=9)|
| GEOGRAPHY     | BYTE_ARRAY (GeoParquet)                             |

## References

- [Export table data to Cloud Storage](https://cloud.google.com/bigquery/docs/exporting-data)
- [EXPORT DATA statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/export-statements)
