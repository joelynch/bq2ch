# bq2ch

CLI tool for migrating tables between BigQuery and ClickHouse via GCS or S3 as intermediate storage.

## Features

- **Snapshot migration** — One-off full table export from BigQuery to Parquet, then bulk ingest into ClickHouse via `s3Cluster`
- **Polling CDC** — Continuous watermark-based incremental exports with ClickHouse S3Queue + Materialized View for automatic ingestion
- **Scheduled CDC** — Server-side BigQuery scheduled queries that export incremental Parquet files on a cron, picked up by S3Queue automatically
- **GCS and S3** — Supports both Google Cloud Storage (via S3-compatible HMAC API) and AWS S3 as the intermediate layer
- **Auto schema mapping** — Introspects BigQuery table schema and creates the corresponding ClickHouse table with type mapping
- **Cluster-aware** — All DDL uses `ON CLUSTER` with `ReplicatedMergeTree` engine

## Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Usage

```bash
# One-off full table migration
bq-ch-migrator snapshot \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-cluster my_cluster \
  --ch-database default --ch-table my_table \
  --order-by "id"

# Continuous CDC with local polling loop
bq-ch-migrator cdc \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --watermark-column updated_at \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-cluster my_cluster \
  --poll-interval 120

# Scheduled CDC (serverless — no long-running process)
bq-ch-migrator scheduled-cdc \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --watermark-column updated_at --schedule "every 15 minutes" \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-cluster my_cluster

# List / delete scheduled queries
bq-ch-migrator list-schedules --bq-project my-project
bq-ch-migrator delete-schedule --transfer-config-name "projects/..."
```

## Authentication

All options can be set via environment variables (12-factor style) or CLI flags:

| Env Var | CLI Flag | Description |
|---------|----------|-------------|
| `BQ_PROJECT` | `--bq-project` | BigQuery project ID |
| `BQ_DATASET` | `--bq-dataset` | BigQuery dataset |
| `BQ_TABLE` | `--bq-table` | BigQuery table |
| `STORAGE_TYPE` | `--storage-type` | `gcs` or `s3` |
| `STORAGE_BUCKET` | `--bucket` | Bucket name |
| `GCS_HMAC_ACCESS_KEY` / `AWS_ACCESS_KEY_ID` | `--storage-access-key` | Storage access key |
| `GCS_HMAC_SECRET` / `AWS_SECRET_ACCESS_KEY` | `--storage-secret-key` | Storage secret key |
| `BQ_CONNECTION` | `--bq-connection` | BQ Connection (required for S3 exports) |
| `CH_HOST` | `--ch-host` | ClickHouse host |
| `CH_PORT` | `--ch-port` | ClickHouse HTTP(S) port (default: 8443) |
| `CH_USER` | `--ch-user` | ClickHouse username |
| `CH_PASSWORD` | `--ch-password` | ClickHouse password |
| `CH_DATABASE` | `--ch-database` | ClickHouse database |
| `CH_CLUSTER` | `--ch-cluster` | ClickHouse cluster name |

## Architecture

```
BigQuery ──EXPORT DATA──▶ GCS/S3 (Parquet) ──▶ ClickHouse
                                                 ├─ s3Cluster (snapshot)
                                                 └─ S3Queue + MV (CDC)
```

### Snapshot Flow
1. Introspect BigQuery table schema
2. Create ClickHouse table with mapped types (`ReplicatedMergeTree ON CLUSTER`)
3. `EXPORT DATA` from BigQuery to Parquet files in GCS/S3
4. `INSERT INTO ... SELECT FROM s3Cluster(...)` to bulk load into ClickHouse

### CDC Flow (Polling)
1. Same schema introspection + table creation
2. Create S3Queue table + Materialized View in ClickHouse
3. Initial full export
4. Poll BigQuery for new rows (watermark column) and export incremental Parquet files
5. S3Queue automatically detects and ingests new files via the MV

### CDC Flow (Scheduled)
1. Same schema introspection + table creation + S3Queue setup
2. Initial full export
3. Create a BigQuery scheduled query that runs `EXPORT DATA` on a cron using `@run_time`
4. S3Queue continuously picks up new Parquet files — no local process needed

## Docs

See [docs/](docs/) for detailed documentation on the underlying technologies:

- [BigQuery EXPORT DATA](docs/bigquery-export-data.md)
- [BigQuery Scheduled Queries](docs/bigquery-scheduled-queries.md)
- [BigQuery CDC](docs/bigquery-cdc.md)
- [BigQuery Continuous Queries](docs/bigquery-continuous-queries.md)
- [ClickHouse s3Cluster](docs/clickhouse-s3cluster.md)
- [ClickHouse S3Queue](docs/clickhouse-s3queue.md)
- [ClickHouse Connect Python Client](docs/clickhouse-connect-python.md)
- [GCS S3 Interoperability](docs/gcs-s3-interop.md)
- [Schema Mapping](docs/schema-mapping.md)
- [Typer CLI](docs/typer-cli.md)
