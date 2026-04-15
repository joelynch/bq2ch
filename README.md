# bq2ch

CLI tool for migrating tables between BigQuery and ClickHouse via GCS or S3 as intermediate storage.

## Features

- **Bidirectional** — Migrate BigQuery → ClickHouse (`bq2ch`) or ClickHouse → BigQuery (`ch2bq`)
- **Snapshot migration** — One-off full table export in either direction via Parquet in GCS/S3
- **Partition snapshot** — Export the N latest partitions of a partitioned BQ table
- **Polling CDC** — Continuous watermark-based incremental exports with ClickHouse S3Queue + Materialized View for automatic ingestion
- **Scheduled CDC** — Server-side scheduled queries (BQ→CH) or BQ Data Transfer Service loads (CH→BQ) on a cron — no long-running process
- **Event-driven CDC** — ClickHouse MV writes to GCS, Cloud Function triggers BQ load on each file (near-real-time, CH→BQ only)
- **GCS and S3** — Supports both Google Cloud Storage (via S3-compatible HMAC API) and AWS S3 as the intermediate layer
- **Auto schema mapping** — Introspects source table schema and creates the destination table with type mapping (both directions)
- **Cluster-aware** — All DDL uses `ON CLUSTER` with `ReplicatedMergeTree` engine

## Install

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## CLI Commands

The CLI is organized into two subcommand groups plus utility commands:

- `bq-ch-migrator bq2ch <command>` — BigQuery → ClickHouse
- `bq-ch-migrator ch2bq <command>` — ClickHouse → BigQuery
- `bq-ch-migrator <command>` — Utility commands (manage schedules, cloud functions)

### bq2ch snapshot

One-off full table migration: BQ → Parquet in GCS/S3 → ClickHouse via `s3Cluster`.

```bash
bq-ch-migrator bq2ch snapshot \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --ch-database default --ch-table my_table \
  --order-by "id"
```

### bq2ch snapshot-partition

Export the N latest partitions of a partitioned BQ table into ClickHouse. Automatically detects partition type (column or ingestion-time).

```bash
bq-ch-migrator bq2ch snapshot-partition \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --num-partitions 3
```

### bq2ch cdc

Continuous CDC: local polling loop exports incremental Parquet files based on a watermark column. ClickHouse S3Queue + Materialized View auto-ingests new files.

```bash
bq-ch-migrator bq2ch cdc \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --watermark-column updated_at \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --poll-interval 120 --watermark-file watermark.json
```

### bq2ch scheduled-cdc

Scheduled CDC: creates a BigQuery scheduled query that runs `EXPORT DATA` on a cron. S3Queue picks up files automatically — no long-running Python process needed.

```bash
bq-ch-migrator bq2ch scheduled-cdc \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --watermark-column updated_at --schedule "every 15 minutes" \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --bq-location US
```

### ch2bq snapshot

One-off full table migration: ClickHouse → Parquet in GCS/S3 → BigQuery.

```bash
bq-ch-migrator ch2bq snapshot \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --ch-database default --ch-source-table my_table \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table
```

### ch2bq scheduled-cdc

Scheduled CDC: ClickHouse MV writes Parquet to time-partitioned GCS paths. BQ Data Transfer Service loads new files on a schedule.

```bash
bq-ch-migrator ch2bq scheduled-cdc \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --ch-database default --ch-source-table my_table \
  --partition-column created_at --schedule "every 1 hours" \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --bq-location US
```

### ch2bq event-driven

Event-driven CDC: ClickHouse MV writes Parquet to GCS. A Cloud Function (gen2, Eventarc) fires on each file finalization and submits a BQ load job. Near-real-time with no polling.

```bash
bq-ch-migrator ch2bq event-driven \
  --ch-host clickhouse.example.com --ch-port 8443 --ch-cluster my_cluster \
  --ch-database default --ch-source-table my_table \
  --storage-type gcs --bucket my-bucket --bucket-path exports/my_table \
  --storage-access-key $GCS_HMAC_ACCESS_KEY --storage-secret-key $GCS_HMAC_SECRET \
  --bq-project my-project --bq-dataset my_dataset --bq-table my_table \
  --cf-region us-central1
```

### list-schedules

List BigQuery scheduled queries created by this tool.

```bash
bq-ch-migrator list-schedules --bq-project my-project --bq-location US
```

### delete-schedule

Delete a BigQuery scheduled query created by `scheduled-cdc`.

```bash
bq-ch-migrator delete-schedule --transfer-config-name "projects/my-project/locations/US/transferConfigs/123"
```

### delete-cloud-function

Delete a Cloud Function deployed by `ch2bq event-driven`.

```bash
bq-ch-migrator delete-cloud-function \
  --gcp-project my-project --region us-central1 --function-name bq-ch-migrator-my-dataset-my-table
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

ClickHouse ──INSERT INTO s3()──▶ GCS/S3 (Parquet) ──▶ BigQuery
                                                       ├─ LOAD DATA (snapshot)
                                                       ├─ Data Transfer Service (scheduled)
                                                       └─ Cloud Function (event-driven)
```

### bq2ch: Snapshot Flow
1. Introspect BigQuery table schema
2. Create ClickHouse table with mapped types (`ReplicatedMergeTree ON CLUSTER`)
3. `EXPORT DATA` from BigQuery to Parquet files in GCS/S3
4. `INSERT INTO ... SELECT FROM s3Cluster(...)` to bulk load into ClickHouse

### bq2ch: Partition Snapshot Flow
1. Same schema introspection + table creation
2. Detect partition type (column or ingestion-time) and find N latest partitions
3. Export each partition to Parquet files in GCS/S3
4. Ingest each partition into ClickHouse via `s3Cluster`

### bq2ch: CDC Flow (Polling)
1. Same schema introspection + table creation
2. Create S3Queue table + Materialized View in ClickHouse
3. Initial full export
4. Poll BigQuery for new rows (watermark column) and export incremental Parquet files
5. S3Queue automatically detects and ingests new files via the MV

### bq2ch: CDC Flow (Scheduled)
1. Same schema introspection + table creation + S3Queue setup
2. Initial full export
3. Create a BigQuery scheduled query that runs `EXPORT DATA` on a cron using `@run_time`
4. S3Queue continuously picks up new Parquet files — no local process needed

### ch2bq: Snapshot Flow
1. Introspect ClickHouse table schema
2. Create BigQuery table with mapped types
3. `INSERT INTO s3(...)` to export Parquet files to GCS/S3
4. Load Parquet files from GCS into BigQuery

### ch2bq: Scheduled CDC Flow
1. Same schema introspection + BQ table creation
2. Create S3 engine table + Materialized View on ClickHouse (time-partitioned paths)
3. Initial snapshot export + BQ load
4. Create BQ Data Transfer Service scheduled load using `{run_time}` parameterized paths

### ch2bq: Event-Driven CDC Flow
1. Same schema introspection + BQ table creation
2. Create S3 engine table + Materialized View on ClickHouse
3. Initial snapshot export + BQ load
4. Deploy Cloud Function (gen2, Eventarc) that triggers BQ load on GCS file finalization

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
