# ClickHouse S3Queue Table Engine

The S3Queue engine provides streaming import from S3-compatible storage (including GCS). It works like Kafka or RabbitMQ engines — each file is processed exactly once, tracked via ZooKeeper/Keeper.

## How It Works

1. Create an S3Queue table pointing at a bucket path with a glob pattern.
2. Attach a **Materialized View** that reads from the S3Queue and writes to a destination MergeTree table.
3. When the MV is attached, S3Queue starts collecting data in the background automatically.
4. New files matching the glob are detected, processed, and tracked in Keeper.

## Create Table Syntax

```sql
CREATE TABLE my_queue (
    col1 String,
    col2 UInt32
)
ENGINE = S3Queue(
    'https://storage.googleapis.com/my-bucket/path/*.parquet',
    'ACCESS_KEY',
    'SECRET_KEY',
    'Parquet'
)
SETTINGS
    mode = 'ordered',
    keeper_path = '/my/queue/path';
```

## Key Settings

| Setting                        | Description                                                    | Default     |
|--------------------------------|----------------------------------------------------------------|-------------|
| `mode`                         | **Required (≥24.6).** `ordered` or `unordered`                 | —           |
| `keeper_path`                  | ZooKeeper/Keeper path for metadata                             | auto        |
| `after_processing`             | What to do with files after processing: `keep`, `delete`, `move`, `tag` | `keep` |
| `loading_retries`              | Number of retries on file load failure                         | `0`         |
| `processing_threads_num`       | Number of processing threads (unordered mode)                  | CPUs or 16  |
| `polling_min_timeout_ms`       | Min poll interval                                              | `1000`      |
| `polling_max_timeout_ms`       | Max poll interval                                              | `10000`     |
| `tracked_files_limit`          | Max tracked files in Keeper (unordered mode)                   | `1000`      |
| `tracked_file_ttl_sec`         | TTL for processed file tracking (unordered mode)               | `0` (forever)|

## Modes

### Ordered Mode
- Files processed in **lexicographic order** by name.
- Only stores the max processed filename in Keeper — efficient.
- Files added with names alphabetically before the last processed file are **skipped**.
- Best for timestamped file prefixes (e.g., `20260415_120000/`).
- Supports `buckets` setting for distributed processing across replicas.

### Unordered Mode
- All files tracked individually in Keeper nodes.
- No ordering guarantee — any file can be processed in any order.
- Higher Keeper storage usage.

## Typical Setup Pattern

```sql
-- 1. Destination table
CREATE TABLE my_data ON CLUSTER my_cluster (
    col1 String,
    col2 UInt32
)
ENGINE = ReplicatedMergeTree
ORDER BY col1;

-- 2. S3Queue source table
CREATE TABLE my_data_queue ON CLUSTER my_cluster (
    col1 String,
    col2 UInt32
)
ENGINE = S3Queue(
    'https://storage.googleapis.com/my-bucket/exports/*.parquet',
    'HMAC_KEY', 'HMAC_SECRET', 'Parquet'
)
SETTINGS
    mode = 'ordered',
    keeper_path = '/my/s3queue/path';

-- 3. Materialized View (triggers background ingestion)
CREATE MATERIALIZED VIEW my_data_consumer ON CLUSTER my_cluster
TO my_data
AS SELECT * FROM my_data_queue;
```

## GCS Compatibility

Use the S3-compatible GCS endpoint:
```
https://storage.googleapis.com/BUCKET_NAME/path/*.parquet
```

With HMAC keys for authentication.

## Virtual Columns

| Column   | Description                   |
|----------|-------------------------------|
| `_path`  | Full path to the file         |
| `_file`  | Filename only                 |
| `_size`  | File size in bytes            |
| `_time`  | File creation time            |

## Introspection

- `system.s3queue_metadata_cache` — In-memory state (currently processing, processed, failed).
- `system.s3queue_log` — Persistent log of processed and failed files.

## Limitations

1. **Duplicates possible** if: parsing exception mid-file with retries enabled, Keeper session expires, or abnormal server termination.
2. `SELECT` from S3Queue is forbidden by default (set `stream_like_engine_allow_direct_select = true` to override).
3. In distributed (multi-replica) setups, `buckets` should be ≥ number of replicas.

## References

- [S3Queue table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3queue)
