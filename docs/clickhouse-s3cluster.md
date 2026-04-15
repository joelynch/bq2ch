# ClickHouse s3Cluster Table Function

The `s3Cluster` table function reads/writes files from S3-compatible storage (including GCS via S3-compatible API) in parallel across all nodes in a ClickHouse cluster.

## Syntax

```sql
s3Cluster(
    cluster_name,
    url
    [, NOSIGN | access_key_id, secret_access_key]
    [, format]
    [, structure]
    [, compression_method]
)
```

## Parameters

| Parameter              | Description                                                              |
|------------------------|--------------------------------------------------------------------------|
| `cluster_name`         | Name of the ClickHouse cluster to distribute reads across                |
| `url`                  | Path to file(s) — supports wildcards `*`, `**`, `?`, `{a,b}`, `{N..M}` |
| `access_key_id`        | S3 access key (or GCS HMAC access key)                                   |
| `secret_access_key`    | S3 secret key (or GCS HMAC secret)                                       |
| `format`               | File format: `Parquet`, `CSV`, `JSONEachRow`, etc.                       |
| `structure`            | Column definitions (optional if inferrable from format)                  |
| `compression_method`   | `none`, `gzip`, `brotli`, `xz`, `zstd` — auto-detected by default       |

## How It Works

1. The initiator node connects to all nodes in the cluster.
2. It lists all files matching the URL glob pattern.
3. Each file is dynamically dispatched to worker nodes for processing.
4. Workers process and return results to the initiator.

## Usage: Insert from S3/GCS into ClickHouse

```sql
INSERT INTO my_table
SELECT * FROM s3Cluster(
    'my_cluster',
    'https://storage.googleapis.com/my-bucket/exports/*.parquet',
    'HMAC_ACCESS_KEY',
    'HMAC_SECRET',
    'Parquet'
)
```

## Usage: Query S3 data directly

```sql
SELECT count(*) FROM s3Cluster(
    'my_cluster',
    'https://s3.amazonaws.com/my-bucket/data/*.parquet',
    'AWS_ACCESS_KEY',
    'AWS_SECRET_KEY',
    'Parquet'
)
```

## GCS Compatibility

Google Cloud Storage has an S3-compatible API. Use:
- **Endpoint:** `https://storage.googleapis.com`
- **Credentials:** GCS HMAC access key + secret (created in GCP Console → Cloud Storage → Settings → Interoperability)

```sql
SELECT * FROM s3Cluster(
    'my_cluster',
    'https://storage.googleapis.com/my-gcs-bucket/path/*.parquet',
    'GCS_HMAC_ACCESS_KEY',
    'GCS_HMAC_SECRET',
    'Parquet'
)
```

## Wildcard Patterns

| Pattern        | Matches                                        |
|----------------|------------------------------------------------|
| `*`            | Any characters except `/`                      |
| `**`           | Any characters including `/`                   |
| `?`            | Any single character                           |
| `{a,b,c}`     | Any of the listed strings                      |
| `{N..M}`      | Number range N to M (inclusive)                 |

## Single-node Alternative: `s3()` Table Function

If you don't have a cluster, use `s3()` with the same syntax (minus the `cluster_name` parameter).

## References

- [s3Cluster table function](https://clickhouse.com/docs/en/sql-reference/table-functions/s3Cluster)
- [s3 table function](https://clickhouse.com/docs/en/sql-reference/table-functions/s3)
- [S3 table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/s3)
