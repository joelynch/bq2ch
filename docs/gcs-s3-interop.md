# GCS S3-Compatible API (Interoperability)

Google Cloud Storage provides an **S3-compatible JSON and XML API** that allows tools expecting Amazon S3 to work with GCS buckets. This is how ClickHouse reads from GCS using its `s3()`, `s3Cluster()`, and `S3Queue` functions.

## Endpoint

```
https://storage.googleapis.com
```

## URL Format

```
https://storage.googleapis.com/BUCKET_NAME/OBJECT_PATH
```

Example:
```
https://storage.googleapis.com/my-bucket/exports/data-*.parquet
```

## Authentication: HMAC Keys

GCS uses **HMAC keys** for S3-compatible access (not OAuth or service account keys):

| Field               | Description                              |
|---------------------|------------------------------------------|
| Access Key          | Identifies the key pair (like AWS Access Key ID) |
| Secret Key          | Used to sign requests (like AWS Secret Access Key) |

### Creating HMAC Keys

1. Go to **GCP Console → Cloud Storage → Settings → Interoperability**.
2. Under "Service account HMAC", click **Create a key for a service account**.
3. Select your service account → **Create key**.
4. Save the Access Key and Secret (secret is shown only once).

Or via `gcloud`:
```bash
gcloud storage hmac create SA_EMAIL
```

## Usage in ClickHouse

```sql
-- s3Cluster
SELECT * FROM s3Cluster(
    'my_cluster',
    'https://storage.googleapis.com/my-bucket/path/*.parquet',
    'HMAC_ACCESS_KEY',
    'HMAC_SECRET_KEY',
    'Parquet'
)

-- S3Queue engine
CREATE TABLE my_queue (...)
ENGINE = S3Queue(
    'https://storage.googleapis.com/my-bucket/path/*.parquet',
    'HMAC_ACCESS_KEY',
    'HMAC_SECRET_KEY',
    'Parquet'
)
SETTINGS mode = 'ordered', ...;
```

## Usage in BigQuery

BigQuery exports to GCS natively using `gs://` URIs:
```sql
EXPORT DATA OPTIONS(uri='gs://my-bucket/path/*.parquet', ...)
```

No HMAC keys needed — BigQuery uses IAM permissions on the GCS bucket.

## Environment Variables

This tool uses the following convention:
- `GCS_HMAC_ACCESS_KEY` — HMAC access key for ClickHouse to read from GCS
- `GCS_HMAC_SECRET` — HMAC secret key

## References

- [GCS Interoperability](https://cloud.google.com/storage/docs/interoperability)
- [HMAC keys for service accounts](https://cloud.google.com/storage/docs/authentication/hmackeys)
- [ClickHouse S3 table function](https://clickhouse.com/docs/en/sql-reference/table-functions/s3)
