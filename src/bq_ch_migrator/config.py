from enum import Enum

from pydantic import BaseModel


class StorageType(str, Enum):
    GCS = "gcs"
    S3 = "s3"


class BigQueryConfig(BaseModel):
    project: str
    dataset: str
    table: str


class ClickHouseConfig(BaseModel):
    host: str
    port: int
    username: str = "default"
    password: str = ""
    database: str = "default"
    table: str
    cluster: str
    secure: bool = True


class StorageConfig(BaseModel):
    storage_type: StorageType
    bucket: str
    bucket_path: str
    access_key: str
    secret_key: str
    bq_connection: str | None = None

    def bq_export_uri(self, suffix: str = "*.parquet") -> str:
        path = self.bucket_path.strip("/")
        if self.storage_type == StorageType.GCS:
            return f"gs://{self.bucket}/{path}/{suffix}"
        return f"s3://{self.bucket}/{path}/{suffix}"

    def ch_storage_url(self, suffix: str = "*.parquet") -> str:
        path = self.bucket_path.strip("/")
        if self.storage_type == StorageType.GCS:
            return f"https://storage.googleapis.com/{self.bucket}/{path}/{suffix}"
        return f"https://s3.amazonaws.com/{self.bucket}/{path}/{suffix}"

    # ── CH → BQ helpers ─────────────────────────────────────────────────────

    def ch_s3_export_url(self) -> str:
        """Base URL for a ClickHouse S3 engine table used for writing Parquet."""
        path = self.bucket_path.strip("/")
        if self.storage_type == StorageType.GCS:
            return f"https://storage.googleapis.com/{self.bucket}/{path}/"
        return f"https://s3.amazonaws.com/{self.bucket}/{path}/"

    def bq_load_uri(self, suffix: str = "*.parquet") -> str:
        """GCS or S3 URI suitable for ``bq_client.load_table_from_uri``."""
        path = self.bucket_path.strip("/")
        if self.storage_type == StorageType.GCS:
            return f"gs://{self.bucket}/{path}/{suffix}"
        return f"s3://{self.bucket}/{path}/{suffix}"

    def bq_scheduled_load_uri(self) -> str:
        """GCS URI with ``{run_time}`` placeholder for BQ Data Transfer Service loads."""
        path = self.bucket_path.strip("/")
        return f'gs://{self.bucket}/{path}/dt={{run_time|"%Y-%m-%d"}}/*.parquet'


class CloudFunctionConfig(BaseModel):
    region: str = "us-central1"
    function_name: str = ""
    service_account: str | None = None
