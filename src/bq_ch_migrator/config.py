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
    port: int = 8443
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
