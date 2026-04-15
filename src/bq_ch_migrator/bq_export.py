from google.cloud import bigquery
from rich.console import Console

from bq_ch_migrator.config import StorageConfig, StorageType

console = Console()


def _run_export_query(bq_client: bigquery.Client, sql: str) -> None:
    console.print(f"[bold]Running BQ EXPORT DATA...[/bold]")
    console.print(f"[dim]{sql}[/dim]")
    query_job = bq_client.query(sql)
    query_job.result()
    console.print("[green]Export complete.[/green]")


def export_full_table(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    storage: StorageConfig,
) -> str:
    dest_uri = storage.bq_export_uri()
    source = f"`{project}.{dataset}.{table}`"

    if storage.storage_type == StorageType.S3:
        if not storage.bq_connection:
            raise ValueError("--bq-connection is required when using S3 storage type")
        sql = (
            f"EXPORT DATA\n"
            f"  WITH CONNECTION `{storage.bq_connection}`\n"
            f"  OPTIONS(\n"
            f"    uri='{dest_uri}',\n"
            f"    format='PARQUET',\n"
            f"    compression='SNAPPY',\n"
            f"    overwrite=true\n"
            f"  ) AS\n"
            f"SELECT * FROM {source}"
        )
    else:
        sql = (
            f"EXPORT DATA\n"
            f"  OPTIONS(\n"
            f"    uri='{dest_uri}',\n"
            f"    format='PARQUET',\n"
            f"    compression='SNAPPY',\n"
            f"    overwrite=true\n"
            f"  ) AS\n"
            f"SELECT * FROM {source}"
        )

    _run_export_query(bq_client, sql)
    return dest_uri


def export_incremental(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    storage: StorageConfig,
    watermark_column: str,
    watermark_value: str,
    export_prefix: str,
) -> str:
    dest_uri = storage.bq_export_uri(suffix=f"{export_prefix}/*.parquet")
    source = f"`{project}.{dataset}.{table}`"
    where = f"WHERE `{watermark_column}` > '{watermark_value}'"

    if storage.storage_type == StorageType.S3:
        if not storage.bq_connection:
            raise ValueError("--bq-connection is required when using S3 storage type")
        sql = (
            f"EXPORT DATA\n"
            f"  WITH CONNECTION `{storage.bq_connection}`\n"
            f"  OPTIONS(\n"
            f"    uri='{dest_uri}',\n"
            f"    format='PARQUET',\n"
            f"    compression='SNAPPY',\n"
            f"    overwrite=true\n"
            f"  ) AS\n"
            f"SELECT * FROM {source}\n{where}"
        )
    else:
        sql = (
            f"EXPORT DATA\n"
            f"  OPTIONS(\n"
            f"    uri='{dest_uri}',\n"
            f"    format='PARQUET',\n"
            f"    compression='SNAPPY',\n"
            f"    overwrite=true\n"
            f"  ) AS\n"
            f"SELECT * FROM {source}\n{where}"
        )

    _run_export_query(bq_client, sql)
    return dest_uri


def get_bq_row_count(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
) -> int:
    table_ref = f"{project}.{dataset}.{table}"
    bq_table = bq_client.get_table(table_ref)
    return bq_table.num_rows
