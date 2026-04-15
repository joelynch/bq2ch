from google.cloud import bigquery_datatransfer
from google.protobuf.struct_pb2 import Struct
from rich.console import Console

from bq_ch_migrator.config import StorageConfig, StorageType

console = Console()


def build_export_query(
    project: str,
    dataset: str,
    table: str,
    storage: StorageConfig,
    watermark_column: str,
) -> str:
    """Build the EXPORT DATA query that uses @run_time for incremental exports."""
    dest_uri = storage.bq_export_uri(suffix="{run_time|'%Y%m%d_%H%M%S'}/*.parquet")
    source = f"`{project}.{dataset}.{table}`"
    where = f"WHERE `{watermark_column}` > TIMESTAMP_SUB(@run_time, INTERVAL 1 HOUR)"

    if storage.storage_type == StorageType.S3:
        if not storage.bq_connection:
            raise ValueError("--bq-connection is required when using S3 storage type")
        return (
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
        return (
            f"EXPORT DATA\n"
            f"  OPTIONS(\n"
            f"    uri='{dest_uri}',\n"
            f"    format='PARQUET',\n"
            f"    compression='SNAPPY',\n"
            f"    overwrite=true\n"
            f"  ) AS\n"
            f"SELECT * FROM {source}\n{where}"
        )


def create_scheduled_export(
    project: str,
    location: str,
    dataset: str,
    table: str,
    storage: StorageConfig,
    watermark_column: str,
    schedule: str = "every 15 minutes",
    display_name: str | None = None,
    service_account: str | None = None,
) -> str:
    """Create a BigQuery scheduled query that exports incremental data to GCS/S3.

    Returns the transfer config name (resource ID).
    """
    query = build_export_query(project, dataset, table, storage, watermark_column)

    if display_name is None:
        display_name = f"bq-ch-migrator: {project}.{dataset}.{table}"

    params = Struct()
    params.update({"query": query})

    transfer_config = bigquery_datatransfer.TransferConfig(
        display_name=display_name,
        data_source_id="scheduled_query",
        schedule=schedule,
        params=params,
    )

    client = bigquery_datatransfer.DataTransferServiceClient()
    parent = f"projects/{project}/locations/{location}"

    request = bigquery_datatransfer.CreateTransferConfigRequest(
        parent=parent,
        transfer_config=transfer_config,
    )

    if service_account:
        request.service_account_name = service_account

    result = client.create_transfer_config(request=request)

    console.print(f"[green]Scheduled query created:[/green] {result.name}")
    console.print(f"  Schedule: {schedule}")
    console.print(f"  Display name: {display_name}")
    console.print(f"[dim]Query:\n{query}[/dim]")

    return result.name


def delete_scheduled_export(transfer_config_name: str) -> None:
    """Delete a scheduled query by its transfer config resource name."""
    client = bigquery_datatransfer.DataTransferServiceClient()
    client.delete_transfer_config(name=transfer_config_name)
    console.print(f"[green]Deleted scheduled query:[/green] {transfer_config_name}")


def list_scheduled_exports(
    project: str,
    location: str,
) -> list[bigquery_datatransfer.TransferConfig]:
    """List scheduled queries created by this tool (filtered by display name prefix)."""
    client = bigquery_datatransfer.DataTransferServiceClient()
    parent = f"projects/{project}/locations/{location}"

    configs = []
    for config in client.list_transfer_configs(
        request=bigquery_datatransfer.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"],
        )
    ):
        if config.display_name.startswith("bq-ch-migrator:"):
            configs.append(config)

    return configs
