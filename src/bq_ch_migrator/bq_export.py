from dataclasses import dataclass
from datetime import datetime, timedelta

from google.cloud import bigquery
from rich.console import Console
from rich.prompt import Confirm

from bq_ch_migrator.config import StorageConfig, StorageType

console = Console()


def build_select_list(
    fields: list[bigquery.SchemaField],
    source: str,
) -> str:
    """Build a SELECT column list, casting unsupported Parquet types to STRING.

    BigQuery cannot export JSON, RANGE, or INTERVAL columns to Parquet directly.
    Returns 'SELECT *' if no problematic columns are present, otherwise returns
    an explicit column list with TO_JSON_STRING(col) for those fields.
    """
    _CAST_TO_STRING_TYPES = {"JSON", "RANGE", "INTERVAL"}
    needs_cast = any(f.field_type in _CAST_TO_STRING_TYPES for f in fields)
    if not needs_cast:
        return f"SELECT * FROM {source}"

    cols = []
    for f in fields:
        if f.field_type in _CAST_TO_STRING_TYPES:
            cols.append(f"TO_JSON_STRING(`{f.name}`) AS `{f.name}`")
        else:
            cols.append(f"`{f.name}`")
    return f"SELECT {', '.join(cols)} FROM {source}"


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
    fields: list[bigquery.SchemaField] | None = None,
) -> str:
    dest_uri = storage.bq_export_uri()
    source = f"`{project}.{dataset}.{table}`"
    select = build_select_list(fields, source) if fields else f"SELECT * FROM {source}"

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
            f"{select}"
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
            f"{select}"
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
    fields: list[bigquery.SchemaField] | None = None,
) -> str:
    dest_uri = storage.bq_export_uri(suffix=f"{export_prefix}/*.parquet")
    source = f"`{project}.{dataset}.{table}`"
    select = build_select_list(fields, source) if fields else f"SELECT * FROM {source}"
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
            f"{select}\n{where}"
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
            f"{select}\n{where}"
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


# ── Partition-aware export ──────────────────────────────────────────────────


@dataclass
class PartitionInfo:
    """Describes how a BigQuery table is partitioned."""

    partition_type: str  # "DAY", "HOUR", "MONTH", "YEAR"
    column: str | None  # None for ingestion-time partitioning
    is_ingestion_time: bool


def get_partition_info(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
) -> PartitionInfo:
    """Introspect BigQuery table metadata to discover partition configuration."""
    bq_table = bq_client.get_table(f"{project}.{dataset}.{table}")

    tp = bq_table.time_partitioning
    if tp is None:
        raise ValueError(
            f"Table `{project}.{dataset}.{table}` is not time-partitioned."
        )

    is_ingestion_time = tp.field is None
    return PartitionInfo(
        partition_type=tp.type_ or "DAY",
        column=tp.field,
        is_ingestion_time=is_ingestion_time,
    )


def get_latest_partition_ids(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    num_partitions: int = 1,
) -> list[str]:
    """Query INFORMATION_SCHEMA.PARTITIONS to find the N latest non-empty partitions."""
    sql = (
        f"SELECT partition_id "
        f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS` "
        f"WHERE table_name = '{table}' "
        f"  AND partition_id != '__NULL__' "
        f"  AND total_rows > 0 "
        f"ORDER BY partition_id DESC "
        f"LIMIT {num_partitions}"
    )
    rows = list(bq_client.query(sql).result())
    if not rows:
        raise ValueError(
            f"No non-empty partitions found for `{project}.{dataset}.{table}`."
        )
    return [row.partition_id for row in rows]


_PARTITION_ID_FORMATS: dict[str, str] = {
    "HOUR": "%Y%m%d%H",
    "DAY": "%Y%m%d",
    "MONTH": "%Y%m",
    "YEAR": "%Y",
}

_PARTITION_SQL_FORMATS: dict[str, str] = {
    "HOUR": "%Y-%m-%d %H:%M:%S",
    "DAY": "%Y-%m-%d",
    "MONTH": "%Y-%m-%d",
    "YEAR": "%Y-%m-%d",
}

_PARTITION_GRANULARITY: dict[str, timedelta] = {
    "HOUR": timedelta(hours=1),
    "DAY": timedelta(days=1),
    "MONTH": timedelta(days=32),  # overshot on purpose, clamped later
    "YEAR": timedelta(days=366),
}


def _partition_id_to_range(
    partition_id: str, partition_type: str
) -> tuple[datetime, datetime]:
    """Convert a partition ID like '20260415' into (start, end_exclusive) datetimes."""
    fmt = _PARTITION_ID_FORMATS.get(partition_type)
    if fmt is None:
        raise ValueError(f"Unsupported partition type: {partition_type}")

    start = datetime.strptime(partition_id, fmt)

    if partition_type == "HOUR":
        end = start + timedelta(hours=1)
    elif partition_type == "DAY":
        end = start + timedelta(days=1)
    elif partition_type == "MONTH":
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    elif partition_type == "YEAR":
        end = start.replace(year=start.year + 1)
    else:
        raise ValueError(f"Unsupported partition type: {partition_type}")

    return start, end


def _build_partition_filter(
    info: PartitionInfo,
    partition_id: str,
) -> str:
    """Build a WHERE clause that targets a single partition."""
    start, end = _partition_id_to_range(partition_id, info.partition_type)
    sql_fmt = _PARTITION_SQL_FORMATS[info.partition_type]

    if info.is_ingestion_time:
        col = "_PARTITIONTIME"
    else:
        col = f"`{info.column}`"

    return (
        f"WHERE {col} >= '{start.strftime(sql_fmt)}' "
        f"AND {col} < '{end.strftime(sql_fmt)}'"
    )


def export_partitions(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    storage: StorageConfig,
    num_partitions: int = 1,
    fields: list[bigquery.SchemaField] | None = None,
) -> list[str]:
    """Export the N latest partitions of a partitioned BigQuery table.

    Returns the list of exported partition IDs.
    """
    info = get_partition_info(bq_client, project, dataset, table)
    partition_ids = get_latest_partition_ids(
        bq_client, project, dataset, table, num_partitions=num_partitions
    )

    console.print(
        f"[bold]Partition info:[/bold] type={info.partition_type}, "
        f"column={'_PARTITIONTIME' if info.is_ingestion_time else info.column}"
    )
    console.print(
        f"[bold]Exporting {len(partition_ids)} partition(s):[/bold] "
        f"{', '.join(partition_ids)}"
    )

    source = f"`{project}.{dataset}.{table}`"
    select = build_select_list(fields, source) if fields else f"SELECT * FROM {source}"

    for partition_id in partition_ids:
        where = _build_partition_filter(info, partition_id)
        dest_uri = storage.bq_export_uri(suffix=f"partition_{partition_id}/*.parquet")

        if storage.storage_type == StorageType.S3:
            if not storage.bq_connection:
                raise ValueError(
                    "--bq-connection is required when using S3 storage type"
                )
            sql = (
                f"EXPORT DATA\n"
                f"  WITH CONNECTION `{storage.bq_connection}`\n"
                f"  OPTIONS(\n"
                f"    uri='{dest_uri}',\n"
                f"    format='PARQUET',\n"
                f"    compression='SNAPPY',\n"
                f"    overwrite=true\n"
                f"  ) AS\n"
                f"{select}\n{where}"
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
                f"{select}\n{where}"
            )

        _run_export_query(bq_client, sql)

    return partition_ids
