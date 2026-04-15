import clickhouse_connect
from clickhouse_connect.driver import Client
from google.cloud import bigquery
from rich.console import Console

from bq_ch_migrator.config import ClickHouseConfig, StorageConfig
from bq_ch_migrator.schema import generate_ch_columns, generate_create_table_sql

console = Console()


def get_ch_client(cfg: ClickHouseConfig) -> Client:
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        secure=cfg.secure,
    )


def create_destination_table(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
    fields: list[bigquery.SchemaField],
    order_by: str | None = None,
    partition_by: str | None = None,
) -> None:
    ddl = generate_create_table_sql(
        database=ch_cfg.database,
        table=ch_cfg.table,
        cluster=ch_cfg.cluster,
        fields=fields,
        order_by=order_by,
        partition_by=partition_by,
    )
    console.print("[bold]Creating ClickHouse destination table...[/bold]")
    console.print(f"[dim]{ddl}[/dim]")
    ch_client.command(ddl)
    console.print("[green]Table created (or already exists).[/green]")


def ingest_from_storage(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
    storage: StorageConfig,
) -> None:
    url = storage.ch_storage_url()
    sql = (
        f"INSERT INTO `{ch_cfg.database}`.`{ch_cfg.table}`\n"
        f"SELECT * FROM s3Cluster(\n"
        f"    '{ch_cfg.cluster}',\n"
        f"    '{url}',\n"
        f"    '{storage.access_key}',\n"
        f"    '{storage.secret_key}',\n"
        f"    'Parquet'\n"
        f")"
    )
    console.print("[bold]Ingesting into ClickHouse via s3Cluster...[/bold]")
    console.print(f"[dim]{sql}[/dim]")
    ch_client.command(sql)
    console.print("[green]Ingestion complete.[/green]")


def setup_s3queue(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
    storage: StorageConfig,
    fields: list[bigquery.SchemaField],
    order_by: str | None = None,
    partition_by: str | None = None,
) -> None:
    columns = generate_ch_columns(fields)
    queue_table = f"{ch_cfg.table}_queue"
    mv_name = f"{ch_cfg.table}_consumer"
    url = storage.ch_storage_url()
    keeper_path = f"/bigquery/{ch_cfg.database}/{queue_table}"

    queue_ddl = (
        f"CREATE TABLE IF NOT EXISTS `{ch_cfg.database}`.`{queue_table}` "
        f"ON CLUSTER `{ch_cfg.cluster}`\n"
        f"(\n{columns}\n)\n"
        f"ENGINE = S3Queue(\n"
        f"    '{url}',\n"
        f"    '{storage.access_key}',\n"
        f"    '{storage.secret_key}',\n"
        f"    'Parquet'\n"
        f")\n"
        f"SETTINGS\n"
        f"    mode = 'ordered',\n"
        f"    keeper_path = '{keeper_path}'"
    )
    console.print("[bold]Creating S3Queue table...[/bold]")
    console.print(f"[dim]{queue_ddl}[/dim]")
    ch_client.command(queue_ddl)
    console.print(f"[green]S3Queue table `{queue_table}` created.[/green]")

    mv_ddl = (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS "
        f"`{ch_cfg.database}`.`{mv_name}` "
        f"ON CLUSTER `{ch_cfg.cluster}` "
        f"TO `{ch_cfg.database}`.`{ch_cfg.table}`\n"
        f"AS SELECT * FROM `{ch_cfg.database}`.`{queue_table}`"
    )
    console.print("[bold]Creating Materialized View...[/bold]")
    console.print(f"[dim]{mv_ddl}[/dim]")
    ch_client.command(mv_ddl)
    console.print(f"[green]Materialized View `{mv_name}` created.[/green]")


def get_ch_row_count(
    ch_client: Client,
    database: str,
    table: str,
) -> int:
    result = ch_client.command(f"SELECT count() FROM `{database}`.`{table}`")
    return int(result)
