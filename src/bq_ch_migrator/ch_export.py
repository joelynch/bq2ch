from clickhouse_connect.driver import Client
from rich.console import Console

from bq_ch_migrator.config import ClickHouseConfig, StorageConfig

console = Console()


def export_snapshot(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
    storage: StorageConfig,
) -> None:
    """One-off export of a full ClickHouse table to GCS/S3 as Parquet."""
    url = storage.ch_s3_export_url()
    sql = (
        f"INSERT INTO FUNCTION s3(\n"
        f"    '{url}',\n"
        f"    '{storage.access_key}',\n"
        f"    '{storage.secret_key}',\n"
        f"    'Parquet'\n"
        f")\n"
        f"SELECT * FROM `{ch_cfg.database}`.`{ch_cfg.table}`"
    )
    console.print("[bold]Exporting ClickHouse table to storage...[/bold]")
    console.print(f"[dim]{sql}[/dim]")
    ch_client.command(sql)
    console.print("[green]Export complete.[/green]")


def setup_s3_export_table(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
    storage: StorageConfig,
    columns_ddl: str,
    partition_expr: str | None = None,
) -> str:
    """Create an S3 engine table on ClickHouse for continuous Parquet writes.

    Returns the name of the created table.
    """
    export_table = f"{ch_cfg.table}_s3_export"
    url = storage.ch_s3_export_url()

    parts = [
        f"CREATE TABLE IF NOT EXISTS `{ch_cfg.database}`.`{export_table}` "
        f"ON CLUSTER `{ch_cfg.cluster}`",
        f"(\n{columns_ddl}\n)",
        (
            f"ENGINE = S3(\n"
            f"    '{url}',\n"
            f"    '{storage.access_key}',\n"
            f"    '{storage.secret_key}',\n"
            f"    'Parquet'\n"
            f")"
        ),
    ]
    settings = ["s3_create_new_file_on_insert = 1"]
    if partition_expr:
        parts.append(f"PARTITION BY {partition_expr}")
    parts.append(f"SETTINGS {', '.join(settings)}")

    ddl = "\n".join(parts)
    console.print("[bold]Creating S3 export table...[/bold]")
    console.print(f"[dim]{ddl}[/dim]")
    ch_client.command(ddl)
    console.print(f"[green]S3 export table `{export_table}` created.[/green]")
    return export_table


def setup_export_mv(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
    source_table: str,
    export_table: str,
) -> str:
    """Create a materialized view that streams inserts from source_table to the S3 export table.

    Returns the MV name.
    """
    mv_name = f"{ch_cfg.table}_s3_consumer"
    ddl = (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS "
        f"`{ch_cfg.database}`.`{mv_name}` "
        f"ON CLUSTER `{ch_cfg.cluster}` "
        f"TO `{ch_cfg.database}`.`{export_table}`\n"
        f"AS SELECT * FROM `{ch_cfg.database}`.`{source_table}`"
    )
    console.print("[bold]Creating export Materialized View...[/bold]")
    console.print(f"[dim]{ddl}[/dim]")
    ch_client.command(ddl)
    console.print(f"[green]Materialized View `{mv_name}` created.[/green]")
    return mv_name


def teardown_export(
    ch_client: Client,
    ch_cfg: ClickHouseConfig,
) -> None:
    """Drop the export MV and S3 engine table."""
    mv_name = f"{ch_cfg.table}_s3_consumer"
    export_table = f"{ch_cfg.table}_s3_export"

    ch_client.command(
        f"DROP VIEW IF EXISTS `{ch_cfg.database}`.`{mv_name}` "
        f"ON CLUSTER `{ch_cfg.cluster}`"
    )
    console.print(f"[green]Dropped MV `{mv_name}`.[/green]")

    ch_client.command(
        f"DROP TABLE IF EXISTS `{ch_cfg.database}`.`{export_table}` "
        f"ON CLUSTER `{ch_cfg.cluster}`"
    )
    console.print(f"[green]Dropped S3 export table `{export_table}`.[/green]")
