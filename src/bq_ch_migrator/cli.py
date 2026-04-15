import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Optional

import typer
from google.cloud import bigquery
from rich.console import Console

from bq_ch_migrator.bq_export import (
    export_full_table,
    export_incremental,
    get_bq_row_count,
)
from bq_ch_migrator.bq_scheduled import (
    create_scheduled_export,
    delete_scheduled_export,
    list_scheduled_exports,
)
from bq_ch_migrator.ch_ingest import (
    create_destination_table,
    get_ch_client,
    get_ch_row_count,
    ingest_from_storage,
    setup_s3queue,
)
from bq_ch_migrator.config import ClickHouseConfig, StorageConfig, StorageType
from bq_ch_migrator.schema import introspect_bq_schema
from bq_ch_migrator.watermark import WatermarkState, get_current_max_watermark

app = typer.Typer(
    name="bq-ch-migrator",
    help="Migrate BigQuery tables to ClickHouse via GCS or S3.",
    rich_markup_mode="rich",
)
console = Console()

# ── Shared option types ─────────────────────────────────────────────────────

BqProject = Annotated[
    str, typer.Option("--bq-project", envvar="BQ_PROJECT", help="BigQuery project ID")
]
BqDataset = Annotated[
    str, typer.Option("--bq-dataset", envvar="BQ_DATASET", help="BigQuery dataset")
]
BqTable = Annotated[
    str, typer.Option("--bq-table", envvar="BQ_TABLE", help="BigQuery table")
]

StorageTypeOpt = Annotated[
    StorageType,
    typer.Option(
        "--storage-type", envvar="STORAGE_TYPE", help="Intermediate storage: gcs or s3"
    ),
]
Bucket = Annotated[
    str, typer.Option("--bucket", envvar="STORAGE_BUCKET", help="GCS or S3 bucket name")
]
BucketPath = Annotated[
    str,
    typer.Option(
        "--bucket-path",
        envvar="STORAGE_BUCKET_PATH",
        help="Path prefix inside the bucket",
    ),
]
StorageAccessKey = Annotated[
    str,
    typer.Option(
        "--storage-access-key",
        envvar=["GCS_HMAC_ACCESS_KEY", "AWS_ACCESS_KEY_ID"],
        help="HMAC/AWS access key for storage",
    ),
]
StorageSecretKey = Annotated[
    str,
    typer.Option(
        "--storage-secret-key",
        envvar=["GCS_HMAC_SECRET", "AWS_SECRET_ACCESS_KEY"],
        help="HMAC/AWS secret key for storage",
    ),
]
BqConnection = Annotated[
    Optional[str],
    typer.Option(
        "--bq-connection",
        envvar="BQ_CONNECTION",
        help="BigQuery Connection name (required for S3)",
    ),
]

ChHost = Annotated[
    str, typer.Option("--ch-host", envvar="CH_HOST", help="ClickHouse host")
]
ChPort = Annotated[
    int, typer.Option("--ch-port", envvar="CH_PORT", help="ClickHouse HTTP(S) port")
]
ChUser = Annotated[
    str, typer.Option("--ch-user", envvar="CH_USER", help="ClickHouse username")
]
ChPassword = Annotated[
    str, typer.Option("--ch-password", envvar="CH_PASSWORD", help="ClickHouse password")
]
ChDatabase = Annotated[
    str, typer.Option("--ch-database", envvar="CH_DATABASE", help="ClickHouse database")
]
ChTable = Annotated[
    str,
    typer.Option("--ch-table", envvar="CH_TABLE", help="ClickHouse destination table"),
]
ChCluster = Annotated[
    str,
    typer.Option("--ch-cluster", envvar="CH_CLUSTER", help="ClickHouse cluster name"),
]
ChSecure = Annotated[
    bool,
    typer.Option(
        "--ch-secure/--ch-no-secure", envvar="CH_SECURE", help="Use TLS for ClickHouse"
    ),
]

OrderBy = Annotated[
    Optional[str],
    typer.Option(
        "--order-by", help="ClickHouse ORDER BY expression (default: tuple())"
    ),
]
PartitionBy = Annotated[
    Optional[str],
    typer.Option("--partition-by", help="ClickHouse PARTITION BY expression"),
]


@app.command()
def snapshot(
    bq_project: BqProject,
    bq_dataset: BqDataset,
    bq_table: BqTable,
    storage_type: StorageTypeOpt,
    bucket: Bucket,
    bucket_path: BucketPath,
    storage_access_key: StorageAccessKey,
    storage_secret_key: StorageSecretKey,
    ch_host: ChHost,
    ch_port: ChPort = 8443,
    ch_user: ChUser = "default",
    ch_password: ChPassword = "",
    ch_database: ChDatabase = "default",
    ch_table: ChTable = "",
    ch_cluster: ChCluster = "",
    ch_secure: ChSecure = True,
    bq_connection: BqConnection = None,
    order_by: OrderBy = None,
    partition_by: PartitionBy = None,
) -> None:
    """One-off full table migration: BQ → Parquet in GCS/S3 → ClickHouse via s3Cluster."""
    if not ch_table:
        ch_table = bq_table
    if not ch_cluster:
        typer.echo("Error: --ch-cluster is required.", err=True)
        raise typer.Exit(1)

    storage = StorageConfig(
        storage_type=storage_type,
        bucket=bucket,
        bucket_path=bucket_path,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        bq_connection=bq_connection,
    )
    ch_cfg = ClickHouseConfig(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_password,
        database=ch_database,
        table=ch_table,
        cluster=ch_cluster,
        secure=ch_secure,
    )

    bq_client = bigquery.Client(project=bq_project)

    # 1. Introspect schema
    console.print("[bold]Introspecting BigQuery schema...[/bold]")
    fields = introspect_bq_schema(bq_client, bq_project, bq_dataset, bq_table)
    console.print(f"  Found {len(fields)} columns.")

    # 2. Create ClickHouse table
    ch_client = get_ch_client(ch_cfg)
    create_destination_table(
        ch_client, ch_cfg, fields, order_by=order_by, partition_by=partition_by
    )

    # 3. Export from BigQuery
    export_full_table(bq_client, bq_project, bq_dataset, bq_table, storage)

    # 4. Ingest into ClickHouse
    ingest_from_storage(ch_client, ch_cfg, storage)

    # 5. Verify
    bq_count = get_bq_row_count(bq_client, bq_project, bq_dataset, bq_table)
    ch_count = get_ch_row_count(ch_client, ch_cfg.database, ch_cfg.table)
    console.print(f"\n[bold]Verification:[/bold]")
    console.print(f"  BigQuery rows:   {bq_count:,}")
    console.print(f"  ClickHouse rows: {ch_count:,}")
    if bq_count == ch_count:
        console.print("[green bold]Row counts match![/green bold]")
    else:
        console.print("[yellow bold]Warning: row counts differ.[/yellow bold]")


@app.command()
def cdc(
    bq_project: BqProject,
    bq_dataset: BqDataset,
    bq_table: BqTable,
    storage_type: StorageTypeOpt,
    bucket: Bucket,
    bucket_path: BucketPath,
    storage_access_key: StorageAccessKey,
    storage_secret_key: StorageSecretKey,
    watermark_column: Annotated[
        str,
        typer.Option(
            "--watermark-column",
            help="Column to track for incremental exports (e.g. updated_at)",
        ),
    ],
    ch_host: ChHost,
    ch_port: ChPort = 8443,
    ch_user: ChUser = "default",
    ch_password: ChPassword = "",
    ch_database: ChDatabase = "default",
    ch_table: ChTable = "",
    ch_cluster: ChCluster = "",
    ch_secure: ChSecure = True,
    bq_connection: BqConnection = None,
    order_by: OrderBy = None,
    partition_by: PartitionBy = None,
    poll_interval: Annotated[
        int,
        typer.Option(
            "--poll-interval", help="Seconds between incremental export polls"
        ),
    ] = 60,
    watermark_file: Annotated[
        Path,
        typer.Option("--watermark-file", help="Path to local watermark state file"),
    ] = Path("watermark.json"),
) -> None:
    """Continuous CDC migration: BQ → Parquet in GCS/S3 → ClickHouse S3Queue + Materialized View."""
    if not ch_table:
        ch_table = bq_table
    if not ch_cluster:
        typer.echo("Error: --ch-cluster is required.", err=True)
        raise typer.Exit(1)

    storage = StorageConfig(
        storage_type=storage_type,
        bucket=bucket,
        bucket_path=bucket_path,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        bq_connection=bq_connection,
    )
    ch_cfg = ClickHouseConfig(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_password,
        database=ch_database,
        table=ch_table,
        cluster=ch_cluster,
        secure=ch_secure,
    )

    bq_client = bigquery.Client(project=bq_project)

    # 1. Introspect schema
    console.print("[bold]Introspecting BigQuery schema...[/bold]")
    fields = introspect_bq_schema(bq_client, bq_project, bq_dataset, bq_table)
    console.print(f"  Found {len(fields)} columns.")

    # 2. Create ClickHouse destination table
    ch_client = get_ch_client(ch_cfg)
    create_destination_table(
        ch_client, ch_cfg, fields, order_by=order_by, partition_by=partition_by
    )

    # 3. Setup S3Queue + Materialized View
    setup_s3queue(
        ch_client, ch_cfg, storage, fields, order_by=order_by, partition_by=partition_by
    )

    # 4. Initialize watermark
    wm_state = WatermarkState(watermark_file)
    last_watermark = wm_state.load()
    if last_watermark:
        console.print(f"[bold]Resuming from watermark:[/bold] {last_watermark}")
    else:
        console.print(
            "[bold]No previous watermark found. Starting initial export...[/bold]"
        )
        export_full_table(bq_client, bq_project, bq_dataset, bq_table, storage)
        last_watermark = get_current_max_watermark(
            bq_client, bq_project, bq_dataset, bq_table, watermark_column
        )
        if last_watermark:
            wm_state.save(last_watermark)
            console.print(
                f"[green]Initial export done. Watermark: {last_watermark}[/green]"
            )
        else:
            console.print(
                "[yellow]Table appears empty. Will poll for new data.[/yellow]"
            )
            last_watermark = ""

    # 5. Poll loop
    running = True

    def _handle_signal(signum: int, frame: object) -> None:
        nonlocal running
        console.print(
            "\n[yellow bold]Received interrupt, stopping after current cycle...[/yellow bold]"
        )
        running = False

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    console.print(
        f"[bold]Entering poll loop (interval={poll_interval}s). Ctrl+C to stop.[/bold]"
    )

    while running:
        time.sleep(poll_interval)
        if not running:
            break

        current_max = get_current_max_watermark(
            bq_client, bq_project, bq_dataset, bq_table, watermark_column
        )

        if current_max is None or current_max == last_watermark:
            console.print(f"[dim]{_now()} No new data.[/dim]")
            continue

        prefix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        console.print(
            f"[bold]{_now()} New data detected (watermark {last_watermark} → {current_max})[/bold]"
        )

        export_incremental(
            bq_client,
            bq_project,
            bq_dataset,
            bq_table,
            storage,
            watermark_column,
            last_watermark or "",
            export_prefix=prefix,
        )

        last_watermark = current_max
        wm_state.save(last_watermark)
        console.print(
            f"[green]{_now()} Exported. S3Queue will pick up new files automatically.[/green]"
        )

    console.print(
        "[bold]CDC loop stopped. S3Queue and Materialized View remain active in ClickHouse.[/bold]"
    )


@app.command(name="scheduled-cdc")
def scheduled_cdc(
    bq_project: BqProject,
    bq_dataset: BqDataset,
    bq_table: BqTable,
    storage_type: StorageTypeOpt,
    bucket: Bucket,
    bucket_path: BucketPath,
    storage_access_key: StorageAccessKey,
    storage_secret_key: StorageSecretKey,
    watermark_column: Annotated[
        str,
        typer.Option(
            "--watermark-column",
            help="Column to track for incremental exports (e.g. updated_at)",
        ),
    ],
    ch_host: ChHost,
    ch_port: ChPort = 8443,
    ch_user: ChUser = "default",
    ch_password: ChPassword = "",
    ch_database: ChDatabase = "default",
    ch_table: ChTable = "",
    ch_cluster: ChCluster = "",
    ch_secure: ChSecure = True,
    bq_connection: BqConnection = None,
    order_by: OrderBy = None,
    partition_by: PartitionBy = None,
    schedule: Annotated[
        str,
        typer.Option(
            "--schedule",
            help="Cron-like schedule for BQ export (e.g. 'every 15 minutes')",
        ),
    ] = "every 15 minutes",
    bq_location: Annotated[
        str,
        typer.Option(
            "--bq-location",
            envvar="BQ_LOCATION",
            help="BigQuery location/region (e.g. US, europe-west1)",
        ),
    ] = "US",
    service_account: Annotated[
        Optional[str],
        typer.Option(
            "--service-account",
            help="GCP service account email for the scheduled query",
        ),
    ] = None,
) -> None:
    """Scheduled CDC: BQ scheduled query exports Parquet to GCS/S3 → ClickHouse S3Queue picks up files automatically.

    Unlike 'cdc' which runs a local polling loop, this creates a BigQuery scheduled query
    that runs server-side on a cron schedule. No long-running Python process needed.
    """
    if not ch_table:
        ch_table = bq_table
    if not ch_cluster:
        typer.echo("Error: --ch-cluster is required.", err=True)
        raise typer.Exit(1)

    storage = StorageConfig(
        storage_type=storage_type,
        bucket=bucket,
        bucket_path=bucket_path,
        access_key=storage_access_key,
        secret_key=storage_secret_key,
        bq_connection=bq_connection,
    )
    ch_cfg = ClickHouseConfig(
        host=ch_host,
        port=ch_port,
        username=ch_user,
        password=ch_password,
        database=ch_database,
        table=ch_table,
        cluster=ch_cluster,
        secure=ch_secure,
    )

    bq_client = bigquery.Client(project=bq_project)

    # 1. Introspect schema
    console.print("[bold]Introspecting BigQuery schema...[/bold]")
    fields = introspect_bq_schema(bq_client, bq_project, bq_dataset, bq_table)
    console.print(f"  Found {len(fields)} columns.")

    # 2. Create ClickHouse destination table
    ch_client = get_ch_client(ch_cfg)
    create_destination_table(
        ch_client, ch_cfg, fields, order_by=order_by, partition_by=partition_by
    )

    # 3. Setup S3Queue + Materialized View
    setup_s3queue(
        ch_client, ch_cfg, storage, fields, order_by=order_by, partition_by=partition_by
    )

    # 4. Do initial full export
    console.print("[bold]Running initial full export...[/bold]")
    export_full_table(bq_client, bq_project, bq_dataset, bq_table, storage)
    console.print(
        "[green]Initial export complete. S3Queue will ingest the data.[/green]"
    )

    # 5. Create BQ scheduled query for incremental exports
    transfer_name = create_scheduled_export(
        project=bq_project,
        location=bq_location,
        dataset=bq_dataset,
        table=bq_table,
        storage=storage,
        watermark_column=watermark_column,
        schedule=schedule,
        service_account=service_account,
    )

    console.print(f"\n[bold green]Setup complete![/bold green]")
    console.print(f"  BigQuery scheduled query: {transfer_name}")
    console.print(f"  Schedule: {schedule}")
    console.print(
        f"  ClickHouse S3Queue + MV will automatically ingest new Parquet files."
    )
    console.print(
        f"\n  To remove the scheduled query later:\n"
        f"    bq-ch-migrator delete-schedule --transfer-config-name '{transfer_name}'"
    )


@app.command(name="delete-schedule")
def delete_schedule(
    transfer_config_name: Annotated[
        str,
        typer.Option(
            "--transfer-config-name",
            help="Full resource name of the BQ transfer config to delete",
        ),
    ],
) -> None:
    """Delete a BigQuery scheduled query created by scheduled-cdc."""
    delete_scheduled_export(transfer_config_name)


@app.command(name="list-schedules")
def list_schedules(
    bq_project: BqProject,
    bq_location: Annotated[
        str,
        typer.Option(
            "--bq-location",
            envvar="BQ_LOCATION",
            help="BigQuery location/region (e.g. US, europe-west1)",
        ),
    ] = "US",
) -> None:
    """List BigQuery scheduled queries created by this tool."""
    configs = list_scheduled_exports(bq_project, bq_location)
    if not configs:
        console.print("[dim]No scheduled exports found.[/dim]")
        return
    for cfg in configs:
        state = cfg.state.name if cfg.state else "UNKNOWN"
        console.print(f"  [bold]{cfg.display_name}[/bold]")
        console.print(f"    Name: {cfg.name}")
        console.print(f"    Schedule: {cfg.schedule}")
        console.print(f"    State: {state}")
        console.print()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("[%H:%M:%S]")


if __name__ == "__main__":
    app()
