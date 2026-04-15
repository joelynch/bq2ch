import importlib.resources
import shutil
import subprocess
import tempfile

from google.cloud import bigquery, bigquery_datatransfer
from google.protobuf.struct_pb2 import Struct
from rich.console import Console

from bq_ch_migrator.config import StorageConfig

console = Console()


def load_from_gcs(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    gcs_uri: str,
) -> int:
    """Load Parquet files from GCS into a BigQuery table.

    Returns the number of rows loaded.
    """
    table_ref = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    console.print(f"[bold]Loading into BigQuery from {gcs_uri}...[/bold]")
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    result = load_job.result()
    rows = result.output_rows or 0
    console.print(f"[green]Loaded {rows:,} rows into `{table_ref}`.[/green]")
    return rows


def create_scheduled_load(
    project: str,
    location: str,
    dataset: str,
    table: str,
    storage: StorageConfig,
    schedule: str = "every 1 hours",
    display_name: str | None = None,
    service_account: str | None = None,
) -> str:
    """Create a BQ Data Transfer Service scheduled load from GCS.

    Uses ``data_source_id = "google_cloud_storage"`` with a parameterized
    ``data_path_template`` that includes ``{run_time}`` for time-partitioned paths.

    Returns the transfer config resource name.
    """
    data_path = storage.bq_scheduled_load_uri()

    if display_name is None:
        display_name = f"bq-ch-migrator: ch2bq {project}.{dataset}.{table}"

    params = Struct()
    params.update(
        {
            "data_path_template": data_path,
            "destination_table_name_template": table,
            "file_format": "PARQUET",
            "write_disposition": "APPEND",
        }
    )

    transfer_config = bigquery_datatransfer.TransferConfig(
        display_name=display_name,
        data_source_id="google_cloud_storage",
        destination_dataset_id=dataset,
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

    console.print(f"[green]Scheduled GCS load created:[/green] {result.name}")
    console.print(f"  Schedule: {schedule}")
    console.print(f"  Data path: {data_path}")
    console.print(f"  Display name: {display_name}")

    return result.name


# ── Cloud Function deployment ───────────────────────────────────────────────


def _get_template_dir() -> str:
    """Copy the bundled Cloud Function template to a temp directory."""
    src = importlib.resources.files("bq_ch_migrator") / "cloud_function_template"
    tmp = tempfile.mkdtemp(prefix="bq_ch_cf_")
    shutil.copytree(str(src), tmp, dirs_exist_ok=True)
    return tmp


def deploy_cloud_function(
    gcp_project: str,
    region: str,
    bucket: str,
    bq_project: str,
    bq_dataset: str,
    bq_table: str,
    function_name: str,
    service_account: str | None = None,
) -> str:
    """Deploy a gen2 Cloud Function triggered by GCS object finalization.

    Returns the function name.
    """
    source_dir = _get_template_dir()

    cmd = [
        "gcloud",
        "functions",
        "deploy",
        function_name,
        "--gen2",
        "--runtime=python311",
        f"--region={region}",
        f"--project={gcp_project}",
        "--trigger-event-filters=type=google.cloud.storage.object.v1.finalized",
        f"--trigger-event-filters=bucket={bucket}",
        f"--source={source_dir}",
        "--entry-point=handle_gcs_event",
        f"--set-env-vars=BQ_PROJECT={bq_project},BQ_DATASET={bq_dataset},BQ_TABLE={bq_table}",
        "--quiet",
    ]
    if service_account:
        cmd.append(f"--run-as={service_account}")

    console.print(f"[bold]Deploying Cloud Function '{function_name}'...[/bold]")
    console.print(f"[dim]{' '.join(cmd)}[/dim]")

    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        console.print(f"[red bold]Cloud Function deployment failed:[/red bold]")
        console.print(proc.stderr)
        raise RuntimeError(f"gcloud functions deploy failed (exit {proc.returncode})")

    console.print(f"[green]Cloud Function '{function_name}' deployed.[/green]")
    return function_name


def delete_cloud_function(
    gcp_project: str,
    region: str,
    function_name: str,
) -> None:
    """Delete a Cloud Function."""
    cmd = [
        "gcloud",
        "functions",
        "delete",
        function_name,
        f"--region={region}",
        f"--project={gcp_project}",
        "--quiet",
    ]
    console.print(f"[bold]Deleting Cloud Function '{function_name}'...[/bold]")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        console.print(f"[red bold]Deletion failed:[/red bold]")
        console.print(proc.stderr)
        raise RuntimeError(f"gcloud functions delete failed (exit {proc.returncode})")
    console.print(f"[green]Cloud Function '{function_name}' deleted.[/green]")
