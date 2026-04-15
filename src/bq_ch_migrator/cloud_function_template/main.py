"""Cloud Function entry point: GCS object finalization → BigQuery load job.

Deployed as a gen2 Cloud Function triggered by Eventarc on
``google.cloud.storage.object.v1.finalized``.

Environment variables (set at deploy time):
    BQ_PROJECT  – BigQuery project ID
    BQ_DATASET  – BigQuery dataset
    BQ_TABLE    – BigQuery destination table
"""

import os

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery


@functions_framework.cloud_event
def handle_gcs_event(cloud_event: CloudEvent) -> None:
    """Handle a GCS object finalization event."""
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]

    # Only process Parquet files
    if not name.endswith(".parquet"):
        return

    bq_project = os.environ["BQ_PROJECT"]
    bq_dataset = os.environ["BQ_DATASET"]
    bq_table = os.environ["BQ_TABLE"]

    gcs_uri = f"gs://{bucket}/{name}"
    table_ref = f"{bq_project}.{bq_dataset}.{bq_table}"

    client = bigquery.Client(project=bq_project)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    result = load_job.result()

    print(
        f"Loaded {result.output_rows} rows from {gcs_uri} into {table_ref}"
    )
