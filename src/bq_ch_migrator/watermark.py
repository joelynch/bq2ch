import json
from pathlib import Path

from google.cloud import bigquery


class WatermarkState:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> str | None:
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text())
        return data.get("last_watermark")

    def save(self, watermark_value: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps({"last_watermark": watermark_value}, indent=2) + "\n"
        )


def get_current_max_watermark(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    column: str,
) -> str | None:
    sql = (
        f"SELECT CAST(MAX(`{column}`) AS STRING) as max_val "
        f"FROM `{project}.{dataset}.{table}`"
    )
    result = bq_client.query(sql).result()
    for row in result:
        return row.max_val
    return None
