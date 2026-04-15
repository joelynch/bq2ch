from google.cloud import bigquery


BQ_TO_CH_TYPE_MAP: dict[str, str] = {
    "INT64": "Int64",
    "INTEGER": "Int64",
    "FLOAT": "Float64",
    "FLOAT64": "Float64",
    "NUMERIC": "Decimal(38, 9)",
    "BIGNUMERIC": "Decimal(76, 38)",
    "BOOL": "Bool",
    "BOOLEAN": "Bool",
    "STRING": "String",
    "BYTES": "String",
    "DATE": "Date",
    "DATETIME": "DateTime64(6)",
    "TIME": "String",
    "TIMESTAMP": "DateTime64(6, 'UTC')",
    "GEOGRAPHY": "String",
    "JSON": "String",
    "RANGE": "String",
}


def bq_type_to_ch_type(field: bigquery.SchemaField) -> str:
    if field.field_type == "RECORD":
        inner = ", ".join(
            f"{sub.name} {_wrap_nullable(sub, bq_type_to_ch_type(sub))}"
            for sub in field.fields
        )
        return f"Tuple({inner})"

    ch_type = BQ_TO_CH_TYPE_MAP.get(field.field_type, "String")

    if field.precision is not None and field.field_type in ("NUMERIC", "BIGNUMERIC"):
        scale = field.scale if field.scale is not None else 0
        ch_type = f"Decimal({field.precision}, {scale})"

    return ch_type


def _wrap_nullable(field: bigquery.SchemaField, ch_type: str) -> str:
    if field.mode == "REPEATED":
        return f"Array({ch_type})"
    if field.mode == "NULLABLE":
        return f"Nullable({ch_type})"
    return ch_type


def introspect_bq_schema(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
) -> list[bigquery.SchemaField]:
    table_ref = f"{project}.{dataset}.{table}"
    bq_table = bq_client.get_table(table_ref)
    return list(bq_table.schema)


def generate_ch_columns(fields: list[bigquery.SchemaField]) -> str:
    lines = []
    for field in fields:
        ch_type = bq_type_to_ch_type(field)
        ch_col_type = _wrap_nullable(field, ch_type)
        lines.append(f"    `{field.name}` {ch_col_type}")
    return ",\n".join(lines)


def generate_create_table_sql(
    database: str,
    table: str,
    cluster: str,
    fields: list[bigquery.SchemaField],
    order_by: str | None = None,
    partition_by: str | None = None,
) -> str:
    columns = generate_ch_columns(fields)
    order_clause = order_by if order_by else "tuple()"
    parts = [
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ON CLUSTER `{cluster}`",
        f"(\n{columns}\n)",
        "ENGINE = ReplicatedMergeTree",
    ]
    if partition_by:
        parts.append(f"PARTITION BY {partition_by}")
    parts.append(f"ORDER BY {order_clause}")
    return "\n".join(parts)
