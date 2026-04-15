from __future__ import annotations

import re

from clickhouse_connect.driver import Client
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
    "INTERVAL": "String",
    "RANGE": "String",
}

# ── Reverse mapping: ClickHouse → BigQuery ──────────────────────────────────

CH_TO_BQ_TYPE_MAP: dict[str, str] = {
    "Int8": "INT64",
    "Int16": "INT64",
    "Int32": "INT64",
    "Int64": "INT64",
    "Int128": "BIGNUMERIC",
    "Int256": "BIGNUMERIC",
    "UInt8": "INT64",
    "UInt16": "INT64",
    "UInt32": "INT64",
    "UInt64": "INT64",
    "UInt128": "BIGNUMERIC",
    "UInt256": "BIGNUMERIC",
    "Float32": "FLOAT64",
    "Float64": "FLOAT64",
    "Bool": "BOOL",
    "String": "STRING",
    "UUID": "STRING",
    "IPv4": "STRING",
    "IPv6": "STRING",
    "Date": "DATE",
    "Date32": "DATE",
    "DateTime": "DATETIME",
    "Nothing": "STRING",
    "JSON": "STRING",
    "Object('json')": "STRING",
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


# ── ClickHouse type string parser ───────────────────────────────────────────


def _find_matching_paren(s: str, start: int) -> int:
    """Find the index of the closing paren matching the opening one at *start*."""
    depth = 1
    i = start + 1
    while i < len(s) and depth > 0:
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
        i += 1
    return i - 1


def _split_top_level(s: str) -> list[str]:
    """Split a string by commas that are not nested inside parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in s:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_ch_type(type_str: str) -> bigquery.SchemaField:
    """Parse a ClickHouse type string and return a dummy SchemaField with correct BQ type.

    This is an internal helper. Use ch_type_to_bq_field() for the public API.
    """
    return ch_type_to_bq_field("_", type_str)


def ch_type_to_bq_field(name: str, type_str: str) -> bigquery.SchemaField:
    """Convert a ClickHouse column name + type string into a BigQuery SchemaField."""
    type_str = type_str.strip()

    # LowCardinality(T) – unwrap
    if type_str.startswith("LowCardinality("):
        inner = type_str[len("LowCardinality(") : -1]
        return ch_type_to_bq_field(name, inner)

    # Nullable(T)
    if type_str.startswith("Nullable("):
        inner = type_str[len("Nullable(") : -1]
        field = ch_type_to_bq_field(name, inner)
        return bigquery.SchemaField(
            name=name,
            field_type=field.field_type,
            mode="NULLABLE",
            fields=field.fields,
        )

    # Array(T)
    if type_str.startswith("Array("):
        inner = type_str[len("Array(") : -1]
        field = ch_type_to_bq_field(name, inner)
        return bigquery.SchemaField(
            name=name,
            field_type=field.field_type,
            mode="REPEATED",
            fields=field.fields,
        )

    # Map(K, V) → REPEATED RECORD with key/value subfields
    if type_str.startswith("Map("):
        inner = type_str[len("Map(") : -1]
        parts = _split_top_level(inner)
        if len(parts) != 2:
            return bigquery.SchemaField(name=name, field_type="STRING", mode="NULLABLE")
        key_field = ch_type_to_bq_field("key", parts[0])
        value_field = ch_type_to_bq_field("value", parts[1])
        return bigquery.SchemaField(
            name=name,
            field_type="RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField(
                    name="key",
                    field_type=key_field.field_type,
                    mode="REQUIRED",
                    fields=key_field.fields,
                ),
                bigquery.SchemaField(
                    name="value",
                    field_type=value_field.field_type,
                    mode="NULLABLE",
                    fields=value_field.fields,
                ),
            ],
        )

    # Tuple(name1 Type1, name2 Type2, ...) → RECORD
    if type_str.startswith("Tuple("):
        inner = type_str[len("Tuple(") : -1]
        parts = _split_top_level(inner)
        sub_fields: list[bigquery.SchemaField] = []
        for i, part in enumerate(parts):
            # Named tuple element: "name Type"
            tokens = part.strip().split(None, 1)
            if len(tokens) == 2 and not tokens[0][0].isupper():
                sub_name, sub_type = tokens
            else:
                # Unnamed tuple element
                sub_name = f"_{i + 1}"
                sub_type = part.strip()
            sub_fields.append(ch_type_to_bq_field(sub_name, sub_type))
        return bigquery.SchemaField(
            name=name,
            field_type="RECORD",
            mode="NULLABLE",
            fields=sub_fields,
        )

    # Enum8('v1'=1, ...) / Enum16(...) → STRING
    if type_str.startswith("Enum8(") or type_str.startswith("Enum16("):
        return bigquery.SchemaField(name=name, field_type="STRING", mode="NULLABLE")

    # FixedString(N) → STRING
    if type_str.startswith("FixedString("):
        return bigquery.SchemaField(name=name, field_type="STRING", mode="NULLABLE")

    # Decimal(P, S) / Decimal32/64/128/256
    m = re.match(r"Decimal(?:32|64|128|256)?\((\d+),\s*(\d+)\)", type_str)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        if precision <= 38 and scale <= 9:
            bq_type = "NUMERIC"
        else:
            bq_type = "BIGNUMERIC"
        return bigquery.SchemaField(name=name, field_type=bq_type, mode="NULLABLE")
    if re.match(r"Decimal\d+$", type_str):
        return bigquery.SchemaField(name=name, field_type="NUMERIC", mode="NULLABLE")

    # DateTime64(p, 'timezone') → TIMESTAMP
    m = re.match(r"DateTime64\(\d+,\s*'[^']+'\)", type_str)
    if m:
        return bigquery.SchemaField(name=name, field_type="TIMESTAMP", mode="NULLABLE")

    # DateTime64(p) → DATETIME (no timezone)
    m = re.match(r"DateTime64\(\d+\)", type_str)
    if m:
        return bigquery.SchemaField(name=name, field_type="DATETIME", mode="NULLABLE")

    # Simple types from the map
    bq_type = CH_TO_BQ_TYPE_MAP.get(type_str, "STRING")
    return bigquery.SchemaField(name=name, field_type=bq_type, mode="NULLABLE")


# ── ClickHouse schema introspection ─────────────────────────────────────────


def introspect_ch_schema(
    ch_client: Client,
    database: str,
    table: str,
) -> list[tuple[str, str]]:
    """Return a list of (column_name, ch_type_string) from ClickHouse system.columns."""
    result = ch_client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        parameters={"db": database, "tbl": table},
    )
    return [(row[0], row[1]) for row in result.result_rows]


def generate_bq_schema(
    ch_columns: list[tuple[str, str]],
) -> list[bigquery.SchemaField]:
    """Convert ClickHouse columns to BigQuery SchemaField list."""
    return [ch_type_to_bq_field(name, ch_type) for name, ch_type in ch_columns]


def create_bq_table(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    schema_fields: list[bigquery.SchemaField],
    partition_field: str | None = None,
) -> None:
    """Create a BigQuery table if it does not already exist."""
    table_ref = f"{project}.{dataset}.{table}"
    bq_table = bigquery.Table(table_ref, schema=schema_fields)
    if partition_field:
        bq_table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
    bq_client.create_table(bq_table, exists_ok=True)
