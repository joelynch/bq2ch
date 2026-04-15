# BigQuery ↔ ClickHouse Schema Mapping

This table documents how BigQuery types are mapped to ClickHouse types in the migrator tool.

## Type Mapping: BigQuery → ClickHouse

| BigQuery Type          | ClickHouse Type                          | Notes                                        |
|------------------------|------------------------------------------|----------------------------------------------|
| `STRING`               | `String`                                 |                                              |
| `BYTES`                | `String`                                 |                                              |
| `INT64` / `INTEGER`    | `Int64`                                  |                                              |
| `FLOAT64` / `FLOAT`    | `Float64`                                |                                              |
| `NUMERIC`              | `Decimal(38, 9)`                         | BigQuery default precision/scale             |
| `BIGNUMERIC`           | `Decimal(76, 38)`                        | BigQuery max precision/scale                 |
| `BOOLEAN` / `BOOL`     | `UInt8`                                  | ClickHouse convention: 0/1                   |
| `DATE`                 | `Date`                                   |                                              |
| `DATETIME`             | `DateTime`                               | No timezone in BQ                            |
| `TIMESTAMP`            | `DateTime64(6)`                          | Microsecond precision                        |
| `TIME`                 | `String`                                 | No native TIME in ClickHouse                 |
| `GEOGRAPHY`            | `String`                                 | WKT or GeoJSON as string                     |
| `JSON`                 | `String`                                 | Store as string, query with JSON functions    |
| `RECORD` / `STRUCT`    | `Tuple(field1 Type1, ...)`               | Recursive mapping of sub-fields              |
| `ARRAY`                | `Array(T)`                               | Element type recursively mapped              |

## Nullable Handling

- BigQuery fields with `mode = NULLABLE` → wrapped in `Nullable(T)` in ClickHouse.
- BigQuery fields with `mode = REQUIRED` → bare type.
- BigQuery fields with `mode = REPEATED` → `Array(T)` (not nullable).

## Struct (RECORD) Mapping

BigQuery `STRUCT` (mode=`RECORD`) maps to ClickHouse `Tuple` with named fields:

```
BigQuery: STRUCT<name STRING, age INT64>
ClickHouse: Tuple(name String, age Int64)
```

Nested structs are recursively mapped.

## Implementation

The mapping is implemented in [src/bq_ch_migrator/schema.py](../src/bq_ch_migrator/schema.py) with:

- `BQ_TO_CH_TYPE_MAP` — Dictionary of simple type mappings
- `bq_type_to_ch_type()` — Recursive function handling RECORD/STRUCT, ARRAY, and nullable wrapping
- `introspect_bq_schema()` — Fetches schema from BigQuery table
- `generate_ch_columns()` — Converts BQ SchemaField list to ClickHouse column definitions
- `generate_create_table_sql()` — Generates full CREATE TABLE DDL

## References

- [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
- [ClickHouse data types](https://clickhouse.com/docs/en/sql-reference/data-types)
