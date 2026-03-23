from __future__ import annotations

import logging
import re
from collections.abc import Mapping


logger = logging.getLogger(__name__)


POD_TABLE_NAME = "cluster_pod_usage_daily"
NAMESPACE_TABLE_NAME = "cluster_namespace_usage_daily"
NODE_INSTITUTION_TABLE_NAME = "node_institution_mapping"
NAMESPACE_METADATA_TABLE_NAME = "namespace_metadata_mapping"

POD_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("date", "Date"),
    ("namespace", "LowCardinality(String)"),
    ("created_by", "LowCardinality(String)"),
    ("node", "LowCardinality(String)"),
    ("pod_hash", "UInt64"),
    ("pod_name", "String"),
    ("resource", "LowCardinality(String)"),
    ("usage", "Decimal64(6)"),
    ("unit", "LowCardinality(String)"),
]

NAMESPACE_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("date", "Date"),
    ("namespace", "LowCardinality(String)"),
    ("created_by", "LowCardinality(String)"),
    ("node", "LowCardinality(String)"),
    ("resource", "LowCardinality(String)"),
    ("usage", "Decimal64(6)"),
    ("unit", "LowCardinality(String)"),
]

NODE_INSTITUTION_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("node", "String"),
    ("institution_name", "LowCardinality(String)"),
]

NAMESPACE_METADATA_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("namespace", "String"),
    ("pi", "LowCardinality(String)"),
    ("institution", "LowCardinality(String)"),
    ("admins", "String"),
    ("user_institutions", "String"),
    ("updated_at", "DateTime"),
]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def table_qualified_name(database: str, table_name: str) -> str:
    return f"{_safe_identifier(database)}.{_safe_identifier(table_name)}"


def create_database_sql(database: str) -> str:
    return f"CREATE DATABASE IF NOT EXISTS {_safe_identifier(database)}"


def create_pod_table_sql(database: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, POD_TABLE_NAME)}
(
    date Date,
    namespace LowCardinality(String),
    created_by LowCardinality(String),
    node LowCardinality(String),
    pod_hash UInt64,
    pod_name String,
    resource LowCardinality(String),
    usage Decimal64(6),
    unit LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, node, resource, pod_hash)
""".strip()


def create_namespace_table_sql(database: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, NAMESPACE_TABLE_NAME)}
(
    date Date,
    namespace LowCardinality(String),
    created_by LowCardinality(String),
    node LowCardinality(String),
    resource LowCardinality(String),
    usage Decimal64(6),
    unit LowCardinality(String)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, node, resource)
""".strip()


def create_node_institution_table_sql(database: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, NODE_INSTITUTION_TABLE_NAME)}
(
    node String,
    institution_name LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY node
""".strip()


def create_namespace_metadata_table_sql(database: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, NAMESPACE_METADATA_TABLE_NAME)}
(
    namespace String,
    pi LowCardinality(String),
    institution LowCardinality(String),
    admins String,
    user_institutions String,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY namespace
""".strip()


def _fetch_existing_columns(client, database: str, table_name: str) -> Mapping[str, str]:
    describe = client.query(f"DESCRIBE TABLE {table_qualified_name(database, table_name)}")
    return {row[0]: row[1] for row in describe.result_rows}


def _types_equivalent(actual: str, expected: str) -> bool:
    normalized_actual = actual.replace(" ", "").lower()
    normalized_expected = expected.replace(" ", "").lower()
    if normalized_actual == normalized_expected:
        return True

    decimal_aliases = {"decimal64(6)", "decimal(18,6)"}
    return normalized_actual in decimal_aliases and normalized_expected in decimal_aliases


def _apply_table_migrations(
    client,
    database: str,
    table_name: str,
    expected_columns: list[tuple[str, str]],
) -> None:
    existing_columns = _fetch_existing_columns(client, database, table_name)

    for column_name, column_type in expected_columns:
        existing_type = existing_columns.get(column_name)
        full_table = table_qualified_name(database, table_name)

        if existing_type is None:
            statement = (
                f"ALTER TABLE {full_table} "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
            )
            logger.info(
                "schema_add_missing_column",
                extra={
                    "table": table_name,
                    "column": column_name,
                    "column_type": column_type,
                },
            )
            client.command(statement)
        elif not _types_equivalent(existing_type, column_type):
            logger.warning(
                "schema_type_mismatch",
                extra={
                    "table": table_name,
                    "column": column_name,
                    "expected": column_type,
                    "actual": existing_type,
                },
            )


def ensure_schema(client, database: str) -> None:
    client.command(create_database_sql(database))
    client.command(create_pod_table_sql(database))
    client.command(create_namespace_table_sql(database))
    client.command(create_node_institution_table_sql(database))
    client.command(create_namespace_metadata_table_sql(database))

    _apply_table_migrations(client, database, POD_TABLE_NAME, POD_EXPECTED_COLUMNS)
    _apply_table_migrations(client, database, NAMESPACE_TABLE_NAME, NAMESPACE_EXPECTED_COLUMNS)
    _apply_table_migrations(
        client,
        database,
        NODE_INSTITUTION_TABLE_NAME,
        NODE_INSTITUTION_EXPECTED_COLUMNS,
    )
    _apply_table_migrations(
        client,
        database,
        NAMESPACE_METADATA_TABLE_NAME,
        NAMESPACE_METADATA_EXPECTED_COLUMNS,
    )
