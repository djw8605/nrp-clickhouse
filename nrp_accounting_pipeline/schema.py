from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


POD_TABLE_NAME = "cluster_pod_usage_daily"
NAMESPACE_TABLE_NAME = "cluster_namespace_usage_daily"
LLM_TOKEN_TABLE_NAME = "llm_token_usage_daily"
NODE_INSTITUTION_TABLE_NAME = "node_institution_mapping"
NAMESPACE_METADATA_TABLE_NAME = "namespace_metadata_mapping"
NAMESPACE_SORTING_KEY = (
    "date, namespace, created_by, node, resource, raw_resource, gpu_model_name, unit"
)

POD_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("date", "Date"),
    ("namespace", "LowCardinality(String)"),
    ("created_by", "LowCardinality(String)"),
    ("node", "LowCardinality(String)"),
    ("pod_hash", "UInt64"),
    ("pod_uid", "String"),
    ("pod_name", "String"),
    ("resource", "LowCardinality(String)"),
    ("raw_resource", "LowCardinality(String)"),
    ("gpu_model_name", "LowCardinality(String)"),
    ("usage", "Decimal64(6)"),
    ("unit", "LowCardinality(String)"),
]

NAMESPACE_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("date", "Date"),
    ("namespace", "LowCardinality(String)"),
    ("created_by", "LowCardinality(String)"),
    ("node", "LowCardinality(String)"),
    ("resource", "LowCardinality(String)"),
    ("raw_resource", "LowCardinality(String)"),
    ("gpu_model_name", "LowCardinality(String)"),
    ("usage", "Decimal64(6)"),
    ("unit", "LowCardinality(String)"),
]

LLM_TOKEN_EXPECTED_COLUMNS: list[tuple[str, str]] = [
    ("date", "Date"),
    ("namespace", "LowCardinality(String)"),
    ("token_alias", "LowCardinality(String)"),
    ("model", "LowCardinality(String)"),
    ("token_type", "LowCardinality(String)"),
    ("tokens_used", "Decimal64(6)"),
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
    pod_uid String,
    pod_name String,
    resource LowCardinality(String),
    raw_resource LowCardinality(String),
    gpu_model_name LowCardinality(String),
    usage Decimal64(6),
    unit LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, node, resource, raw_resource, gpu_model_name, pod_hash, pod_uid)
""".strip()


def _create_namespace_table_sql(database: str, table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, table_name)}
(
    date Date,
    namespace LowCardinality(String),
    created_by LowCardinality(String),
    node LowCardinality(String),
    resource LowCardinality(String),
    raw_resource LowCardinality(String),
    gpu_model_name LowCardinality(String),
    usage Decimal64(6),
    unit LowCardinality(String)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY ({NAMESPACE_SORTING_KEY})
""".strip()


def create_namespace_table_sql(database: str) -> str:
    return _create_namespace_table_sql(database, NAMESPACE_TABLE_NAME)


def create_llm_token_table_sql(database: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, LLM_TOKEN_TABLE_NAME)}
(
    date Date,
    namespace LowCardinality(String),
    token_alias LowCardinality(String),
    model LowCardinality(String),
    token_type LowCardinality(String),
    tokens_used Decimal64(6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, token_alias, model, token_type)
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


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _normalize_sorting_key(value: str) -> str:
    normalized = re.sub(r"[`\s()]+", "", value or "").lower()
    return normalized


def _fetch_sorting_key(client, database: str, table_name: str) -> str:
    result = client.query(
        "SELECT sorting_key FROM system.tables "
        f"WHERE database = {_sql_string_literal(database)} "
        f"AND name = {_sql_string_literal(table_name)}"
    )
    if not result.result_rows:
        return ""
    return str(result.result_rows[0][0] or "")


def _namespace_rebuild_select_expression(column_name: str) -> str:
    if column_name == "raw_resource":
        return "if(empty(raw_resource), resource, raw_resource) AS raw_resource"
    if column_name == "gpu_model_name":
        return (
            "if(empty(gpu_model_name), "
            "if(resource = 'gpu', 'unknown', 'not_applicable'), "
            "gpu_model_name) AS gpu_model_name"
        )
    return column_name


def _rebuild_namespace_table_if_sort_key_changed(client, database: str) -> None:
    actual_sorting_key = _fetch_sorting_key(client, database, NAMESPACE_TABLE_NAME)
    if not actual_sorting_key:
        return

    if _normalize_sorting_key(actual_sorting_key) == _normalize_sorting_key(NAMESPACE_SORTING_KEY):
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    replacement_table = f"{NAMESPACE_TABLE_NAME}__gpu_model_rebuild_{timestamp}"
    backup_table = f"{NAMESPACE_TABLE_NAME}__pre_gpu_model_{timestamp}"
    column_names = [column_name for column_name, _ in NAMESPACE_EXPECTED_COLUMNS]
    columns_sql = ", ".join(column_names)
    select_sql = ", ".join(_namespace_rebuild_select_expression(column) for column in column_names)

    logger.warning(
        "schema_rebuild_namespace_sorting_key",
        extra={
            "table": NAMESPACE_TABLE_NAME,
            "actual_sorting_key": actual_sorting_key,
            "expected_sorting_key": NAMESPACE_SORTING_KEY,
            "backup_table": backup_table,
        },
    )
    client.command(_create_namespace_table_sql(database, replacement_table))
    client.command(
        f"INSERT INTO {table_qualified_name(database, replacement_table)} ({columns_sql}) "
        f"SELECT {select_sql} FROM {table_qualified_name(database, NAMESPACE_TABLE_NAME)}"
    )
    client.command(
        f"RENAME TABLE {table_qualified_name(database, NAMESPACE_TABLE_NAME)} "
        f"TO {table_qualified_name(database, backup_table)}, "
        f"{table_qualified_name(database, replacement_table)} "
        f"TO {table_qualified_name(database, NAMESPACE_TABLE_NAME)}"
    )


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
    client.command(create_llm_token_table_sql(database))
    client.command(create_node_institution_table_sql(database))
    client.command(create_namespace_metadata_table_sql(database))

    _apply_table_migrations(client, database, POD_TABLE_NAME, POD_EXPECTED_COLUMNS)
    _apply_table_migrations(client, database, NAMESPACE_TABLE_NAME, NAMESPACE_EXPECTED_COLUMNS)
    _rebuild_namespace_table_if_sort_key_changed(client, database)
    _apply_table_migrations(client, database, LLM_TOKEN_TABLE_NAME, LLM_TOKEN_EXPECTED_COLUMNS)
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
