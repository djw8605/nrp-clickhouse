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
    ("commercial", "Bool"),
]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CLUSTER_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Replication coordinates for the Altinity operator. The {uuid}, {shard} and
# {replica} macros are injected per-host by the operator (see system.macros), so
# every replica derives the same ZooKeeper/Keeper path for a given table.
REPLICATED_PATH = "/clickhouse/tables/{uuid}/{shard}"
REPLICATED_NAME = "{replica}"


def _safe_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def _on_cluster_clause(cluster: str) -> str:
    """Return ' ON CLUSTER `<cluster>`' when replication is enabled, else ''."""
    cluster = (cluster or "").strip()
    if not cluster:
        return ""
    if not _CLUSTER_RE.match(cluster):
        raise ValueError(f"Unsafe cluster name: {cluster!r}")
    return f" ON CLUSTER `{cluster}`"


def _merge_tree_engine(base_engine: str, cluster: str) -> str:
    """Map a *MergeTree engine to its Replicated* variant when a cluster is set.

    With no cluster (single-node / dev) the plain engine is returned so the
    schema still works without ClickHouse Keeper.
    """
    if (cluster or "").strip():
        return f"Replicated{base_engine}('{REPLICATED_PATH}', '{REPLICATED_NAME}')"
    return base_engine


def table_qualified_name(database: str, table_name: str) -> str:
    return f"{_safe_identifier(database)}.{_safe_identifier(table_name)}"


def create_database_sql(database: str, cluster: str = "") -> str:
    return (
        f"CREATE DATABASE IF NOT EXISTS {_safe_identifier(database)}"
        f"{_on_cluster_clause(cluster)}"
    )


def create_pod_table_sql(database: str, cluster: str = "") -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, POD_TABLE_NAME)}{_on_cluster_clause(cluster)}
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
ENGINE = {_merge_tree_engine("MergeTree", cluster)}
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, node, resource, raw_resource, gpu_model_name, pod_hash, pod_uid)
""".strip()


def _create_namespace_table_sql(database: str, table_name: str, cluster: str = "") -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, table_name)}{_on_cluster_clause(cluster)}
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
ENGINE = {_merge_tree_engine("SummingMergeTree", cluster)}
PARTITION BY toYYYYMM(date)
ORDER BY ({NAMESPACE_SORTING_KEY})
""".strip()


def create_namespace_table_sql(database: str, cluster: str = "") -> str:
    return _create_namespace_table_sql(database, NAMESPACE_TABLE_NAME, cluster)


def create_llm_token_table_sql(database: str, cluster: str = "") -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, LLM_TOKEN_TABLE_NAME)}{_on_cluster_clause(cluster)}
(
    date Date,
    namespace LowCardinality(String),
    token_alias LowCardinality(String),
    model LowCardinality(String),
    token_type LowCardinality(String),
    tokens_used Decimal64(6)
)
ENGINE = {_merge_tree_engine("MergeTree", cluster)}
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, token_alias, model, token_type)
""".strip()


def create_node_institution_table_sql(database: str, cluster: str = "") -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, NODE_INSTITUTION_TABLE_NAME)}{_on_cluster_clause(cluster)}
(
    node String,
    institution_name LowCardinality(String)
)
ENGINE = {_merge_tree_engine("MergeTree", cluster)}
ORDER BY node
""".strip()


def create_namespace_metadata_table_sql(database: str, cluster: str = "") -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_qualified_name(database, NAMESPACE_METADATA_TABLE_NAME)}{_on_cluster_clause(cluster)}
(
    namespace String,
    pi LowCardinality(String),
    institution LowCardinality(String),
    admins String,
    user_institutions String,
    updated_at DateTime,
    commercial Bool DEFAULT false
)
ENGINE = {_merge_tree_engine("MergeTree", cluster)}
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


def _rebuild_namespace_table_if_sort_key_changed(
    client, database: str, cluster: str = ""
) -> None:
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
    client.command(_create_namespace_table_sql(database, replacement_table, cluster))
    # INSERT runs once; on a Replicated* engine the rows propagate to every replica
    # via the replication log, so we deliberately do not fan this out ON CLUSTER.
    client.command(
        f"INSERT INTO {table_qualified_name(database, replacement_table)} ({columns_sql}) "
        f"SELECT {select_sql} FROM {table_qualified_name(database, NAMESPACE_TABLE_NAME)}"
    )
    client.command(
        f"RENAME TABLE {table_qualified_name(database, NAMESPACE_TABLE_NAME)} "
        f"TO {table_qualified_name(database, backup_table)}, "
        f"{table_qualified_name(database, replacement_table)} "
        f"TO {table_qualified_name(database, NAMESPACE_TABLE_NAME)}"
        f"{_on_cluster_clause(cluster)}"
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
    cluster: str = "",
) -> None:
    existing_columns = _fetch_existing_columns(client, database, table_name)

    for column_name, column_type in expected_columns:
        existing_type = existing_columns.get(column_name)
        full_table = table_qualified_name(database, table_name)

        if existing_type is None:
            statement = (
                f"ALTER TABLE {full_table}{_on_cluster_clause(cluster)} "
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


def ensure_schema(client, database: str, cluster: str = "") -> None:
    client.command(create_database_sql(database, cluster))
    client.command(create_pod_table_sql(database, cluster))
    client.command(create_namespace_table_sql(database, cluster))
    client.command(create_llm_token_table_sql(database, cluster))
    client.command(create_node_institution_table_sql(database, cluster))
    client.command(create_namespace_metadata_table_sql(database, cluster))

    _apply_table_migrations(client, database, POD_TABLE_NAME, POD_EXPECTED_COLUMNS, cluster)
    _apply_table_migrations(
        client, database, NAMESPACE_TABLE_NAME, NAMESPACE_EXPECTED_COLUMNS, cluster
    )
    _rebuild_namespace_table_if_sort_key_changed(client, database, cluster)
    _apply_table_migrations(
        client, database, LLM_TOKEN_TABLE_NAME, LLM_TOKEN_EXPECTED_COLUMNS, cluster
    )
    _apply_table_migrations(
        client,
        database,
        NODE_INSTITUTION_TABLE_NAME,
        NODE_INSTITUTION_EXPECTED_COLUMNS,
        cluster,
    )
    _apply_table_migrations(
        client,
        database,
        NAMESPACE_METADATA_TABLE_NAME,
        NAMESPACE_METADATA_EXPECTED_COLUMNS,
        cluster,
    )
