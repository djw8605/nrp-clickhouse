from __future__ import annotations

import logging
import time
from datetime import date
from typing import Callable, Iterable, Sequence

from .config import Settings, get_settings
from .models import (
    LlmTokenUsageRecord,
    NamespaceMetadataRecord,
    NamespaceUsageRecord,
    NodeInstitutionRecord,
    PodUsageRecord,
)
from .schema import (
    LLM_TOKEN_TABLE_NAME,
    NAMESPACE_METADATA_TABLE_NAME,
    NAMESPACE_TABLE_NAME,
    NODE_INSTITUTION_TABLE_NAME,
    POD_TABLE_NAME,
    create_database_sql,
    ensure_schema,
    table_qualified_name,
)


logger = logging.getLogger(__name__)


try:
    import clickhouse_connect
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    clickhouse_connect = None


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _retry_with_backoff(
    operation_name: str,
    retry_limit: int,
    func: Callable[[], object],
) -> object:
    for attempt in range(1, retry_limit + 1):
        start_time = time.monotonic()
        try:
            result = func()
            logger.info(
                "clickhouse_operation_complete",
                extra={
                    "operation": operation_name,
                    "attempt": attempt,
                    "duration_seconds": round(time.monotonic() - start_time, 3),
                },
            )
            return result
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "clickhouse_operation_failed",
                extra={
                    "operation": operation_name,
                    "attempt": attempt,
                    "duration_seconds": round(time.monotonic() - start_time, 3),
                    "error": str(exc),
                },
            )
            if attempt >= retry_limit:
                raise

            sleep_seconds = min(2 ** (attempt - 1), 30)
            logger.info(
                "clickhouse_operation_retrying",
                extra={
                    "operation": operation_name,
                    "attempt": attempt,
                    "retry_in_seconds": sleep_seconds,
                },
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Operation unexpectedly failed: {operation_name}")


def connect_clickhouse(settings: Settings | None = None):
    if clickhouse_connect is None:
        raise RuntimeError(
            "clickhouse-connect is not installed. Install dependencies before running ETL writes."
        )

    active_settings = settings or get_settings()

    bootstrap_client = clickhouse_connect.get_client(
        host=active_settings.CLICKHOUSE_HOST,
        port=active_settings.CLICKHOUSE_PORT,
        username=active_settings.CLICKHOUSE_USER,
        password=active_settings.CLICKHOUSE_PASSWORD,
        secure=active_settings.CLICKHOUSE_SECURE,
        database="default",
    )
    bootstrap_client.command(create_database_sql(active_settings.CLICKHOUSE_DATABASE))
    bootstrap_client.close()

    return clickhouse_connect.get_client(
        host=active_settings.CLICKHOUSE_HOST,
        port=active_settings.CLICKHOUSE_PORT,
        username=active_settings.CLICKHOUSE_USER,
        password=active_settings.CLICKHOUSE_PASSWORD,
        secure=active_settings.CLICKHOUSE_SECURE,
        database=active_settings.CLICKHOUSE_DATABASE,
    )


def create_tables_if_not_exist(client, settings: Settings | None = None) -> None:
    active_settings = settings or get_settings()

    _retry_with_backoff(
        "create_tables_if_not_exist",
        active_settings.RETRY_LIMIT,
        lambda: ensure_schema(client, active_settings.CLICKHOUSE_DATABASE),
    )


def _chunk_rows(rows: Sequence, batch_size: int) -> Iterable[Sequence]:
    for idx in range(0, len(rows), batch_size):
        yield rows[idx : idx + batch_size]


def _delete_for_date(client, table_name: str, target_date: date, settings: Settings) -> None:
    statement = (
        f"ALTER TABLE {table_qualified_name(settings.CLICKHOUSE_DATABASE, table_name)} "
        f"DELETE WHERE date = toDate('{target_date.isoformat()}') SETTINGS mutations_sync = 1"
    )
    _retry_with_backoff(
        f"delete_existing_partition_{table_name}",
        settings.RETRY_LIMIT,
        lambda: client.command(statement),
    )


def delete_existing_partitions(client, target_date: date, settings: Settings | None = None) -> None:
    active_settings = settings or get_settings()
    _delete_for_date(client, POD_TABLE_NAME, target_date, active_settings)
    _delete_for_date(client, NAMESPACE_TABLE_NAME, target_date, active_settings)
    _delete_for_date(client, LLM_TOKEN_TABLE_NAME, target_date, active_settings)


def insert_pod_usage(
    client,
    rows: Sequence[PodUsageRecord],
    settings: Settings | None = None,
) -> int:
    if not rows:
        logger.info("clickhouse_insert_pod_skipped_no_rows")
        return 0

    active_settings = settings or get_settings()
    batch_size = max(1, active_settings.CLICKHOUSE_WRITE_BATCH_SIZE)
    table_name = table_qualified_name(active_settings.CLICKHOUSE_DATABASE, POD_TABLE_NAME)

    inserted = 0
    for batch in _chunk_rows(rows, batch_size):
        payload = [row.to_clickhouse_tuple() for row in batch]

        def _insert_batch() -> None:
            client.insert(
                table_name,
                payload,
                column_names=[
                    "date",
                    "namespace",
                    "created_by",
                    "node",
                    "pod_hash",
                    "pod_name",
                    "resource",
                    "usage",
                    "unit",
                ],
            )

        _retry_with_backoff("insert_pod_usage_batch", active_settings.RETRY_LIMIT, _insert_batch)
        inserted += len(batch)

    logger.info("clickhouse_insert_pod_complete", extra={"row_count": inserted})
    return inserted


def insert_namespace_usage(
    client,
    rows: Sequence[NamespaceUsageRecord],
    settings: Settings | None = None,
) -> int:
    if not rows:
        logger.info("clickhouse_insert_namespace_skipped_no_rows")
        return 0

    active_settings = settings or get_settings()
    batch_size = max(1, active_settings.CLICKHOUSE_WRITE_BATCH_SIZE)
    table_name = table_qualified_name(active_settings.CLICKHOUSE_DATABASE, NAMESPACE_TABLE_NAME)

    inserted = 0
    for batch in _chunk_rows(rows, batch_size):
        payload = [row.to_clickhouse_tuple() for row in batch]

        def _insert_batch() -> None:
            client.insert(
                table_name,
                payload,
                column_names=[
                    "date",
                    "namespace",
                    "created_by",
                    "node",
                    "resource",
                    "usage",
                    "unit",
                ],
            )

        _retry_with_backoff(
            "insert_namespace_usage_batch", active_settings.RETRY_LIMIT, _insert_batch
        )
        inserted += len(batch)

    logger.info("clickhouse_insert_namespace_complete", extra={"row_count": inserted})
    return inserted


def insert_llm_token_usage(
    client,
    rows: Sequence[LlmTokenUsageRecord],
    settings: Settings | None = None,
) -> int:
    if not rows:
        logger.info("clickhouse_insert_llm_token_skipped_no_rows")
        return 0

    active_settings = settings or get_settings()
    batch_size = max(1, active_settings.CLICKHOUSE_WRITE_BATCH_SIZE)
    table_name = table_qualified_name(active_settings.CLICKHOUSE_DATABASE, LLM_TOKEN_TABLE_NAME)

    inserted = 0
    for batch in _chunk_rows(rows, batch_size):
        payload = [row.to_clickhouse_tuple() for row in batch]

        def _insert_batch() -> None:
            client.insert(
                table_name,
                payload,
                column_names=[
                    "date",
                    "namespace",
                    "token_alias",
                    "model",
                    "token_type",
                    "tokens_used",
                ],
            )

        _retry_with_backoff(
            "insert_llm_token_usage_batch", active_settings.RETRY_LIMIT, _insert_batch
        )
        inserted += len(batch)

    logger.info("clickhouse_insert_llm_token_complete", extra={"row_count": inserted})
    return inserted


def replace_node_institution_mapping(
    client,
    rows: Sequence[NodeInstitutionRecord],
    settings: Settings | None = None,
) -> int:
    active_settings = settings or get_settings()
    table_name = table_qualified_name(active_settings.CLICKHOUSE_DATABASE, NODE_INSTITUTION_TABLE_NAME)

    _retry_with_backoff(
        "truncate_node_institution_mapping",
        active_settings.RETRY_LIMIT,
        lambda: client.command(f"TRUNCATE TABLE {table_name}"),
    )

    if not rows:
        logger.info("clickhouse_insert_node_institution_skipped_no_rows")
        return 0

    batch_size = max(1, active_settings.CLICKHOUSE_WRITE_BATCH_SIZE)
    inserted = 0

    for batch in _chunk_rows(rows, batch_size):
        payload = [row.to_clickhouse_tuple() for row in batch]

        def _insert_batch() -> None:
            client.insert(
                table_name,
                payload,
                column_names=["node", "institution_name"],
            )

        _retry_with_backoff(
            "insert_node_institution_batch",
            active_settings.RETRY_LIMIT,
            _insert_batch,
        )
        inserted += len(batch)

    logger.info("clickhouse_insert_node_institution_complete", extra={"row_count": inserted})
    return inserted


def _fetch_existing_namespace_metadata(
    client,
    namespaces: Sequence[str],
    settings: Settings,
) -> dict[str, NamespaceMetadataRecord]:
    if not namespaces:
        return {}

    table_name = table_qualified_name(settings.CLICKHOUSE_DATABASE, NAMESPACE_METADATA_TABLE_NAME)
    namespace_list = ", ".join(_sql_string_literal(namespace) for namespace in sorted(set(namespaces)))
    query = (
        f"SELECT namespace, pi, institution, admins, user_institutions, updated_at "
        f"FROM {table_name} WHERE namespace IN ({namespace_list})"
    )

    result_rows = _retry_with_backoff(
        "fetch_namespace_metadata",
        settings.RETRY_LIMIT,
        lambda: client.query(query).result_rows,
    )
    return {
        row[0]: NamespaceMetadataRecord(
            namespace=row[0],
            pi=row[1],
            institution=row[2],
            admins=row[3],
            user_institutions=row[4],
            updated_at=row[5],
        )
        for row in result_rows
    }


def insert_namespace_metadata(
    client,
    rows: Sequence[NamespaceMetadataRecord],
    settings: Settings | None = None,
) -> int:
    if not rows:
        logger.info("clickhouse_insert_namespace_metadata_skipped_no_rows")
        return 0

    active_settings = settings or get_settings()
    batch_size = max(1, active_settings.CLICKHOUSE_WRITE_BATCH_SIZE)
    table_name = table_qualified_name(active_settings.CLICKHOUSE_DATABASE, NAMESPACE_METADATA_TABLE_NAME)

    inserted = 0
    for batch in _chunk_rows(rows, batch_size):
        payload = [row.to_clickhouse_tuple() for row in batch]

        def _insert_batch() -> None:
            client.insert(
                table_name,
                payload,
                column_names=[
                    "namespace",
                    "pi",
                    "institution",
                    "admins",
                    "user_institutions",
                    "updated_at",
                ],
            )

        _retry_with_backoff(
            "insert_namespace_metadata_batch",
            active_settings.RETRY_LIMIT,
            _insert_batch,
        )
        inserted += len(batch)

    logger.info("clickhouse_insert_namespace_metadata_complete", extra={"row_count": inserted})
    return inserted


def update_namespace_metadata(
    client,
    rows: Sequence[NamespaceMetadataRecord],
    settings: Settings | None = None,
) -> int:
    if not rows:
        logger.info("clickhouse_update_namespace_metadata_skipped_no_rows")
        return 0

    active_settings = settings or get_settings()
    table_name = table_qualified_name(active_settings.CLICKHOUSE_DATABASE, NAMESPACE_METADATA_TABLE_NAME)

    updated = 0
    for row in rows:
        timestamp_literal = _sql_string_literal(row.updated_at.isoformat().replace("+00:00", "Z"))
        statement = (
            f"ALTER TABLE {table_name} UPDATE "
            f"pi = {_sql_string_literal(row.pi)}, "
            f"institution = {_sql_string_literal(row.institution)}, "
            f"admins = {_sql_string_literal(row.admins)}, "
            f"user_institutions = {_sql_string_literal(row.user_institutions)}, "
            f"updated_at = parseDateTimeBestEffort({timestamp_literal}) "
            f"WHERE namespace = {_sql_string_literal(row.namespace)} "
            f"SETTINGS mutations_sync = 1"
        )

        _retry_with_backoff(
            "update_namespace_metadata_row",
            active_settings.RETRY_LIMIT,
            lambda statement=statement: client.command(statement),
        )
        updated += 1

    logger.info("clickhouse_update_namespace_metadata_complete", extra={"row_count": updated})
    return updated


def sync_namespace_metadata(
    client,
    rows: Sequence[NamespaceMetadataRecord],
    settings: Settings | None = None,
) -> dict[str, int]:
    active_settings = settings or get_settings()

    if not rows:
        logger.info("clickhouse_sync_namespace_metadata_skipped_no_rows")
        return {"inserted": 0, "updated": 0}

    existing_rows = _fetch_existing_namespace_metadata(
        client,
        [row.namespace for row in rows],
        active_settings,
    )

    rows_to_insert: list[NamespaceMetadataRecord] = []
    rows_to_update: list[NamespaceMetadataRecord] = []

    for row in rows:
        existing = existing_rows.get(row.namespace)
        if existing is None:
            rows_to_insert.append(row)
            continue

        if (
            existing.pi != row.pi
            or existing.institution != row.institution
            or existing.admins != row.admins
            or existing.user_institutions != row.user_institutions
        ):
            rows_to_update.append(row)

    inserted = insert_namespace_metadata(client, rows_to_insert, active_settings)
    updated = update_namespace_metadata(client, rows_to_update, active_settings)

    logger.info(
        "clickhouse_sync_namespace_metadata_complete",
        extra={
            "row_count": len(rows),
            "inserted_rows": inserted,
            "updated_rows": updated,
            "unchanged_rows": len(rows) - inserted - updated,
        },
    )
    return {"inserted": inserted, "updated": updated}


def _count_rows_for_date(client, table_name: str, target_date: date, settings: Settings) -> int:
    query = (
        f"SELECT count() FROM {table_qualified_name(settings.CLICKHOUSE_DATABASE, table_name)} "
        f"WHERE date = toDate('{target_date.isoformat()}')"
    )
    return int(
        _retry_with_backoff(
            f"count_rows_for_date_{table_name}",
            settings.RETRY_LIMIT,
            lambda: client.query(query).result_rows[0][0],
        )
    )


def has_data_for_date(client, target_date: date, settings: Settings | None = None) -> bool:
    active_settings = settings or get_settings()
    pod_count = _count_rows_for_date(client, POD_TABLE_NAME, target_date, active_settings)
    namespace_count = _count_rows_for_date(
        client, NAMESPACE_TABLE_NAME, target_date, active_settings
    )
    return pod_count > 0 and namespace_count > 0


def get_ingested_dates(
    client,
    start_date: date,
    end_date: date,
    settings: Settings | None = None,
) -> set[date]:
    active_settings = settings or get_settings()
    query = (
        f"SELECT DISTINCT date FROM {table_qualified_name(active_settings.CLICKHOUSE_DATABASE, NAMESPACE_TABLE_NAME)} "
        f"WHERE date >= toDate('{start_date.isoformat()}') "
        f"AND date <= toDate('{end_date.isoformat()}')"
    )

    rows = _retry_with_backoff(
        "get_ingested_dates",
        active_settings.RETRY_LIMIT,
        lambda: client.query(query).result_rows,
    )

    result: set[date] = set()
    for row in rows:
        value = row[0]
        if isinstance(value, date):
            result.add(value)
        else:
            result.add(date.fromisoformat(str(value)))
    return result
