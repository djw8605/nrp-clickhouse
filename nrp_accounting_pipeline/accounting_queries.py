from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any, Sequence

from .config import Settings, get_settings
from .schema import (
    NAMESPACE_METADATA_TABLE_NAME,
    NAMESPACE_TABLE_NAME,
    NODE_INSTITUTION_TABLE_NAME,
    POD_TABLE_NAME,
    table_qualified_name,
)


DEFAULT_RESULT_LIMIT = 500
MAX_RESULT_LIMIT = 5000

DEFAULT_GROUP_BY: dict[str, list[str]] = {
    "namespace": ["date", "namespace", "institution", "node", "resource", "unit"],
    "pod": ["date", "namespace", "institution", "node", "pod_name", "resource", "unit"],
}

_TABLE_BY_GRANULARITY = {
    "namespace": NAMESPACE_TABLE_NAME,
    "pod": POD_TABLE_NAME,
}

_GROUPABLE_EXPRESSIONS: dict[str, str] = {
    "date": "usage.date",
    "namespace": "usage.namespace",
    "created_by": "usage.created_by",
    "node": "usage.node",
    "resource": "usage.resource",
    "unit": "usage.unit",
    "institution": "coalesce(meta.institution, 'Unknown')",
    "pi": "coalesce(meta.pi, 'Unknown')",
    "node_institution": "coalesce(node_map.institution_name, 'Unknown')",
    "pod_name": "usage.pod_name",
}


@dataclass(frozen=True)
class AccountingQuerySpec:
    sql: str
    granularity: str
    start_date: date
    end_date: date
    group_by: list[str]
    limit: int


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _normalize_string_list(value: str | Sequence[str] | None) -> list[str]:
    if value is None:
        return []

    raw_values = [value] if isinstance(value, str) else list(value)
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_value in raw_values:
        item = str(raw_value).strip()
        if not item or item in seen:
            continue
        normalized.append(item)
        seen.add(item)

    return normalized


def _coerce_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _latest_ingested_date(
    client,
    *,
    granularity: str,
    settings: Settings,
) -> date:
    table_name = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        _TABLE_BY_GRANULARITY[granularity],
    )
    result = client.query(f"SELECT max(date) FROM {table_name}")
    latest_value = result.result_rows[0][0] if result.result_rows else None
    if latest_value is None:
        raise RuntimeError("No accounting data is available in ClickHouse yet.")
    return _coerce_date(latest_value)  # type: ignore[return-value]


def _resolve_date_window(
    client,
    *,
    granularity: str,
    settings: Settings,
    start_date: date | datetime | str | None,
    end_date: date | datetime | str | None,
) -> tuple[date, date]:
    parsed_start = _coerce_date(start_date)
    parsed_end = _coerce_date(end_date)

    if parsed_start is None and parsed_end is None:
        latest_date = _latest_ingested_date(
            client,
            granularity=granularity,
            settings=settings,
        )
        return latest_date, latest_date

    if parsed_start is None:
        parsed_start = parsed_end
    if parsed_end is None:
        parsed_end = parsed_start

    assert parsed_start is not None
    assert parsed_end is not None

    if parsed_start > parsed_end:
        raise ValueError("start_date must be on or before end_date")

    return parsed_start, parsed_end


def _validate_group_by(granularity: str, group_by: Sequence[str] | None) -> list[str]:
    requested = list(group_by) if group_by else DEFAULT_GROUP_BY[granularity]
    validated: list[str] = []
    seen: set[str] = set()

    for column in requested:
        if column not in _GROUPABLE_EXPRESSIONS:
            allowed = ", ".join(sorted(_GROUPABLE_EXPRESSIONS))
            raise ValueError(f"Unsupported group_by field {column!r}. Allowed values: {allowed}")
        if granularity != "pod" and column == "pod_name":
            raise ValueError("pod_name can only be used when granularity='pod'")
        if column in seen:
            continue
        validated.append(column)
        seen.add(column)

    if not validated:
        raise ValueError("group_by must contain at least one dimension")

    return validated


def _validate_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_RESULT_LIMIT
    if limit < 1:
        raise ValueError("limit must be at least 1")
    return min(limit, MAX_RESULT_LIMIT)


def _validate_resource_safety(group_by: Sequence[str], resource_filters: Sequence[str]) -> None:
    if "resource" in group_by:
        return
    if len(resource_filters) == 1:
        return
    raise ValueError(
        "group_by must include 'resource' unless exactly one resource filter is provided"
    )


def _maybe_add_in_filter(
    clauses: list[str],
    *,
    expression: str,
    values: Sequence[str],
) -> None:
    if not values:
        return
    literal_values = ", ".join(_sql_string_literal(value) for value in values)
    clauses.append(f"{expression} IN ({literal_values})")


def _jsonify_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def build_resource_usage_query(
    client,
    *,
    granularity: str = "namespace",
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    namespace: str | Sequence[str] | None = None,
    institution: str | Sequence[str] | None = None,
    node: str | Sequence[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | Sequence[str] | None = None,
    resource: str | Sequence[str] | None = None,
    group_by: Sequence[str] | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> AccountingQuerySpec:
    if granularity not in _TABLE_BY_GRANULARITY:
        allowed = ", ".join(sorted(_TABLE_BY_GRANULARITY))
        raise ValueError(f"Unsupported granularity {granularity!r}. Allowed values: {allowed}")

    active_settings = settings or get_settings()
    start_day, end_day = _resolve_date_window(
        client,
        granularity=granularity,
        settings=active_settings,
        start_date=start_date,
        end_date=end_date,
    )
    normalized_group_by = _validate_group_by(granularity, group_by)
    safe_limit = _validate_limit(limit)

    namespace_filters = _normalize_string_list(namespace)
    institution_filters = _normalize_string_list(institution)
    node_filters = _normalize_string_list(node)
    node_institution_filters = _normalize_string_list(node_institution)
    resource_filters = _normalize_string_list(resource)

    _validate_resource_safety(normalized_group_by, resource_filters)

    if node_regex:
        re.compile(node_regex)

    usage_table = table_qualified_name(
        active_settings.CLICKHOUSE_DATABASE,
        _TABLE_BY_GRANULARITY[granularity],
    )
    metadata_table = table_qualified_name(
        active_settings.CLICKHOUSE_DATABASE,
        NAMESPACE_METADATA_TABLE_NAME,
    )
    node_institution_table = table_qualified_name(
        active_settings.CLICKHOUSE_DATABASE,
        NODE_INSTITUTION_TABLE_NAME,
    )

    where_clauses = [
        f"usage.date >= toDate('{start_day.isoformat()}')",
        f"usage.date <= toDate('{end_day.isoformat()}')",
    ]
    _maybe_add_in_filter(where_clauses, expression="usage.namespace", values=namespace_filters)
    _maybe_add_in_filter(
        where_clauses,
        expression="coalesce(meta.institution, 'Unknown')",
        values=institution_filters,
    )
    _maybe_add_in_filter(where_clauses, expression="usage.node", values=node_filters)
    _maybe_add_in_filter(
        where_clauses,
        expression="coalesce(node_map.institution_name, 'Unknown')",
        values=node_institution_filters,
    )
    _maybe_add_in_filter(where_clauses, expression="usage.resource", values=resource_filters)

    if node_regex:
        where_clauses.append(f"match(usage.node, {_sql_string_literal(node_regex)})")

    select_expressions = [
        f"{_GROUPABLE_EXPRESSIONS[column]} AS {column}" for column in normalized_group_by
    ]
    group_expressions = [_GROUPABLE_EXPRESSIONS[column] for column in normalized_group_by]

    order_by_parts: list[str] = []
    if "date" in normalized_group_by:
        order_by_parts.append("date DESC")
    order_by_parts.extend(column for column in normalized_group_by if column != "date")
    if "date" not in normalized_group_by:
        order_by_parts.append("usage DESC")

    sql = f"""
SELECT
  {", ".join(select_expressions)},
  sum(usage.usage) AS usage
FROM {usage_table} AS usage
LEFT JOIN {metadata_table} AS meta ON usage.namespace = meta.namespace
LEFT JOIN {node_institution_table} AS node_map ON usage.node = node_map.node
WHERE {" AND ".join(where_clauses)}
GROUP BY {", ".join(group_expressions)}
ORDER BY {", ".join(order_by_parts)}
LIMIT {safe_limit}
""".strip()

    return AccountingQuerySpec(
        sql=sql,
        granularity=granularity,
        start_date=start_day,
        end_date=end_day,
        group_by=normalized_group_by,
        limit=safe_limit,
    )


def query_resource_usage(
    client,
    *,
    granularity: str = "namespace",
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    namespace: str | Sequence[str] | None = None,
    institution: str | Sequence[str] | None = None,
    node: str | Sequence[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | Sequence[str] | None = None,
    resource: str | Sequence[str] | None = None,
    group_by: Sequence[str] | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    spec = build_resource_usage_query(
        client,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        namespace=namespace,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        resource=resource,
        group_by=group_by,
        limit=limit,
        settings=settings,
    )
    result = client.query(spec.sql)
    column_names = list(getattr(result, "column_names", []) or spec.group_by + ["usage"])

    rows: list[dict[str, Any]] = []
    for row in result.result_rows:
        rows.append(
            {
                column_name: _jsonify_value(value)
                for column_name, value in zip(column_names, row, strict=False)
            }
        )

    return {
        "granularity": spec.granularity,
        "start_date": spec.start_date.isoformat(),
        "end_date": spec.end_date.isoformat(),
        "group_by": spec.group_by,
        "limit": spec.limit,
        "row_count": len(rows),
        "rows": rows,
    }
