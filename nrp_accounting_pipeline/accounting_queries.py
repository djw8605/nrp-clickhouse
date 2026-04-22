from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import re
from typing import Any, Sequence

from .aggregation import normalize_resource as normalize_metric_resource
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
DEFAULT_TREND_DAYS = 30
DEFAULT_DISCOVERY_LIMIT = 100
DEFAULT_TOP_LIMIT = 10
DEFAULT_ACTIVE_NAMESPACE_LIMIT = MAX_RESULT_LIMIT

_CANONICAL_RESOURCE_VALUES = (
    "cpu",
    "gpu",
    "fpga",
    "memory",
    "storage",
    "network",
    "other",
)

_RESOURCE_INPUT_ALIASES = {
    "cpu": "cpu",
    "cpu_core_hour": "cpu",
    "cpu_core_hours": "cpu",
    "cpu_hour": "cpu",
    "cpu_hours": "cpu",
    "gpu": "gpu",
    "gpu_hour": "gpu",
    "gpu_hours": "gpu",
    "fpga": "fpga",
    "fpga_hour": "fpga",
    "fpga_hours": "fpga",
    "memory": "memory",
    "memory_gb": "memory",
    "memory_gb_hour": "memory",
    "memory_gb_hours": "memory",
    "ram": "memory",
    "ram_gb": "memory",
    "ram_gb_hour": "memory",
    "ram_gb_hours": "memory",
    "storage": "storage",
    "storage_gb": "storage",
    "storage_gb_hour": "storage",
    "storage_gb_hours": "storage",
    "ephemeral_storage": "storage",
    "disk": "storage",
    "disk_gb": "storage",
    "disk_gb_hour": "storage",
    "disk_gb_hours": "storage",
    "network": "network",
    "network_gb": "network",
    "network_receive": "network",
    "network_transmit": "network",
    "other": "other",
}

_RESOURCE_ALIAS_EXAMPLES = (
    "gpu_hours -> gpu",
    "gpu-hours -> gpu",
    "GPU hours -> gpu",
    "cpu_core_hours -> cpu",
)

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

_LISTABLE_DIMENSIONS = {
    "namespace": _GROUPABLE_EXPRESSIONS["namespace"],
    "institution": _GROUPABLE_EXPRESSIONS["institution"],
    "node": _GROUPABLE_EXPRESSIONS["node"],
    "resource": _GROUPABLE_EXPRESSIONS["resource"],
    "pi": _GROUPABLE_EXPRESSIONS["pi"],
    "node_institution": _GROUPABLE_EXPRESSIONS["node_institution"],
    "pod_name": _GROUPABLE_EXPRESSIONS["pod_name"],
}

_TOP_DIMENSIONS = {
    "namespace": _GROUPABLE_EXPRESSIONS["namespace"],
    "institution": _GROUPABLE_EXPRESSIONS["institution"],
    "node": _GROUPABLE_EXPRESSIONS["node"],
}

_TIMESERIES_DIMENSIONS = {
    "namespace": _GROUPABLE_EXPRESSIONS["namespace"],
    "institution": _GROUPABLE_EXPRESSIONS["institution"],
    "node": _GROUPABLE_EXPRESSIONS["node"],
    "node_institution": _GROUPABLE_EXPRESSIONS["node_institution"],
}

_NAMESPACE_METADATA_COLUMNS = [
    "namespace",
    "pi",
    "institution",
    "admins",
    "user_institutions",
    "updated_at",
]


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


def _resource_alias_key(value: str) -> str:
    key = (value or "").strip().lower()
    key = re.sub(r"[\s\-/]+", "_", key)
    key = re.sub(r"_+", "_", key)
    return key.strip("_")


def _normalize_resource_value(value: str) -> str:
    alias_key = _resource_alias_key(value)
    if not alias_key:
        raise ValueError("resource values must be non-empty strings")

    direct_match = _RESOURCE_INPUT_ALIASES.get(alias_key)
    if direct_match is not None:
        return direct_match

    normalized_metric = normalize_metric_resource(value)
    if normalized_metric != "other":
        # Allow raw metric names and vendor-specific labels from Prometheus-style inputs.
        if any(
            token in alias_key
            for token in (
                "nvidia_com",
                "amd_com_xilinx",
                "network_receive",
                "network_transmit",
                "fs_usage",
            )
        ):
            return normalized_metric

    allowed = ", ".join(_CANONICAL_RESOURCE_VALUES)
    examples = "; ".join(_RESOURCE_ALIAS_EXAMPLES)
    raise ValueError(
        f"Unsupported resource {value!r}. Use one of: {allowed}. "
        f"Common aliases are normalized automatically: {examples}."
    )


def _normalize_resource_list(value: str | Sequence[str] | None) -> list[str]:
    if value is None:
        return []

    raw_values = [value] if isinstance(value, str) else list(value)
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_value in raw_values:
        item = str(raw_value).strip()
        if not item:
            continue
        canonical = _normalize_resource_value(item)
        if canonical in seen:
            continue
        normalized.append(canonical)
        seen.add(canonical)

    return normalized


def _coerce_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _validate_granularity(granularity: str) -> str:
    if granularity not in _TABLE_BY_GRANULARITY:
        allowed = ", ".join(sorted(_TABLE_BY_GRANULARITY))
        raise ValueError(f"Unsupported granularity {granularity!r}. Allowed values: {allowed}")
    return granularity


def _latest_ingested_date(
    client,
    *,
    granularity: str,
    settings: Settings,
) -> date:
    safe_granularity = _validate_granularity(granularity)
    table_name = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        _TABLE_BY_GRANULARITY[safe_granularity],
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


def _resolve_date_window_with_default_days(
    client,
    *,
    granularity: str,
    settings: Settings,
    start_date: date | datetime | str | None,
    end_date: date | datetime | str | None,
    default_days: int,
) -> tuple[date, date]:
    parsed_start = _coerce_date(start_date)
    parsed_end = _coerce_date(end_date)

    if parsed_start is None and parsed_end is None:
        latest_date = _latest_ingested_date(
            client,
            granularity=granularity,
            settings=settings,
        )
        span_days = max(default_days - 1, 0)
        return latest_date - timedelta(days=span_days), latest_date

    return _resolve_date_window(
        client,
        granularity=granularity,
        settings=settings,
        start_date=start_date,
        end_date=end_date,
    )


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


def _validate_dimension(
    name: str,
    allowed_dimensions: dict[str, str],
    *,
    granularity: str,
) -> str:
    if name not in allowed_dimensions:
        allowed = ", ".join(sorted(allowed_dimensions))
        raise ValueError(f"Unsupported dimension {name!r}. Allowed values: {allowed}")
    if granularity != "pod" and name == "pod_name":
        raise ValueError("pod_name can only be used when granularity='pod'")
    return name


def _validate_limit(limit: int | None, *, default: int = DEFAULT_RESULT_LIMIT) -> int:
    if limit is None:
        return default
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


def _require_single_filter_value(name: str, value: str | Sequence[str] | None) -> str:
    values = _normalize_string_list(value)
    if len(values) != 1:
        raise ValueError(f"{name} requires exactly one value")
    return values[0]


def _require_single_resource_value(value: str | Sequence[str] | None) -> str:
    values = _normalize_resource_list(value)
    if len(values) != 1:
        raise ValueError("resource requires exactly one value")
    return values[0]


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


def _resolve_usage_tables(
    *,
    granularity: str,
    settings: Settings,
) -> tuple[str, str, str]:
    safe_granularity = _validate_granularity(granularity)
    usage_table = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        _TABLE_BY_GRANULARITY[safe_granularity],
    )
    metadata_table = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        NAMESPACE_METADATA_TABLE_NAME,
    )
    node_institution_table = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        NODE_INSTITUTION_TABLE_NAME,
    )
    return usage_table, metadata_table, node_institution_table


def _build_usage_where_clauses(
    *,
    start_day: date,
    end_day: date,
    namespace_filters: Sequence[str] = (),
    institution_filters: Sequence[str] = (),
    node_filters: Sequence[str] = (),
    node_regex: str | None = None,
    node_institution_filters: Sequence[str] = (),
    resource_filters: Sequence[str] = (),
    extra_clauses: Sequence[str] = (),
) -> list[str]:
    if node_regex:
        re.compile(node_regex)

    clauses = [
        f"usage.date >= toDate('{start_day.isoformat()}')",
        f"usage.date <= toDate('{end_day.isoformat()}')",
    ]
    _maybe_add_in_filter(clauses, expression="usage.namespace", values=namespace_filters)
    _maybe_add_in_filter(
        clauses,
        expression="coalesce(meta.institution, 'Unknown')",
        values=institution_filters,
    )
    _maybe_add_in_filter(clauses, expression="usage.node", values=node_filters)
    _maybe_add_in_filter(
        clauses,
        expression="coalesce(node_map.institution_name, 'Unknown')",
        values=node_institution_filters,
    )
    _maybe_add_in_filter(clauses, expression="usage.resource", values=resource_filters)

    if node_regex:
        clauses.append(f"match(usage.node, {_sql_string_literal(node_regex)})")

    clauses.extend(extra_clauses)
    return clauses


def _query_rows(
    client,
    sql: str,
    fallback_column_names: Sequence[str],
) -> list[dict[str, Any]]:
    result = client.query(sql)
    column_names = list(getattr(result, "column_names", []) or fallback_column_names)

    rows: list[dict[str, Any]] = []
    for row in result.result_rows:
        rows.append(
            {
                column_name: _jsonify_value(value)
                for column_name, value in zip(column_names, row, strict=False)
            }
        )
    return rows


def _query_scalar(client, sql: str) -> Any:
    result = client.query(sql)
    if not result.result_rows:
        return None
    return result.result_rows[0][0]


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
    safe_granularity = _validate_granularity(granularity)
    active_settings = settings or get_settings()
    start_day, end_day = _resolve_date_window(
        client,
        granularity=safe_granularity,
        settings=active_settings,
        start_date=start_date,
        end_date=end_date,
    )
    normalized_group_by = _validate_group_by(safe_granularity, group_by)
    safe_limit = _validate_limit(limit)

    namespace_filters = _normalize_string_list(namespace)
    institution_filters = _normalize_string_list(institution)
    node_filters = _normalize_string_list(node)
    node_institution_filters = _normalize_string_list(node_institution)
    resource_filters = _normalize_resource_list(resource)

    _validate_resource_safety(normalized_group_by, resource_filters)

    usage_table, metadata_table, node_institution_table = _resolve_usage_tables(
        granularity=safe_granularity,
        settings=active_settings,
    )
    where_clauses = _build_usage_where_clauses(
        start_day=start_day,
        end_day=end_day,
        namespace_filters=namespace_filters,
        institution_filters=institution_filters,
        node_filters=node_filters,
        node_regex=node_regex,
        node_institution_filters=node_institution_filters,
        resource_filters=resource_filters,
    )

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
        granularity=safe_granularity,
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
    rows = _query_rows(client, spec.sql, spec.group_by + ["usage"])

    return {
        "granularity": spec.granularity,
        "start_date": spec.start_date.isoformat(),
        "end_date": spec.end_date.isoformat(),
        "group_by": spec.group_by,
        "limit": spec.limit,
        "row_count": len(rows),
        "rows": rows,
    }


def get_latest_data_date(
    client,
    *,
    granularity: str = "namespace",
    settings: Settings | None = None,
) -> dict[str, str]:
    safe_granularity = _validate_granularity(granularity)
    active_settings = settings or get_settings()
    latest_date = _latest_ingested_date(
        client,
        granularity=safe_granularity,
        settings=active_settings,
    )
    return {
        "granularity": safe_granularity,
        "latest_data_date": latest_date.isoformat(),
    }


def list_filter_values(
    client,
    *,
    dimension: str,
    granularity: str = "namespace",
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    namespace: str | Sequence[str] | None = None,
    institution: str | Sequence[str] | None = None,
    node: str | Sequence[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | Sequence[str] | None = None,
    resource: str | Sequence[str] | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    return _list_distinct_dimension_values(
        client,
        dimension=dimension,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        namespace=namespace,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        resource=resource,
        prefix=prefix,
        regex=regex,
        limit=limit,
        settings=settings,
    )


def _list_distinct_dimension_values(
    client,
    *,
    dimension: str,
    granularity: str = "namespace",
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    namespace: str | Sequence[str] | None = None,
    institution: str | Sequence[str] | None = None,
    node: str | Sequence[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | Sequence[str] | None = None,
    resource: str | Sequence[str] | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
    default_days: int | None = None,
    default_limit: int = DEFAULT_DISCOVERY_LIMIT,
) -> dict[str, Any]:
    safe_granularity = _validate_granularity(granularity)
    safe_dimension = _validate_dimension(
        dimension,
        _LISTABLE_DIMENSIONS,
        granularity=safe_granularity,
    )
    safe_limit = _validate_limit(limit, default=default_limit)
    active_settings = settings or get_settings()
    if default_days is None:
        start_day, end_day = _resolve_date_window(
            client,
            granularity=safe_granularity,
            settings=active_settings,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        start_day, end_day = _resolve_date_window_with_default_days(
            client,
            granularity=safe_granularity,
            settings=active_settings,
            start_date=start_date,
            end_date=end_date,
            default_days=default_days,
        )

    if regex:
        re.compile(regex)

    usage_table, metadata_table, node_institution_table = _resolve_usage_tables(
        granularity=safe_granularity,
        settings=active_settings,
    )
    dimension_expression = _LISTABLE_DIMENSIONS[safe_dimension]

    extra_clauses = [f"notEmpty(toString({dimension_expression}))"]
    if prefix:
        extra_clauses.append(f"{dimension_expression} LIKE {_sql_string_literal(prefix + '%')}")
    if regex:
        extra_clauses.append(f"match({dimension_expression}, {_sql_string_literal(regex)})")

    where_clauses = _build_usage_where_clauses(
        start_day=start_day,
        end_day=end_day,
        namespace_filters=_normalize_string_list(namespace),
        institution_filters=_normalize_string_list(institution),
        node_filters=_normalize_string_list(node),
        node_regex=node_regex,
        node_institution_filters=_normalize_string_list(node_institution),
        resource_filters=_normalize_resource_list(resource),
        extra_clauses=extra_clauses,
    )

    from_and_where_sql = f"""
FROM {usage_table} AS usage
LEFT JOIN {metadata_table} AS meta ON usage.namespace = meta.namespace
LEFT JOIN {node_institution_table} AS node_map ON usage.node = node_map.node
WHERE {" AND ".join(where_clauses)}
""".strip()

    sql = f"""
SELECT DISTINCT
  {dimension_expression} AS value
{from_and_where_sql}
ORDER BY value
LIMIT {safe_limit}
""".strip()
    rows = _query_rows(client, sql, ["value"])
    values = [row["value"] for row in rows]
    total_count_sql = f"""
SELECT count() AS total_count
FROM (
  SELECT DISTINCT
    {dimension_expression} AS value
  {from_and_where_sql}
)
""".strip()
    total_count = int(_query_scalar(client, total_count_sql) or 0)

    return {
        "dimension": safe_dimension,
        "granularity": safe_granularity,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "limit": safe_limit,
        "count": len(values),
        "total_count": total_count,
        "is_truncated": total_count > len(values),
        "value_source": "observed_usage_rows",
        "values": values,
    }


def list_active_namespaces(
    client,
    *,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    institution: str | Sequence[str] | None = None,
    node: str | Sequence[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | Sequence[str] | None = None,
    resource: str | Sequence[str] | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    result = _list_distinct_dimension_values(
        client,
        dimension="namespace",
        granularity="namespace",
        start_date=start_date,
        end_date=end_date,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        resource=resource,
        prefix=prefix,
        regex=regex,
        limit=limit,
        settings=settings,
        default_days=DEFAULT_TREND_DAYS,
        default_limit=DEFAULT_ACTIVE_NAMESPACE_LIMIT,
    )

    return {
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "limit": result["limit"],
        "count": result["count"],
        "total_count": result["total_count"],
        "is_truncated": result["is_truncated"],
        "value_source": result["value_source"],
        "namespaces": result["values"],
    }


def top_resource_consumers(
    client,
    *,
    dimension: str,
    resource: str,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    granularity: str = "namespace",
    namespace: str | Sequence[str] | None = None,
    institution: str | Sequence[str] | None = None,
    node: str | Sequence[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | Sequence[str] | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    safe_granularity = _validate_granularity(granularity)
    safe_dimension = _validate_dimension(
        dimension,
        _TOP_DIMENSIONS,
        granularity=safe_granularity,
    )
    safe_resource = _require_single_resource_value(resource)
    safe_limit = _validate_limit(limit, default=DEFAULT_TOP_LIMIT)
    active_settings = settings or get_settings()
    start_day, end_day = _resolve_date_window(
        client,
        granularity=safe_granularity,
        settings=active_settings,
        start_date=start_date,
        end_date=end_date,
    )
    usage_table, metadata_table, node_institution_table = _resolve_usage_tables(
        granularity=safe_granularity,
        settings=active_settings,
    )
    dimension_expression = _TOP_DIMENSIONS[safe_dimension]

    where_clauses = _build_usage_where_clauses(
        start_day=start_day,
        end_day=end_day,
        namespace_filters=_normalize_string_list(namespace),
        institution_filters=_normalize_string_list(institution),
        node_filters=_normalize_string_list(node),
        node_regex=node_regex,
        node_institution_filters=_normalize_string_list(node_institution),
        resource_filters=[safe_resource],
        extra_clauses=[f"notEmpty(toString({dimension_expression}))"],
    )

    sql = f"""
SELECT
  {dimension_expression} AS {safe_dimension},
  min(usage.unit) AS unit,
  sum(usage.usage) AS usage
FROM {usage_table} AS usage
LEFT JOIN {metadata_table} AS meta ON usage.namespace = meta.namespace
LEFT JOIN {node_institution_table} AS node_map ON usage.node = node_map.node
WHERE {" AND ".join(where_clauses)}
GROUP BY {dimension_expression}
ORDER BY usage DESC, {safe_dimension}
LIMIT {safe_limit}
""".strip()
    rows = _query_rows(client, sql, [safe_dimension, "unit", "usage"])

    return {
        "dimension": safe_dimension,
        "resource": safe_resource,
        "granularity": safe_granularity,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "limit": safe_limit,
        "row_count": len(rows),
        "rows": rows,
    }


def get_usage_timeseries(
    client,
    *,
    dimension: str,
    value: str,
    resource: str,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    granularity: str = "namespace",
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    safe_granularity = _validate_granularity(granularity)
    safe_dimension = _validate_dimension(
        dimension,
        _TIMESERIES_DIMENSIONS,
        granularity=safe_granularity,
    )
    safe_resource = _require_single_resource_value(resource)
    safe_limit = _validate_limit(limit, default=366)
    active_settings = settings or get_settings()
    start_day, end_day = _resolve_date_window_with_default_days(
        client,
        granularity=safe_granularity,
        settings=active_settings,
        start_date=start_date,
        end_date=end_date,
        default_days=DEFAULT_TREND_DAYS,
    )
    usage_table, metadata_table, node_institution_table = _resolve_usage_tables(
        granularity=safe_granularity,
        settings=active_settings,
    )
    dimension_expression = _TIMESERIES_DIMENSIONS[safe_dimension]

    where_clauses = _build_usage_where_clauses(
        start_day=start_day,
        end_day=end_day,
        resource_filters=[safe_resource],
        extra_clauses=[f"{dimension_expression} = {_sql_string_literal(value)}"],
    )

    sql = f"""
SELECT
  usage.date AS date,
  min(usage.unit) AS unit,
  sum(usage.usage) AS usage
FROM {usage_table} AS usage
LEFT JOIN {metadata_table} AS meta ON usage.namespace = meta.namespace
LEFT JOIN {node_institution_table} AS node_map ON usage.node = node_map.node
WHERE {" AND ".join(where_clauses)}
GROUP BY usage.date
ORDER BY date ASC
LIMIT {safe_limit}
""".strip()
    rows = _query_rows(client, sql, ["date", "unit", "usage"])

    return {
        "dimension": safe_dimension,
        "value": value,
        "resource": safe_resource,
        "granularity": safe_granularity,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "limit": safe_limit,
        "row_count": len(rows),
        "rows": rows,
    }


def get_namespace_summary(
    client,
    *,
    namespace: str,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    resource: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    result = query_resource_usage(
        client,
        granularity="namespace",
        start_date=start_date,
        end_date=end_date,
        namespace=namespace,
        resource=resource,
        group_by=["resource", "unit"],
        limit=100,
        settings=settings,
    )
    return {
        "namespace": namespace,
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "row_count": result["row_count"],
        "rows": result["rows"],
    }


def get_namespace_daily_trend(
    client,
    *,
    namespace: str,
    resource: str | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    start_day, end_day = _resolve_date_window_with_default_days(
        client,
        granularity="namespace",
        settings=active_settings,
        start_date=start_date,
        end_date=end_date,
        default_days=DEFAULT_TREND_DAYS,
    )
    group_by = ["date", "unit"] if resource else ["date", "resource", "unit"]
    result = query_resource_usage(
        client,
        granularity="namespace",
        start_date=start_day,
        end_date=end_day,
        namespace=namespace,
        resource=resource,
        group_by=group_by,
        limit=DEFAULT_RESULT_LIMIT,
        settings=active_settings,
    )
    rows = sorted(
        result["rows"],
        key=lambda row: (
            row["date"],
            str(row.get("resource", "")),
            str(row.get("unit", "")),
        ),
    )
    return {
        "namespace": namespace,
        "resource": resource,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "row_count": len(rows),
        "rows": rows,
    }


def top_nodes_for_namespace(
    client,
    *,
    namespace: str,
    resource: str,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    result = top_resource_consumers(
        client,
        dimension="node",
        resource=resource,
        start_date=start_date,
        end_date=end_date,
        granularity="namespace",
        namespace=namespace,
        limit=limit,
        settings=settings,
    )
    return {
        "namespace": namespace,
        "resource": resource,
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "limit": result["limit"],
        "row_count": result["row_count"],
        "rows": result["rows"],
    }


def _fetch_namespace_metadata(
    client,
    *,
    namespace: str,
    settings: Settings,
) -> dict[str, Any] | None:
    metadata_table = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        NAMESPACE_METADATA_TABLE_NAME,
    )
    sql = f"""
SELECT
  namespace,
  pi,
  institution,
  admins,
  user_institutions,
  updated_at
FROM {metadata_table}
WHERE namespace = {_sql_string_literal(namespace)}
LIMIT 1
""".strip()
    rows = _query_rows(client, sql, _NAMESPACE_METADATA_COLUMNS)
    return rows[0] if rows else None


def get_namespace_details(
    client,
    *,
    namespace: str,
    trend_days: int = DEFAULT_TREND_DAYS,
    top_node_limit: int = DEFAULT_TOP_LIMIT,
    top_nodes_resource: str | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    latest_date = _latest_ingested_date(
        client,
        granularity="namespace",
        settings=active_settings,
    )
    trend_start = latest_date - timedelta(days=max(trend_days - 1, 0))
    metadata = _fetch_namespace_metadata(
        client,
        namespace=namespace,
        settings=active_settings,
    )
    latest_summary = get_namespace_summary(
        client,
        namespace=namespace,
        start_date=latest_date,
        end_date=latest_date,
        settings=active_settings,
    )
    daily_trend = get_namespace_daily_trend(
        client,
        namespace=namespace,
        start_date=trend_start,
        end_date=latest_date,
        settings=active_settings,
    )

    resources_for_top_nodes: list[str]
    if top_nodes_resource:
        resources_for_top_nodes = [top_nodes_resource]
    else:
        resources_for_top_nodes = sorted(
            {
                str(row["resource"])
                for row in latest_summary["rows"]
                if row.get("resource")
            }
        )

    top_nodes_by_resource: dict[str, list[dict[str, Any]]] = {}
    for resource_name in resources_for_top_nodes:
        top_nodes_result = top_nodes_for_namespace(
            client,
            namespace=namespace,
            resource=resource_name,
            start_date=latest_date,
            end_date=latest_date,
            limit=top_node_limit,
            settings=active_settings,
        )
        top_nodes_by_resource[resource_name] = top_nodes_result["rows"]

    return {
        "namespace": namespace,
        "latest_data_date": latest_date.isoformat(),
        "metadata": metadata,
        "latest_summary": latest_summary["rows"],
        "daily_trend": daily_trend["rows"],
        "top_nodes_by_resource": top_nodes_by_resource,
    }
