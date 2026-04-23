from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import pytest

from nrp_accounting_pipeline.accounting_queries import (
    build_resource_usage_query,
    get_latest_data_date,
    list_active_namespaces,
    get_namespace_details,
    get_namespace_llm_daily_trend,
    get_namespace_llm_summary,
    get_namespace_summary,
    get_usage_timeseries,
    list_filter_values,
    list_llm_filter_values,
    query_llm_token_usage,
    query_resource_usage,
    top_resource_consumers,
)
from nrp_accounting_pipeline.config import Settings


TEST_SETTINGS = Settings(
    PROMETHEUS_URL="http://localhost:9090",
    PORTAL_RPC_URL="https://portal.nrp.ai/rpc",
    CLICKHOUSE_HOST="localhost",
    CLICKHOUSE_USER="default",
    CLICKHOUSE_PASSWORD="",
    CLICKHOUSE_DATABASE="accounting",
    MAX_QUERY_WORKERS=5,
    QUERY_STEP="1h",
    RETRY_LIMIT=3,
    CLICKHOUSE_PORT=8123,
    CLICKHOUSE_SECURE=False,
    PROMETHEUS_TIMEOUT_SECONDS=60.0,
    PORTAL_TIMEOUT_SECONDS=60.0,
    CLICKHOUSE_WRITE_BATCH_SIZE=5000,
    INSTITUTION_CSV_URL=None,
    MCP_ENABLE_DNS_REBINDING_PROTECTION=True,
    MCP_ALLOWED_HOSTS=["127.0.0.1:*", "localhost:*"],
    MCP_ALLOWED_ORIGINS=["http://127.0.0.1:*", "http://localhost:*"],
)


@dataclass
class FakeQueryResult:
    result_rows: list[tuple[object, ...]]
    column_names: list[str] | None = None


class PatternClient:
    def __init__(self, patterns: list[tuple[str, FakeQueryResult]]) -> None:
        self.patterns = patterns
        self.queries: list[str] = []

    def query(self, sql: str) -> FakeQueryResult:
        normalized = " ".join(sql.split())
        self.queries.append(normalized)
        for pattern, result in self.patterns:
            if pattern in normalized:
                return result
        raise AssertionError(f"Unexpected query: {normalized}")


def test_build_resource_usage_query_applies_filters_and_latest_date_default() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
        ]
    )

    spec = build_resource_usage_query(
        client,
        namespace="demo-ns",
        institution="Delta University",
        node_regex="^gpu-node-",
        resource="gpu",
        group_by=["namespace", "institution", "node", "resource", "unit"],
        settings=TEST_SETTINGS,
    )

    assert spec.start_date == date(2026, 4, 21)
    assert spec.end_date == date(2026, 4, 21)
    assert "FROM accounting.cluster_namespace_usage_daily AS usage" in spec.sql
    assert "usage.namespace IN ('demo-ns')" in spec.sql
    assert "coalesce(meta.institution, 'Unknown') IN ('Delta University')" in spec.sql
    assert "match(usage.node, '^gpu-node-')" in spec.sql
    assert "usage.resource IN ('gpu')" in spec.sql
    assert spec.group_by == ["namespace", "institution", "node", "resource", "unit"]


def test_build_resource_usage_query_normalizes_resource_aliases() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
        ]
    )

    spec = build_resource_usage_query(
        client,
        resource=["gpu-hours", "cpu_core_hours"],
        group_by=["namespace", "resource", "unit"],
        settings=TEST_SETTINGS,
    )

    assert "usage.resource IN ('gpu', 'cpu')" in spec.sql


def test_build_resource_usage_query_normalizes_llm_resource_aliases() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
        ]
    )

    spec = build_resource_usage_query(
        client,
        resource=["llm_tokens", "tokens"],
        group_by=["namespace", "resource", "unit"],
        settings=TEST_SETTINGS,
    )

    assert "usage.resource IN ('llm')" in spec.sql


def test_build_resource_usage_query_rejects_unknown_resource_values() -> None:
    client = PatternClient([])

    with pytest.raises(ValueError, match="Unsupported resource 'gpu-hours-total'"):
        build_resource_usage_query(
            client,
            start_date="2026-04-20",
            end_date="2026-04-21",
            resource="gpu-hours-total",
            group_by=["namespace", "resource", "unit"],
            settings=TEST_SETTINGS,
        )


def test_build_resource_usage_query_requires_resource_dimension_when_multiple_resources() -> None:
    client = PatternClient([])

    with pytest.raises(ValueError, match="group_by must include 'resource'"):
        build_resource_usage_query(
            client,
            start_date="2026-04-20",
            end_date="2026-04-21",
            group_by=["namespace", "institution"],
            settings=TEST_SETTINGS,
        )


def test_query_resource_usage_serializes_rows() -> None:
    client = PatternClient(
        [
            (
                "GROUP BY usage.namespace, coalesce(meta.institution, 'Unknown'), usage.node, usage.resource, usage.unit",
                FakeQueryResult(
                    [
                        (
                            "demo-ns",
                            "Delta University",
                            "gpu-node-a",
                            "gpu",
                            "gpu_hours",
                            Decimal("12.500000"),
                        ),
                        (
                            "demo-ns",
                            "Delta University",
                            "gpu-node-b",
                            "gpu",
                            "gpu_hours",
                            Decimal("4.250000"),
                        ),
                    ],
                    ["namespace", "institution", "node", "resource", "unit", "usage"],
                ),
            ),
        ]
    )

    result = query_resource_usage(
        client,
        start_date="2026-04-20",
        end_date="2026-04-21",
        namespace="demo-ns",
        resource="gpu",
        group_by=["namespace", "institution", "node", "resource", "unit"],
        settings=TEST_SETTINGS,
    )

    assert result["granularity"] == "namespace"
    assert result["start_date"] == "2026-04-20"
    assert result["end_date"] == "2026-04-21"
    assert result["row_count"] == 2
    assert result["rows"][0] == {
        "namespace": "demo-ns",
        "institution": "Delta University",
        "node": "gpu-node-a",
        "resource": "gpu",
        "unit": "gpu_hours",
        "usage": 12.5,
    }


def test_query_llm_token_usage_serializes_rows() -> None:
    client = PatternClient(
        [
            (
                "FROM accounting.llm_token_usage_daily AS usage",
                FakeQueryResult(
                    [
                        ("demo-ns", "qwen3", "input", Decimal("1200.000000")),
                        ("demo-ns", "qwen3", "output", Decimal("300.000000")),
                    ],
                    ["namespace", "model", "token_type", "tokens_used"],
                ),
            ),
        ]
    )

    result = query_llm_token_usage(
        client,
        start_date="2026-04-20",
        end_date="2026-04-21",
        namespace="demo-ns",
        group_by=["namespace", "model", "token_type"],
        settings=TEST_SETTINGS,
    )

    assert result["metric"] == "tokens_used"
    assert result["start_date"] == "2026-04-20"
    assert result["row_count"] == 2
    assert result["rows"][0]["tokens_used"] == 1200.0


def test_get_latest_data_date_returns_latest_day() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
        ]
    )

    result = get_latest_data_date(client, settings=TEST_SETTINGS)

    assert result == {
        "granularity": "namespace",
        "latest_data_date": "2026-04-21",
    }


def test_list_filter_values_returns_distinct_dimension_values() -> None:
    client = PatternClient(
        [
            (
                "SELECT count() AS total_count FROM ( SELECT DISTINCT coalesce(meta.institution, 'Unknown') AS value",
                FakeQueryResult([(2,)], ["total_count"]),
            ),
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
            (
                "SELECT DISTINCT coalesce(meta.institution, 'Unknown') AS value",
                FakeQueryResult(
                    [("Delta University",), ("Other University",)],
                    ["value"],
                ),
            ),
        ]
    )

    result = list_filter_values(
        client,
        dimension="institution",
        prefix="D",
        limit=20,
        settings=TEST_SETTINGS,
    )

    assert result["dimension"] == "institution"
    assert result["start_date"] == "2026-04-21"
    assert result["values"] == ["Delta University", "Other University"]
    assert result["total_count"] == 2
    assert result["is_truncated"] is False
    assert result["value_source"] == "observed_usage_rows"
    assert "LIKE 'D%'" in client.queries[-1]


def test_list_llm_filter_values_returns_distinct_values() -> None:
    client = PatternClient(
        [
            (
                "SELECT count() AS total_count FROM ( SELECT DISTINCT usage.model AS value",
                FakeQueryResult([(2,)], ["total_count"]),
            ),
            (
                "SELECT max(date) FROM accounting.llm_token_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
            (
                "SELECT DISTINCT usage.model AS value",
                FakeQueryResult(
                    [("gemma",), ("qwen3",)],
                    ["value"],
                ),
            ),
        ]
    )

    result = list_llm_filter_values(
        client,
        dimension="model",
        prefix="q",
        settings=TEST_SETTINGS,
    )

    assert result["dimension"] == "model"
    assert result["start_date"] == "2026-04-21"
    assert result["values"] == ["gemma", "qwen3"]
    assert result["value_source"] == "observed_llm_usage_rows"
    assert "LIKE 'q%'" in client.queries[-1]


def test_list_active_namespaces_defaults_to_last_30_days_and_reports_truncation() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
            (
                "SELECT count() AS total_count FROM ( SELECT DISTINCT usage.namespace AS value",
                FakeQueryResult([(3,)], ["total_count"]),
            ),
            (
                "SELECT DISTINCT usage.namespace AS value",
                FakeQueryResult(
                    [("alpha-ns",), ("beta-ns",)],
                    ["value"],
                ),
            ),
        ]
    )

    result = list_active_namespaces(client, settings=TEST_SETTINGS, limit=2)

    assert result["start_date"] == "2026-03-23"
    assert result["end_date"] == "2026-04-21"
    assert result["count"] == 2
    assert result["total_count"] == 3
    assert result["is_truncated"] is True
    assert result["value_source"] == "observed_usage_rows"
    assert result["namespaces"] == ["alpha-ns", "beta-ns"]


def test_top_resource_consumers_returns_ranked_rows() -> None:
    client = PatternClient(
        [
            (
                "GROUP BY coalesce(meta.institution, 'Unknown') ORDER BY usage DESC, institution LIMIT 5",
                FakeQueryResult(
                    [
                        ("Delta University", "gpu_hours", Decimal("17.250000")),
                        ("Other University", "gpu_hours", Decimal("7.000000")),
                    ],
                    ["institution", "unit", "usage"],
                ),
            ),
        ]
    )

    result = top_resource_consumers(
        client,
        dimension="institution",
        resource="gpu",
        start_date="2026-04-20",
        end_date="2026-04-21",
        limit=5,
        settings=TEST_SETTINGS,
    )

    assert result["dimension"] == "institution"
    assert result["resource"] == "gpu"
    assert result["row_count"] == 2
    assert result["rows"][0]["institution"] == "Delta University"
    assert result["rows"][0]["usage"] == 17.25


def test_top_resource_consumers_normalizes_resource_alias_to_canonical_value() -> None:
    client = PatternClient(
        [
            (
                "usage.resource IN ('gpu')",
                FakeQueryResult(
                    [("demo-ns", "gpu_hours", Decimal("17.250000"))],
                    ["namespace", "unit", "usage"],
                ),
            ),
        ]
    )

    result = top_resource_consumers(
        client,
        dimension="namespace",
        resource="GPU hours",
        start_date="2026-04-20",
        end_date="2026-04-21",
        limit=5,
        settings=TEST_SETTINGS,
    )

    assert result["resource"] == "gpu"
    assert result["rows"][0]["unit"] == "gpu_hours"


def test_get_usage_timeseries_defaults_to_last_30_days() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
            (
                "GROUP BY usage.date ORDER BY date ASC LIMIT 366",
                FakeQueryResult(
                    [
                        (date(2026, 4, 19), "gpu_hours", Decimal("5.000000")),
                        (date(2026, 4, 20), "gpu_hours", Decimal("6.500000")),
                    ],
                    ["date", "unit", "usage"],
                ),
            ),
        ]
    )

    result = get_usage_timeseries(
        client,
        dimension="institution",
        value="Delta University",
        resource="gpu",
        settings=TEST_SETTINGS,
    )

    assert result["start_date"] == "2026-03-23"
    assert result["end_date"] == "2026-04-21"
    assert result["rows"][0]["date"] == "2026-04-19"
    assert "coalesce(meta.institution, 'Unknown') = 'Delta University'" in client.queries[-1]


def test_get_namespace_summary_groups_by_resource() -> None:
    client = PatternClient(
        [
            (
                "GROUP BY usage.resource, usage.unit",
                FakeQueryResult(
                    [
                        ("gpu", "gpu_hours", Decimal("12.500000")),
                        ("cpu", "cpu_core_hours", Decimal("100.000000")),
                    ],
                    ["resource", "unit", "usage"],
                ),
            ),
        ]
    )

    result = get_namespace_summary(
        client,
        namespace="demo-ns",
        start_date="2026-04-21",
        end_date="2026-04-21",
        settings=TEST_SETTINGS,
    )

    assert result["namespace"] == "demo-ns"
    assert result["row_count"] == 2
    assert result["rows"][0]["resource"] == "gpu"


def test_get_namespace_llm_summary_groups_by_token_dimensions() -> None:
    client = PatternClient(
        [
            (
                "GROUP BY usage.token_alias, usage.model, usage.token_type",
                FakeQueryResult(
                    [
                        ("lab-assistant", "qwen3", "input", Decimal("1200.000000")),
                        ("lab-assistant", "qwen3", "output", Decimal("300.000000")),
                    ],
                    ["token_alias", "model", "token_type", "tokens_used"],
                ),
            ),
        ]
    )

    result = get_namespace_llm_summary(
        client,
        namespace="demo-ns",
        start_date="2026-04-21",
        end_date="2026-04-21",
        settings=TEST_SETTINGS,
    )

    assert result["namespace"] == "demo-ns"
    assert result["row_count"] == 2
    assert result["rows"][0]["token_alias"] == "lab-assistant"


def test_get_namespace_llm_daily_trend_defaults_to_last_30_days() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.llm_token_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
            (
                "GROUP BY usage.date ORDER BY date DESC LIMIT 366",
                FakeQueryResult(
                    [
                        (date(2026, 4, 20), Decimal("1200.000000")),
                        (date(2026, 4, 21), Decimal("1500.000000")),
                    ],
                    ["date", "tokens_used"],
                ),
            ),
        ]
    )

    result = get_namespace_llm_daily_trend(
        client,
        namespace="demo-ns",
        settings=TEST_SETTINGS,
    )

    assert result["start_date"] == "2026-03-23"
    assert result["end_date"] == "2026-04-21"
    assert result["rows"][0]["date"] == "2026-04-20"
    assert result["rows"][1]["tokens_used"] == 1500.0


def test_get_namespace_details_composes_summary_trend_and_top_nodes() -> None:
    client = PatternClient(
        [
            (
                "SELECT max(date) FROM accounting.cluster_namespace_usage_daily",
                FakeQueryResult([(date(2026, 4, 21),)], ["max(date)"]),
            ),
            (
                "FROM accounting.namespace_metadata_mapping WHERE namespace = 'demo-ns' LIMIT 1",
                FakeQueryResult(
                    [
                        (
                            "demo-ns",
                            "Dr Example",
                            "Delta University",
                            "admin1,admin2",
                            "Delta University",
                            datetime(2026, 4, 21, 12, 0, 0),
                        ),
                    ],
                    [
                        "namespace",
                        "pi",
                        "institution",
                        "admins",
                        "user_institutions",
                        "updated_at",
                    ],
                ),
            ),
            (
                "WHERE usage.date >= toDate('2026-04-21') AND usage.date <= toDate('2026-04-21') AND usage.namespace IN ('demo-ns') GROUP BY usage.resource, usage.unit",
                FakeQueryResult(
                    [
                        ("gpu", "gpu_hours", Decimal("12.500000")),
                        ("cpu", "cpu_core_hours", Decimal("100.000000")),
                        ("llm", "tokens", Decimal("1500.000000")),
                    ],
                    ["resource", "unit", "usage"],
                ),
            ),
            (
                "WHERE usage.date >= toDate('2026-03-23') AND usage.date <= toDate('2026-04-21') AND usage.namespace IN ('demo-ns') GROUP BY usage.date, usage.resource, usage.unit",
                FakeQueryResult(
                    [
                        (date(2026, 4, 20), "gpu", "gpu_hours", Decimal("5.000000")),
                        (date(2026, 4, 21), "gpu", "gpu_hours", Decimal("7.500000")),
                    ],
                    ["date", "resource", "unit", "usage"],
                ),
            ),
            (
                "usage.namespace IN ('demo-ns') AND usage.resource IN ('gpu')",
                FakeQueryResult(
                    [
                        ("gpu-node-a", "gpu_hours", Decimal("8.000000")),
                        ("gpu-node-b", "gpu_hours", Decimal("4.500000")),
                    ],
                    ["node", "unit", "usage"],
                ),
            ),
            (
                "usage.namespace IN ('demo-ns') AND usage.resource IN ('cpu')",
                FakeQueryResult(
                    [
                        ("cpu-node-a", "cpu_core_hours", Decimal("60.000000")),
                        ("cpu-node-b", "cpu_core_hours", Decimal("40.000000")),
                    ],
                    ["node", "unit", "usage"],
                ),
            ),
        ]
    )

    result = get_namespace_details(
        client,
        namespace="demo-ns",
        settings=TEST_SETTINGS,
    )

    assert result["namespace"] == "demo-ns"
    assert result["latest_data_date"] == "2026-04-21"
    assert result["metadata"]["institution"] == "Delta University"
    assert len(result["latest_summary"]) == 3
    assert len(result["daily_trend"]) == 2
    assert set(result["top_nodes_by_resource"]) == {"gpu", "cpu"}
