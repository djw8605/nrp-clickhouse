from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest

from nrp_accounting_pipeline.accounting_queries import (
    build_resource_usage_query,
    query_resource_usage,
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
)


@dataclass
class FakeQueryResult:
    result_rows: list[tuple[object, ...]]
    column_names: list[str] | None = None


class FakeClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, sql: str) -> FakeQueryResult:
        self.queries.append(sql)
        if sql.startswith("SELECT max(date)"):
            return FakeQueryResult(result_rows=[(date(2026, 4, 21),)], column_names=["max(date)"])

        return FakeQueryResult(
            result_rows=[
                ("demo-ns", "Delta University", "gpu-node-a", "gpu", "gpu_hours", Decimal("12.500000")),
                ("demo-ns", "Delta University", "gpu-node-b", "gpu", "gpu_hours", Decimal("4.250000")),
            ],
            column_names=["namespace", "institution", "node", "resource", "unit", "usage"],
        )


def test_build_resource_usage_query_applies_filters_and_latest_date_default() -> None:
    client = FakeClient()

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
    assert client.queries == ["SELECT max(date) FROM accounting.cluster_namespace_usage_daily"]


def test_build_resource_usage_query_requires_resource_dimension_when_multiple_resources() -> None:
    client = FakeClient()

    with pytest.raises(ValueError, match="group_by must include 'resource'"):
        build_resource_usage_query(
            client,
            start_date="2026-04-20",
            end_date="2026-04-21",
            group_by=["namespace", "institution"],
            settings=TEST_SETTINGS,
        )


def test_query_resource_usage_serializes_rows() -> None:
    client = FakeClient()

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
    assert len(client.queries) == 1
