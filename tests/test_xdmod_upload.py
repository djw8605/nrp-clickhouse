from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest

from nrp_accounting_pipeline.config import Settings
from nrp_accounting_pipeline.xdmod_upload import (
    XdmodUploadSettings,
    XdmodUsageRecord,
    build_xdmod_usage_query,
    fetch_xdmod_usage_records,
    run_upload_for_date,
    split_payload_batches,
    upload_xdmod_records,
)


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


class RecordingQueryClient:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.queries: list[str] = []
        self.closed = False

    def query(self, sql: str) -> FakeQueryResult:
        self.queries.append(" ".join(sql.split()))
        return FakeQueryResult(self.rows)

    def close(self) -> None:
        self.closed = True


def _record(name: str = "trainer-0") -> XdmodUsageRecord:
    return XdmodUsageRecord(
        pod_uid=f"{name}-uid",
        pod_name=name,
        user="jane.doe",
        user_organization="Delta University",
        account="analytics",
        record_date=date(2025, 12, 15),
        cpu=Decimal("24.500000"),
        gpu=Decimal("2.000000"),
        fpga=Decimal("0.000000"),
        mem=Decimal("0.000000"),
        storage=Decimal("12.250000"),
    )


def _upload_settings() -> XdmodUploadSettings:
    return XdmodUploadSettings(
        endpoint="https://xdmod.example.org/usage",
        auth_header=None,
        auth_value=None,
        timeout_seconds=10.0,
        retry_limit=1,
        max_records_per_post=5000,
        max_bytes_per_post=5_000_000,
    )


def test_build_xdmod_usage_query_pivots_resource_rows_without_node_dimension() -> None:
    sql = build_xdmod_usage_query(date(2025, 12, 15), TEST_SETTINGS)

    assert "FROM accounting.cluster_pod_usage_daily AS usage" in sql
    assert "LEFT JOIN accounting.namespace_metadata_mapping AS meta" in sql
    assert "sumIf(usage.usage, usage.resource = 'cpu') AS cpu" in sql
    assert "sumIf(usage.usage, usage.resource = 'storage') AS storage" in sql
    assert "usage.pod_uid" in sql
    assert "usage.date = toDate('2025-12-15')" in sql
    assert "usage.node" not in sql


def test_fetch_xdmod_usage_records_maps_clickhouse_rows_to_payload() -> None:
    client = RecordingQueryClient(
        [
            (
                date(2025, 12, 15),
                "analytics",
                "jane.doe",
                "pod-uid-1",
                "trainer-0",
                "Delta University",
                Decimal("24.500000"),
                Decimal("2.000000"),
                Decimal("0.000000"),
                Decimal("0.000000"),
                Decimal("12.250000"),
            )
        ]
    )

    records = fetch_xdmod_usage_records(client, date(2025, 12, 15), settings=TEST_SETTINGS)

    assert len(records) == 1
    assert records[0].to_payload() == {
        "PodUID": "pod-uid-1",
        "PodName": "trainer-0",
        "NumberOfContainers": 1,
        "User": "jane.doe",
        "UserOrganization": "Delta University",
        "Account": "analytics",
        "RecordStartTime": "2025-12-15 00:00:00",
        "RecordEndTime": "2025-12-15 23:59:59",
        "CPU": 24.5,
        "CPUType": "",
        "GPU": 2,
        "GPUType": "",
        "FPGA": 0,
        "FPGAType": "",
        "Mem": 1,
        "Storage": 12.25,
    }


def test_split_payload_batches_honors_record_limit() -> None:
    records = [{"PodName": "a"}, {"PodName": "b"}, {"PodName": "c"}]

    batches = list(
        split_payload_batches(
            records,
            max_records_per_post=2,
            max_bytes_per_post=10_000,
        )
    )

    assert batches == [[{"PodName": "a"}, {"PodName": "b"}], [{"PodName": "c"}]]


def test_split_payload_batches_rejects_single_record_over_byte_limit() -> None:
    with pytest.raises(ValueError, match="single XDMod record"):
        list(
            split_payload_batches(
                [{"PodName": "a" * 100}],
                max_records_per_post=10,
                max_bytes_per_post=10,
            )
        )


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class SplitOnTooLargeSession:
    def __init__(self) -> None:
        self.responses = [FakeResponse(413), FakeResponse(200), FakeResponse(200)]
        self.payloads: list[list[dict[str, object]]] = []

    def post(self, endpoint: str, *, data: bytes, headers: dict[str, str], timeout: float):
        assert endpoint == "https://xdmod.example.org/usage"
        assert headers["Content-Type"] == "application/json"
        assert timeout == 10.0
        self.payloads.append(json.loads(data.decode("utf-8")))
        return self.responses.pop(0)


def test_upload_xdmod_records_splits_http_413_batches() -> None:
    session = SplitOnTooLargeSession()

    post_count = upload_xdmod_records(
        [_record("trainer-0"), _record("trainer-1")],
        upload_settings=_upload_settings(),
        session=session,
    )

    assert post_count == 2
    assert [len(payload) for payload in session.payloads] == [2, 1, 1]


def test_run_upload_for_date_dry_run_does_not_require_endpoint(capsys) -> None:
    client = RecordingQueryClient(
        [
            (
                date(2025, 12, 15),
                "analytics",
                "jane.doe",
                "pod-uid-1",
                "trainer-0",
                "Delta University",
                Decimal("1.000000"),
                Decimal("0.000000"),
                Decimal("0.000000"),
                Decimal("2.000000"),
                Decimal("0.000000"),
            )
        ]
    )

    result = run_upload_for_date(
        date(2025, 12, 15),
        settings=TEST_SETTINGS,
        clickhouse_client=client,
        dry_run=True,
    )

    assert result.record_count == 1
    assert result.post_count == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["PodName"] == "trainer-0"
    assert payload[0]["PodUID"] == "pod-uid-1"
    assert payload[0]["Storage"] == 1
