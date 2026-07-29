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
    build_payload_records,
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


def _record(name: str = "trainer-0", **overrides: object) -> XdmodUsageRecord:
    fields: dict[str, object] = {
        "pod_uid": f"{name}-uid",
        "pod_name": name,
        "user": "jane.doe",
        "user_organization": "Delta University",
        "account": "analytics",
        "record_date": date(2025, 12, 15),
        "wall_hours": Decimal("24.000000"),
        "cpu_hours": Decimal("24.500000"),
        "gpu_hours": Decimal("2.000000"),
        "fpga": Decimal("0.000000"),
        "mem": Decimal("0.000000"),
        "storage": Decimal("12.250000"),
        "gpu_model_count": 0,
        "gpu_model_name": "",
        "fpga_raw_resource": "",
    }
    fields.update(overrides)
    return XdmodUsageRecord(**fields)  # type: ignore[arg-type]


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
    assert "sumIf(usage.usage, usage.resource = 'cpu') AS cpu_hours" in sql
    assert "sumIf(usage.usage, usage.resource = 'storage') AS storage" in sql
    assert "usage.pod_uid" in sql
    assert "usage.date = toDate('2025-12-15')" in sql
    assert "usage.node" not in sql


def test_build_xdmod_usage_query_selects_wall_hours_and_type_columns() -> None:
    sql = build_xdmod_usage_query(date(2025, 12, 15), TEST_SETTINGS)

    assert "sumIf(usage.usage, usage.resource = 'wall') AS wall_hours" in sql
    assert "'wall'" in sql.split("WHERE", 1)[1]
    # The GROUP BY is on pod, not on gpu_model_name or raw_resource, so the type
    # columns need conditional aggregates.
    # uniqExactIf rather than countDistinctIf: countDistinct is an alias for
    # uniqExact, and ClickHouse combinators are not guaranteed on aliases.
    assert (
        "uniqExactIf(usage.gpu_model_name, usage.resource = 'gpu') AS gpu_model_count" in sql
    )
    assert "anyIf(usage.gpu_model_name, usage.resource = 'gpu') AS gpu_model_name" in sql
    assert "anyIf(usage.raw_resource, usage.resource = 'fpga') AS fpga_raw_resource" in sql


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
                Decimal("96.000000"),  # cpu_hours: 4 cores for 24h
                Decimal("24.000000"),  # gpu_hours: 1 gpu for 24h
                Decimal("0.000000"),
                Decimal("0.000000"),
                Decimal("12.250000"),
                Decimal("24.000000"),  # wall_hours
                1,
                "a100",
                "",
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
        "WallHours": 24,
        "CPU": 4,
        "CPUType": "",
        "CPUHours": 96,
        "GPU": 1,
        "GPUType": "a100",
        "GPUHours": 24,
        "FPGA": 0,
        "FPGAType": "",
        "Mem": 1,
        "Storage": 12.25,
    }


def test_pod_holding_four_gpus_for_six_hours_reports_count_and_hours_separately() -> None:
    record = _record(
        wall_hours=Decimal("6.000000"),
        cpu_hours=Decimal("48.000000"),
        gpu_hours=Decimal("24.000000"),
        gpu_model_count=1,
        gpu_model_name="a100",
    )

    payload = record.to_payload()

    assert payload["WallHours"] == 6
    assert payload["GPU"] == 4
    assert payload["GPUHours"] == 24
    assert payload["CPU"] == 8
    assert payload["CPUHours"] == 48


def test_gpu_type_is_mixed_when_a_pod_spans_several_models() -> None:
    record = _record(gpu_model_count=2, gpu_model_name="a100")

    assert record.to_payload()["GPUType"] == "mixed"


def test_gpu_type_is_blank_when_the_pod_used_no_gpu() -> None:
    record = _record(gpu_hours=Decimal("0.000000"), gpu_model_count=0, gpu_model_name="")

    assert record.to_payload()["GPUType"] == ""


def test_fpga_type_comes_from_the_raw_resource_label() -> None:
    record = _record(
        fpga=Decimal("12.000000"),
        fpga_raw_resource="amd_com_xilinx_u55c",
    )

    assert record.to_payload()["FPGAType"] == "amd_com_xilinx_u55c"


def test_missing_wall_hours_falls_back_to_a_24_hour_day(caplog) -> None:
    record = _record(
        wall_hours=Decimal("0.000000"),
        cpu_hours=Decimal("48.000000"),
        gpu_hours=Decimal("0.000000"),
    )

    assert record.wall_hours_missing is True

    with caplog.at_level("WARNING"):
        payload = build_payload_records([record], date(2025, 12, 15))

    assert payload[0]["WallHours"] == 24
    assert payload[0]["CPU"] == 2
    assert payload[0]["CPUHours"] == 48
    assert "xdmod_upload_wall_hours_missing" in caplog.text


def test_record_with_no_usage_at_all_does_not_warn(caplog) -> None:
    record = _record(
        wall_hours=Decimal("0.000000"),
        cpu_hours=Decimal("0.000000"),
        gpu_hours=Decimal("0.000000"),
    )

    assert record.wall_hours_missing is False

    with caplog.at_level("WARNING"):
        payload = build_payload_records([record], date(2025, 12, 15))

    assert payload[0]["CPU"] == 0
    assert payload[0]["GPU"] == 0
    assert "xdmod_upload_wall_hours_missing" not in caplog.text


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
                Decimal("24.000000"),
                0,
                "",
                "",
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
