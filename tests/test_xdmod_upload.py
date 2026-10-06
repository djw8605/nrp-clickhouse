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
        "fpga_hours": Decimal("0.000000"),
        "mem_gb_hours": Decimal("0.000000"),
        "storage_gb_hours": Decimal("12.250000"),
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
                Decimal("294.000000"),  # storage: 12.25 GB for 24h
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
        "Account": "nrp-analytics",
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
        "Mem": 0,
        "Storage": 12250000000,
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


def test_access_allocation_namespace_reports_the_bare_allocation_as_account() -> None:
    # ACCESS-allocated namespaces are named nrp-<allocation>; XDMoD maps the bare
    # allocation straight onto an ACCESS project.
    record = _record(account="nrp-agr260006")

    assert record.to_payload()["Account"] == "agr260006"


def test_non_access_namespace_is_prefixed_with_nrp() -> None:
    record = _record(account="unl-weitzel")

    assert record.to_payload()["Account"] == "nrp-unl-weitzel"


def test_non_access_namespace_already_starting_with_nrp_is_still_prefixed() -> None:
    # Prefixing every non-ACCESS namespace keeps it distinct from a namespace
    # literally named "web" once both carry the nrp- marker.
    record = _record(account="nrp-web")

    assert record.to_payload()["Account"] == "nrp-nrp-web"


@pytest.mark.parametrize(
    "namespace",
    ["nrp-agr26000", "nrp-agr2600061", "nrp-agr260006-dev", "agr260006", "nrp-ag1260006"],
)
def test_near_miss_allocation_names_are_treated_as_non_access(namespace: str) -> None:
    record = _record(account=namespace)

    assert record.to_payload()["Account"] == f"nrp-{namespace}"


def test_gpu_type_is_mixed_when_a_pod_spans_several_models() -> None:
    record = _record(gpu_model_count=2, gpu_model_name="a100")

    assert record.to_payload()["GPUType"] == "mixed"


def test_gpu_type_is_blank_when_the_pod_used_no_gpu() -> None:
    record = _record(gpu_hours=Decimal("0.000000"), gpu_model_count=0, gpu_model_name="")

    assert record.to_payload()["GPUType"] == ""


def test_mem_reports_bytes_allocated_not_gigabyte_hours() -> None:
    # 64 GB held for 6 hours is 384 GB-hours; XDMod wants the 64 GB allocated,
    # reported in bytes so XDMod can pick the display unit.
    record = _record(wall_hours=Decimal("6.000000"), mem_gb_hours=Decimal("384.000000"))

    assert record.to_payload()["Mem"] == 64_000_000_000


def test_storage_reports_bytes_allocated_not_gigabyte_hours() -> None:
    record = _record(wall_hours=Decimal("6.000000"), storage_gb_hours=Decimal("120.000000"))

    assert record.to_payload()["Storage"] == 20_000_000_000


def test_mem_and_storage_fall_back_to_a_24_hour_day_when_wall_hours_are_missing() -> None:
    record = _record(
        wall_hours=Decimal("0.000000"),
        cpu_hours=Decimal("24.000000"),
        mem_gb_hours=Decimal("48.000000"),
        storage_gb_hours=Decimal("240.000000"),
    )

    payload = record.to_payload()

    assert payload["Mem"] == 2_000_000_000
    assert payload["Storage"] == 10_000_000_000


def test_mem_below_one_gigabyte_survives_as_bytes() -> None:
    # 0.5 GB held for 24h is 12 GB-hours; in bytes it stays exact rather than
    # rounding to 0 or 1 GB.
    record = _record(wall_hours=Decimal("24.000000"), mem_gb_hours=Decimal("12.000000"))

    assert record.to_payload()["Mem"] == 500_000_000


def test_mem_and_storage_are_whole_bytes_when_the_division_is_not_exact() -> None:
    # 12.25 GB-hours over 24h is 510416666.66... bytes; XDMod gets whole bytes.
    record = _record(
        wall_hours=Decimal("24.000000"),
        mem_gb_hours=Decimal("12.250000"),
        storage_gb_hours=Decimal("12.250000"),
    )

    payload = record.to_payload()

    assert payload["Mem"] == 510416667
    assert payload["Storage"] == 510416667
    assert isinstance(payload["Mem"], int)
    assert isinstance(payload["Storage"], int)


def test_mem_and_storage_report_zero_when_nothing_was_requested() -> None:
    record = _record(mem_gb_hours=Decimal("0.000000"), storage_gb_hours=Decimal("0.000000"))

    payload = record.to_payload()

    assert payload["Mem"] == 0
    assert payload["Storage"] == 0


def test_fpga_reports_devices_allocated_not_fpga_hours() -> None:
    # 2 FPGAs held for 6 hours is 12 FPGA-hours; XDMod wants the 2 devices.
    record = _record(wall_hours=Decimal("6.000000"), fpga_hours=Decimal("12.000000"))

    assert record.to_payload()["FPGA"] == 2


def test_fpga_type_comes_from_the_raw_resource_label() -> None:
    record = _record(
        fpga_hours=Decimal("12.000000"),
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


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class RecordingFileSession:
    """Captures the multipart upload the way requests would receive it."""

    def __init__(self, statuses: list[int] | None = None) -> None:
        self.statuses = statuses or [200]
        self.calls: list[dict[str, object]] = []

    def post(self, endpoint: str, *, files, headers, timeout):
        self.calls.append(
            {"endpoint": endpoint, "files": files, "headers": headers, "timeout": timeout}
        )
        return FakeResponse(self.statuses.pop(0))


def test_upload_posts_one_multipart_file_containing_every_record() -> None:
    session = RecordingFileSession()

    post_count = upload_xdmod_records(
        [_record("trainer-0"), _record("trainer-1")],
        upload_settings=_upload_settings(),
        session=session,
    )

    assert post_count == 1
    assert len(session.calls) == 1
    _, file_body, content_type = session.calls[0]["files"]["file"]
    assert content_type == "application/json"
    uploaded = json.loads(file_body.decode("utf-8"))
    assert [record["PodName"] for record in uploaded] == ["trainer-0", "trainer-1"]


def test_upload_names_the_uploaded_file_after_the_record_date() -> None:
    session = RecordingFileSession()

    upload_xdmod_records(
        [_record("trainer-0")],
        upload_settings=_upload_settings(),
        session=session,
    )

    filename, _, _ = session.calls[0]["files"]["file"]
    assert filename == "nrp-usage-2025-12-15.json"


def test_upload_sends_auth_header_and_leaves_content_type_to_requests() -> None:
    settings = XdmodUploadSettings(
        endpoint="https://xdmod.example.org/usage",
        auth_header="Authorization",
        auth_value="Bearer secret-token",
        timeout_seconds=10.0,
        retry_limit=1,
    )
    session = RecordingFileSession()

    upload_xdmod_records(
        [_record("trainer-0")],
        upload_settings=settings,
        session=session,
    )

    headers = session.calls[0]["headers"]
    assert headers["Authorization"] == "Bearer secret-token"
    # requests must set Content-Type itself so the multipart boundary is correct.
    assert "Content-Type" not in headers


def test_upload_retries_the_file_post_after_a_server_error() -> None:
    session = RecordingFileSession(statuses=[500, 200])
    settings = XdmodUploadSettings(
        endpoint="https://xdmod.example.org/usage",
        auth_header=None,
        auth_value=None,
        timeout_seconds=10.0,
        retry_limit=2,
    )

    post_count = upload_xdmod_records(
        [_record("trainer-0")],
        upload_settings=settings,
        session=session,
    )

    assert post_count == 1
    assert len(session.calls) == 2


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
    assert payload[0]["Storage"] == 0
