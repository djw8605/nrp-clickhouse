from __future__ import annotations

from datetime import date
from decimal import Decimal

from nrp_accounting_pipeline.clickhouse_client import (
    insert_llm_token_usage,
    insert_namespace_usage,
    insert_pod_usage,
)
from nrp_accounting_pipeline.config import Settings
from nrp_accounting_pipeline.models import (
    LlmTokenUsageRecord,
    NamespaceUsageRecord,
    PodUsageRecord,
)


def _test_settings(batch_size: int = 5000) -> Settings:
    return Settings(
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
        CLICKHOUSE_WRITE_BATCH_SIZE=batch_size,
        INSTITUTION_CSV_URL=None,
        MCP_ENABLE_DNS_REBINDING_PROTECTION=True,
        MCP_ALLOWED_HOSTS=["127.0.0.1:*", "localhost:*"],
        MCP_ALLOWED_ORIGINS=["http://127.0.0.1:*", "http://localhost:*"],
    )


class RecordingClient:
    def __init__(self, fail_first_insert: bool = False) -> None:
        self.insert_calls: list[dict[str, object]] = []
        self._fail_first_insert = fail_first_insert

    def insert(self, table, data, column_names=None, settings=None):
        self.insert_calls.append(
            {
                "table": table,
                "row_count": len(data),
                "settings": dict(settings or {}),
            }
        )
        if self._fail_first_insert and len(self.insert_calls) == 1:
            raise RuntimeError("simulated network failure")


def _pod_row(pod_name: str) -> PodUsageRecord:
    return PodUsageRecord(
        date=date(2026, 7, 10),
        namespace="analytics",
        created_by="jane.doe",
        node="gpu-node-a",
        pod_hash=1,
        pod_uid="pod-uid-1",
        pod_name=pod_name,
        resource="cpu",
        raw_resource="cpu",
        gpu_model_name="not_applicable",
        usage=Decimal("1.000000"),
        unit="cpu_core_hours",
    )


def _namespace_row() -> NamespaceUsageRecord:
    return NamespaceUsageRecord(
        date=date(2026, 7, 10),
        namespace="analytics",
        created_by="jane.doe",
        node="gpu-node-a",
        resource="cpu",
        raw_resource="cpu",
        gpu_model_name="not_applicable",
        usage=Decimal("1.000000"),
        unit="cpu_core_hours",
    )


def _llm_row() -> LlmTokenUsageRecord:
    return LlmTokenUsageRecord(
        date=date(2026, 7, 10),
        namespace="analytics",
        token_alias="shared-token",
        model="qwen3",
        token_type="input",
        tokens_used=Decimal("42.000000"),
    )


def test_insert_pod_usage_sets_unique_dedup_token_per_batch() -> None:
    client = RecordingClient()
    rows = [_pod_row("trainer-0"), _pod_row("trainer-1"), _pod_row("trainer-2")]

    inserted = insert_pod_usage(client, rows, _test_settings(batch_size=2))

    assert inserted == 3
    tokens = [call["settings"].get("insert_deduplication_token") for call in client.insert_calls]
    assert all(tokens), "every insert batch must carry a deduplication token"
    assert len(set(tokens)) == len(tokens), "tokens must be unique per batch"


def test_insert_usage_tokens_differ_across_runs_with_identical_rows() -> None:
    # Re-ingesting the same date after a delete must NOT be absorbed by the
    # ReplicatedMergeTree block-dedup window, so identical data inserted by a
    # different run needs a different token.
    settings = _test_settings()
    rows = [_pod_row("trainer-0")]

    first_client = RecordingClient()
    insert_pod_usage(first_client, rows, settings)
    second_client = RecordingClient()
    insert_pod_usage(second_client, rows, settings)

    first_token = first_client.insert_calls[0]["settings"]["insert_deduplication_token"]
    second_token = second_client.insert_calls[0]["settings"]["insert_deduplication_token"]
    assert first_token != second_token


def test_insert_retry_reuses_same_dedup_token(monkeypatch) -> None:
    # A retried batch must reuse its token so a commit that succeeded
    # server-side before the client error is not double-inserted.
    monkeypatch.setattr("nrp_accounting_pipeline.clickhouse_client.time.sleep", lambda _: None)
    client = RecordingClient(fail_first_insert=True)

    inserted = insert_pod_usage(client, [_pod_row("trainer-0")], _test_settings())

    assert inserted == 1
    assert len(client.insert_calls) == 2
    first, second = client.insert_calls
    assert (
        first["settings"]["insert_deduplication_token"]
        == second["settings"]["insert_deduplication_token"]
    )


def test_namespace_and_llm_inserts_set_dedup_tokens() -> None:
    settings = _test_settings()

    namespace_client = RecordingClient()
    insert_namespace_usage(namespace_client, [_namespace_row()], settings)
    llm_client = RecordingClient()
    insert_llm_token_usage(llm_client, [_llm_row()], settings)

    for client in (namespace_client, llm_client):
        assert client.insert_calls[0]["settings"].get("insert_deduplication_token")
