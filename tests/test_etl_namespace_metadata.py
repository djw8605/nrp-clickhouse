from __future__ import annotations

from datetime import date

from nrp_accounting_pipeline.config import Settings
from nrp_accounting_pipeline.etl import run_for_date


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


class DummyClient:
    def close(self) -> None:
        return None


def test_run_for_date_syncs_metadata_for_llm_only_namespace(monkeypatch) -> None:
    sync_calls: list[list[object]] = []

    monkeypatch.setattr(
        "nrp_accounting_pipeline.etl._query_allocated_resources",
        lambda **kwargs: {"namespace_allocated_resources": {"data": {"result": []}}},
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.etl._query_llm_token_usage",
        lambda **kwargs: {
            "gen_ai_client_token_usage_sum": {
                "data": {
                    "result": [
                        {
                            "metric": {
                                "team_id": "llm-only-ns",
                                "token_alias": "shared-token",
                                "gen_ai_original_model": "qwen3",
                                "gen_ai_token_type": "input",
                            },
                            "value": [1700007200, "42.0"],
                        }
                    ]
                }
            }
        },
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.create_tables_if_not_exist",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.delete_existing_partitions",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.has_data_for_date",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.insert_pod_usage",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.insert_namespace_usage",
        lambda *args, **kwargs: 1,
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.insert_llm_token_usage",
        lambda *args, **kwargs: 1,
    )

    def _record_sync(client, rows, settings):
        sync_calls.append(list(rows))
        return {"inserted": 1, "updated": 0}

    monkeypatch.setattr(
        "nrp_accounting_pipeline.clickhouse_client.sync_namespace_metadata",
        _record_sync,
    )
    monkeypatch.setattr(
        "nrp_accounting_pipeline.etl.download_and_import_institutions",
        lambda **kwargs: 0,
    )

    result = run_for_date(
        date(2026, 4, 21),
        settings=TEST_SETTINGS,
        clickhouse_client=DummyClient(),
        namespace_metadata_seed_rows=[],
    )

    assert result["status"] == "success"
    assert sync_calls
    namespaces = {row.namespace for row in sync_calls[0]}
    assert "llm-only-ns" in namespaces
