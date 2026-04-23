from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from nrp_accounting_pipeline.aggregation import (
    aggregate_llm_namespace_usage,
    aggregate_llm_token_usage,
)
from nrp_accounting_pipeline.clickhouse_client import (
    delete_existing_partitions,
    insert_llm_token_usage,
)
from nrp_accounting_pipeline.config import Settings
from nrp_accounting_pipeline.etl import run_test_mode
from nrp_accounting_pipeline.models import LlmTokenUsageRecord
from nrp_accounting_pipeline.schema import create_llm_token_table_sql


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


class RecordingClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserts: list[dict[str, object]] = []

    def command(self, statement: str) -> None:
        self.commands.append(statement)

    def insert(self, table_name: str, payload: object, *, column_names: list[str]) -> None:
        self.inserts.append(
            {
                "table_name": table_name,
                "payload": payload,
                "column_names": column_names,
            }
        )


def test_aggregate_llm_token_usage_defaults_missing_labels_and_sums_duplicates() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "team_id": "analytics",
                        "token_alias": "lab-assistant",
                        "gen_ai_original_model": "qwen3",
                        "gen_ai_token_type": "input",
                    },
                    "value": [1700007200, "100.0"],
                },
                {
                    "metric": {
                        "team_id": "analytics",
                        "token_alias": "lab-assistant",
                        "gen_ai_original_model": "qwen3",
                        "gen_ai_token_type": "input",
                    },
                    "value": [1700007200, "50.0"],
                },
                {
                    "metric": {
                        "gen_ai_token_type": "output",
                    },
                    "value": [1700007200, "25.0"],
                },
            ]
        }
    }

    rows = aggregate_llm_token_usage(payload, target_date=date(2026, 4, 21))

    assert len(rows) == 2
    assert rows[0].namespace == "analytics"
    assert rows[0].tokens_used == Decimal("150.000000")
    assert rows[1].namespace == "unknown"
    assert rows[1].token_alias == "unknown"
    assert rows[1].model == "unknown"


def test_aggregate_llm_namespace_usage_rolls_up_all_token_types() -> None:
    rows = [
        LlmTokenUsageRecord(
            date=date(2026, 4, 21),
            namespace="analytics",
            token_alias="lab-assistant",
            model="qwen3",
            token_type="input",
            tokens_used=Decimal("1200.000000"),
        ),
        LlmTokenUsageRecord(
            date=date(2026, 4, 21),
            namespace="analytics",
            token_alias="lab-assistant",
            model="qwen3",
            token_type="output",
            tokens_used=Decimal("300.000000"),
        ),
        LlmTokenUsageRecord(
            date=date(2026, 4, 21),
            namespace="analytics",
            token_alias="shared-token",
            model="gemma",
            token_type="cached_input",
            tokens_used=Decimal("100.000000"),
        ),
    ]

    namespace_rows = aggregate_llm_namespace_usage(rows)

    assert len(namespace_rows) == 1
    assert namespace_rows[0].resource == "llm"
    assert namespace_rows[0].unit == "tokens"
    assert namespace_rows[0].created_by == "llm-gateway"
    assert namespace_rows[0].usage == Decimal("1600.000000")


def test_create_llm_token_table_sql_contains_expected_schema() -> None:
    sql = create_llm_token_table_sql("accounting")

    assert "accounting.llm_token_usage_daily" in sql
    assert "token_alias LowCardinality(String)" in sql
    assert "tokens_used Decimal64(6)" in sql
    assert "ORDER BY (date, namespace, token_alias, model, token_type)" in sql


def test_delete_existing_partitions_includes_llm_table() -> None:
    client = RecordingClient()

    delete_existing_partitions(client, date(2026, 4, 21), TEST_SETTINGS)

    assert any("cluster_pod_usage_daily" in command for command in client.commands)
    assert any("cluster_namespace_usage_daily" in command for command in client.commands)
    assert any("llm_token_usage_daily" in command for command in client.commands)


def test_insert_llm_token_usage_writes_expected_columns() -> None:
    client = RecordingClient()
    rows = [
        LlmTokenUsageRecord(
            date=date(2026, 4, 21),
            namespace="analytics",
            token_alias="lab-assistant",
            model="qwen3",
            token_type="input",
            tokens_used=Decimal("1200.000000"),
        )
    ]

    inserted = insert_llm_token_usage(client, rows, TEST_SETTINGS)

    assert inserted == 1
    assert client.inserts[0]["table_name"] == "accounting.llm_token_usage_daily"
    assert client.inserts[0]["column_names"] == [
        "date",
        "namespace",
        "token_alias",
        "model",
        "token_type",
        "tokens_used",
    ]


def test_run_test_mode_includes_llm_rows(capsys) -> None:
    exit_code = run_test_mode()

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["llm_rows"]
    assert any(row["resource"] == "llm" for row in payload["namespace_rows"])
