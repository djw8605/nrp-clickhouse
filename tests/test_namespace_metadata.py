from __future__ import annotations

from datetime import datetime, timezone

from nrp_accounting_pipeline import namespace_metadata as namespace_metadata_module
from nrp_accounting_pipeline.config import Settings
from nrp_accounting_pipeline.models import NamespaceMetadataRecord
from nrp_accounting_pipeline.namespace_metadata import fetch_namespace_metadata, merge_namespace_metadata_rows


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


class FakePortalResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {
            "result": {
                "Namespaces": [
                    {
                        "Name": "commercial-ns",
                        "PI": "Dr Example",
                        "Institution": "Delta University",
                        "Admins": ["admin1", "admin2"],
                        "UserInstitutions": ["Delta University"],
                        "commercial": True,
                    }
                ]
            }
        }


class FakePortalSession:
    def post(self, *args, **kwargs) -> FakePortalResponse:
        return FakePortalResponse()


def test_fetch_namespace_metadata_parses_commercial_field(monkeypatch) -> None:
    monkeypatch.setattr(namespace_metadata_module, "requests", object())

    rows = fetch_namespace_metadata(
        settings=TEST_SETTINGS,
        session=FakePortalSession(),
    )

    assert len(rows) == 1
    assert rows[0].namespace == "commercial-ns"
    assert rows[0].commercial is True


def test_merge_namespace_metadata_rows_reuses_base_namespace_for_llm_suffix() -> None:
    updated_at = datetime(2026, 4, 23, 18, 55, 3, tzinfo=timezone.utc)
    portal_rows = [
        NamespaceMetadataRecord(
            namespace="wang-research-lab",
            pi="Chenguang Wang",
            institution="University of California, Santa Cruz",
            admins="Unknown",
            user_institutions="Unknown",
            updated_at=updated_at,
            commercial=True,
        )
    ]

    rows = merge_namespace_metadata_rows(
        portal_rows,
        observed_namespaces=["wang-research-lab-llms"],
    )

    by_namespace = {row.namespace: row for row in rows}
    assert by_namespace["wang-research-lab"].institution == "University of California, Santa Cruz"
    assert by_namespace["wang-research-lab-llms"].pi == "Chenguang Wang"
    assert by_namespace["wang-research-lab-llms"].institution == "University of California, Santa Cruz"
    assert by_namespace["wang-research-lab-llms"].commercial is True


def test_merge_namespace_metadata_rows_keeps_unknown_for_unmapped_llm_namespace() -> None:
    rows = merge_namespace_metadata_rows(
        portal_rows=[],
        observed_namespaces=["orphan-llm-namespace-llms"],
    )

    assert len(rows) == 1
    assert rows[0].namespace == "orphan-llm-namespace-llms"
    assert rows[0].pi == "Unknown"
    assert rows[0].institution == "Unknown"
    assert rows[0].commercial is False
