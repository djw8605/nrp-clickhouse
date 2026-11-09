"""The MCP server, OpenAPI bridge and XDMoD upload log in as a readonly=1 user.

ClickHouse rejects every DDL statement from such a user (Code 164, READONLY), even
CREATE ... IF NOT EXISTS, so these read paths must not run the schema bootstrap the
ETL does. A fake clickhouse_connect records each statement that would reach the server.
"""

from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from nrp_accounting_pipeline import clickhouse_client, mcp_server, xdmod_upload
from nrp_accounting_pipeline.schema import create_database_sql
from tests.test_xdmod_upload import TEST_SETTINGS

REPO = Path(__file__).resolve().parents[1]


class FakeClient:
    def __init__(self, log: list[tuple]) -> None:
        self.log = log

    def command(self, sql, *args, **kwargs):
        self.log.append(("command", sql))

    def query(self, sql, *args, **kwargs):
        self.log.append(("query", sql))
        return SimpleNamespace(result_rows=[])

    def close(self) -> None:
        pass


@pytest.fixture
def server_log(monkeypatch) -> list[tuple]:
    log: list[tuple] = []

    def get_client(**kwargs):
        log.append(("connect", kwargs["database"]))
        return FakeClient(log)

    monkeypatch.setattr(clickhouse_client, "clickhouse_connect", SimpleNamespace(get_client=get_client))
    return log


def _commands(log: list[tuple]) -> list[tuple]:
    return [entry for entry in log if entry[0] == "command"]


def test_mcp_server_connects_without_running_ddl(server_log, monkeypatch) -> None:
    monkeypatch.setattr(mcp_server, "get_settings", lambda: TEST_SETTINGS)

    async def open_and_close() -> None:
        async with mcp_server.app_lifespan(None):
            pass

    asyncio.run(open_and_close())

    assert _commands(server_log) == []
    assert server_log == [("connect", TEST_SETTINGS.CLICKHOUSE_DATABASE)]


def test_xdmod_upload_reads_without_running_ddl(server_log) -> None:
    xdmod_upload.run_upload_for_date(date(2025, 12, 15), settings=TEST_SETTINGS, dry_run=True)

    assert _commands(server_log) == []
    assert [entry[0] for entry in server_log] == ["connect", "query"]


def test_etl_connection_still_creates_the_database(server_log) -> None:
    clickhouse_client.connect_clickhouse(TEST_SETTINGS)

    assert _commands(server_log) == [("command", create_database_sql(TEST_SETTINGS.CLICKHOUSE_DATABASE))]


def test_only_the_read_paths_log_in_as_the_readonly_user() -> None:
    # Dropping the env override would not break the readers: they would quietly fall
    # back to the writer user from nrp-accounting-secrets. Check the manifests.
    users = {}
    for path in sorted((REPO / "k8s" / "base").glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if not doc or doc["kind"] not in ("Deployment", "CronJob"):
                continue
            spec = doc["spec"]
            template = spec["jobTemplate"]["spec"]["template"] if doc["kind"] == "CronJob" else spec["template"]
            component = template.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component")
            env = {e["name"]: e for c in template["spec"]["containers"] for e in c.get("env", [])}
            users[component] = env.get("CLICKHOUSE_USER", {}).get("value")
            if users[component] == "accounting_ro":
                assert env["CLICKHOUSE_PASSWORD"]["valueFrom"]["secretKeyRef"]["name"] == "clickhouse-readonly-credentials"

    assert users == {
        "mcp": "accounting_ro",
        "openapi": "accounting_ro",
        "xdmod-upload": "accounting_ro",
        "etl": None,
        "backup": None,
    }
