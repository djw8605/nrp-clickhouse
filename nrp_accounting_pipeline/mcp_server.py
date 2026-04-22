from __future__ import annotations

import argparse
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from .accounting_queries import query_resource_usage as run_resource_usage_query
from .clickhouse_client import connect_clickhouse
from .config import Settings, get_settings

try:
    from mcp.server.fastmcp import Context, FastMCP
    from mcp.server.session import ServerSession
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    Context = Any  # type: ignore[assignment]
    FastMCP = None
    ServerSession = Any  # type: ignore[assignment]


@dataclass
class AppContext:
    client: Any
    settings: Settings


@asynccontextmanager
async def app_lifespan(server: Any) -> AsyncIterator[AppContext]:
    del server
    settings = get_settings()
    client = connect_clickhouse(settings)
    try:
        yield AppContext(client=client, settings=settings)
    finally:
        client.close()


def _require_fastmcp() -> Any:
    if FastMCP is None:
        raise RuntimeError(
            "The MCP server dependencies are not installed. Install requirements.txt first."
        )
    return FastMCP


_FastMCP = _require_fastmcp()
mcp = _FastMCP(
    "NRP Accounting",
    instructions=(
        "Read-only access to NRP accounting usage data stored in ClickHouse. "
        "Use query_resource_usage to filter by namespace, institution, node name, "
        "node regex, and resource."
    ),
    lifespan=app_lifespan,
    stateless_http=True,
    json_response=True,
    streamable_http_path="/",
)


@mcp.tool()
def query_resource_usage(
    ctx: Context[ServerSession, AppContext],
    start_date: str | None = None,
    end_date: str | None = None,
    namespace: str | list[str] | None = None,
    institution: str | list[str] | None = None,
    node: str | list[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | list[str] | None = None,
    resource: str | list[str] | None = None,
    granularity: str = "namespace",
    group_by: list[str] | None = None,
    limit: int = 500,
) -> dict[str, object]:
    """Query NRP accounting usage with optional namespace, institution, node, and resource filters.

    If start_date and end_date are both omitted, the tool uses the most recent ingested date.
    If only one of start_date or end_date is provided, it is treated as a single-day query.

    Supported group_by values:
    - date
    - namespace
    - institution
    - pi
    - node
    - node_institution
    - created_by
    - resource
    - unit
    - pod_name (only with granularity='pod')
    """
    app_context = ctx.request_context.lifespan_context
    return run_resource_usage_query(
        app_context.client,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        namespace=namespace,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        resource=resource,
        group_by=group_by,
        limit=limit,
        settings=app_context.settings,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NRP accounting MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="MCP transport to run",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface for streamable HTTP transport",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for streamable HTTP transport",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.transport == "streamable-http":
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        mcp.run(transport="streamable-http")
        return

    mcp.run()


if __name__ == "__main__":
    main()
