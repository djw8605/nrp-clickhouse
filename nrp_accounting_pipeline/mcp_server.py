from __future__ import annotations

import argparse
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from .accounting_queries import (
    get_latest_data_date as run_get_latest_data_date,
    list_active_namespaces as run_list_active_namespaces,
    get_namespace_daily_trend as run_get_namespace_daily_trend,
    get_namespace_details as run_get_namespace_details,
    get_namespace_summary as run_get_namespace_summary,
    get_usage_timeseries as run_get_usage_timeseries,
    list_filter_values as run_list_filter_values,
    query_resource_usage as run_resource_usage_query,
    top_nodes_for_namespace as run_top_nodes_for_namespace,
    top_resource_consumers as run_top_resource_consumers,
)
from .clickhouse_client import connect_clickhouse
from .config import Settings, get_settings

try:
    from mcp.server.fastmcp import Context, FastMCP
    from mcp.server.session import ServerSession
    from mcp.server.transport_security import TransportSecuritySettings
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    Context = Any  # type: ignore[assignment]
    FastMCP = None
    ServerSession = Any  # type: ignore[assignment]
    TransportSecuritySettings = Any  # type: ignore[assignment]


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


def _build_transport_security_settings(settings: Settings) -> Any:
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=settings.MCP_ENABLE_DNS_REBINDING_PROTECTION,
        allowed_hosts=settings.MCP_ALLOWED_HOSTS,
        allowed_origins=settings.MCP_ALLOWED_ORIGINS,
    )


_FastMCP = _require_fastmcp()
_server_settings = get_settings()
mcp = _FastMCP(
    "NRP Accounting",
    instructions=(
        "Read-only access to NRP accounting usage data stored in ClickHouse. "
        "Use the focused tools for common accounting questions like latest data date, "
        "active namespaces, top consumers, filter discovery, timeseries, and namespace details. "
        "Use query_resource_usage for custom aggregations."
    ),
    lifespan=app_lifespan,
    stateless_http=True,
    json_response=True,
    streamable_http_path="/",
    transport_security=_build_transport_security_settings(_server_settings),
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


@mcp.tool()
def get_latest_data_date(
    ctx: Context[ServerSession, AppContext],
    granularity: str = "namespace",
) -> dict[str, object]:
    """Get the most recent ingested accounting date."""
    app_context = ctx.request_context.lifespan_context
    return run_get_latest_data_date(
        app_context.client,
        granularity=granularity,
        settings=app_context.settings,
    )


@mcp.tool()
def list_filter_values(
    ctx: Context[ServerSession, AppContext],
    dimension: str,
    granularity: str = "namespace",
    start_date: str | None = None,
    end_date: str | None = None,
    namespace: str | list[str] | None = None,
    institution: str | list[str] | None = None,
    node: str | list[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | list[str] | None = None,
    resource: str | list[str] | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    limit: int = 100,
) -> dict[str, object]:
    """List distinct values observed in accounting usage rows for the filtered date range.

    This is not a static catalog lookup. The returned values come from actual usage rows that match
    the requested filters and date window. If start_date and end_date are omitted, the tool defaults
    to the latest ingested day. Check is_truncated and total_count when you need a complete list.
    """
    app_context = ctx.request_context.lifespan_context
    return run_list_filter_values(
        app_context.client,
        dimension=dimension,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        namespace=namespace,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        resource=resource,
        prefix=prefix,
        regex=regex,
        limit=limit,
        settings=app_context.settings,
    )


@mcp.tool()
def list_active_namespaces(
    ctx: Context[ServerSession, AppContext],
    start_date: str | None = None,
    end_date: str | None = None,
    institution: str | list[str] | None = None,
    node: str | list[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | list[str] | None = None,
    resource: str | list[str] | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    limit: int = 5000,
) -> dict[str, object]:
    """List namespaces that had observed accounting usage in a date window.

    If start_date and end_date are omitted, this tool defaults to the last 30 days ending on the
    latest ingested accounting date. Use this for prompts like "what namespaces had usage this
    month?" or "list active namespaces last week".
    """
    app_context = ctx.request_context.lifespan_context
    return run_list_active_namespaces(
        app_context.client,
        start_date=start_date,
        end_date=end_date,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        resource=resource,
        prefix=prefix,
        regex=regex,
        limit=limit,
        settings=app_context.settings,
    )


@mcp.tool()
def top_resource_consumers(
    ctx: Context[ServerSession, AppContext],
    dimension: str,
    resource: str,
    start_date: str | None = None,
    end_date: str | None = None,
    granularity: str = "namespace",
    namespace: str | list[str] | None = None,
    institution: str | list[str] | None = None,
    node: str | list[str] | None = None,
    node_regex: str | None = None,
    node_institution: str | list[str] | None = None,
    limit: int = 10,
) -> dict[str, object]:
    """Rank the top namespaces, institutions, or nodes for a specific resource."""
    app_context = ctx.request_context.lifespan_context
    return run_top_resource_consumers(
        app_context.client,
        dimension=dimension,
        resource=resource,
        start_date=start_date,
        end_date=end_date,
        granularity=granularity,
        namespace=namespace,
        institution=institution,
        node=node,
        node_regex=node_regex,
        node_institution=node_institution,
        limit=limit,
        settings=app_context.settings,
    )


@mcp.tool()
def get_usage_timeseries(
    ctx: Context[ServerSession, AppContext],
    dimension: str,
    value: str,
    resource: str,
    start_date: str | None = None,
    end_date: str | None = None,
    granularity: str = "namespace",
    limit: int = 366,
) -> dict[str, object]:
    """Get a daily usage trend for one namespace, institution, node, or node-institution value."""
    app_context = ctx.request_context.lifespan_context
    return run_get_usage_timeseries(
        app_context.client,
        dimension=dimension,
        value=value,
        resource=resource,
        start_date=start_date,
        end_date=end_date,
        granularity=granularity,
        limit=limit,
        settings=app_context.settings,
    )


@mcp.tool()
def get_namespace_summary(
    ctx: Context[ServerSession, AppContext],
    namespace: str,
    start_date: str | None = None,
    end_date: str | None = None,
    resource: str | None = None,
) -> dict[str, object]:
    """Get a namespace summary grouped by resource and unit."""
    app_context = ctx.request_context.lifespan_context
    return run_get_namespace_summary(
        app_context.client,
        namespace=namespace,
        start_date=start_date,
        end_date=end_date,
        resource=resource,
        settings=app_context.settings,
    )


@mcp.tool()
def get_namespace_daily_trend(
    ctx: Context[ServerSession, AppContext],
    namespace: str,
    resource: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, object]:
    """Get a namespace's daily trend, defaulting to the last 30 days when no dates are supplied."""
    app_context = ctx.request_context.lifespan_context
    return run_get_namespace_daily_trend(
        app_context.client,
        namespace=namespace,
        resource=resource,
        start_date=start_date,
        end_date=end_date,
        settings=app_context.settings,
    )


@mcp.tool()
def top_nodes_for_namespace(
    ctx: Context[ServerSession, AppContext],
    namespace: str,
    resource: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 10,
) -> dict[str, object]:
    """Rank the highest-usage nodes for one namespace and one resource."""
    app_context = ctx.request_context.lifespan_context
    return run_top_nodes_for_namespace(
        app_context.client,
        namespace=namespace,
        resource=resource,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        settings=app_context.settings,
    )


@mcp.tool()
def get_namespace_details(
    ctx: Context[ServerSession, AppContext],
    namespace: str,
    trend_days: int = 30,
    top_node_limit: int = 10,
    top_nodes_resource: str | None = None,
) -> dict[str, object]:
    """Get namespace metadata, latest summary, recent trend, and top nodes by resource."""
    app_context = ctx.request_context.lifespan_context
    return run_get_namespace_details(
        app_context.client,
        namespace=namespace,
        trend_days=trend_days,
        top_node_limit=top_node_limit,
        top_nodes_resource=top_nodes_resource,
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
