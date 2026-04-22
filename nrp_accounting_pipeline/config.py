from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlsplit


default_prometheus_url = "http://localhost:9090"
default_clickhouse_host = "localhost"
default_clickhouse_database = "default"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_csv(name: str, default: tuple[str, ...] = ()) -> list[str]:
    raw = os.getenv(name)
    if raw in (None, ""):
        return list(default)

    values: list[str] = []
    for part in raw.split(","):
        value = part.strip()
        if value:
            values.append(value)
    return values


def _parse_clickhouse_host_and_port(raw_host: str, fallback_port: int) -> tuple[str, int]:
    host_value = (raw_host or default_clickhouse_host).strip()
    if not host_value:
        return default_clickhouse_host, fallback_port

    parsed_input = host_value if "://" in host_value else f"//{host_value}"
    parsed = urlsplit(parsed_input)

    parsed_host = parsed.hostname or host_value
    parsed_port = None
    try:
        parsed_port = parsed.port
    except ValueError:
        parsed_port = None

    return parsed_host, parsed_port or fallback_port


@dataclass(frozen=True)
class Settings:
    PROMETHEUS_URL: str
    PORTAL_RPC_URL: str
    CLICKHOUSE_HOST: str
    CLICKHOUSE_USER: str
    CLICKHOUSE_PASSWORD: str
    CLICKHOUSE_DATABASE: str
    MAX_QUERY_WORKERS: int
    QUERY_STEP: str
    RETRY_LIMIT: int
    CLICKHOUSE_PORT: int
    CLICKHOUSE_SECURE: bool
    PROMETHEUS_TIMEOUT_SECONDS: float
    PORTAL_TIMEOUT_SECONDS: float
    CLICKHOUSE_WRITE_BATCH_SIZE: int
    INSTITUTION_CSV_URL: str | None
    MCP_ENABLE_DNS_REBINDING_PROTECTION: bool
    MCP_ALLOWED_HOSTS: list[str]
    MCP_ALLOWED_ORIGINS: list[str]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    clickhouse_host, clickhouse_port = _parse_clickhouse_host_and_port(
        os.getenv("CLICKHOUSE_HOST", default_clickhouse_host),
        _env_int("CLICKHOUSE_PORT", 8123),
    )

    return Settings(
        PROMETHEUS_URL=os.getenv("PROMETHEUS_URL", default_prometheus_url).rstrip("/"),
        PORTAL_RPC_URL=os.getenv("PORTAL_RPC_URL", "https://portal.nrp.ai/rpc").rstrip("/"),
        CLICKHOUSE_HOST=clickhouse_host,
        CLICKHOUSE_USER=os.getenv("CLICKHOUSE_USER", "default"),
        CLICKHOUSE_PASSWORD=os.getenv("CLICKHOUSE_PASSWORD", ""),
        CLICKHOUSE_DATABASE=os.getenv("CLICKHOUSE_DATABASE", default_clickhouse_database),
        MAX_QUERY_WORKERS=_env_int("MAX_QUERY_WORKERS", 5),
        QUERY_STEP=os.getenv("QUERY_STEP", "1h"),
        RETRY_LIMIT=_env_int("RETRY_LIMIT", 3),
        CLICKHOUSE_PORT=clickhouse_port,
        CLICKHOUSE_SECURE=_env_bool("CLICKHOUSE_SECURE", False),
        PROMETHEUS_TIMEOUT_SECONDS=_env_float("PROMETHEUS_TIMEOUT_SECONDS", 60.0),
        PORTAL_TIMEOUT_SECONDS=_env_float("PORTAL_TIMEOUT_SECONDS", 60.0),
        CLICKHOUSE_WRITE_BATCH_SIZE=_env_int("CLICKHOUSE_WRITE_BATCH_SIZE", 5000),
        INSTITUTION_CSV_URL=os.getenv("INSTITUTION_CSV_URL") or None,
        MCP_ENABLE_DNS_REBINDING_PROTECTION=_env_bool(
            "MCP_ENABLE_DNS_REBINDING_PROTECTION",
            True,
        ),
        MCP_ALLOWED_HOSTS=_env_csv(
            "MCP_ALLOWED_HOSTS",
            default=(
                "127.0.0.1:*",
                "localhost:*",
                "nrp-accounting-mcp.nrp-nautilus.io",
                "nrp-accounting-mcp.nrp-nautilus.io:*",
            ),
        ),
        MCP_ALLOWED_ORIGINS=_env_csv(
            "MCP_ALLOWED_ORIGINS",
            default=(
                "http://127.0.0.1:*",
                "http://localhost:*",
                "https://nrp-accounting-mcp.nrp-nautilus.io",
            ),
        ),
    )
