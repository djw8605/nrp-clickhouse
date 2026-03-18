from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Any, Mapping

from .config import Settings, get_settings


logger = logging.getLogger(__name__)

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    requests = None


def _to_unix_timestamp(value: float | int | datetime | date) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).timestamp()
        return value.timestamp()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc).timestamp()
    return float(value)


def _request_with_retries(
    *,
    endpoint: str,
    params: Mapping[str, Any],
    settings: Settings,
    timeout_seconds: float,
    retry_limit: int,
    session: Any = None,
) -> dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests is not installed. Install dependencies to query Prometheus.")

    url = f"{settings.PROMETHEUS_URL}{endpoint}"
    client = session or requests.Session()

    for attempt in range(1, retry_limit + 1):
        start_time = time.monotonic()
        try:
            response = client.get(url, params=params, timeout=timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            status = payload.get("status")
            if status != "success":
                raise RuntimeError(f"Prometheus returned status={status!r}")

            logger.info(
                "prometheus_query_complete",
                extra={
                    "endpoint": endpoint,
                    "attempt": attempt,
                    "duration_seconds": round(time.monotonic() - start_time, 3),
                    "query": params.get("query"),
                    "result_count": len(payload.get("data", {}).get("result", [])),
                },
            )
            return payload
        except Exception as exc:  # noqa: BLE001
            elapsed = round(time.monotonic() - start_time, 3)
            logger.warning(
                "prometheus_query_failed",
                extra={
                    "endpoint": endpoint,
                    "attempt": attempt,
                    "duration_seconds": elapsed,
                    "query": params.get("query"),
                    "error": str(exc),
                },
            )
            if attempt >= retry_limit:
                raise

            backoff_seconds = min(2 ** (attempt - 1), 30)
            logger.info(
                "prometheus_query_retrying",
                extra={
                    "attempt": attempt,
                    "retry_in_seconds": backoff_seconds,
                    "query": params.get("query"),
                },
            )
            time.sleep(backoff_seconds)


def query_prometheus(
    query: str,
    *,
    settings: Settings | None = None,
    timeout_seconds: float | None = None,
    retry_limit: int | None = None,
    session: Any = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    return _request_with_retries(
        endpoint="/api/v1/query",
        params={"query": query},
        settings=active_settings,
        timeout_seconds=timeout_seconds or active_settings.PROMETHEUS_TIMEOUT_SECONDS,
        retry_limit=retry_limit or active_settings.RETRY_LIMIT,
        session=session,
    )


def query_range(
    query: str,
    start_ts: float | int | datetime | date,
    end_ts: float | int | datetime | date,
    step: str | int | float,
    *,
    settings: Settings | None = None,
    timeout_seconds: float | None = None,
    retry_limit: int | None = None,
    session: Any = None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()
    return _request_with_retries(
        endpoint="/api/v1/query_range",
        params={
            "query": query,
            "start": _to_unix_timestamp(start_ts),
            "end": _to_unix_timestamp(end_ts),
            "step": step,
        },
        settings=active_settings,
        timeout_seconds=timeout_seconds or active_settings.PROMETHEUS_TIMEOUT_SECONDS,
        retry_limit=retry_limit or active_settings.RETRY_LIMIT,
        session=session,
    )


def query_range_parallel(
    queries: Mapping[str, str],
    *,
    start_ts: float | int | datetime | date,
    end_ts: float | int | datetime | date,
    step: str | int | float,
    settings: Settings | None = None,
    max_workers: int | None = None,
    timeout_seconds: float | None = None,
    retry_limit: int | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    active_settings = settings or get_settings()
    worker_count = max_workers or active_settings.MAX_QUERY_WORKERS

    results: dict[str, dict[str, Any]] = {}
    failures: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                query_range,
                query,
                start_ts,
                end_ts,
                step,
                settings=active_settings,
                timeout_seconds=timeout_seconds,
                retry_limit=retry_limit,
            ): name
            for name, query in queries.items()
        }

        for future in as_completed(future_map):
            metric_name = future_map[future]
            try:
                results[metric_name] = future.result()
            except Exception as exc:  # noqa: BLE001
                failures[metric_name] = str(exc)
                logger.error(
                    "prometheus_parallel_query_failed",
                    extra={"metric": metric_name, "error": str(exc)},
                )

    return results, failures
