from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from .config import Settings, get_settings
from .models import NamespaceMetadataRecord


logger = logging.getLogger(__name__)


UNKNOWN_VALUE = "Unknown"
LIST_NS_INFO_METHOD = "guest.ListNsInfo"


try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    requests = None


def _normalize_scalar(value: object) -> str:
    if value is None:
        return UNKNOWN_VALUE
    text = str(value).strip()
    return text or UNKNOWN_VALUE


def _normalize_joined(value: object) -> str:
    if isinstance(value, (list, tuple, set)):
        normalized = [str(item).strip() for item in value if str(item).strip()]
        if normalized:
            return ",".join(normalized)
        return UNKNOWN_VALUE
    return _normalize_scalar(value)


def _extract_namespace_rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if payload.get("error"):
        raise RuntimeError(f"portal JSON-RPC returned an error: {payload['error']}")

    result = payload.get("result", payload)
    if not isinstance(result, Mapping):
        raise RuntimeError("portal JSON-RPC response did not contain an object result")

    namespaces = result.get("Namespaces")
    if not isinstance(namespaces, list):
        raise RuntimeError("portal JSON-RPC response did not contain a Namespaces list")

    rows: list[Mapping[str, Any]] = []
    for raw_row in namespaces:
        if isinstance(raw_row, Mapping):
            rows.append(raw_row)
    return rows


def _request_with_retries(
    *,
    settings: Settings,
    timeout_seconds: float,
    retry_limit: int,
    session: Any = None,
) -> Mapping[str, Any]:
    if requests is None:
        raise RuntimeError("requests is not installed. Install dependencies to query portal.nrp.ai.")

    client = session or requests.Session()
    request_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": LIST_NS_INFO_METHOD,
        "params": [],
    }

    for attempt in range(1, retry_limit + 1):
        start_time = time.monotonic()
        try:
            response = client.post(
                settings.PORTAL_RPC_URL,
                json=request_body,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            namespace_rows = _extract_namespace_rows(payload)

            logger.info(
                "portal_namespace_query_complete",
                extra={
                    "attempt": attempt,
                    "duration_seconds": round(time.monotonic() - start_time, 3),
                    "namespace_count": len(namespace_rows),
                    "url": settings.PORTAL_RPC_URL,
                },
            )
            return payload
        except Exception as exc:  # noqa: BLE001
            elapsed = round(time.monotonic() - start_time, 3)
            logger.warning(
                "portal_namespace_query_failed",
                extra={
                    "attempt": attempt,
                    "duration_seconds": elapsed,
                    "error": str(exc),
                    "url": settings.PORTAL_RPC_URL,
                },
            )
            if attempt >= retry_limit:
                raise

            backoff_seconds = min(2 ** (attempt - 1), 30)
            logger.info(
                "portal_namespace_query_retrying",
                extra={
                    "attempt": attempt,
                    "retry_in_seconds": backoff_seconds,
                    "url": settings.PORTAL_RPC_URL,
                },
            )
            time.sleep(backoff_seconds)

    raise RuntimeError("portal namespace query unexpectedly exhausted retries")


def fetch_namespace_metadata(
    *,
    settings: Settings | None = None,
    timeout_seconds: float | None = None,
    retry_limit: int | None = None,
    session: Any = None,
) -> list[NamespaceMetadataRecord]:
    active_settings = settings or get_settings()
    payload = _request_with_retries(
        settings=active_settings,
        timeout_seconds=timeout_seconds or active_settings.PORTAL_TIMEOUT_SECONDS,
        retry_limit=retry_limit or active_settings.RETRY_LIMIT,
        session=session,
    )

    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    rows_by_namespace: dict[str, NamespaceMetadataRecord] = {}

    for row in _extract_namespace_rows(payload):
        namespace = _normalize_scalar(row.get("Name"))
        if namespace == UNKNOWN_VALUE:
            continue

        rows_by_namespace[namespace] = NamespaceMetadataRecord(
            namespace=namespace,
            pi=_normalize_scalar(row.get("PI")),
            institution=_normalize_scalar(row.get("Institution")),
            admins=_normalize_joined(row.get("Admins")),
            user_institutions=_normalize_joined(row.get("UserInstitutions")),
            updated_at=updated_at,
        )

    parsed_rows = [rows_by_namespace[name] for name in sorted(rows_by_namespace)]
    logger.info("portal_namespace_metadata_parsed", extra={"row_count": len(parsed_rows)})
    return parsed_rows


def merge_namespace_metadata_rows(
    portal_rows: Sequence[NamespaceMetadataRecord],
    observed_namespaces: Sequence[str],
) -> list[NamespaceMetadataRecord]:
    merged_rows = {row.namespace: row for row in portal_rows}
    updated_at = datetime.now(timezone.utc).replace(microsecond=0)
    if portal_rows:
        updated_at = max(row.updated_at for row in portal_rows)

    for raw_namespace in observed_namespaces:
        namespace = raw_namespace.strip()
        if not namespace or namespace in merged_rows:
            continue

        # Some LLM accounting namespaces are derived from a base portal namespace,
        # for example `wang-research-lab-llms` in metrics vs `wang-research-lab`
        # in portal metadata. Reuse the base namespace metadata when available.
        alias_source = None
        if namespace.endswith("-llms"):
            alias_source = merged_rows.get(namespace[: -len("-llms")])

        if alias_source is not None:
            merged_rows[namespace] = NamespaceMetadataRecord(
                namespace=namespace,
                pi=alias_source.pi,
                institution=alias_source.institution,
                admins=alias_source.admins,
                user_institutions=alias_source.user_institutions,
                updated_at=alias_source.updated_at,
            )
            continue

        merged_rows[namespace] = NamespaceMetadataRecord.unknown(
            namespace,
            updated_at=updated_at,
        )

    rows = [merged_rows[name] for name in sorted(merged_rows)]
    logger.info(
        "portal_namespace_metadata_merged",
        extra={
            "portal_row_count": len(portal_rows),
            "observed_namespace_count": len({name.strip() for name in observed_namespaces if name.strip()}),
            "merged_row_count": len(rows),
        },
    )
    return rows
