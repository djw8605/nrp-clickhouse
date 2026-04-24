from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit

from .config import Settings, get_settings
from .logging_config import configure_logging
from .schema import NAMESPACE_METADATA_TABLE_NAME, POD_TABLE_NAME, table_qualified_name


logger = logging.getLogger(__name__)

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    requests = None


@dataclass(frozen=True)
class XdmodUploadSettings:
    endpoint: str
    auth_header: str | None
    auth_value: str | None
    timeout_seconds: float
    retry_limit: int
    max_records_per_post: int
    max_bytes_per_post: int


@dataclass(frozen=True)
class XdmodUsageRecord:
    pod_uid: str
    pod_name: str
    user: str
    user_organization: str
    account: str
    record_date: date
    cpu: Decimal
    gpu: Decimal
    fpga: Decimal
    mem: Decimal
    storage: Decimal

    def to_payload(self) -> dict[str, object]:
        return {
            "PodUID": self.pod_uid,
            "PodName": self.pod_name,
            "NumberOfContainers": 1,
            "User": self.user,
            "UserOrganization": self.user_organization,
            "Account": self.account,
            "RecordStartTime": f"{self.record_date.isoformat()} 00:00:00",
            "RecordEndTime": f"{self.record_date.isoformat()} 23:59:59",
            "CPU": _json_number(self.cpu),
            "CPUType": "",
            "GPU": _json_number(self.gpu),
            "GPUType": "",
            "FPGA": _json_number(self.fpga),
            "FPGAType": "",
            "Mem": _json_number(self.mem if self.mem > 0 else Decimal("1")),
            "Storage": _json_number(self.storage if self.storage > 0 else Decimal("1")),
        }


@dataclass(frozen=True)
class XdmodUploadResult:
    date: date
    record_count: int
    post_count: int
    dry_run: bool


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


def _default_target_date() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _json_number(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _sql_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _validate_endpoint(endpoint: str) -> str:
    value = endpoint.strip()
    parsed = urlsplit(value)
    if value in {"", "replace-me"} or parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("XDMOD_ENDPOINT must be set to an http(s) endpoint before uploading")
    return value


def get_xdmod_upload_settings() -> XdmodUploadSettings:
    auth_header = os.getenv("XDMOD_AUTH_HEADER") or None
    auth_value = os.getenv("XDMOD_AUTH_VALUE") or None
    if bool(auth_header) != bool(auth_value):
        raise ValueError("Set both XDMOD_AUTH_HEADER and XDMOD_AUTH_VALUE, or leave both empty")

    return XdmodUploadSettings(
        endpoint=_validate_endpoint(os.getenv("XDMOD_ENDPOINT", "")),
        auth_header=auth_header,
        auth_value=auth_value,
        timeout_seconds=_env_float("XDMOD_UPLOAD_TIMEOUT_SECONDS", 60.0),
        retry_limit=max(1, _env_int("XDMOD_UPLOAD_RETRY_LIMIT", 3)),
        max_records_per_post=max(1, _env_int("XDMOD_MAX_RECORDS_PER_POST", 5000)),
        max_bytes_per_post=max(1, _env_int("XDMOD_MAX_BYTES_PER_POST", 5_000_000)),
    )


def build_xdmod_usage_query(target_date: date, settings: Settings) -> str:
    pod_table = table_qualified_name(settings.CLICKHOUSE_DATABASE, POD_TABLE_NAME)
    metadata_table = table_qualified_name(
        settings.CLICKHOUSE_DATABASE,
        NAMESPACE_METADATA_TABLE_NAME,
    )
    date_literal = _sql_string_literal(target_date.isoformat())

    return f"""
SELECT
    usage.date,
    usage.namespace,
    usage.created_by,
    usage.pod_uid,
    usage.pod_name,
    coalesce(nullIf(meta.institution, ''), 'Unknown') AS institution,
    sumIf(usage.usage, usage.resource = 'cpu') AS cpu,
    sumIf(usage.usage, usage.resource = 'gpu') AS gpu,
    sumIf(usage.usage, usage.resource = 'fpga') AS fpga,
    sumIf(usage.usage, usage.resource = 'memory') AS mem,
    sumIf(usage.usage, usage.resource = 'storage') AS storage
FROM {pod_table} AS usage
LEFT JOIN {metadata_table} AS meta ON usage.namespace = meta.namespace
WHERE usage.date = toDate({date_literal})
  AND usage.resource IN ('cpu', 'gpu', 'fpga', 'memory', 'storage')
GROUP BY
    usage.date,
    usage.namespace,
    usage.created_by,
    usage.pod_uid,
    usage.pod_name,
    institution
ORDER BY
    usage.namespace,
    usage.created_by,
    usage.pod_uid,
    usage.pod_name
""".strip()


def _to_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def fetch_xdmod_usage_records(
    client,
    target_date: date,
    *,
    settings: Settings | None = None,
) -> list[XdmodUsageRecord]:
    active_settings = settings or get_settings()
    query = build_xdmod_usage_query(target_date, active_settings)
    result = client.query(query)

    records = [
        XdmodUsageRecord(
            record_date=_to_date(row[0]),
            account=str(row[1] or "unknown"),
            user=str(row[2] or "unknown"),
            pod_uid=str(row[3] or ""),
            pod_name=str(row[4] or "unknown"),
            user_organization=str(row[5] or "Unknown"),
            cpu=_to_decimal(row[6]),
            gpu=_to_decimal(row[7]),
            fpga=_to_decimal(row[8]),
            mem=_to_decimal(row[9]),
            storage=_to_decimal(row[10]),
        )
        for row in result.result_rows
    ]

    logger.info(
        "xdmod_usage_records_fetched",
        extra={"date": target_date.isoformat(), "record_count": len(records)},
    )
    return records


def _serialize_payload(records: Sequence[dict[str, object]]) -> bytes:
    return json.dumps(records, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def split_payload_batches(
    records: Sequence[dict[str, object]],
    *,
    max_records_per_post: int,
    max_bytes_per_post: int,
) -> Iterable[list[dict[str, object]]]:
    batch: list[dict[str, object]] = []

    for record in records:
        record_size = len(_serialize_payload([record]))
        if record_size > max_bytes_per_post:
            raise ValueError(
                "A single XDMod record exceeds XDMOD_MAX_BYTES_PER_POST; increase the limit"
            )

        candidate = [*batch, record]
        candidate_size = len(_serialize_payload(candidate))

        if batch and (
            len(candidate) > max_records_per_post or candidate_size > max_bytes_per_post
        ):
            yield batch
            batch = [record]
        else:
            batch = candidate

    if batch:
        yield batch


def _request_headers(settings: XdmodUploadSettings) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if settings.auth_header and settings.auth_value:
        headers[settings.auth_header] = settings.auth_value
    return headers


def _post_batch(
    session,
    batch: Sequence[dict[str, object]],
    settings: XdmodUploadSettings,
) -> int:
    body = _serialize_payload(batch)
    headers = _request_headers(settings)

    for attempt in range(1, settings.retry_limit + 1):
        start_time = time.monotonic()
        try:
            response = session.post(
                settings.endpoint,
                data=body,
                headers=headers,
                timeout=settings.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "xdmod_upload_post_failed",
                extra={
                    "attempt": attempt,
                    "record_count": len(batch),
                    "payload_bytes": len(body),
                    "duration_seconds": round(time.monotonic() - start_time, 3),
                    "error": str(exc),
                },
            )
            if attempt >= settings.retry_limit:
                raise

            sleep_seconds = min(2 ** (attempt - 1), 30)
            logger.info(
                "xdmod_upload_post_retrying",
                extra={"attempt": attempt, "retry_in_seconds": sleep_seconds},
            )
            time.sleep(sleep_seconds)
            continue

        if response.status_code == 413 and len(batch) > 1:
            midpoint = len(batch) // 2
            logger.warning(
                "xdmod_upload_batch_too_large_splitting",
                extra={
                    "attempt": attempt,
                    "record_count": len(batch),
                    "payload_bytes": len(body),
                },
            )
            return _post_batch(session, batch[:midpoint], settings) + _post_batch(
                session,
                batch[midpoint:],
                settings,
            )

        try:
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "xdmod_upload_post_failed",
                extra={
                    "attempt": attempt,
                    "record_count": len(batch),
                    "payload_bytes": len(body),
                    "duration_seconds": round(time.monotonic() - start_time, 3),
                    "error": str(exc),
                },
            )
            if attempt >= settings.retry_limit:
                raise

            sleep_seconds = min(2 ** (attempt - 1), 30)
            logger.info(
                "xdmod_upload_post_retrying",
                extra={"attempt": attempt, "retry_in_seconds": sleep_seconds},
            )
            time.sleep(sleep_seconds)
            continue

        logger.info(
            "xdmod_upload_post_complete",
            extra={
                "attempt": attempt,
                "record_count": len(batch),
                "payload_bytes": len(body),
                "duration_seconds": round(time.monotonic() - start_time, 3),
                "status_code": response.status_code,
            },
        )
        return 1

    raise RuntimeError("XDMod upload unexpectedly exhausted retries")


def upload_xdmod_records(
    records: Sequence[XdmodUsageRecord],
    *,
    upload_settings: XdmodUploadSettings | None = None,
    session: Any = None,
) -> int:
    if requests is None and session is None:
        raise RuntimeError("requests is not installed. Install dependencies to upload to XDMod.")

    payload_records = [record.to_payload() for record in records]
    if not payload_records:
        logger.info("xdmod_upload_skipped_no_records")
        return 0

    active_upload_settings = upload_settings or get_xdmod_upload_settings()
    client = session or requests.Session()
    post_count = 0
    for batch in split_payload_batches(
        payload_records,
        max_records_per_post=active_upload_settings.max_records_per_post,
        max_bytes_per_post=active_upload_settings.max_bytes_per_post,
    ):
        post_count += _post_batch(client, batch, active_upload_settings)

    logger.info(
        "xdmod_upload_complete",
        extra={"record_count": len(payload_records), "post_count": post_count},
    )
    return post_count


def run_upload_for_date(
    target_date: date,
    *,
    settings: Settings | None = None,
    clickhouse_client: Any = None,
    upload_settings: XdmodUploadSettings | None = None,
    dry_run: bool = False,
    require_data: bool = False,
    session: Any = None,
) -> XdmodUploadResult:
    active_settings = settings or get_settings()

    if clickhouse_client is None:
        from .clickhouse_client import connect_clickhouse, create_tables_if_not_exist

        clickhouse_client = connect_clickhouse(active_settings)
        create_tables_if_not_exist(clickhouse_client, active_settings)
        managed_client = True
    else:
        managed_client = False

    try:
        records = fetch_xdmod_usage_records(
            clickhouse_client,
            target_date,
            settings=active_settings,
        )
    finally:
        if managed_client:
            clickhouse_client.close()

    if require_data and not records:
        raise RuntimeError(f"No ClickHouse pod usage records found for {target_date.isoformat()}")

    if dry_run:
        print(json.dumps([record.to_payload() for record in records], indent=2, sort_keys=True))
        return XdmodUploadResult(
            date=target_date,
            record_count=len(records),
            post_count=0,
            dry_run=True,
        )

    post_count = upload_xdmod_records(
        records,
        upload_settings=upload_settings,
        session=session,
    )
    return XdmodUploadResult(
        date=target_date,
        record_count=len(records),
        post_count=post_count,
        dry_run=False,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload daily NRP usage records to XDMod")
    parser.add_argument(
        "--date",
        type=_parse_iso_date,
        default=None,
        help="Usage date in YYYY-MM-DD (defaults to previous UTC day)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the XDMod payload without sending it",
    )
    parser.add_argument(
        "--require-data",
        action="store_true",
        help="Fail when ClickHouse has no pod usage records for the date",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)
    target_date = args.date or _default_target_date()

    result = run_upload_for_date(
        target_date,
        dry_run=args.dry_run,
        require_data=args.require_data,
    )
    logger.info(
        "xdmod_upload_run_complete",
        extra={
            "date": result.date.isoformat(),
            "record_count": result.record_count,
            "post_count": result.post_count,
            "dry_run": result.dry_run,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
