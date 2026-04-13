from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .aggregation import aggregate_daily_metrics, aggregate_namespace_usage
from .config import Settings, get_settings
from .institution_import import download_and_import_institutions
from .logging_config import configure_logging
from .namespace_metadata import fetch_namespace_metadata, merge_namespace_metadata_rows
from .prometheus_client import query_prometheus


logger = logging.getLogger(__name__)


ALLOCATED_RESOURCES_QUERY_TEMPLATE = (
    "sum_over_time(namespace_allocated_resources[1d:5m]@{end_ts})"
)


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _default_target_date() -> date:
    # Daily jobs typically process the previous full UTC day.
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _day_bounds_utc(target_date: date) -> tuple[datetime, datetime]:
    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _query_allocated_resources(
    *,
    end_ts: datetime,
    settings: Settings,
) -> dict[str, dict[str, Any]]:
    query = ALLOCATED_RESOURCES_QUERY_TEMPLATE.format(end_ts=int(end_ts.timestamp()))
    payload = query_prometheus(
        query,
        settings=settings,
        timeout_seconds=settings.PROMETHEUS_TIMEOUT_SECONDS,
        retry_limit=settings.RETRY_LIMIT,
    )
    return {"namespace_allocated_resources": payload}



def _default_mock_file() -> Path:
    return Path(__file__).resolve().parent / "testdata" / "mock_prometheus_results.json"


def _load_mock_prometheus_results(path: Path | None = None) -> dict[str, dict[str, Any]]:
    mock_path = path or _default_mock_file()
    data = json.loads(mock_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Mock data file must contain an object: {mock_path}")
    return data


def run_for_date(
    target_date: date,
    *,
    settings: Settings | None = None,
    clickhouse_client=None,
    skip_if_exists: bool = False,
    force_reprocess: bool = False,
    namespace_metadata_seed_rows=None,
) -> dict[str, Any]:
    active_settings = settings or get_settings()

    from .clickhouse_client import (
        create_tables_if_not_exist,
        delete_existing_partitions,
        has_data_for_date,
        sync_namespace_metadata,
        insert_namespace_usage,
        insert_pod_usage,
    )

    if clickhouse_client is None:
        from .clickhouse_client import connect_clickhouse

        clickhouse_client = connect_clickhouse(active_settings)
        managed_client = True
    else:
        managed_client = False

    try:
        create_tables_if_not_exist(clickhouse_client, active_settings)

        _, end_ts = _day_bounds_utc(target_date)

        query_started = time.monotonic()
        results = _query_allocated_resources(
            end_ts=end_ts,
            settings=active_settings,
        )
        failed_queries: dict[str, str] = {}
        query_duration = round(time.monotonic() - query_started, 3)

        if failed_queries:
            logger.warning(
                "etl_partial_query_failures",
                extra={"failed_queries": failed_queries, "date": target_date.isoformat()},
            )

        aggregation_started = time.monotonic()
        pod_rows = aggregate_daily_metrics(results, target_date=target_date)
        namespace_rows = aggregate_namespace_usage(pod_rows)
        aggregation_duration = round(time.monotonic() - aggregation_started, 3)

        observed_namespaces = sorted({row.namespace for row in namespace_rows})
        portal_rows = namespace_metadata_seed_rows
        if portal_rows is None:
            portal_rows = fetch_namespace_metadata(settings=active_settings)
        namespace_metadata_rows = merge_namespace_metadata_rows(portal_rows, observed_namespaces)
        namespace_metadata_result = sync_namespace_metadata(
            clickhouse_client,
            namespace_metadata_rows,
            active_settings,
        )

        if skip_if_exists and not force_reprocess and has_data_for_date(
            clickhouse_client, target_date, active_settings
        ):
            institution_rows = download_and_import_institutions(settings=active_settings)
            logger.info(
                "etl_date_skipped_already_ingested",
                extra={
                    "date": target_date.isoformat(),
                    "namespace_metadata_inserted_rows": namespace_metadata_result["inserted"],
                    "namespace_metadata_updated_rows": namespace_metadata_result["updated"],
                    "institution_rows": institution_rows,
                },
            )
            return {
                "status": "skipped",
                "date": target_date.isoformat(),
                "inserted_pod_rows": 0,
                "inserted_namespace_rows": 0,
                "namespace_metadata_inserted_rows": namespace_metadata_result["inserted"],
                "namespace_metadata_updated_rows": namespace_metadata_result["updated"],
                "institution_rows": institution_rows,
                "failed_queries": {},
            }

        delete_existing_partitions(clickhouse_client, target_date, active_settings)
        inserted_pod_rows = insert_pod_usage(clickhouse_client, pod_rows, active_settings)
        inserted_namespace_rows = insert_namespace_usage(
            clickhouse_client, namespace_rows, active_settings
        )

        institution_rows = download_and_import_institutions(settings=active_settings)

        logger.info(
            "etl_run_complete",
            extra={
                "date": target_date.isoformat(),
                "inserted_pod_rows": inserted_pod_rows,
                "inserted_namespace_rows": inserted_namespace_rows,
                "namespace_metadata_inserted_rows": namespace_metadata_result["inserted"],
                "namespace_metadata_updated_rows": namespace_metadata_result["updated"],
                "institution_rows": institution_rows,
                "query_duration_seconds": query_duration,
                "aggregation_duration_seconds": aggregation_duration,
                "failed_query_count": len(failed_queries),
            },
        )

        return {
            "status": "success",
            "date": target_date.isoformat(),
            "inserted_pod_rows": inserted_pod_rows,
            "inserted_namespace_rows": inserted_namespace_rows,
            "namespace_metadata_inserted_rows": namespace_metadata_result["inserted"],
            "namespace_metadata_updated_rows": namespace_metadata_result["updated"],
            "institution_rows": institution_rows,
            "failed_queries": failed_queries,
        }
    finally:
        if managed_client:
            clickhouse_client.close()


def run_test_mode(mock_file: str | None = None) -> int:
    target_date = _default_target_date()
    started = time.monotonic()

    payloads = _load_mock_prometheus_results(Path(mock_file) if mock_file else None)
    pod_rows = aggregate_daily_metrics(payloads, target_date=target_date)
    namespace_rows = aggregate_namespace_usage(pod_rows)

    result = {
        "pod_rows": [row.to_dict() for row in pod_rows],
        "namespace_rows": [row.to_dict() for row in namespace_rows],
    }
    print(json.dumps(result, indent=2, sort_keys=True))

    logger.info(
        "etl_test_mode_complete",
        extra={
            "date": target_date.isoformat(),
            "pod_row_count": len(pod_rows),
            "namespace_row_count": len(namespace_rows),
            "duration_seconds": round(time.monotonic() - started, 3),
        },
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily Kubernetes accounting ETL")
    parser.add_argument(
        "--date",
        type=_parse_iso_date,
        default=None,
        help="Processing date in YYYY-MM-DD (defaults to previous UTC day)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip the date when rows already exist in ClickHouse",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess even if the date already exists",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run the full aggregation pipeline against mock Prometheus JSON",
    )
    parser.add_argument(
        "--mock-file",
        default=None,
        help="Optional path to mock Prometheus JSON used with --test",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)

    if args.test:
        return run_test_mode(args.mock_file)

    target_date = args.date or _default_target_date()
    result = run_for_date(
        target_date,
        skip_if_exists=args.skip_existing,
        force_reprocess=args.force,
    )

    if result["status"] in {"success", "skipped"}:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
