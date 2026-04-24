from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .aggregation import (
    aggregate_daily_metrics,
    aggregate_llm_namespace_usage,
    aggregate_llm_token_usage,
    aggregate_namespace_usage,
)
from .config import Settings, get_settings
from .institution_import import download_and_import_institutions
from .logging_config import configure_logging
from .namespace_metadata import fetch_namespace_metadata, merge_namespace_metadata_rows
from .prometheus_client import query_prometheus


logger = logging.getLogger(__name__)


POD_RESOURCE_METRIC_NAME = "kube_pod_container_resource_requests"
POD_RESOURCE_REQUESTS_QUERY_TEMPLATE = (
    "sum_over_time(kube_pod_container_resource_requests[1d:5m]@{end_ts})"
)
POD_ANNOTATIONS_QUERY_TEMPLATE = (
    "max_over_time(kube_pod_annotations[1d:5m]@{end_ts})"
)
LLM_TOKEN_USAGE_QUERY_TEMPLATE = (
    "sum(increase(gen_ai_client_token_usage_sum[1d]@{end_ts})) "
    "by (team_id, token_alias, gen_ai_original_model, gen_ai_token_type)"
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


def _build_pod_annotation_lookup(
    annotations_payload: dict[str, Any],
) -> tuple[dict[tuple[str, str, str], str], dict[tuple[str, str], str]]:
    by_uid: dict[tuple[str, str, str], str] = {}
    by_pod: dict[tuple[str, str], str] = {}

    for series in annotations_payload.get("data", {}).get("result", []):
        labels = series.get("metric", {}) or {}
        namespace = str(labels.get("namespace") or "")
        pod = str(labels.get("pod") or "")
        uid = str(labels.get("uid") or "")
        username = str(labels.get("annotation_nrp_ai_username") or "").strip()

        if not namespace or not pod or not username:
            continue

        if uid:
            by_uid.setdefault((namespace, pod, uid), username)
        by_pod.setdefault((namespace, pod), username)

    return by_uid, by_pod


def attach_pod_annotations_to_resource_payload(
    resource_payload: dict[str, Any],
    annotations_payload: dict[str, Any],
) -> dict[str, Any]:
    annotation_by_uid, annotation_by_pod = _build_pod_annotation_lookup(annotations_payload)
    data = resource_payload.get("data", {})
    result = data.get("result", [])
    enriched_result: list[dict[str, Any]] = []

    for series in result:
        enriched_series = dict(series)
        labels = dict(series.get("metric", {}) or {})
        namespace = str(labels.get("namespace") or "")
        pod = str(labels.get("pod") or "")
        uid = str(labels.get("uid") or "")

        username = labels.get("annotation_nrp_ai_username")
        if not username and namespace and pod:
            username = annotation_by_uid.get((namespace, pod, uid)) if uid else None
            username = username or annotation_by_pod.get((namespace, pod))

        if username:
            labels["annotation_nrp_ai_username"] = str(username)

        enriched_series["metric"] = labels
        enriched_result.append(enriched_series)

    enriched_data = dict(data)
    enriched_data["result"] = enriched_result
    enriched_payload = dict(resource_payload)
    enriched_payload["data"] = enriched_data
    return enriched_payload


def _query_allocated_resources(
    *,
    end_ts: datetime,
    settings: Settings,
) -> dict[str, dict[str, Any]]:
    end_timestamp = int(end_ts.timestamp())
    resource_query = POD_RESOURCE_REQUESTS_QUERY_TEMPLATE.format(end_ts=end_timestamp)
    resource_payload = query_prometheus(
        resource_query,
        settings=settings,
        timeout_seconds=settings.PROMETHEUS_TIMEOUT_SECONDS,
        retry_limit=settings.RETRY_LIMIT,
    )

    annotations_query = POD_ANNOTATIONS_QUERY_TEMPLATE.format(end_ts=end_timestamp)
    annotations_payload = query_prometheus(
        annotations_query,
        settings=settings,
        timeout_seconds=settings.PROMETHEUS_TIMEOUT_SECONDS,
        retry_limit=settings.RETRY_LIMIT,
    )
    enriched_payload = attach_pod_annotations_to_resource_payload(
        resource_payload,
        annotations_payload,
    )
    return {POD_RESOURCE_METRIC_NAME: enriched_payload}


def _query_llm_token_usage(
    *,
    end_ts: datetime,
    settings: Settings,
) -> dict[str, dict[str, Any]]:
    query = LLM_TOKEN_USAGE_QUERY_TEMPLATE.format(end_ts=int(end_ts.timestamp()))
    payload = query_prometheus(
        query,
        settings=settings,
        timeout_seconds=settings.PROMETHEUS_TIMEOUT_SECONDS,
        retry_limit=settings.RETRY_LIMIT,
    )
    return {"gen_ai_client_token_usage_sum": payload}



def _default_mock_file() -> Path:
    return Path(__file__).resolve().parent / "testdata" / "mock_prometheus_results.json"


def _load_mock_prometheus_results(path: Path | None = None) -> dict[str, dict[str, Any]]:
    mock_path = path or _default_mock_file()
    data = json.loads(mock_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Mock data file must contain an object: {mock_path}")
    return data


def _pod_metric_payloads(results: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        metric_name: payload
        for metric_name, payload in results.items()
        if metric_name != "gen_ai_client_token_usage_sum"
    }


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
        insert_llm_token_usage,
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
        results: dict[str, dict[str, Any]] = {}
        results.update(
            _query_allocated_resources(
                end_ts=end_ts,
                settings=active_settings,
            )
        )
        results.update(
            _query_llm_token_usage(
                end_ts=end_ts,
                settings=active_settings,
            )
        )
        failed_queries: dict[str, str] = {}
        query_duration = round(time.monotonic() - query_started, 3)

        if failed_queries:
            logger.warning(
                "etl_partial_query_failures",
                extra={"failed_queries": failed_queries, "date": target_date.isoformat()},
            )

        aggregation_started = time.monotonic()
        pod_rows = aggregate_daily_metrics(_pod_metric_payloads(results), target_date=target_date)
        namespace_rows = aggregate_namespace_usage(pod_rows)
        llm_rows = aggregate_llm_token_usage(
            results.get("gen_ai_client_token_usage_sum", {}),
            target_date=target_date,
        )
        llm_namespace_rows = aggregate_llm_namespace_usage(llm_rows)
        namespace_rows = sorted(
            namespace_rows + llm_namespace_rows,
            key=lambda row: (row.date, row.namespace, row.node, row.resource, row.created_by),
        )
        aggregation_duration = round(time.monotonic() - aggregation_started, 3)

        # Keep metadata sync aligned with both rolled-up namespace usage rows and
        # the raw LLM token rows so namespaces with only LLM activity are always
        # considered observed.
        observed_namespaces = sorted(
            {row.namespace for row in namespace_rows} | {row.namespace for row in llm_rows}
        )
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
                "inserted_llm_rows": 0,
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
        inserted_llm_rows = insert_llm_token_usage(clickhouse_client, llm_rows, active_settings)

        institution_rows = download_and_import_institutions(settings=active_settings)

        logger.info(
            "etl_run_complete",
            extra={
                "date": target_date.isoformat(),
                "inserted_pod_rows": inserted_pod_rows,
                "inserted_namespace_rows": inserted_namespace_rows,
                "inserted_llm_rows": inserted_llm_rows,
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
            "inserted_llm_rows": inserted_llm_rows,
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
    pod_rows = aggregate_daily_metrics(_pod_metric_payloads(payloads), target_date=target_date)
    namespace_rows = aggregate_namespace_usage(pod_rows)
    llm_rows = aggregate_llm_token_usage(
        payloads.get("gen_ai_client_token_usage_sum", {}),
        target_date=target_date,
    )
    namespace_rows = sorted(
        namespace_rows + aggregate_llm_namespace_usage(llm_rows),
        key=lambda row: (row.date, row.namespace, row.node, row.resource, row.created_by),
    )

    result = {
        "llm_rows": [row.to_dict() for row in llm_rows],
        "pod_rows": [row.to_dict() for row in pod_rows],
        "namespace_rows": [row.to_dict() for row in namespace_rows],
    }
    print(json.dumps(result, indent=2, sort_keys=True))

    logger.info(
        "etl_test_mode_complete",
        extra={
            "date": target_date.isoformat(),
            "llm_row_count": len(llm_rows),
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
