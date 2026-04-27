from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import date
from typing import Any, Iterable, Mapping

from .models import LlmTokenUsageRecord, NamespaceUsageRecord, PodUsageRecord, quantize_usage


logger = logging.getLogger(__name__)


BYTES_PER_GB = 1024.0**3
DECIMAL_BYTES_PER_GB = 1_000_000_000.0
SECONDS_PER_HOUR = 3600.0
DEFAULT_DAY_SECONDS = 24 * 3600.0

# The ETL subquery step size in minutes.  sum_over_time sums one sample per
# step, so dividing by SAMPLES_PER_HOUR converts the raw sum to hourly units
# (e.g. core-hours, gb-hours).
SUBQUERY_STEP_MINUTES = 5
SAMPLES_PER_HOUR = 60 / SUBQUERY_STEP_MINUTES

_RESOURCE_TO_UNIT = {
    "cpu": "cpu_core_hours",
    "gpu": "gpu_hours",
    "fpga": "fpga_hours",
    "llm": "tokens",
    "memory": "memory_gb_hours",
    "storage": "storage_gb_hours",
    "network": "network_gb",
}

LLM_RESOURCE = "llm"
LLM_CREATED_BY = "llm-gateway"
LLM_NODE = "llm-gateway"
UNKNOWN_LABEL_VALUE = "unknown"

try:
    import cityhash  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - exercised in environments without deps
    cityhash = None
    _cityhash_warning_emitted = False


def normalize_resource(resource_name: str) -> str:
    value = (resource_name or "").lower()

    if should_ignore_resource(value):
        return "other"

    if value in {"llm", "llm_tokens"}:
        return LLM_RESOURCE

    # Primary mapping for namespace_allocated_resources (matches historical JS logic).
    if "amd_com_xilinx" in value:
        return "fpga"
    if value == "cpu":
        return "cpu"
    if "nvidia_com" in value:
        return "gpu"
    if value == "memory":
        return "memory"
    if value == "ephemeral_storage":
        return "storage"

    # Compatibility fallbacks for other metric naming styles.
    if "network_receive" in value or "network_transmit" in value or "network" in value:
        return "network"
    if "gpu" in value:
        return "gpu"
    if "cpu" in value:
        return "cpu"
    if "memory" in value:
        return "memory"
    if "storage" in value or "fs_usage" in value:
        return "storage"
    return "other"


def should_ignore_resource(resource_name: str) -> bool:
    value = (resource_name or "").strip().lower()
    return value != "memory" and value.endswith("_memory")


def resource_unit(resource_name: str) -> str:
    if resource_name in _RESOURCE_TO_UNIT:
        return _RESOURCE_TO_UNIT[resource_name]
    normalized = normalize_resource(resource_name)
    return _RESOURCE_TO_UNIT.get(normalized, "other")


def _normalize_label_value(value: Any) -> str:
    if value is None:
        return UNKNOWN_LABEL_VALUE
    normalized = str(value).strip()
    return normalized or UNKNOWN_LABEL_VALUE


def _extract_node_label(labels: Mapping[str, Any]) -> str:
    node = (
        labels.get("node")
        or labels.get("kubernetes_node")
        or labels.get("node_name")
        or labels.get("kubernetes_io_hostname")
    )
    if node:
        return str(node)

    instance = labels.get("instance")
    if instance:
        instance_str = str(instance)
        if ":" in instance_str:
            return instance_str.split(":", 1)[0]
        return instance_str

    return "unknown"


def _parse_sample_value(raw_value: Any) -> float | None:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return None


def _extract_sample_points(series: Mapping[str, Any]) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []

    values = series.get("values")
    if isinstance(values, list):
        for point in values:
            if not (isinstance(point, list) and len(point) >= 2):
                continue
            timestamp = _parse_sample_value(point[0])
            value = _parse_sample_value(point[1])
            if timestamp is None or value is None:
                continue
            points.append((timestamp, value))
        return points

    value = series.get("value")
    if isinstance(value, list) and len(value) >= 2:
        timestamp = _parse_sample_value(value[0])
        parsed_value = _parse_sample_value(value[1])
        if timestamp is not None and parsed_value is not None:
            points.append((timestamp, parsed_value))

    return points


def _integrate_as_step(points: list[tuple[float, float]], fallback_seconds: float = DEFAULT_DAY_SECONDS) -> float:
    if not points:
        return 0.0
    if len(points) == 1:
        return points[0][1] * fallback_seconds

    points = sorted(points, key=lambda p: p[0])
    total = 0.0
    for idx in range(1, len(points)):
        prev_ts, prev_value = points[idx - 1]
        curr_ts, _ = points[idx]
        delta_seconds = curr_ts - prev_ts
        if delta_seconds <= 0:
            continue
        total += prev_value * delta_seconds

    return total


def _counter_delta(points: list[tuple[float, float]]) -> float:
    if not points:
        return 0.0
    if len(points) == 1:
        return max(points[0][1], 0.0)

    points = sorted(points, key=lambda p: p[0])
    delta = 0.0
    for idx in range(1, len(points)):
        prev = points[idx - 1][1]
        curr = points[idx][1]
        if curr >= prev:
            delta += curr - prev
        else:
            # Counter reset: keep the post-reset value.
            delta += max(curr, 0.0)
    return delta


def compute_pod_hash(pod_name: str) -> int:
    global _cityhash_warning_emitted
    pod = pod_name or "unknown"
    if cityhash is not None:
        return int(cityhash.CityHash64(pod))

    if not _cityhash_warning_emitted:
        logger.warning(
            "cityhash_not_installed_using_fallback_hash",
            extra={"fallback": "blake2b-64"},
        )
        _cityhash_warning_emitted = True

    digest = hashlib.blake2b(pod.encode("utf-8"), digest_size=8).digest()
    fallback_hash = int.from_bytes(digest, byteorder="little", signed=False)
    return fallback_hash


def _series_usage_for_metric(metric_name: str, resource_name: str, points: list[tuple[float, float]]) -> float:
    normalized = normalize_resource(resource_name)
    metric = metric_name.lower()

    if (
        "namespace_allocated_resources" in metric
        or "kube_pod_container_resource_requests" in metric
    ):
        raw_value = sum(value for _, value in points)
        # sum_over_time sums one sample per subquery step; divide by
        # SAMPLES_PER_HOUR to convert to hourly units (core-hours, gb-hours).
        if normalized in {"memory", "storage"}:
            return raw_value / DECIMAL_BYTES_PER_GB / SAMPLES_PER_HOUR
        return raw_value / SAMPLES_PER_HOUR

    if normalized == "cpu":
        core_seconds = _integrate_as_step(points)
        return core_seconds / SECONDS_PER_HOUR

    if normalized in {"memory", "storage"}:
        byte_seconds = _integrate_as_step(points)
        gb_hours = (byte_seconds / BYTES_PER_GB) / SECONDS_PER_HOUR
        return gb_hours

    if normalized == "network" or "network_receive" in metric or "network_transmit" in metric:
        bytes_total = _integrate_as_step(points)
        return bytes_total / BYTES_PER_GB

    if normalized in {"gpu", "fpga"}:
        gpu_seconds = _counter_delta(points)
        return gpu_seconds / SECONDS_PER_HOUR

    return sum(value for _, value in points)


def aggregate_daily_metrics(
    results: Mapping[str, Mapping[str, Any]],
    target_date: date,
) -> list[PodUsageRecord]:
    aggregated: dict[tuple[str, str, str, str, str, str, str], float] = defaultdict(float)

    for metric_name, payload in results.items():
        data = payload.get("data", {})
        series_list = data.get("result", [])

        for series in series_list:
            labels = series.get("metric", {}) or {}
            namespace = labels.get("namespace") or "unknown"
            pod_name = labels.get("pod") or labels.get("pod_name") or "unknown"
            pod_uid = str(labels.get("uid") or labels.get("pod_uid") or "")
            created_by = (
                labels.get("annotation_nrp_ai_username")
                or labels.get("created_by")
                or labels.get("created-by")
                or labels.get("createdBy")
                or "unknown"
            )
            node = _extract_node_label(labels)

            raw_resource = labels.get("resource") or metric_name
            if should_ignore_resource(raw_resource):
                logger.debug(
                    "aggregation_skip_ignored_resource",
                    extra={"metric": metric_name, "resource": raw_resource, "labels": labels},
                )
                continue

            resource = normalize_resource(raw_resource)
            unit = resource_unit(resource)
            points = _extract_sample_points(series)

            if not points:
                logger.debug(
                    "aggregation_skip_empty_series",
                    extra={"metric": metric_name, "labels": labels},
                )
                continue

            usage_value = _series_usage_for_metric(metric_name, raw_resource, points)
            key = (namespace, created_by, node, pod_name, pod_uid, resource, unit)
            aggregated[key] += usage_value

    pod_rows = [
        PodUsageRecord(
            date=target_date,
            namespace=namespace,
            created_by=created_by,
            node=node,
            pod_hash=compute_pod_hash(pod_name),
            pod_uid=pod_uid,
            pod_name=pod_name,
            resource=resource,
            usage=quantize_usage(usage),
            unit=unit,
        )
        for (
            namespace,
            created_by,
            node,
            pod_name,
            pod_uid,
            resource,
            unit,
        ), usage in aggregated.items()
    ]

    pod_rows.sort(
        key=lambda row: (
            row.date,
            row.namespace,
            row.node,
            row.resource,
            row.pod_hash,
            row.pod_uid,
            row.created_by,
            row.pod_name,
        )
    )
    return pod_rows


def aggregate_namespace_usage(pod_rows: Iterable[PodUsageRecord]) -> list[NamespaceUsageRecord]:
    namespace_totals: dict[tuple[date, str, str, str, str, str], float] = defaultdict(float)

    for row in pod_rows:
        key = (row.date, row.namespace, row.created_by, row.node, row.resource, row.unit)
        namespace_totals[key] += float(row.usage)

    namespace_rows = [
        NamespaceUsageRecord(
            date=target_date,
            namespace=namespace,
            created_by=created_by,
            node=node,
            resource=resource,
            usage=quantize_usage(usage),
            unit=unit,
        )
        for (target_date, namespace, created_by, node, resource, unit), usage in namespace_totals.items()
    ]

    namespace_rows.sort(
        key=lambda row: (row.date, row.namespace, row.node, row.resource, row.created_by)
    )
    return namespace_rows


def aggregate_llm_token_usage(
    payload: Mapping[str, Any],
    *,
    target_date: date,
) -> list[LlmTokenUsageRecord]:
    aggregated: dict[tuple[str, str, str, str], float] = defaultdict(float)
    data = payload.get("data", {})
    series_list = data.get("result", [])

    for series in series_list:
        labels = series.get("metric", {}) or {}
        namespace = _normalize_label_value(labels.get("team_id"))
        token_alias = _normalize_label_value(labels.get("token_alias"))
        model = _normalize_label_value(labels.get("gen_ai_original_model"))
        token_type = _normalize_label_value(labels.get("gen_ai_token_type"))
        points = _extract_sample_points(series)

        if not points:
            logger.debug(
                "aggregation_skip_empty_llm_series",
                extra={"labels": labels},
            )
            continue

        aggregated[(namespace, token_alias, model, token_type)] += sum(value for _, value in points)

    rows = [
        LlmTokenUsageRecord(
            date=target_date,
            namespace=namespace,
            token_alias=token_alias,
            model=model,
            token_type=token_type,
            tokens_used=quantize_usage(tokens_used),
        )
        for (namespace, token_alias, model, token_type), tokens_used in aggregated.items()
    ]
    rows.sort(
        key=lambda row: (
            row.date,
            row.namespace,
            row.token_alias,
            row.model,
            row.token_type,
        )
    )
    return rows


def aggregate_llm_namespace_usage(
    llm_rows: Iterable[LlmTokenUsageRecord],
) -> list[NamespaceUsageRecord]:
    namespace_totals: dict[tuple[date, str], float] = defaultdict(float)

    for row in llm_rows:
        namespace_totals[(row.date, row.namespace)] += float(row.tokens_used)

    rows = [
        NamespaceUsageRecord(
            date=target_date,
            namespace=namespace,
            created_by=LLM_CREATED_BY,
            node=LLM_NODE,
            resource=LLM_RESOURCE,
            usage=quantize_usage(tokens_used),
            unit=resource_unit(LLM_RESOURCE),
        )
        for (target_date, namespace), tokens_used in namespace_totals.items()
    ]
    rows.sort(
        key=lambda row: (row.date, row.namespace, row.node, row.resource, row.created_by)
    )
    return rows
