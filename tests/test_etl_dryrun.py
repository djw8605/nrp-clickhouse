"""
ETL dry-run integration tests against the public NRP Prometheus instance.

These tests validate:
  1. Prometheus connectivity and metric availability
  2. The annotation_nrp_ai_username label is present in live data
  3. The aggregation pipeline maps that label into the created_by field
  4. Resource normalisation, namespace roll-up, and basic row counts

No ClickHouse connection is required — all tests stop before any writes.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import requests

# Make sure the package is importable when running from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nrp_accounting_pipeline.aggregation import (
    aggregate_daily_metrics,
    aggregate_llm_token_usage,
    aggregate_namespace_usage,
    normalize_resource,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROMETHEUS_URL = os.environ.get(
    "PROMETHEUS_URL", "https://prometheus.nrp-nautilus.io"
).rstrip("/")

TIMEOUT = int(os.environ.get("PROMETHEUS_TIMEOUT_SECONDS", "60"))

# Use yesterday as the target date so the @timestamp query always resolves.
TARGET_DATE = date.today() - timedelta(days=1)
_END_TS = int(
    datetime(
        TARGET_DATE.year, TARGET_DATE.month, TARGET_DATE.day, tzinfo=timezone.utc
    ).timestamp()
    + 86400
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _instant_query(query: str) -> dict[str, Any]:
    """Run an instant Prometheus query and return the parsed JSON payload."""
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": query},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    assert payload.get("status") == "success", f"Prometheus error: {payload}"
    return payload


def _fetch_allocated_resources() -> dict[str, Any]:
    """Run the production ETL query against real Prometheus."""
    return _instant_query(f"sum_over_time(namespace_allocated_resources[1d:5m]@{_END_TS})")


def _fetch_llm_token_usage() -> dict[str, Any]:
    """Run the production LLM ETL query against real Prometheus."""
    return _instant_query(
        "sum(increase(gen_ai_client_token_usage_sum[1d]@"
        f"{_END_TS})) by (team_id, token_alias, gen_ai_original_model, gen_ai_token_type)"
    )


# Cache the live payload so we only hit Prometheus once per test session.
_live_payload: dict[str, Any] | None = None
_live_llm_payload: dict[str, Any] | None = None


def _get_live_payload() -> dict[str, Any]:
    global _live_payload
    if _live_payload is None:
        _live_payload = _fetch_allocated_resources()
    return _live_payload


def _get_live_llm_payload() -> dict[str, Any]:
    global _live_llm_payload
    if _live_llm_payload is None:
        _live_llm_payload = _fetch_llm_token_usage()
    return _live_llm_payload


# ---------------------------------------------------------------------------
# Suite 1 – Prometheus connectivity and label presence
# ---------------------------------------------------------------------------


@pytest.mark.timeout(90)
def test_prometheus_reachable():
    """Prometheus /api/v1/query endpoint responds successfully."""
    resp = requests.get(
        f"{PROMETHEUS_URL}/api/v1/query",
        params={"query": "up"},
        timeout=TIMEOUT,
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    body = resp.json()
    assert body.get("status") == "success"
    print(f"\n  [OK] Prometheus at {PROMETHEUS_URL} is reachable")


@pytest.mark.timeout(90)
def test_namespace_allocated_resources_has_data():
    """namespace_allocated_resources returns at least one time series."""
    payload = _instant_query("namespace_allocated_resources")
    series = payload["data"]["result"]
    assert len(series) > 0, "No series returned for namespace_allocated_resources"
    print(f"\n  [OK] namespace_allocated_resources: {len(series)} series found")


@pytest.mark.timeout(90)
def test_annotation_nrp_ai_username_label_exists_in_prometheus():
    """
    count by (annotation_nrp_ai_username) (namespace_allocated_resources)
    must return at least one non-empty username value.
    """
    payload = _instant_query(
        "count by (annotation_nrp_ai_username) (namespace_allocated_resources)"
    )
    series = payload["data"]["result"]
    assert len(series) > 0, "No series returned — annotation label may be missing entirely"

    usernames = [s["metric"].get("annotation_nrp_ai_username", "") for s in series]
    non_empty = [u for u in usernames if u]
    assert len(non_empty) > 0, (
        f"annotation_nrp_ai_username label exists but all values are empty. "
        f"Values seen: {usernames[:10]}"
    )
    print(
        f"\n  [OK] annotation_nrp_ai_username: {len(non_empty)} non-empty values "
        f"(sample: {non_empty[:3]})"
    )


@pytest.mark.timeout(90)
def test_etl_query_returns_data():
    """The production ETL query (sum_over_time … @timestamp) returns series."""
    payload = _get_live_payload()
    series = payload["data"]["result"]
    assert len(series) > 0, "ETL query returned no series"
    print(f"\n  [OK] ETL query returned {len(series)} series")


@pytest.mark.timeout(90)
def test_llm_etl_query_returns_data():
    """The production LLM ETL query returns at least one time series."""
    payload = _get_live_llm_payload()
    series = payload["data"]["result"]
    assert len(series) > 0, "LLM ETL query returned no series"
    print(f"\n  [OK] LLM ETL query returned {len(series)} series")


@pytest.mark.timeout(90)
def test_llm_etl_query_has_expected_labels():
    """The LLM ETL query preserves namespace, token, model, and token type labels."""
    payload = _get_live_llm_payload()
    series = payload["data"]["result"]
    first = series[0]["metric"]
    for required_label in (
        "team_id",
        "token_alias",
        "gen_ai_original_model",
        "gen_ai_token_type",
    ):
        assert first.get(required_label), f"Missing expected label {required_label!r}: {first}"
    print(f"\n  [OK] LLM ETL labels present on sample series: {first}")


@pytest.mark.timeout(90)
@pytest.mark.xfail(
    reason="annotation_nrp_ai_username is new; sum_over_time only preserves labels "
           "present across the full subquery window. Will pass once propagated.",
    strict=False,
)
def test_annotation_label_present_in_etl_query():
    """
    At least one series returned by the ETL query carries
    annotation_nrp_ai_username.  Will fail until the label has been present
    on pods for a full day so sum_over_time preserves it.
    """
    payload = _get_live_payload()
    series = payload["data"]["result"]
    annotated = [
        s for s in series
        if s.get("metric", {}).get("annotation_nrp_ai_username", "")
    ]
    assert len(annotated) > 0, (
        "No series in the ETL query result have annotation_nrp_ai_username set. "
        "Sample labels from first 3 series: "
        + str([s.get("metric", {}) for s in series[:3]])
    )
    sample = [s["metric"]["annotation_nrp_ai_username"] for s in annotated[:5]]
    print(
        f"\n  [OK] {len(annotated)}/{len(series)} series carry "
        f"annotation_nrp_ai_username (sample: {sample})"
    )


# ---------------------------------------------------------------------------
# Suite 2 – Aggregation dry-run (no ClickHouse)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def aggregated():
    """Run the full aggregation pipeline once; share the result across tests."""
    payload = _get_live_payload()
    results = {"namespace_allocated_resources": payload}
    pod_rows = aggregate_daily_metrics(results, target_date=TARGET_DATE)
    namespace_rows = aggregate_namespace_usage(pod_rows)
    return pod_rows, namespace_rows


@pytest.fixture(scope="module")
def aggregated_llm():
    """Run the LLM aggregation pipeline once; share the result across tests."""
    payload = _get_live_llm_payload()
    return aggregate_llm_token_usage(payload, target_date=TARGET_DATE)


@pytest.mark.timeout(120)
def test_aggregation_produces_pod_rows(aggregated):
    """Aggregation produces at least one PodUsageRecord."""
    pod_rows, _ = aggregated
    assert len(pod_rows) > 0, "aggregate_daily_metrics returned no rows"
    print(f"\n  [OK] {len(pod_rows)} PodUsageRecords produced")


@pytest.mark.timeout(120)
def test_aggregation_produces_namespace_rows(aggregated):
    """Aggregation produces at least one NamespaceUsageRecord."""
    _, namespace_rows = aggregated
    assert len(namespace_rows) > 0, "aggregate_namespace_usage returned no rows"
    print(f"\n  [OK] {len(namespace_rows)} NamespaceUsageRecords produced")


@pytest.mark.timeout(120)
@pytest.mark.xfail(
    reason="annotation_nrp_ai_username is new; sum_over_time only preserves labels "
           "present across the full subquery window. Will pass once propagated.",
    strict=False,
)
def test_aggregation_created_by_populated_from_annotation(aggregated):
    """
    At least one row must have created_by set to a non-'unknown' value.
    Will fail until annotation_nrp_ai_username has been present for a full day.
    """
    pod_rows, _ = aggregated
    non_unknown = [r for r in pod_rows if r.created_by != "unknown"]
    assert len(non_unknown) > 0, (
        "All PodUsageRecords have created_by='unknown'. "
        "annotation_nrp_ai_username has not yet appeared in the ETL query results."
    )
    sample = list({r.created_by for r in non_unknown})[:5]
    print(
        f"\n  [OK] {len(non_unknown)}/{len(pod_rows)} rows have a non-'unknown' "
        f"created_by (sample: {sample})"
    )


@pytest.mark.timeout(120)
@pytest.mark.xfail(
    reason="annotation_nrp_ai_username is new; sum_over_time only preserves labels "
           "present across the full subquery window. Will pass once propagated.",
    strict=False,
)
def test_aggregation_annotation_username_used_as_created_by(aggregated):
    """
    Usernames from annotation_nrp_ai_username appear verbatim in
    PodUsageRecord.created_by.  Will fail until the label has propagated.
    """
    # Collect expected usernames from the current instant query (not ETL subquery).
    payload = _instant_query(
        "count by (annotation_nrp_ai_username) (namespace_allocated_resources)"
    )
    expected_usernames = {
        s["metric"]["annotation_nrp_ai_username"]
        for s in payload["data"]["result"]
        if s.get("metric", {}).get("annotation_nrp_ai_username", "")
    }
    if not expected_usernames:
        pytest.skip("No annotation_nrp_ai_username values found in Prometheus")

    pod_rows, _ = aggregated
    found_usernames = {r.created_by for r in pod_rows}
    matched = expected_usernames & found_usernames
    assert len(matched) > 0, (
        f"None of the annotation usernames appear in PodUsageRecord.created_by.\n"
        f"  Expected (from Prometheus): {sorted(expected_usernames)[:5]}\n"
        f"  Found in rows:              {sorted(found_usernames)[:5]}"
    )
    print(
        f"\n  [OK] {len(matched)} annotation username(s) found in aggregated rows "
        f"(sample: {sorted(matched)[:3]})"
    )


@pytest.mark.timeout(120)
def test_aggregation_resource_types(aggregated):
    """Aggregated rows cover at least cpu and memory resource types."""
    pod_rows, _ = aggregated
    resources = {r.resource for r in pod_rows}
    print(f"\n  [INFO] Resource types found: {sorted(resources)}")
    for expected in ("cpu", "memory"):
        assert expected in resources, (
            f"Expected resource '{expected}' not found in aggregated rows. "
            f"Resources present: {sorted(resources)}"
        )
    print("  [OK] cpu and memory resources present")


@pytest.mark.timeout(120)
def test_aggregation_usage_values_positive(aggregated):
    """All usage values in aggregated rows must be >= 0."""
    pod_rows, _ = aggregated
    negative = [r for r in pod_rows if float(r.usage) < 0]
    assert len(negative) == 0, (
        f"{len(negative)} rows have negative usage: "
        + str([(r.namespace, r.resource, float(r.usage)) for r in negative[:5]])
    )
    print(f"\n  [OK] All {len(pod_rows)} rows have non-negative usage")


@pytest.mark.timeout(120)
def test_aggregation_namespaces_non_empty(aggregated):
    """No row should have an empty namespace field."""
    pod_rows, _ = aggregated
    empty_ns = [r for r in pod_rows if not r.namespace or r.namespace == ""]
    assert len(empty_ns) == 0, f"{len(empty_ns)} rows have an empty namespace"
    namespaces = {r.namespace for r in pod_rows}
    print(f"\n  [OK] {len(namespaces)} distinct namespaces in aggregated rows")


@pytest.mark.timeout(120)
def test_llm_aggregation_produces_rows(aggregated_llm):
    """LLM aggregation produces at least one LlmTokenUsageRecord."""
    assert len(aggregated_llm) > 0, "aggregate_llm_token_usage returned no rows"
    print(f"\n  [OK] {len(aggregated_llm)} LlmTokenUsageRecords produced")


@pytest.mark.timeout(120)
def test_llm_aggregation_preserves_common_token_types(aggregated_llm):
    """LLM aggregation preserves the common input and output token types."""
    token_types = {row.token_type for row in aggregated_llm}
    for expected in ("input", "output"):
        assert expected in token_types, (
            f"Expected token type {expected!r} not found. Present: {sorted(token_types)}"
        )
    print(f"\n  [OK] LLM token types include: {sorted(token_types)}")


# ---------------------------------------------------------------------------
# Suite 3 – Unit / normalisation smoke tests (no network)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("cpu", "cpu"),
        ("memory", "memory"),
        ("ephemeral_storage", "storage"),
        ("nvidia_com_gpu", "gpu"),
        ("amd_com_xilinx_u250", "fpga"),
        ("network_receive_bytes_total", "network"),
    ],
)
def test_normalize_resource(raw, expected):
    assert normalize_resource(raw) == expected, (
        f"normalize_resource({raw!r}) = {normalize_resource(raw)!r}, expected {expected!r}"
    )
