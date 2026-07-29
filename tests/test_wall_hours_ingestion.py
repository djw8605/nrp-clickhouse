from __future__ import annotations

from datetime import date

from decimal import Decimal

from nrp_accounting_pipeline.aggregation import (
    aggregate_daily_metrics,
    aggregate_namespace_usage,
    normalize_resource,
    resource_unit,
)
from nrp_accounting_pipeline.etl import (
    POD_WALL_HOURS_QUERY_TEMPLATE,
    attach_node_labels_to_payload,
)


def test_wall_hours_query_collapses_duplicate_kube_state_metrics_series() -> None:
    query = POD_WALL_HOURS_QUERY_TEMPLATE.format(end_ts=1769040000)

    assert query == (
        "sum_over_time((max by(namespace, pod, uid) "
        '(kube_pod_status_phase{phase="Running"} == 1)'
        ")[1d:5m]@1769040000)"
    )


def test_phase_metric_normalizes_to_wall_resource() -> None:
    assert normalize_resource("kube_pod_status_phase") == "wall"
    assert normalize_resource("wall") == "wall"


def test_wall_resource_is_measured_in_wall_hours() -> None:
    assert resource_unit("wall") == "wall_hours"


def _wall_payload(sum_over_time_value: str, **label_overrides: str) -> dict:
    labels = {
        "namespace": "analytics",
        "pod": "trainer-0",
        "uid": "pod-uid-1",
        "node": "gpu-node-a",
    }
    labels.update(label_overrides)
    return {
        "status": "success",
        "data": {"result": [{"metric": labels, "value": [1769040000, sum_over_time_value]}]},
    }


def test_wall_samples_are_converted_to_hours_not_left_as_sample_counts() -> None:
    # A full day of 5m samples is 285 points of value 1 at the subquery
    # boundaries; 285 / SAMPLES_PER_HOUR is 23.75 hours.
    rows = aggregate_daily_metrics(
        {"kube_pod_status_phase": _wall_payload("285")},
        date(2025, 12, 15),
    )

    assert len(rows) == 1
    assert rows[0].resource == "wall"
    assert rows[0].unit == "wall_hours"
    assert rows[0].usage == Decimal("23.750000")


def test_wall_rows_keep_the_node_joined_from_the_resource_payload() -> None:
    rows = aggregate_daily_metrics(
        {"kube_pod_status_phase": _wall_payload("72", node="cpu-node-b")},
        date(2025, 12, 15),
    )

    assert rows[0].node == "cpu-node-b"
    assert rows[0].usage == Decimal("6.000000")


def test_wall_rows_are_not_rejected_by_the_request_plausibility_guard() -> None:
    rows = aggregate_daily_metrics(
        {"kube_pod_status_phase": _wall_payload("288")},
        date(2025, 12, 15),
    )

    assert rows[0].usage == Decimal("24.000000")


def _resource_payload_for_node(node: str, **label_overrides: str) -> dict:
    labels = {
        "namespace": "analytics",
        "pod": "trainer-0",
        "uid": "pod-uid-1",
        "node": node,
        "resource": "cpu",
    }
    labels.update(label_overrides)
    return {
        "status": "success",
        "data": {"result": [{"metric": labels, "value": [1769040000, "96"]}]},
    }


def test_attach_node_labels_matches_by_uid() -> None:
    wall_payload = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "uid": "pod-uid-1",
                    },
                    "value": [1769040000, "285"],
                }
            ]
        },
    }

    enriched = attach_node_labels_to_payload(
        wall_payload,
        _resource_payload_for_node("gpu-node-a"),
    )

    assert enriched["data"]["result"][0]["metric"]["node"] == "gpu-node-a"


def test_attach_node_labels_falls_back_to_pod_name_when_uid_is_absent() -> None:
    wall_payload = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"namespace": "analytics", "pod": "trainer-0"},
                    "value": [1769040000, "285"],
                }
            ]
        },
    }

    enriched = attach_node_labels_to_payload(
        wall_payload,
        _resource_payload_for_node("gpu-node-a"),
    )

    assert enriched["data"]["result"][0]["metric"]["node"] == "gpu-node-a"


def test_attach_node_labels_never_falls_back_to_the_kube_state_metrics_pod_ip() -> None:
    # kube_pod_status_phase has no node label and its instance label is the KSM
    # pod IP.  Leaving node unset would make _extract_node_label record
    # "10.244.27.150" as the node -- the defect behind 10.244.x.x "nodes".
    wall_payload = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "pod-with-no-requests",
                        "uid": "pod-uid-9",
                        "instance": "10.244.27.150:8443",
                    },
                    "value": [1769040000, "285"],
                }
            ]
        },
    }

    enriched = attach_node_labels_to_payload(
        wall_payload,
        _resource_payload_for_node("gpu-node-a"),
    )

    assert enriched["data"]["result"][0]["metric"]["node"] == "unknown"

    rows = aggregate_daily_metrics({"kube_pod_status_phase": enriched}, date(2025, 12, 15))
    assert rows[0].node == "unknown"


def test_wall_hours_roll_up_into_namespace_rows() -> None:
    pod_rows = aggregate_daily_metrics(
        {"kube_pod_status_phase": _wall_payload("285")},
        date(2025, 12, 15),
    )

    namespace_rows = aggregate_namespace_usage(pod_rows)

    wall_rows = [row for row in namespace_rows if row.resource == "wall"]
    assert len(wall_rows) == 1
    assert wall_rows[0].namespace == "analytics"
    assert wall_rows[0].unit == "wall_hours"
    assert wall_rows[0].usage == Decimal("23.750000")
