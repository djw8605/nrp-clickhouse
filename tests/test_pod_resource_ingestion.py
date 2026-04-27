from __future__ import annotations

from datetime import date
from decimal import Decimal

from nrp_accounting_pipeline.aggregation import aggregate_daily_metrics
from nrp_accounting_pipeline.etl import attach_pod_annotations_to_resource_payload


def test_attach_pod_annotations_to_resource_payload_matches_by_uid() -> None:
    resource_payload = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "cpu",
                    },
                    "value": [1700007200, "24"],
                }
            ]
        },
    }
    annotations_payload = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "uid": "pod-uid-1",
                        "annotation_nrp_ai_username": "jane.doe",
                    },
                    "value": [1700007200, "1"],
                }
            ]
        },
    }

    enriched = attach_pod_annotations_to_resource_payload(
        resource_payload,
        annotations_payload,
    )

    metric = enriched["data"]["result"][0]["metric"]
    assert metric["pod"] == "trainer-0"
    assert metric["annotation_nrp_ai_username"] == "jane.doe"


def test_aggregate_daily_metrics_uses_pod_level_resource_requests() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "cpu",
                        "annotation_nrp_ai_username": "jane.doe",
                    },
                    "value": [1700007200, "24"],
                },
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "memory",
                        "annotation_nrp_ai_username": "jane.doe",
                    },
                    "value": [1700007200, "12000000000"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 4, 23),
    )

    by_resource = {row.resource: row for row in rows}
    assert by_resource["cpu"].pod_name == "trainer-0"
    assert by_resource["cpu"].pod_uid == "pod-uid-1"
    assert by_resource["cpu"].created_by == "jane.doe"
    assert by_resource["cpu"].usage == Decimal("2.000000")
    assert by_resource["memory"].usage == Decimal("1.000000")


def test_aggregate_daily_metrics_ignores_extended_gpu_memory_resources() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "lemn-lab",
                        "pod": "hongao-job-test-82jrc",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                    },
                    "value": [1765584000, "2"],
                },
                {
                    "metric": {
                        "namespace": "lemn-lab",
                        "pod": "hongao-job-test-82jrc",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu_memory",
                    },
                    "value": [1765584000, "68719476736"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2025, 12, 12),
    )

    assert [(row.resource, row.usage) for row in rows] == [("gpu", Decimal("0.166667"))]
