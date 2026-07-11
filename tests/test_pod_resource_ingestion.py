from __future__ import annotations

from datetime import date
from decimal import Decimal

from nrp_accounting_pipeline.aggregation import (
    aggregate_daily_metrics,
    aggregate_namespace_usage,
)
from nrp_accounting_pipeline.etl import (
    GPU_MODEL_QUERY_TEMPLATE,
    POD_RESOURCE_REQUESTS_QUERY_TEMPLATE,
    attach_gpu_model_names_to_resource_payload,
    attach_pod_annotations_to_resource_payload,
)


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


def test_pod_resource_requests_query_bills_only_running_pods() -> None:
    query = POD_RESOURCE_REQUESTS_QUERY_TEMPLATE.format(end_ts=1769040000)

    assert query == (
        "sum_over_time((kube_pod_container_resource_requests"
        " * on(namespace, pod) group_left()"
        ' max by(namespace, pod) (kube_pod_status_phase{phase="Running"} == 1)'
        ")[1d:5m]@1769040000)"
    )


def test_gpu_model_query_template_preserves_prometheus_label_selector() -> None:
    query = GPU_MODEL_QUERY_TEMPLATE.format(end_ts=1769040000)

    assert query == 'max_over_time(DCGM_FI_DEV_GPU_UTIL{modelName!=""}[1d:5m]@1769040000)'


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
    assert by_resource["cpu"].raw_resource == "cpu"
    assert by_resource["cpu"].gpu_model_name == "not_applicable"
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
                {
                    "metric": {
                        "namespace": "lemn-lab",
                        "pod": "hongao-job-test-82jrc",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu_mem",
                    },
                    "value": [1765584000, "80000000000"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2025, 12, 12),
    )

    assert [(row.resource, row.usage) for row in rows] == [("gpu", Decimal("0.166667"))]
    assert rows[0].raw_resource == "nvidia_com_gpu"
    assert rows[0].gpu_model_name == "unknown"


def test_aggregate_daily_metrics_skips_non_finite_prometheus_samples() -> None:
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
                    "value": [1700007200, "NaN"],
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
                    "value": [1700007200, "Infinity"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 4, 23),
    )

    assert rows == []


def test_aggregate_daily_metrics_skips_out_of_range_usage_values() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "osg-opportunistic",
                        "pod": "osg-direct-694335ec-02f064-lpg45",
                        "uid": "3ca560b1-76d6-48aa-9c31-b5415132995c",
                        "node": "gpu-node-a",
                        "resource": "cpu",
                    },
                    "value": [1777602682.048, "270215977642229760"],
                }
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 2, 21),
    )

    assert rows == []


def test_aggregate_daily_metrics_skips_implausible_cpu_request_samples() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "gp-engine-mizzou-matisziw",
                        "pod": "airflow-webserver-5d6d5cf75b-gdzdt",
                        "uid": "1350e235-1b4d-477b-ae03-8d89bb41a7d6",
                        "node": "10.244.52.130",
                        "resource": "cpu",
                    },
                    "value": [1746748800, "440234147840"],
                },
                {
                    "metric": {
                        "namespace": "gp-engine-mizzou-matisziw",
                        "pod": "airflow-redis-0",
                        "uid": "772fc955-d10e-4bc3-bb1b-080ffd8ba3f0",
                        "node": "hcc-nrp-shor-c6021.unl.edu",
                        "resource": "cpu",
                    },
                    "value": [1746748800, "1.2"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2025, 5, 8),
    )

    assert [(row.pod_name, row.usage) for row in rows] == [
        ("airflow-redis-0", Decimal("0.100000"))
    ]


def test_aggregate_daily_metrics_skips_cpu_requests_above_lowered_cap() -> None:
    # A day-long request of ~3,500 cores passed the old 1M-core cap; no pod
    # legitimately requests more than the largest node (a few hundred cores).
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "typo-cores",
                        "uid": "pod-uid-1",
                        "node": "cpu-node-a",
                        "resource": "cpu",
                    },
                    "value": [1746748800, "1000000"],
                }
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 7, 10),
    )

    assert rows == []


def test_aggregate_daily_metrics_skips_implausible_gpu_request_samples() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "typo-gpus",
                        "uid": "pod-uid-1",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                    },
                    # 1,000 GPUs held for a full day of 5m samples.
                    "value": [1746748800, "288000"],
                },
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "full-node",
                        "uid": "pod-uid-2",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                    },
                    # 8 GPUs held for a full day: plausible.
                    "value": [1746748800, "2304"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 7, 10),
    )

    assert [(row.pod_name, row.usage) for row in rows] == [
        ("full-node", Decimal("192.000000"))
    ]


def test_aggregate_daily_metrics_skips_implausible_memory_request_samples() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "typo-memory",
                        "uid": "pod-uid-1",
                        "node": "cpu-node-a",
                        "resource": "memory",
                    },
                    # ~1 PB requested for a full day of 5m samples.
                    "value": [1746748800, "288000000000000000"],
                },
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "sane-memory",
                        "uid": "pod-uid-2",
                        "node": "cpu-node-a",
                        "resource": "memory",
                    },
                    "value": [1746748800, "12000000000"],
                },
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 7, 10),
    )

    assert [(row.pod_name, row.usage) for row in rows] == [
        ("sane-memory", Decimal("1.000000"))
    ]


def test_aggregate_daily_metrics_skips_implausible_storage_request_samples() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "typo-storage",
                        "uid": "pod-uid-1",
                        "node": "cpu-node-a",
                        "resource": "ephemeral_storage",
                    },
                    "value": [1746748800, "288000000000000000"],
                }
            ]
        }
    }

    rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 7, 10),
    )

    assert rows == []


def test_attach_gpu_model_names_to_resource_payload_matches_pod_container() -> None:
    resource_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                    },
                    "value": [1700007200, "2"],
                }
            ]
        }
    }
    dcgm_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "Hostname": "gpu-node-a",
                        "modelName": "NVIDIA A100-SXM4-40GB",
                    },
                    "value": [1700007200, "0"],
                }
            ]
        }
    }

    enriched = attach_gpu_model_names_to_resource_payload(resource_payload, dcgm_payload)

    assert enriched["data"]["result"][0]["metric"]["gpu_model_name"] == "NVIDIA A100-SXM4-40GB"


def test_attach_gpu_model_names_to_resource_payload_uses_single_model_node_fallback() -> None:
    resource_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                    },
                    "value": [1700007200, "2"],
                }
            ]
        }
    }
    dcgm_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "Hostname": "gpu-node-a",
                        "modelName": "NVIDIA H100 80GB HBM3",
                    },
                    "value": [1700007200, "0"],
                }
            ]
        }
    }

    enriched = attach_gpu_model_names_to_resource_payload(resource_payload, dcgm_payload)

    assert enriched["data"]["result"][0]["metric"]["gpu_model_name"] == "NVIDIA H100 80GB HBM3"


def test_attach_gpu_model_names_to_resource_payload_marks_mixed_models() -> None:
    resource_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                    },
                    "value": [1700007200, "2"],
                }
            ]
        }
    }
    dcgm_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "Hostname": "gpu-node-a",
                        "modelName": "NVIDIA A100-SXM4-40GB",
                    },
                    "value": [1700007200, "0"],
                },
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "Hostname": "gpu-node-a",
                        "modelName": "NVIDIA H100 80GB HBM3",
                    },
                    "value": [1700007200, "0"],
                },
            ]
        }
    }

    enriched = attach_gpu_model_names_to_resource_payload(resource_payload, dcgm_payload)

    assert enriched["data"]["result"][0]["metric"]["gpu_model_name"] == "mixed"


def test_attach_gpu_model_names_to_resource_payload_uses_raw_resource_fallback() -> None:
    resource_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-0",
                        "container": "train",
                        "node": "mixed-node-a",
                        "resource": "nvidia_com_a100",
                    },
                    "value": [1700007200, "1"],
                }
            ]
        }
    }
    dcgm_payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "Hostname": "mixed-node-a",
                        "modelName": "NVIDIA A100-SXM4-40GB",
                    },
                    "value": [1700007200, "0"],
                },
                {
                    "metric": {
                        "Hostname": "mixed-node-a",
                        "modelName": "NVIDIA H100 80GB HBM3",
                    },
                    "value": [1700007200, "0"],
                },
            ]
        }
    }

    enriched = attach_gpu_model_names_to_resource_payload(resource_payload, dcgm_payload)

    assert enriched["data"]["result"][0]["metric"]["gpu_model_name"] == "a100"


def test_aggregate_namespace_usage_preserves_gpu_model_splits() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-a",
                        "uid": "pod-uid-a",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                        "gpu_model_name": "NVIDIA A100-SXM4-40GB",
                    },
                    "value": [1700007200, "1"],
                },
                {
                    "metric": {
                        "namespace": "analytics",
                        "pod": "trainer-b",
                        "uid": "pod-uid-b",
                        "node": "gpu-node-a",
                        "resource": "nvidia_com_gpu",
                        "gpu_model_name": "NVIDIA H100 80GB HBM3",
                    },
                    "value": [1700007200, "1"],
                },
            ]
        }
    }

    pod_rows = aggregate_daily_metrics(
        {"kube_pod_container_resource_requests": payload},
        target_date=date(2026, 4, 23),
    )
    namespace_rows = aggregate_namespace_usage(pod_rows)

    assert [row.gpu_model_name for row in namespace_rows] == [
        "NVIDIA A100-SXM4-40GB",
        "NVIDIA H100 80GB HBM3",
    ]
