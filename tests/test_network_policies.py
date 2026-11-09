"""Keep the two halves of the ClickHouse network policies in step.

k8s/base/networkpolicy.yaml default-denies every pod of this repo and opens flows
per component; k8s/clickhouse-networkpolicies.yaml admits only listed components
into ClickHouse. A component missing from either file loses its traffic with no
error but a timeout, so these tests compare both files with the pod templates
that exist.
"""

from __future__ import annotations

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
ACCOUNTING_POLICIES = REPO / "k8s" / "base" / "networkpolicy.yaml"
CLICKHOUSE_POLICIES = REPO / "k8s" / "clickhouse-networkpolicies.yaml"
STANDALONE_JOBS = [
    REPO / "k8s" / "backfill" / "backfill-job.yaml",
    REPO / "k8s" / "pod-name-backfill" / "backfill-job.yaml",
]
COMPONENT = "app.kubernetes.io/component"
PART_OF = "app.kubernetes.io/part-of"
ACCOUNTING_NS = {"kubernetes.io/metadata.name": "access-accounting"}
CLICKHOUSE_NS = {"kubernetes.io/metadata.name": "clickhouse"}
DNS_NS = {"kubernetes.io/metadata.name": "kube-system"}
CLICKHOUSE_PODS = {"clickhouse.altinity.com/chi": "access-accounting"}


def _docs(path: Path) -> list[dict]:
    return [doc for doc in yaml.safe_load_all(path.read_text()) if doc]


def _pod_template(doc: dict) -> dict | None:
    if doc["kind"] in ("Deployment", "Job"):
        return doc["spec"]["template"]
    if doc["kind"] == "CronJob":
        return doc["spec"]["jobTemplate"]["spec"]["template"]
    return None


def _templates(paths) -> dict[str, dict]:
    templates = {}
    for path in paths:
        for doc in _docs(path):
            template = _pod_template(doc)
            if template is not None:
                labels = template.get("metadata", {}).get("labels", {})
                assert COMPONENT in labels, f"{path.name}: pod template needs a {COMPONENT} label"
                templates[labels[COMPONENT]] = labels
    return templates


def _base_templates() -> dict[str, dict]:
    return _templates(sorted((REPO / "k8s" / "base").glob("*.yaml")))


def _components(selector: dict) -> set[str]:
    if COMPONENT in selector.get("matchLabels", {}):
        return {selector["matchLabels"][COMPONENT]}
    for expr in selector.get("matchExpressions", []):
        if expr["key"] == COMPONENT and expr["operator"] == "In":
            return set(expr["values"])
    return set()


def _ports(rule: dict) -> set[int]:
    return {port["port"] for port in rule["ports"]}


def _clickhouse_ports_allowed_out() -> dict[str, set[int]]:
    allowed: dict[str, set[int]] = {}
    for policy in _docs(ACCOUNTING_POLICIES):
        components = _components(policy["spec"]["podSelector"])
        for rule in policy["spec"].get("egress", []):
            for peer in rule.get("to", []):
                if (
                    peer.get("namespaceSelector", {}).get("matchLabels") == CLICKHOUSE_NS
                    and peer.get("podSelector", {}).get("matchLabels") == CLICKHOUSE_PODS
                ):
                    for component in components:
                        allowed.setdefault(component, set()).update(_ports(rule))
    return allowed


def _clickhouse_ports_allowed_in() -> dict[str, set[int]]:
    server = next(d for d in _docs(CLICKHOUSE_POLICIES) if d["metadata"]["name"] == "clickhouse-server")
    allowed: dict[str, set[int]] = {}
    for rule in server["spec"]["ingress"]:
        for peer in rule["from"]:
            if peer.get("namespaceSelector", {}).get("matchLabels") == ACCOUNTING_NS:
                for component in _components(peer["podSelector"]):
                    allowed.setdefault(component, set()).update(_ports(rule))
    return allowed


def test_every_pod_template_is_covered_by_the_default_deny():
    kustomization = yaml.safe_load((REPO / "k8s" / "base" / "kustomization.yaml").read_text())
    assert "networkpolicy.yaml" in kustomization["resources"]
    assert any(
        entry.get("includeTemplates") and entry["pairs"].get(PART_OF) == "nrp-accounting"
        for entry in kustomization["labels"]
    ), "base pod templates must get the part-of label the policies select on"

    for component, labels in _templates(STANDALONE_JOBS).items():
        assert labels.get(PART_OF) == "nrp-accounting", component


def test_every_component_has_an_allow_policy():
    covered = set()
    for policy in _docs(ACCOUNTING_POLICIES):
        covered |= _components(policy["spec"]["podSelector"])

    workloads = set(_base_templates()) | set(_templates(STANDALONE_JOBS))
    assert workloads - covered == set(), "these pods would be denied all traffic"
    assert covered - workloads == set(), "policies name components that no longer exist"


def test_clickhouse_ingress_matches_accounting_egress():
    allowed_out = _clickhouse_ports_allowed_out()
    allowed_in = _clickhouse_ports_allowed_in()

    assert allowed_out == allowed_in
    assert {"mcp", "openapi", "etl", "xdmod-upload", "backup"} <= set(allowed_in)


def test_clickhouse_and_keeper_reach_nothing_outside_their_namespace():
    policies = {d["metadata"]["name"]: d for d in _docs(CLICKHOUSE_POLICIES)}
    assert set(policies) == {"clickhouse-server", "clickhouse-keeper"}

    for name, policy in policies.items():
        assert "Egress" in policy["spec"]["policyTypes"], name
        for rule in policy["spec"]["egress"]:
            for peer in rule["to"]:
                assert "ipBlock" not in peer, name
                namespace = peer.get("namespaceSelector", {}).get("matchLabels")
                # A peer without a namespaceSelector is in the policy's own namespace.
                if namespace is not None:
                    assert namespace == DNS_NS and _ports(rule) == {53}, name
