from __future__ import annotations

from datetime import datetime, timezone

from nrp_accounting_pipeline.models import NamespaceMetadataRecord
from nrp_accounting_pipeline.namespace_metadata import merge_namespace_metadata_rows


def test_merge_namespace_metadata_rows_reuses_base_namespace_for_llm_suffix() -> None:
    updated_at = datetime(2026, 4, 23, 18, 55, 3, tzinfo=timezone.utc)
    portal_rows = [
        NamespaceMetadataRecord(
            namespace="wang-research-lab",
            pi="Chenguang Wang",
            institution="University of California, Santa Cruz",
            admins="Unknown",
            user_institutions="Unknown",
            updated_at=updated_at,
        )
    ]

    rows = merge_namespace_metadata_rows(
        portal_rows,
        observed_namespaces=["wang-research-lab-llms"],
    )

    by_namespace = {row.namespace: row for row in rows}
    assert by_namespace["wang-research-lab"].institution == "University of California, Santa Cruz"
    assert by_namespace["wang-research-lab-llms"].pi == "Chenguang Wang"
    assert by_namespace["wang-research-lab-llms"].institution == "University of California, Santa Cruz"


def test_merge_namespace_metadata_rows_keeps_unknown_for_unmapped_llm_namespace() -> None:
    rows = merge_namespace_metadata_rows(
        portal_rows=[],
        observed_namespaces=["orphan-llm-namespace-llms"],
    )

    assert len(rows) == 1
    assert rows[0].namespace == "orphan-llm-namespace-llms"
    assert rows[0].pi == "Unknown"
    assert rows[0].institution == "Unknown"
