from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP


DECIMAL_SCALE = Decimal("0.000001")


def quantize_usage(value: float | Decimal) -> Decimal:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return decimal_value.quantize(DECIMAL_SCALE, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class PodUsageRecord:
    date: date
    namespace: str
    created_by: str
    node: str
    pod_hash: int
    pod_name: str
    resource: str
    usage: Decimal
    unit: str

    def to_clickhouse_tuple(self) -> tuple[object, ...]:
        return (
            self.date,
            self.namespace,
            self.created_by,
            self.node,
            self.pod_hash,
            self.pod_name,
            self.resource,
            self.usage,
            self.unit,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "date": self.date.isoformat(),
            "namespace": self.namespace,
            "created_by": self.created_by,
            "node": self.node,
            "pod_hash": self.pod_hash,
            "pod_name": self.pod_name,
            "resource": self.resource,
            "usage": float(self.usage),
            "unit": self.unit,
        }


@dataclass(frozen=True)
class NamespaceUsageRecord:
    date: date
    namespace: str
    created_by: str
    node: str
    resource: str
    usage: Decimal
    unit: str

    def to_clickhouse_tuple(self) -> tuple[object, ...]:
        return (
            self.date,
            self.namespace,
            self.created_by,
            self.node,
            self.resource,
            self.usage,
            self.unit,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "date": self.date.isoformat(),
            "namespace": self.namespace,
            "created_by": self.created_by,
            "node": self.node,
            "resource": self.resource,
            "usage": float(self.usage),
            "unit": self.unit,
        }


@dataclass(frozen=True)
class NodeInstitutionRecord:
    node: str
    institution_name: str

    def to_clickhouse_tuple(self) -> tuple[object, ...]:
        return (self.node, self.institution_name)

    def to_dict(self) -> dict[str, object]:
        return {"node": self.node, "institution_name": self.institution_name}
