from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
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


@dataclass(frozen=True)
class NamespaceMetadataRecord:
    namespace: str
    pi: str
    institution: str
    admins: str
    user_institutions: str
    updated_at: datetime

    @classmethod
    def unknown(
        cls,
        namespace: str,
        *,
        updated_at: datetime,
    ) -> "NamespaceMetadataRecord":
        return cls(
            namespace=namespace,
            pi="Unknown",
            institution="Unknown",
            admins="Unknown",
            user_institutions="Unknown",
            updated_at=updated_at,
        )

    def to_clickhouse_tuple(self) -> tuple[object, ...]:
        return (
            self.namespace,
            self.pi,
            self.institution,
            self.admins,
            self.user_institutions,
            self.updated_at,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "namespace": self.namespace,
            "pi": self.pi,
            "institution": self.institution,
            "admins": self.admins,
            "user_institutions": self.user_institutions,
            "updated_at": self.updated_at.isoformat(),
        }
