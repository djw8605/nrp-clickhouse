from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

from .clickhouse_client import (
    connect_clickhouse,
    create_tables_if_not_exist,
    replace_node_institution_mapping,
)
from .config import Settings, get_settings
from .logging_config import configure_logging
from .models import NodeInstitutionRecord


logger = logging.getLogger(__name__)


OSG_IDENTIFIER_COLUMN = "OSG Identifier"
INSTITUTION_NAME_COLUMN = "Institution Name"


def parse_institution_csv(csv_path: Path) -> list[NodeInstitutionRecord]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        field_names = set(reader.fieldnames or [])

        required = {OSG_IDENTIFIER_COLUMN, INSTITUTION_NAME_COLUMN}
        missing = required - field_names
        if missing:
            raise ValueError(
                f"CSV is missing required column(s): {', '.join(sorted(missing))}"
            )

        row_map: dict[str, str] = {}
        for row in reader:
            node = (row.get(OSG_IDENTIFIER_COLUMN) or "").strip()
            institution_name = (row.get(INSTITUTION_NAME_COLUMN) or "").strip()
            if not node or not institution_name:
                continue
            row_map[node] = institution_name

    parsed = [
        NodeInstitutionRecord(node=node, institution_name=institution)
        for node, institution in sorted(row_map.items())
    ]

    logger.info(
        "institution_csv_parsed",
        extra={"csv_path": str(csv_path), "row_count": len(parsed)},
    )
    return parsed


def run_import(csv_path: Path, settings: Settings | None = None) -> int:
    active_settings = settings or get_settings()
    rows = parse_institution_csv(csv_path)

    client = connect_clickhouse(active_settings)
    try:
        create_tables_if_not_exist(client, active_settings)
        inserted = replace_node_institution_mapping(client, rows, active_settings)
    finally:
        client.close()

    logger.info(
        "institution_import_complete",
        extra={
            "csv_path": str(csv_path),
            "inserted_rows": inserted,
            "database": active_settings.CLICKHOUSE_DATABASE,
        },
    )
    return inserted


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import node-to-institution mappings from CSV into ClickHouse"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help='Path to CSV file with "OSG Identifier" and "Institution Name" columns',
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV file does not exist: {csv_path}")

    run_import(csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
