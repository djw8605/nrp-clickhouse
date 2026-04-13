from __future__ import annotations

import argparse
import csv
import logging
import tempfile
import time
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

try:
    import requests
except ModuleNotFoundError:
    requests = None


def download_institution_csv(
    url: str,
    *,
    settings: Settings | None = None,
    timeout_seconds: float = 60.0,
    retry_limit: int | None = None,
) -> Path:
    if requests is None:
        raise RuntimeError("requests is not installed. Install dependencies to download institution CSV.")

    active_settings = settings or get_settings()
    active_retry_limit = retry_limit or active_settings.RETRY_LIMIT
    client = requests.Session()

    for attempt in range(1, active_retry_limit + 1):
        start_time = time.monotonic()
        try:
            response = client.get(url, timeout=timeout_seconds)
            response.raise_for_status()
            csv_content = response.text

            elapsed = round(time.monotonic() - start_time, 3)
            logger.info(
                "institution_csv_download_complete",
                extra={
                    "attempt": attempt,
                    "duration_seconds": elapsed,
                    "url": url,
                    "content_length": len(csv_content),
                },
            )

            temp_file = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".csv",
                delete=False,
                encoding="utf-8",
            )
            temp_file.write(csv_content)
            temp_file.close()
            return Path(temp_file.name)
        except Exception as exc:  # noqa: BLE001
            elapsed = round(time.monotonic() - start_time, 3)
            logger.warning(
                "institution_csv_download_failed",
                extra={
                    "attempt": attempt,
                    "duration_seconds": elapsed,
                    "error": str(exc),
                    "url": url,
                },
            )
            if attempt >= active_retry_limit:
                raise

            backoff_seconds = min(2 ** (attempt - 1), 30)
            logger.info(
                "institution_csv_download_retrying",
                extra={
                    "attempt": attempt,
                    "retry_in_seconds": backoff_seconds,
                    "url": url,
                },
            )
            time.sleep(backoff_seconds)

    raise RuntimeError("institution CSV download unexpectedly exhausted retries")


def download_and_import_institutions(
    *,
    settings: Settings | None = None,
) -> int:
    active_settings = settings or get_settings()

    if not active_settings.INSTITUTION_CSV_URL:
        logger.info("institution_csv_url_not_configured")
        return 0

    csv_path = download_institution_csv(
        active_settings.INSTITUTION_CSV_URL,
        settings=active_settings,
    )

    try:
        return run_import(csv_path, settings=active_settings)
    finally:
        csv_path.unlink(missing_ok=True)


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
