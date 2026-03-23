from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from .config import get_settings
from .etl import run_for_date
from .logging_config import configure_logging
from .namespace_metadata import fetch_namespace_metadata


logger = logging.getLogger(__name__)


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def _date_range(start_date: date, end_date: date):
    cursor = start_date
    while cursor <= end_date:
        yield cursor
        cursor += timedelta(days=1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run historical backfill for accounting ETL")
    parser.add_argument("--start", required=True, type=_parse_iso_date, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=_parse_iso_date, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess dates even if already ingested",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)

    if args.end < args.start:
        raise SystemExit("--end must be on or after --start")

    settings = get_settings()

    from .clickhouse_client import connect_clickhouse, create_tables_if_not_exist

    client = connect_clickhouse(settings)
    create_tables_if_not_exist(client, settings)
    namespace_metadata_seed_rows = fetch_namespace_metadata(settings=settings)

    processed = 0
    skipped = 0
    failures: list[tuple[str, str]] = []

    try:
        for target_date in _date_range(args.start, args.end):
            try:
                result = run_for_date(
                    target_date,
                    settings=settings,
                    clickhouse_client=client,
                    skip_if_exists=not args.force,
                    force_reprocess=args.force,
                    namespace_metadata_seed_rows=namespace_metadata_seed_rows,
                )
                if result["status"] == "skipped":
                    skipped += 1
                else:
                    processed += 1
            except Exception as exc:  # noqa: BLE001
                failures.append((target_date.isoformat(), str(exc)))
                logger.exception(
                    "backfill_date_failed",
                    extra={"date": target_date.isoformat(), "error": str(exc)},
                )

        logger.info(
            "backfill_complete",
            extra={
                "start": args.start.isoformat(),
                "end": args.end.isoformat(),
                "processed_dates": processed,
                "skipped_dates": skipped,
                "failure_count": len(failures),
            },
        )
    finally:
        client.close()

    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
