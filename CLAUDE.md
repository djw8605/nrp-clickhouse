# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Nightly accounting pipeline for the NRP Kubernetes cluster: PromQL queries against Thanos pull per-pod resource requests and LLM token counters, Python aggregates them into daily usage rows, and the results are written to ClickHouse (`access_accounting` database). A read-only MCP server and OpenAPI bridge serve queries over that data. Billing unit is resource-requests-while-Running (core-hours, gpu-hours, gb-hours), not actual utilization.

## Commands

```bash
# Setup (uv also works: uv pip install -r requirements-test.txt)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-test.txt   # includes requirements.txt

# Tests
pytest tests/ -q                                  # full suite (~2 min, see note below)
pytest tests/ -q --deselect tests/test_etl_dryrun.py   # offline-only tests (<1 s)
pytest tests/test_pod_resource_ingestion.py::test_pod_resource_requests_query_bills_only_running_pods -q

# Run the pipeline
python etl.py --date 2026-03-13          # one day; --force to reprocess, --skip-existing to skip
python etl.py --test                     # full aggregation against mock JSON, no network/ClickHouse
python backfill.py --start 2025-01-01 --end 2025-02-01 [--force]
python xdmod_upload.py --date 2026-03-13 [--dry-run]

# MCP server (read-only ClickHouse queries)
python3 -m nrp_accounting_pipeline.mcp_server                          # stdio
python3 -m nrp_accounting_pipeline.mcp_server --transport streamable-http --port 8000
```

`tests/test_etl_dryrun.py` is a live integration suite: it runs the production PromQL against a real Prometheus endpoint (default `https://thanos.nrp-nautilus.io`). The full-cluster phase-joined query takes ~100s. Do not point it at `https://prometheus.nrp-nautilus.io` — that gateway 504s at ~60s.

Configuration is entirely environment variables (see `nrp_accounting_pipeline/config.py` and the README's Environment section). `source.sh` in the repo root holds working local credentials (ClickHouse via a local port-forward) and is untracked — never commit it.

## Architecture

Root-level `etl.py`, `backfill.py`, `xdmod_upload.py`, `institution_import.py` are thin shims; all real code lives in `nrp_accounting_pipeline/`.

**Write path** (`etl.py:run_for_date`, called per-date by `backfill.py`):
1. Three instant PromQL queries anchored with `@end_ts` subqueries over `[1d:5m]`: pod resource requests (joined against `kube_pod_status_phase{phase="Running"}` — see gotchas), pod annotations (user attribution), and DCGM GPU utilization (GPU model names). Plus one LLM token query (`gen_ai_client_token_usage_sum`).
2. Enrichment joins happen client-side in Python, not in PromQL: `attach_pod_annotations_to_resource_payload` and `attach_gpu_model_names_to_resource_payload` merge labels into the resource payload.
3. `aggregation.py` converts each series' `sum_over_time` sample-sum into hourly units by dividing by `SAMPLES_PER_HOUR` (12 samples/hour at the 5m step — if you change the subquery step, change `SUBQUERY_STEP_MINUTES` too). `normalize_resource` maps raw resource labels (`nvidia_com_*` → gpu, etc.); `*_memory`/`*_mem` suffixed resources are ignored as byte counts. Implausible request values are dropped per `MAX_REASONABLE_REQUEST_BY_RESOURCE`.
4. Pod-level rows roll up into namespace rows; LLM rows are kept in their own table and also folded into namespace rows as `resource='llm'`.
5. Writes are idempotent per date: `delete_existing_partitions` (ALTER DELETE) then insert. Each ETL run also syncs `namespace_metadata_mapping` from portal.nrp.ai and re-imports the node→institution CSV.

**Tables** (schema mirrored in `docs/schema/access_accounting.sql`, created/migrated at runtime by `schema.py:ensure_schema`): `cluster_pod_usage_daily` (high cardinality, ReplicatedMergeTree), `cluster_namespace_usage_daily` (ReplicatedSummingMergeTree — duplicate key rows sum on merge), `llm_token_usage_daily`, plus `namespace_metadata_mapping` and `node_institution_mapping` dimension tables. Monthly partitions. `CLICKHOUSE_CLUSTER` set → `ON CLUSTER` DDL + Replicated engines; empty → single-node dev.

**Read path**: `accounting_queries.py` builds validated SQL (joins usage to the two dimension tables) and is the single query layer used by `mcp_server.py` (FastMCP tools + OpenAPI bridge).

**Deployment**: GitHub Actions builds `ghcr.io/djw8605/nrp-clickhouse` on push to main and commits the new `sha-<commit>` tag into `apps/nrp-clickhouse/overlays/dev/kustomization.yaml`, which ArgoCD tracks. `k8s/` contains the deployable base (ETL CronJob, MCP/OpenAPI deployments, backup CronJob) and one-off backfill Job templates (`k8s/backfill*/`) where you edit start/end dates and image tag. ClickHouse itself is deployed separately (Altinity operator), not from this repo.

## Critical gotchas

- **The Running-phase join in `POD_RESOURCE_REQUESTS_QUERY_TEMPLATE` is load-bearing.** kube-state-metrics exports `kube_pod_container_resource_requests` for every pod *object* — Pending, Succeeded, and Failed included — until it is deleted. Without the join, completed/queued batch pods are billed 24h/day (this caused a historical ~2.2× cluster-wide GPU over-billing). Do not "simplify" the query back to the bare metric.
- **Insert dedup**: the tables are Replicated engines with `insert_deduplicate=1`; the delete-then-reinsert pattern means a reprocessed date's identical blocks would be *silently dropped* (0 rows, while logs claim success). The `insert_deduplication_token` passed by every insert in `clickhouse_client.py` (unique per run, stable across retries of a batch) is what prevents this. Keep it when touching insert code.
- **Thanos data contains user typos verbatim**: Kubernetes accepts `cpu: 10G` (= 10 billion cores) and kube-state-metrics exports it. The per-resource caps in `aggregation.py` are the backstop; the phase join keeps such never-schedulable pods out entirely.
- **Historical data quality**: rows ingested by older code versions stay wrong until the date is re-run with `--force`. When changing billing semantics, expect to re-backfill affected months and verify daily totals against physical capacity (e.g., total GPU-hours/day ≤ allocatable GPUs × 24).
- Prometheus results with a missing `node` label fall back to the `instance` label in `_extract_node_label`, which for kube-state-metrics is the KSM pod IP — "nodes" like `10.244.x.x` in old data come from this.
