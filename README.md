# NRP ClickHouse Accounting Pipeline

Production-grade Python ETL pipeline to pull Kubernetes usage metrics from Prometheus/Thanos and build daily accounting datasets in ClickHouse.

## High-Cardinality Design

The pipeline writes two usage tables:

1. `cluster_pod_usage_daily` (high cardinality)
2. `cluster_namespace_usage_daily` (low cardinality, accounting totals)

`cluster_pod_usage_daily` stores both `pod_name` and `pod_hash` (`CityHash64`) so large pod-cardinality workloads remain query-efficient while retaining a debug-friendly raw pod identifier.
Both tables include a `node` dimension so namespace-level summaries can be broken down by the node where jobs ran.

## Table Architecture

### `cluster_pod_usage_daily` (high cardinality)

- Purpose: detailed per-pod accounting rows
- Engine: `MergeTree`
- Partition: `toYYYYMM(date)`
- Order key: `(date, namespace, node, resource, pod_hash)`

Columns:

- `date Date`
- `namespace LowCardinality(String)`
- `created_by LowCardinality(String)`
- `node LowCardinality(String)`
- `pod_hash UInt64`
- `pod_name String`
- `resource LowCardinality(String)`
- `usage Decimal64(6)`
- `unit LowCardinality(String)`

### `cluster_namespace_usage_daily` (low cardinality summary)

- Purpose: namespace-level accounting summary (rolled up from pod rows)
- Engine: `SummingMergeTree`
- Partition: `toYYYYMM(date)`
- Order key: `(date, namespace, node, resource)`

Columns:

- `date Date`
- `namespace LowCardinality(String)`
- `created_by LowCardinality(String)`
- `node LowCardinality(String)`
- `resource LowCardinality(String)`
- `usage Decimal64(6)`
- `unit LowCardinality(String)`

### `node_institution_mapping` (dimension table)

- Purpose: lookup table for joining node hostname to institution
- Engine: `MergeTree`
- Order key: `node`

Columns:

- `node String`
- `institution_name LowCardinality(String)`

## What Data Goes Where

- Source metric: `namespace_allocated_resources`
- Daily query method: `sum_over_time(namespace_allocated_resources[1d:1h]@end_ts)`
- Resource normalization:
  - `amd_com_xilinx* -> fpga`
  - `cpu -> cpu`
  - `nvidia_com* -> gpu`
  - `memory -> memory`
  - `ephemeral_storage -> storage`
  - everything else -> `other`
- Unit mapping:
  - `cpu -> cpu_core_hours`
  - `gpu -> gpu_hours`
  - `fpga -> fpga_hours`
  - `memory -> memory_gb_hours`
  - `storage -> storage_gb_hours`
  - `network -> network_gb`
  - `other -> other`

Row placement:

- `cluster_pod_usage_daily`: one row per `(date, namespace, created_by, node, pod_name, resource, unit)` with `pod_hash` derived from `pod_name`.
- `cluster_namespace_usage_daily`: sum of pod usage grouped by `(date, namespace, created_by, node, resource, unit)`.

Idempotency behavior:

- For each processing date, ETL deletes existing rows for that date from both tables, then inserts fresh aggregates.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment

```bash
export PROMETHEUS_URL="https://thanos.example.org"
export CLICKHOUSE_HOST="clickhouse.example.org"   # or clickhouse.example.org:28394
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="..."
export CLICKHOUSE_DATABASE="accounting"
export MAX_QUERY_WORKERS=8
export QUERY_STEP="1h"
export RETRY_LIMIT=4
```

`CLICKHOUSE_HOST` supports `host` or `host:port`. If a port is embedded in `CLICKHOUSE_HOST`, it takes precedence over `CLICKHOUSE_PORT`.

## Kubernetes (Kustomize)

ClickHouse is intentionally not deployed here. These manifests deploy only the ETL/backfill pipeline.

Apply production overlay:

```bash
kubectl apply -k k8s/overlays/prod
```

Files to customize before apply:

- `k8s/base/secret.yaml`: set `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- `k8s/overlays/prod/kustomization.yaml`: set your pipeline image name/tag
- `k8s/base/configmap.yaml`: adjust Prometheus URL and runtime tuning values

Run a one-time backfill job:

1. Edit the date range in `k8s/backfill/backfill-job.yaml`
2. Apply:

```bash
kubectl apply -k k8s/backfill
```

## Run Daily ETL

```bash
python etl.py --date 2026-03-13
```

Options:

- `--skip-existing`: skip date if already ingested
- `--force`: reprocess date even if already ingested
- `--test`: run full aggregation pipeline from mock Prometheus JSON only
- `--mock-file /path/to/mock.json`: override default test payload file

## Backfill

```bash
python backfill.py --start 2025-01-01 --end 2025-02-01
```

Backfill runs dates sequentially and is safe to restart.

## Institution Mapping CSV Import

You can import a CSV dimension table to map node hostnames to institutions.

Required CSV columns:

- `OSG Identifier` (node hostname)
- `Institution Name`

Run importer:

```bash
python institution_import.py --csv /path/to/institution_mapping.csv
```

This loads data into:

- `node_institution_mapping(node, institution_name)`

The import is idempotent: it truncates and reloads the mapping table on each run.

Example: namespace usage by institution

```sql
SELECT
  u.date,
  m.institution_name,
  u.namespace,
  u.resource,
  u.unit,
  sum(u.usage) AS usage
FROM cluster_namespace_usage_daily AS u
LEFT JOIN node_institution_mapping AS m ON u.node = m.node
GROUP BY u.date, m.institution_name, u.namespace, u.resource, u.unit
ORDER BY u.date DESC, m.institution_name, u.namespace, u.resource;
```

Example: all usage for one institution

```sql
SELECT
  u.date,
  m.institution_name,
  u.namespace,
  u.node,
  u.resource,
  u.unit,
  sum(u.usage) AS usage
FROM cluster_pod_usage_daily AS u
INNER JOIN node_institution_mapping AS m ON u.node = m.node
WHERE m.institution_name = 'Your Institution Name'
GROUP BY u.date, m.institution_name, u.namespace, u.node, u.resource, u.unit
ORDER BY u.date DESC, u.namespace, u.node, u.resource;
```

## Reliability + Idempotency

- Daily usage query via `sum_over_time(namespace_allocated_resources[1d:1h]@end_ts)`
- Retries with exponential backoff for Prometheus and ClickHouse operations
- Idempotent daily writes by deleting existing date rows before insert
- Structured JSON logging for query duration, retries, row counts, and aggregation timing

## Test Mode

Default test data file:

`nrp_accounting_pipeline/testdata/mock_prometheus_results.json`

Run:

```bash
python etl.py --test
```
