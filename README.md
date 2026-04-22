# NRP ClickHouse Accounting Pipeline

Production-grade Python ETL pipeline to pull Kubernetes usage metrics from Prometheus/Thanos and build daily accounting datasets in ClickHouse.

## High-Cardinality Design

The pipeline writes two usage tables:

1. `cluster_pod_usage_daily` (high cardinality)
2. `cluster_namespace_usage_daily` (low cardinality, accounting totals)

`cluster_pod_usage_daily` stores both `pod_name` and `pod_hash` (`CityHash64`) so large pod-cardinality workloads remain query-efficient while retaining a debug-friendly raw pod identifier.
Both tables include a `node` dimension so namespace-level summaries can be broken down by the node where jobs ran.
The ETL also maintains a namespace dimension table populated from `portal.nrp.ai`.

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

### `namespace_metadata_mapping` (dimension table)

- Purpose: lookup table for joining namespace usage to current namespace metadata from `portal.nrp.ai`
- Engine: `MergeTree`
- Order key: `namespace`
- Free-form fields like `Description` are intentionally omitted

Columns:

- `namespace String`
- `pi LowCardinality(String)`
- `institution LowCardinality(String)`
- `admins String`
- `user_institutions String`
- `updated_at DateTime`

## What Data Goes Where

- Source metric: `namespace_allocated_resources`
- Daily query method: `sum_over_time(namespace_allocated_resources[1d:5m]@end_ts)`
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
export PORTAL_RPC_URL="https://portal.nrp.ai/rpc"
export CLICKHOUSE_HOST="clickhouse.example.org"   # or clickhouse.example.org:28394
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="..."
export CLICKHOUSE_DATABASE="accounting"
export MAX_QUERY_WORKERS=8
export QUERY_STEP="1h"
export RETRY_LIMIT=4
export PORTAL_TIMEOUT_SECONDS=60
export INSTITUTION_CSV_URL="https://raw.githubusercontent.com/djw8605/nrp-ror-labeler/refs/heads/main/node-institution.csv"
```

`CLICKHOUSE_HOST` supports `host` or `host:port`. If a port is embedded in `CLICKHOUSE_HOST`, it takes precedence over `CLICKHOUSE_PORT`.

## Kubernetes (Kustomize)

ClickHouse is intentionally not deployed here. These manifests deploy the ETL/backfill pipeline plus an MCP query service.

Apply production overlay:

```bash
kubectl apply -k k8s/overlays/prod
```

Files to customize before apply:

- `k8s/base/secret.yaml`: set `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- `k8s/overlays/prod/kustomization.yaml`: set your pipeline image name/tag
- `k8s/base/configmap.yaml`: adjust Prometheus URL, portal URL, and runtime tuning values

Deployed components:

- `CronJob/nrp-accounting-etl`: daily accounting ETL
- `Deployment/nrp-accounting-mcp`: read-only MCP server over streamable HTTP
- `Service/nrp-accounting-mcp`: cluster-internal service on port `8000`
- `Ingress/nrp-accounting-mcp`: external HAProxy ingress for `https://nrp-accounting-mcp.nrp-nautilus.io/`

The provided ingress follows the NRP HAProxy pattern:

- `ingressClassName: haproxy`
- host `nrp-accounting-mcp.nrp-nautilus.io`
- TLS enabled for that host
- root path `/` routed to the MCP service
- FastMCP DNS rebinding protection configured to allow that public host

If you want a different public subdomain, update the `host` value in [ingress-mcp.yaml](/Users/derekweitzel/git/nrp-clickhouse/k8s/base/ingress-mcp.yaml) or patch it in an overlay before applying.

Image pull behavior:

- The manifests currently use the mutable `:latest` image tag.
- To avoid stale cached images on Kubernetes nodes, ETL, backfill, and MCP workloads use `imagePullPolicy: Always`.
- For production-grade releases, prefer replacing `latest` in the overlay with an immutable tag such as a Git SHA image tag from the GitHub Actions build.

MCP transport security:

- The Python MCP SDK validates `Host` and `Origin` headers when DNS rebinding protection is enabled.
- The Kubernetes config sets `MCP_ALLOWED_HOSTS` and `MCP_ALLOWED_ORIGINS` so the public NRP ingress hostname is accepted.
- If you change the ingress host, update those config values to match or the server will return `421 Invalid Host header`.

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

Each ETL run also queries `portal.nrp.ai` and syncs `namespace_metadata_mapping`.
Namespaces observed in usage data but missing from the portal response are inserted with `Unknown` metadata so the mapping table still covers observed usage.

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

## MCP Server

This repo also includes a read-only MCP server for querying accounting usage from ClickHouse.

Run over stdio:

```bash
python3 -m nrp_accounting_pipeline.mcp_server
```

Run over streamable HTTP:

```bash
python3 -m nrp_accounting_pipeline.mcp_server --transport streamable-http --host 0.0.0.0 --port 8000
```

Primary tool:

- `query_resource_usage`

Supported filters:

- `namespace`: one namespace or a list of namespaces
- `institution`: namespace institution from `namespace_metadata_mapping`
- `node`: one node name or a list of node names
- `node_regex`: ClickHouse regex matched against `node`
- `node_institution`: institution from `node_institution_mapping`
- `resource`: normalized resource name such as `cpu`, `gpu`, `memory`, `storage`, `fpga`
- `granularity`: `namespace` or `pod`
- `group_by`: any of `date`, `namespace`, `institution`, `pi`, `node`, `node_institution`, `created_by`, `resource`, `unit`, and `pod_name` (pod granularity only)

Date behavior:

- If `start_date` and `end_date` are both omitted, the tool queries the most recent ingested date.
- If only one of them is supplied, the query is treated as a single-day lookup.

Example prompt/tool call intent:

- "Show GPU usage for namespace `foo` on the latest ingested day"
- "Sum memory usage for institution `Delta University` between `2026-04-01` and `2026-04-07`"
- "Find usage on nodes matching `^gpu-node-` grouped by namespace and resource"

When deployed with the included Kubernetes ingress, external MCP clients should connect to `https://nrp-accounting-mcp.nrp-nautilus.io/`.

## Institution Mapping CSV Import

You can import a CSV dimension table to map node hostnames to institutions.

Required CSV columns:

- `OSG Identifier` (node hostname)
- `Institution Name`

Run importer:

```bash
python institution_import.py --csv /path/to/institution_mapping.csv
```

Or set the `INSTITUTION_CSV_URL` environment variable to import from a raw GitHub URL:

```bash
export INSTITUTION_CSV_URL="https://raw.githubusercontent.com/djw8605/nrp-ror-labeler/refs/heads/main/node-institution.csv"
python institution_import.py
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

Example: namespace usage by PI and institution

```sql
SELECT
  u.date,
  m.pi,
  m.institution,
  u.namespace,
  u.resource,
  u.unit,
  sum(u.usage) AS usage
FROM cluster_namespace_usage_daily AS u
LEFT JOIN namespace_metadata_mapping AS m ON u.namespace = m.namespace
GROUP BY u.date, m.pi, m.institution, u.namespace, u.resource, u.unit
ORDER BY u.date DESC, m.institution, m.pi, u.namespace;
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

- Daily usage query via `sum_over_time(namespace_allocated_resources[1d:5m]@end_ts)`
- Retries with exponential backoff for Prometheus, `portal.nrp.ai`, and ClickHouse operations
- Idempotent daily writes by deleting existing date rows before insert
- Namespace metadata rows are inserted for new namespaces and updated when portal metadata changes
- Namespace metadata rows are never deleted automatically, even if a namespace later disappears from the portal
- Structured JSON logging for query duration, retries, row counts, and aggregation timing

## Test Mode

Default test data file:

`nrp_accounting_pipeline/testdata/mock_prometheus_results.json`

Run:

```bash
python etl.py --test
```
