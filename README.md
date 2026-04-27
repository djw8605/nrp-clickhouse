# NRP ClickHouse Accounting Pipeline

Production-grade Python ETL pipeline to pull Kubernetes usage metrics from Prometheus/Thanos and build daily accounting datasets in ClickHouse.

## High-Cardinality Design

The pipeline writes three usage tables:

1. `cluster_pod_usage_daily` (high cardinality)
2. `cluster_namespace_usage_daily` (low cardinality, accounting totals)
3. `llm_token_usage_daily` (daily LLM token detail)

`cluster_pod_usage_daily` stores `pod_uid`, `pod_name`, and `pod_hash` (`CityHash64`) so large pod-cardinality workloads remain query-efficient while retaining Kubernetes pod identity and a debug-friendly raw pod identifier.
The Kubernetes infrastructure usage tables include a `node` dimension so namespace-level summaries can be broken down by the node where jobs ran.
GPU usage rows also retain the source resource label in `raw_resource` and the resolved DCGM model in `gpu_model_name` so queries can filter, group, and audit GPU model attribution.
`llm_token_usage_daily` stores token-level daily totals from the Envoy AI gateway Prometheus metric and also feeds synthetic namespace-level `llm` rows into `cluster_namespace_usage_daily`.
The ETL also maintains a namespace dimension table populated from `portal.nrp.ai`.

## Table Architecture

### `cluster_pod_usage_daily` (high cardinality)

- Purpose: detailed per-pod accounting rows
- Engine: `MergeTree`
- Partition: `toYYYYMM(date)`
- Order key: `(date, namespace, node, resource, raw_resource, gpu_model_name, pod_hash, pod_uid)`

Columns:

- `date Date`
- `namespace LowCardinality(String)`
- `created_by LowCardinality(String)`
- `node LowCardinality(String)`
- `pod_hash UInt64`
- `pod_uid String`
- `pod_name String`
- `resource LowCardinality(String)`
- `raw_resource LowCardinality(String)`
- `gpu_model_name LowCardinality(String)`
- `usage Decimal64(6)`
- `unit LowCardinality(String)`

### `cluster_namespace_usage_daily` (low cardinality summary)

- Purpose: namespace-level accounting summary (rolled up from pod rows plus synthetic namespace-level LLM token totals)
- Engine: `SummingMergeTree`
- Partition: `toYYYYMM(date)`
- Order key: `(date, namespace, created_by, node, resource, raw_resource, gpu_model_name, unit)`

Columns:

- `date Date`
- `namespace LowCardinality(String)`
- `created_by LowCardinality(String)`
- `node LowCardinality(String)`
- `resource LowCardinality(String)`
- `raw_resource LowCardinality(String)`
- `gpu_model_name LowCardinality(String)`
- `usage Decimal64(6)`
- `unit LowCardinality(String)`

### `llm_token_usage_daily` (LLM token detail)

- Purpose: namespace/token/model/token-type daily LLM accounting rows from the AI gateway
- Engine: `MergeTree`
- Partition: `toYYYYMM(date)`
- Order key: `(date, namespace, token_alias, model, token_type)`

Columns:

- `date Date`
- `namespace LowCardinality(String)`
- `token_alias LowCardinality(String)`
- `model LowCardinality(String)`
- `token_type LowCardinality(String)`
- `tokens_used Decimal64(6)`

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

- Source metric: `kube_pod_container_resource_requests`
- Daily query method: `sum_over_time(kube_pod_container_resource_requests[1d:5m]@end_ts)`
- User attribution source: `max_over_time(kube_pod_annotations[1d:5m]@end_ts)` joined locally by namespace, pod, and UID
- GPU model attribution source: `max_over_time(DCGM_FI_DEV_GPU_UTIL{modelName!=""}[1d:5m]@end_ts)` joined locally by namespace, pod, and container; if that direct match is absent, the ETL falls back to a single-model DCGM `Hostname == node`, then typed `raw_resource`, then `unknown`
- LLM source metric: `gen_ai_client_token_usage_sum`
- LLM daily query method: `sum(increase(gen_ai_client_token_usage_sum[1d]@end_ts)) by (team_id, token_alias, gen_ai_original_model, gen_ai_token_type)`
- Resource normalization:
  - `amd_com_xilinx* -> fpga`
  - `cpu -> cpu`
  - `nvidia_com* -> gpu`
  - `*_memory` extended resources, such as `nvidia_com_gpu_memory`, are ignored
  - `llm -> llm`
  - `memory -> memory`
  - `ephemeral_storage -> storage`
  - everything else -> `other`
- Unit mapping:
  - `cpu -> cpu_core_hours`
  - `gpu -> gpu_hours`
  - `fpga -> fpga_hours`
  - `llm -> tokens`
  - `memory -> memory_gb_hours`
  - `storage -> storage_gb_hours`
  - `network -> network_gb`
  - `other -> other`

Row placement:

- `cluster_pod_usage_daily`: one row per `(date, namespace, created_by, node, pod_uid, pod_name, resource, raw_resource, gpu_model_name, unit)` with `pod_hash` derived from `pod_name`.
- `cluster_namespace_usage_daily`: sum of pod usage grouped by `(date, namespace, created_by, node, resource, raw_resource, gpu_model_name, unit)`.
- `llm_token_usage_daily`: one row per `(date, namespace, token_alias, model, token_type)` with `tokens_used` from the AI gateway Prometheus counter increase.
- `cluster_namespace_usage_daily` also receives one synthetic LLM row per `(date, namespace)` with `resource='llm'`, `raw_resource='llm'`, `gpu_model_name='not_applicable'`, `unit='tokens'`, `created_by='llm-gateway'`, and `node='llm-gateway'`.

Idempotency behavior:

- For each processing date, ETL deletes existing rows for that date from all usage tables, then inserts fresh aggregates.

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
export XDMOD_ENDPOINT="https://xdmod.example.org/usage"
export XDMOD_AUTH_HEADER="Authorization"
export XDMOD_AUTH_VALUE="Bearer ..."
export XDMOD_MAX_RECORDS_PER_POST=5000
export XDMOD_MAX_BYTES_PER_POST=5000000
```

`CLICKHOUSE_HOST` supports `host` or `host:port`. If a port is embedded in `CLICKHOUSE_HOST`, it takes precedence over `CLICKHOUSE_PORT`.

## Kubernetes (Kustomize)

ClickHouse is intentionally not deployed here. These manifests deploy the ETL/backfill pipeline plus MCP and OpenAPI query services.

Apply production overlay:

```bash
kubectl apply -k k8s/overlays/prod
```

Files to customize before apply:

- `k8s/base/secret.yaml`: set `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `MCPO_API_KEY`, `XDMOD_ENDPOINT`, and optional XDMod auth values
- `k8s/overlays/prod/kustomization.yaml`: set your pipeline image name/tag
- `k8s/base/configmap.yaml`: adjust Prometheus URL, portal URL, XDMod upload limits, and runtime tuning values

Deployed components:

- `CronJob/nrp-accounting-etl`: daily accounting ETL
- `CronJob/nrp-accounting-xdmod-upload`: daily XDMod upload, separate from ETL so XDMod failures do not affect ClickHouse ingestion
- `Deployment/nrp-accounting-mcp`: read-only MCP server over streamable HTTP
- `Deployment/nrp-accounting-openapi`: OpenAPI bridge for Open WebUI and other OpenAPI clients
- `Service/nrp-accounting-mcp`: cluster-internal service on port `8000`
- `Service/nrp-accounting-openapi`: cluster-internal service on port `8000`
- `Ingress/nrp-accounting-mcp`: external HAProxy ingress for `https://nrp-accounting-mcp.nrp-nautilus.io/`

The provided ingress follows the NRP HAProxy pattern:

- `ingressClassName: haproxy`
- host `nrp-accounting-mcp.nrp-nautilus.io`
- TLS enabled for that host
- root path `/` routed to the native MCP service
- path `/openapi` routed to the OpenAPI bridge service
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

Run the pod-name reingestion backfill:

```bash
kubectl apply -k k8s/pod-name-backfill
```

This job runs `python backfill.py --force` for the date range in [k8s/pod-name-backfill/backfill-job.yaml](/Users/derekweitzel/git/nrp-clickhouse/k8s/pod-name-backfill/backfill-job.yaml). Forced backfills rewrite both pod-level and namespace-level daily rows for each date.

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

## Upload Daily XDMod Usage

```bash
python xdmod_upload.py --date 2026-03-13
```

The XDMod uploader reads already-ingested `cluster_pod_usage_daily` rows from ClickHouse, joins `namespace_metadata_mapping` for the namespace institution, and POSTs one JSON array of daily pod records to `XDMOD_ENDPOINT`. If the payload exceeds `XDMOD_MAX_RECORDS_PER_POST` or `XDMOD_MAX_BYTES_PER_POST`, it is split into multiple POSTs. An HTTP `413` response also causes the current batch to be split and retried.

The Kubernetes uploader is intentionally a separate CronJob from `nrp-accounting-etl`, scheduled later in the morning. Its success or failure does not change Prometheus-to-ClickHouse ingestion.

Options:

- `--dry-run`: print the XDMod payload without sending it
- `--require-data`: fail when no ClickHouse pod usage exists for the date

## Backfill

```bash
python backfill.py --start 2025-01-01 --end 2025-02-01
```

Backfill runs dates sequentially and is safe to restart.
If you are enabling LLM accounting on a ClickHouse database that already has historical namespace usage rows, rerun historical dates with `--force` so `llm_token_usage_daily` and namespace-level `llm` totals are populated for those days.
If you are enabling GPU model attribution on historical data, rerun historical dates with `--force` so pod and namespace rows are rewritten with `gpu_model_name` where DCGM/Thanos history has coverage.

## MCP Server

This repo also includes a read-only MCP server for querying accounting usage from ClickHouse, plus an OpenAPI-compatible bridge for Open WebUI-style tool servers.

Run over stdio:

```bash
python3 -m nrp_accounting_pipeline.mcp_server
```

Run over streamable HTTP:

```bash
python3 -m nrp_accounting_pipeline.mcp_server --transport streamable-http --host 0.0.0.0 --port 8000
```

Run as an OpenAPI-compatible server with `mcpo`:

```bash
export MCPO_API_KEY="replace-me"
mcpo --host 0.0.0.0 --port 8000 --root-path /openapi --api-key "$MCPO_API_KEY" -- python3 -m nrp_accounting_pipeline.mcp_server
```

This exposes OpenAPI docs at `http://localhost:8000/openapi/docs`.

Available tools:

- `get_latest_data_date`: most recent ingested accounting date
- `list_active_namespaces`: namespaces with observed usage in a date window, defaulting to the last 30 days
- `list_filter_values`: discover distinct namespaces, institutions, nodes, resources, and other values observed in usage rows for the filtered date range
- `list_llm_filter_values`: discover distinct namespaces, token aliases, models, and token types observed in daily LLM rows
- `top_resource_consumers`: top namespaces, institutions, or nodes for one resource
- `get_usage_timeseries`: daily trend for one namespace, institution, node, or node-institution
- `get_namespace_summary`: latest or date-bounded namespace summary by resource
- `get_namespace_daily_trend`: namespace trend view, defaulting to the last 30 days
- `get_namespace_llm_summary`: latest or date-bounded namespace LLM summary by token alias, model, and token type
- `get_namespace_llm_daily_trend`: namespace LLM token trend, defaulting to the last 30 days
- `top_nodes_for_namespace`: top nodes for one namespace and one resource
- `get_namespace_details`: namespace metadata, latest summary, recent trend, and top nodes
- `query_resource_usage`: flexible escape hatch for custom aggregations
- `query_llm_token_usage`: flexible escape hatch for token/model/type LLM aggregations

Supported filters:

- `namespace`: one namespace or a list of namespaces
- `institution`: namespace institution from `namespace_metadata_mapping`
- `node`: one node name or a list of node names
- `node_regex`: ClickHouse regex matched against `node`
- `node_institution`: institution from `node_institution_mapping`
- `resource`: normalized resource name such as `cpu`, `gpu`, `memory`, `storage`, `fpga`, `network`, or `llm`
- `raw_resource`: original source resource label such as `nvidia_com_gpu`, `nvidia_com_a100`, `cpu`, or `memory`
- `gpu_model_name`: exact DCGM GPU model value, or one of `unknown`, `mixed`, or `not_applicable`
- `gpu_model_regex`: ClickHouse regex matched against `gpu_model_name`; normally pair this with `resource=gpu`
- Resource aliases like `gpu_hours`, `gpu-hours`, `GPU hours`, `cpu_core_hours`, `llm_tokens`, and `tokens` are accepted and normalized automatically
- For example, use `resource=gpu` for GPU-hours queries, `resource=cpu` for CPU core-hour queries, and `resource=llm` for namespace-level LLM token totals; units come back separately
- Units are returned separately in the `unit` field, for example `resource=gpu` yields `unit=gpu_hours`
- `granularity`: `namespace` or `pod`
- `group_by`: any of `date`, `namespace`, `institution`, `pi`, `node`, `node_institution`, `created_by`, `resource`, `raw_resource`, `gpu_model_name`, `unit`, and `pod_name` (pod granularity only)
- LLM-specific filters: `token_alias`, `model`, and `token_type`
- LLM-specific `group_by`: any of `date`, `namespace`, `token_alias`, `model`, and `token_type`

Date behavior:

- If `start_date` and `end_date` are both omitted, the tool queries the most recent ingested date.
- If only one of them is supplied, the query is treated as a single-day lookup.
- Trend-oriented tools default to the last 30 days when no dates are supplied.
- `list_active_namespaces` also defaults to the last 30 days when no dates are supplied.
- Unknown resource inputs now return a validation error instead of silently producing empty rows.

Discovery behavior:

- `list_filter_values` and `list_active_namespaces` return values observed in actual accounting usage rows, not a static catalog.
- `list_llm_filter_values` returns values observed in actual daily LLM usage rows, not a static catalog.
- Discovery responses include `total_count` and `is_truncated` so callers can tell when a result set was cut off by `limit`.

Example prompt/tool call intent:

- "Show GPU usage for namespace `foo` on the latest ingested day"
- "Sum memory usage for institution `Delta University` between `2026-04-01` and `2026-04-07`"
- "Find usage on nodes matching `^gpu-node-` grouped by namespace and resource"
- "What namespaces exist in the latest accounting data?"
- "Who are the top 10 GPU-consuming institutions this week?"
- "What namespaces were the largest users of NRP A100 GPUs?" maps to `top_resource_consumers(dimension="namespace", resource="gpu", gpu_model_regex="(?i)A100")`
- "Which GPU models are available?" maps to `list_filter_values(dimension="gpu_model_name", resource="gpu")`
- "Show a 30-day GPU trend for namespace `foo`"
- "Show namespace `foo` LLM token totals for the latest day"
- "Break down namespace `foo` LLM usage by token alias and model this week"
- "Show a 30-day LLM token trend for namespace `foo`"

When deployed with the included Kubernetes ingress:

- external MCP clients should connect to `https://nrp-accounting-mcp.nrp-nautilus.io/`
- external OpenAPI clients should connect to `https://nrp-accounting-mcp.nrp-nautilus.io/openapi`
- the generated OpenAPI docs are served at `https://nrp-accounting-mcp.nrp-nautilus.io/openapi/docs`

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

- Daily usage query via `sum_over_time(kube_pod_container_resource_requests[1d:5m]@end_ts)`, enriched with pod annotations for user attribution
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
