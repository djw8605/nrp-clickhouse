-- ClickHouse schema for the access_accounting database.
-- Host:     clickhouse-access-accounting.clickhouse.svc.cluster.local
-- HTTP:     8123   Native: 9000
-- User:     aime
-- Namespace: clickhouse
--
-- Apply with:
--   clickhouse-client --host=<host> --port=9000 --user=aime --password=<pass> \
--     --multiquery < deployment/schema/access_accounting.sql

CREATE DATABASE IF NOT EXISTS access_accounting;

-- ---------------------------------------------------------------------------
-- namespace_metadata_mapping
-- One row per namespace; synced nightly from the portal API by nrp-accounting-etl.
-- commercial column added 2026-06-24 to track commercial namespaces.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.namespace_metadata_mapping
(
    `namespace`        String,
    `pi`               LowCardinality(String),
    `institution`      LowCardinality(String),
    `admins`           String,
    `user_institutions` String,
    `updated_at`       DateTime,
    `commercial`       Bool DEFAULT false
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY namespace
SETTINGS index_granularity = 8192;

-- To add the commercial column to an existing installation:
--   ALTER TABLE access_accounting.namespace_metadata_mapping
--     ADD COLUMN commercial Bool DEFAULT false;

-- ---------------------------------------------------------------------------
-- cluster_namespace_usage_daily
-- Daily rolled-up resource usage per namespace, aggregated from pod-level data.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.cluster_namespace_usage_daily
(
    `date`           Date,
    `namespace`      LowCardinality(String),
    `created_by`     LowCardinality(String),
    `node`           LowCardinality(String),
    `resource`       LowCardinality(String),
    `raw_resource`   LowCardinality(String),
    `gpu_model_name` LowCardinality(String),
    `usage`          Decimal(18, 6),
    `unit`           LowCardinality(String)
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, created_by, node, resource, raw_resource, gpu_model_name, unit)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- cluster_pod_usage_daily
-- Daily resource usage at pod granularity.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.cluster_pod_usage_daily
(
    `date`           Date,
    `namespace`      LowCardinality(String),
    `created_by`     LowCardinality(String),
    `node`           LowCardinality(String),
    `pod_hash`       UInt64,
    `pod_name`       String,
    `resource`       LowCardinality(String),
    `usage`          Decimal(18, 6),
    `unit`           LowCardinality(String),
    `pod_uid`        String,
    `raw_resource`   LowCardinality(String),
    `gpu_model_name` LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, node, resource, pod_hash)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- llm_token_usage_daily
-- Daily LLM token usage per namespace/model/token-type.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.llm_token_usage_daily
(
    `date`        Date,
    `namespace`   LowCardinality(String),
    `token_alias` LowCardinality(String),
    `model`       LowCardinality(String),
    `token_type`  LowCardinality(String),
    `tokens_used` Decimal(18, 6)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (date, namespace, token_alias, model, token_type)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- namespace_usage_hourly
-- Hourly aggregated resource usage (legacy/raw ingest path).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.namespace_usage_hourly
(
    `hour`               DateTime,
    `allocation_id`      String,
    `namespace`          String,
    `cluster`            String,
    `cpu_core_hours`     Float64,
    `memory_gib_hours`   Float64,
    `gpu_hours`          Float64,
    `storage_gib_hours`  Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (allocation_id, namespace, hour)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- namespace_usage_raw
-- Raw per-scrape resource snapshots.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.namespace_usage_raw
(
    `ts`             DateTime,
    `allocation_id`  String,
    `namespace`      String,
    `cluster`        String,
    `cpu_cores`      Float64,
    `memory_bytes`   UInt64,
    `gpu_count`      UInt32,
    `storage_bytes`  UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (allocation_id, namespace, ts)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- node_institution_mapping
-- Maps cluster node names to institution names.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS access_accounting.node_institution_mapping
(
    `node`             String,
    `institution_name` LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY node
SETTINGS index_granularity = 8192;
