# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 08 - Cost Attribution Tracker
# MAGIC
# MAGIC Builds first-class daily workload cost attribution for UC Steward and adjacent governance operations using `system.billing.usage` plus `system.billing.list_prices`.
# MAGIC
# MAGIC **Design**
# MAGIC * Tight date filter on system tables
# MAGIC * Workspace-scoped aggregation
# MAGIC * Multi-level attribution: `job`, `notebook`, `warehouse`, `pipeline`, `endpoint`, `app`, `unattributed`
# MAGIC * Uses `system.billing.attributed_usage` only as an optional future extension; current implementation relies on `system.billing.usage` because it is populated in this workspace
# MAGIC

# COMMAND ----------

# DBTITLE 1,Aggregate daily attributed cost
# Databricks notebook source
dbutils.widgets.text("catalog",                 "")
dbutils.widgets.text("control_schema",           "uc_hygiene")
dbutils.widgets.text("workspace_id",             "")
dbutils.widgets.text("cost_lookback_days",        "7")
dbutils.widgets.text("cost_currency_code",        "USD")
dbutils.widgets.text("cost_min_dbu_threshold",    "0")
dbutils.widgets.text("framework_tag",             "uc-steward")


def require_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Missing required widget: '{name}'")
    return value


catalog                = require_widget("catalog")
control_schema         = require_widget("control_schema")
workspace_id           = require_widget("workspace_id")
cost_lookback_days     = int(dbutils.widgets.get("cost_lookback_days")     or "7")
cost_currency_code     = (dbutils.widgets.get("cost_currency_code")        or "USD").strip().upper()
cost_min_dbu_threshold = float(dbutils.widgets.get("cost_min_dbu_threshold") or "0")
framework_tag          = (dbutils.widgets.get("framework_tag")              or "uc-steward").strip()
control_fqn            = f"{catalog}.{control_schema}"

print(f"Catalog:         {catalog}")
print(f"Control schema:  {control_fqn}")
print(f"Workspace ID:    {workspace_id}")
print(f"Lookback:        {cost_lookback_days} day(s)")
print(f"Currency:        {cost_currency_code}")
print(f"Min DBU:         {cost_min_dbu_threshold}")
print(f"Framework tag:   {framework_tag}")

import time
from datetime import date

_start = time.time()
_today = date.today()

# ── Idempotent delete: clear the lookback window before re-inserting ─────────
spark.sql(f"""
DELETE FROM {control_fqn}.cost_attribution_daily
WHERE usage_date >= current_date() - INTERVAL {cost_lookback_days} DAYS
  AND workspace_id = '{workspace_id}'
""")

# ── Main cost rollup: all attributable usage in the lookback window ────────
cost_rollup = spark.sql(f"""
WITH usage_base AS (
  SELECT
    usage_date,
    workspace_id,
    sku_name,
    cloud,
    usage_unit,
    usage_quantity,
    custom_tags,
    usage_metadata,
    -- Attribution owner: prefer job-level custom tag, fall back to user tag
    COALESCE(
      custom_tags['owner'], custom_tags['Owner'],
      custom_tags['team'],  custom_tags['Team']
    ) AS attribution_owner,
    -- Which tag value identifies this framework?
    COALESCE(custom_tags['framework'], custom_tags['Framework'], '') AS tag_framework,
    -- Attribution level by resource type
    CASE
      WHEN usage_metadata.job_id        IS NOT NULL THEN 'job'
      WHEN usage_metadata.notebook_path IS NOT NULL THEN 'notebook'
      WHEN usage_metadata.warehouse_id  IS NOT NULL THEN 'warehouse'
      WHEN usage_metadata.dlt_pipeline_id IS NOT NULL THEN 'pipeline'
      WHEN usage_metadata.endpoint_name IS NOT NULL THEN 'endpoint'
      WHEN usage_metadata.app_name      IS NOT NULL THEN 'app'
      ELSE 'unattributed'
    END AS attribution_level,
    CASE
      WHEN usage_metadata.job_id        IS NOT NULL THEN CAST(usage_metadata.job_id AS STRING)
      WHEN usage_metadata.notebook_path IS NOT NULL THEN usage_metadata.notebook_path
      WHEN usage_metadata.warehouse_id  IS NOT NULL THEN usage_metadata.warehouse_id
      WHEN usage_metadata.dlt_pipeline_id IS NOT NULL THEN usage_metadata.dlt_pipeline_id
      WHEN usage_metadata.endpoint_name IS NOT NULL THEN usage_metadata.endpoint_name
      WHEN usage_metadata.app_name      IS NOT NULL THEN usage_metadata.app_name
      ELSE COALESCE(usage_metadata.cluster_id, 'workspace_unattributed')
    END AS attribution_key,
    CASE
      WHEN usage_metadata.job_name      IS NOT NULL THEN usage_metadata.job_name
      WHEN usage_metadata.notebook_path IS NOT NULL THEN usage_metadata.notebook_path
      WHEN usage_metadata.warehouse_id  IS NOT NULL THEN CONCAT('warehouse:', usage_metadata.warehouse_id)
      WHEN usage_metadata.dlt_pipeline_id IS NOT NULL THEN CONCAT('pipeline:', usage_metadata.dlt_pipeline_id)
      WHEN usage_metadata.endpoint_name IS NOT NULL THEN usage_metadata.endpoint_name
      WHEN usage_metadata.app_name      IS NOT NULL THEN usage_metadata.app_name
      ELSE COALESCE(usage_metadata.cluster_id, 'workspace_unattributed')
    END AS attribution_label,
    COALESCE(
      usage_metadata.job_name, usage_metadata.notebook_path,
      usage_metadata.endpoint_name, usage_metadata.app_name
    ) AS billing_origin_product
  FROM system.billing.usage
  WHERE usage_date  >= current_date() - INTERVAL {cost_lookback_days} DAYS
    AND workspace_id = '{workspace_id}'
    AND usage_unit   = 'DBU'
    AND usage_quantity >= {cost_min_dbu_threshold}
),
prices AS (
  -- Point-in-time price lookup: join on SKU + cloud + unit, filter by valid date range
  SELECT
    sku_name,
    cloud,
    usage_unit,
    price_start_time,
    COALESCE(price_end_time, TIMESTAMP '9999-12-31') AS price_end_time,
    pricing.default AS list_unit_price
  FROM system.billing.list_prices
  WHERE currency_code = '{cost_currency_code}'
),
usage_priced AS (
  SELECT
    u.usage_date,
    u.workspace_id,
    '{framework_tag}'        AS framework_tag,
    u.attribution_level,
    u.attribution_key,
    u.attribution_label,
    u.attribution_owner,
    COALESCE(u.billing_origin_product, u.attribution_label) AS billing_origin_product,
    u.sku_name,
    u.usage_unit,
    u.usage_quantity,
    p.list_unit_price,
    CAST(u.usage_quantity * p.list_unit_price AS DECIMAL(38,10)) AS estimated_cost
  FROM usage_base u
  LEFT JOIN prices p
    ON  u.sku_name    = p.sku_name
    AND u.cloud       = p.cloud
    AND u.usage_unit  = p.usage_unit
    AND CAST(u.usage_date AS TIMESTAMP) >= p.price_start_time
    AND CAST(u.usage_date AS TIMESTAMP) <  p.price_end_time
)
SELECT
  usage_date,
  workspace_id,
  framework_tag,
  attribution_level,
  attribution_key,
  attribution_label,
  attribution_owner,
  billing_origin_product,
  sku_name,
  usage_unit,
  CAST(SUM(usage_quantity)              AS DECIMAL(38,10)) AS usage_quantity,
  CAST(MAX(list_unit_price)             AS DECIMAL(38,10)) AS list_unit_price,
  CAST(SUM(COALESCE(estimated_cost, 0)) AS DECIMAL(38,10)) AS estimated_cost,
  COUNT(*)                                                  AS record_count,
  'system.billing.usage'                                    AS source_table,
  CURRENT_TIMESTAMP()                                       AS created_at
FROM usage_priced
GROUP BY
  usage_date, workspace_id, framework_tag,
  attribution_level, attribution_key, attribution_label, attribution_owner,
  billing_origin_product, sku_name, usage_unit
""")

cost_rollup.createOrReplaceTempView("_cost_rollup")

spark.sql(f"""
INSERT INTO {control_fqn}.cost_attribution_daily
SELECT * FROM _cost_rollup
""")

rows_written = spark.sql("SELECT COUNT(*) AS n FROM _cost_rollup").collect()[0]["n"]

# ── Framework-scoped summary: usage tagged as this framework ──────────────
print(f"\n{'='*60}")
print(f"  ALL WORKSPACE — top 20 attribution sources ({cost_lookback_days}d)")
print(f"{'='*60}")
spark.sql(f"""
SELECT
  usage_date,
  attribution_level,
  attribution_label,
  ROUND(SUM(usage_quantity), 2) AS dbus,
  ROUND(SUM(estimated_cost), 4) AS est_cost_{cost_currency_code}
FROM {control_fqn}.cost_attribution_daily
WHERE usage_date   >= current_date() - INTERVAL {cost_lookback_days} DAYS
  AND workspace_id  = '{workspace_id}'
GROUP BY usage_date, attribution_level, attribution_label
ORDER BY usage_date DESC, est_cost_{cost_currency_code} DESC
LIMIT 20
""").show(truncate=False)

print(f"{'='*60}")
print(f"  UC-HYGIENE FRAMEWORK — cost by task ({cost_lookback_days}d)")
print(f"  (rows where framework tag = '{framework_tag}')")
print(f"{'='*60}")
spark.sql(f"""
SELECT
  usage_date,
  attribution_label,
  ROUND(SUM(usage_quantity), 4) AS dbus,
  ROUND(SUM(estimated_cost), 4) AS est_cost_{cost_currency_code},
  MAX(attribution_owner)        AS owner
FROM {control_fqn}.cost_attribution_daily
WHERE usage_date   >= current_date() - INTERVAL {cost_lookback_days} DAYS
  AND workspace_id  = '{workspace_id}'
  AND framework_tag = '{framework_tag}'
GROUP BY usage_date, attribution_label
ORDER BY usage_date DESC, est_cost_{cost_currency_code} DESC
""").show(truncate=False)

# ── Write execution record to job_run_history ────────────────────────────
_duration = int(time.time() - _start)
spark.sql(f"""
INSERT INTO {control_fqn}.job_run_history VALUES (
  DATE('{_today}'),
  'uc_hygiene_daily_governance',
  'p5_cost_attribution',
  'p5_observability',
  'success',
  NULL,
  {rows_written},
  {rows_written},
  {_duration},
  'lookback={cost_lookback_days}d workspace={workspace_id} framework_tag={framework_tag}',
  CURRENT_TIMESTAMP()
)
""")

print(f"\nRows written to cost_attribution_daily: {rows_written:,}")
print(f"Duration: {_duration}s")


