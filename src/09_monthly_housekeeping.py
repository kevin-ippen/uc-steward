# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 09 - Monthly Housekeeping
# MAGIC
# MAGIC Runs on the 1st of each month. Keeps control tables lean and the governance program healthy.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Prunes `scan_results` older than `scan_results_retention_days`
# MAGIC 2. Prunes resolved `migration_plans` older than `plan_retention_days`
# MAGIC 3. Prunes `cost_attribution_daily` older than `cost_attribution_retention_days`
# MAGIC 4. Expires certifications whose `next_review_date` is overdue
# MAGIC 5. Captures a point-in-time `asset_inventory_snapshot` for trend analysis
# MAGIC 6. Emits a governance health summary
# MAGIC 7. Writes execution record to `job_run_history`
# MAGIC
# MAGIC **Outputs**: Writes to all control tables in `{catalog}.{control_schema}`

# COMMAND ----------

# DBTITLE 1,Widgets and config
dbutils.widgets.text("catalog",                          "")
dbutils.widgets.text("control_schema",                   "uc_hygiene")
dbutils.widgets.text("target_catalogs",                  "")
dbutils.widgets.text("notification_email",               "")
dbutils.widgets.text("scan_results_retention_days",       "90")
dbutils.widgets.text("plan_retention_days",               "180")
dbutils.widgets.text("cost_attribution_retention_days",   "365")


def require_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Missing required widget: '{name}'")
    return value


catalog        = require_widget("catalog")
control_schema = require_widget("control_schema")
target_catalogs_raw = dbutils.widgets.get("target_catalogs").strip()
target_catalogs = [c.strip() for c in target_catalogs_raw.split(",") if c.strip()] if target_catalogs_raw else []
notification_email = dbutils.widgets.get("notification_email").strip()

scan_results_retention_days     = int(dbutils.widgets.get("scan_results_retention_days")     or "90")
plan_retention_days             = int(dbutils.widgets.get("plan_retention_days")             or "180")
cost_attribution_retention_days = int(dbutils.widgets.get("cost_attribution_retention_days") or "365")

control_fqn = f"{catalog}.{control_schema}"

print(f"Control schema:  {control_fqn}")
print(f"Target catalogs: {target_catalogs or '(all — inventory snapshot only)'}")
print(f"Retention: scan_results={scan_results_retention_days}d  plans={plan_retention_days}d  cost={cost_attribution_retention_days}d")

# COMMAND ----------

# DBTITLE 1,Phase 2a — Prune old control table records
import time
from datetime import date

_start = time.time()
_today = date.today()
_stats = {}   # table -> rows_deleted

# ── 1. Prune scan_results ─────────────────────────────────────────────────────
before = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.scan_results").collect()[0]["n"]
spark.sql(f"""
DELETE FROM {control_fqn}.scan_results
WHERE scan_date < current_date() - INTERVAL {scan_results_retention_days} DAYS
""")
after = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.scan_results").collect()[0]["n"]
_stats["scan_results"] = before - after
print(f"scan_results:        {_stats['scan_results']:>6,} rows pruned  ({after:,} remaining)")

# ── 2. Prune completed/cancelled migration_plans ─────────────────────────────
before = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.migration_plans").collect()[0]["n"]
spark.sql(f"""
DELETE FROM {control_fqn}.migration_plans
WHERE plan_status IN ('completed', 'cancelled')
  AND created_at < current_timestamp() - INTERVAL {plan_retention_days} DAYS
""")
after = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.migration_plans").collect()[0]["n"]
_stats["migration_plans"] = before - after
print(f"migration_plans:     {_stats['migration_plans']:>6,} rows pruned  ({after:,} remaining)")

# ── 3. Prune old cost attribution records ──────────────────────────────────
before = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.cost_attribution_daily").collect()[0]["n"]
spark.sql(f"""
DELETE FROM {control_fqn}.cost_attribution_daily
WHERE usage_date < current_date() - INTERVAL {cost_attribution_retention_days} DAYS
""")
after = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.cost_attribution_daily").collect()[0]["n"]
_stats["cost_attribution_daily"] = before - after
print(f"cost_attribution_daily: {_stats['cost_attribution_daily']:>4,} rows pruned  ({after:,} remaining)")

# ── 4. Prune acknowledged notification_log older than plan_retention_days ───────
before = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.notification_log").collect()[0]["n"]
spark.sql(f"""
DELETE FROM {control_fqn}.notification_log
WHERE response_status IN ('acknowledged', 'action_taken', 'ignored')
  AND sent_at < current_timestamp() - INTERVAL {plan_retention_days} DAYS
""")
after = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.notification_log").collect()[0]["n"]
_stats["notification_log"] = before - after
print(f"notification_log:    {_stats['notification_log']:>6,} rows pruned  ({after:,} remaining)")

# COMMAND ----------

# DBTITLE 1,Phase 2b — Expire overdue certifications
# Tables whose next_review_date has passed and are still pending/certified get
# demoted to 'overdue' so they surface in the owner notifier.
expired_count_row = spark.sql(f"""
SELECT COUNT(*) AS n
FROM {control_fqn}.certification_state
WHERE current_status IN ('pending_review', 'certified')
  AND next_review_date < current_date()
""").collect()[0]
expired_count = expired_count_row["n"]

if expired_count > 0:
    spark.sql(f"""
    UPDATE {control_fqn}.certification_state
    SET current_status    = 'overdue',
        status_changed_at = CURRENT_TIMESTAMP(),
        status_changed_by = 'system:monthly_housekeeping'
    WHERE current_status IN ('pending_review', 'certified')
      AND next_review_date < current_date()
    """)
    print(f"Certification expirations: {expired_count:,} tables set to 'overdue'")
else:
    print("Certification expirations: 0 tables overdue (clean)")

# COMMAND ----------

# DBTITLE 1,Phase 2b2 — Expire and DROP stale bridge views
# Bridge views past their expires_at are DROPped from target catalogs and marked
# expired in the control table. This prevents orphaned views from accumulating.
# Safety: only drops views that UC Steward itself created (registered in bridge_views).

from lib.common import validate_identifier, safe_table_ref

_bridge_tbl = f"{control_fqn}.bridge_views"

try:
    expired_bridges = spark.sql(f"""
    SELECT bridge_view_fqn, plan_id
    FROM {_bridge_tbl}
    WHERE status = 'active'
      AND expires_at < current_timestamp()
    """).collect()
except Exception as e:
    expired_bridges = []
    print(f"  ⚠ Could not read bridge_views: {e}")

bridges_dropped = 0
for bridge in expired_bridges:
    fqn = bridge.bridge_view_fqn
    parts = fqn.split(".")
    if len(parts) != 3:
        print(f"  ⚠ Malformed bridge FQN, skipping: {fqn}")
        continue

    try:
        validate_identifier(parts[0], "catalog")
        validate_identifier(parts[1], "schema")
        validate_identifier(parts[2], "table")
        _ref = safe_table_ref(parts[0], parts[1], parts[2])
        spark.sql(f"DROP VIEW IF EXISTS {_ref}")
        bridges_dropped += 1
    except ValueError as e:
        print(f"  ⚠ Skipping invalid bridge identifier {fqn}: {e}")
        continue
    except Exception as e:
        print(f"  ⚠ DROP VIEW failed for {fqn}: {e}")
        continue

# Mark all expired bridges in control table
if expired_bridges:
    spark.sql(f"""
    UPDATE {_bridge_tbl}
    SET status = 'expired'
    WHERE status = 'active'
      AND expires_at < current_timestamp()
    """)

# Also mark corresponding migration_plans as completed if bridge was last step
if bridges_dropped > 0:
    spark.sql(f"""
    UPDATE {control_fqn}.migration_plans
    SET plan_status = 'completed',
        completed_at = current_timestamp(),
        completion_notes = 'Bridge expired and dropped by monthly housekeeping'
    WHERE plan_status = 'in_progress'
      AND plan_id IN (
        SELECT plan_id FROM {_bridge_tbl}
        WHERE status = 'expired'
          AND expires_at >= current_timestamp() - INTERVAL 1 DAY
      )
    """)

print(f"Bridge view cleanup: {bridges_dropped} expired views dropped, {len(expired_bridges)} marked expired")
_stats["bridge_views_expired"] = bridges_dropped

# COMMAND ----------

# DBTITLE 1,Phase 2c — Asset inventory snapshot
# Capture a point-in-time cross-catalog inventory for trend analysis.
# Uses information_schema per target catalog for efficiency.
if not target_catalogs:
    print("No target_catalogs configured — skipping asset inventory snapshot.")
else:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import TableType
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType

    _sdk = WorkspaceClient()
    _EXEMPT = frozenset(["information_schema", "__databricks_internal", control_schema, f"{control_schema}_dev"])

    rows = []
    for cat in target_catalogs:
        try:
            for sc in _sdk.schemas.list(catalog_name=cat):
                if sc.name in _EXEMPT:
                    continue
                for t in _sdk.tables.list(catalog_name=cat, schema_name=sc.name):
                    tags = {p.key: p.value for p in (t.properties or [])} if t.properties else {}
                    rows.append(Row(
                        snapshot_date     = str(_today),
                        catalog_name      = cat,
                        schema_name       = sc.name,
                        table_name        = t.name,
                        table_type        = t.table_type.value if t.table_type else "UNKNOWN",
                        owner             = t.owner or "",
                        row_count         = None,
                        size_bytes        = None,
                        has_owner_tag     = "owner" in tags,
                        has_domain_tag    = "domain" in tags,
                        has_quality_tag   = "quality_tier" in tags,
                        certification_status = tags.get("certification_status", "uncertified"),
                        last_altered      = None,
                        created_at        = None,
                    ))
        except Exception as e:
            print(f"  Warning: could not enumerate {cat}: {e}")

    if rows:
        schema = StructType([
            StructField("snapshot_date",          StringType(),    True),
            StructField("catalog_name",            StringType(),    True),
            StructField("schema_name",             StringType(),    True),
            StructField("table_name",              StringType(),    True),
            StructField("table_type",              StringType(),    True),
            StructField("owner",                   StringType(),    True),
            StructField("row_count",               LongType(),      True),
            StructField("size_bytes",              LongType(),      True),
            StructField("has_owner_tag",           BooleanType(),   True),
            StructField("has_domain_tag",          BooleanType(),   True),
            StructField("has_quality_tag",         BooleanType(),   True),
            StructField("certification_status",    StringType(),    True),
            StructField("last_altered",            TimestampType(), True),
            StructField("created_at",              TimestampType(), True),
        ])
        snap_df = spark.createDataFrame(rows, schema=schema)
        snap_df.createOrReplaceTempView("_inventory_snap")
        spark.sql(f"""
        INSERT INTO {control_fqn}.asset_inventory_snapshot
        SELECT
            CAST(snapshot_date AS DATE),
            catalog_name, schema_name, table_name, table_type, owner,
            row_count, size_bytes,
            has_owner_tag, has_domain_tag, has_quality_tag,
            certification_status, last_altered, created_at
        FROM _inventory_snap
        """)
        snap_count = len(rows)
        print(f"Asset inventory snapshot: {snap_count:,} tables captured across {len(target_catalogs)} catalog(s)")
    else:
        snap_count = 0
        print("Asset inventory snapshot: no tables found")

# COMMAND ----------

# DBTITLE 1,Phase 2d — Governance health summary
# Emit a governance health report based on current control table state.
print(f"\n{'='*60}")
print(f"  UC HYGIENE — MONTHLY HEALTH REPORT  ({_today})")
print(f"{'='*60}")

# Open violations by severity
print("\nOpen scan findings (unresolved):")
spark.sql(f"""
SELECT
  scan_type,
  finding_severity,
  COUNT(*) AS count
FROM {control_fqn}.scan_results
WHERE resolved_at IS NULL
GROUP BY scan_type, finding_severity
ORDER BY scan_type, finding_severity
""").show(truncate=False)

# Certification health
print("Certification state distribution:")
spark.sql(f"""
SELECT
  current_status,
  COUNT(*) AS table_count
FROM {control_fqn}.certification_state
GROUP BY current_status
ORDER BY current_status
""").show(truncate=False)

# Migration plan backlog
print("Migration plan backlog:")
spark.sql(f"""
SELECT
  plan_status,
  risk_level,
  COUNT(*) AS count
FROM {control_fqn}.migration_plans
WHERE plan_status NOT IN ('completed', 'cancelled')
GROUP BY plan_status, risk_level
ORDER BY plan_status, risk_level
""").show(truncate=False)

# 30-day notification response rate
print("Notification response rate (last 30 days):")
spark.sql(f"""
SELECT
  notification_type,
  COUNT(*)                                                    AS sent,
  SUM(CASE WHEN response_status = 'acknowledged'   THEN 1 ELSE 0 END) AS acknowledged,
  SUM(CASE WHEN response_status = 'action_taken'   THEN 1 ELSE 0 END) AS actioned,
  SUM(CASE WHEN response_status = 'ignored'        THEN 1 ELSE 0 END) AS ignored,
  ROUND(100.0 * SUM(CASE WHEN response_status IN ('acknowledged','action_taken')
    THEN 1 ELSE 0 END) / COUNT(*), 1)                        AS response_rate_pct
FROM {control_fqn}.notification_log
WHERE sent_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY notification_type
ORDER BY notification_type
""").show(truncate=False)

# Tag coverage trend (last 3 monthly snapshots)
print("Tag coverage trend (monthly snapshots):")
try:
    spark.sql(f"""
    SELECT
      snapshot_date,
      COUNT(*)                                                AS total_tables,
      ROUND(100.0 * SUM(CASE WHEN has_owner_tag   THEN 1 ELSE 0 END) / COUNT(*), 1) AS owner_tag_pct,
      ROUND(100.0 * SUM(CASE WHEN has_domain_tag  THEN 1 ELSE 0 END) / COUNT(*), 1) AS domain_tag_pct,
      ROUND(100.0 * SUM(CASE WHEN has_quality_tag THEN 1 ELSE 0 END) / COUNT(*), 1) AS quality_tag_pct
    FROM {control_fqn}.asset_inventory_snapshot
    WHERE snapshot_date >= current_date() - INTERVAL 90 DAYS
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
    LIMIT 3
    """).show(truncate=False)
except Exception as e:
    print(f"  (snapshot trend not available: {e})")

print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,Write job_run_history
_duration = int(time.time() - _start)
total_rows_pruned = sum(_stats.values())

spark.sql(f"""
INSERT INTO {control_fqn}.job_run_history VALUES (
  DATE('{_today}'),
  'uc_hygiene_monthly_housekeeping',
  'p2_housekeeping',
  'p2_housekeeping',
  'success',
  NULL,
  {total_rows_pruned},
  NULL,
  {_duration},
  'pruned={total_rows_pruned} expired_certs={expired_count} retention: scan={scan_results_retention_days}d plans={plan_retention_days}d cost={cost_attribution_retention_days}d',
  CURRENT_TIMESTAMP()
)
""")

print(f"\nHousekeeping complete in {_duration}s")
print(f"  Total rows pruned: {total_rows_pruned:,}")
print(f"  Certifications expired: {expired_count:,}")

# COMMAND ----------


