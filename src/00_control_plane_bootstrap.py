# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 00 - Control Plane Bootstrap
# MAGIC
# MAGIC Creates the UC Steward control schema and all control tables required by the jobs. Safe to run repeatedly.
# MAGIC
# MAGIC **Purpose**
# MAGIC * Centralize schema/table bootstrap in one place
# MAGIC * Reduce duplicated DDL across notebooks
# MAGIC * Add enterprise control-plane tables for cost attribution and run tracking
# MAGIC

# COMMAND ----------

# DBTITLE 1,Bootstrap control tables
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("control_schema", "uc_hygiene")


def require_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Missing required widget: '{name}'")
    return value


catalog        = require_widget("catalog")
control_schema = require_widget("control_schema")
control_fqn    = f"{catalog}.{control_schema}"

# ── Shared TBLPROPERTIES for all control tables ──────────────────────────────
_TBLPROPS = """
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
)
"""

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {control_fqn}")
print(f"Schema: {control_fqn}")

# ── 1. scan_results ──────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.scan_results (
  scan_id           STRING   COMMENT 'UUID for the scan run',
  scan_date         DATE     COMMENT 'Date the scan executed',
  scan_type         STRING   COMMENT 'staleness | tag_compliance | naming | enrichment',
  asset_type        STRING   COMMENT 'table | model | notebook | dashboard | endpoint | file',
  catalog_name      STRING,
  schema_name       STRING,
  table_name        STRING,
  column_name       STRING   COMMENT 'Populated for column-level findings',
  finding_type      STRING,
  finding_severity  STRING   COMMENT 'critical | warning | info',
  finding_detail    STRING,
  recommended_action STRING,
  owner_email       STRING,
  resolved_at       TIMESTAMP,
  resolved_by       STRING
)
COMMENT 'Daily scan findings — staleness, tag gaps, naming violations'
{_TBLPROPS}
""")

# ── 2. certification_state ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.certification_state (
  catalog_name        STRING,
  schema_name         STRING,
  table_name          STRING,
  asset_type          STRING   COMMENT 'table | model | notebook | dashboard | endpoint | file',
  current_status      STRING   COMMENT 'uncertified | pending_review | certified | deprecated | overdue',
  status_changed_at   TIMESTAMP,
  status_changed_by   STRING,
  next_review_date    DATE,
  reviewer_email      STRING,
  certification_notes STRING
)
COMMENT 'Certification lifecycle state per table'
{_TBLPROPS}
""")

# ── Backward-compat: add asset_type to tables created before this column ─────
# ALTER TABLE ... ADD COLUMN IF NOT EXISTS is idempotent — safe on every run.
for _tbl in ("scan_results", "certification_state"):
    try:
        spark.sql(
            f"ALTER TABLE {control_fqn}.{_tbl} "
            "ADD COLUMN IF NOT EXISTS asset_type STRING "
            "COMMENT 'table | model | notebook | dashboard | endpoint | file'"
        )
    except Exception:
        pass  # column already present; CREATE TABLE above handles new deployments

# ── 3. migration_plans ───────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.migration_plans (
  plan_id            STRING,
  created_at         TIMESTAMP,
  catalog_name       STRING,
  schema_name        STRING,
  table_name         STRING,
  violation_type     STRING,
  proposed_new_name  STRING,
  upstream_count     INT,
  downstream_count   INT,
  upstream_entities  STRING,
  downstream_entities STRING,
  risk_level         STRING   COMMENT 'low | medium | high | critical',
  estimated_effort   STRING   COMMENT 'hours estimate',
  priority_score     FLOAT,
  migration_steps    STRING,
  rollback_plan      STRING,
  plan_status        STRING   COMMENT 'pending | approved | in_progress | completed | cancelled',
  approver           STRING,
  approved_at        TIMESTAMP,
  jira_issue_key     STRING,
  notes              STRING
)
COMMENT 'AI-generated migration and remediation plans'
{_TBLPROPS}
""")

# ── 4. bridge_views ──────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.bridge_views (
  bridge_view_fqn    STRING   COMMENT 'FQN of the compatibility view',
  original_table_fqn STRING,
  new_table_fqn      STRING,
  created_at         TIMESTAMP,
  expires_at         TIMESTAMP,
  bridge_status      STRING   COMMENT 'active | expired | removed',
  plan_id            STRING
)
COMMENT 'Backward-compat views created during table renames'
{_TBLPROPS}
""")

# ── 5. notification_log ──────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.notification_log (
  notification_id   STRING,
  sent_at           TIMESTAMP,
  recipient_email   STRING,
  notification_type STRING   COMMENT 'stale_warning | compliance_gap | escalation | archive_notice | certification_reminder',
  asset_reference   STRING,
  message_summary   STRING,
  response_status   STRING   COMMENT 'pending | acknowledged | action_taken | ignored',
  response_at       TIMESTAMP
)
COMMENT 'History of all owner notifications sent'
{_TBLPROPS}
""")

# ── 6. cost_attribution_daily ─────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.cost_attribution_daily (
  usage_date             DATE,
  workspace_id           STRING,
  framework_tag          STRING   COMMENT 'Tag value used to scope attribution (e.g. uc-steward)',
  attribution_level      STRING   COMMENT 'job | notebook | warehouse | pipeline | endpoint | app | unattributed',
  attribution_key        STRING,
  attribution_label      STRING,
  attribution_owner      STRING,
  billing_origin_product STRING,
  sku_name               STRING,
  usage_unit             STRING,
  usage_quantity         DECIMAL(38,10),
  list_unit_price        DECIMAL(38,10),
  estimated_cost         DECIMAL(38,10),
  record_count           BIGINT,
  source_table           STRING,
  created_at             TIMESTAMP
)
COMMENT 'Daily workload cost attribution for the UC Steward framework'
{_TBLPROPS}
""")

# ── 7. asset_inventory_snapshot ───────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.asset_inventory_snapshot (
  snapshot_date     DATE     COMMENT 'Date of the inventory capture',
  catalog_name      STRING,
  schema_name       STRING,
  table_name        STRING,
  table_type        STRING   COMMENT 'MANAGED | EXTERNAL | VIEW',
  owner             STRING,
  row_count         BIGINT,
  size_bytes        BIGINT,
  has_owner_tag     BOOLEAN,
  has_domain_tag    BOOLEAN,
  has_quality_tag   BOOLEAN,
  certification_status STRING,
  last_altered      TIMESTAMP,
  created_at        TIMESTAMP
)
COMMENT 'Point-in-time table inventory used for trend analysis and health reporting'
{_TBLPROPS}
""")

# ── 8. job_run_history ───────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.job_run_history (
  run_date          DATE,
  job_name          STRING,
  task_name         STRING,
  phase             STRING   COMMENT 'p1_bootstrap | p2_detection | p3_remediation | p4_communication | p5_observability',
  run_status        STRING   COMMENT 'success | failure | skipped',
  records_scanned   BIGINT,
  records_written   BIGINT,
  findings_count    BIGINT,
  duration_seconds  INT,
  run_notes         STRING,
  created_at        TIMESTAMP
)
COMMENT 'Execution history for all UC Steward tasks'
{_TBLPROPS}
""")

# ── 9. exemptions ─────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.exemptions (
  exemption_id  STRING    NOT NULL COMMENT 'UUID for this rule',
  pattern_type  STRING    NOT NULL COMMENT 'catalog | schema | table | table_prefix | schema_prefix',
  pattern       STRING    NOT NULL COMMENT 'Exact value or prefix to match against',
  reason        STRING    COMMENT 'Human-readable justification for this exemption',
  exempt_until  DATE      COMMENT 'Expiry date; NULL = permanent',
  created_by    STRING,
  created_at    TIMESTAMP NOT NULL,
  is_active     BOOLEAN   NOT NULL COMMENT 'Set false to disable without deleting the row'
)
COMMENT 'Pattern-based scan exemptions. Add rows here to exclude catalogs, schemas, or tables from hygiene scans. Complement with the uc_hygiene_exempt=true UC tag for per-object opt-out.'
{_TBLPROPS}
""")

# ── 10. governance_policy ─────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.governance_policy (
  policy_key     STRING    NOT NULL COMMENT 'Dot-delimited policy key (e.g. required_table_tags)',
  policy_value   STRING    NOT NULL COMMENT 'JSON-encoded value — scalars, lists, and dicts all JSON',
  description    STRING    COMMENT 'Human-readable description of this policy entry',
  is_active      BOOLEAN   NOT NULL COMMENT 'Set false to soft-delete without dropping the row',
  updated_at     TIMESTAMP NOT NULL,
  updated_by     STRING
)
COMMENT 'Canonical governance policy. Seeded from policies/policy.yml on every bootstrap run.'
{_TBLPROPS}
""")

# ── 11. feature_check_results ─────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.feature_check_results (
  check_name     STRING    NOT NULL COMMENT 'e.g. predictive_optimization_enabled, pii_columns_masked',
  tier           STRING    NOT NULL COMMENT 'standard | elevated',
  status         STRING    NOT NULL COMMENT 'PASSED | FAILED | SKIPPED',
  scope_type     STRING    NOT NULL COMMENT 'catalog | schema | table',
  scope_name     STRING    NOT NULL COMMENT 'FQN of the scope checked',
  message        STRING    COMMENT 'Human-readable detail or skip reason',
  checked_at     TIMESTAMP NOT NULL,
  scan_date      DATE      NOT NULL COMMENT 'Date of the scan run'
)
COMMENT 'Platform feature enablement check results. SKIPPED = SP lacks privileges for that check.'
{_TBLPROPS}
""")

# ── 12. external_tool_registry ────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.external_tool_registry (
  registry_id       STRING    NOT NULL COMMENT 'UUID for this entry',
  tool_type         STRING    NOT NULL COMMENT 'tableau | powerbi | looker | dbt | airflow | jdbc_client | other',
  tool_name         STRING    NOT NULL COMMENT 'Human-readable name (e.g. Finance Dashboard)',
  connection_method STRING    COMMENT 'jdbc | odbc | rest_api | dbt_ref | airflow_dag | manual',
  table_fqn         STRING    NOT NULL COMMENT 'Fully qualified table name this tool depends on',
  owner_email       STRING    COMMENT 'Team or individual owning this external dependency',
  notes             STRING,
  is_active         BOOLEAN   NOT NULL DEFAULT true,
  created_at        TIMESTAMP NOT NULL,
  created_by        STRING
)
COMMENT 'Manual registry of external tools that depend on governed tables but are invisible to UC lineage (BI tools, dbt, Airflow, etc.)'
{_TBLPROPS}
""")

# ── 13. schema_drift_events ───────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.schema_drift_events (
  event_id          STRING    NOT NULL COMMENT 'UUID for this drift event',
  detected_date     DATE      NOT NULL,
  catalog_name      STRING    NOT NULL,
  schema_name       STRING    NOT NULL,
  table_name        STRING    NOT NULL,
  drift_type        STRING    NOT NULL COMMENT 'column_added | column_removed | type_changed | nullable_changed',
  column_name       STRING    NOT NULL,
  previous_state    STRING    COMMENT 'JSON: previous type/nullable/comment',
  current_state     STRING    COMMENT 'JSON: current type/nullable/comment',
  severity          STRING    NOT NULL COMMENT 'info | warning | critical',
  acknowledged_at   TIMESTAMP,
  acknowledged_by   STRING
)
COMMENT 'Schema drift events detected by comparing current DESCRIBE output against last inventory snapshot'
{_TBLPROPS}
""")

# ── 14. schema_column_baseline ────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.schema_column_baseline (
  catalog_name    STRING NOT NULL,
  schema_name     STRING NOT NULL,
  table_name      STRING NOT NULL,
  column_name     STRING NOT NULL,
  data_type       STRING NOT NULL,
  is_nullable     BOOLEAN,
  column_comment  STRING,
  ordinal_position INT,
  captured_at     TIMESTAMP NOT NULL
)
COMMENT 'Column-level baseline for schema drift detection. Updated after each drift scan.'
{_TBLPROPS}
""")

# ── 15. cross_workspace_rollup ────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {control_fqn}.cross_workspace_rollup (
  rollup_date       DATE      NOT NULL COMMENT 'Date of the rollup',
  source_workspace  STRING    NOT NULL COMMENT 'Workspace ID or name',
  source_catalog    STRING    NOT NULL,
  metric_name       STRING    NOT NULL COMMENT 'tag_coverage_pct | cert_coverage_pct | critical_violations | stale_tables | feature_score',
  metric_value      DOUBLE    NOT NULL,
  metric_detail     STRING    COMMENT 'JSON with breakdown (e.g. per-schema)',
  collected_at      TIMESTAMP NOT NULL
)
COMMENT 'Aggregated hygiene metrics from multiple workspaces for central reporting'
{_TBLPROPS}
""")

# ── Summary: row counts per table ────────────────────────────────────────────
tables = [
    "scan_results", "certification_state", "migration_plans",
    "bridge_views", "notification_log", "cost_attribution_daily",
    "asset_inventory_snapshot", "job_run_history",
    "exemptions", "governance_policy", "feature_check_results",
    "external_tool_registry", "schema_drift_events",
    "schema_column_baseline", "cross_workspace_rollup",
]
print(f"\n{'='*52}")
print(f"  Control plane ready: {control_fqn}")
print(f"{'='*52}")
for t in tables:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {control_fqn}.{t}").collect()[0]["n"]
        print(f"  {t:<35} {cnt:>8,} rows")
    except Exception as e:
        print(f"  {t:<35} ERROR: {e}")
print(f"{'='*52}")


# COMMAND ----------

# DBTITLE 1,Seed governance_policy from policies/policy.yml
# ── Seed governance_policy from policies/policy.yml ─────────────────────────────
# Idempotent MERGE: safe to run repeatedly.
# Operator workflow: edit policies/policy.yml → re-run this notebook → policy takes effect.
import yaml, json
from datetime import datetime, timezone

_nb_path = (
    dbutils.notebook.entry_point.getDbutils().notebook()
    .getContext().notebookPath().get()
)
_repo_root  = '/Workspace' + '/'.join(_nb_path.split('/')[:-2])
_policy_path = f"{_repo_root}/policies/policy.yml"

with open(_policy_path) as _f:
    _raw_policy = yaml.safe_load(_f)

_SKIP_KEYS   = {"version", "effective_date"}
_now         = datetime.now(timezone.utc).replace(tzinfo=None)
_current_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

_policy_rows = [
    (k, json.dumps(v), True, _now, _current_user)
    for k, v in _raw_policy.items()
    if k not in _SKIP_KEYS
]

(
    spark.createDataFrame(
        _policy_rows,
        "policy_key STRING, policy_value STRING, is_active BOOLEAN, "
        "updated_at TIMESTAMP, updated_by STRING",
    ).createOrReplaceTempView("_policy_seed")
)

spark.sql(f"""
MERGE INTO {control_fqn}.governance_policy tgt
USING _policy_seed src ON tgt.policy_key = src.policy_key
WHEN MATCHED THEN UPDATE SET
  policy_value = src.policy_value,
  is_active    = src.is_active,
  updated_at   = src.updated_at,
  updated_by   = src.updated_by
WHEN NOT MATCHED THEN INSERT
  (policy_key, policy_value, is_active, updated_at, updated_by)
VALUES
  (src.policy_key, src.policy_value, src.is_active, src.updated_at, src.updated_by)
""")

print(f"  governance_policy: upserted {len(_policy_rows)} entries from {_policy_path}")

