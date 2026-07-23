# Required Permissions

This document details the Unity Catalog permissions needed to run UC Steward.

## Minimum Viable Permissions (Read-Only Mode)

For detection and plan generation only (no remediation):

```sql
-- On each target catalog
GRANT USE CATALOG ON CATALOG <target_catalog> TO `uc-steward-sp`;
GRANT USE SCHEMA ON CATALOG <target_catalog>.* TO `uc-steward-sp`;

-- System tables (staleness + cost)
GRANT SELECT ON TABLE system.access.table_lineage TO `uc-steward-sp`;
GRANT SELECT ON TABLE system.billing.usage TO `uc-steward-sp`;
GRANT SELECT ON TABLE system.query.history TO `uc-steward-sp`;

-- Control schema (full access)
GRANT ALL PRIVILEGES ON SCHEMA <catalog>.<control_schema> TO `uc-steward-sp`;
```

## Full Permissions (Detection + Remediation)

Adds the ability to create bridge views, set tags, and write to target schemas:

```sql
-- Everything from read-only, plus:

-- Bridge view creation
GRANT CREATE TABLE ON SCHEMA <target_catalog>.<schema> TO `uc-steward-sp`;

-- Tag management (for auto-remediation of tag violations)
GRANT APPLY TAG ON SCHEMA <target_catalog>.<schema> TO `uc-steward-sp`;

-- Information schema access (for schema drift detection)
GRANT SELECT ON TABLE <target_catalog>.information_schema.columns TO `uc-steward-sp`;
GRANT SELECT ON TABLE <target_catalog>.information_schema.table_tags TO `uc-steward-sp`;
GRANT SELECT ON TABLE <target_catalog>.information_schema.column_tags TO `uc-steward-sp`;
```

## Feature Enablement Checks (Elevated Mode)

When `feature_check_mode: "elevated"`:

```sql
-- Classification scan coverage
GRANT SELECT ON TABLE system.data_classification.__table_statuses TO `uc-steward-sp`;

-- Lakehouse monitoring
-- Requires metastore admin or workspace-level quality_monitors access
```

> **Note:** In `standard` mode, checks requiring admin access gracefully SKIP
> (not FAIL), so you can run UC Steward without these permissions.

## Jira Integration

If `jira_base_url` is configured:

```sql
-- Secret scope access (for API token)
-- Configure via Databricks CLI:
databricks secrets put-secret uc-steward jira-api-token
databricks secrets put-secret uc-steward jira-email
```

## AI Features

If `enable_ai_plans: "true"` or `enable_ai_descriptions: "true"`:

- The service principal needs access to the configured model endpoint
- Default model: `databricks-gemini-3-5-flash` (pay-per-token, no endpoint needed)
- The `ai_query()` SQL function requires the SP to have AI function permissions

## Security Notes

1. **Principle of least privilege:** Start with read-only mode (`dry_run: "true"`) and grant remediation permissions only after validating scan results.

2. **No data access needed:** UC Steward never reads table *data* — only metadata, lineage, and system tables.

3. **Exemptions:** Use the `exemptions` table to exclude sensitive schemas from scanning entirely.

4. **Audit trail:** All actions are logged to `job_run_history` and `notification_log`.
