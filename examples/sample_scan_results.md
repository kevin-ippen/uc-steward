# Sample scan_results Output

These are redacted examples of what UC Steward produces after a daily governance run.

## Staleness Findings

| scan_date | catalog | schema | table | finding_type | severity | detail | recommended_action |
|-----------|---------|--------|-------|-------------|----------|--------|-------------------|
| 2026-07-01 | prod | analytics | user_sessions_backup_v2 | stale_table | warning | Last accessed 94 days ago | Archive or drop — no downstream consumers |
| 2026-07-01 | prod | raw | legacy_orders_2023 | stale_table | critical | Last accessed 187 days ago, 42GB storage | Archive to cold storage, $380/quarter savings |
| 2026-07-01 | prod | staging | tmp_migration_20250315 | stale_table | warning | Last accessed 108 days ago | Drop — temp table from completed migration |

## Tag Compliance Findings

| scan_date | catalog | schema | table | finding_type | severity | detail |
|-----------|---------|--------|-------|-------------|----------|--------|
| 2026-07-01 | prod | gold | customer_360 | missing_tag | critical | Missing: owner, domain |
| 2026-07-01 | prod | gold | revenue_daily | missing_tag | warning | Missing: quality_tier |
| 2026-07-01 | prod | silver | events_parsed | missing_tag | warning | Missing: owner |

## Naming Convention Violations

| scan_date | catalog | schema | table | finding_type | severity | detail | recommended_action |
|-----------|---------|--------|-------|-------------|----------|--------|-------------------|
| 2026-07-01 | prod | analytics | UserSessions | naming_violation | warning | PascalCase table name | Rename to user_sessions + bridge view |
| 2026-07-01 | prod | raw | DIM_Store | naming_violation | warning | Mixed case with prefix | Rename to store_dim + bridge view |
