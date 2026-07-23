"""Auto-GRANT/REVOKE proposal engine for UC Steward.

Based on certification_state, proposes:
- GRANT SELECT to appropriate groups for certified tables
- REVOKE SELECT proposals for deprecated tables
- Writes proposals to migration_plans with violation_type = 'access_proposal'

Does NOT auto-execute grants. Creates proposals for human review.

Usage:
    from lib.access_proposals import generate_access_proposals

    proposals_df = generate_access_proposals(
        spark, catalog, control_schema,
        grant_group="data_consumers",      # group to GRANT for certified tables
        revoke_after_days=30,              # days after deprecation before proposing REVOKE
    )
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from .common import safe_table_ref

__all__ = ["generate_access_proposals"]


def generate_access_proposals(
    spark: Any,
    catalog: str,
    control_schema: str,
    grant_group: str = "data_consumers",
    revoke_after_days: int = 30,
) -> Any:
    """Generate GRANT/REVOKE proposals based on certification status.

    - Certified tables without existing proposals -> propose GRANT SELECT
    - Deprecated tables past the grace period -> propose REVOKE SELECT

    Returns a DataFrame of newly created proposals.
    """
    plans_ref = safe_table_ref(catalog, control_schema, "migration_plans")
    cert_ref = safe_table_ref(catalog, control_schema, "certification_state")
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # --- GRANT proposals for certified tables ---
    # Find certified tables that don't already have an access_proposal
    grant_candidates = spark.sql(f"""
        SELECT c.catalog_name, c.schema_name, c.table_name
        FROM {cert_ref} c
        WHERE c.current_status = 'certified'
          AND NOT EXISTS (
            SELECT 1 FROM {plans_ref} p
            WHERE p.catalog_name = c.catalog_name
              AND p.schema_name = c.schema_name
              AND p.table_name = c.table_name
              AND p.violation_type = 'access_grant_proposal'
              AND p.plan_status IN ('pending', 'approved', 'completed')
          )
    """).collect()

    # --- REVOKE proposals for deprecated tables past grace period ---
    revoke_candidates = spark.sql(f"""
        SELECT c.catalog_name, c.schema_name, c.table_name, c.status_changed_at
        FROM {cert_ref} c
        WHERE c.current_status = 'deprecated'
          AND c.status_changed_at < TIMESTAMP '{now.isoformat()}' - INTERVAL {revoke_after_days} DAYS
          AND NOT EXISTS (
            SELECT 1 FROM {plans_ref} p
            WHERE p.catalog_name = c.catalog_name
              AND p.schema_name = c.schema_name
              AND p.table_name = c.table_name
              AND p.violation_type = 'access_revoke_proposal'
              AND p.plan_status IN ('pending', 'approved', 'completed')
          )
    """).collect()

    proposals = []

    for row in grant_candidates:
        fqn = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
        proposals.append((
            str(uuid.uuid4()),          # plan_id
            now,                         # created_at
            row["catalog_name"],
            row["schema_name"],
            row["table_name"],
            "access_grant_proposal",     # violation_type
            None,                        # proposed_new_name
            0, 0, None, None,            # lineage counts
            "low",                       # risk_level
            "minutes",                   # estimated_effort
            0.5,                         # priority_score
            f"GRANT SELECT ON TABLE {fqn} TO `{grant_group}`",  # migration_steps
            f"REVOKE SELECT ON TABLE {fqn} FROM `{grant_group}`",  # rollback_plan
            "pending",                   # plan_status
            None, None, None,            # approver, approved_at, jira_issue_key
            f"Auto-proposed: table is certified, grant to {grant_group}",  # notes
        ))

    for row in revoke_candidates:
        fqn = f"{row['catalog_name']}.{row['schema_name']}.{row['table_name']}"
        proposals.append((
            str(uuid.uuid4()),
            now,
            row["catalog_name"],
            row["schema_name"],
            row["table_name"],
            "access_revoke_proposal",
            None,
            0, 0, None, None,
            "medium",
            "minutes",
            0.7,
            f"REVOKE SELECT ON TABLE {fqn} FROM `{grant_group}`",
            f"GRANT SELECT ON TABLE {fqn} TO `{grant_group}`",
            "pending",
            None, None, None,
            f"Auto-proposed: table deprecated >{revoke_after_days}d, revoke from {grant_group}",
        ))

    if not proposals:
        return spark.sql(f"SELECT * FROM {plans_ref} WHERE 1=0")

    schema_str = (
        "plan_id STRING, created_at TIMESTAMP, catalog_name STRING, "
        "schema_name STRING, table_name STRING, violation_type STRING, "
        "proposed_new_name STRING, upstream_count INT, downstream_count INT, "
        "upstream_entities STRING, downstream_entities STRING, "
        "risk_level STRING, estimated_effort STRING, priority_score FLOAT, "
        "migration_steps STRING, rollback_plan STRING, plan_status STRING, "
        "approver STRING, approved_at TIMESTAMP, jira_issue_key STRING, notes STRING"
    )
    proposals_df = spark.createDataFrame(proposals, schema_str)
    proposals_df.createOrReplaceTempView("_access_proposals")
    spark.sql(f"INSERT INTO {plans_ref} SELECT * FROM _access_proposals")

    return proposals_df
