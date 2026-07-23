"""
UC Steward — Tag Taxonomy Reference

This file is a REFERENCE DOCUMENT only. It is not imported at runtime.

Runtime governance policy is stored in the governance_policy control table
and loaded via src/lib/policy.py:

    from lib.policy import load_policy, required_table_tags, pii_patterns, ...
    _policy = load_policy(spark, catalog, control_schema)

To change a policy value:
  1. Edit policies/policy.yml
  2. Re-run 00_control_plane_bootstrap (MERGE is idempotent)
  3. The new value is active on the next notebook run — no code changes needed.

Allowed tag values below are informational; they drive tag validation in the
certification workflow and can be added to governed-tag attribute definitions.
"""

# ============================================================
# TAG TAXONOMY (Governed Tags)
# ============================================================

REQUIRED_TABLE_TAGS = {
    "owner": {
        "description": "Email of responsible team/individual",
        "example": "data-engineering@company.com",
        "validation": r"^[\w.+-]+@[\w-]+\.[\w.-]+$"
    },
    "domain": {
        "description": "Business domain this table belongs to",
        "allowed_values": [
            "finance", "marketing", "operations", "product",
            "customer", "supply_chain", "hr", "engineering"
        ]
    },
    "quality_tier": {
        "description": "Data quality/medallion tier",
        "allowed_values": ["bronze", "silver", "gold", "platinum"]
    },
}

REQUIRED_COLUMN_TAGS_WHEN_PII = {
    "sensitivity": {
        "description": "Sensitivity classification",
        "allowed_values": ["public", "internal", "confidential", "restricted"]
    },
    "pii": {
        "description": "Whether column contains PII",
        "allowed_values": ["true", "false"]
    },
    "pii_type": {
        "description": "Type of PII if pii=true",
        "allowed_values": [
            "email", "phone", "ssn", "name", "address",
            "dob", "ip_address", "quasi-identifier"
        ]
    }
}

OPTIONAL_TABLE_TAGS = {
    "certification_status": {
        "allowed_values": ["uncertified", "pending_review", "certified", "deprecated"]
    },
    "last_certified_date": {"description": "ISO date of last certification"},
    "retention_days": {"description": "Numeric retention period"},
    "regulation": {"allowed_values": ["GDPR", "CCPA", "HIPAA", "SOX", "PCI"]},
    "sla_tier": {"allowed_values": ["bronze", "silver", "gold", "platinum"]},
}


# ============================================================
# NAMING CONVENTIONS
# ============================================================

NAMING_RULES = {
    "schema": {
        "pattern": r"^[a-z][a-z0-9_]{2,63}$",
        "description": "lowercase, starts with letter, 3-64 chars, underscores only"
    },
    "table": {
        "pattern": r"^[a-z][a-z0-9_]{2,127}$",
        "description": "lowercase, starts with letter, 3-128 chars, underscores only"
    },
    "column": {
        "pattern": r"^[a-z][a-z0-9_]{0,127}$",
        "description": "lowercase, starts with letter, underscores only"
    },
    "volume": {
        "pattern": r"^[a-z][a-z0-9_-]{2,63}$",
        "description": "lowercase, starts with letter, hyphens and underscores"
    }
}

# Prefixes that indicate table purpose (optional enforcement)
TABLE_PREFIX_CONVENTIONS = {
    "fact_": "Fact tables (measures/events)",
    "dim_": "Dimension tables (descriptive attributes)",
    "stg_": "Staging tables (intermediate processing)",
    "raw_": "Raw ingestion tables",
    "agg_": "Pre-aggregated tables",
    "ref_": "Reference/lookup tables",
    "tmp_": "Temporary tables (auto-archive candidates)",
}


# ============================================================
# PII DETECTION PATTERNS (for auto-enrichment)
# ============================================================

PII_COLUMN_PATTERNS = {
    "email": [r"email", r"e_mail", r"email_address"],
    "phone": [r"phone", r"mobile", r"cell", r"fax", r"tel"],
    "ssn": [r"ssn", r"social_security", r"sin_number"],
    "name": [r"first_name", r"last_name", r"full_name", r"customer_name"],
    "address": [r"address", r"street", r"city", r"zip", r"postal"],
    "dob": [r"date_of_birth", r"dob", r"birth_date", r"birthday"],
    "ip_address": [r"ip_addr", r"ip_address", r"client_ip", r"source_ip"],
}


# ============================================================
# CONTROL TABLE SCHEMAS
# ============================================================

CONTROL_TABLES = {
    # Bootstrapped by 00_control_plane_bootstrap on every job run.
    # Pruned by 09_monthly_housekeeping per retention settings.
    "scan_results": {
        "description": "Daily scan findings — staleness, tag gaps, naming violations",
        "columns": [
            "scan_id STRING",
            "scan_date DATE",
            "scan_type STRING",          # staleness|tag_compliance|naming|enrichment
            "asset_type STRING",         # table|model|notebook|dashboard|endpoint|file
            "catalog_name STRING",
            "schema_name STRING",
            "table_name STRING",
            "column_name STRING",
            "finding_type STRING",
            "finding_severity STRING",   # critical|warning|info
            "finding_detail STRING",
            "recommended_action STRING",
            "owner_email STRING",
            "resolved_at TIMESTAMP",
            "resolved_by STRING",
        ]
    },
    "certification_state": {
        "description": "Certification lifecycle state per governable asset",
        "columns": [
            "catalog_name STRING",
            "schema_name STRING",
            "table_name STRING",
            "asset_type STRING",         # table|model|notebook|dashboard|endpoint|file
            "current_status STRING",     # uncertified|pending_review|certified|deprecated|overdue
            "status_changed_at TIMESTAMP",
            "status_changed_by STRING",
            "next_review_date DATE",
            "reviewer_email STRING",
            "certification_notes STRING",
        ]
    },
    "migration_plans": {
        "description": "AI-generated and heuristic migration/rename plans",
        "columns": [
            "plan_id STRING",
            "created_at TIMESTAMP",
            "catalog_name STRING",
            "schema_name STRING",
            "table_name STRING",
            "violation_type STRING",
            "proposed_new_name STRING",
            "upstream_count INT",
            "downstream_count INT",
            "upstream_entities STRING",  # JSON array
            "downstream_entities STRING",# JSON array
            "risk_level STRING",          # low|medium|high|critical
            "estimated_effort STRING",
            "priority_score FLOAT",       # 0-100
            "migration_steps STRING",     # JSON array
            "rollback_plan STRING",
            "plan_status STRING",         # proposed|approved|in_progress|completed|cancelled
            "approved_by STRING",
            "approved_at TIMESTAMP",
            "completed_at TIMESTAMP",
            "execution_notes STRING",
        ]
    },
    "bridge_views": {
        "description": "Backward-compatible views created during table renames",
        "columns": [
            "bridge_view_fqn STRING",
            "original_table_fqn STRING",
            "new_table_fqn STRING",
            "created_at TIMESTAMP",
            "expires_at TIMESTAMP",
            "plan_id STRING",
        ]
    },
    "notification_log": {
        "description": "History of all owner notifications sent",
        "columns": [
            "notification_id STRING",
            "sent_at TIMESTAMP",
            "recipient_email STRING",
            "notification_type STRING",  # hygiene_alert|escalation|deprecation_warning
            "asset_reference STRING",
            "message_summary STRING",
            "response_status STRING",    # pending|acknowledged|action_taken|ignored
            "response_at TIMESTAMP",
        ]
    },
    "cost_attribution_daily": {
        "description": "Daily workload cost attribution for this framework",
        "columns": [
            "usage_date DATE",
            "workspace_id STRING",
            "attribution_label STRING",
            "usage_quantity DECIMAL(38,10)",
            "estimated_cost DECIMAL(38,10)",
            "currency_code STRING",
            "framework_tag STRING",
        ]
    },
    "asset_inventory_snapshot": {
        "description": "Monthly point-in-time table inventory for trend analysis",
        "columns": [
            "snapshot_date DATE",
            "catalog_name STRING",
            "schema_name STRING",
            "table_name STRING",
            "table_type STRING",
            "owner STRING",
            "row_count BIGINT",
            "size_bytes BIGINT",
            "has_owner_tag BOOLEAN",
            "has_domain_tag BOOLEAN",
            "has_quality_tag BOOLEAN",
            "certification_status STRING",
            "last_altered TIMESTAMP",
            "created_at TIMESTAMP",
        ]
    },
    "job_run_history": {
        "description": "Execution history for all UC Steward tasks",
        "columns": [
            "run_date DATE",
            "job_name STRING",
            "task_key STRING",
            "phase STRING",
            "status STRING",             # success|failure|skipped
            "records_scanned BIGINT",
            "findings_count BIGINT",
            "duration_seconds INT",
            "error_message STRING",
            "run_metadata STRING",
            "created_at TIMESTAMP",
        ]
    },
}


# ============================================================
# STALENESS THRESHOLDS
# ============================================================

STALENESS_TIERS = {
    "warning": 30,    # Days since last access → stale_warning (configurable via staleness_days widget)
    "critical": 60,   # 2× warning threshold → stale_critical (hardcoded multiplier in 01_staleness_detector)
    # Note: escalation cadence is controlled by the escalation_days widget (default 7d),
    # not by a fixed threshold here.
}

# Tables/schemas exempt from staleness checks
STALENESS_EXEMPTIONS = [
    "information_schema",
    "uc_hygiene",       # Our own control schema
    "uc_hygiene_dev",
    "__databricks_internal",
]
