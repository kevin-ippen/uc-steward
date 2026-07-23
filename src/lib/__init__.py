"""UC Steward shared library.

Modules:
    common           - Shared helpers (validation, table refs, exemptions)
    policy           - Policy accessors (load from governance_policy table)
    feature_checks   - Platform feature enablement probes
    schema_drift     - Schema drift detection (column add/remove/type change)
    jira_sync        - Bi-directional Jira ticket status sync
    access_proposals - Auto-GRANT/REVOKE proposal generation
    alerts           - SQL Alert query definitions
    governance_score - Composite governance score + dashboard datasets
"""
