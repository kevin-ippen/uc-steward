"""
UC Steward — shared library modules.

Architecture note: UC Steward operates in the "risky-per-asset" space —
anything touching a name or schema where a consumer breaks needs lineage
scoping, a compatibility layer, and rollback. Bulk-safe operations (tags,
classification, metadata quality at scale) are Hub's domain; we complement
by writing trust signals back into UC after approved transitions.

Modules:
  common.py           — Validation, safe SQL interpolation, exemption checks
  policy.py           — Governance policy accessors (from policies/policy.yml)
  feature_checks.py   — Platform feature enablement probes
  schema_drift.py     — Schema drift detection engine (column-level)
  semantic_drift.py   — Semantic invalidation routing (drift → owner review)
  jira_sync.py        — Bi-directional Jira status sync
  access_proposals.py — GRANT/REVOKE access proposals
  alerts.py           — SQL alert definitions
  governance_score.py — Adoption readiness score + dashboard datasets
  decertification.py  — Decertification candidate detection
  domain_scanner.py   — Domain assignment scanner (reads native UC domains)
  cost_staleness.py   — Stale asset cost attribution (annualized waste)

Design principles:
  - Propose, don't author (steward approves, tool executes)
  - Native is system of record (tags, domains, certification status)
  - Bridge views are the unit of safe change
  - Detect exceptions and conflicts, don't rebuild native vocabularies
"""
