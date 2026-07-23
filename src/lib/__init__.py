"""
UC Steward — shared library modules.

Modules:
  common.py          — Validation, safe SQL interpolation, exemption checks
  policy.py          — Governance policy accessors (from policies/policy.yml)
  feature_checks.py  — Platform feature enablement probes
  schema_drift.py    — Schema drift detection engine
  jira_sync.py       — Bi-directional Jira status sync
  access_proposals.py — GRANT/REVOKE access proposals
  alerts.py          — SQL alert definitions
  governance_score.py — Composite certification-readiness score
  decertification.py — Decertification candidate detection (certified + drifted/stale)
  domain_scanner.py  — Domain assignment scanner with majority-vote suggestions
  cost_staleness.py  — Stale asset cost attribution (annualized waste)
"""
