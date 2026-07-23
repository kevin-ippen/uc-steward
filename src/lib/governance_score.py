"""Governance score calculation for UC Steward dashboards.

Provides dataset queries for the full governance score dashboard:
- Tag coverage percentage
- Violation backlog (open critical/warning findings)
- Certification distribution
- Overall governance composite score

Usage:
    from lib.governance_score import get_dashboard_datasets

    datasets = get_dashboard_datasets(control_fqn)
    for name, query in datasets.items():
        spark.sql(query).display()
"""
from __future__ import annotations

__all__ = ["get_dashboard_datasets", "compute_composite_score"]


def get_dashboard_datasets(control_fqn: str) -> dict[str, str]:
    """Return named SQL queries for the governance score dashboard."""
    return {
        # ── Overall composite governance score ────────────────────────────────
        "governance_composite_score": f"""
            WITH tag_coverage AS (
                SELECT
                    COALESCE(
                        100.0 * SUM(CASE WHEN has_owner_tag THEN 1 ELSE 0 END) /
                        NULLIF(COUNT(*), 0), 0
                    ) AS owner_tag_pct,
                    COALESCE(
                        100.0 * SUM(CASE WHEN has_domain_tag THEN 1 ELSE 0 END) /
                        NULLIF(COUNT(*), 0), 0
                    ) AS domain_tag_pct,
                    COALESCE(
                        100.0 * SUM(CASE WHEN has_quality_tag THEN 1 ELSE 0 END) /
                        NULLIF(COUNT(*), 0), 0
                    ) AS quality_tag_pct
                FROM {control_fqn}.asset_inventory_snapshot
                WHERE snapshot_date = (
                    SELECT MAX(snapshot_date)
                    FROM {control_fqn}.asset_inventory_snapshot
                )
            ),
            cert_coverage AS (
                SELECT
                    COALESCE(
                        100.0 * SUM(CASE WHEN current_status = 'certified' THEN 1 ELSE 0 END) /
                        NULLIF(COUNT(*), 0), 0
                    ) AS certified_pct
                FROM {control_fqn}.certification_state
            ),
            feature_score AS (
                SELECT COALESCE(
                    100.0 * SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) /
                    NULLIF(SUM(CASE WHEN status IN ('PASSED', 'FAILED') THEN 1 ELSE 0 END), 0),
                    0
                ) AS feature_pct
                FROM {control_fqn}.feature_check_results
                WHERE scan_date = (
                    SELECT MAX(scan_date) FROM {control_fqn}.feature_check_results
                )
            ),
            violation_backlog AS (
                SELECT COUNT(*) AS open_violations
                FROM {control_fqn}.scan_results
                WHERE resolved_at IS NULL
                  AND finding_severity IN ('critical', 'warning')
            )
            SELECT
                ROUND((tc.owner_tag_pct + tc.domain_tag_pct + tc.quality_tag_pct) / 3, 1) AS tag_coverage_score,
                ROUND(cc.certified_pct, 1) AS certification_score,
                ROUND(fs.feature_pct, 1) AS feature_score,
                vb.open_violations,
                ROUND(
                    (
                        (tc.owner_tag_pct + tc.domain_tag_pct + tc.quality_tag_pct) / 3 * 0.3
                        + cc.certified_pct * 0.3
                        + fs.feature_pct * 0.3
                        + GREATEST(0, 100 - vb.open_violations * 5) * 0.1
                    ), 1
                ) AS composite_score
            FROM tag_coverage tc
            CROSS JOIN cert_coverage cc
            CROSS JOIN feature_score fs
            CROSS JOIN violation_backlog vb
        """,

        # ── Tag coverage breakdown ────────────────────────────────────────────
        "tag_coverage_breakdown": f"""
            SELECT
                'owner' AS tag_type,
                COUNT(*) AS total_tables,
                SUM(CASE WHEN has_owner_tag THEN 1 ELSE 0 END) AS tagged_tables,
                ROUND(100.0 * SUM(CASE WHEN has_owner_tag THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) AS coverage_pct
            FROM {control_fqn}.asset_inventory_snapshot
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {control_fqn}.asset_inventory_snapshot)
            UNION ALL
            SELECT
                'domain' AS tag_type,
                COUNT(*),
                SUM(CASE WHEN has_domain_tag THEN 1 ELSE 0 END),
                ROUND(100.0 * SUM(CASE WHEN has_domain_tag THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1)
            FROM {control_fqn}.asset_inventory_snapshot
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {control_fqn}.asset_inventory_snapshot)
            UNION ALL
            SELECT
                'quality' AS tag_type,
                COUNT(*),
                SUM(CASE WHEN has_quality_tag THEN 1 ELSE 0 END),
                ROUND(100.0 * SUM(CASE WHEN has_quality_tag THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1)
            FROM {control_fqn}.asset_inventory_snapshot
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {control_fqn}.asset_inventory_snapshot)
        """,

        # ── Violation backlog by severity ─────────────────────────────────────
        "violation_backlog": f"""
            SELECT
                finding_severity,
                COUNT(*) AS open_count,
                MIN(scan_date) AS oldest_finding_date,
                MAX(scan_date) AS newest_finding_date
            FROM {control_fqn}.scan_results
            WHERE resolved_at IS NULL
            GROUP BY finding_severity
            ORDER BY CASE finding_severity
                WHEN 'critical' THEN 1
                WHEN 'warning' THEN 2
                WHEN 'info' THEN 3
            END
        """,

        # ── Certification distribution ────────────────────────────────────────
        "certification_distribution": f"""
            SELECT
                current_status,
                COUNT(*) AS table_count,
                ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 1) AS pct
            FROM {control_fqn}.certification_state
            GROUP BY current_status
            ORDER BY CASE current_status
                WHEN 'certified' THEN 1
                WHEN 'pending_review' THEN 2
                WHEN 'uncertified' THEN 3
                WHEN 'overdue' THEN 4
                WHEN 'deprecated' THEN 5
            END
        """,

        # ── Governance trend (weekly composite scores) ────────────────────────
        "governance_trend": f"""
            SELECT
                rollup_date,
                metric_name,
                metric_value
            FROM {control_fqn}.cross_workspace_rollup
            WHERE metric_name IN ('tag_coverage_pct', 'cert_coverage_pct', 'feature_score', 'composite_score')
            ORDER BY rollup_date, metric_name
        """,
    }


def compute_composite_score(spark, control_fqn: str) -> float:
    """Compute and return the current composite governance score (0-100)."""
    datasets = get_dashboard_datasets(control_fqn)
    row = spark.sql(datasets["governance_composite_score"]).first()
    return float(row["composite_score"]) if row else 0.0
