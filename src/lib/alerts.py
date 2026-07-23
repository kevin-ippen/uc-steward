"""SQL Alert query definitions for UC Steward real-time monitoring.

Defines alert queries that can be deployed as Databricks SQL Alerts.
Each alert has a query, trigger condition, and notification template.

Usage:
    from lib.alerts import ALERT_DEFINITIONS, create_alerts_sql

    # Get SQL for all alerts
    for alert in ALERT_DEFINITIONS:
        print(alert["name"], alert["query"])
"""
from __future__ import annotations

__all__ = ["ALERT_DEFINITIONS", "get_alert_query"]

# Each alert definition contains:
# - name: human-readable alert name
# - query: SQL query that returns rows when the alert condition is met
# - schedule_interval: how often to run (minutes)
# - description: what this alert monitors
# - severity: critical | warning | info

ALERT_DEFINITIONS = [
    {
        "name": "Critical Violations Unresolved >7d",
        "description": "Fires when critical-severity scan findings remain unresolved for more than 7 days.",
        "severity": "critical",
        "schedule_interval": 60,
        "query": """
            SELECT
                COUNT(*) AS unresolved_critical_count,
                COLLECT_LIST(CONCAT(catalog_name, '.', schema_name, '.', table_name)) AS affected_tables
            FROM {control_fqn}.scan_results
            WHERE finding_severity = 'critical'
              AND resolved_at IS NULL
              AND scan_date < CURRENT_DATE() - INTERVAL 7 DAYS
            HAVING COUNT(*) > 0
        """,
    },
    {
        "name": "Certification Expiry Warning",
        "description": "Fires when certified tables have next_review_date within the next 14 days.",
        "severity": "warning",
        "schedule_interval": 1440,  # daily
        "query": """
            SELECT
                COUNT(*) AS expiring_count,
                COLLECT_LIST(
                    CONCAT(catalog_name, '.', schema_name, '.', table_name, ' (', next_review_date, ')')
                ) AS expiring_tables
            FROM {control_fqn}.certification_state
            WHERE current_status = 'certified'
              AND next_review_date BETWEEN CURRENT_DATE() AND CURRENT_DATE() + INTERVAL 14 DAYS
            HAVING COUNT(*) > 0
        """,
    },
    {
        "name": "Feature Check Failures Detected",
        "description": "Fires when any feature check fails on the most recent scan.",
        "severity": "warning",
        "schedule_interval": 1440,  # daily
        "query": """
            SELECT
                COUNT(*) AS failed_checks,
                COLLECT_LIST(CONCAT(check_name, ' (', scope_name, ')')) AS failures
            FROM {control_fqn}.feature_check_results
            WHERE scan_date = (SELECT MAX(scan_date) FROM {control_fqn}.feature_check_results)
              AND status = 'FAILED'
            HAVING COUNT(*) > 0
        """,
    },
    {
        "name": "Schema Drift Critical Events",
        "description": "Fires when column removals or type changes are detected (critical/warning severity).",
        "severity": "critical",
        "schedule_interval": 1440,  # daily
        "query": """
            SELECT
                COUNT(*) AS drift_event_count,
                COLLECT_LIST(
                    CONCAT(drift_type, ': ', catalog_name, '.', schema_name, '.', table_name, '.', column_name)
                ) AS events
            FROM {control_fqn}.schema_drift_events
            WHERE severity IN ('critical', 'warning')
              AND detected_date = CURRENT_DATE()
              AND acknowledged_at IS NULL
            HAVING COUNT(*) > 0
        """,
    },
    {
        "name": "Stale Tables Without Owner Action",
        "description": "Fires when stale table notifications have been pending >30 days with no response.",
        "severity": "warning",
        "schedule_interval": 1440,  # daily
        "query": """
            SELECT
                COUNT(*) AS stale_notification_count,
                COLLECT_LIST(asset_reference) AS affected_assets
            FROM {control_fqn}.notification_log
            WHERE notification_type = 'stale_warning'
              AND response_status = 'pending'
              AND sent_at < CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
            HAVING COUNT(*) > 0
        """,
    },
    {
        "name": "Job Run Failures",
        "description": "Fires when any UC Steward job task fails.",
        "severity": "critical",
        "schedule_interval": 60,
        "query": """
            SELECT
                job_name, task_name, phase, run_notes,
                run_date
            FROM {control_fqn}.job_run_history
            WHERE run_status = 'failure'
              AND run_date >= CURRENT_DATE() - INTERVAL 1 DAY
        """,
    },
]


def get_alert_query(alert_name: str, control_fqn: str) -> str | None:
    """Get the rendered SQL for a specific alert, with control_fqn substituted."""
    for alert in ALERT_DEFINITIONS:
        if alert["name"] == alert_name:
            return alert["query"].format(control_fqn=control_fqn).strip()
    return None


def render_all_alerts(control_fqn: str) -> list[dict]:
    """Render all alert queries with the given control_fqn."""
    rendered = []
    for alert in ALERT_DEFINITIONS:
        rendered.append({
            **alert,
            "query": alert["query"].format(control_fqn=control_fqn).strip(),
        })
    return rendered
