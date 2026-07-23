"""Bi-directional Jira sync for UC Steward migration plans.

Pulls ticket status from Jira and updates migration_plans accordingly:
- Jira ticket closed/done -> plan_status = 'completed'
- Jira ticket cancelled -> plan_status = 'cancelled'
- Jira ticket in progress -> plan_status = 'in_progress'

Uses Databricks SQL http_request() for API calls (no external Python deps).
Requires Jira credentials in a Databricks secret scope.

Usage:
    from lib.jira_sync import sync_jira_statuses

    updated = sync_jira_statuses(
        spark, dbutils, catalog, control_schema,
        jira_url="https://your-org.atlassian.net",
        secret_scope="uc-steward",
        secret_key="jira-api-token",
        jira_email_key="jira-email",
    )
"""
from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from typing import Any

from .common import safe_table_ref

__all__ = ["sync_jira_statuses"]


def _fetch_jira_status_via_sql(
    spark: Any,
    jira_url: str,
    issue_key: str,
    auth_header: str,
) -> dict | None:
    """Fetch Jira issue status using Databricks SQL http_request()."""
    url = f"{jira_url.rstrip('/')}/rest/api/3/issue/{issue_key}?fields=status,resolution"
    result = spark.sql(f"""
        SELECT http_request(
            url => '{url}',
            method => 'GET',
            headers => map('Authorization', '{auth_header}', 'Accept', 'application/json')
        ) AS resp
    """).first()

    resp = result["resp"]
    if resp["status_code"] != 200:
        return None

    data = json.loads(resp["text"])
    fields = data.get("fields", {})
    status = fields.get("status", {})
    return {
        "status_name": status.get("name", "").lower(),
        "status_category": status.get("statusCategory", {}).get("key", "").lower(),
        "resolution": (fields.get("resolution") or {}).get("name", "").lower(),
    }


def sync_jira_statuses(
    spark: Any,
    dbutils: Any,
    catalog: str,
    control_schema: str,
    jira_url: str,
    secret_scope: str,
    secret_key: str,
    jira_email_key: str,
) -> int:
    """Sync Jira ticket statuses back to migration_plans.

    Checks all plans with a non-null jira_issue_key whose plan_status
    is not yet 'completed' or 'cancelled'. Updates plan_status based
    on the Jira ticket state.

    Returns the number of plans updated.
    """
    plans_ref = safe_table_ref(catalog, control_schema, "migration_plans")

    # Build auth header from secrets
    email = dbutils.secrets.get(scope=secret_scope, key=jira_email_key)
    api_token = dbutils.secrets.get(scope=secret_scope, key=secret_key)
    auth_raw = f"{email}:{api_token}"
    auth_header = "Basic " + base64.b64encode(auth_raw.encode()).decode()

    # Get plans with Jira keys that are still open
    open_plans = spark.sql(f"""
        SELECT plan_id, jira_issue_key, plan_status
        FROM {plans_ref}
        WHERE jira_issue_key IS NOT NULL
          AND plan_status NOT IN ('completed', 'cancelled')
    """).collect()

    if not open_plans:
        return 0

    updated = 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    for plan in open_plans:
        issue_key = plan["jira_issue_key"]
        current_status = plan["plan_status"]

        jira_info = _fetch_jira_status_via_sql(
            spark, jira_url, issue_key, auth_header
        )
        if jira_info is None:
            continue

        # Map Jira status category to plan_status
        category = jira_info["status_category"]
        resolution = jira_info["resolution"]

        if category == "done":
            new_status = "cancelled" if resolution == "cancelled" else "completed"
        elif category == "indeterminate":
            new_status = "in_progress"
        else:
            new_status = "pending"

        if new_status != current_status:
            note = f"Jira sync: {new_status} at {now.isoformat()}"
            spark.sql(f"""
                UPDATE {plans_ref}
                SET plan_status = '{new_status}',
                    notes = CONCAT(COALESCE(notes, ''), ' | {note}')
                WHERE plan_id = '{plan["plan_id"]}'
            """)
            updated += 1

    return updated
