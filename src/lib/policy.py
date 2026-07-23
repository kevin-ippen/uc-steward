"""UC Steward — policy loader.

Single access point for governance policy settings.
Reads from the governance_policy control table, which is seeded from
policies/policy.yml by 00_control_plane_bootstrap on every run.

All helpers accept a ``policy`` dict returned by load_policy() and
fall back to hardcoded defaults when the table is missing (pre-bootstrap
or dry-run).

Import pattern (add alongside the lib.common import in each notebook):

    from lib.policy import (
        load_policy,
        required_table_tags,
        pii_patterns,
        naming_patterns,
        anti_patterns,
    )
    _policy = load_policy(spark, catalog, control_schema)
"""
from __future__ import annotations

import json
from typing import Any


def load_policy(spark: Any, catalog: str, control_schema: str) -> dict[str, Any]:
    """Load all active policy entries from the governance_policy table.

    Returns a ``policy_key → Python value`` dict.
    Returns {} if the table does not exist (pre-bootstrap or dry run);
    notebooks fall back to each helper's hardcoded defaults.
    """
    fqn = f"{catalog}.{control_schema}.governance_policy"
    try:
        rows = (
            spark.sql(
                f"SELECT policy_key, policy_value FROM {fqn} WHERE is_active = true"
            ).collect()
        )
        result: dict[str, Any] = {}
        for row in rows:
            try:
                result[row.policy_key] = json.loads(row.policy_value)
            except (json.JSONDecodeError, ValueError):
                result[row.policy_key] = row.policy_value  # keep as string
        print(f"  Policy: loaded {len(result)} entries from {fqn}")
        return result
    except Exception as e:
        print(f"  ⚠  Could not load governance_policy ({fqn}): {e}")
        print("      Using hardcoded defaults — run 00_control_plane_bootstrap to seed.")
        return {}


# ── Tag policy ──────────────────────────────────────────────────────────────────────────

def required_table_tags(
    policy: dict, widget_override: str = ""
) -> list[str]:
    """Return required table tag names.

    Priority: widget_override (explicit run param) > policy table > hardcoded default.
    """
    if widget_override.strip():
        return [t.strip() for t in widget_override.split(",") if t.strip()]
    return list(policy.get("required_table_tags", ["owner", "domain", "quality_tier"]))


# ── PII patterns ────────────────────────────────────────────────────────────────────────

_DEFAULT_PII_PATTERNS: dict[str, list[str]] = {
    "email":      [".*email.*", ".*e_mail.*"],
    "phone":      [".*phone.*", ".*mobile.*", ".*cell.*"],
    "ssn":        [".*ssn.*", ".*social_sec.*"],
    "name":       [".*first_name.*", ".*last_name.*", ".*full_name.*"],
    "address":    [".*address.*", ".*street.*", ".*zip_code.*"],
    "dob":        [".*date_of_birth.*", r".*\bdob\b.*", ".*birth_date.*"],
    "ip_address": [".*ip_addr.*", ".*client_ip.*", ".*source_ip.*"],
}


def pii_patterns(policy: dict) -> dict[str, list[str]]:
    """Return PII column-name regex patterns by category."""
    return dict(policy.get("pii_patterns", _DEFAULT_PII_PATTERNS))


# ── Naming policy ───────────────────────────────────────────────────────────────────────

def naming_patterns(policy: dict) -> tuple[str, str]:
    """Return ``(table_name_pattern, schema_name_pattern)`` regexes."""
    return (
        policy.get("table_name_pattern", "^[a-z][a-z0-9_]*$"),
        policy.get("schema_name_pattern", "^[a-z][a-z0-9_]*$"),
    )


_DEFAULT_ANTI_PATTERNS: list[tuple[str, str]] = [
    (r".*\d{8}$",    "Date suffix detected — use table partitioning instead"),
    (r".*_copy\d*$", "Copy suffix — likely a duplicated table"),
    (r".*_backup$",  "Backup suffix — use Delta time travel instead"),
    (r".*_old$",     "Old suffix — candidate for deprecation"),
    (r".*_test$",    "Test suffix — should not be in production"),
    (r".*_temp$",    "Temp suffix — should be cleaned up"),
    (r".*_v\d+$",    "Version suffix — use table versioning instead"),
]


def anti_patterns(policy: dict) -> list[tuple[str, str]]:
    """Return list of ``(regex_pattern, description)`` anti-pattern tuples."""
    raw = policy.get("naming_anti_patterns", _DEFAULT_ANTI_PATTERNS)
    result: list[tuple[str, str]] = []
    for item in raw:
        if isinstance(item, dict):
            result.append((item["pattern"], item.get("description", "")))
        elif isinstance(item, (list, tuple)):
            result.append((str(item[0]), str(item[1]) if len(item) > 1 else ""))
        else:
            result.append((str(item), ""))
    return result
