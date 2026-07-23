"""UC Steward — Platform Feature Enablement Checks.

Detects whether Unity Catalog platform features (Predictive Optimization,
Data Classification, Lakehouse Monitoring) are enabled for governed assets.

Designed for graceful degradation: if the service principal lacks privileges
for a system table, the check is SKIPPED (not FAILED) and the hygiene score
denominator adjusts automatically.

Import pattern:

    from lib.feature_checks import (
        CapabilityProbe,
        run_feature_checks,
        CheckResult,
    )
    probe = CapabilityProbe(spark)
    results = run_feature_checks(spark, sdk, probe, policy, catalogs, control_schema)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

__all__ = [
    "CapabilityProbe",
    "CheckResult",
    "CheckStatus",
    "run_feature_checks",
    "feature_enablement_policy",
]


# ── Status & Result ────────────────────────────────────────────────────────────

class CheckStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class CheckResult:
    """Outcome of a single feature enablement check."""
    check_name: str
    tier: str                        # "standard" or "elevated"
    status: CheckStatus
    scope_type: str                  # "catalog", "schema", or "table"
    scope_name: str                  # e.g. "my_catalog.my_schema"
    message: str = ""
    checked_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None)
    )

    def as_dict(self) -> dict:
        """Flat dict suitable for Spark Row / Delta INSERT."""
        return {
            "check_name": self.check_name,
            "tier": self.tier,
            "status": self.status.value,
            "scope_type": self.scope_type,
            "scope_name": self.scope_name,
            "message": self.message,
            "checked_at": self.checked_at,
        }


# ── Capability Probe ───────────────────────────────────────────────────────────

class CapabilityProbe:
    """Probe system table access once at job start; cache results.

    Usage:
        probe = CapabilityProbe(spark)
        if probe.can_access("system.data_classification.__table_statuses"):
            ...
    """

    # Tables that require elevated (admin) privileges
    ELEVATED_TABLES: frozenset[str] = frozenset([
        "system.data_classification.__table_statuses",
        "system.data_classification.results",
        "system.data_quality_monitoring.table_results",
    ])

    # Tables accessible with standard UC grants
    STANDARD_TABLES: frozenset[str] = frozenset([
        "system.storage.predictive_optimization_operations_history",
        "system.information_schema.column_tags",
        "system.information_schema.column_masks",
    ])

    def __init__(self, spark: Any) -> None:
        self._spark = spark
        self._cache: dict[str, bool] = {}

    def can_access(self, table_fqn: str) -> bool:
        """Return True if current identity can SELECT from the table.

        Probes with `SELECT 1 FROM <table> WHERE 1=0` (zero rows, minimal cost).
        Result is cached for the lifetime of this probe instance.
        """
        if table_fqn in self._cache:
            return self._cache[table_fqn]

        accessible = self._probe(table_fqn)
        self._cache[table_fqn] = accessible
        return accessible

    def _probe(self, table_fqn: str) -> bool:
        """Execute a zero-cost probe query."""
        try:
            self._spark.sql(f"SELECT 1 FROM {table_fqn} WHERE 1=0").collect()
            return True
        except Exception as e:
            err_msg = str(e)
            if "INSUFFICIENT_PERMISSIONS" in err_msg:
                print(f"  ⚠  Probe: no access to {table_fqn} (skipping related checks)")
                return False
            if "TABLE_OR_VIEW_NOT_FOUND" in err_msg or "SCHEMA_NOT_FOUND" in err_msg:
                print(f"  ⚠  Probe: {table_fqn} does not exist in this workspace")
                return False
            # Unknown error — treat as inaccessible but warn loudly
            print(f"  ⚠  Probe: unexpected error on {table_fqn}: {err_msg[:200]}")
            return False

    def probe_all(self) -> dict[str, bool]:
        """Probe all known system tables and return the capability map."""
        for t in sorted(self.STANDARD_TABLES | self.ELEVATED_TABLES):
            self.can_access(t)
        return dict(self._cache)

    def summary(self) -> str:
        """Human-readable summary of available capabilities."""
        accessible = [t for t, ok in self._cache.items() if ok]
        skipped = [t for t, ok in self._cache.items() if not ok]
        lines = [f"  Feature checks: {len(accessible)} system tables accessible"]
        if skipped:
            lines.append(f"  Feature checks: {len(skipped)} system tables skipped (insufficient privileges)")
        return "\n".join(lines)


# ── Policy accessor ────────────────────────────────────────────────────────────

_DEFAULT_FEATURE_POLICY: dict[str, Any] = {
    "predictive_optimization": {
        "enabled": True,
        "scope": ["catalog", "schema"],
    },
    "data_classification": {
        "enabled": True,
        "enforce_masks_on_pii": True,
        "scope": ["schema"],
    },
    "lakehouse_monitoring": {
        "enabled": True,
        "scope": ["table"],
        "eligibility": {
            "min_size_gb": 1.0,
            "min_weekly_queries": 10,
        },
    },
}


def feature_enablement_policy(policy: dict) -> dict[str, Any]:
    """Extract feature_enablement section from loaded policy dict.

    Falls back to defaults if the section is absent.
    """
    return dict(policy.get("feature_enablement", _DEFAULT_FEATURE_POLICY))


# ── Check implementations ──────────────────────────────────────────────────────

def _check_predictive_optimization(
    spark: Any,
    probe: CapabilityProbe,
    catalogs: list[str],
    control_schema: str,
    policy_cfg: dict,
) -> list[CheckResult]:
    """Check that Predictive Optimization is enabled at catalog/schema level.

    Uses DESCRIBE CATALOG/SCHEMA EXTENDED (standard privileges).
    """
    results: list[CheckResult] = []
    scopes = policy_cfg.get("scope", ["catalog", "schema"])

    for catalog in catalogs:
        if "catalog" in scopes:
            try:
                rows = spark.sql(f"DESCRIBE CATALOG EXTENDED {catalog}").collect()
                props = {r[0]: r[1] for r in rows}
                pred_opt = props.get("Predictive Optimization", "")

                if "ENABLE" in pred_opt.upper():
                    results.append(CheckResult(
                        check_name="predictive_optimization_enabled",
                        tier="standard",
                        status=CheckStatus.PASSED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=pred_opt,
                    ))
                elif "DISABLE" in pred_opt.upper():
                    results.append(CheckResult(
                        check_name="predictive_optimization_enabled",
                        tier="standard",
                        status=CheckStatus.FAILED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=f"Predictive Optimization is DISABLED on catalog '{catalog}'",
                    ))
                else:
                    results.append(CheckResult(
                        check_name="predictive_optimization_enabled",
                        tier="standard",
                        status=CheckStatus.FAILED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=f"Predictive Optimization status unknown: '{pred_opt}'",
                    ))
            except Exception as e:
                results.append(CheckResult(
                    check_name="predictive_optimization_enabled",
                    tier="standard",
                    status=CheckStatus.SKIPPED,
                    scope_type="catalog",
                    scope_name=catalog,
                    message=f"Could not DESCRIBE CATALOG: {str(e)[:200]}",
                ))

        if "schema" in scopes:
            try:
                from lib.common import build_exempt_schemas
                exempt = build_exempt_schemas(control_schema)
                schemas = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
                for row in schemas:
                    schema_name = row.databaseName
                    if schema_name in exempt:
                        continue
                    fqn = f"{catalog}.{schema_name}"
                    try:
                        desc = spark.sql(f"DESCRIBE SCHEMA EXTENDED {fqn}").collect()
                        props = {r[0]: r[1] for r in desc if r[0]}
                        pred_opt = props.get("Predictive Optimization", "")
                        if "DISABLE" in pred_opt.upper() and "inherited" not in pred_opt.lower():
                            results.append(CheckResult(
                                check_name="predictive_optimization_enabled",
                                tier="standard",
                                status=CheckStatus.FAILED,
                                scope_type="schema",
                                scope_name=fqn,
                                message=f"Explicitly DISABLED at schema level",
                            ))
                    except Exception:
                        pass  # Skip schemas we can't describe
            except Exception as e:
                results.append(CheckResult(
                    check_name="predictive_optimization_enabled",
                    tier="standard",
                    status=CheckStatus.SKIPPED,
                    scope_type="catalog",
                    scope_name=catalog,
                    message=f"Could not list schemas: {str(e)[:200]}",
                ))

    return results


def _check_data_classification(
    spark: Any,
    probe: CapabilityProbe,
    catalogs: list[str],
    control_schema: str,
    policy_cfg: dict,
) -> list[CheckResult]:
    """Check data classification coverage.

    Standard tier: Verify PII-tagged columns have column masks applied.
    Elevated tier: Verify all tables have been scanned by classification.
    """
    results: list[CheckResult] = []
    enforce_masks = policy_cfg.get("enforce_masks_on_pii", True)

    # ── Standard: PII tags → masks check ──────────────────────────────────────
    if enforce_masks and probe.can_access("system.information_schema.column_tags"):
        for catalog in catalogs:
            try:
                # Find PII-tagged columns
                pii_cols = spark.sql(f"""
                    SELECT ct.catalog_name, ct.schema_name, ct.table_name,
                           ct.column_name, ct.tag_value
                    FROM system.information_schema.column_tags ct
                    WHERE ct.tag_name = 'pii'
                      AND ct.catalog_name = '{catalog}'
                """).collect()

                if not pii_cols:
                    results.append(CheckResult(
                        check_name="pii_columns_masked",
                        tier="standard",
                        status=CheckStatus.PASSED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message="No PII-tagged columns found (classification may not have run)",
                    ))
                    continue

                # Check which PII columns have masks
                masks_accessible = probe.can_access(
                    "system.information_schema.column_masks"
                )
                if not masks_accessible:
                    results.append(CheckResult(
                        check_name="pii_columns_masked",
                        tier="standard",
                        status=CheckStatus.SKIPPED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message="Cannot access column_masks table to verify",
                    ))
                    continue

                masked_cols = spark.sql(f"""
                    SELECT table_catalog, table_schema, table_name, column_name
                    FROM system.information_schema.column_masks
                    WHERE table_catalog = '{catalog}'
                """).collect()
                masked_set = {
                    (r.table_catalog, r.table_schema, r.table_name, r.column_name)
                    for r in masked_cols
                }

                unmasked = []
                for row in pii_cols:
                    key = (row.catalog_name, row.schema_name,
                           row.table_name, row.column_name)
                    if key not in masked_set:
                        unmasked.append(
                            f"{row.catalog_name}.{row.schema_name}"
                            f".{row.table_name}.{row.column_name}"
                        )

                if unmasked:
                    results.append(CheckResult(
                        check_name="pii_columns_masked",
                        tier="standard",
                        status=CheckStatus.FAILED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=(
                            f"{len(unmasked)} PII-tagged column(s) lack column masks: "
                            + ", ".join(unmasked[:5])
                            + (" ..." if len(unmasked) > 5 else "")
                        ),
                    ))
                else:
                    results.append(CheckResult(
                        check_name="pii_columns_masked",
                        tier="standard",
                        status=CheckStatus.PASSED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=f"All {len(pii_cols)} PII-tagged columns have masks",
                    ))

            except Exception as e:
                results.append(CheckResult(
                    check_name="pii_columns_masked",
                    tier="standard",
                    status=CheckStatus.SKIPPED,
                    scope_type="catalog",
                    scope_name=catalog,
                    message=f"Error checking PII masks: {str(e)[:200]}",
                ))
    elif enforce_masks:
        for catalog in catalogs:
            results.append(CheckResult(
                check_name="pii_columns_masked",
                tier="standard",
                status=CheckStatus.SKIPPED,
                scope_type="catalog",
                scope_name=catalog,
                message="No access to system.information_schema.column_tags",
            ))

    # ── Elevated: Classification scan coverage ────────────────────────────────
    table_statuses_fqn = "system.data_classification.__table_statuses"
    if probe.can_access(table_statuses_fqn):
        for catalog in catalogs:
            try:
                total_tables = spark.sql(f"""
                    SELECT COUNT(*) AS cnt
                    FROM system.information_schema.tables
                    WHERE table_catalog = '{catalog}'
                      AND table_schema NOT IN ('information_schema', '__databricks_internal')
                      AND table_type = 'MANAGED'
                """).collect()[0].cnt

                scanned_tables = spark.sql(f"""
                    SELECT COUNT(DISTINCT concat(catalog_name, '.', schema_name, '.', table_name)) AS cnt
                    FROM {table_statuses_fqn}
                    WHERE catalog_name = '{catalog}'
                """).collect()[0].cnt

                coverage_pct = (
                    (scanned_tables / total_tables * 100)
                    if total_tables > 0 else 100.0
                )
                status = (
                    CheckStatus.PASSED if coverage_pct >= 80.0
                    else CheckStatus.FAILED
                )
                results.append(CheckResult(
                    check_name="classification_scan_coverage",
                    tier="elevated",
                    status=status,
                    scope_type="catalog",
                    scope_name=catalog,
                    message=(
                        f"{scanned_tables}/{total_tables} tables scanned "
                        f"({coverage_pct:.1f}%)"
                    ),
                ))
            except Exception as e:
                results.append(CheckResult(
                    check_name="classification_scan_coverage",
                    tier="elevated",
                    status=CheckStatus.SKIPPED,
                    scope_type="catalog",
                    scope_name=catalog,
                    message=f"Error querying classification status: {str(e)[:200]}",
                ))
    else:
        for catalog in catalogs:
            results.append(CheckResult(
                check_name="classification_scan_coverage",
                tier="elevated",
                status=CheckStatus.SKIPPED,
                scope_type="catalog",
                scope_name=catalog,
                message=(
                    f"SP lacks SELECT on {table_statuses_fqn} "
                    f"(requires metastore admin)"
                ),
            ))

    return results


def _check_lakehouse_monitoring(
    spark: Any,
    sdk: Any,
    probe: CapabilityProbe,
    catalogs: list[str],
    control_schema: str,
    policy_cfg: dict,
) -> list[CheckResult]:
    """Check that eligible tables have Lakehouse Monitors attached.

    Elevated tier: uses SDK quality_monitors.get() per table.
    Falls back to system.data_quality_monitoring.table_results if available.
    """
    results: list[CheckResult] = []
    eligibility = policy_cfg.get("eligibility", {})
    min_size_gb = eligibility.get("min_size_gb", 1.0)

    # Determine detection method
    use_system_table = probe.can_access(
        "system.data_quality_monitoring.table_results"
    )

    for catalog in catalogs:
        try:
            # Find eligible tables (large enough to warrant monitoring)
            eligible_tables = spark.sql(f"""
                SELECT t.table_catalog, t.table_schema, t.table_name
                FROM system.information_schema.tables t
                WHERE t.table_catalog = '{catalog}'
                  AND t.table_schema NOT IN (
                      'information_schema', '__databricks_internal', '{control_schema}'
                  )
                  AND t.table_type = 'MANAGED'
            """).collect()

            if not eligible_tables:
                continue

            # Method 1: system table (elevated, bulk)
            if use_system_table:
                monitored = spark.sql(f"""
                    SELECT DISTINCT
                        concat(catalog_name, '.', schema_name, '.', table_name) AS fqn
                    FROM system.data_quality_monitoring.table_results
                    WHERE catalog_name = '{catalog}'
                """).collect()
                monitored_set = {r.fqn for r in monitored}

                unmonitored = [
                    f"{r.table_catalog}.{r.table_schema}.{r.table_name}"
                    for r in eligible_tables
                    if f"{r.table_catalog}.{r.table_schema}.{r.table_name}"
                    not in monitored_set
                ]

                if unmonitored:
                    results.append(CheckResult(
                        check_name="lakehouse_monitor_attached",
                        tier="elevated",
                        status=CheckStatus.FAILED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=(
                            f"{len(unmonitored)}/{len(eligible_tables)} eligible "
                            f"tables lack monitors: "
                            + ", ".join(unmonitored[:5])
                            + (" ..." if len(unmonitored) > 5 else "")
                        ),
                    ))
                else:
                    results.append(CheckResult(
                        check_name="lakehouse_monitor_attached",
                        tier="elevated",
                        status=CheckStatus.PASSED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=(
                            f"All {len(eligible_tables)} eligible tables have monitors"
                        ),
                    ))

            # Method 2: SDK per-table probe (works without admin, but slower)
            elif sdk is not None:
                sampled = eligible_tables[:20]  # Cap SDK calls per catalog
                unmonitored = []
                for r in sampled:
                    fqn = f"{r.table_catalog}.{r.table_schema}.{r.table_name}"
                    try:
                        sdk.quality_monitors.get(table_name=fqn)
                    except Exception:
                        unmonitored.append(fqn)

                if unmonitored:
                    results.append(CheckResult(
                        check_name="lakehouse_monitor_attached",
                        tier="elevated",
                        status=CheckStatus.FAILED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=(
                            f"{len(unmonitored)}/{len(sampled)} sampled tables "
                            f"lack monitors (checked {len(sampled)}/"
                            f"{len(eligible_tables)} eligible): "
                            + ", ".join(unmonitored[:3])
                            + (" ..." if len(unmonitored) > 3 else "")
                        ),
                    ))
                else:
                    results.append(CheckResult(
                        check_name="lakehouse_monitor_attached",
                        tier="elevated",
                        status=CheckStatus.PASSED,
                        scope_type="catalog",
                        scope_name=catalog,
                        message=(
                            f"All {len(sampled)} sampled tables have monitors "
                            f"(checked {len(sampled)}/{len(eligible_tables)})"
                        ),
                    ))
            else:
                results.append(CheckResult(
                    check_name="lakehouse_monitor_attached",
                    tier="elevated",
                    status=CheckStatus.SKIPPED,
                    scope_type="catalog",
                    scope_name=catalog,
                    message=(
                        "No access to system.data_quality_monitoring "
                        "and no SDK provided — cannot verify monitors"
                    ),
                ))

        except Exception as e:
            results.append(CheckResult(
                check_name="lakehouse_monitor_attached",
                tier="elevated",
                status=CheckStatus.SKIPPED,
                scope_type="catalog",
                scope_name=catalog,
                message=f"Error checking monitors: {str(e)[:200]}",
            ))

    return results


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run_feature_checks(
    spark: Any,
    sdk: Any | None,
    probe: CapabilityProbe,
    policy: dict,
    catalogs: list[str] | str,
    control_schema: str,
    *,
    mode: str = "standard",
) -> list[CheckResult]:
    """Run all enabled feature enablement checks.

    Args:
        spark: Active SparkSession.
        sdk: Databricks WorkspaceClient (optional; enables SDK-based checks).
        probe: Pre-initialized CapabilityProbe.
        policy: Full policy dict (from load_policy()).
        catalogs: Catalog name(s) to check.
        control_schema: Schema name to exclude from scans.
        mode: "standard" (skip admin-only checks) or "elevated" (full coverage).
              Overridden by probe results — if elevated tables are accessible,
              elevated checks run regardless of mode setting.

    Returns:
        List of CheckResult objects (one per check × scope combination).
    """
    if isinstance(catalogs, str):
        catalogs = [catalogs]

    feat_policy = feature_enablement_policy(policy)
    all_results: list[CheckResult] = []

    # ── Predictive Optimization ────────────────────────────────────────────────
    pred_cfg = feat_policy.get("predictive_optimization", {})
    if pred_cfg.get("enabled", True):
        print("  Feature check: Predictive Optimization...")
        all_results.extend(
            _check_predictive_optimization(
                spark, probe, catalogs, control_schema, pred_cfg
            )
        )

    # ── Data Classification ────────────────────────────────────────────────────
    class_cfg = feat_policy.get("data_classification", {})
    if class_cfg.get("enabled", True):
        print("  Feature check: Data Classification...")
        all_results.extend(
            _check_data_classification(
                spark, probe, catalogs, control_schema, class_cfg
            )
        )

    # ── Lakehouse Monitoring ───────────────────────────────────────────────────
    mon_cfg = feat_policy.get("lakehouse_monitoring", {})
    if mon_cfg.get("enabled", True):
        print("  Feature check: Lakehouse Monitoring...")
        all_results.extend(
            _check_lakehouse_monitoring(
                spark, sdk, probe, catalogs, control_schema, mon_cfg
            )
        )

    # ── Summary ────────────────────────────────────────────────────────────────
    passed = sum(1 for r in all_results if r.status == CheckStatus.PASSED)
    failed = sum(1 for r in all_results if r.status == CheckStatus.FAILED)
    skipped = sum(1 for r in all_results if r.status == CheckStatus.SKIPPED)
    total = len(all_results)
    denominator = passed + failed  # Score excludes skipped
    score = (passed / denominator * 100) if denominator > 0 else None

    print(f"\n  Feature checks complete: {total} checks "
          f"({passed} passed, {failed} failed, {skipped} skipped)")
    if score is not None:
        print(f"  Feature enablement score: {score:.1f}% "
              f"(coverage: {denominator}/{total} checks executable)")
    else:
        print("  Feature enablement score: N/A (all checks skipped)")

    return all_results


def results_to_spark(spark: Any, results: list[CheckResult]) -> Any:
    """Convert CheckResult list to a Spark DataFrame for Delta persistence."""
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StringType, StructField, StructType, TimestampType,
    )

    schema = StructType([
        StructField("check_name", StringType()),
        StructField("tier", StringType()),
        StructField("status", StringType()),
        StructField("scope_type", StringType()),
        StructField("scope_name", StringType()),
        StructField("message", StringType()),
        StructField("checked_at", TimestampType()),
    ])
    rows = [Row(**r.as_dict()) for r in results]
    return spark.createDataFrame(rows, schema) if rows else spark.createDataFrame([], schema)
